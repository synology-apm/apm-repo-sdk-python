"""Tests for the restore flow of examples/restore_to_nutanix_ahv.py: ``execute_restore`` over a fake target,
``Rollback``, and the repository side (``open_repository``, ``resolve_start``, ``collect_disks``,
``detect_firmware``, ``plan_restore``, ``export_to_lun``, ``restore``)."""

from __future__ import annotations

import asyncio
import re
import struct
import uuid
from collections.abc import AsyncIterator
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar

import pytest

from support.fakes import faithful_to, unchecked_fake
from synology_apm_repo.sdk import Frame, Node, NodeFrame, NodeRef, RestorableUnit, RootFrame, UnitKind
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.export import ExportResult
from synology_apm_repo.sdk.units.base import ContentSource, UnitProvider

_MIB = 1 << 20
_GIB = 1 << 30
_ESP = "c12a7328-f81f-11d2-ba4b-00a0c93ec93b"


# -- Rollback ------------------------------------------------------------------


async def test_rollback_runs_steps_newest_first_when_the_body_raises(ex: ModuleType) -> None:
    order: list[str] = []

    async def step(name: str) -> None:
        order.append(name)

    with pytest.raises(KeyError, match="boom"):
        async with ex.Rollback() as rollback:
            rollback.add(step, "first")
            rollback.add(step, "second")
            raise KeyError("boom")

    assert order == ["second", "first"]


async def test_rollback_runs_nothing_after_clear_or_on_success(ex: ModuleType) -> None:
    order: list[str] = []

    async def step(name: str) -> None:
        order.append(name)

    async with ex.Rollback() as rollback:
        rollback.add(step, "ok")
    with pytest.raises(KeyError, match="boom"):
        async with ex.Rollback() as cleared:
            cleared.add(step, "cleared")
            cleared.clear()
            raise KeyError("boom")

    assert order == []


async def test_rollback_attaches_a_failing_step_to_the_original_error_and_keeps_going(ex: ModuleType) -> None:
    order: list[str] = []

    async def bad(name: str) -> None:
        order.append(name)
        raise RuntimeError(f"{name} failed")

    async def good(name: str) -> None:
        order.append(name)

    with pytest.raises(KeyError, match="original") as caught:
        async with ex.Rollback() as rollback:
            rollback.add(good, "a")
            rollback.add(bad, "b")
            raise KeyError("original")

    assert order == ["b", "a"]
    assert caught.value.__notes__ == ["rollback step failed: b failed"]


async def test_rollback_also_runs_when_the_task_is_cancelled(ex: ModuleType) -> None:
    ran: list[str] = []

    async def step() -> None:
        ran.append("cleanup")

    with pytest.raises(asyncio.CancelledError):
        async with ex.Rollback() as rollback:
            rollback.add(step)
            raise asyncio.CancelledError

    assert ran == ["cleanup"]


# -- execute_restore -----------------------------------------------------------

# The index each Volume Group disk reports, by position: neither sequential nor in order, so that anything
# using the position instead of the index would pick the wrong LUN.
_INDEXES = (7, 3, 5, 1)


@unchecked_fake("examples/restore_to_nutanix_ahv.py's RestoreTarget, loaded at test time")
class _FakeTarget:
    """A ``RestoreTarget`` that records calls; ``fail`` maps a method name to the error it raises."""

    def __init__(self, ex: ModuleType) -> None:
        self._ex = ex
        self.log: list[tuple[Any, ...]] = []
        self.fail: dict[str, BaseException] = {}
        self.detachable = True
        self.size_adjust = 0  # the group's disks report a size other than the one asked for

    def _call(self, name: str, *args: Any) -> None:
        self.log.append((name, *args))
        if name in self.fail:
            raise self.fail[name]

    def names(self) -> list[str]:
        return [entry[0] for entry in self.log]

    async def create_volume_group(self, name: str, cluster: str, container: str, sizes: list[int]) -> Any:
        self._call("create_volume_group", name, cluster, container, sizes)
        disks = tuple(
            self._ex.VolumeGroupDisk(f"d{i}", _INDEXES[i], size + self.size_adjust) for i, size in enumerate(sizes)
        )
        return self._ex.VolumeGroupInfo("vg-1", "tgt", disks)

    async def allow_initiator(self, volume_group_id: str, iqn: str) -> None:
        self._call("allow_initiator", volume_group_id, iqn)

    async def create_vm(
        self, name: str, cluster: str, container: str, group: Any, sizes: list[int], **options: Any
    ) -> str:
        self._call("create_vm", name, cluster, container, sizes, options)
        return "vm-1"

    async def wait_hydrated(self, vm_id: str) -> bool:
        self._call("wait_hydrated", vm_id)
        return self.detachable

    async def delete_volume_group(self, ext_id: str) -> None:
        self._call("delete_volume_group", ext_id)

    async def delete_vm(self, ext_id: str) -> None:
        self._call("delete_vm", ext_id)


def _settings(ex: ModuleType, **overrides: Any) -> Any:
    values: dict[str, Any] = {
        "vm_name": "web-01",
        "cluster": "cluster-1",
        "container": "container-1",
        "dsip": "10.0.0.9",
        "iqn_prefix": "iqn.p:",
        "initiator_iqn": "iqn.me",
        "cpus": 1,
        "memory_gib": 2,
        "keep_volume_group": False,
    }
    return ex.RestoreSettings(**{**values, **overrides})


def _plan(ex: ModuleType, *sizes: int, firmware: str = "uefi") -> Any:
    disks = [ex.SourceDisk(f"disk-{i}", SimpleNamespace(), size) for i, size in enumerate(sizes)]
    return ex.RestorePlan(disks, firmware)


class _Exporter:
    """An ``ExportDisk`` that records into the target's log, so the order of every step can be asserted."""

    def __init__(self, target: _FakeTarget) -> None:
        self._target = target
        self.calls: list[tuple[str, str, int, int, int]] = []
        self.fail_on: int | None = None

    async def __call__(self, disk: Any, url: str, index: int, total: int, capacity: int) -> None:
        self.calls.append((disk.label, url, index, total, capacity))
        self._target.log.append(("export", disk.label))
        if index == self.fail_on:
            raise OSError("iSCSI went away")


async def _run(ex: ModuleType, plan: Any, target: _FakeTarget, **options: Any) -> Any:
    export = options.pop("export", None) or _Exporter(target)
    settings = options.pop("settings", None) or _settings(ex)
    return await ex.execute_restore(plan, settings, target, export)


async def test_execute_restore_runs_the_steps_in_order_and_returns_the_vm(ex: ModuleType) -> None:
    target = _FakeTarget(ex)

    vm_id = await _run(ex, _plan(ex, 10 * _MIB, 20 * _MIB), target)

    assert vm_id == "vm-1"
    assert target.names() == [
        "create_volume_group",
        "allow_initiator",
        "export",
        "export",
        "create_vm",
        "wait_hydrated",
        "delete_volume_group",
    ]
    _, name, cluster, container, sizes = target.log[0]
    assert re.fullmatch(r"web-01-restore-[0-9a-f]{8}", name)
    assert (cluster, container, sizes) == ("cluster-1", "container-1", [10 * _MIB, 20 * _MIB])
    assert target.log[1] == ("allow_initiator", "vg-1", "iqn.me")
    assert target.log[-1] == ("delete_volume_group", "vg-1")


async def test_execute_restore_exports_each_disk_to_the_lun_numbered_by_its_volume_group_disk_index(
    ex: ModuleType,
) -> None:
    target = _FakeTarget(ex)
    export = _Exporter(target)

    await _run(ex, _plan(ex, 10 * _MIB, 20 * _MIB, 30 * _MIB), target, export=export)

    # The disks report indexes 7, 3 and 5: those are the LUN numbers, not 0, 1 and 2.
    assert [(label, url.rsplit("/", 1)[1]) for label, url, *_ in export.calls] == [
        ("disk-0", "7"),
        ("disk-1", "3"),
        ("disk-2", "5"),
    ]
    assert export.calls[0][1] == "iscsi://10.0.0.9:3260/iqn.p:tgt/7"


async def test_execute_restore_tells_the_exporter_the_capacity_the_lun_must_report(ex: ModuleType) -> None:
    target = _FakeTarget(ex)
    export = _Exporter(target)

    await _run(ex, _plan(ex, 10 * _MIB, 20 * _MIB), target, export=export)

    assert [(position, total, capacity) for _, _, position, total, capacity in export.calls] == [
        (0, 2, 10 * _MIB),
        (1, 2, 20 * _MIB),
    ]


async def test_execute_restore_checks_the_lun_against_the_size_the_volume_group_disk_reports(
    ex: ModuleType,
) -> None:
    target = _FakeTarget(ex)
    target.size_adjust = 4096  # e.g. the cluster rounded the disk up
    export = _Exporter(target)

    await _run(ex, _plan(ex, 10 * _MIB, 20 * _MIB), target, export=export)

    assert [capacity for *_, capacity in export.calls] == [10 * _MIB + 4096, 20 * _MIB + 4096]


@pytest.mark.parametrize(
    ("vm_name", "group_prefix"),
    [
        ("web-01", "web-01-restore-"),
        ("my vm.1_x", "my-vm.1_x-restore-"),
        ("ünï&co", "-n--co-restore-"),
    ],
)
async def test_execute_restore_names_the_group_with_safe_characters_only(
    ex: ModuleType, vm_name: str, group_prefix: str
) -> None:
    target = _FakeTarget(ex)

    await _run(ex, _plan(ex, _MIB), target, settings=_settings(ex, vm_name=vm_name))

    assert re.fullmatch(re.escape(group_prefix) + "[0-9a-f]{8}", target.log[0][1])


@pytest.mark.parametrize(("firmware", "uefi"), [("uefi", True), ("bios", False)])
async def test_execute_restore_maps_the_firmware_and_converts_the_memory(
    ex: ModuleType, firmware: str, uefi: bool
) -> None:
    target = _FakeTarget(ex)

    await _run(ex, _plan(ex, _MIB, firmware=firmware), target, settings=_settings(ex, cpus=3, memory_gib=4))

    options = target.log[target.names().index("create_vm")][-1]
    assert options == {"cpus": 3, "memory_bytes": 4 * _GIB, "uefi": uefi}


async def test_execute_restore_keeps_the_group_when_the_vm_still_depends_on_it(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    target = _FakeTarget(ex)
    target.detachable = False

    await _run(ex, _plan(ex, _MIB), target)

    assert "delete_volume_group" not in target.names()
    assert "kept Volume Group vg-1" in capsys.readouterr().out


async def test_execute_restore_keeps_the_group_when_asked_to(ex: ModuleType) -> None:
    target = _FakeTarget(ex)

    await _run(ex, _plan(ex, _MIB), target, settings=_settings(ex, keep_volume_group=True))

    assert "delete_volume_group" not in target.names()
    assert "delete_vm" not in target.names()


async def test_execute_restore_removes_the_group_when_an_export_fails(ex: ModuleType) -> None:
    target = _FakeTarget(ex)
    export = _Exporter(target)
    export.fail_on = 1

    with pytest.raises(OSError, match="iSCSI went away"):
        await _run(ex, _plan(ex, _MIB, _MIB), target, export=export)

    assert target.names() == ["create_volume_group", "allow_initiator", "export", "export", "delete_volume_group"]


async def test_execute_restore_removes_the_group_when_the_lun_is_not_the_disks_own(ex: ModuleType) -> None:
    target = _FakeTarget(ex)

    async def wrong_lun(disk: Any, url: str, index: int, total: int, capacity: int) -> None:
        raise ValueError("the LUN reports 2097152 B but the Volume Group disk behind it is 1048576 B")

    with pytest.raises(ValueError, match="the LUN reports"):
        await _run(ex, _plan(ex, _MIB), target, export=wrong_lun)

    assert target.names() == ["create_volume_group", "allow_initiator", "delete_volume_group"]
    assert "create_vm" not in target.names()


@pytest.mark.parametrize("step", ["allow_initiator", "create_vm"])
async def test_execute_restore_removes_the_group_when_a_step_fails(ex: ModuleType, step: str) -> None:
    target = _FakeTarget(ex)
    target.fail[step] = RuntimeError(f"{step} failed")

    with pytest.raises(RuntimeError, match=f"{step} failed"):
        await _run(ex, _plan(ex, _MIB), target)

    assert target.names()[-1] == "delete_volume_group"
    assert "delete_vm" not in target.names()


async def test_execute_restore_removes_the_vm_before_the_group_when_hydration_fails(ex: ModuleType) -> None:
    target = _FakeTarget(ex)
    target.fail["wait_hydrated"] = RuntimeError("hydration failed")

    with pytest.raises(RuntimeError, match="hydration failed"):
        await _run(ex, _plan(ex, _MIB), target)

    assert target.names()[-2:] == ["delete_vm", "delete_volume_group"]


async def test_execute_restore_succeeds_with_a_warning_when_only_the_final_group_delete_fails(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    target = _FakeTarget(ex)
    target.fail["delete_volume_group"] = RuntimeError("cannot delete group")

    assert await _run(ex, _plan(ex, _MIB), target) == "vm-1"

    assert "delete_vm" not in target.names()
    assert target.names().count("delete_volume_group") == 1
    err = capsys.readouterr().err
    assert "VM vm-1 was restored" in err and "Volume Group vg-1" in err and "cannot delete group" in err


async def test_execute_restore_surfaces_the_original_error_when_cleanup_also_fails(ex: ModuleType) -> None:
    target = _FakeTarget(ex)
    export = _Exporter(target)
    export.fail_on = 0
    target.fail["delete_volume_group"] = RuntimeError("cleanup failed")

    with pytest.raises(OSError, match="iSCSI went away") as caught:
        await _run(ex, _plan(ex, _MIB), target, export=export)

    assert caught.value.__notes__ == ["rollback step failed: cleanup failed"]


def test_restore_settings_come_from_the_parsed_arguments(ex: ModuleType) -> None:
    args = ex.parse_args(
        [
            "r#a/b/c",
            "--pc-host",
            "pc",
            "--pc-user",
            "u",
            "--cluster",
            "cl",
            "--container",
            "ct",
            "--dsip",
            "1.2.3.4",
            "--vm-name",
            "vm",
            "--cpus",
            "4",
            "--memory-gib",
            "8",
            "--keep-volume-group",
        ]
    )

    settings = ex.RestoreSettings.from_args(args)

    assert (settings.vm_name, settings.cluster, settings.container, settings.dsip) == ("vm", "cl", "ct", "1.2.3.4")
    assert (settings.cpus, settings.memory_gib, settings.keep_volume_group) == (4, 8, True)
    assert settings.iqn_prefix == args.iqn_prefix and settings.initiator_iqn == args.initiator_iqn


def test_volume_group_info_lists_its_disk_ids_in_order(ex: ModuleType) -> None:
    group = ex.VolumeGroupInfo("vg", "t", (ex.VolumeGroupDisk("a", 4, 10), ex.VolumeGroupDisk("b", 0, 20)))

    assert group.disk_ext_ids == ("a", "b")


# -- repository fakes ----------------------------------------------------------


def _node(name: str, *, leaf: bool, kind: UnitKind | None = None, size: int | None = None) -> Node:
    return Node(ref=NodeRef("repo", (name,)), name=name, is_leaf=leaf, kind=kind, size=size)


@faithful_to(ContentSource)
class _Content:
    """A ``ContentSource`` over bytes that records its reads."""

    def __init__(self, data: bytes, size: int | None = None) -> None:
        self._data = data
        self.size = size if size is not None else len(data)
        self.reads: list[tuple[int, int]] = []

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        n = length if length is not None else self.size - offset
        self.reads.append((offset, n))
        return self._data[offset : offset + n].ljust(n, b"\0")

    async def stream(self, block: int = _MIB) -> AsyncIterator[tuple[int, bytes]]:
        return
        yield  # unreachable: makes this an (empty) async generator

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self, sink: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: object = None
    ) -> ExportResult:
        raise NotImplementedError


@faithful_to(UnitProvider)
class _Provider:
    def __init__(self, children: dict[str, list[Node]]) -> None:
        self._children = children
        self.visited: list[str] = []

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        self.visited.append(node.name)
        return self._children.get(node.name, [])

    async def unit(self, node: Node) -> RestorableUnit:
        return RestorableUnit(
            ref=node.ref, name=node.name, is_leaf=True, kind=node.kind, size=node.size, content=_Content(b"", node.size)
        )


def _vm_tree() -> tuple[_Provider, Node]:
    root = _node("Devices", leaf=False)
    device = _node("host-1", leaf=False)
    tree = {
        "Devices": [device],
        "host-1": [
            _node("disk.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=30 * _GIB),
            _node("(filesystem)", leaf=False, kind=UnitKind.DISK_FILESYSTEM),
            _node("notes.txt", leaf=True, kind=UnitKind.FILE, size=10),
        ],
        "(filesystem)": [_node("must-not-be-listed", leaf=True, kind=UnitKind.DISK_IMAGE, size=1)],
    }
    return _Provider(tree), root


# -- collect_disks -------------------------------------------------------------


async def test_collect_disks_walks_a_vm_and_skips_filesystem_containers_and_files(ex: ModuleType) -> None:
    provider, root = _vm_tree()

    disks = await ex.collect_disks(provider, root)

    assert [(d.label, d.size) for d in disks] == [("host-1/disk.img", 30 * _GIB)]
    assert "(filesystem)" not in provider.visited  # walking it would read the whole file tree
    assert isinstance(disks[0].unit, RestorableUnit)


async def test_collect_disks_lists_every_device_disk_in_order(ex: ModuleType) -> None:
    root = _node("Devices", leaf=False)
    provider = _Provider(
        {
            "Devices": [_node("host-a", leaf=False), _node("host-b", leaf=False)],
            "host-a": [_node("a0.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=1)],
            "host-b": [
                _node("b0.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=2),
                _node("b1.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=3),
            ],
        }
    )

    disks = await ex.collect_disks(provider, root)

    assert [d.label for d in disks] == ["host-a/a0.img", "host-b/b0.img", "host-b/b1.img"]


async def test_collect_disks_handles_a_pc_or_ps_version_whose_disks_hang_off_the_root(ex: ModuleType) -> None:
    root = _node("Disks", leaf=False)
    provider = _Provider(
        {
            "Disks": [
                _node("Disk 0", leaf=True, kind=UnitKind.DISK_IMAGE, size=100),
                _node("Disk 0 (filesystem)", leaf=False, kind=UnitKind.DISK_FILESYSTEM),
                _node("Disk 1", leaf=True, kind=UnitKind.DISK_IMAGE, size=200),
            ]
        }
    )

    disks = await ex.collect_disks(provider, root)

    assert [(d.label, d.size) for d in disks] == [("Disk 0", 100), ("Disk 1", 200)]
    assert "Disk 0 (filesystem)" not in provider.visited


async def test_collect_disks_accepts_a_ref_to_a_single_disk(ex: ModuleType) -> None:
    disk = _node("only.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=7)

    disks = await ex.collect_disks(_Provider({}), disk)

    assert [(d.label, d.size) for d in disks] == [("only.img", 7)]


async def test_collect_disks_rejects_a_disk_of_unknown_size(ex: ModuleType) -> None:
    root = _node("Disks", leaf=False)
    provider = _Provider({"Disks": [_node("Disk 0", leaf=True, kind=UnitKind.DISK_IMAGE, size=None)]})

    with pytest.raises(SystemExit, match="'Disk 0' has no known size"):
        await ex.collect_disks(provider, root)


async def test_collect_disks_rejects_a_ref_without_disks(ex: ModuleType) -> None:
    root = _node("Devices", leaf=False)
    provider = _Provider({"Devices": [_node("mail", leaf=True, kind=UnitKind.MAIL, size=1)]})

    with pytest.raises(SystemExit, match="no restorable disk images"):
        await ex.collect_disks(provider, root)


# -- resolve_start ---------------------------------------------------------------


@faithful_to(Repository)
class _FakeRepo:
    """The ``Repository`` calls the example makes."""

    def __init__(self, provider: Any = None, node: Node | None = None, level: str = "node") -> None:
        self.provider, self.node, self.level = provider, node, level
        self.is_encrypted = False
        self.keys: list[str] = []
        self.key_ok = True

    async def locate(self, ref: NodeRef, *, raw: object = None) -> Frame:
        if self.level == "node":
            assert self.node is not None
            return NodeFrame(self.provider, self.node)
        return RootFrame()

    async def set_key(self, key_string: str) -> SimpleNamespace:
        self.keys.append(key_string)
        return SimpleNamespace(verification=SimpleNamespace(ok=self.key_ok))


async def test_resolve_start_follows_a_human_ref_to_its_node(ex: ModuleType) -> None:
    provider, root = _vm_tree()

    got_provider, got_node = await ex.resolve_start(_FakeRepo(provider, root), NodeRef("repo", ("src", "wl", "ver")))

    assert (got_provider, got_node) == (provider, root)


@pytest.mark.parametrize("level", ["root", "catalog", "workload"])
async def test_resolve_start_needs_the_ref_to_reach_a_version(ex: ModuleType, level: str) -> None:
    with pytest.raises(SystemExit, match="must reach a backup version"):
        await ex.resolve_start(_FakeRepo(level=level), NodeRef("repo", ("src",)))


async def test_resolve_start_resolves_a_canonical_ref_through_its_version(ex: ModuleType) -> None:
    provider, root = _vm_tree()
    ref = NodeRef.parse("repo#cat:1/wl:2/ver:abc")

    got_provider, got_node = await ex.resolve_start(_FakeRepo(provider, root), ref)

    assert (got_provider, got_node) == (provider, root)


async def test_resolve_start_rejects_a_raw_ref(ex: ModuleType) -> None:
    with pytest.raises(SystemExit, match="raw refs are not supported"):
        await ex.resolve_start(_FakeRepo(), NodeRef.parse("repo#raw/x"))


def test_parse_ref_treats_a_bare_path_as_a_human_ref_with_no_segments(ex: ModuleType) -> None:
    assert ex.parse_ref("/backups/repo") == NodeRef.human("/backups/repo")
    assert ex.parse_ref("/backups/repo#src/wl/ver") == NodeRef("/backups/repo", ("src", "wl", "ver"))


# -- detect_firmware -------------------------------------------------------------


def _gpt(
    *,
    entries_lba: int = 2,
    count: int = 128,
    entry_size: int = 128,
    esp: bool = True,
    length: int = _MIB,
    sector: int = 512,
) -> bytes:
    disk = bytearray(length)
    disk[510:512] = b"\x55\xaa"
    disk[sector : sector + 8] = b"EFI PART"
    disk[sector + 72 : sector + 88] = struct.pack("<QII", entries_lba, count, entry_size)
    if esp and entry_size >= 16 and entries_lba >= 2:  # LBA 0/1 hold the MBR and this header itself
        offset = entries_lba * sector
        disk[offset : offset + 16] = uuid.UUID(_ESP).bytes_le
    return bytes(disk)


def _source(ex: ModuleType, data: bytes, size: int = 20 * _GIB) -> Any:
    content = _Content(data, size)
    return ex.SourceDisk("disk", SimpleNamespace(content=content), size)


async def test_detect_firmware_picks_uefi_for_a_gpt_disk_with_an_esp(ex: ModuleType) -> None:
    assert await ex.detect_firmware(_source(ex, _gpt())) == "uefi"


async def test_detect_firmware_finds_the_gpt_of_a_4096_byte_sector_disk(ex: ModuleType) -> None:
    assert await ex.detect_firmware(_source(ex, _gpt(sector=4096))) == "uefi"
    assert await ex.detect_firmware(_source(ex, _gpt(sector=4096, esp=False))) == "bios"


async def test_detect_firmware_reads_a_4096_sector_table_beyond_the_first_mib_at_its_own_sector_size(
    ex: ModuleType,
) -> None:
    data = bytearray(_gpt(sector=4096, entries_lba=1024, esp=False, length=6 * _MIB))
    data[1024 * 4096 : 1024 * 4096 + 16] = uuid.UUID(_ESP).bytes_le
    source = _source(ex, bytes(data))

    assert await ex.detect_firmware(source) == "uefi"
    assert source.unit.content.reads[-1] == (1024 * 4096, 128 * 128)


async def test_detect_firmware_picks_bios_for_a_gpt_disk_without_an_esp(ex: ModuleType) -> None:
    assert await ex.detect_firmware(_source(ex, _gpt(esp=False))) == "bios"


async def test_detect_firmware_picks_bios_for_an_mbr_disk(ex: ModuleType) -> None:
    mbr = bytearray(_MIB)
    mbr[510:512] = b"\x55\xaa"

    assert await ex.detect_firmware(_source(ex, bytes(mbr))) == "bios"


async def test_detect_firmware_reads_a_partition_table_that_lies_beyond_the_first_mib(ex: ModuleType) -> None:
    data = bytearray(_gpt(entries_lba=4096, esp=False, length=3 * _MIB))
    data[4096 * 512 : 4096 * 512 + 16] = uuid.UUID(_ESP).bytes_le
    source = _source(ex, bytes(data))

    assert await ex.detect_firmware(source) == "uefi"
    assert source.unit.content.reads[-1] == (4096 * 512, 128 * 128)


@pytest.mark.parametrize(
    "header",
    [
        {"entry_size": 0},
        {"entry_size": 8},
        {"entry_size": 100},  # below the 128-byte minimum
        {"entry_size": 130},  # in range, but not a multiple of 8
        {"count": 0},
        {"count": 0x7FFFFFFF},
        {"entries_lba": 0},
        {"entries_lba": 1},
        {"count": 65536},  # an 8 MiB table: inside the disk, but past the 1 MiB cap
        {"entry_size": 8192, "count": 16},  # a plausible table size, with an implausible entry size
    ],
)
async def test_detect_firmware_distrusts_an_implausible_gpt_header(
    ex: ModuleType, capsys: pytest.CaptureFixture[str], header: dict[str, Any]
) -> None:
    source = _source(ex, _gpt(**header))

    assert await ex.detect_firmware(source) == "bios"

    assert "looks corrupt" in capsys.readouterr().err
    assert max(length for _, length in source.unit.content.reads) <= _MIB  # nothing larger was requested


async def test_detect_firmware_distrusts_a_table_beyond_the_end_of_the_disk(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    assert await ex.detect_firmware(_source(ex, _gpt(), size=4096)) == "bios"

    assert "looks corrupt" in capsys.readouterr().err


# -- open_repository / plan_restore ----------------------------------------------


@faithful_to(Session)
class _FakeSession:
    def __init__(self, repos: list[Any]) -> None:
        self.repos = repos
        self.calls: list[tuple[Any, ...]] = []

    async def open(
        self, source: Any, key: str | None = None, *, root: str = "", progress: object = None, trace: object = None
    ) -> list[Any]:
        if isinstance(source, str):
            self.calls.append(("open", source))
        else:
            self.calls.append(("open_store", source, root))
        return self.repos


async def test_open_repository_opens_a_local_path(ex: ModuleType) -> None:
    repo = _FakeRepo()
    session = _FakeSession([repo])

    got = await ex.open_repository(session, NodeRef("/backups/repo", ()), profile=None, key=None)

    assert got is repo
    assert session.calls == [("open", "/backups/repo")]


async def test_open_repository_goes_through_a_saved_profile_with_a_store_relative_root(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_store_from_profile(name: str) -> tuple[str, str]:
        return ("store", name)

    monkeypatch.setattr(ex, "store_from_profile", fake_store_from_profile)
    session = _FakeSession([_FakeRepo()])

    await ex.open_repository(session, NodeRef("sub/path", ()), profile="my-profile", key=None)

    assert session.calls == [("open_store", ("store", "my-profile"), "sub/path")]


@pytest.mark.parametrize("count", [0, 2])
async def test_open_repository_needs_exactly_one_repository(ex: ModuleType, count: int) -> None:
    session = _FakeSession([_FakeRepo() for _ in range(count)])

    with pytest.raises(SystemExit, match=rf"expected one repository .* found {count}"):
        await ex.open_repository(session, NodeRef("/x", ()), profile=None, key=None)


async def test_open_repository_needs_a_key_for_an_encrypted_repository(ex: ModuleType) -> None:
    repo = _FakeRepo()
    repo.is_encrypted = True

    with pytest.raises(SystemExit, match="encrypted; pass --key"):
        await ex.open_repository(_FakeSession([repo]), NodeRef("/x", ()), profile=None, key=None)


async def test_open_repository_rejects_a_wrong_key(ex: ModuleType) -> None:
    repo = _FakeRepo()
    repo.is_encrypted = True
    repo.key_ok = False

    with pytest.raises(SystemExit, match="key was rejected"):
        await ex.open_repository(_FakeSession([repo]), NodeRef("/x", ()), profile=None, key="id@secret")


async def test_open_repository_unlocks_with_the_key(ex: ModuleType) -> None:
    repo = _FakeRepo()
    repo.is_encrypted = True

    assert await ex.open_repository(_FakeSession([repo]), NodeRef("/x", ()), profile=None, key="id@secret") is repo
    assert repo.keys == ["id@secret"]


async def test_plan_restore_detects_the_firmware_from_the_first_disk(ex: ModuleType) -> None:
    root = _node("Disks", leaf=False)
    disk = _node("Disk 0", leaf=True, kind=UnitKind.DISK_IMAGE, size=20 * _GIB)
    provider = _Provider({"Disks": [disk]})
    gpt = _Content(_gpt(), 20 * _GIB)

    async def unit(node: Node) -> RestorableUnit:
        return RestorableUnit(ref=node.ref, name=node.name, is_leaf=True, kind=node.kind, size=node.size, content=gpt)

    provider.unit = unit  # type: ignore[method-assign]

    plan = await ex.plan_restore(_FakeRepo(provider, root), NodeRef("repo", ("s", "w", "v")), firmware="auto")

    assert plan.firmware == "uefi"
    assert [d.label for d in plan.disks] == ["Disk 0"]


async def test_plan_restore_does_not_read_the_disk_when_the_firmware_is_given(ex: ModuleType) -> None:
    provider, root = _vm_tree()
    plan = await ex.plan_restore(_FakeRepo(provider, root), NodeRef("repo", ("s", "w", "v")), firmware="bios")

    assert plan.firmware == "bios"
    assert plan.disks[0].unit.content.reads == []


def test_describe_plan_lists_the_disks_and_the_firmware(ex: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    plan = _plan(ex, 30 * _GIB, 512 * _MIB, firmware="uefi")

    ex.describe_plan(plan, requested_firmware="auto")
    ex.describe_plan(plan, requested_firmware="uefi")

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "disk 1/2: disk-0  30.0 GiB"
    assert lines[1] == "disk 2/2: disk-1  512.0 MiB"
    assert lines[2] == "firmware: UEFI (detected from the first disk)"
    assert lines[5] == "firmware: UEFI"


# -- export_to_lun / restore -----------------------------------------------------


@unchecked_fake("examples/restore_to_nutanix_ahv.py's LibiscsiWriter, loaded at test time")
class _LunWriter:
    """Stands in for ``LibiscsiWriter``: an in-memory LUN of 4 MiB."""

    instances: ClassVar[list[_LunWriter]] = []

    def __init__(self, url: str, initiator_iqn: str) -> None:
        self.url, self.initiator_iqn = url, initiator_iqn
        self.writes: list[tuple[int, bytes]] = []
        self.closed = 0
        _LunWriter.instances.append(self)

    def geometry(self) -> tuple[int, int]:
        return 512, 8192

    def write(self, offset: int, data: bytes) -> None:
        self.writes.append((offset, data))

    def close(self) -> None:
        self.closed += 1


@faithful_to(ContentSource)
class _ExportingContent:
    size = 2 * _MIB

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    async def planned_bytes(self, start: int, end: int) -> int:
        return 8192  # the one DATA block export_range writes

    async def export_range(
        self, sink: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: object = None
    ) -> ExportResult:
        await sink.write_at(0, b"A" * 8192)
        if progress is not None:
            await progress(8192)
        return ExportResult(bytes_written=8192, logical_size=self.size, holes=0, zeros=0)


async def test_export_to_lun_exports_through_a_block_sink_and_reports(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _LunWriter.instances.clear()
    monkeypatch.setattr(ex, "LibiscsiWriter", _LunWriter)
    unit = SimpleNamespace(content=_ExportingContent())
    disk = ex.SourceDisk("disk", unit, 2 * _MIB)

    await ex.export_to_lun(disk, "iscsi://h/iqn/1", 0, 2, 4 * _MIB, initiator_iqn="iqn.me")

    writers = [w for w in _LunWriter.instances if w.writes]
    assert [(w.url, w.initiator_iqn) for w in writers] == [("iscsi://h/iqn/1", "iqn.me")]
    assert writers[0].writes == [(0, b"A" * 8192)]
    assert all(w.closed == 1 for w in _LunWriter.instances)
    err = capsys.readouterr().err
    assert "disk 1/2" in err and "100%" in err
    assert re.search(r"disk 1/2: wrote 8\.0 KiB of 2\.0 MiB in \d+s", err)


async def test_export_to_lun_refuses_a_lun_that_does_not_report_its_volume_group_disks_size(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    _LunWriter.instances.clear()
    monkeypatch.setattr(ex, "LibiscsiWriter", _LunWriter)  # reports 4 MiB
    unit = SimpleNamespace(content=_ExportingContent())
    disk = ex.SourceDisk("disk", unit, 2 * _MIB)

    with pytest.raises(ValueError, match="not the LUN of that disk"):
        await ex.export_to_lun(disk, "iscsi://h/iqn/1", 0, 1, 2 * _MIB, initiator_iqn="iqn.me")

    assert all(not w.writes for w in _LunWriter.instances)  # nothing was written to the wrong LUN


class _SessionContext:
    """What ``Session()`` returns: an async context manager yielding the fake session."""

    def __init__(self, session: _FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> _FakeSession:
        return self._session

    async def __aexit__(self, *exc: object) -> None:
        return None


def _patch_session(ex: ModuleType, monkeypatch: pytest.MonkeyPatch, repo: _FakeRepo) -> _FakeSession:
    session = _FakeSession([repo])
    monkeypatch.setattr(ex, "Session", lambda: _SessionContext(session))
    return session


async def test_restore_dry_run_lists_the_plan_and_never_touches_nutanix(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    provider, root = _vm_tree()
    _patch_session(ex, monkeypatch, _FakeRepo(provider, root))

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("a dry run must not connect to Nutanix")

    monkeypatch.setattr(ex, "NutanixSdk", forbidden)

    await ex.restore(ex.parse_args(["/repo#src/wl/ver", "--dry-run"]), "")

    out = capsys.readouterr().out
    assert "disk 1/1: host-1/disk.img  30.0 GiB" in out
    assert "firmware: BIOS (detected from the first disk)" in out


async def test_restore_builds_the_target_and_runs_the_plan(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    provider, root = _vm_tree()
    _patch_session(ex, monkeypatch, _FakeRepo(provider, root))
    built: dict[str, Any] = {}

    def fake_sdk(host: str, user: str, password: str, *, verify_tls: bool) -> str:
        built["sdk"] = (host, user, password, verify_tls)
        return "sdk"

    def fake_target(sdk: Any) -> str:
        built["target"] = sdk
        return "target"

    async def fake_execute(plan: Any, settings: Any, target: Any, export_disk: Any) -> str:
        built["execute"] = (plan, settings, target, export_disk)
        return "vm-9"

    monkeypatch.setattr(ex, "NutanixSdk", fake_sdk)
    monkeypatch.setattr(ex, "AhvTarget", fake_target)
    monkeypatch.setattr(ex, "execute_restore", fake_execute)
    args = ex.parse_args(
        [
            "/repo#src/wl/ver",
            "--pc-host",
            "pc.example",
            "--pc-user",
            "admin",
            "--insecure",
            "--cluster",
            "cl",
            "--container",
            "ct",
            "--dsip",
            "10.0.0.9",
            "--vm-name",
            "web-01",
        ]
    )

    await ex.restore(args, "s3cret")

    assert built["sdk"] == ("pc.example", "admin", "s3cret", False)  # --insecure turns TLS verification off
    assert built["target"] == "sdk"
    plan, settings, target, export_disk = built["execute"]
    assert [d.label for d in plan.disks] == ["host-1/disk.img"] and plan.firmware == "bios"
    assert settings.vm_name == "web-01" and target == "target"
    assert export_disk.keywords == {"initiator_iqn": args.initiator_iqn}
    assert "restored 1 disk(s) into VM vm-9" in capsys.readouterr().out
