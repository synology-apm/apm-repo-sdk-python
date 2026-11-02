"""Tests for the restore flow of examples/hypervisor-restore/restore_to_nutanix_ahv.py: ``execute_restore`` over
a fake target, ``RestoreSettings``, ``VolumeGroupInfo``, ``export_to_lun`` and ``restore``."""

from __future__ import annotations

import re
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar

import pytest

from support.fakes import faithful_to, unchecked_fake
from synology_apm_repo.sdk.export import ExportResult, ExportTuning
from synology_apm_repo.sdk.units.base import ContentSource
from unit.examples.hypervisor_restore_fakes import GIB, MIB, FakeRepo, make_plan, patch_session, vm_tree

# The index each Volume Group disk reports, by position: neither sequential nor in order, so that anything
# using the position instead of the index would pick the wrong LUN.
_INDEXES = (7, 3, 5, 1)


@unchecked_fake("examples/hypervisor-restore/restore_to_nutanix_ahv.py's RestoreTarget, loaded at test time")
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

    vm_id = await _run(ex, make_plan(ex, 10 * MIB, 20 * MIB), target)

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
    assert re.fullmatch(r"apm-restore-web-01-[0-9a-f]{6}", name)
    assert (cluster, container, sizes) == ("cluster-1", "container-1", [10 * MIB, 20 * MIB])
    assert target.log[1] == ("allow_initiator", "vg-1", "iqn.me")
    assert target.log[-1] == ("delete_volume_group", "vg-1")


async def test_execute_restore_exports_each_disk_to_the_lun_numbered_by_its_volume_group_disk_index(
    ex: ModuleType,
) -> None:
    target = _FakeTarget(ex)
    export = _Exporter(target)

    await _run(ex, make_plan(ex, 10 * MIB, 20 * MIB, 30 * MIB), target, export=export)

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

    await _run(ex, make_plan(ex, 10 * MIB, 20 * MIB), target, export=export)

    assert [(position, total, capacity) for _, _, position, total, capacity in export.calls] == [
        (0, 2, 10 * MIB),
        (1, 2, 20 * MIB),
    ]


async def test_execute_restore_checks_the_lun_against_the_size_the_volume_group_disk_reports(
    ex: ModuleType,
) -> None:
    target = _FakeTarget(ex)
    target.size_adjust = 4096  # e.g. the cluster rounded the disk up
    export = _Exporter(target)

    await _run(ex, make_plan(ex, 10 * MIB, 20 * MIB), target, export=export)

    assert [capacity for *_, capacity in export.calls] == [10 * MIB + 4096, 20 * MIB + 4096]


@pytest.mark.parametrize(("firmware", "uefi"), [("uefi", True), ("bios", False)])
async def test_execute_restore_maps_the_firmware_and_converts_the_memory(
    ex: ModuleType, firmware: str, uefi: bool
) -> None:
    target = _FakeTarget(ex)

    await _run(ex, make_plan(ex, MIB, firmware=firmware), target, settings=_settings(ex, cpus=3, memory_gib=4))

    options = target.log[target.names().index("create_vm")][-1]
    assert options == {"cpus": 3, "memory_bytes": 4 * GIB, "uefi": uefi}


async def test_execute_restore_keeps_the_group_when_the_vm_still_depends_on_it(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    target = _FakeTarget(ex)
    target.detachable = False

    await _run(ex, make_plan(ex, MIB), target)

    assert "delete_volume_group" not in target.names()
    assert "kept Volume Group vg-1" in capsys.readouterr().out


async def test_execute_restore_keeps_the_group_when_asked_to(ex: ModuleType) -> None:
    target = _FakeTarget(ex)

    await _run(ex, make_plan(ex, MIB), target, settings=_settings(ex, keep_volume_group=True))

    assert "delete_volume_group" not in target.names()
    assert "delete_vm" not in target.names()


async def test_execute_restore_removes_the_group_when_an_export_fails(ex: ModuleType) -> None:
    target = _FakeTarget(ex)
    export = _Exporter(target)
    export.fail_on = 1

    with pytest.raises(OSError, match="iSCSI went away"):
        await _run(ex, make_plan(ex, MIB, MIB), target, export=export)

    assert target.names() == ["create_volume_group", "allow_initiator", "export", "export", "delete_volume_group"]


async def test_execute_restore_removes_the_group_when_the_lun_is_not_the_disks_own(ex: ModuleType) -> None:
    target = _FakeTarget(ex)

    async def wrong_lun(disk: Any, url: str, index: int, total: int, capacity: int) -> None:
        raise ValueError("the LUN reports 2097152 B but the Volume Group disk behind it is 1048576 B")

    with pytest.raises(ValueError, match="the LUN reports"):
        await _run(ex, make_plan(ex, MIB), target, export=wrong_lun)

    assert target.names() == ["create_volume_group", "allow_initiator", "delete_volume_group"]
    assert "create_vm" not in target.names()


@pytest.mark.parametrize("step", ["allow_initiator", "create_vm"])
async def test_execute_restore_removes_the_group_when_a_step_fails(ex: ModuleType, step: str) -> None:
    target = _FakeTarget(ex)
    target.fail[step] = RuntimeError(f"{step} failed")

    with pytest.raises(RuntimeError, match=f"{step} failed"):
        await _run(ex, make_plan(ex, MIB), target)

    assert target.names()[-1] == "delete_volume_group"
    assert "delete_vm" not in target.names()


async def test_execute_restore_removes_the_vm_before_the_group_when_hydration_fails(ex: ModuleType) -> None:
    target = _FakeTarget(ex)
    target.fail["wait_hydrated"] = RuntimeError("hydration failed")

    with pytest.raises(RuntimeError, match="hydration failed"):
        await _run(ex, make_plan(ex, MIB), target)

    assert target.names()[-2:] == ["delete_vm", "delete_volume_group"]


async def test_execute_restore_succeeds_with_a_warning_when_only_the_final_group_delete_fails(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    target = _FakeTarget(ex)
    target.fail["delete_volume_group"] = RuntimeError("cannot delete group")

    assert await _run(ex, make_plan(ex, MIB), target) == "vm-1"

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
        await _run(ex, make_plan(ex, MIB), target, export=export)

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


@unchecked_fake("examples/hypervisor-restore/restore_to_nutanix_ahv.py's LibiscsiWriter, loaded at test time")
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
    size = 2 * MIB

    def __init__(self) -> None:
        self.tuning: object = None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    async def planned_bytes(self, start: int, end: int) -> int:
        return 8192  # the one DATA block export_range writes

    async def export_range(
        self, sink: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: object = None
    ) -> ExportResult:
        self.tuning = tuning
        await sink.write_at(0, b"A" * 8192)
        if progress is not None:
            await progress(8192)
        return ExportResult(bytes_written=8192, logical_size=self.size, holes=0, zeros=0)


async def test_export_to_lun_exports_through_a_block_sink_and_reports(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _LunWriter.instances.clear()
    monkeypatch.setattr(ex, "LibiscsiWriter", _LunWriter)
    content = _ExportingContent()
    disk = ex.SourceDisk("disk", SimpleNamespace(content=content), 2 * MIB)

    await ex.export_to_lun(disk, "iscsi://h/iqn/1", 0, 2, 4 * MIB, initiator_iqn="iqn.me")

    assert content.tuning == ExportTuning(workers=4)  # one iSCSI session per worker

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
    disk = ex.SourceDisk("disk", unit, 2 * MIB)

    with pytest.raises(ValueError, match="not the LUN of that disk"):
        await ex.export_to_lun(disk, "iscsi://h/iqn/1", 0, 1, 2 * MIB, initiator_iqn="iqn.me")

    assert all(not w.writes for w in _LunWriter.instances)  # nothing was written to the wrong LUN


async def test_restore_dry_run_lists_the_plan_and_never_touches_nutanix(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    provider, root = vm_tree()
    patch_session(ex, monkeypatch, FakeRepo(provider, root))

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
    provider, root = vm_tree()
    patch_session(ex, monkeypatch, FakeRepo(provider, root))
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
