"""Tests for the repository side and the step plumbing of examples/hypervisor-restore/apm_restore_common.py:
Rollback, collect_disks, resolve_start, parse_ref, detect_firmware, open_repository, plan_restore,
describe_plan, wait_until and export_disk."""

from __future__ import annotations

import asyncio
import struct
import uuid
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk import Node, NodeRef, RestorableUnit, UnitKind
from synology_apm_repo.sdk.export import BufferedExportSink
from unit.examples.hypervisor_restore_fakes import (
    GIB,
    MIB,
    ExtentContent,
    FakeContent,
    FakeProvider,
    FakeRepo,
    FakeSession,
    make_plan,
    node,
    vm_tree,
)

_ESP = "c12a7328-f81f-11d2-ba4b-00a0c93ec93b"


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


async def test_collect_disks_walks_a_vm_and_skips_filesystem_containers_and_files(ex: ModuleType) -> None:
    provider, root = vm_tree()

    disks = await ex.collect_disks(provider, root)

    assert [(d.label, d.size) for d in disks] == [("host-1/disk.img", 30 * GIB)]
    assert "(filesystem)" not in provider.visited  # walking it would read the whole file tree
    assert isinstance(disks[0].unit, RestorableUnit)


async def test_collect_disks_lists_every_device_disk_in_order(ex: ModuleType) -> None:
    root = node("Devices", leaf=False)
    provider = FakeProvider(
        {
            "Devices": [node("host-a", leaf=False), node("host-b", leaf=False)],
            "host-a": [node("a0.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=1)],
            "host-b": [
                node("b0.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=2),
                node("b1.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=3),
            ],
        }
    )

    disks = await ex.collect_disks(provider, root)

    assert [d.label for d in disks] == ["host-a/a0.img", "host-b/b0.img", "host-b/b1.img"]


async def test_collect_disks_handles_a_pc_or_ps_version_whose_disks_hang_off_the_root(ex: ModuleType) -> None:
    root = node("Disks", leaf=False)
    provider = FakeProvider(
        {
            "Disks": [
                node("Disk 0", leaf=True, kind=UnitKind.DISK_IMAGE, size=100),
                node("Disk 0 (filesystem)", leaf=False, kind=UnitKind.DISK_FILESYSTEM),
                node("Disk 1", leaf=True, kind=UnitKind.DISK_IMAGE, size=200),
            ]
        }
    )

    disks = await ex.collect_disks(provider, root)

    assert [(d.label, d.size) for d in disks] == [("Disk 0", 100), ("Disk 1", 200)]
    assert "Disk 0 (filesystem)" not in provider.visited


async def test_collect_disks_accepts_a_ref_to_a_single_disk(ex: ModuleType) -> None:
    disk = node("only.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=7)

    disks = await ex.collect_disks(FakeProvider({}), disk)

    assert [(d.label, d.size) for d in disks] == [("only.img", 7)]


async def test_collect_disks_rejects_a_disk_of_unknown_size(ex: ModuleType) -> None:
    root = node("Disks", leaf=False)
    provider = FakeProvider({"Disks": [node("Disk 0", leaf=True, kind=UnitKind.DISK_IMAGE, size=None)]})

    with pytest.raises(SystemExit, match="'Disk 0' has no known size"):
        await ex.collect_disks(provider, root)


async def test_collect_disks_rejects_a_ref_without_disks(ex: ModuleType) -> None:
    root = node("Devices", leaf=False)
    provider = FakeProvider({"Devices": [node("mail", leaf=True, kind=UnitKind.MAIL, size=1)]})

    with pytest.raises(SystemExit, match="no restorable disk images"):
        await ex.collect_disks(provider, root)


async def test_resolve_start_follows_a_human_ref_to_itsnode(ex: ModuleType) -> None:
    provider, root = vm_tree()

    got_provider, got_node = await ex.resolve_start(FakeRepo(provider, root), NodeRef("repo", ("src", "wl", "ver")))

    assert (got_provider, got_node) == (provider, root)


@pytest.mark.parametrize("level", ["root", "catalog", "workload"])
async def test_resolve_start_needs_the_ref_to_reach_a_version(ex: ModuleType, level: str) -> None:
    with pytest.raises(SystemExit, match="must reach a backup version"):
        await ex.resolve_start(FakeRepo(level=level), NodeRef("repo", ("src",)))


async def test_resolve_start_resolves_a_canonical_ref_through_its_version(ex: ModuleType) -> None:
    provider, root = vm_tree()
    ref = NodeRef.parse("repo#@cat:1/wl:2/ver:abc")

    got_provider, got_node = await ex.resolve_start(FakeRepo(provider, root), ref)

    assert (got_provider, got_node) == (provider, root)


async def test_resolve_start_rejects_a_raw_ref(ex: ModuleType) -> None:
    with pytest.raises(SystemExit, match="raw refs are not supported"):
        await ex.resolve_start(FakeRepo(), NodeRef.parse("repo#@raw/x"))


def test_parse_ref_treats_a_bare_path_as_a_human_ref_with_no_segments(ex: ModuleType) -> None:
    assert ex.parse_ref("/backups/repo") == NodeRef.human("/backups/repo")
    assert ex.parse_ref("/backups/repo#src/wl/ver") == NodeRef("/backups/repo", ("src", "wl", "ver"))


def _gpt(
    *,
    entries_lba: int = 2,
    count: int = 128,
    entry_size: int = 128,
    esp: bool = True,
    length: int = MIB,
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


def _source(ex: ModuleType, data: bytes, size: int = 20 * GIB) -> Any:
    content = FakeContent(data, size)
    return ex.SourceDisk("disk", SimpleNamespace(content=content), size)


async def test_detect_firmware_picks_uefi_for_a_gpt_disk_with_an_esp(ex: ModuleType) -> None:
    assert await ex.detect_firmware(_source(ex, _gpt())) == "uefi"


async def test_detect_firmware_finds_the_gpt_of_a_4096_byte_sector_disk(ex: ModuleType) -> None:
    assert await ex.detect_firmware(_source(ex, _gpt(sector=4096))) == "uefi"
    assert await ex.detect_firmware(_source(ex, _gpt(sector=4096, esp=False))) == "bios"


async def test_detect_firmware_reads_a_4096_sector_table_beyond_the_first_mib_at_its_own_sector_size(
    ex: ModuleType,
) -> None:
    data = bytearray(_gpt(sector=4096, entries_lba=1024, esp=False, length=6 * MIB))
    data[1024 * 4096 : 1024 * 4096 + 16] = uuid.UUID(_ESP).bytes_le
    source = _source(ex, bytes(data))

    assert await ex.detect_firmware(source) == "uefi"
    assert source.unit.content.reads[-1] == (1024 * 4096, 128 * 128)


async def test_detect_firmware_picks_bios_for_a_gpt_disk_without_an_esp(ex: ModuleType) -> None:
    assert await ex.detect_firmware(_source(ex, _gpt(esp=False))) == "bios"


async def test_detect_firmware_picks_bios_for_an_mbr_disk(ex: ModuleType) -> None:
    mbr = bytearray(MIB)
    mbr[510:512] = b"\x55\xaa"

    assert await ex.detect_firmware(_source(ex, bytes(mbr))) == "bios"


async def test_detect_firmware_reads_a_partition_table_that_lies_beyond_the_first_mib(ex: ModuleType) -> None:
    data = bytearray(_gpt(entries_lba=4096, esp=False, length=3 * MIB))
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
    assert max(length for _, length in source.unit.content.reads) <= MIB  # nothing larger was requested


async def test_detect_firmware_distrusts_a_table_beyond_the_end_of_the_disk(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    assert await ex.detect_firmware(_source(ex, _gpt(), size=4096)) == "bios"

    assert "looks corrupt" in capsys.readouterr().err


async def test_open_repository_opens_a_local_path(ex: ModuleType) -> None:
    repo = FakeRepo()
    session = FakeSession([repo])

    got = await ex.open_repository(session, NodeRef("/backups/repo", ()), profile=None, key=None)

    assert got is repo
    assert session.calls == [("open", "/backups/repo")]


async def test_open_repository_goes_through_a_saved_profile_with_a_store_relative_root(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_store_from_profile(name: str) -> tuple[str, str]:
        return ("store", name)

    monkeypatch.setattr(ex, "store_from_profile", fake_store_from_profile)
    session = FakeSession([FakeRepo()])

    await ex.open_repository(session, NodeRef("sub/path", ()), profile="my-profile", key=None)

    assert session.calls == [("open_store", ("store", "my-profile"), "sub/path")]


@pytest.mark.parametrize("count", [0, 2])
async def test_open_repository_needs_exactly_one_repository(ex: ModuleType, count: int) -> None:
    session = FakeSession([FakeRepo() for _ in range(count)])

    with pytest.raises(SystemExit, match=rf"expected one repository .* found {count}"):
        await ex.open_repository(session, NodeRef("/x", ()), profile=None, key=None)


async def test_open_repository_needs_a_key_for_an_encrypted_repository(ex: ModuleType) -> None:
    repo = FakeRepo()
    repo.is_encrypted = True

    with pytest.raises(SystemExit, match="encrypted; pass --key"):
        await ex.open_repository(FakeSession([repo]), NodeRef("/x", ()), profile=None, key=None)


async def test_open_repository_rejects_a_wrong_key(ex: ModuleType) -> None:
    repo = FakeRepo()
    repo.is_encrypted = True
    repo.key_ok = False

    with pytest.raises(SystemExit, match="key was rejected"):
        await ex.open_repository(FakeSession([repo]), NodeRef("/x", ()), profile=None, key="id@secret")


async def test_open_repository_unlocks_with_the_key(ex: ModuleType) -> None:
    repo = FakeRepo()
    repo.is_encrypted = True

    assert await ex.open_repository(FakeSession([repo]), NodeRef("/x", ()), profile=None, key="id@secret") is repo
    assert repo.keys == ["id@secret"]


async def test_plan_restore_detects_the_firmware_from_the_first_disk(ex: ModuleType) -> None:
    root = node("Disks", leaf=False)
    disk = node("Disk 0", leaf=True, kind=UnitKind.DISK_IMAGE, size=20 * GIB)
    provider = FakeProvider({"Disks": [disk]})
    gpt = FakeContent(_gpt(), 20 * GIB)

    async def unit(node: Node) -> RestorableUnit:
        return RestorableUnit(ref=node.ref, name=node.name, is_leaf=True, kind=node.kind, size=node.size, content=gpt)

    provider.unit = unit  # type: ignore[method-assign]

    plan = await ex.plan_restore(FakeRepo(provider, root), NodeRef("repo", ("s", "w", "v")), firmware="auto")

    assert plan.firmware == "uefi"
    assert [d.label for d in plan.disks] == ["Disk 0"]


async def test_plan_restore_does_not_read_the_disk_when_the_firmware_is_given(ex: ModuleType) -> None:
    provider, root = vm_tree()
    plan = await ex.plan_restore(FakeRepo(provider, root), NodeRef("repo", ("s", "w", "v")), firmware="bios")

    assert plan.firmware == "bios"
    assert plan.disks[0].unit.content.reads == []


def test_describe_plan_lists_the_disks_and_the_firmware(ex: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    plan = make_plan(ex, 30 * GIB, 512 * MIB, firmware="uefi")

    ex.describe_plan(plan, requested_firmware="auto")
    ex.describe_plan(plan, requested_firmware="uefi")

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "disk 1/2: disk-0  30.0 GiB"
    assert lines[1] == "disk 2/2: disk-1  512.0 MiB"
    assert lines[2] == "firmware: UEFI (detected from the first disk)"
    assert lines[5] == "firmware: UEFI"


# -- wait_until ------------------------------------------------------------------


async def test_wait_until_returns_the_first_answer_that_is_not_none(ex: ModuleType) -> None:
    answers = iter([None, None, "ready"])

    async def probe() -> str | None:
        return next(answers)

    assert await ex.wait_until(probe, what="the lease", limit=10, interval=0) == "ready"


async def test_wait_until_gives_up_after_the_limit(ex: ModuleType) -> None:
    async def probe() -> None:
        return None

    with pytest.raises(ex.RestoreError, match="the lease: still not done after 0s"):
        await ex.wait_until(probe, what="the lease", limit=0.01, interval=0)


async def test_wait_until_lets_the_probes_error_through(ex: ModuleType) -> None:
    async def probe() -> None:
        raise ex.RestoreError("the import failed")

    with pytest.raises(ex.RestoreError, match="the import failed"):
        await ex.wait_until(probe, what="the lease", limit=None)


# -- export_disk -----------------------------------------------------------------


async def test_export_disk_exports_the_disk_sparse_and_prints_a_summary(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    content = ExtentContent(4 * MIB, {MIB: b"x" * MIB})
    disk = ex.SourceDisk("d", SimpleNamespace(content=content), content.size)
    seen: list[bool] = []

    @faithful_to(BufferedExportSink)
    class Sink(BufferedExportSink):
        def __init__(self) -> None:
            super().__init__(MIB)
            self.data = bytearray()

        async def create_destination(self, logical_size: int, *, sparse: bool) -> None:
            seen.append(sparse)

        async def flush_segment(self, segment: Any) -> None:
            self.data += await segment.read_all()

        async def finalize_destination(self) -> None:
            pass

        async def discard_destination(self) -> bool:
            return False

    sink = Sink()

    result = await ex.export_disk(disk, sink, "disk 1/1")

    assert seen == [True] and bytes(sink.data) == bytes(MIB) + b"x" * MIB + bytes(2 * MIB)
    assert result.logical_size == 4 * MIB
    assert capsys.readouterr().err.splitlines()[-1].startswith("disk 1/1: wrote 4.0 MiB of 4.0 MiB in ")
