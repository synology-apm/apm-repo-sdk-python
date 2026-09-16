"""Regression test for ``VirtualDiskContentSource``'s overlap/gap
resolution — replayed from a committed fixture recorded against a real
PS workload's fragment data, with **no external dependency**: this
always runs, on CI or anywhere else, because it goes through
``ReplayStore`` instead of a live S3 endpoint.

The fixture (``tests/fixtures/units_pcps_disk_minio_testuser.json.gz``)
is recorded against the ``c2-apm-sample-1`` profile (a live
Synology C2 cloud bucket) -- see ``tests/CLAUDE.md``'s "Recording a
fixture" section for the ``pytest --record-against=...``/``make
record-fixture`` workflow. It records every ``ObjectStore`` call the
same disk-resolution walk plus all five real-data reads/export below
make, and does **not** cover exporting the whole ~120 GiB disk.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
import pytest_asyncio

from synology_apm_repo.sdk.api import Session
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.content.pcps_disk import VirtualDiskContentSource

# Only matters when re-recording (replay never opens a real socket): the
# 6 tests below share one recording session's cached S3 client across
# pytest-asyncio's normally per-test event loop, and a client created
# under one test's loop can't be reused once that loop closes. Pinning
# every test in this file to one shared loop keeps the client's
# connection valid across all of them.
pytestmark = pytest.mark.asyncio(loop_scope="module")

_REPO_ROOT = "@ActiveProtectData/jkwXlvh40ECN"
_DISK_UUID = "E055BA86-3FC4-4850-B201-D6378C51BCB4"
_DISK_TOTAL = 120034123776

# The real gap this session found: fragment O(118995550208) ends at
# 120031539200 and the next fragment O(120031543296) starts exactly
# 4096 bytes later -- nothing registered in between at all (a genuine
# hole, not a ZERO declaration inside either fragment's own composition).
_TRUE_GAP_START = 120031539200
_TRUE_GAP_END = 120031543296

# sha256 of the real 4096 bytes fragment O(17408) contributes at [16384,
# 20480) -- a regression that filled the overlap with the wrong (but
# still non-zero) fragment's data would fail this, unlike a mere
# any(b != 0 for b in ...) check.
_OVERLAP_SHA256 = "7d0164942f37caf3c7e50c0942921d5b24bcf2cc627ade96059800bdba988242"

#: An internal catalog identifier -- stable and non-identifying (never
#: touched by catalog-metadata anonymization).
_PS_WORKLOAD_ID = 2


async def _open_testuser_disk_0(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> tuple[Session, VirtualDiskContentSource]:
    # allow_content=True: every test in this file reads real fragment
    # bytes as a structural oracle (zero-region/hash checks, never the
    # disk's own meaning) -- see the module docstring.
    store = await record_target("units_pcps_disk_minio_testuser.json.gz", allow_content=True)
    session = Session()
    content: VirtualDiskContentSource | None = None
    async for repo in session.discover_remote(store, root=_REPO_ROOT):
        # This real bucket root holds several catalogs (several copy
        # targets/generations sharing one repo) -- find the one that
        # actually has the known PS workload, rather than assuming
        # there's exactly one (true of this project's other real
        # fixtures' sources, not this one).
        catalog = None
        ps = None
        for candidate in await repo.catalogs():
            all_workloads = await candidate.workloads()
            hit = next((w for w in all_workloads if w.workload_id == _PS_WORKLOAD_ID), None)
            if hit is not None:
                catalog, ps = candidate, hit
                break
        assert catalog is not None and ps is not None
        [version] = (await catalog.versions(ps))[:1]
        provider = await catalog.provider(version)
        disks = await provider.children(provider.root())
        disk = next(d for d in disks if d.attrs.get("disk_uuid") == _DISK_UUID)
        unit = await provider.unit(disk)
        opened = unit.open()
        assert isinstance(opened, VirtualDiskContentSource)
        content = opened
        break
    assert content is not None
    return session, content


@pytest_asyncio.fixture(loop_scope="module")
async def disk_0(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> AsyncIterator[VirtualDiskContentSource]:
    """Opened fresh per test: every test below only ever calls
    ``content.read()``/inspects ``content.fragments`` -- no mutation --
    but ``record_target`` (unlike a bare ``ReplayStore.from_path``) is
    function-scoped, so this fixture is too rather than module-scoped.
    ``loop_scope="module"`` only matters when re-recording (see this
    module's ``pytestmark``) -- it doesn't change this fixture's own
    (function) scope, just which event loop its async work runs on."""
    session, content = await _open_testuser_disk_0(record_target)
    try:
        yield content
    finally:
        await session.close()


async def test_replayed_disk_0_size_matches_the_registered_whole_disk_capacity(
    disk_0: VirtualDiskContentSource,
) -> None:
    assert disk_0.size == _DISK_TOTAL


async def test_replayed_disk_0_starts_with_a_real_protective_mbr_and_gpt_header(
    disk_0: VirtualDiskContentSource,
) -> None:
    header = await disk_0.read(0, 520)
    assert header[510:512] == b"\x55\xaa"  # MBR boot signature
    assert header[512:520] == b"EFI PART"  # GPT header magic


async def test_replayed_region_between_the_mbr_and_the_overlap_reads_real_zero(
    disk_0: VirtualDiskContentSource,
) -> None:
    assert await disk_0.read(4096, 12288) == bytes(12288)


async def test_replayed_overlap_between_two_real_fragments_prefers_the_later_ones_real_data(
    disk_0: VirtualDiskContentSource,
) -> None:
    overlap = await disk_0.read(16384, 4096)
    assert hashlib.sha256(overlap).hexdigest() == _OVERLAP_SHA256


async def test_replayed_true_gap_between_two_fragments_reads_as_a_real_hole(
    disk_0: VirtualDiskContentSource,
) -> None:
    length = _TRUE_GAP_END - _TRUE_GAP_START
    assert await disk_0.read(_TRUE_GAP_START, length) == bytes(length)


async def test_replayed_export_to_mechanism_lands_the_later_fragments_data_in_the_overlap(
    tmp_path: Path, disk_0: VirtualDiskContentSource
) -> None:
    frag_early = next(f for f in disk_0.fragments if f.start == 0)
    frag_late = next(f for f in disk_0.fragments if f.start == 16384)
    span = 24576  # covers frag_early's own whole real extent, [0, 20480)
    dst = tmp_path / "overlap.bin"
    dst.write_bytes(bytes(span))

    early_view = frag_early.dedup_file.view(0, span)
    await early_view.export_to(dst, sparse=False, dst_offset=0, create=False)
    late_view = frag_late.dedup_file.view(16384, 4096)
    await late_view.export_to(dst, sparse=False, dst_offset=16384, create=False)

    result = dst.read_bytes()
    assert len(result) == span
    assert hashlib.sha256(result[16384:20480]).hexdigest() == _OVERLAP_SHA256


__all__: list[str] = []
