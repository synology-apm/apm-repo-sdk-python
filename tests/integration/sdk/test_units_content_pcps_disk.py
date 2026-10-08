"""Regression tests for ``VirtualDiskContentSource``'s overlap/gap
resolution on a real PS workload's fragmented disk.

Fixture: ``units_pcps_disk_overlapping_fragments.json.gz``, recorded against
the ``pcps`` sample; ``test_replayed_export_to_mechanism_lands_the_later_fragments_data_in_the_overlap``
is its recording recipe (a superset of every other test's calls).
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest_asyncio

from synology_apm_repo.sdk.api import Session
from synology_apm_repo.sdk.dedup.export_sink import OffsetWriter
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileSink
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.content.pcps_disk import VirtualDiskContentSource

_REPO_ROOT = "@ActiveProtectData/jkwXlvh40ECN"
_DISK_UUID = "E055BA86-3FC4-4850-B201-D6378C51BCB4"
_DISK_TOTAL = 120034123776

# A genuine hole: fragment O(118995550208) ends at 120031539200 and the next
# starts 4096 bytes later, with nothing registered in between (not a ZERO
# declaration inside either fragment's composition).
_TRUE_GAP_START = 120031539200
_TRUE_GAP_END = 120031543296

# sha256 of the 4096 bytes fragment O(17408) contributes at [16384, 20480);
# pins the overlap to that fragment's data, not merely to non-zero bytes.
_OVERLAP_SHA256 = "7d0164942f37caf3c7e50c0942921d5b24bcf2cc627ade96059800bdba988242"

_PS_WORKLOAD_ID = 2


async def _open_testuser_disk_0(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> tuple[Session, VirtualDiskContentSource]:
    # allow_content=True: every test here reads fragment bytes only as a
    # structural oracle (zero-region/hash checks).
    store = await record_target("units_pcps_disk_overlapping_fragments.json.gz", allow_content=True)
    session = Session()
    content: VirtualDiskContentSource | None = None
    async for repo in session.discover(store, root=_REPO_ROOT):
        # Several catalogs share this root; find the one holding the PS workload.
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
        disk = next(d for d in disks if d.details.get("disk_uuid") == _DISK_UUID)
        unit = await provider.unit(disk)
        opened = unit.content
        assert isinstance(opened, VirtualDiskContentSource)
        content = opened
        break
    assert content is not None
    return session, content


@pytest_asyncio.fixture
async def disk_0(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> AsyncIterator[VirtualDiskContentSource]:
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
    sink = LocalFileSink(dst, staged=False)
    await sink.open(span, sparse=False)

    early_view = frag_early.dedup_file.view(0, span)
    await early_view.export_range(OffsetWriter(sink, 0), 0, early_view.size, sparse=False)
    late_view = frag_late.dedup_file.view(16384, 4096)
    await late_view.export_range(OffsetWriter(sink, 16384), 0, late_view.size, sparse=False)
    await sink.commit()

    result = dst.read_bytes()
    assert len(result) == span
    assert hashlib.sha256(result[16384:20480]).hexdigest() == _OVERLAP_SHA256
