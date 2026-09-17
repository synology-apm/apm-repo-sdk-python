"""Unit tests for ``chunk_walk`` — the shared two-pass grouping/execution engine extracted from
``export_scheduler.py``. End-to-end correctness (progress reporting, cancellation, byte-for-byte output against a
naive reference implementation) is already covered through ``export_scheduler``'s own tests, since ``export_to()``
delegates to this module entirely — this file covers the packing helpers directly, plus a couple of
``plan_chunks_windowed``/``exec_chunks`` behaviors that are awkward to observe only through that public entry point.
Fixture-building helpers below intentionally duplicate (rather than import from)
``test_dedup_export_scheduler.py``'s own — matching this project's existing convention of each test module being
self-contained (no test module imports another; ``tests/`` isn't a package).
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import os
import sys
import zlib
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk import concurrency
from synology_apm_repo.sdk.dedup import chunk_walk
from synology_apm_repo.sdk.dedup.chunk_walk import (
    ChunkPlan,
    ChunkRun,
    ExportGroupWorkerArgs,
    _export_worker_init,
    _export_worker_shutdown,
    _flush_run,
    _GapDelta,
    _handle_gap_extent,
    _iter_chunk_runs,
    _merge_overlapping_ranges,
    _validate_window_start,
    _walk_extents,
    count_planned_bytes,
    exec_chunks,
    export_bucket_group_worker,
    plan_chunks_windowed,
)
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile, Extent, ExtentKind
from synology_apm_repo.sdk.dedup.pool import BucketReader, Pool
from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore

_O_BINARY = getattr(os, "O_BINARY", 0)

_STREAM_ID = StreamId(7)
_SESSION_ID = SessionId(3)
_HEAD_OFF = 64
_SIZE = 45056  # matches test_dedup_export_scheduler.py's own standard fixture

_CHUNK_PLAINTEXTS = [bytes([i]) * 4096 for i in range(5)]  # bucket 0, chunks 0..4


async def _reference_plan_chunks(
    base: DedupFile,
    start: int,
    end: int,
    window_start: int,
    *,
    write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
) -> ChunkPlan:
    """Unwindowed reference oracle every ``TestPlanChunksWindowed`` case
    checks its own windowed output against. Builds on the same
    ``_walk_extents``/``_validate_window_start`` the real
    ``plan_chunks_windowed`` builds on, so it still tracks real walking
    behavior rather than a second, independently-maintained copy of it."""
    _validate_window_start(window_start, start, fn_name="_reference_plan_chunks")
    groups: dict[tuple[StreamId, BucketId], list[ChunkRun]] = {}
    holes = zeros = 0
    async for event in _walk_extents(base, start, end, window_start, write_zero_fill):
        if isinstance(event, _GapDelta):
            holes += event.holes
            zeros += event.zeros
        else:
            groups.setdefault(event.key, []).append(event.run)
    return ChunkPlan(groups=groups, holes=holes, zeros=zeros)


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    idx_bytes = file_chunk_idx.to_bytes(7, "big")
    return bytes([type_byte]) + idx_bytes + addr_int.to_bytes(8, "big") + tail_u32.to_bytes(4, "big")


def _mapping_record(file_offset: int, bucket_id: int, chunk_idx: int, map_num: int, repeat: int = 0) -> bytes:
    addr_int = ChunkAddress(StreamId(0), BucketId(bucket_id), ChunkIdx(chunk_idx)).to_int()
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value,
        file_chunk_idx=file_offset >> 12,
        addr_int=addr_int,
        tail_u32=(map_num << 16) | repeat,
    )


def _zero_record(file_offset: int, zero_num: int) -> bytes:
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.ZERO.value, file_chunk_idx=file_offset >> 12, addr_int=0, tail_u32=zero_num
    )


def _composition_header_bytes() -> bytes:
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = SUB_FILE_SIZE.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _record_head_bytes(*, map_num: int, map_crc: int = 0, mode: int = 0x0001) -> bytes:
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = map_num.to_bytes(8, "big")
    head[14:18] = map_crc.to_bytes(4, "big")
    head[18:20] = mode.to_bytes(2, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(head)


def _write_standard_composition(comp_root: Path, entries: bytes) -> None:
    map_num = len(entries) // 20
    map_crc = zlib.crc32(entries) & 0xFFFFFFFF
    record_bytes = _record_head_bytes(map_num=map_num, map_crc=map_crc) + entries
    path = comp_root / str(_STREAM_ID) / f"{_SESSION_ID}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_composition_header_bytes() + record_bytes)


def _encode_size_store(entries: list[tuple[int, int]]) -> bytes:
    n = len(entries)
    tight_len = (n * 15 + 7) >> 3
    buf = bytearray(tight_len + 4)
    for idx, (type_value, size) in enumerate(entries):
        bit_off = idx * 15
        byte_off = bit_off >> 3
        bit_shift = 17 - (bit_off & 7)
        blob = (type_value << 12) | size
        window = int.from_bytes(buf[byte_off : byte_off + 4], "big")
        window |= (blob << bit_shift) & 0xFFFFFFFF
        buf[byte_off : byte_off + 4] = window.to_bytes(4, "big")
    return bytes(buf[:tight_len])


def _write_bucket(path: Path, plaintexts: list[bytes]) -> None:
    compressor = zstandard.ZstdCompressor()
    payloads = [compressor.compress(p) for p in plaintexts]
    entries = [(CompressType.ZSTD.value, len(payload)) for payload in payloads]
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = (MODE_COMPRESS | MODE_CHUNK_CRC).to_bytes(4, "big")
    header[12:16] = len(plaintexts).to_bytes(4, "big")
    header[16:20] = chunk_size_crc.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    trailer_bytes = 4 * len(plaintexts) + redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256)
    trailer = bytes(trailer_bytes)  # deterministic, not os.urandom() — this module never reads the trailer
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(payloads) + trailer)


def _standard_entries() -> bytes:
    # Same layout as test_dedup_export_scheduler.py's standard fixture:
    # [0,12288)=DATA(3), [12288,20480)=ZERO(2), [20480,24576)=HOLE,
    # [24576,40960)=DATA(4, templated via repeat), [40960,45056)=trailing HOLE.
    return (
        _mapping_record(0, 0, 0, map_num=3)
        + _zero_record(12288, zero_num=2)
        + _mapping_record(24576, 0, 3, map_num=2, repeat=1)
    )


@pytest.fixture
def dedup_file(tmp_path: Path) -> DedupFile:
    _write_standard_composition(tmp_path / "Composition", _standard_entries())
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=_SIZE)


async def _noop_on_run(off: int, data: bytes | memoryview) -> None:
    """``on_run`` must be ``Callable[[int, bytes | memoryview], Awaitable[None]]``
    — a plain ``lambda off, data: None`` would hand ``exec_chunks()`` a
    non-awaitable and blow up at the ``await`` site."""


def _recording_zero_fill(calls: list[tuple[int, int]]) -> Callable[[int, int], Awaitable[None]]:
    """``write_zero_fill`` is awaitable, so a plain
    ``lambda off, length: calls.append(...)`` can't be handed to it —
    this wraps the recorder in a coroutine function instead."""

    async def _write_zero_fill(off: int, length: int) -> None:
        calls.append((off, length))

    return _write_zero_fill


class _BlockingStore:
    """An ``ObjectStore`` wrapper that can be *armed* to park forever
    inside its next ``read()``.

    Cancellation tests use this to park the callee at a real ``await``
    inside the SDK, then cancel the surrounding ``asyncio.Task`` —
    there is no ``cancel=`` parameter on any signature; cancellation is
    always via the Task."""

    def __init__(self, backing: LocalFsStore) -> None:
        self._backing = backing
        self.armed = False
        self.blocked = asyncio.Event()  # set once a read has actually parked
        self._never_released = asyncio.Event()  # deliberately never set

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if self.armed:
            self.blocked.set()
            await self._never_released.wait()
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._backing.listdir(path)


def _build_blocking_dedup_file(tmp_path: Path) -> tuple[DedupFile, _BlockingStore]:
    """The standard fixture, but over a ``_BlockingStore``."""
    _write_standard_composition(tmp_path / "Composition", _standard_entries())
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
    store = _BlockingStore(LocalFsStore(tmp_path))
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=_SIZE), store


_CHUNKS_PER_BUCKET = 2


def _multi_bucket_plaintexts(bucket_id: int) -> list[bytes]:
    # Distinct byte value per (bucket, chunk) pair so a byte-for-byte
    # comparison can catch a chunk landing at the wrong bucket/offset,
    # not just "some bucket's bytes, somewhere".
    return [bytes([(bucket_id * 10 + i) % 256]) * 4096 for i in range(_CHUNKS_PER_BUCKET)]


def _build_multi_bucket_dedup_file(tmp_path: Path, num_buckets: int) -> DedupFile:
    """``num_buckets`` independent buckets, each contributing
    ``_CHUNKS_PER_BUCKET`` contiguous chunks to the file — real
    cross-*bucket* parallelization has nothing to parallelize
    over with this module's own single-bucket ``dedup_file`` fixture
    above, so the parallel-specific tests need their own multi-bucket
    one."""
    bucket_span = _CHUNKS_PER_BUCKET * 4096
    entries = b"".join(_mapping_record(b * bucket_span, b, 0, map_num=_CHUNKS_PER_BUCKET) for b in range(num_buckets))
    _write_standard_composition(tmp_path / "Composition", entries)
    for b in range(num_buckets):
        _write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", _multi_bucket_plaintexts(b))
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=num_buckets * bucket_span)


def _build_blocking_multi_bucket_dedup_file(tmp_path: Path, num_buckets: int) -> tuple[DedupFile, _BlockingStore]:
    """``_build_multi_bucket_dedup_file`` over a ``_BlockingStore``, for
    the multi-bucket cancellation test."""
    bucket_span = _CHUNKS_PER_BUCKET * 4096
    entries = b"".join(_mapping_record(b * bucket_span, b, 0, map_num=_CHUNKS_PER_BUCKET) for b in range(num_buckets))
    _write_standard_composition(tmp_path / "Composition", entries)
    for b in range(num_buckets):
        _write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", _multi_bucket_plaintexts(b))
    store = _BlockingStore(LocalFsStore(tmp_path))
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=num_buckets * bucket_span), store


def _expand_placements(groups: dict[tuple[StreamId, BucketId], list[ChunkRun]]) -> list[tuple[int, int]]:
    """Every group's ``ChunkRun``s expanded back to individual
    ``(chunk_idx, dest_offset)`` placements, sorted — the same shape the
    old flat packed-array tests asserted against, so a run-based plan and
    the fully-expanded reference it should be equivalent to can be
    compared directly regardless of how the placements happen to be
    grouped into runs."""
    return sorted(
        (run.chunk_idx_start + i, run.dest_offset_start + i * 4096)
        for runs in groups.values()
        for run in runs
        for i in range(run.length)
    )


class TestIterChunkRuns:
    """``_iter_chunk_runs`` is where the run-length arithmetic this
    module's own docstring describes actually lives — covered directly
    here since ``TestPlanChunks``/``TestPlanChunksWindowed``
    only observe its output already merged into a ``ChunkPlan``."""

    def test_a_plain_run_with_no_repeat_or_carry_is_one_run(self) -> None:
        addr = ChunkAddress(StreamId(0), BucketId(0), ChunkIdx(0))
        runs = list(_iter_chunk_runs(addr, map_num=5, first_k=0, last_k=4))
        assert len(runs) == 1
        start_addr, run_len, k_start = runs[0]
        assert (start_addr.bucket_id, start_addr.chunk_idx, run_len, k_start) == (0, 0, 5, 0)

    def test_a_sub_window_within_one_cycle_is_still_one_run(self) -> None:
        """Mirrors ``test_data_chunks_entirely_before_start_are_skipped_not_negative``'s
        fixture: chunks 1..2 of a map_num=3 record starting at chunk 0."""
        addr = ChunkAddress(StreamId(0), BucketId(0), ChunkIdx(0))
        runs = list(_iter_chunk_runs(addr, map_num=3, first_k=1, last_k=2))
        assert len(runs) == 1
        start_addr, run_len, k_start = runs[0]
        assert (start_addr.chunk_idx, run_len, k_start) == (1, 2, 1)

    def test_repeat_wraparound_splits_into_one_run_per_cycle(self) -> None:
        """``map_num=2, repeat=1`` (k=0..3): the on-disk record's own
        semantics jump the Pool address back to the template start every
        2 steps — chunks [3,4,3,4], never [3,4,5,6] — so this must split
        into two runs, not one contiguous run of length 4."""
        addr = ChunkAddress(StreamId(0), BucketId(0), ChunkIdx(3))
        runs = list(_iter_chunk_runs(addr, map_num=2, first_k=0, last_k=3))
        assert [(r.chunk_idx, length, k) for r, length, k in runs] == [(3, 2, 0), (3, 2, 2)]

    def test_bucket_carry_splits_a_run_that_crosses_the_bucket_boundary(self) -> None:
        """A run that would cross ``BUCKET_MAX_CHUNK_NUM`` (8192) splits
        at the carry, matching ``ChunkAddress.advance``'s own carry
        semantics — mirrors
        ``test_read_spanning_two_buckets_via_a_single_extents_carry``'s
        fixture in ``test_dedup_dedup_file.py``. Starts one chunk before
        the real boundary (not a monkeypatched one: ``advance()`` carries
        via its own imported ``BUCKET_MAX_CHUNK_NUM``, in
        ``format/addressing.py``, not this module's copy, so patching only
        this module's name would desync the two and silently break the
        loop's own termination instead of testing anything real)."""
        addr = ChunkAddress(StreamId(0), BucketId(0), ChunkIdx(8191))
        runs = list(_iter_chunk_runs(addr, map_num=2, first_k=0, last_k=1))
        assert len(runs) == 2
        (first_addr, first_len, first_k), (second_addr, second_len, second_k) = runs
        assert (first_addr.bucket_id, first_addr.chunk_idx, first_len, first_k) == (0, 8191, 1, 0)
        assert (second_addr.bucket_id, second_addr.chunk_idx, second_len, second_k) == (1, 0, 1, 1)


class TestMergeOverlappingRanges:
    """``_merge_overlapping_ranges`` (its own docstring has the full
    story): two *different* ``ChunkRun``s for one bucket can reference
    genuinely overlapping-but-not-identical physical ranges, which plain
    tuple-equality dedup never catches."""

    def test_disjoint_ranges_pass_through_unchanged_sorted_by_start(self) -> None:
        assert _merge_overlapping_ranges([(100, 10), (0, 10)]) == [(0, 10), (100, 10)]

    def test_identical_ranges_collapse_to_one_the_repeat_case(self) -> None:
        assert _merge_overlapping_ranges([(50, 20), (50, 20), (50, 20)]) == [(50, 20)]

    def test_touching_ranges_merge_gap_of_exactly_zero(self) -> None:
        """[10,20) and [20,30) share no chunk_idx but touch exactly —
        merging them (rather than leaving a needless boundary) matches
        _fits_in_run()'s own ``gap == 0`` acceptance."""
        assert _merge_overlapping_ranges([(10, 10), (20, 10)]) == [(10, 20)]

    def test_a_range_fully_nested_inside_another_merges_to_the_outer_ones_span(self) -> None:
        """The real shape found in production: [417,468) is a strict
        subset of [417,1485) — merging must keep the *larger* span, not
        whichever range happened to sort first."""
        assert _merge_overlapping_ranges([(417, 1068), (417, 51)]) == [(417, 1068)]

    def test_a_range_partially_overlapping_a_later_one_merges_to_their_union(self) -> None:
        """[0,417) and [119,172) overlap without either containing the
        other in full — union is [0,417) since 172 < 417, but a
        differently-shaped overlap (e.g. [0,417) and [300,600)) must
        union to the *wider* span, not just keep the first one."""
        assert _merge_overlapping_ranges([(0, 417), (119, 53)]) == [(0, 417)]
        assert _merge_overlapping_ranges([(0, 417), (300, 300)]) == [(0, 600)]

    def test_the_real_production_bucket_11716_shape_collapses_correctly(self) -> None:
        """The exact (chunk_idx_start, length) set measured on a real PS
        sample's bucket 11716 — 15 ChunkRuns from overlapping destination
        extents that a plain tuple-dedup left as 15 separate ranges (10 of
        which caused a spurious extra read each); interval-merging must
        collapse it to its true disjoint footprint."""
        ranges = [
            (0, 417),
            (417, 1068),
            (417, 51),
            (1485, 521),
            (468, 105),
            (2006, 5511),
            (7428, 157),
            (7517, 675),
            (6928, 500),
            (5765, 102),
            (6110, 78),
            (3920, 75),
            (4038, 56),
            (119, 53),
            (5976, 52),
        ]
        merged = _merge_overlapping_ranges(ranges)
        # Every range chains transitively into one continuous span here:
        # [0,417) touches [417,1485) touches [1485,2006) touches
        # [2006,7517) (which already contains every range from 3920 to
        # 6928), which touches [7428,7585) which is overtaken by
        # [7517,8192) -- the whole bucket's touched footprint in this
        # window turns out to be one gap-free run once ordered correctly.
        assert merged == [(0, 8192)]
        # The invariants below are the ones that actually matter -- an
        # exact expected list is easy to get subtly wrong by hand for a
        # 15-range fixture, but these two must hold regardless:
        total_input_chunks = sum(length for _start, length in ranges)
        total_merged_chunks = sum(length for _start, length in merged)
        assert total_merged_chunks < total_input_chunks  # real overlap was actually removed
        for (s1, l1), (s2, _l2) in zip(merged, merged[1:], strict=False):
            assert s1 + l1 < s2  # strictly increasing, non-touching, non-overlapping between output ranges
        assert merged == sorted(merged)


class TestHandleGapExtent:
    async def test_extent_entirely_outside_the_window_clips_to_an_empty_span(self) -> None:
        extent = Extent(offset=0, length=100, kind=ExtentKind.HOLE)
        result = await _handle_gap_extent(extent, start=200, end=300, window_start=0, write_zero_fill=None)
        assert result == (0, 0)


class TestFlushRun:
    async def test_empty_run_buf_is_a_no_op(self) -> None:
        async def on_run(offset: int, data: bytes | memoryview) -> None:
            raise AssertionError("must not be called for an empty run")

        written = await _flush_run(on_run, run_start=0, run_buf=bytearray(), size=100)
        assert written == 0

    async def test_run_start_at_or_past_size_writes_nothing(self) -> None:
        async def on_run(offset: int, data: bytes | memoryview) -> None:
            raise AssertionError("must not be called once run_start already reached size")

        written = await _flush_run(on_run, run_start=100, run_buf=bytearray(b"data"), size=100)
        assert written == 0


class TestPlanChunks:
    async def test_write_zero_fill_none_only_counts_holes_and_zeros(self, dedup_file: DedupFile) -> None:
        plan = await _reference_plan_chunks(dedup_file, 0, _SIZE, 0, write_zero_fill=None)
        # Standard fixture: [12288,20480) ZERO, [20480,24576) HOLE, trailing [40960,45056) HOLE.
        assert plan.zeros == 8192
        assert plan.holes == 4096 + 4096

    async def test_write_zero_fill_callback_is_invoked_for_holes_and_zeros(self, dedup_file: DedupFile) -> None:
        calls: list[tuple[int, int]] = []
        await _reference_plan_chunks(dedup_file, 0, _SIZE, 0, write_zero_fill=_recording_zero_fill(calls))
        assert (12288, 8192) in calls  # the ZERO extent
        assert (20480, 4096) in calls  # the HOLE between ZERO and the second DATA extent
        assert (40960, 4096) in calls  # the trailing HOLE

    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Parks the plan walk at the real composition read it issues
        (see ``_BlockingStore``), then cancels the surrounding Task,
        which must propagate ``asyncio.CancelledError``."""
        file, store = _build_blocking_dedup_file(tmp_path)

        async def do_plan() -> ChunkPlan:
            store.armed = True
            return await _reference_plan_chunks(file, 0, _SIZE, 0, write_zero_fill=None)

        task = asyncio.create_task(do_plan())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_zero_extent_is_clipped_to_a_requested_end_inside_it(self, dedup_file: DedupFile) -> None:
        """The fixture's ZERO extent spans [12288, 20480) — requesting
        only up to 12290 must report/write just the 2 bytes
        actually inside [0, 12290), not the extent's own full,
        un-clipped 8192-byte span."""
        end = 12290
        calls: list[tuple[int, int]] = []
        plan = await _reference_plan_chunks(dedup_file, 0, end, 0, write_zero_fill=_recording_zero_fill(calls))
        assert plan.zeros == 2
        assert calls == [(12288, 2)]

    async def test_hole_extent_is_clipped_to_a_requested_end_inside_it(self, dedup_file: DedupFile) -> None:
        """Same fix, the HOLE case: the fixture's [20480, 24576) HOLE
        clipped to end=20482 must report/write only 2 bytes."""
        end = 20482
        calls: list[tuple[int, int]] = []
        plan = await _reference_plan_chunks(dedup_file, 0, end, 0, write_zero_fill=_recording_zero_fill(calls))
        assert plan.holes == 2
        assert (20480, 2) in calls
        assert (20480, 4096) not in calls

    async def test_data_chunks_entirely_before_start_are_skipped_not_negative(self, dedup_file: DedupFile) -> None:
        """Chunks entirely before ``start`` are excluded, not included
        with a negative ``dest_offset``: for a start/window_start that
        falls in the middle of the fixture's first DATA record
        ([0, 12288), 3 chunks), chunk 0 must not appear at all, and
        chunks 1 and 2 (the ones actually inside the window) must have
        correct, non-negative, window-relative dest_offsets."""
        plan = await _reference_plan_chunks(dedup_file, 4096, 12288, 4096, write_zero_fill=None)
        assert _expand_placements(plan.groups) == [(1, 0), (2, 4096)]

    async def test_rejects_a_non_chunk_aligned_window_start(self, dedup_file: DedupFile) -> None:
        """A non-aligned window_start would silently misalign every
        dest_offset off its true 4096-byte chunk boundary instead of
        raising — rejected outright instead, since silent corruption is a
        strictly worse failure mode than a clear error for a case this
        module doesn't attempt to actually support."""
        with pytest.raises(ValueError, match="chunk-aligned window_start"):
            await _reference_plan_chunks(dedup_file, 100, _SIZE, 100, write_zero_fill=None)

    async def test_rejects_a_window_start_greater_than_start(self, dedup_file: DedupFile) -> None:
        """``window_start`` must not exceed ``start``: an included
        chunk's own absolute position falling before ``window_start``
        would otherwise produce a negative packed dest_offset, so this
        is rejected outright."""
        with pytest.raises(ValueError, match=r"window_start \(4096\) <= start \(0\)"):
            await _reference_plan_chunks(dedup_file, 0, 12288, 4096, write_zero_fill=None)

    async def test_window_start_less_than_start_positions_chunks_at_their_absolute_offset(
        self, dedup_file: DedupFile
    ) -> None:
        """A window_start < start is legitimate (already
        implicitly relied on by TestPlanChunksWindowed's own
        all-HOLE-range test below, though that one never exercises a
        real DATA extent) — dest_offset must stay anchored to
        window_start, not shift to make ``start`` the new zero point."""
        plan = await _reference_plan_chunks(dedup_file, 4096, 12288, 0, write_zero_fill=None)
        assert _expand_placements(plan.groups) == [(1, 4096), (2, 8192)]


def _merge_windows(windows: list[ChunkPlan]) -> tuple[dict[tuple[StreamId, BucketId], list[ChunkRun]], int, int]:
    """Combine every yielded window's groups/holes/zeros into the same
    shape ``_reference_plan_chunks``'s own single ``ChunkPlan`` has, so
    the two can be compared directly regardless of how many windows the
    placements were split across."""
    merged: dict[tuple[StreamId, BucketId], list[ChunkRun]] = {}
    holes = zeros = 0
    for plan in windows:
        holes += plan.holes
        zeros += plan.zeros
        for key, runs in plan.groups.items():
            merged.setdefault(key, []).extend(runs)
    return merged, holes, zeros


class TestPlanChunksWindowed:
    """The sliding-window memory bound — verified against
    ``_reference_plan_chunks``'s own single-``ChunkPlan`` result as the
    reference (matching this project's own established pattern of
    keeping an unwindowed/non-optimized path around as an oracle for a
    windowed/optimized one, e.g. logical vs. physical export order)."""

    @pytest.mark.parametrize("max_entries", [1, 2, 3, 4, 5, 6, 7, 8, 100])
    async def test_matches_the_unwindowed_plan_for_various_window_sizes(
        self, dedup_file: DedupFile, max_entries: int
    ) -> None:
        reference = await _reference_plan_chunks(dedup_file, 0, _SIZE, 0, write_zero_fill=None)
        windows = [
            w
            async for w in plan_chunks_windowed(dedup_file, 0, _SIZE, 0, write_zero_fill=None, max_entries=max_entries)
        ]
        merged_groups, holes, zeros = _merge_windows(windows)

        assert holes == reference.holes
        assert zeros == reference.zeros
        # Compared at the individual-chunk level, not as raw ChunkRun
        # lists: a window boundary can split one contiguous run into two
        # (or more) smaller ones, so the windowed and unwindowed plans can
        # legitimately disagree on run *shape* while still describing the
        # exact same set of (chunk_idx, dest_offset) placements.
        assert _expand_placements(merged_groups) == _expand_placements(reference.groups)

    @pytest.mark.parametrize("max_entries", [1, 2, 3])
    async def test_no_window_exceeds_max_entries(self, dedup_file: DedupFile, max_entries: int) -> None:
        windows = [
            w
            async for w in plan_chunks_windowed(dedup_file, 0, _SIZE, 0, write_zero_fill=None, max_entries=max_entries)
        ]
        for plan in windows:
            total = sum(run.length for arr in plan.groups.values() for run in arr)
            assert total <= max_entries

    async def test_small_window_yields_more_than_one_plan(self, dedup_file: DedupFile) -> None:
        # The standard fixture has 7 DATA chunk placements total (3 + 4,
        # see _standard_entries()'s own comment) -- max_entries=2 must
        # split them across multiple windows, not silently plan
        # everything into one anyway.
        windows = [w async for w in plan_chunks_windowed(dedup_file, 0, _SIZE, 0, write_zero_fill=None, max_entries=2)]
        assert len(windows) > 1

    async def test_large_window_yields_exactly_one_plan(self, dedup_file: DedupFile) -> None:
        windows = [
            w async for w in plan_chunks_windowed(dedup_file, 0, _SIZE, 0, write_zero_fill=None, max_entries=1_000_000)
        ]
        assert len(windows) == 1

    async def test_always_yields_at_least_one_plan_even_for_an_all_hole_range(self, dedup_file: DedupFile) -> None:
        # [20480, 24576) is the fixture's own HOLE-only extent (between
        # the ZERO region and the second DATA extent) -- no DATA chunks
        # at all in this sub-range, but the "at least one plan" contract
        # must still hold, matching _reference_plan_chunks()'s own behavior of
        # never returning nothing.
        windows = [
            w async for w in plan_chunks_windowed(dedup_file, 20480, 24576, 0, write_zero_fill=None, max_entries=1)
        ]
        assert len(windows) == 1
        assert windows[0].holes == 4096
        assert windows[0].groups == {}

    async def test_write_zero_fill_is_invoked_exactly_like_the_unwindowed_version(self, dedup_file: DedupFile) -> None:
        calls: list[tuple[int, int]] = []
        [
            w
            async for w in plan_chunks_windowed(
                dedup_file,
                0,
                _SIZE,
                0,
                write_zero_fill=_recording_zero_fill(calls),
                max_entries=2,
            )
        ]
        assert (12288, 8192) in calls
        assert (20480, 4096) in calls
        assert (40960, 4096) in calls

    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Same rewrite as ``TestPlanChunks``'s own cancellation test
        — cancelling the Task consuming this async generator must raise
        ``asyncio.CancelledError`` out of the ``async for``."""
        file, store = _build_blocking_dedup_file(tmp_path)

        async def consume() -> list[ChunkPlan]:
            store.armed = True
            return [w async for w in plan_chunks_windowed(file, 0, _SIZE, 0, write_zero_fill=None, max_entries=1)]

        task = asyncio.create_task(consume())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_data_chunks_entirely_before_start_are_skipped(self, dedup_file: DedupFile) -> None:
        """Chunks before start/window_start are excluded here too —
        chunk 0 of the fixture's first DATA record must not appear when
        start=4096 skips it."""
        windows = [w async for w in plan_chunks_windowed(dedup_file, 4096, 12288, 4096, write_zero_fill=None)]
        merged_groups, _holes, _zeros = _merge_windows(windows)
        assert _expand_placements(merged_groups) == [(1, 0), (2, 4096)]

    async def test_rejects_a_non_chunk_aligned_window_start(self, dedup_file: DedupFile) -> None:
        with pytest.raises(ValueError, match="chunk-aligned window_start"):
            [w async for w in plan_chunks_windowed(dedup_file, 100, _SIZE, 100, write_zero_fill=None)]

    async def test_rejects_a_window_start_greater_than_start(self, dedup_file: DedupFile) -> None:
        """Same window_start <= start precondition as _reference_plan_chunks()."""
        with pytest.raises(ValueError, match=r"window_start \(4096\) <= start \(0\)"):
            [w async for w in plan_chunks_windowed(dedup_file, 0, 12288, 4096, write_zero_fill=None)]

    async def test_window_start_less_than_start_positions_chunks_at_their_absolute_offset(
        self, dedup_file: DedupFile
    ) -> None:
        """Same legitimate window_start < start case as
        _reference_plan_chunks()'s own equivalent test."""
        windows = [w async for w in plan_chunks_windowed(dedup_file, 4096, 12288, 0, write_zero_fill=None)]
        merged_groups, _holes, _zeros = _merge_windows(windows)
        assert _expand_placements(merged_groups) == [(1, 4096), (2, 8192)]


class TestCountPlannedBytes:
    async def test_matches_the_unwindowed_plans_own_total(self, dedup_file: DedupFile) -> None:
        reference = await _reference_plan_chunks(dedup_file, 0, _SIZE, 0, write_zero_fill=None)
        expected = sum(run.length for arr in reference.groups.values() for run in arr) * 4096
        assert await count_planned_bytes(dedup_file, 0, _SIZE) == expected

    async def test_matches_the_unwindowed_plans_own_total_for_a_leading_boundary_window(
        self, dedup_file: DedupFile
    ) -> None:
        """``count_planned_bytes()`` must also exclude chunks before
        start/window_start — checked against _reference_plan_chunks()'s own count
        for the exact same non-trivial window, not just the full-file
        case above (which is 4096-aligned at both ends and wouldn't
        exercise this)."""
        reference = await _reference_plan_chunks(dedup_file, 4096, 12288, 4096, write_zero_fill=None)
        expected = sum(run.length for arr in reference.groups.values() for run in arr) * 4096
        assert await count_planned_bytes(dedup_file, 4096, 12288) == expected
        assert expected == 2 * 4096  # chunks 1, 2 only -- chunk 0 excluded

    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Same rewrite as ``TestPlanChunks``'s own cancellation
        test — this second extents() walk is cancellable the same way."""
        file, store = _build_blocking_dedup_file(tmp_path)

        async def do_count() -> int:
            store.armed = True
            return await count_planned_bytes(file, 0, _SIZE)

        task = asyncio.create_task(do_count())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class TestExecChunks:
    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """The plan is built first (store unarmed), then the store is
        armed so ``exec_chunks()`` parks on its first bucket read;
        cancelling the surrounding Task must propagate
        ``asyncio.CancelledError`` with ``on_run`` never having been
        called, i.e. nothing was written."""
        file, store = _build_blocking_dedup_file(tmp_path)
        plan = await _reference_plan_chunks(file, 0, _SIZE, 0, write_zero_fill=None)
        runs: list[tuple[int, int]] = []

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            runs.append((off, len(data)))

        async def do_exec() -> int:
            store.armed = True
            return await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=_SIZE)

        task = asyncio.create_task(do_exec())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert runs == []

    async def test_on_run_calls_reconstruct_the_correct_data_bytes(self, dedup_file: DedupFile) -> None:
        """Doesn't assert a specific run count (repeat-run dest offsets
        don't stay contiguous once sorted primarily by chunk_idx — see
        ``exec_chunks``'s own docstring on why merging is a best-effort
        optimization, not a guarantee); instead reconstructs the full
        output buffer from whatever runs ``on_run`` was actually called
        with and checks the DATA bytes land at the right offsets — the
        actual observable contract, and the same one
        ``test_dedup_export_scheduler``'s byte-for-byte comparison
        against logical order already exercises end-to-end."""
        plan = await _reference_plan_chunks(dedup_file, 0, _SIZE, 0, write_zero_fill=None)
        out = bytearray(_SIZE)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            out[off : off + len(data)] = data

        bytes_run = await exec_chunks(plan, pool=dedup_file._pool, on_run=_on_run, size=_SIZE)
        assert bytes_run == 3 * 4096 + 4 * 4096
        assert bytes(out[0:12288]) == b"".join(_CHUNK_PLAINTEXTS[0:3])
        assert bytes(out[24576:28672]) == _CHUNK_PLAINTEXTS[3]
        assert bytes(out[28672:32768]) == _CHUNK_PLAINTEXTS[4]
        assert bytes(out[32768:36864]) == _CHUNK_PLAINTEXTS[3]  # repeat
        assert bytes(out[36864:40960]) == _CHUNK_PLAINTEXTS[4]  # repeat

    async def test_overlapping_chunk_runs_for_one_bucket_produce_correct_bytes_with_no_redundant_read(
        self, tmp_path: Path
    ) -> None:
        """The real shape this session found in production
        (``_merge_overlapping_ranges``'s own docstring): two different
        destination extents reference overlapping-but-not-identical
        physical chunk ranges within the same bucket — record 2's chunks
        3-7 are a strict subset of record 1's chunks 0-7. Must produce
        correct bytes at both destination locations *and* only one real
        ``BucketReader._read_run`` call — two would mean the old
        tuple-equality-only dedup's spurious extra (partially redundant)
        read regressed."""
        plaintexts = [bytes([i]) * 4096 for i in range(8)]  # bucket 0, chunks 0..7
        entries = _mapping_record(0, 0, 0, map_num=8) + _mapping_record(  # dest [0,32768): chunks 0-7
            8 * 4096, 0, 3, map_num=5
        )  # dest [32768,53248): chunks 3-7 (subset, overlapping)
        _write_standard_composition(tmp_path / "Composition", entries)
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", plaintexts)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        size = 13 * 4096
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=size)

        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        out = bytearray(size)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            out[off : off + len(data)] = data

        real_read_run = BucketReader._read_run
        calls: list[object] = []

        async def _tracking_read_run(
            self: BucketReader, run: object, result: object, *, verify_ciphertext_crc: bool = False
        ) -> None:
            calls.append(run)
            await real_read_run(self, run, result, verify_ciphertext_crc=verify_ciphertext_crc)  # type: ignore[arg-type]

        BucketReader._read_run = _tracking_read_run  # type: ignore[method-assign]
        try:
            await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=size)
        finally:
            BucketReader._read_run = real_read_run  # type: ignore[method-assign]

        assert bytes(out[0:32768]) == b"".join(plaintexts[0:8])
        assert bytes(out[32768:53248]) == b"".join(plaintexts[3:8])
        assert len(calls) == 1  # not 2 -- the whole point of the fix


class TestExecChunksMultiBucket:
    """Multi-bucket execution — needs its own fixture (see
    ``_build_multi_bucket_dedup_file``'s own docstring for why):
    correct bytes across many buckets, non-overlapping destination
    ranges, monotonic progress, exception propagation, cancellation."""

    async def test_matches_expected_output_across_many_buckets(self, tmp_path: Path) -> None:
        num_buckets = 8
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        reference_plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        out = bytearray(size)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            # No lock: exec_chunks() runs one bucket group at a time,
            # and this coroutine contains no await anyway.
            out[off : off + len(data)] = data

        bytes_run = await exec_chunks(
            reference_plan,
            pool=file._pool,
            on_run=_on_run,
            size=size,
        )

        expected = b"".join(b"".join(_multi_bucket_plaintexts(b)) for b in range(num_buckets))
        assert bytes(out) == expected
        assert bytes_run == len(expected)

    async def test_no_two_bucket_groups_ever_report_an_overlapping_destination_range(self, tmp_path: Path) -> None:
        """Every destination byte is written by exactly one composition
        extent's own expansion, so no two bucket groups should ever hand
        ``on_run`` overlapping ranges — checked directly rather than
        trusted, by recording every call's byte range and asserting none
        intersect. A violation would mean the *plan* is wrong, which
        would corrupt output serially too."""
        num_buckets = 8
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        ranges: list[tuple[int, int]] = []

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            ranges.append((off, off + len(data)))

        await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=size)

        ranges.sort()
        for (start_a, end_a), (start_b, end_b) in zip(ranges, ranges[1:], strict=False):
            assert end_a <= start_b, f"overlapping runs: [{start_a},{end_a}) and [{start_b},{end_b})"

    async def test_progress_is_monotonic_and_reaches_the_full_total(self, tmp_path: Path) -> None:
        num_buckets = 8
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        calls: list[tuple[int, int]] = []

        async def _progress(done: int, total: int) -> None:
            calls.append((done, total))

        await exec_chunks(
            plan,
            pool=file._pool,
            on_run=_noop_on_run,
            size=size,
            progress=_progress,
        )

        assert calls
        totals = {total for _done, total in calls}
        assert len(totals) == 1  # never changes mid-run
        dones = [done for done, _total in calls]
        # One bucket group at a time and a single writer of bytes_run, so
        # progress is monotonic by construction — no lock involved.
        assert dones == sorted(dones)
        assert dones[-1] == num_buckets * _CHUNKS_PER_BUCKET * 4096

    async def test_an_on_run_exception_propagates_out_of_exec_chunks(self, tmp_path: Path) -> None:
        num_buckets = 4
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)

        async def _failing_on_run(off: int, data: bytes | memoryview) -> None:
            raise RuntimeError("simulated worker failure")

        # Plainly the child exception, with no BaseExceptionGroup in
        # sight: there is a single execution path, so the exception
        # shape is exactly the RuntimeError on_run raised.
        with pytest.raises(RuntimeError, match="simulated worker failure"):
            await exec_chunks(plan, pool=file._pool, on_run=_failing_on_run, size=size)

    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """The plan is built first, the store is armed so the first
        bucket group parks on its first read, and cancelling the
        surrounding Task must propagate ``asyncio.CancelledError``
        with no ``on_run`` call ever having happened."""
        num_buckets = 4
        file, store = _build_blocking_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        runs: list[int] = []

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            runs.append(off)

        async def do_exec() -> int:
            store.armed = True
            return await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=size)

        task = asyncio.create_task(do_exec())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert runs == []


class _SelectiveBlockingStore:
    """Unlike ``_BlockingStore`` (blocks *every* read once armed),
    this one blocks only reads matching one exact ``(path, offset)``
    predicate — everything else (every bucket's own header-open read,
    every other bucket's data read) passes straight through.

    Built for ``TestExecChunksPrefetch``'s own need: prove that
    *other* buckets' opens actually happen concurrently with one bucket's
    data read still in flight, which needs the header read (offset 0,
    ``COMPRESS_RESERVED_LENG`` bytes) to succeed immediately for every
    bucket while exactly one bucket's own chunk-data read (offset > 0)
    parks — ``_BlockingStore`` blocking indiscriminately can't isolate
    that."""

    def __init__(self, backing: LocalFsStore, blocked_path: str) -> None:
        self._backing = backing
        self._blocked_path = blocked_path
        self.blocked = asyncio.Event()  # set once the targeted read has actually parked
        self._release = asyncio.Event()

    def release(self) -> None:
        self._release.set()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        # offset > 0 excludes every bucket's own header-open read
        # (BucketReader.open() always reads starting at offset 0) so only
        # the targeted bucket's *data* read ever parks here.
        if path == self._blocked_path and offset > 0:
            self.blocked.set()
            await self._release.wait()
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._backing.listdir(path)


def _build_selective_blocking_multi_bucket_dedup_file(
    tmp_path: Path, num_buckets: int, *, blocked_bucket_id: int
) -> tuple[DedupFile, _SelectiveBlockingStore]:
    """``_build_multi_bucket_dedup_file`` over a
    ``_SelectiveBlockingStore`` that parks only
    ``blocked_bucket_id``'s own data read."""
    bucket_span = _CHUNKS_PER_BUCKET * 4096
    entries = b"".join(_mapping_record(b * bucket_span, b, 0, map_num=_CHUNKS_PER_BUCKET) for b in range(num_buckets))
    _write_standard_composition(tmp_path / "Composition", entries)
    for b in range(num_buckets):
        _write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", _multi_bucket_plaintexts(b))
    blocked_path = f"Pool/0/{blocked_bucket_id}.buk"
    store = _SelectiveBlockingStore(LocalFsStore(tmp_path), blocked_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=num_buckets * bucket_span), store


class TestExecChunksPrefetch:
    """``max_concurrent_opens`` — the background bucket-open prefetch
    (``_prefetch_bucket_opens``).
    """

    async def test_output_is_unchanged_with_prefetch_enabled(self, tmp_path: Path) -> None:
        """Same assertion as ``TestExecChunksMultiBucket``'s own
        byte-for-byte test, just with ``max_concurrent_opens`` on — the
        prefetch only ever warms ``export_cache.buckets``, never touches
        ``on_run``/decode/write, so output must be identical either way."""
        num_buckets = 8
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        out = bytearray(size)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            out[off : off + len(data)] = data

        bytes_run = await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=size, max_concurrent_opens=4)

        expected = b"".join(b"".join(_multi_bucket_plaintexts(b)) for b in range(num_buckets))
        assert bytes(out) == expected
        assert bytes_run == len(expected)

    async def test_prefetches_other_buckets_while_one_buckets_read_is_in_flight(self, tmp_path: Path) -> None:
        """The actual value proposition: while bucket 0's own data read is
        parked, the *other* buckets' ``open_bucket_uncached()`` calls
        (spied on directly, real work still delegated through) should
        already have happened — the whole point of running the prefetch
        as an independent background task rather than only ever opening
        a bucket right before ``_exec_one_bucket_group`` reads it."""
        num_buckets = 4
        file, store = _build_selective_blocking_multi_bucket_dedup_file(tmp_path, num_buckets, blocked_bucket_id=0)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)

        opened: list[int] = []
        all_prefetched = asyncio.Event()
        real_open_bucket_uncached = Pool.open_bucket_uncached

        async def _tracking_open_bucket_uncached(self: Pool, stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
            reader = await real_open_bucket_uncached(self, stream_id, bucket_id)
            opened.append(bucket_id)
            if set(opened) >= {1, 2, 3}:
                all_prefetched.set()
            return reader

        Pool.open_bucket_uncached = _tracking_open_bucket_uncached  # type: ignore[method-assign]
        try:
            task = asyncio.create_task(
                exec_chunks(
                    plan,
                    pool=file._pool,
                    on_run=_noop_on_run,
                    size=size,
                    max_concurrent_reads=1,  # serial reads -- bucket 0 alone would block everything without prefetch
                    max_concurrent_opens=4,
                )
            )
            await store.blocked.wait()  # bucket 0's own data read has parked
            # Let the prefetch task's own (already in-flight) opens for
            # buckets 1-3 actually complete. Each open is a real
            # ``asyncio.to_thread()`` local-filesystem read (LocalFsStore),
            # so it needs the executor thread pool to actually get
            # scheduled -- a fixed count of zero-duration ``asyncio.sleep(0)``
            # yields only guarantees event-loop ticks, not real wall-clock
            # time for that thread to run, and was observed to fail
            # intermittently under CPU contention (e.g. many parallel
            # ``pytest -n auto`` workers). Wait on the real condition
            # (``all_prefetched``, set by ``_tracking_open_bucket_uncached``
            # above) with a generous timeout instead; not asserting on
            # timing itself, only on which opens happened before release
            # below.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(all_prefetched.wait(), timeout=5.0)
            assert set(opened) >= {1, 2, 3}, (
                f"expected buckets 1-3 prefetched while bucket 0's read was in flight, got {opened!r}"
            )
            store.release()
            bytes_run = await task
        finally:
            Pool.open_bucket_uncached = real_open_bucket_uncached  # type: ignore[method-assign]

        assert bytes_run == num_buckets * _CHUNKS_PER_BUCKET * 4096
        assert opened.count(0) == 1  # bucket 0 opened exactly once -- prefetch + main loop shared the one fetch

    async def test_a_failed_prefetch_open_is_silently_retried_by_the_main_loop(self, tmp_path: Path) -> None:
        """_prefetch_bucket_opens' own documented contract: "a failed
        open here is silently dropped and simply re-attempted by the
        main loop's own call right after". Make one bucket's *first*
        ``open_bucket_uncached`` call (the prefetch's own) raise, and
        prove the export still completes with byte-identical output --
        the main loop's own resolve() on that same key must not also be
        poisoned by the prefetch's swallowed failure."""
        num_buckets = 4
        failing_bucket_id = 2
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        out = bytearray(size)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            out[off : off + len(data)] = data

        real_open_bucket_uncached = Pool.open_bucket_uncached
        call_count = 0

        async def _fails_once_for_bucket_2(self: Pool, stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
            nonlocal call_count
            if bucket_id == failing_bucket_id:
                call_count += 1
                if call_count == 1:
                    raise ConnectionError("simulated transient prefetch failure")
            return await real_open_bucket_uncached(self, stream_id, bucket_id)

        Pool.open_bucket_uncached = _fails_once_for_bucket_2  # type: ignore[method-assign]
        try:
            bytes_run = await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=size, max_concurrent_opens=4)
        finally:
            Pool.open_bucket_uncached = real_open_bucket_uncached  # type: ignore[method-assign]

        expected = b"".join(b"".join(_multi_bucket_plaintexts(b)) for b in range(num_buckets))
        assert bytes(out) == expected
        assert bytes_run == len(expected)
        assert call_count == 2  # the prefetch's failed attempt, then the main loop's own successful retry

    async def test_an_on_run_exception_still_propagates_unwrapped_with_prefetch_enabled(self, tmp_path: Path) -> None:
        """The compatibility guarantee ``exec_chunks``'s own docstring
        makes for ``max_concurrent_opens``: the prefetch task never
        raises and is spawned via a plain ``asyncio.create_task`` rather
        than folded into the serial path's own control flow, so a real
        ``on_run`` failure on the (default) ``max_concurrent_reads=1``
        path must still surface as the plain exception, never wrapped in
        an ``ExceptionGroup``."""
        num_buckets = 4
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)

        async def _failing_on_run(off: int, data: bytes | memoryview) -> None:
            raise RuntimeError("simulated worker failure")

        with pytest.raises(RuntimeError, match="simulated worker failure"):
            await exec_chunks(plan, pool=file._pool, on_run=_failing_on_run, size=size, max_concurrent_opens=4)


class _ConcurrencyTrackingStore:
    """Tracks the peak number of concurrently in-flight bucket *data*
    reads (``"Pool/" in path`` and ``offset >= 16384`` — excludes every
    bucket's own header-open read and every Composition-side read) by
    parking each one on ``_peak_reached``, real time, until either
    ``expected_peak`` is actually reached or a generous timeout elapses —
    long enough for every other read already scheduled (its own
    semaphore permit already acquired) to also enter and bump
    ``active``, so ``peak`` reflects genuine overlap rather than just
    call order. A fixed count of zero-duration ``asyncio.sleep(0)``
    ticks was tried first and observed to fail intermittently under
    real scheduling contention (many parallel workers): each tick only
    guarantees an event-loop turn, not real wall-clock time for a
    sibling read's own prior awaits — ``asyncio.to_thread()`` dispatch
    among them — to actually resolve. Waiting on the real condition
    removes that dependency on how many ticks happen to be "enough."

    Built for ``TestExecChunksCombinedConcurrencyCeiling``'s own
    need: prove the cross-bucket and in-bucket levels really do share one
    ceiling instead of stacking (a concern ``exec_chunks``'s own
    docstring calls out as the easiest part of this design to get
    wrong)."""

    def __init__(self, backing: LocalFsStore, *, expected_peak: int) -> None:
        self._backing = backing
        self._expected_peak = expected_peak
        self._peak_reached = asyncio.Event()
        self.active = 0
        self.peak = 0

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if "Pool/" in path and offset >= 16384:
            self.active += 1
            self.peak = max(self.peak, self.active)
            if self.peak >= self._expected_peak:
                self._peak_reached.set()
            try:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._peak_reached.wait(), timeout=2.0)
                return await self._backing.read(path, offset, length)
            finally:
                self.active -= 1
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._backing.listdir(path)


def _build_split_run_dedup_file(tmp_path: Path, *, expected_peak: int) -> tuple[DedupFile, _ConcurrencyTrackingStore]:
    """Two buckets contributing three total read "sources": bucket 0's
    own chunk_idx 0 and 2 (chunk_idx 1 deliberately never referenced, so
    with ``_GAP_TOLERANCE`` monkeypatched to 0 ``BucketReader.read_chunks``
    sees them as two separate merge runs — the same technique
    ``test_dedup_pool.py::TestReadChunksConcurrentReads`` uses) plus
    bucket 1's own single chunk. Exercises the in-bucket/cross-bucket
    hand-off together: the cross-bucket dispatch loop's two outer permits
    (one per bucket) already exhaust a ``max_concurrent_reads=2``
    semaphore, so bucket 0's own *second* run has nothing left to acquire
    until one of the two buckets' outer permits is released — exactly the
    scenario that would silently double-count if the hand-off contract
    between ``exec_chunks`` and ``read_chunks`` were wrong."""
    entries = (
        _mapping_record(0, 0, 0, map_num=1)  # bucket 0, chunk_idx 0 -> dest [0, 4096)
        + _mapping_record(4096, 0, 2, map_num=1)  # bucket 0, chunk_idx 2 -> dest [4096, 8192), skips chunk_idx 1
        + _mapping_record(8192, 1, 0, map_num=1)  # bucket 1, chunk_idx 0 -> dest [8192, 12288)
    )
    _write_standard_composition(tmp_path / "Composition", entries)
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", [bytes([1]) * 4096 for _ in range(3)])  # chunk_idx 1 unreferenced
    _write_bucket(tmp_path / "Pool" / "0" / "1.buk", [bytes([2]) * 4096])
    store = _ConcurrencyTrackingStore(LocalFsStore(tmp_path), expected_peak=expected_peak)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=12288), store


class TestExecChunksCombinedConcurrencyCeiling:
    """The whole point of unifying cross-bucket and in-bucket concurrency
    onto one shared ``asyncio.Semaphore`` (see ``exec_chunks``'s own
    docstring for the exact hand-off mechanics): the *combined* number of
    concurrently in-flight reads — across bucket boundaries and within
    one bucket's own multiple runs — must never exceed
    ``max_concurrent_reads``, not just each axis checked in isolation
    (``test_dedup_pool.py::TestReadChunksConcurrentReads`` already proves
    the in-bucket axis alone; ``TestExecChunksMultiBucket``'s own
    tests prove cross-bucket output correctness, not a ceiling)."""

    async def test_combined_ceiling_is_never_exceeded(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("synology_apm_repo.sdk.dedup.pool._GAP_TOLERANCE", 0)
        file, store = _build_split_run_dedup_file(tmp_path, expected_peak=2)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        # Confirms the fixture actually exercises what this test claims
        # before trusting its own concurrency assertion below: 3 total
        # ChunkRuns (bucket 0's two + bucket 1's one) across 2 buckets.
        assert sum(len(runs) for runs in plan.groups.values()) == 3
        assert len(plan.groups) == 2

        bytes_run = await exec_chunks(plan, pool=file._pool, on_run=_noop_on_run, size=size, max_concurrent_reads=2)

        assert bytes_run == size
        assert store.peak <= 2, f"expected at most 2 concurrent reads, observed a peak of {store.peak}"
        assert store.peak == 2, "expected the 3 available sources to actually reach the ceiling, not undershoot it"


@pytest.fixture(autouse=True)
def _reset_export_worker_globals() -> Iterator[None]:
    """``_worker_store``/``_worker_pool``/``_worker_dst_fd`` (this module's
    own process-global worker state) and ``concurrency``'s own persistent
    ``_worker_runner`` must not leak between tests — a real worker process
    only ever sets these once, per its own whole lifetime, but this suite
    runs every test in the same process."""
    yield
    if chunk_walk._worker_dst_fd is not None:
        with contextlib.suppress(OSError):
            os.close(chunk_walk._worker_dst_fd)
    chunk_walk._worker_store = None
    chunk_walk._worker_pool = None
    chunk_walk._worker_dst_fd = None
    if concurrency._worker_runner is not None:
        concurrency.close_worker_loop()


class _LoopCheckingStore:
    """Reproduces ``S3Store._get_client``'s exact hazard
    (``storage/s3.py``) — a network client lazily built and cached on
    first use, bound to whichever event loop happens to be running then —
    without touching ``aioboto3``/``aiohttp``: caches the running loop on
    this store's first call and raises if a later call runs on a
    *different* one, the same observable failure a per-task
    ``asyncio.run()`` worker produces against a real lazily-cached client
    once a second task reuses it."""

    def __init__(self, backing: LocalFsStore) -> None:
        self._backing = backing
        self._bound_loop: asyncio.AbstractEventLoop | None = None

    def _check_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._bound_loop is None:
            self._bound_loop = loop
        elif self._bound_loop is not loop:
            raise RuntimeError("Event loop is closed")

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self._check_loop()
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        self._check_loop()
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        self._check_loop()
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        self._check_loop()
        return await self._backing.listdir(path)


class TestExportWorkerLoopReuse:
    """Regression test for the bug ``concurrency.run_in_worker_loop`` fixes:
    a ``ProcessPoolExecutor`` worker handles many bucket-group tasks over
    its lifetime, and its ``ObjectStore`` (built once per worker process —
    see ``_export_worker_init``'s own docstring) is meant to survive every
    one of them, including a lazily-cached, loop-bound client
    (``S3Store._get_client``, say). Exercised in-process — no real
    subprocess needed, since the bug is about ``asyncio.run()``'s own
    per-call loop, not about multiprocessing itself — via
    ``_LoopCheckingStore``, which reproduces that hazard without a real
    network backend."""

    def test_second_task_in_the_same_worker_reuses_the_first_ones_loop(self, tmp_path: Path) -> None:
        _write_standard_composition(tmp_path / "Composition", _standard_entries())
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = _LoopCheckingStore(LocalFsStore(tmp_path))
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(4096))
        chunk_walk._worker_pool = pool
        chunk_walk._worker_dst_fd = os.open(dst, os.O_WRONLY | _O_BINARY)
        args = ExportGroupWorkerArgs(
            stream_id=StreamId(0), bucket_id=BucketId(0), runs=[ChunkRun(0, 1, 0)], size=4096, dst_offset=0
        )

        # First task in this "worker": builds the loop-checking store's
        # cached loop.
        export_bucket_group_worker(args)
        # Second task, same worker (same process-global state, exactly
        # like a real ProcessPoolExecutor worker handling a second item)
        # — must reuse the same loop, not open a fresh one that orphans
        # the first task's cached loop reference. Before this fix, this
        # raised "Event loop is closed".
        export_bucket_group_worker(args)


class TestExportWorkerShutdown:
    """``_export_worker_init`` registers ``_export_worker_shutdown`` via
    ``atexit`` as its very last statement; ``_export_worker_shutdown``
    itself releases an ``AsyncCloseable`` store and the destination fd,
    tolerating the store's own ``aclose()`` raising. See
    ``test_concurrency.py``'s own module docstring for why a real
    spawned-worker proof that ``atexit`` itself fires isn't possible in
    this test suite — this class covers this shutdown function's own
    logic instead, via direct calls."""

    def test_export_worker_init_registers_the_shutdown_hook(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_standard_composition(tmp_path / "Composition", _standard_entries())
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        descriptor = PoolDescriptor.from_pool(pool)
        assert descriptor is not None
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")
        registered: list[object] = []
        monkeypatch.setattr(atexit, "register", registered.append)

        _export_worker_init(descriptor, str(dst))

        assert registered == [_export_worker_shutdown]

    def test_shutdown_acloses_an_asynccloseable_store_and_closes_the_fd(self, tmp_path: Path) -> None:
        closed: list[bool] = []

        class _FakeAsyncCloseableStore:
            async def aclose(self) -> None:
                closed.append(True)

        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")
        fd = os.open(dst, os.O_WRONLY | _O_BINARY)
        chunk_walk._worker_store = _FakeAsyncCloseableStore()  # type: ignore[assignment]
        chunk_walk._worker_dst_fd = fd

        _export_worker_shutdown()

        assert closed == [True]
        with pytest.raises(OSError):
            os.write(fd, b"x")  # the fd was actually closed, not merely dropped
        chunk_walk._worker_dst_fd = None  # already closed above; the autouse fixture must not double-close it

    def test_shutdown_tolerates_the_store_s_aclose_raising(self, tmp_path: Path) -> None:
        class _FailingAsyncCloseableStore:
            async def aclose(self) -> None:
                raise RuntimeError("synthetic aclose failure")

        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")
        fd = os.open(dst, os.O_WRONLY | _O_BINARY)
        chunk_walk._worker_store = _FailingAsyncCloseableStore()  # type: ignore[assignment]
        chunk_walk._worker_dst_fd = fd

        _export_worker_shutdown()  # must not raise despite aclose() failing

        with pytest.raises(OSError):
            os.write(fd, b"x")  # the fd still got closed
        chunk_walk._worker_dst_fd = None  # already closed above; the autouse fixture must not double-close it


class TestPositionalWriteFallback:
    """``os.pwrite`` doesn't exist on Windows — this project's own CI runs
    on Linux/macOS, where the ``sys.platform != "win32"`` branch is always
    taken; here the ``lseek``+``write`` fallback is exercised directly by
    forcing ``sys.platform`` to ``"win32"``, proving it lands bytes at the
    same offset ``os.pwrite`` would."""

    def test_fallback_lands_bytes_at_the_given_offset(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "platform", "win32")
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(10))
        fd = os.open(dst, os.O_WRONLY | _O_BINARY)
        try:
            chunk_walk._pwrite(fd, b"hello", 3)
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(3) + b"hello" + bytes(2)
