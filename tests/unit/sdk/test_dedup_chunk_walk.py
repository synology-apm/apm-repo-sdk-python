"""Unit tests for ``synology_apm_repo.sdk.dedup.chunk_walk``, the two-pass planning/execution engine behind ``export_scheduler``: the packing
helpers directly, plus ``plan_chunks_windowed``/``exec_chunks`` behaviors awkward to observe through
``DedupFile.export_range()``. End-to-end output correctness is covered by ``test_dedup_export_scheduler.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import random
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest

from support.format_builders import (
    mapping_record,
)
from support.repo_builders import (
    write_bucket,
    write_composition_entries,
)
from support.store_fakes import BlockingStore, WrappingStore
from synology_apm_repo.sdk.dedup.chunk_walk import (
    _MAX_MERGED_RUN,
    ChunkPlan,
    ChunkRun,
    _flush_run,
    _GapDelta,
    _handle_gap_extent,
    _merge_and_flush_writes,
    _validate_window_start,
    _walk_extents,
    ascending_runs,
    count_planned_bytes,
    exec_chunks,
    iter_bucket_keys,
    iter_chunk_runs,
    merge_overlapping_ranges,
    plan_chunks_windowed,
)
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.extent import ExtentKind, GapExtent
from synology_apm_repo.sdk.dedup.pool import BucketReader, Pool
from synology_apm_repo.sdk.format.const import FIXED_CHUNK_LENGTH
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.dedup_export_fakes import (
    CHUNK_PLAINTEXTS,
    HEAD_OFF,
    SESSION_ID,
    SIZE,
    STREAM_ID,
    build_blocking_dedup_file,
    dedup_file_at,
)
from unit.sdk.pool_fakes import chunk_address


async def _reference_plan_chunks(
    base: DedupFile,
    start: int,
    end: int,
    window_start: int,
    *,
    write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
) -> ChunkPlan:
    """Unwindowed oracle for ``plan_chunks_windowed``, built on the same
    ``_walk_extents``/``_validate_window_start``."""
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


@pytest.fixture
def dedup_file(tmp_path: Path) -> DedupFile:
    return dedup_file_at(tmp_path)


async def _exec_counting(
    plan: ChunkPlan, *, on_run: Callable[[int, bytes | memoryview], Awaitable[None]], **kwargs: Any
) -> int:
    """``exec_chunks``, returning the bytes that reached ``on_run``."""
    written = 0

    async def counting(offset: int, data: bytes | memoryview) -> None:
        nonlocal written
        written += len(data)
        await on_run(offset, data)

    await exec_chunks(plan, on_run=counting, **kwargs)
    return written


async def _noop_on_run(off: int, data: bytes | memoryview) -> None:
    """An awaitable ``on_run`` that discards its input."""


def _recording_zero_fill(calls: list[tuple[int, int]]) -> Callable[[int, int], Awaitable[None]]:
    """An awaitable ``write_zero_fill`` that appends ``(off, length)`` to ``calls``."""

    async def _write_zero_fill(off: int, length: int) -> None:
        calls.append((off, length))

    return _write_zero_fill


_CHUNKS_PER_BUCKET = 2


def _multi_bucket_plaintexts(bucket_id: int) -> list[bytes]:
    # Distinct byte value per (bucket, chunk) so a misplaced chunk is detectable.
    return [bytes([(bucket_id * 10 + i) % 256]) * 4096 for i in range(_CHUNKS_PER_BUCKET)]


def _build_multi_bucket_dedup_file(tmp_path: Path, num_buckets: int) -> DedupFile:
    """``num_buckets`` buckets, each contributing ``_CHUNKS_PER_BUCKET``
    contiguous chunks to the file."""
    bucket_span = _CHUNKS_PER_BUCKET * 4096
    entries = b"".join(mapping_record(b * bucket_span, b, 0, map_num=_CHUNKS_PER_BUCKET) for b in range(num_buckets))
    write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
    for b in range(num_buckets):
        write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", _multi_bucket_plaintexts(b))
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, HEAD_OFF, size=num_buckets * bucket_span)


def _build_blocking_multi_bucket_dedup_file(tmp_path: Path, num_buckets: int) -> tuple[DedupFile, BlockingStore]:
    """``_build_multi_bucket_dedup_file`` over a ``BlockingStore``."""
    bucket_span = _CHUNKS_PER_BUCKET * 4096
    entries = b"".join(mapping_record(b * bucket_span, b, 0, map_num=_CHUNKS_PER_BUCKET) for b in range(num_buckets))
    write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
    for b in range(num_buckets):
        write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", _multi_bucket_plaintexts(b))
    store = BlockingStore(LocalFsStore(tmp_path))
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, HEAD_OFF, size=num_buckets * bucket_span), store


def _expand_placements(groups: dict[tuple[StreamId, BucketId], list[ChunkRun]]) -> list[tuple[int, int]]:
    """Every group's ``ChunkRun``s expanded to sorted ``(chunk_idx, dest_offset)``
    placements, so plans compare regardless of how placements split into runs."""
    return sorted(
        (run.chunk_idx_start + i, run.dest_offset_start + i * 4096)
        for runs in groups.values()
        for run in runs
        for i in range(run.length)
    )


class TestIterChunkRuns:
    """``iter_chunk_runs`` splits a run only at a repeat-cycle wraparound
    (the address jumping back to the template start) or a bucket carry."""

    def test_a_plain_run_with_no_repeat_or_carry_is_one_run(self) -> None:
        addr = chunk_address(0, 0, 0)
        runs = list(iter_chunk_runs(addr, map_num=5, first_k=0, last_k=4))
        assert len(runs) == 1
        start_addr, run_len, k_start = runs[0]
        assert (start_addr.bucket_id, start_addr.chunk_idx, run_len, k_start) == (0, 0, 5, 0)

    def test_a_sub_window_within_one_cycle_is_still_one_run(self) -> None:
        """Chunks 1..2 of a map_num=3 record starting at chunk 0."""
        addr = chunk_address(0, 0, 0)
        runs = list(iter_chunk_runs(addr, map_num=3, first_k=1, last_k=2))
        assert len(runs) == 1
        start_addr, run_len, k_start = runs[0]
        assert (start_addr.chunk_idx, run_len, k_start) == (1, 2, 1)

    def test_repeat_wraparound_splits_into_one_run_per_cycle(self) -> None:
        """``map_num=2, repeat=1`` yields chunks [3,4,3,4], never [3,4,5,6]:
        two runs, not one of length 4."""
        addr = chunk_address(0, 0, 3)
        runs = list(iter_chunk_runs(addr, map_num=2, first_k=0, last_k=3))
        assert [(r.chunk_idx, length, k) for r, length, k in runs] == [(3, 2, 0), (3, 2, 2)]

    def test_bucket_carry_splits_a_run_that_crosses_the_bucket_boundary(self) -> None:
        """A run crossing ``BUCKET_MAX_CHUNK_NUM`` (8192) splits at the carry,
        as ``ChunkAddress.advance`` carries."""
        addr = chunk_address(0, 0, 8191)
        runs = list(iter_chunk_runs(addr, map_num=2, first_k=0, last_k=1))
        assert len(runs) == 2
        (first_addr, first_len, first_k), (second_addr, second_len, second_k) = runs
        assert (first_addr.bucket_id, first_addr.chunk_idx, first_len, first_k) == (0, 8191, 1, 0)
        assert (second_addr.bucket_id, second_addr.chunk_idx, second_len, second_k) == (1, 0, 1, 1)


class TestMergeOverlappingRanges:
    """``merge_overlapping_ranges``: two different ``ChunkRun``s for one bucket
    can reference overlapping-but-not-identical physical ranges."""

    @pytest.mark.parametrize(
        ("ranges", "expected"),
        [
            pytest.param(
                [(100, 10), (0, 10)], [(0, 10), (100, 10)], id="disjoint_ranges_pass_through_unchanged_sorted_by_start"
            ),
            pytest.param(
                [(50, 20), (50, 20), (50, 20)], [(50, 20)], id="identical_ranges_collapse_to_one_the_repeat_case"
            ),
            # [10,20) and [20,30) share no chunk_idx but touch, so they merge.
            pytest.param([(10, 10), (20, 10)], [(10, 20)], id="touching_ranges_merge_gap_of_exactly_zero"),
            # [417,468) nested in [417,1485) merges to the larger span, not the first-sorted one.
            pytest.param(
                [(417, 1068), (417, 51)],
                [(417, 1068)],
                id="a_range_fully_nested_inside_another_merges_to_the_outer_ones_span",
            ),
        ],
    )
    def test_merge(self, ranges: list[tuple[int, int]], expected: list[tuple[int, int]]) -> None:
        assert merge_overlapping_ranges(ranges) == expected

    def test_a_range_partially_overlapping_a_later_one_merges_to_their_union(self) -> None:
        """A range nested in the earlier one keeps its span ([0,417)+[119,172)
        is [0,417)); a partial overlap merges to the union ([0,417)+[300,600)
        is [0,600))."""
        assert merge_overlapping_ranges([(0, 417), (119, 53)]) == [(0, 417)]
        assert merge_overlapping_ranges([(0, 417), (300, 300)]) == [(0, 600)]

    def test_fifteen_transitively_overlapping_ranges_collapse_to_one_span(self) -> None:
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
        merged = merge_overlapping_ranges(ranges)
        assert merged == [(0, 8192)]
        # General invariants, independent of the exact list above:
        total_input_chunks = sum(length for _start, length in ranges)
        total_merged_chunks = sum(length for _start, length in merged)
        assert total_merged_chunks < total_input_chunks  # real overlap was actually removed
        for (s1, l1), (s2, _l2) in itertools.pairwise(merged):
            assert s1 + l1 < s2  # strictly increasing, non-touching, non-overlapping between output ranges
        assert merged == sorted(merged)


class TestHandleGapExtent:
    async def test_extent_entirely_outside_the_window_clips_to_an_empty_span(self) -> None:
        extent = GapExtent(offset=0, length=100, kind=ExtentKind.HOLE)
        result = await _handle_gap_extent(extent, start=200, end=300, window_start=0, write_zero_fill=None)
        assert result == (0, 0)


class TestFlushRun:
    async def test_empty_run_buf_is_a_no_op(self) -> None:
        calls: list[tuple[int, bytes]] = []

        async def on_run(offset: int, data: bytes | memoryview) -> None:
            calls.append((offset, bytes(data)))

        await _flush_run(on_run, run_start=0, run_buf=bytearray(), size=100)
        assert calls == []

    async def test_run_start_at_or_past_size_writes_nothing(self) -> None:
        calls: list[tuple[int, bytes]] = []

        async def on_run(offset: int, data: bytes | memoryview) -> None:
            calls.append((offset, bytes(data)))

        await _flush_run(on_run, run_start=100, run_buf=bytearray(b"data"), size=100)
        await _flush_run(on_run, run_start=98, run_buf=bytearray(b"data"), size=100)
        assert calls == [(98, b"da")]  # only the in-size control call writes, clipped to size


class TestMergeAndFlushWrites:
    """``_merge_and_flush_writes`` must produce the same writes as
    ``_reference_merge_and_flush_writes``, the per-chunk loop oracle."""

    @staticmethod
    def _cache(indices: range, *, lengths: dict[int, int] | None = None) -> dict[int, bytes | memoryview]:
        lengths = lengths or {}
        return {i: bytes([i % 251]) * lengths.get(i, FIXED_CHUNK_LENGTH) for i in indices}

    @staticmethod
    async def _flushes(
        merge: Callable[..., Awaitable[None]], runs: list[ChunkRun], cache: dict[int, bytes | memoryview], size: int
    ) -> list[tuple[int, bytes]]:
        writes: list[tuple[int, bytes]] = []

        async def on_run(offset: int, data: bytes | memoryview) -> None:
            writes.append((offset, bytes(data)))

        await merge(runs, cache, on_run=on_run, size=size)
        return writes

    async def _assert_same_as_reference(
        self, runs: list[ChunkRun], cache: dict[int, bytes | memoryview], size: int
    ) -> list[tuple[int, bytes]]:
        expected = await self._flushes(_reference_merge_and_flush_writes, runs, cache, size)
        actual = await self._flushes(_merge_and_flush_writes, runs, cache, size)
        assert actual == expected
        return actual

    async def test_adjacent_runs_merge_into_one_write(self) -> None:
        runs = [ChunkRun(0, 2, 0), ChunkRun(10, 3, 2 * FIXED_CHUNK_LENGTH)]
        cache = self._cache(range(13))
        writes = await self._assert_same_as_reference(runs, cache, size=5 * FIXED_CHUNK_LENGTH)
        assert [(offset, len(data)) for offset, data in writes] == [(0, 5 * FIXED_CHUNK_LENGTH)]

    async def test_runs_with_a_gap_between_them_are_separate_writes(self) -> None:
        runs = [ChunkRun(0, 2, 0), ChunkRun(10, 2, 5 * FIXED_CHUNK_LENGTH)]
        cache = self._cache(range(12))
        writes = await self._assert_same_as_reference(runs, cache, size=7 * FIXED_CHUNK_LENGTH)
        assert [offset for offset, _ in writes] == [0, 5 * FIXED_CHUNK_LENGTH]

    async def test_dest_order_not_input_order_decides_the_merge(self) -> None:
        runs = [ChunkRun(10, 2, 2 * FIXED_CHUNK_LENGTH), ChunkRun(0, 2, 0)]
        cache = self._cache(range(12))
        writes = await self._assert_same_as_reference(runs, cache, size=4 * FIXED_CHUNK_LENGTH)
        assert [offset for offset, _ in writes] == [0]

    async def test_a_run_longer_than_the_merge_cap_is_split_at_the_cap(self) -> None:
        chunks = _MAX_MERGED_RUN // FIXED_CHUNK_LENGTH * 2 + 5  # two full caps and a remainder
        runs = [ChunkRun(0, chunks, 0)]
        cache = self._cache(range(chunks))
        writes = await self._assert_same_as_reference(runs, cache, size=chunks * FIXED_CHUNK_LENGTH)
        assert [len(data) for _, data in writes] == [_MAX_MERGED_RUN, _MAX_MERGED_RUN, 5 * FIXED_CHUNK_LENGTH]

    async def test_a_write_is_clipped_to_the_logical_size(self) -> None:
        runs = [ChunkRun(0, 4, 0)]
        cache = self._cache(range(4))
        writes = await self._assert_same_as_reference(runs, cache, size=3 * FIXED_CHUNK_LENGTH + 100)
        assert [len(data) for _, data in writes] == [3 * FIXED_CHUNK_LENGTH + 100]

    async def test_a_short_chunk_breaks_contiguity_exactly_as_the_per_chunk_loop_does(self) -> None:
        runs = [ChunkRun(0, 4, 0)]
        cache = self._cache(range(4), lengths={1: 100})
        writes = await self._assert_same_as_reference(runs, cache, size=4 * FIXED_CHUNK_LENGTH)
        # chunk 1 is short, so chunk 2's offset no longer meets the end of what was gathered: a new write starts there.
        assert [(offset, len(data)) for offset, data in writes] == [
            (0, FIXED_CHUNK_LENGTH + 100),
            (2 * FIXED_CHUNK_LENGTH, 2 * FIXED_CHUNK_LENGTH),
        ]

    async def test_no_runs_writes_nothing(self) -> None:
        assert await self._assert_same_as_reference([], {}, size=FIXED_CHUNK_LENGTH) == []

    @pytest.mark.parametrize("seed", range(25))
    async def test_random_layouts_match_the_per_chunk_loop(self, seed: int) -> None:
        rng = random.Random(seed)
        runs: list[ChunkRun] = []
        next_dest = 0
        next_chunk = 0
        lengths: dict[int, int] = {}
        for _ in range(rng.randint(1, 12)):
            length = rng.choice([1, 2, 3, 17, 2048, 2049, 4100])
            next_dest += rng.choice([0, 0, 0, 1, 7]) * FIXED_CHUNK_LENGTH  # mostly touching, sometimes a gap
            runs.append(ChunkRun(next_chunk, length, next_dest))
            if rng.random() < 0.2:
                lengths[next_chunk + rng.randrange(length)] = rng.choice([1, 100, FIXED_CHUNK_LENGTH - 1])
            next_dest += length * FIXED_CHUNK_LENGTH
            next_chunk += length + rng.choice([0, 3])
        rng.shuffle(runs)
        cache = self._cache(range(next_chunk), lengths=lengths)
        size = rng.choice([next_dest, next_dest - 100, next_dest // 2])
        await self._assert_same_as_reference(runs, cache, size=max(size, FIXED_CHUNK_LENGTH))


async def _reference_merge_and_flush_writes(
    runs: list[ChunkRun],
    cache: dict[int, bytes | memoryview],
    *,
    on_run: Callable[[int, bytes | memoryview], Awaitable[None]],
    size: int,
) -> None:
    """Per-chunk oracle for ``_merge_and_flush_writes``: one Python step per chunk."""
    dest_offset_major = sorted(runs, key=lambda r: r.dest_offset_start)
    run_start = -1
    run_buf = bytearray()
    for run in dest_offset_major:
        # _MAX_MERGED_RUN is a multiple of FIXED_CHUNK_LENGTH, so a cap flush lands on a chunk boundary.
        for i in range(run.length):
            chunk = cache[run.chunk_idx_start + i]
            dest_offset = run.dest_offset_start + i * FIXED_CHUNK_LENGTH
            if run_buf and dest_offset == run_start + len(run_buf) and len(run_buf) < _MAX_MERGED_RUN:
                run_buf += chunk
                continue
            await _flush_run(on_run, run_start, run_buf, size)
            run_start = dest_offset
            run_buf = bytearray(chunk)
    await _flush_run(on_run, run_start, run_buf, size)


class TestPlanChunks:
    async def test_write_zero_fill_none_only_counts_holes_and_zeros(self, dedup_file: DedupFile) -> None:
        plan = await _reference_plan_chunks(dedup_file, 0, SIZE, 0, write_zero_fill=None)
        # Standard fixture: [12288,20480) ZERO, [20480,24576) HOLE, trailing [40960,45056) HOLE.
        assert plan.zeros == 8192
        assert plan.holes == 4096 + 4096

    async def test_write_zero_fill_callback_is_invoked_for_holes_and_zeros(self, dedup_file: DedupFile) -> None:
        calls: list[tuple[int, int]] = []
        await _reference_plan_chunks(dedup_file, 0, SIZE, 0, write_zero_fill=_recording_zero_fill(calls))
        assert (12288, 8192) in calls  # the ZERO extent
        assert (20480, 4096) in calls  # the HOLE between ZERO and the second DATA extent
        assert (40960, 4096) in calls  # the trailing HOLE

    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Cancelling the Task while the plan walk is parked in a read raises ``CancelledError``."""
        file, store = build_blocking_dedup_file(tmp_path)

        async def do_plan() -> ChunkPlan:
            store.armed = True
            return await _reference_plan_chunks(file, 0, SIZE, 0, write_zero_fill=None)

        task = asyncio.create_task(do_plan())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_zero_extent_is_clipped_to_a_requested_end_inside_it(self, dedup_file: DedupFile) -> None:
        """end=12290 inside the ZERO extent [12288, 20480) reports only 2 bytes."""
        end = 12290
        calls: list[tuple[int, int]] = []
        plan = await _reference_plan_chunks(dedup_file, 0, end, 0, write_zero_fill=_recording_zero_fill(calls))
        assert plan.zeros == 2
        assert calls == [(12288, 2)]

    async def test_hole_extent_is_clipped_to_a_requested_end_inside_it(self, dedup_file: DedupFile) -> None:
        """The HOLE extent [20480, 24576) with end=20482 reports only 2 bytes."""
        end = 20482
        calls: list[tuple[int, int]] = []
        plan = await _reference_plan_chunks(dedup_file, 0, end, 0, write_zero_fill=_recording_zero_fill(calls))
        assert plan.holes == 2
        assert (20480, 2) in calls
        assert (20480, 4096) not in calls

    async def test_data_chunks_entirely_before_start_are_skipped_not_negative(self, dedup_file: DedupFile) -> None:
        """Chunks entirely before ``start`` are excluded rather than given a negative
        ``dest_offset``; chunks 1 and 2 get window-relative offsets."""
        plan = await _reference_plan_chunks(dedup_file, 4096, 12288, 4096, write_zero_fill=None)
        assert _expand_placements(plan.groups) == [(1, 0), (2, 4096)]

    async def test_rejects_a_non_chunk_aligned_window_start(self, dedup_file: DedupFile) -> None:
        """A non-aligned ``window_start`` would misalign every ``dest_offset``, so it raises."""
        with pytest.raises(ValueError, match="chunk-aligned window_start"):
            await _reference_plan_chunks(dedup_file, 100, SIZE, 100, write_zero_fill=None)

    async def test_rejects_a_window_start_greater_than_start(self, dedup_file: DedupFile) -> None:
        """``window_start`` must not exceed ``start``, or ``dest_offset`` would go negative."""
        with pytest.raises(ValueError, match=r"window_start \(4096\) <= start \(0\)"):
            await _reference_plan_chunks(dedup_file, 0, 12288, 4096, write_zero_fill=None)

    async def test_window_start_less_than_start_positions_chunks_at_their_absolute_offset(
        self, dedup_file: DedupFile
    ) -> None:
        """With ``window_start < start``, ``dest_offset`` stays anchored to ``window_start``."""
        plan = await _reference_plan_chunks(dedup_file, 4096, 12288, 0, write_zero_fill=None)
        assert _expand_placements(plan.groups) == [(1, 4096), (2, 8192)]


def _merge_windows(windows: list[ChunkPlan]) -> tuple[dict[tuple[StreamId, BucketId], list[ChunkRun]], int, int]:
    """Combine every window's groups/holes/zeros into the shape of one
    ``_reference_plan_chunks`` ``ChunkPlan``."""
    merged: dict[tuple[StreamId, BucketId], list[ChunkRun]] = {}
    holes = zeros = 0
    for plan in windows:
        holes += plan.holes
        zeros += plan.zeros
        for key, runs in plan.groups.items():
            merged.setdefault(key, []).extend(runs)
    return merged, holes, zeros


class TestPlanChunksWindowed:
    """The sliding-window memory bound, checked against ``_reference_plan_chunks``."""

    @pytest.mark.parametrize("max_entries", [1, 2, 3, 4, 5, 6, 7, 8, 100])
    async def test_matches_the_unwindowed_plan_for_various_window_sizes(
        self, dedup_file: DedupFile, max_entries: int
    ) -> None:
        reference = await _reference_plan_chunks(dedup_file, 0, SIZE, 0, write_zero_fill=None)
        windows = [
            w async for w in plan_chunks_windowed(dedup_file, 0, SIZE, 0, write_zero_fill=None, max_entries=max_entries)
        ]
        merged_groups, holes, zeros = _merge_windows(windows)

        assert holes == reference.holes
        assert zeros == reference.zeros
        # Compare placements, not runs: a window boundary can split a run.
        assert _expand_placements(merged_groups) == _expand_placements(reference.groups)

    @pytest.mark.parametrize("max_entries", [1, 2, 3])
    async def test_no_window_exceeds_max_entries(self, dedup_file: DedupFile, max_entries: int) -> None:
        windows = [
            w async for w in plan_chunks_windowed(dedup_file, 0, SIZE, 0, write_zero_fill=None, max_entries=max_entries)
        ]
        for plan in windows:
            total = sum(run.length for arr in plan.groups.values() for run in arr)
            assert total <= max_entries

    async def test_small_window_yields_more_than_one_plan(self, dedup_file: DedupFile) -> None:
        # The fixture has 7 DATA chunk placements; max_entries=2 must split them.
        windows = [w async for w in plan_chunks_windowed(dedup_file, 0, SIZE, 0, write_zero_fill=None, max_entries=2)]
        assert len(windows) > 1

    async def test_large_window_yields_exactly_one_plan(self, dedup_file: DedupFile) -> None:
        windows = [
            w async for w in plan_chunks_windowed(dedup_file, 0, SIZE, 0, write_zero_fill=None, max_entries=1_000_000)
        ]
        assert len(windows) == 1

    async def test_always_yields_at_least_one_plan_even_for_an_all_hole_range(self, dedup_file: DedupFile) -> None:
        # [20480, 24576) is HOLE-only: no DATA chunks, yet one plan is still yielded.
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
                SIZE,
                0,
                write_zero_fill=_recording_zero_fill(calls),
                max_entries=2,
            )
        ]
        reference_calls: list[tuple[int, int]] = []
        await _reference_plan_chunks(dedup_file, 0, SIZE, 0, write_zero_fill=_recording_zero_fill(reference_calls))
        assert (12288, 8192) in reference_calls
        assert calls == reference_calls

    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Cancelling the Task consuming the generator raises ``CancelledError`` out of the ``async for``."""
        file, store = build_blocking_dedup_file(tmp_path)

        async def consume() -> list[ChunkPlan]:
            store.armed = True
            return [w async for w in plan_chunks_windowed(file, 0, SIZE, 0, write_zero_fill=None, max_entries=1)]

        task = asyncio.create_task(consume())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_data_chunks_entirely_before_start_are_skipped(self, dedup_file: DedupFile) -> None:
        """``start=4096`` excludes chunk 0."""
        windows = [w async for w in plan_chunks_windowed(dedup_file, 4096, 12288, 4096, write_zero_fill=None)]
        merged_groups, _holes, _zeros = _merge_windows(windows)
        assert _expand_placements(merged_groups) == [(1, 0), (2, 4096)]

    async def test_rejects_a_non_chunk_aligned_window_start(self, dedup_file: DedupFile) -> None:
        with pytest.raises(ValueError, match="chunk-aligned window_start"):
            [w async for w in plan_chunks_windowed(dedup_file, 100, SIZE, 100, write_zero_fill=None)]

    async def test_rejects_a_window_start_greater_than_start(self, dedup_file: DedupFile) -> None:
        with pytest.raises(ValueError, match=r"window_start \(4096\) <= start \(0\)"):
            [w async for w in plan_chunks_windowed(dedup_file, 0, 12288, 4096, write_zero_fill=None)]

    async def test_window_start_less_than_start_positions_chunks_at_their_absolute_offset(
        self, dedup_file: DedupFile
    ) -> None:
        """``dest_offset`` stays anchored to ``window_start`` here too."""
        windows = [w async for w in plan_chunks_windowed(dedup_file, 4096, 12288, 0, write_zero_fill=None)]
        merged_groups, _holes, _zeros = _merge_windows(windows)
        assert _expand_placements(merged_groups) == [(1, 4096), (2, 8192)]


class TestIterBucketKeys:
    """``iter_bucket_keys`` names exactly the buckets a plan of the same
    range would group, without building one."""

    @staticmethod
    async def _keys(file: DedupFile, start: int, end: int) -> set[tuple[StreamId, BucketId]]:
        return {key async for key in iter_bucket_keys(file, start, end)}

    @pytest.mark.parametrize(("start", "end"), [(0, SIZE), (4096, 12288), (24576, 40960), (28672, 32768), (0, 0)])
    async def test_matches_the_plans_groups(self, dedup_file: DedupFile, start: int, end: int) -> None:
        window_start = start - start % 4096
        reference = await _reference_plan_chunks(dedup_file, start, end, window_start, write_zero_fill=None)
        assert await self._keys(dedup_file, start, end) == set(reference.groups)

    async def test_matches_the_plans_groups_across_buckets(self, tmp_path: Path) -> None:
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets=4)
        size = file.size
        assert size is not None
        for start, end in [(0, size), (4096, size - 4096), (size // 2, size // 2 + 4096)]:
            reference = await _reference_plan_chunks(file, start, end, start, write_zero_fill=None)
            assert await self._keys(file, start, end) == set(reference.groups)


class TestCountPlannedBytes:
    async def test_matches_the_unwindowed_plans_own_total(self, dedup_file: DedupFile) -> None:
        reference = await _reference_plan_chunks(dedup_file, 0, SIZE, 0, write_zero_fill=None)
        expected = sum(run.length for arr in reference.groups.values() for run in arr) * 4096
        assert await count_planned_bytes(dedup_file, 0, SIZE) == expected

    async def test_matches_the_unwindowed_plans_own_total_for_a_leading_boundary_window(
        self, dedup_file: DedupFile
    ) -> None:
        """``count_planned_bytes()`` excludes chunks before ``start``, matching the reference plan."""
        reference = await _reference_plan_chunks(dedup_file, 4096, 12288, 4096, write_zero_fill=None)
        expected = sum(run.length for arr in reference.groups.values() for run in arr) * 4096
        assert await count_planned_bytes(dedup_file, 4096, 12288) == expected
        assert expected == 2 * 4096  # chunks 1, 2 only -- chunk 0 excluded

    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Cancelling the Task during the extents walk raises ``CancelledError``."""
        file, store = build_blocking_dedup_file(tmp_path)

        async def do_count() -> int:
            store.armed = True
            return await count_planned_bytes(file, 0, SIZE)

        task = asyncio.create_task(do_count())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class TestExecChunks:
    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Cancelling while ``exec_chunks()`` is parked on its first bucket read
        raises ``CancelledError`` with ``on_run`` never called."""
        file, store = build_blocking_dedup_file(tmp_path)
        plan = await _reference_plan_chunks(file, 0, SIZE, 0, write_zero_fill=None)
        runs: list[tuple[int, int]] = []

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            runs.append((off, len(data)))

        async def do_exec() -> None:
            store.armed = True
            await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=SIZE)

        task = asyncio.create_task(do_exec())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert runs == []

    async def test_on_run_calls_reconstruct_the_correct_data_bytes(self, dedup_file: DedupFile) -> None:
        """The DATA bytes land at the right offsets in the buffer rebuilt from
        ``on_run`` calls (the run count itself is not part of the contract)."""
        plan = await _reference_plan_chunks(dedup_file, 0, SIZE, 0, write_zero_fill=None)
        out = bytearray(SIZE)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            out[off : off + len(data)] = data

        bytes_run = await _exec_counting(plan, pool=dedup_file._pool, on_run=_on_run, size=SIZE)
        assert bytes_run == 3 * 4096 + 4 * 4096
        assert bytes(out[0:12288]) == b"".join(CHUNK_PLAINTEXTS[0:3])
        assert bytes(out[24576:28672]) == CHUNK_PLAINTEXTS[3]
        assert bytes(out[28672:32768]) == CHUNK_PLAINTEXTS[4]
        assert bytes(out[32768:36864]) == CHUNK_PLAINTEXTS[3]  # repeat
        assert bytes(out[36864:40960]) == CHUNK_PLAINTEXTS[4]  # repeat

    async def test_overlapping_chunk_runs_for_one_bucket_produce_correct_bytes_with_no_redundant_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Record 2's chunks 3-7 are a subset of record 1's chunks 0-7 in one
        bucket: both destinations get correct bytes from a single
        ``BucketReader._read_run`` call."""
        plaintexts = [bytes([i]) * 4096 for i in range(8)]  # bucket 0, chunks 0..7
        entries = mapping_record(0, 0, 0, map_num=8) + mapping_record(  # dest [0,32768): chunks 0-7
            8 * 4096, 0, 3, map_num=5
        )  # dest [32768,53248): chunks 3-7 (subset, overlapping)
        write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", plaintexts)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        size = 13 * 4096
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=size)

        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        out = bytearray(size)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            out[off : off + len(data)] = data

        real_read_run = BucketReader._read_run
        calls: list[object] = []

        async def _tracking_read_run(self: BucketReader, address: object, run: object, result: object) -> None:
            calls.append(run)
            await real_read_run(self, address, run, result)  # type: ignore[arg-type]

        monkeypatch.setattr(BucketReader, "_read_run", _tracking_read_run)
        await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=size)

        assert bytes(out[0:32768]) == b"".join(plaintexts[0:8])
        assert bytes(out[32768:53248]) == b"".join(plaintexts[3:8])
        assert len(calls) == 1


class TestExecChunksMultiBucket:
    """Execution across many buckets: correct bytes, non-overlapping destination
    ranges, exception propagation, cancellation."""

    async def test_matches_expected_output_across_many_buckets(self, tmp_path: Path) -> None:
        num_buckets = 8
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        reference_plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        out = bytearray(size)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            out[off : off + len(data)] = data

        bytes_run = await _exec_counting(
            reference_plan,
            pool=file._pool,
            on_run=_on_run,
            size=size,
        )

        expected = b"".join(b"".join(_multi_bucket_plaintexts(b)) for b in range(num_buckets))
        assert bytes(out) == expected
        assert bytes_run == len(expected)

    async def test_no_two_bucket_groups_ever_report_an_overlapping_destination_range(self, tmp_path: Path) -> None:
        """No two ``on_run`` calls cover overlapping destination ranges."""
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
        for (start_a, end_a), (start_b, end_b) in itertools.pairwise(ranges):
            assert end_a <= start_b, f"overlapping runs: [{start_a},{end_a}) and [{start_b},{end_b})"

    async def test_an_on_run_exception_propagates_out_of_exec_chunks(self, tmp_path: Path) -> None:
        num_buckets = 4
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)

        async def _failing_on_run(off: int, data: bytes | memoryview) -> None:
            raise RuntimeError("simulated worker failure")

        # Default serial path: the plain exception, not an ExceptionGroup.
        with pytest.raises(RuntimeError, match="simulated worker failure"):
            await exec_chunks(plan, pool=file._pool, on_run=_failing_on_run, size=size)

    async def test_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Cancelling while the first bucket group is parked on a read raises
        ``CancelledError`` with ``on_run`` never called."""
        num_buckets = 4
        file, store = _build_blocking_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        runs: list[int] = []

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            runs.append(off)

        async def do_exec() -> None:
            store.armed = True
            await exec_chunks(plan, pool=file._pool, on_run=_on_run, size=size)

        task = asyncio.create_task(do_exec())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert runs == []


class _SelectiveBlockingStore(WrappingStore):
    """Parks only the data read (offset > 0) of one bucket file until
    ``_unblock()``; every header-open read and other bucket passes through."""

    def __init__(self, backing: LocalFsStore, blocked_path: str) -> None:
        super().__init__(backing)
        self._blocked_path = blocked_path
        self.blocked = asyncio.Event()  # set once the targeted read has actually parked
        self._release = asyncio.Event()

    def _unblock(self) -> None:
        self._release.set()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if path == self._blocked_path and offset > 0:
            self.blocked.set()
            await self._release.wait()
        return await self._backing.read(path, offset, length)


def _build_selective_blocking_multi_bucket_dedup_file(
    tmp_path: Path, num_buckets: int, *, blocked_bucket_id: int
) -> tuple[DedupFile, _SelectiveBlockingStore]:
    """``_build_multi_bucket_dedup_file`` over a ``_SelectiveBlockingStore``
    parking only ``blocked_bucket_id``'s data read."""
    bucket_span = _CHUNKS_PER_BUCKET * 4096
    entries = b"".join(mapping_record(b * bucket_span, b, 0, map_num=_CHUNKS_PER_BUCKET) for b in range(num_buckets))
    write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
    for b in range(num_buckets):
        write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", _multi_bucket_plaintexts(b))
    blocked_path = f"Pool/0/{blocked_bucket_id}.buk"
    store = _SelectiveBlockingStore(LocalFsStore(tmp_path), blocked_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, HEAD_OFF, size=num_buckets * bucket_span), store


class TestExecChunksPrefetch:
    """``max_concurrent_opens``: the background bucket-open prefetch (``_prefetch_bucket_opens``)."""

    async def test_output_is_unchanged_with_prefetch_enabled(self, tmp_path: Path) -> None:
        """Output is byte-identical with the prefetch on; it only warms ``export_cache``."""
        num_buckets = 8
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        out = bytearray(size)

        async def _on_run(off: int, data: bytes | memoryview) -> None:
            out[off : off + len(data)] = data

        bytes_run = await _exec_counting(plan, pool=file._pool, on_run=_on_run, size=size, max_concurrent_opens=4)

        expected = b"".join(b"".join(_multi_bucket_plaintexts(b)) for b in range(num_buckets))
        assert bytes(out) == expected
        assert bytes_run == len(expected)

    async def test_prefetches_other_buckets_while_one_buckets_read_is_in_flight(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """While bucket 0's data read is parked, the other buckets' ``open_bucket_uncached()``
        calls have already happened."""
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

        monkeypatch.setattr(Pool, "open_bucket_uncached", _tracking_open_bucket_uncached)
        task = asyncio.create_task(
            _exec_counting(
                plan,
                pool=file._pool,
                on_run=_noop_on_run,
                size=size,
                max_concurrent_reads=1,  # serial reads -- bucket 0 alone would block everything without prefetch
                max_concurrent_opens=4,
            )
        )
        await store.blocked.wait()  # bucket 0's own data read has parked
        # LocalFsStore reads hop through threads, so wait on the condition rather than event-loop ticks.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(all_prefetched.wait(), timeout=5.0)
        assert set(opened) >= {1, 2, 3}, (
            f"expected buckets 1-3 prefetched while bucket 0's read was in flight, got {opened!r}"
        )
        store._unblock()
        bytes_run = await task

        assert bytes_run == num_buckets * _CHUNKS_PER_BUCKET * 4096
        assert opened.count(0) == 1  # prefetch and main loop share one open

    async def test_a_failed_prefetch_open_is_silently_retried_by_the_main_loop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A bucket's first ``open_bucket_uncached`` (the prefetch's) raises; the
        main loop re-opens it and the output is still byte-identical."""
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

        monkeypatch.setattr(Pool, "open_bucket_uncached", _fails_once_for_bucket_2)
        bytes_run = await _exec_counting(plan, pool=file._pool, on_run=_on_run, size=size, max_concurrent_opens=4)

        expected = b"".join(b"".join(_multi_bucket_plaintexts(b)) for b in range(num_buckets))
        assert bytes(out) == expected
        assert bytes_run == len(expected)
        assert call_count == 2  # failed prefetch open, then the main loop's retry

    async def test_an_on_run_exception_still_propagates_unwrapped_with_prefetch_enabled(self, tmp_path: Path) -> None:
        """With the prefetch on, an ``on_run`` failure on the default
        ``max_concurrent_reads=1`` path is still the plain exception, not an ``ExceptionGroup``."""
        num_buckets = 4
        file = _build_multi_bucket_dedup_file(tmp_path, num_buckets)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)

        async def _failing_on_run(off: int, data: bytes | memoryview) -> None:
            raise RuntimeError("simulated worker failure")

        with pytest.raises(RuntimeError, match="simulated worker failure"):
            await exec_chunks(plan, pool=file._pool, on_run=_failing_on_run, size=size, max_concurrent_opens=4)


class _ConcurrencyTrackingStore(WrappingStore):
    """Records the peak number of concurrently in-flight bucket data reads
    (``"Pool/"`` paths at ``offset >= 16384``, past the bucket header's
    reserved region), parking each until
    ``expected_peak`` is reached or a timeout elapses so ``peak`` reflects real overlap."""

    def __init__(self, backing: LocalFsStore, *, expected_peak: int) -> None:
        super().__init__(backing)
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


def _build_split_run_dedup_file(tmp_path: Path, *, expected_peak: int) -> tuple[DedupFile, _ConcurrencyTrackingStore]:
    """Three read sources over two buckets: bucket 0's chunk_idx 0 and 2
    (1 unreferenced, so with ``_GAP_TOLERANCE`` at 0 they are two merge runs)
    and bucket 1's chunk 0. With ``max_concurrent_reads=2`` the two outer
    per-bucket permits exhaust the semaphore, so bucket 0's second run only
    proceeds through the ``exec_chunks``/``read_chunks`` permit hand-off."""
    entries = (
        mapping_record(0, 0, 0, map_num=1)  # bucket 0, chunk_idx 0 -> dest [0, 4096)
        + mapping_record(4096, 0, 2, map_num=1)  # bucket 0, chunk_idx 2 -> dest [4096, 8192), skips chunk_idx 1
        + mapping_record(8192, 1, 0, map_num=1)  # bucket 1, chunk_idx 0 -> dest [8192, 12288)
    )
    write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
    write_bucket(tmp_path / "Pool" / "0" / "0.buk", [bytes([1]) * 4096 for _ in range(3)])  # chunk_idx 1 unreferenced
    write_bucket(tmp_path / "Pool" / "0" / "1.buk", [bytes([2]) * 4096])
    store = _ConcurrencyTrackingStore(LocalFsStore(tmp_path), expected_peak=expected_peak)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, HEAD_OFF, size=12288), store


class TestExecChunksCombinedConcurrencyCeiling:
    """Cross-bucket and in-bucket reads share one ceiling: their combined
    in-flight count never exceeds ``max_concurrent_reads``."""

    async def test_combined_ceiling_is_never_exceeded(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("synology_apm_repo.sdk.dedup.pool._bucket_reader._GAP_TOLERANCE", 0)
        file, store = _build_split_run_dedup_file(tmp_path, expected_peak=2)
        size = file.size
        assert size is not None
        plan = await _reference_plan_chunks(file, 0, size, 0, write_zero_fill=None)
        # Fixture check: 3 ChunkRuns across 2 buckets.
        assert sum(len(runs) for runs in plan.groups.values()) == 3
        assert len(plan.groups) == 2

        bytes_run = await _exec_counting(plan, pool=file._pool, on_run=_noop_on_run, size=size, max_concurrent_reads=2)

        assert bytes_run == size
        assert store.peak <= 2, f"expected at most 2 concurrent reads, observed a peak of {store.peak}"
        assert store.peak == 2, "expected the 3 available sources to actually reach the ceiling, not undershoot it"


def test_ascending_runs_groups_consecutive_indices() -> None:
    assert ascending_runs([]) == []
    assert ascending_runs([0]) == [(0, 1)]
    assert ascending_runs([0, 1, 2, 5, 6, 9]) == [(0, 3), (5, 2), (9, 1)]
