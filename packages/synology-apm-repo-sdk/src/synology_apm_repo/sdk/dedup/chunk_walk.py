"""The shared chunk-map walking engine: bucket-major (physical-order)
export, plus the run splitting and bucket decode ``DedupFile.read`` and
``verify`` reuse.

``plan_chunks_windowed`` (Pass 1) groups each ``DATA`` chunk placement by
the ``(stream_id, bucket_id)`` it physically lives in, as compact
``ChunkRun``\\ s; ``exec_chunks`` (Pass 2) visits those groups
bucket-major and decodes each unique chunk once, writing through a
caller-supplied ``on_run(dest_offset, data)`` callback.

This module is the one deliberate exception to ``DedupFile._extents()``
being private (every call carries ``# noqa: SLF001``): bucket-major
planning needs the chunk-native ``addr``/``map_num``/``repeat`` fields
``DedupFile.read`` never exposes.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..format.addressing import ChunkAddress
from ..format.const import BUCKET_MAX_CHUNK_NUM, FIXED_CHUNK_LENGTH
from ..identifiers import BucketId, ChunkIdx, StreamId
from .extent import DataExtent, ExtentKind, GapExtent
from .pool import BucketReader, BucketReaderCache, Pool

if TYPE_CHECKING:
    from .dedup_file import DedupFile


@dataclass(frozen=True, slots=True)
class ChunkRun:
    """A maximal run of physically-contiguous chunks within one bucket:
    ``chunk_idx_start``, ``chunk_idx_start + 1``, ..., mapping to
    ``dest_offset_start``, ``dest_offset_start + FIXED_CHUNK_LENGTH``, ...
    in destination order.

    Each repetition of a ``ChunkMapKind.MAPPING`` record's address template
    (FORMAT-SPEC.md: ChunkMapRecord) is one run, split again where it crosses
    a bucket boundary (``iter_chunk_runs``).
    """

    chunk_idx_start: int
    length: int
    dest_offset_start: int


def iter_chunk_runs(
    addr: ChunkAddress, map_num: int, first_k: int, last_k: int
) -> Iterator[tuple[ChunkAddress, int, int]]:
    """Yield ``(start_addr, run_length, k_of_start)`` for each maximal
    contiguous run within ``k in [first_k, last_k]``, splitting only where
    the repeat cycle wraps back to the template start (``k % map_num == 0``;
    FORMAT-SPEC.md: ChunkMapRecord) or ``ChunkAddress.advance`` carries into
    the next ``bucket_id``.
    """
    k = first_k
    while k <= last_k:
        cycle_offset = k % map_num
        start_addr = addr.advance(cycle_offset)
        room_in_cycle = map_num - cycle_offset
        room_in_bucket = BUCKET_MAX_CHUNK_NUM - start_addr.chunk_idx
        run_len = min(room_in_cycle, room_in_bucket, last_k - k + 1)
        # advance() keeps chunk_idx in [0, BUCKET_MAX_CHUNK_NUM), so both room_* are > 0.
        assert run_len > 0, f"non-advancing chunk run at k={k} (run_len={run_len}) — this would loop forever"
        yield start_addr, run_len, k
        k += run_len


def _validate_window_start(window_start: int, start: int, *, fn_name: str) -> None:
    """Precondition for ``plan_chunks_windowed``: ``window_start`` is
    chunk-aligned and ``<= start``."""
    if window_start % FIXED_CHUNK_LENGTH != 0:
        # Defense-in-depth: real data is always 4096-byte aligned.
        raise ValueError(
            f"{fn_name}() requires a chunk-aligned window_start, got {window_start} "
            f"(not a multiple of {FIXED_CHUNK_LENGTH})"
        )
    if window_start > start:
        raise ValueError(
            f"{fn_name}() requires window_start ({window_start}) <= start ({start}) — "
            "otherwise a chunk before window_start would get a negative dest_offset"
        )


def data_extent_k_bounds(extent: DataExtent, start: int, end: int) -> tuple[int, int]:
    """``(first_k, last_k)``, the inclusive chunk-index bounds of a
    ``DATA`` extent clipped to ``[start, end)``: a chunk overlapping the
    range is included in full, one entirely outside is skipped."""
    seg_start = max(extent.offset, start)
    seg_end = min(extent.end, end)
    return (seg_start - extent.offset) // FIXED_CHUNK_LENGTH, (seg_end - 1 - extent.offset) // FIXED_CHUNK_LENGTH


async def _handle_gap_extent(
    extent: GapExtent,
    start: int,
    end: int,
    window_start: int,
    write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
) -> tuple[int, int]:
    """Clips a ``HOLE``/``ZERO`` extent to ``[start, end)``, calls
    ``write_zero_fill(local_offset, length)`` if given, and returns the
    ``(holes_delta, zeros_delta)`` byte counts (``(0, 0)`` for an empty clip)."""
    seg_start = max(extent.offset, start)
    seg_end = min(extent.end, end)
    if seg_end <= seg_start:
        return 0, 0
    length = seg_end - seg_start
    local_off = seg_start - window_start
    holes_delta = length if extent.kind is ExtentKind.HOLE else 0
    zeros_delta = length if extent.kind is ExtentKind.ZERO else 0
    if write_zero_fill is not None:
        await write_zero_fill(local_off, length)
    return holes_delta, zeros_delta


@dataclass(frozen=True, slots=True)
class ChunkPlan:
    """Pass 1's output: every ``DATA`` chunk placement in the range,
    grouped by the ``(stream_id, bucket_id)`` it lives in — ready for
    ``exec_chunks`` to visit bucket-major. ``holes``/``zeros`` are byte
    totals only; their zero-fill (if wanted) is already written by
    ``plan_chunks_windowed`` during the same walk."""

    groups: dict[tuple[StreamId, BucketId], list[ChunkRun]]
    holes: int
    zeros: int


@dataclass(frozen=True, slots=True)
class _GapDelta:
    """One ``_walk_extents`` event: a clipped ``HOLE``/``ZERO`` extent's
    byte counts."""

    holes: int
    zeros: int


@dataclass(frozen=True, slots=True)
class _DataRun:
    """One ``_walk_extents`` event: a maximal chunk run from a ``DATA``
    extent (see ``iter_chunk_runs``) and its ``(stream_id, bucket_id)`` group."""

    key: tuple[StreamId, BucketId]
    run: ChunkRun


async def _walk_extents(
    base: DedupFile,
    start: int,
    end: int,
    window_start: int,
    write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
) -> AsyncIterator[_GapDelta | _DataRun]:
    """The extent walk ``plan_chunks_windowed`` builds on: one ``_GapDelta``
    per ``HOLE``/``ZERO`` extent (resolved immediately) and one unsplit
    ``_DataRun`` per maximal run of a ``DATA`` extent."""
    async for extent in base._extents(start, end):  # noqa: SLF001
        if extent.kind is not ExtentKind.DATA:
            holes_delta, zeros_delta = await _handle_gap_extent(extent, start, end, window_start, write_zero_fill)
            yield _GapDelta(holes_delta, zeros_delta)
            continue

        local_off = extent.offset - window_start
        assert extent.map_num > 0
        first_k, last_k = data_extent_k_bounds(extent, start, end)
        for start_addr, run_len, k_start in iter_chunk_runs(extent.addr, extent.map_num, first_k, last_k):
            dest_offset = local_off + k_start * FIXED_CHUNK_LENGTH
            key = (start_addr.stream_id, start_addr.bucket_id)
            yield _DataRun(key, ChunkRun(start_addr.chunk_idx, run_len, dest_offset))


DEFAULT_WINDOW_ENTRIES = 1 << 20
"""Default ``max_entries`` for ``plan_chunks_windowed``, in chunks (not runs)
per window. It bounds a plan's memory however large the range."""


async def plan_chunks_windowed(
    base: DedupFile,
    start: int,
    end: int,
    window_start: int,
    *,
    write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
    max_entries: int = DEFAULT_WINDOW_ENTRIES,
) -> AsyncIterator[ChunkPlan]:
    """One extent walk: ``DATA`` chunks are grouped as ``ChunkRun``\\ s for
    ``exec_chunks``; ``ZERO``/``HOLE`` regions are resolved immediately by
    calling ``write_zero_fill(local_offset, length)`` (the caller decides
    whether that writes zeros or does nothing), or just counted when it is
    ``None``.

    Yields a ``ChunkPlan`` every ``max_entries`` *chunks* (a run counts its
    own ``length``), splitting a run that crosses the boundary and carrying
    the remainder into the next window. Always yields at least one plan,
    possibly empty (a range that is entirely ``HOLE``/``ZERO``).

    ``ZERO``/``HOLE`` extents are clipped to ``[start, end)``; a boundary
    ``DATA`` extent is not, because chunks are the atomic decode unit: any
    chunk overlapping ``[start, end)`` is decoded in full.

    Raises:
        ValueError: ``window_start`` is not a multiple of
            ``FIXED_CHUNK_LENGTH``, or is past ``start``.
    """
    _validate_window_start(window_start, start, fn_name="plan_chunks_windowed")
    groups: dict[tuple[StreamId, BucketId], list[ChunkRun]] = {}
    holes = zeros = 0
    window_entries = 0
    yielded_any = False
    async for event in _walk_extents(base, start, end, window_start, write_zero_fill):
        if isinstance(event, _GapDelta):
            holes += event.holes
            zeros += event.zeros
            continue

        key = event.key
        remaining_addr = ChunkAddress(key[0], key[1], ChunkIdx(event.run.chunk_idx_start))
        remaining_len = event.run.length
        remaining_dest = event.run.dest_offset_start
        while remaining_len > 0:
            room = max_entries - window_entries
            if room <= 0:
                yield ChunkPlan(groups=groups, holes=holes, zeros=zeros)
                yielded_any = True
                groups = {}
                holes = zeros = 0
                window_entries = 0
                continue
            take = min(room, remaining_len)
            groups.setdefault(key, []).append(ChunkRun(remaining_addr.chunk_idx, take, remaining_dest))
            window_entries += take
            remaining_len -= take
            remaining_dest += take * FIXED_CHUNK_LENGTH
            if remaining_len > 0:
                remaining_addr = remaining_addr.advance(take)
    if not yielded_any or groups or holes or zeros:
        yield ChunkPlan(groups=groups, holes=holes, zeros=zeros)


async def iter_bucket_keys(base: DedupFile, start: int, end: int) -> AsyncIterator[tuple[StreamId, BucketId]]:
    """Every ``(stream_id, bucket_id)`` a ``DATA`` chunk of ``[start, end)``
    lives in, in walk order, without building a plan; a key may repeat
    across extents. An extent spanning a whole repeat cycle is resolved from
    its ``map_num``-chunk template once rather than per repetition.
    """
    async for extent in base._extents(start, end):  # noqa: SLF001
        if extent.kind is not ExtentKind.DATA:
            continue
        first_k, last_k = data_extent_k_bounds(extent, start, end)
        if last_k - first_k + 1 >= extent.map_num:
            first_k, last_k = 0, extent.map_num - 1
        previous: tuple[StreamId, BucketId] | None = None
        for start_addr, _, _ in iter_chunk_runs(extent.addr, extent.map_num, first_k, last_k):
            key = (start_addr.stream_id, start_addr.bucket_id)
            if key != previous:
                yield key
                previous = key


async def count_planned_bytes(base: DedupFile, start: int, end: int) -> int:
    """The ``DATA`` bytes ``plan_chunks_windowed`` would plan across
    ``[start, end)``: the ``repeat``-expanded chunk count times
    ``FIXED_CHUNK_LENGTH``.

    An O(1)-memory walk of its own, so a progress denominator covers the
    whole range rather than one planning window.
    """
    total_chunks = 0
    async for extent in base._extents(start, end):  # noqa: SLF001
        if extent.kind is ExtentKind.DATA:
            assert extent.map_num > 0
            first_k, last_k = data_extent_k_bounds(extent, start, end)
            total_chunks += last_k - first_k + 1
    return total_chunks * FIXED_CHUNK_LENGTH


_OnRun = Callable[[int, bytes | memoryview], Awaitable[None]]
"""Receives ``(dest_offset, data)`` for each merged write run."""

_MAX_MERGED_RUN = 8 << 20
"""Cap on one merged *write* run, bounding memory and cancellation latency.
A longer ``ChunkRun`` is sliced at this cap; as a multiple of
``FIXED_CHUNK_LENGTH`` it never splits a chunk. Merged *reads* carry no such
cap."""


async def _flush_run(on_run: _OnRun, run_start: int, run_buf: bytes | bytearray, size: int) -> None:
    write_len = min(len(run_buf), size - run_start)
    if write_len > 0:
        # A view, not a slice: the caller never touches run_buf again.
        await on_run(run_start, memoryview(run_buf)[:write_len].toreadonly())


def merge_overlapping_ranges(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """Collapse ``(chunk_idx_start, length)`` ranges that overlap or touch
    into their union, sorted ascending by start.

    Two ``ChunkRun``\\ s of one bucket can reference overlapping physical
    ranges (dedup). The result also gives ``decode_bucket_chunks`` the
    sorted, duplicate-free ``chunk_idx`` requests ``BucketReader.read_chunks``
    requires.
    """
    merged: list[tuple[int, int]] = []
    for start, length in sorted(ranges):
        end = start + length
        if merged and start <= merged[-1][0] + merged[-1][1]:
            prev_start, prev_length = merged[-1]
            merged[-1] = (prev_start, max(prev_length, end - prev_start))
        else:
            merged.append((start, length))
    return merged


def ascending_runs(chunk_indices: list[int]) -> list[tuple[int, int]]:
    """Sorted, distinct ``chunk_indices`` as ``(start, length)`` runs of
    consecutive indices: ``merge_overlapping_ranges``' output shape, built in
    one linear pass from indices instead of from ranges."""
    runs: list[tuple[int, int]] = []
    start = prev = -2
    for idx in chunk_indices:
        if idx != prev + 1:
            if start >= 0:
                runs.append((start, prev - start + 1))
            start = idx
        prev = idx
    if start >= 0:
        runs.append((start, prev - start + 1))
    return runs


async def _merge_and_flush_writes(
    runs: list[ChunkRun],
    cache: dict[int, bytes | memoryview],
    *,
    on_run: _OnRun,
    size: int,
) -> None:
    """The destination-side half of ``exec_one_bucket_group``: merges
    dest-offset-contiguous chunks (up to ``_MAX_MERGED_RUN``) from ``cache``
    and flushes each merged run via ``on_run``.

    Walks ``runs`` in dest-offset order, not the chunk-index order ``cache``
    was decoded in — under dedup one bucket serves widely separated file
    regions, so the two orders disagree.

    A slice of a run is gathered whole unless it holds a chunk shorter than
    ``FIXED_CHUNK_LENGTH``, which forces chunk-by-chunk placement."""
    dest_offset_major = sorted(runs, key=lambda r: r.dest_offset_start)
    run_start = -1
    parts: list[bytes | memoryview] = []
    run_len = 0

    async def flush() -> None:
        await _flush_run(on_run, run_start, b"".join(parts), size)

    async def extend_or_start_run_at(dest: int) -> None:
        """Keeps the open run when ``dest`` continues it and it has room, else flushes it and starts one at ``dest``."""
        nonlocal run_start, parts, run_len
        if parts and dest == run_start + run_len and run_len < _MAX_MERGED_RUN:
            return
        if parts:
            await flush()
        run_start, parts, run_len = dest, [], 0

    for run in dest_offset_major:
        index = run.chunk_idx_start
        dest = run.dest_offset_start
        remaining = run.length
        while remaining > 0:
            await extend_or_start_run_at(dest)
            take = max(1, min((_MAX_MERGED_RUN - run_len) // FIXED_CHUNK_LENGTH, remaining))
            taken = [cache[i] for i in range(index, index + take)]
            if set(map(len, taken)) == {FIXED_CHUNK_LENGTH}:
                parts.extend(taken)
                run_len += take * FIXED_CHUNK_LENGTH
                dest += take * FIXED_CHUNK_LENGTH
            else:
                for chunk in taken:
                    await extend_or_start_run_at(dest)
                    parts.append(chunk)
                    run_len += len(chunk)
                    dest += FIXED_CHUNK_LENGTH
            index += take
            remaining -= take
    if parts:
        await flush()


async def decode_bucket_chunks(
    reader: BucketReader,
    stream_id: StreamId,
    bucket_id: BucketId,
    ranges: Iterable[tuple[int, int]],
    *,
    pool: Pool,
    semaphore: asyncio.Semaphore | None = None,
) -> dict[int, bytes | memoryview]:
    """Every chunk in ``ranges`` (``(chunk_idx_start, length)``, overlap
    allowed) of ``reader``'s bucket, decoded once each with merged reads and
    ``pool``'s fingerprint policy applied, keyed by chunk index.

    The caller opens ``reader`` from whichever cache it reads through — a
    sweep's private one, or ``Pool``'s own for an interactive read — so this
    never decides what gets cached. ``semaphore`` goes to
    ``BucketReader.read_chunks`` as-is (a permit already acquired for this
    call is spent by its first run).
    """
    decoded = await reader.read_chunks(stream_id, bucket_id, merge_overlapping_ranges(ranges), semaphore=semaphore)
    # read_chunks() bypasses Pool.read_chunk(), so apply its fingerprint policy here.
    await pool.verify_fingerprints(stream_id, bucket_id, decoded)
    return decoded


async def exec_one_bucket_group(
    stream_id: StreamId,
    bucket_id: BucketId,
    runs: list[ChunkRun],
    *,
    pool: Pool,
    on_run: _OnRun,
    size: int,
    export_cache: BucketReaderCache,
    semaphore: asyncio.Semaphore | None = None,
) -> None:
    """One ``(stream_id, bucket_id)`` group's Pass-2 job:
    ``decode_bucket_chunks``, then merged runs flushed via ``on_run``.

    ``runs`` is visited twice: in ``chunk_idx`` order for reads, in
    ``dest_offset`` order for writes (see ``_merge_and_flush_writes``).
    ``export_cache`` holds the ``BucketReader`` so the sweep never evicts
    ``Pool``'s own interactive entries.
    """
    reader = await pool.bucket(stream_id, bucket_id, cache=export_cache)
    cache = await decode_bucket_chunks(
        reader,
        stream_id,
        bucket_id,
        ((run.chunk_idx_start, run.length) for run in runs),
        pool=pool,
        semaphore=semaphore,
    )
    # From runs, not the merged read ranges: a repeated range is written once per destination occurrence.
    await _merge_and_flush_writes(runs, cache, on_run=on_run, size=size)


async def _prefetch_bucket_opens(
    groups: list[tuple[StreamId, BucketId]],
    *,
    pool: Pool,
    export_cache: BucketReaderCache,
    max_concurrent_opens: int,
) -> None:
    """Best-effort warm-up for ``exec_chunks``: opens each bucket of
    ``groups``, in the main loop's visiting order, ahead of
    ``exec_one_bucket_group``.

    A failed open is dropped and retried by the main loop. Concurrency is
    bounded by ``max_concurrent_opens``, independent of
    ``max_concurrent_reads``.
    """
    semaphore = asyncio.Semaphore(max_concurrent_opens)

    async def _open_one(key: tuple[StreamId, BucketId]) -> None:
        async with semaphore:
            # Exception, not BaseException: CancelledError must propagate.
            # resolve() drops a failed entry, so the main loop's retry is clean.
            with contextlib.suppress(Exception):
                await pool.bucket(*key, cache=export_cache)

    async with asyncio.TaskGroup() as tg:
        for key in groups:
            tg.create_task(_open_one(key))


async def exec_chunks(
    plan: ChunkPlan,
    *,
    pool: Pool,
    on_run: _OnRun,
    size: int,
    export_cache: BucketReaderCache | None = None,
    max_concurrent_opens: int = 1,
    max_concurrent_reads: int = 1,
) -> None:
    """Pass 2: visits buckets ascending, chunks ascending within each,
    decoding each unique chunk once (see ``exec_one_bucket_group``).

    Args:
        plan: Pass 1's output from ``plan_chunks_windowed``.
        pool: Source of the ``BucketReader``\\ s and fingerprint policy.
        on_run: Called as ``on_run(dest_offset, data)`` for each merged run,
            with ``dest_offset`` relative to the plan's ``window_start``.
        size: Destination size in bytes; a run is truncated at ``size``.
        max_concurrent_reads: Bound on in-flight reads, cross-bucket and
            in-bucket combined (default 1: serial). ``on_run`` must tolerate
            concurrent, out-of-order calls at scattered offsets when this
            is ``> 1``.
        max_concurrent_opens: Above 1, runs ``_prefetch_bucket_opens`` in
            the background to warm ``export_cache`` (default 1: off).
        export_cache: ``None`` creates one for this call (sized
            ``DEFAULT_LIMITS.bucket_readers``); pass one to share it across calls.

    Note:
        With ``max_concurrent_reads > 1`` and more than one bucket group,
        an exception arrives wrapped in an ``ExceptionGroup``.
    """
    if export_cache is None:
        export_cache = BucketReaderCache()
    groups = sorted(plan.groups)

    def exec_group(stream_id: StreamId, bucket_id: BucketId, semaphore: asyncio.Semaphore | None) -> Awaitable[None]:
        return exec_one_bucket_group(
            stream_id,
            bucket_id,
            plan.groups[(stream_id, bucket_id)],
            pool=pool,
            on_run=on_run,
            size=size,
            export_cache=export_cache,
            semaphore=semaphore,
        )

    prefetch_task: asyncio.Task[None] | None = None
    if max_concurrent_opens > 1 and len(groups) > 1:
        # Not a TaskGroup: that would wrap exceptions in an ExceptionGroup on the serial path too.
        prefetch_task = asyncio.create_task(
            _prefetch_bucket_opens(
                groups, pool=pool, export_cache=export_cache, max_concurrent_opens=max_concurrent_opens
            )
        )
    try:
        if max_concurrent_reads <= 1 or len(groups) <= 1:
            for stream_id, bucket_id in groups:
                await exec_group(stream_id, bucket_id, None)
        else:
            semaphore = asyncio.Semaphore(max_concurrent_reads)

            async def _run_bucket_and_release(stream_id: StreamId, bucket_id: BucketId) -> None:
                # Releases the permit the dispatch loop acquired for this call;
                # read_chunks() spends it on its first run, so in-flight reads
                # never exceed max_concurrent_reads.
                try:
                    await exec_group(stream_id, bucket_id, semaphore)
                finally:
                    semaphore.release()

            async with asyncio.TaskGroup() as tg:
                for stream_id, bucket_id in groups:
                    # Acquire before create_task: bounds live tasks to max_concurrent_reads.
                    await semaphore.acquire()
                    tg.create_task(_run_bucket_and_release(stream_id, bucket_id))
    finally:
        if prefetch_task is not None:
            prefetch_task.cancel()
            # Waits without raising the prefetch's own outcome, while this
            # task's own cancellation still propagates.
            await asyncio.wait({prefetch_task})
