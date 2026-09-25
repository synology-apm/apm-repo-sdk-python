"""The shared chunk-map walking engine behind bucket-major
(physical-order) export.

``plan_chunks_windowed`` (Pass 1) groups each ``DATA`` chunk placement in
a range by the ``(stream_id, bucket_id)`` it physically lives in, as
compact ``ChunkRun``\\ s rather than per-chunk entries; ``exec_chunks``
(Pass 2) visits those groups bucket-major and decodes each unique
chunk once, writing through a caller-supplied
``on_run(dest_offset, data)`` callback rather than a hardcoded
``os.pwrite()``.

**This module is the one deliberate exception to ``DedupFile._extents()``
being private**: every call here carries its own ``# noqa: SLF001``
because bucket-major planning genuinely needs the chunk-native fields
(``addr``/``map_num``/``repeat``) that ``DedupFile.read`` deliberately
never exposes. Any *other* module reaching for ``_extents()`` wants
plaintext for a byte window (``read``/``stream``), not a raw
chunk-map record.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass

from ..format.addressing import ChunkAddress
from ..format.const import BUCKET_MAX_CHUNK_NUM, FIXED_CHUNK_LENGTH
from ..identifiers import BucketId, ChunkIdx, StreamId
from .dedup_file import DedupFile, Extent, ExtentKind
from .pool import DEFAULT_BUCKET_CACHE_SIZE, BucketReaderCache, Pool


@dataclass(frozen=True)
class ChunkRun:
    """A maximal run of physically-contiguous chunks within one bucket:
    ``chunk_idx_start``, ``chunk_idx_start + 1``, ..., ``chunk_idx_start +
    length - 1``, mapping to ``dest_offset_start``, ``dest_offset_start +
    FIXED_CHUNK_LENGTH``, ... in destination order.

    Preserves a ``ChunkMapKind.MAPPING`` record's own compactness (an address
    template of ``map_num`` chunks replayed ``1 + repeat`` times,
    FORMAT-SPEC.md: ChunkMapRecord) instead of expanding every replay into a
    per-chunk entry — collapses millions of chunk placements into a run
    count several orders of magnitude smaller. Plain ``@dataclass``, not a
    hot-path ``NamedTuple`` like ``ChunkAddress``
    — there are only thousands of these per export, not millions.
    """

    chunk_idx_start: int
    length: int
    dest_offset_start: int


def _iter_chunk_runs(
    addr: ChunkAddress, map_num: int, first_k: int, last_k: int
) -> Iterator[tuple[ChunkAddress, int, int]]:
    """Yield ``(start_addr, run_length, k_of_start)`` for each maximal
    contiguous run within ``k in [first_k, last_k]`` — splitting only at a
    repeat-cycle wraparound (``k % map_num == 0``, i.e. the Pool address
    jumping back to the template start — FORMAT-SPEC.md: ChunkMapRecord) or a
    ``bucket_id`` carry (``ChunkAddress.advance``'s own carry
    semantics).
    """
    k = first_k
    while k <= last_k:
        cycle_offset = k % map_num
        start_addr = addr.advance(cycle_offset)
        room_in_cycle = map_num - cycle_offset
        room_in_bucket = BUCKET_MAX_CHUNK_NUM - start_addr.chunk_idx
        # advance() called once per yielded run, not once per chunk: the
        # run length is plain arithmetic over how much room is left in
        # the cycle/bucket/caller window, never a chunk-by-chunk walk.
        run_len = min(room_in_cycle, room_in_bucket, last_k - k + 1)
        # advance()'s own contract always normalizes chunk_idx into
        # [0, BUCKET_MAX_CHUNK_NUM), so room_in_bucket (and room_in_cycle,
        # from cycle_offset < map_num) can never be <= 0 here — this
        # would otherwise move k *backward* forever instead of raising.
        assert run_len > 0, f"non-advancing chunk run at k={k} (run_len={run_len}) — this would loop forever"
        yield start_addr, run_len, k
        k += run_len


def _validate_window_start(window_start: int, start: int, *, fn_name: str) -> None:
    """Precondition for ``plan_chunks_windowed``: ``window_start`` must
    be chunk-aligned and ``<= start``, or every ``dest_offset`` derived
    from it would be misaligned or negative."""
    if window_start % FIXED_CHUNK_LENGTH != 0:
        # The write side always pads an object's write to a 4096-byte
        # boundary, so this should never actually trip on real data —
        # defense-in-depth, not a live gap.
        raise ValueError(
            f"{fn_name}() requires a chunk-aligned window_start, got {window_start} "
            f"(not a multiple of {FIXED_CHUNK_LENGTH})"
        )
    if window_start > start:
        raise ValueError(
            f"{fn_name}() requires window_start ({window_start}) <= start ({start}) — "
            "a chunk positioned before window_start but still inside [start, end) would "
            "get a negative dest_offset otherwise"
        )


def _data_extent_k_bounds(extent: Extent, start: int, end: int) -> tuple[int, int]:
    """``(first_k, last_k)``, the inclusive chunk-index bounds of a
    ``DATA`` extent clipped to ``[start, end)`` — the boundary rule every
    ``DATA``-extent walk in this module shares: a chunk overlapping
    ``[start, end)`` is planned/counted in full, only a chunk entirely
    outside it is skipped."""
    seg_start = max(extent.offset, start)
    seg_end = min(extent.end, end)
    return (seg_start - extent.offset) // FIXED_CHUNK_LENGTH, (seg_end - 1 - extent.offset) // FIXED_CHUNK_LENGTH


async def _handle_gap_extent(
    extent: Extent,
    start: int,
    end: int,
    window_start: int,
    write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
) -> tuple[int, int]:
    """Clips a ``HOLE``/``ZERO`` extent to ``[start, end)``, invoking
    ``write_zero_fill(local_offset, length)`` when given one, and
    returns ``(holes_delta, zeros_delta)`` for the caller to add to its
    own running totals — ``(0, 0)`` when the clipped span is empty."""
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


@dataclass(frozen=True)
class ChunkPlan:
    """Pass 1's output: every ``DATA`` chunk placement in the walked
    range, grouped by the ``(stream_id, bucket_id)`` it actually lives in
    — ready for ``exec_chunks`` to visit bucket-major. ``holes``/
    ``zeros`` are byte totals only (their zero-fill, if wanted, is already
    written by ``plan_chunks_windowed`` itself during the same ``extents()``
    walk that groups the ``DATA`` chunks, rather than deferred to a
    separate pass that would have to re-walk or re-store every
    ``ZERO``/``HOLE`` extent for no benefit)."""

    groups: dict[tuple[StreamId, BucketId], list[ChunkRun]]
    holes: int
    zeros: int


@dataclass(frozen=True)
class _GapDelta:
    """One ``_walk_extents`` event: a clipped ``HOLE``/``ZERO`` extent's
    own ``(holes, zeros)`` byte delta — ``(0, 0)`` for an empty clip."""

    holes: int
    zeros: int


@dataclass(frozen=True)
class _DataRun:
    """One ``_walk_extents`` event: one maximal, unsplit contiguous chunk
    run from a ``DATA`` extent (see ``_iter_chunk_runs``), plus the
    ``(stream_id, bucket_id)`` group it belongs in."""

    key: tuple[StreamId, BucketId]
    run: ChunkRun


async def _walk_extents(
    base: DedupFile,
    start: int,
    end: int,
    window_start: int,
    write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
) -> AsyncIterator[_GapDelta | _DataRun]:
    """The ``extents()`` walk ``plan_chunks_windowed`` builds on, factored
    out into its own generator so a test-only unwindowed reference oracle
    can build on the identical walk instead of a second,
    independently-maintained copy of it.

    ``HOLE``/``ZERO`` extents are resolved immediately (clipped to
    ``[start, end)``, ``write_zero_fill`` invoked if given) and yielded as
    one ``_GapDelta`` each. A ``DATA`` extent yields one
    ``_DataRun`` per maximal contiguous run ``_iter_chunk_runs``
    finds, unsplit — ``plan_chunks_windowed`` is the one that further
    slices a run's own length against its ``max_entries`` budget.
    """
    async for extent in base._extents(start, end):  # noqa: SLF001 - bucket-major planning needs the chunk-native addr/map_num/repeat fields DedupFile.read never exposes
        if extent.kind is ExtentKind.HOLE or extent.kind is ExtentKind.ZERO:
            holes_delta, zeros_delta = await _handle_gap_extent(extent, start, end, window_start, write_zero_fill)
            yield _GapDelta(holes_delta, zeros_delta)
            continue

        local_off = extent.offset - window_start
        assert extent.addr is not None and extent.map_num > 0
        first_k, last_k = _data_extent_k_bounds(extent, start, end)
        for start_addr, run_len, k_start in _iter_chunk_runs(extent.addr, extent.map_num, first_k, last_k):
            dest_offset = local_off + k_start * FIXED_CHUNK_LENGTH
            key = (start_addr.stream_id, start_addr.bucket_id)
            yield _DataRun(key, ChunkRun(start_addr.chunk_idx, run_len, dest_offset))


DEFAULT_WINDOW_ENTRIES = 1 << 20
"""One window bounds ``max_entries`` total *chunks* (not runs — see
``plan_chunks_windowed``), regardless of how few ``ChunkRun``\\ s
that expands to — a run-based plan for a real 32 GiB VM's ~2.5M chunks
costs a few hundred KB across every bucket group, not the ~20 MB a flat
per-chunk array would, but is still unbounded in principle for however
large a range a caller hands it. ``plan_chunks_windowed`` bounds peak
plan memory to one window's worth of chunks regardless of total range
size."""


async def plan_chunks_windowed(
    base: DedupFile,
    start: int,
    end: int,
    window_start: int,
    *,
    write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
    max_entries: int = DEFAULT_WINDOW_ENTRIES,
) -> AsyncIterator[ChunkPlan]:
    """One ``extents()`` walk: ``DATA`` chunks are grouped as
    ``ChunkRun``\\ s for ``exec_chunks``; ``ZERO``/``HOLE``
    regions are resolved immediately — calling ``write_zero_fill(local_offset,
    length)`` if given (the caller's job to decide whether that means
    writing actual zero bytes or doing nothing, e.g. a sparse destination
    that's already zero-filled), or just counted if ``write_zero_fill``
    is ``None``. Yields a ``ChunkPlan`` every ``max_entries`` *chunks*
    (counting each ``ChunkRun``'s own ``length``, not 1 per run) so
    peak plan memory is bounded regardless of how compactly the runs
    themselves happen to pack. Always yields at least one plan, possibly
    empty (a range that is entirely ``HOLE``/``ZERO``). A window boundary
    may fall mid-run: a run that would cross ``max_entries`` is split at
    the boundary, its own remainder carried into the next window.

    ``ZERO``/``HOLE`` extents are clipped to ``[start, end)``; a boundary
    ``DATA`` extent is not, because chunks are the atomic decode unit —
    any chunk overlapping ``[start, end)`` is decoded in full, only a
    chunk entirely outside it is skipped (same ``first_k``/``last_k``
    bound as ``_fill_data_extent``).

    ``window_start`` must be a multiple of ``FIXED_CHUNK_LENGTH`` and
    ``<= start``, or this raises ``ValueError`` — a non-aligned
    ``window_start`` would silently misalign every ``dest_offset``, and
    one past ``start`` makes an included chunk's ``dest_offset``
    negative.
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


async def count_planned_bytes(base: DedupFile, start: int, end: int) -> int:
    """How many bytes ``plan_chunks_windowed`` would plan across
    ``[start, end)`` — i.e. the ``DATA`` extents'
    total *expanded* (``repeat``-multiplied) chunk count times
    ``FIXED_CHUNK_LENGTH``, the exact same number
    ``exec_chunks`` computes as its own ``planned_total`` from a
    single (unwindowed) plan's ``groups``.

    A second, O(1)-memory ``extents()`` walk purely for this count (never
    building any placement array). Windowed planning's own per-window
    ``planned_total`` resets at every window boundary, so a progress bar
    built on it would look like it restarts partway through; the progress
    denominator has to be the planned work across the *whole* range. This
    extra walk is cheap relative to the decode pass it makes accurate.
    """
    total_chunks = 0
    async for extent in base._extents(start, end):  # noqa: SLF001
        if extent.kind is ExtentKind.DATA:
            assert extent.map_num > 0
            # Same first_k/last_k bound as plan_chunks_windowed() — a
            # boundary chunk outside [start, end) isn't planned there, so
            # counting it here would inflate the progress-bar denominator
            # relative to what actually gets decoded.
            first_k, last_k = _data_extent_k_bounds(extent, start, end)
            total_chunks += last_k - first_k + 1
    return total_chunks * FIXED_CHUNK_LENGTH


_MAX_MERGED_RUN = 8 << 20
"""Cap one merged *write* run at 8 MiB. Bounds memory for a
pathologically long contiguous run and keeps a single ``on_run`` call
from covering long enough a span to meaningfully hurt cancellation
latency — a stalled local ``os.pwrite()`` is a lower-latency-risk
operation than a stalled network read, which is why ``pool``'s merged
*reads* carry no equivalent cap (bounded instead by a bucket's own
chunk index space). A single ``ChunkRun`` can itself exceed this (a whole
bucket's ``map_num`` can be up to 8192 chunks, 32 MiB) — the merge loop
in ``_exec_one_bucket_group`` slices such a run into ≤8 MiB pieces
at this exact cap; since it's a multiple of ``FIXED_CHUNK_LENGTH``, a
split never lands mid-chunk."""


async def _flush_run(
    on_run: Callable[[int, bytes | memoryview], Awaitable[None]], run_start: int, run_buf: bytearray, size: int
) -> int:
    if not run_buf:
        return 0
    write_len = min(len(run_buf), size - run_start)
    if write_len <= 0:
        return 0
    # A memoryview slice never copies (unlike a bytearray slice, which
    # always does, even when write_len == len(run_buf)) — ``run_buf`` is
    # never touched again after this call returns (the caller immediately
    # rebinds its own ``run_buf`` name to a fresh bytearray for the next
    # run), so handing off a view instead of a copy is safe. ``.toreadonly()``
    # is free (same buffer, no copy) and turns "nobody should mutate this
    # after handoff" from an assumption into something a stray write
    # actually fails on.
    await on_run(run_start, memoryview(run_buf)[:write_len].toreadonly())
    return write_len


def _merge_overlapping_ranges(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """Collapse ``(chunk_idx_start, length)`` ranges that overlap or touch
    into their union, sorted ascending by start.

    Needed because two *different* ``ChunkRun``\\ s for the same
    bucket can genuinely reference overlapping-but-not-identical physical
    ranges: dedup means the same Pool bytes can be referenced from more
    than one destination extent, each becoming its own independent
    chunk-map record with its own ``(chunk_idx_start, length)`` — nothing
    guarantees two *different* ranges stay disjoint. Left unmerged, a
    later-sorted range starting inside an earlier one already covered
    makes the flattened ``chunk_idx`` sequence jump backward mid-bucket,
    which ``BucketReader._fits_in_run``'s
    gap check (``0 <= gap <= _GAP_TOLERANCE``) rejects outright — forcing
    a spurious extra read for bytes an earlier run already fetched, and
    per-request TTFB (not data volume) is what that actually costs.

    Also guarantees ``_exec_one_bucket_group``'s resulting flat
    per-chunk ``requests`` list has strictly increasing ``chunk_idx``
    values, the property ``BucketReader.read_chunks`` requires but never
    re-derives itself — an out-of-order or duplicate sequence there would
    surface as a wrong/inefficient merge, not an error.
    """
    merged: list[tuple[int, int]] = []
    for start, length in sorted(ranges):
        end = start + length
        # A repeat region's exact-duplicate range (FORMAT-SPEC.md:
        # ChunkMapRecord) merges here too, for free, since it trivially unions
        # with itself — not handled as a separate dedup-by-tuple case.
        if merged and start <= merged[-1][0] + merged[-1][1]:
            prev_start, prev_length = merged[-1]
            merged[-1] = (prev_start, max(prev_length, end - prev_start))
        else:
            merged.append((start, length))
    return merged


async def _merge_and_flush_writes(
    runs: list[ChunkRun],
    cache: dict[int, bytes | memoryview],
    *,
    on_run: Callable[[int, bytes | memoryview], Awaitable[None]],
    size: int,
    on_bytes: Callable[[int], Awaitable[None]],
) -> None:
    """The destination-side half of ``_exec_one_bucket_group``'s job:
    merges dest-offset-contiguous chunks (up to ``_MAX_MERGED_RUN``)
    already decoded in ``cache`` and flushes each merged run via
    ``on_run``. Deliberately re-derives its own dest-offset order from
    ``runs`` (not the read order the caller decoded ``cache`` in) — dedup
    means those two orders genuinely disagree within one bucket, since a
    bucket can serve chunks from widely separated regions of the file's
    logical layout."""
    dest_offset_major = sorted(runs, key=lambda r: r.dest_offset_start)
    run_start = -1
    run_buf = bytearray()
    for run in dest_offset_major:
        # Chunk-granularity, not byte-granularity: every chunk is exactly
        # FIXED_CHUNK_LENGTH and _MAX_MERGED_RUN is an exact multiple of
        # it, so a cap-triggered flush always lands on a chunk boundary —
        # appending straight from ``cache`` into ``run_buf`` here means each
        # chunk's bytes get copied once (into run_buf), not twice (once
        # into a throwaway ``b"".join(...)`` of the whole run, then again
        # out of that into run_buf).
        for i in range(run.length):
            chunk = cache[run.chunk_idx_start + i]
            dest_offset = run.dest_offset_start + i * FIXED_CHUNK_LENGTH
            if run_buf and dest_offset == run_start + len(run_buf) and len(run_buf) < _MAX_MERGED_RUN:
                run_buf += chunk
                continue
            flushed = await _flush_run(on_run, run_start, run_buf, size)
            if flushed:
                await on_bytes(flushed)
            run_start = dest_offset
            run_buf = bytearray(chunk)
    flushed = await _flush_run(on_run, run_start, run_buf, size)
    if flushed:
        await on_bytes(flushed)


async def _exec_one_bucket_group(
    stream_id: StreamId,
    bucket_id: BucketId,
    runs: list[ChunkRun],
    *,
    pool: Pool,
    on_run: Callable[[int, bytes | memoryview], Awaitable[None]],
    size: int,
    on_bytes: Callable[[int], Awaitable[None]],
    export_cache: BucketReaderCache,
    semaphore: asyncio.Semaphore | None = None,
) -> None:
    """One ``(stream_id, bucket_id)`` group's whole Pass-2 job: merged
    reads, each unique chunk decoded once, then destination-side merged
    runs flushed via ``on_run``.

    ``runs`` is visited twice on purpose — once in ``chunk_idx`` order for
    reads, once in ``dest_offset`` order for writes — since dedup means
    those two orders genuinely disagree within one bucket (a bucket can
    serve chunks from widely separated regions of the file's logical
    layout); a single shared order would break the write-side merge.

    ``export_cache`` is used for the ``BucketReader``, never ``Pool``'s
    own shared ``_buckets``, so this bucket-major sweep doesn't evict
    ``Pool``'s hot interactive entries. ``semaphore`` is passed straight
    through to ``BucketReader.read_chunks`` unmodified, which handles the
    caller's pre-acquired-permit hand-off itself.
    """
    key = (stream_id, bucket_id)
    reader = await export_cache.buckets.resolve(key, pool.open_bucket_uncached_by_key)

    # Collapse every reference (repeat *or* dedup-overlap) to a given
    # physical range into one decode before expanding into the flat
    # per-chunk request list read_chunks() needs — chunk sizes are
    # genuinely per-chunk (no run-length compression at the SizeStore
    # level). A repeat region can reference the same physical range more
    # than once at different dest_offset_start values (FORMAT-SPEC.md:
    # ChunkMapRecord), and two *different* ChunkRuns can reference genuinely
    # overlapping (not just identical) ranges via ordinary dedup — both
    # collapse here; left unmerged, a later range starting inside an
    # earlier one already covered would force a spurious, avoidable extra
    # read. One write per destination occurrence still happens below
    # regardless of how many times a range merged.
    read_order = _merge_overlapping_ranges((run.chunk_idx_start, run.length) for run in runs)

    # ChunkAddress is only ever dereferenced by read_chunks()/_decode_run()
    # inside their own is_vault_encrypted branch (it supplies the AES-CTR
    # IV) — a non-encrypted bucket, the common case, never touches it at
    # all. Skipping the construction here rather than after the fact:
    # this loop runs once per physical chunk this bucket group actually
    # reads (millions of times across a large export), and building
    # None is cheaper here than a real ChunkAddress.
    build_addr = reader.header.is_vault_encrypted
    requests: list[tuple[int, ChunkAddress | None]] = []
    for chunk_idx_start, length in read_order:
        for i in range(length):
            chunk_idx = chunk_idx_start + i
            addr = ChunkAddress(stream_id, bucket_id, ChunkIdx(chunk_idx)) if build_addr else None
            requests.append((chunk_idx, addr))

    cache: dict[int, bytes | memoryview] = await reader.read_chunks(requests, semaphore=semaphore)
    # read_chunks() bypasses Pool.read_chunk() entirely, so the
    # fingerprint-verification policy that method would normally apply
    # never runs unless called explicitly here.
    await pool.verify_fingerprints(stream_id, bucket_id, cache)

    # dest_offset-major — deliberately re-derived from ``runs`` itself (not
    # ``read_order``), since a repeated/overlapping range needs one write
    # per destination occurrence even though it was decoded only once
    # above.
    await _merge_and_flush_writes(runs, cache, on_run=on_run, size=size, on_bytes=on_bytes)


async def _prefetch_bucket_opens(
    groups: list[tuple[StreamId, BucketId]],
    *,
    pool: Pool,
    export_cache: BucketReaderCache,
    max_concurrent_opens: int,
) -> None:
    """Best-effort background warm-up for ``exec_chunks``'s main
    consumption loop: walks ``groups`` in the same ascending order that
    loop visits them in, calling the same ``export_cache.buckets.resolve()``
    ahead of time so each bucket's open (one small header+SizeStore GET)
    has already landed, or is in flight, by the time
    ``_exec_one_bucket_group`` reaches that group — instead of sitting
    in front of that group's own, much larger, read on the critical path.

    Has no correctness role, only a latency-hiding one — a failed open
    here is silently dropped and simply re-attempted by the main loop's
    own call right after, exactly as if this prefetch had never run.

    Bounded by its own ``max_concurrent_opens``, independent of
    ``exec_chunks``'s ``max_concurrent_reads``: opening is a small,
    read-only, non-CPU-bound GET with none of the AES-CTR/GIL contention
    that keeps read concurrency conservative by default, so it's safe to
    run noticeably more of these at once than full bucket-group
    executions — though still bounded, not unbounded, to avoid bursting
    every group's open at once against a backend that may rate-limit a
    sudden spike.
    """
    semaphore = asyncio.Semaphore(max_concurrent_opens)

    async def _open_one(key: tuple[StreamId, BucketId]) -> None:
        async with semaphore:
            # Exception, not BaseException: a real fetch failure (NotFoundError,
            # DataCorruptError, a network error, ...) is fine to swallow here —
            # this prefetch has no correctness role, only a latency-hiding one,
            # and the main loop below simply retries the open itself — but
            # asyncio.CancelledError must keep propagating normally so this task (and the
            # TaskGroup below) still cancels promptly when exec_chunks'
            # own ``finally`` cancels it.
            #
            # AsyncKeyedCache.resolve() deletes the in-flight entry on
            # failure rather than storing the exception, so a swallowed
            # failure here never poisons the cache for the main loop's
            # own resolve() call on the same key.
            with contextlib.suppress(Exception):
                await export_cache.buckets.resolve(key, pool.open_bucket_uncached_by_key)

    async with asyncio.TaskGroup() as tg:
        for key in groups:
            tg.create_task(_open_one(key))


async def exec_chunks(
    plan: ChunkPlan,
    *,
    pool: Pool,
    on_run: Callable[[int, bytes | memoryview], Awaitable[None]],
    size: int,
    export_cache: BucketReaderCache | None = None,
    progress: Callable[[int, int], Awaitable[None]] | None = None,
    max_concurrent_opens: int = 1,
    max_concurrent_reads: int = 1,
) -> int:
    """Pass 2: visits buckets ascending, chunks ascending within each,
    decoding each unique chunk once. Two independent merges happen around
    decode: the source side merges adjacent chunks' still-compressed byte
    ranges into one ``ObjectStore.read`` per ``BucketReader.read_chunks``;
    the destination side merges consecutive ``dest_offset`` chunks into
    one buffer per ``on_run`` call (see ``_exec_one_bucket_group``).

    Args:
        max_concurrent_reads: The single knob for every kind of read
            concurrency this function produces, cross-bucket or in-bucket
            (default 1: serial) — one ``asyncio.Semaphore`` sized to it is
            shared by the cross-bucket dispatch loop below and by each
            bucket's own in-bucket fan-out inside
            ``BucketReader.read_chunks``, where the caller's pre-acquired
            permit covers that call's first merged run and only an
            additional run within the same bucket acquires a further
            permit from the same pool. ``on_run`` must tolerate concurrent,
            out-of-order calls at scattered offsets when this is ``> 1``
            (``_ExportSink``'s ``os.pwrite`` already does).
        max_concurrent_opens: Runs ``_prefetch_bucket_opens`` in the
            background to warm ``export_cache`` ahead of the dispatch loop
            (default 1: off) — orthogonal to ``max_concurrent_reads``,
            since it only ever touches bucket opens, never
            ``on_run``/decode/write.
        export_cache: Default ``None`` creates one here (bounded to
            ``DEFAULT_BUCKET_CACHE_SIZE``, the same 16-bucket cap ``Pool``
            itself defaults to, reused here as one shared constant instead
            of a separately hardcoded copy), discarded at the end of this
            call — share one explicitly across a whole batch via
            ``export_to`` instead.
            Safe to pass to every concurrent
            ``_exec_one_bucket_group`` call unmodified: ``plan.groups``
            keys each task on its own distinct ``(stream_id, bucket_id)``,
            so two concurrent groups never touch the same cache entry.

    Note:
        Cancellation is native ``asyncio.CancelledError``, arriving at the
        next ``await``; with ``max_concurrent_reads > 1`` **and** more than
        one bucket group in ``plan``, a ``TaskGroup`` supervises the
        concurrent groups, so a caller-visible exception then arrives
        wrapped in an ``ExceptionGroup`` rather than as the original
        exception type — a real shape difference from the single-group/
        ``max_concurrent_reads=1`` path that an opted-in caller must be
        ready for.
    """
    if export_cache is None:
        export_cache = BucketReaderCache(maxsize=DEFAULT_BUCKET_CACHE_SIZE)
    bytes_run = 0
    planned_total = sum(run.length for arr in plan.groups.values() for run in arr) * FIXED_CHUNK_LENGTH

    # No lock: even with several bucket groups in flight, each on_bytes()
    # call's ``bytes_run += flushed`` never spans an await, so it's atomic
    # with respect to asyncio's single-threaded cooperative scheduling —
    # nothing else can observe or mutate bytes_run mid-increment.
    async def on_bytes(flushed: int) -> None:
        nonlocal bytes_run
        bytes_run += flushed
        if progress is not None:
            await progress(bytes_run, planned_total)

    groups = sorted(plan.groups)

    prefetch_task: asyncio.Task[None] | None = None
    if max_concurrent_opens > 1 and len(groups) > 1:
        # Plain create_task(), not folded into a TaskGroup below: a failed
        # open in this task is swallowed internally by _open_one's own
        # contextlib.suppress, so this task never raises and must never be
        # able to change either consumption path's own exception shape —
        # in particular, the serial path's real exception must stay
        # un-wrapped, and only the max_concurrent_reads > 1 path's own
        # TaskGroup should ever produce an ExceptionGroup.
        prefetch_task = asyncio.create_task(
            _prefetch_bucket_opens(
                groups, pool=pool, export_cache=export_cache, max_concurrent_opens=max_concurrent_opens
            )
        )
    try:
        if max_concurrent_reads <= 1 or len(groups) <= 1:
            # Plain serial loop rather than TaskGroup(max_concurrent_reads=1):
            # not just because the ordering would be equivalent
            # (Semaphore(1)'s FIFO-ish wakeup already gives the same
            # effective order), but because TaskGroup.__aexit__() always
            # wraps a child task's exception in an ExceptionGroup
            # regardless of how many tasks were ever actually concurrent.
            # Keeping this fast path is what lets max_concurrent_reads=1
            # raise a plain exception, never wrapped in an ExceptionGroup.
            for stream_id, bucket_id in groups:
                await _exec_one_bucket_group(
                    stream_id,
                    bucket_id,
                    plan.groups[(stream_id, bucket_id)],
                    pool=pool,
                    on_run=on_run,
                    size=size,
                    on_bytes=on_bytes,
                    export_cache=export_cache,
                    semaphore=None,
                )
        else:
            semaphore = asyncio.Semaphore(max_concurrent_reads)

            async def _run_bucket_and_release(stream_id: StreamId, bucket_id: BucketId) -> None:
                # Releases the one permit the dispatch loop below acquired
                # for this bucket — held for this whole call (open, every
                # in-bucket run, decode, write), not just its first run.
                # A separate wrapper rather than folded into
                # _exec_one_bucket_group() itself, since that function has
                # no business knowing whether its own semaphore came
                # pre-acquired by a caller one level up.
                #
                # BucketReader.read_chunks() spends this same held permit
                # for its own first merged run for free (no re-acquire),
                # only acquiring additional, independently released
                # permits from the same pool for a second/third/... run
                # when this bucket's requested chunks genuinely land in
                # more than one physically-separate region of its .buk
                # file — so the combined invariant (total concurrently
                # in-flight reads, cross-bucket and in-bucket combined,
                # never exceeds max_concurrent_reads) holds because every
                # acquire, outer or inner, draws from this one pool.
                try:
                    await _exec_one_bucket_group(
                        stream_id,
                        bucket_id,
                        plan.groups[(stream_id, bucket_id)],
                        pool=pool,
                        on_run=on_run,
                        size=size,
                        on_bytes=on_bytes,
                        export_cache=export_cache,
                        semaphore=semaphore,
                    )
                finally:
                    semaphore.release()

            async with asyncio.TaskGroup() as tg:
                for stream_id, bucket_id in groups:
                    # Acquire *before* create_task — bounds how many
                    # bucket tasks exist at once to max_concurrent_reads,
                    # instead of instantiating every group's task (and
                    # coroutine frame) up front and letting them all race
                    # for the same semaphore, which for a range spanning
                    # hundreds or thousands of buckets would mean that
                    # many Tasks live and blocked before the first one
                    # even starts. A pending acquire() here is cancelled
                    # the same as any other pending await, so there's no
                    # window where cancellation is missed just because a
                    # bucket's task hasn't been created yet.
                    await semaphore.acquire()
                    tg.create_task(_run_bucket_and_release(stream_id, bucket_id))
    finally:
        if prefetch_task is not None:
            prefetch_task.cancel()
            with contextlib.suppress(BaseException):
                await prefetch_task
    return bytes_run
