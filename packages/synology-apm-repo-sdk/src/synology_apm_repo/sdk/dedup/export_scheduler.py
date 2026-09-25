"""Export execution: turns a ``DedupFile``/``ByteRangeView``'s own extents
into bytes on a real destination file.

Dedup means "logical file offset ascending" is very often **not** "Pool
address ascending" — an incremental VM backup's later versions mostly
``INHERIT`` chunks scattered across whichever bucket happened to hold
them at write time. Rather than walk extents in logical order and jump
between hundreds of ``.buk`` files in essentially random order, this
module always groups ``DATA`` chunks by ``(stream_id, bucket_id)`` via
``chunk_walk`` and fetches each bucket's needed chunks with one merged
``BucketReader.read_chunks`` call instead of one ``Pool.read_chunk`` per
chunk. A naive one-chunk-at-a-time path exists only as an independent
correctness oracle in the test suite, never in production.

Every write goes through ``_ExportSink``'s dedicated writer thread when
running in a single process; see ``export_to`` for the
``max_concurrent_reads``/``max_concurrent_opens`` concurrency knobs. When
this repository's store can be reconstructed in a fresh process
(``dedup.pool_descriptor.PoolDescriptor.from_pool``), every ``DATA``
chunk-group's decode+write instead moves to a real
``ProcessPoolExecutor`` — real multi-core parallelism the single-process
path never gets past CPython's GIL for this CPU-bound work — leaving
``_ExportSink`` to handle only gap/zero-fill writes (cheap, not worth
parallelizing) in-process either way. See ``ARCHITECTURE.md``'s
"Async-native, by design" section for the measurement this is based on —
see ``_walk_bucket_major`` for a cost this windowed path specifically,
and only it, accepts.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import dataclasses
import os
import queue
import sys
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from ..concurrency import (
    close_worker_loop,
    default_worker_count,
    dispatch_to_pool,
    new_process_pool,
    run_in_worker_loop,
)
from ..identifiers import BucketId, StreamId
from ..storage.base import ObjectStore
from .chunk_walk import (
    DEFAULT_WINDOW_ENTRIES,
    ChunkPlan,
    ChunkRun,
    _exec_one_bucket_group,
    count_planned_bytes,
    exec_chunks,
    plan_chunks_windowed,
)
from .dedup_file import ByteRangeView, DedupFile, ExportResult
from .pool import DEFAULT_BUCKET_CACHE_SIZE, BucketReaderCache, Pool
from .pool_descriptor import PoolDescriptor, aclose_worker_store, build_worker_pool
from .presized_file import create_presized, open_destination

_WRITER_QUEUE_SIZE = 8
"""Bounded backpressure for ``_ExportSink``'s writer thread — sized
for the *bound*, not the throughput. Decode is consistently the slower
side, so the queue rarely holds more than a couple of runs; this only
exists to cap worst-case memory if write ever falls behind."""


@dataclasses.dataclass(frozen=True)
class _DataJob:
    offset: int
    payload: bytes | memoryview


@dataclasses.dataclass(frozen=True)
class _GapJob:
    offset: int
    length: int


_WriteJob = _DataJob | _GapJob
_SENTINEL = object()


def _pwrite(fd: int, data: bytes | memoryview, offset: int) -> None:
    """``os.pwrite()`` where available (POSIX); Windows has no positional
    write, so this falls back to ``lseek``+``write`` — safe because every
    caller of this already serializes its own access to ``fd``: a single
    dedicated writer thread in the single-process path, or one task at a
    time per worker process (each with its own independent fd) in the
    multiprocess path."""
    if sys.platform != "win32":
        os.pwrite(fd, data, offset)
    else:
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)


def _write_zeros_at(fd: int, offset: int, length: int) -> None:
    block = bytes(1 << 20)
    remaining = length
    pos = offset
    while remaining > 0:
        take = min(remaining, len(block))
        _pwrite(fd, block[:take] if take != len(block) else block, pos)
        pos += take
        remaining -= take


class _ExportSink:
    """The one place the walk touches the destination file: every write
    goes through one dedicated, persistent writer thread's ``os.pwrite``
    on one already-open fd, rather than an inline
    ``asyncio.to_thread(os.pwrite, ...)`` per merged run — bucket-major
    reads land at scattered, non-monotonic destination offsets, which
    ``pwrite``'s explicit-offset, no-shared-file-position semantics
    handle natively (and is what makes concurrent bucket-group writes
    safe at all, with no file-position race between them). Decoupling
    write from decode this way also lets the two genuinely overlap
    (CPU-bound decode vs. disk-bound write), reducing and stabilizing
    total export wall-clock time.

    ``bytes_written``/``progress`` update as soon as a write is *handed
    off* to the writer thread, not once confirmed on disk — a bounded
    lag that only matters for the progress indicator; by the time
    ``close`` returns without raising, every handed-off write has
    genuinely completed, so the final count is exact. No lock is needed:
    ``bytes_written``/``_error`` are each touched by exactly one side
    (the event-loop thread vs. the writer thread).

    ``dst_offset`` (default 0) shifts every write's destination position
    by a constant, applied here at the last step before the real
    ``pwrite`` rather than by feeding a translated ``window_start``
    into ``plan_chunks_windowed`` — that keeps ``window_start``'s own job
    (translating read-side addressing) separate from this write-side
    placement instead of conflating the two. The one real caller is
    ``pcps_disk``, assembling several disk-absolute-addressed PC/PS
    fragments into one combined sparse disk image.
    """

    def __init__(
        self,
        fd: int,
        *,
        sparse: bool,
        planned_total: int,
        progress: Callable[[int, int], Awaitable[None]] | None,
        dst_offset: int = 0,
    ) -> None:
        self._fd = fd
        self._sparse = sparse
        self._planned_total = planned_total
        self._progress = progress
        self._dst_offset = dst_offset
        self.bytes_written = 0
        self._queue: queue.Queue[_WriteJob | object] = queue.Queue(maxsize=_WRITER_QUEUE_SIZE)
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="export-writer", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is _SENTINEL:
                return
            try:
                # dst_offset is added here, not upstream: every value fed
                # into this queue is already chunk-aligned on its own
                # terms (a PC/PS fragment's real disk-absolute start is
                # always a multiple of FIXED_CHUNK_LENGTH, decoded from a
                # chunk-map file_offset that's structurally incapable of
                # being anything else — see FORMAT-SPEC.md: pcps-fragments), so this
                # isn't fixing a misalignment; dst_offset is deliberately
                # applied at this last write-side step, not upstream (see
                # the class docstring's dst_offset paragraph above).
                match item:
                    case _DataJob():
                        _pwrite(self._fd, item.payload, item.offset + self._dst_offset)
                    case _GapJob():
                        _write_zeros_at(self._fd, item.offset + self._dst_offset, item.length)
            except BaseException as exc:
                self._error = exc
                return

    async def write_data(self, dest_offset: int, payload: bytes | memoryview) -> None:
        if self._error is not None:
            raise self._error
        await self._put(_DataJob(dest_offset, payload))
        self.bytes_written += len(payload)
        if self._progress is not None:
            await self._progress(self.bytes_written, self._planned_total)

    async def record_external_bytes(self, nbytes: int) -> None:
        """Bookkeeping-only counterpart to ``write_data``, for the
        multiprocess dispatch path: a worker process already ``pwrite``
        its own bytes directly (this sink has no visibility into that fd
        at all), so this only updates ``bytes_written``/reports progress
        exactly as ``write_data`` would, without touching the writer
        thread/queue. Still checks ``self._error`` first, the same guard
        ``write_data`` applies, so a gap-write failure already observed on
        this sink's own writer thread surfaces here too rather than being
        silently outrun by unrelated multiprocess bookkeeping."""
        if self._error is not None:
            raise self._error
        self.bytes_written += nbytes
        if self._progress is not None:
            await self._progress(self.bytes_written, self._planned_total)

    async def write_gap(self, dest_offset: int, length: int) -> None:
        if not self._sparse:
            if self._error is not None:
                raise self._error
            await self._put(_GapJob(dest_offset, length))

    async def _put(self, job: _WriteJob) -> None:
        """Hand ``job`` to the writer thread. The queue (see
        ``_WRITER_QUEUE_SIZE``) is almost never actually full — so
        the common case is a plain ``put_nowait()`` right here on the
        event-loop thread: a ``queue.Queue`` put is a lock-protected,
        non-blocking operation, not a blocking call that needs to get off
        the event loop. Only the rare case where the queue really is full
        falls back to a genuine ``asyncio.to_thread()`` hop, so a caller
        that would actually block still does so off the event loop
        instead of stalling it."""
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            await asyncio.to_thread(self._queue.put, job)

    async def close(self) -> None:
        """Drain and stop the writer thread. Always safe to call — even
        after an error, so a caller's ``finally`` can call this
        unconditionally before closing the fd."""
        await asyncio.to_thread(self._queue.put, _SENTINEL)
        await asyncio.to_thread(self._thread.join)
        if self._error is not None:
            raise self._error


async def _dispatch_window_multiprocess(
    plan: ChunkPlan, *, executor: ProcessPoolExecutor, size: int, dst_offset: int, sink: _ExportSink
) -> None:
    """One ``plan_chunks_windowed`` window's own multiprocess dispatch:
    every ``(stream_id, bucket_id)`` group in it becomes one
    ``export_bucket_group_worker`` task, via the same
    ``concurrency.dispatch_to_pool`` primitive ``verify_reachable.py``'s
    own multiprocess path uses — a free worker always picks up the next
    not-yet-started group. ``sink.record_external_bytes`` is the
    bookkeeping-only counterpart to ``sink.write_data`` here: the actual
    ``pwrite`` already happened inside the worker's own process, on its
    own fd, so this sink never touches the data itself for these groups —
    only gap/zero-fill writes (``sink.write_gap``, called from
    ``plan_chunks_windowed`` itself) still go through its writer thread.
    """

    async def _on_result(_args: ExportGroupWorkerArgs, written: int) -> None:
        await sink.record_external_bytes(written)

    items = [
        ExportGroupWorkerArgs(stream_id=key[0], bucket_id=key[1], runs=runs, size=size, dst_offset=dst_offset)
        for key, runs in sorted(plan.groups.items())
    ]
    await dispatch_to_pool(
        executor, export_bucket_group_worker, items, max_concurrent=default_worker_count(), on_result=_on_result
    )


async def _walk_bucket_major(
    base: DedupFile,
    pool: Pool,
    window_start: int,
    window_end: int,
    sink: _ExportSink,
    window_entries: int,
    max_concurrent_opens: int,
    max_concurrent_reads: int,
    export_cache: BucketReaderCache,
    *,
    executor: ProcessPoolExecutor | None,
    dst_offset: int,
) -> tuple[int, int]:
    """Feed ``sink`` every DATA/HOLE/ZERO region in
    ``[window_start, window_end)``, DATA chunks grouped by ``(stream_id,
    bucket_id)`` so each bucket's needed chunks are fetched with one
    merged read instead of jumping between buckets in logical-offset
    order — see ``chunk_walk`` for the plan/exec
    mechanics. Returns ``(holes, zeros)`` byte totals. ``export_cache``/
    ``max_concurrent_opens``/``max_concurrent_reads`` are threaded
    straight through to ``exec_chunks``
    unchanged: a prefetch task never spans a window boundary, but a window already
    holds up to ``window_entries`` chunks' worth of bucket groups, plenty
    for it to have real work ahead of the main loop within one window.

    ``exec_chunks``'s own ``progress`` is deliberately left ``None``: its
    ``planned_total`` resets every window, whereas ``sink.bytes_written``
    persists across windows — routing progress through the sink instead
    gives a continuous count for free.

    ``executor`` (``None``: this repository's store isn't describable, or the
    caller decided against multiprocess for some other reason) picks
    which of the two DATA-group dispatch paths runs per window: the
    multiprocess path (real multi-core parallelism for this CPU-bound
    decode work, past what in-process ``asyncio`` concurrency ever gets
    through CPython's GIL) when given, the in-process ``exec_chunks``
    fallback otherwise. Either way, gap/zero
    writes go through ``sink.write_gap`` (called from
    ``plan_chunks_windowed`` itself), never through either dispatch path.

    A known, accepted cost specific to this windowed path: the same
    bucket, independently re-referenced (via internal dedup) at two
    logically-far-apart points in the file, can land in two different
    windows and get opened/decoded twice. Not fixed by this design — the
    duplication comes from genuinely separate references, not a chunk-run
    split across a window boundary, so narrowing the window can't help —
    and structurally impossible in the single-process fallback, which
    shares one ``BucketReaderCache`` across the whole export.
    """
    size = window_end - window_start
    holes = zeros = 0
    async for plan in plan_chunks_windowed(
        base, window_start, window_end, window_start, write_zero_fill=sink.write_gap, max_entries=window_entries
    ):
        holes += plan.holes
        zeros += plan.zeros
        if executor is not None:
            await _dispatch_window_multiprocess(plan, executor=executor, size=size, dst_offset=dst_offset, sink=sink)
        else:
            await exec_chunks(
                plan,
                pool=pool,
                on_run=sink.write_data,
                size=size,
                progress=None,
                max_concurrent_opens=max_concurrent_opens,
                max_concurrent_reads=max_concurrent_reads,
                export_cache=export_cache,
            )
    return holes, zeros


async def export_to(
    file_like: DedupFile | ByteRangeView,
    dst: Path,
    *,
    sparse: bool = True,
    progress: Callable[[int, int], Awaitable[None]] | None = None,
    window_entries: int = DEFAULT_WINDOW_ENTRIES,
    max_concurrent_opens: int | None = None,
    max_concurrent_reads: int = 1,
    export_cache: BucketReaderCache | None = None,
    dst_offset: int = 0,
    create: bool = True,
    executor: ProcessPoolExecutor | None = None,
) -> ExportResult:
    """The one export entry point for a whole ``DedupFile`` or a
    ``ByteRangeView`` window into one — grouping DATA chunks bucket-major
    via ``chunk_walk`` so each bucket's needed chunks are fetched with one
    merged read instead of jumping between hundreds of ``.buk`` files in
    logical-offset order.
    ``DedupFile.export_to``/``ByteRangeView.export_to`` delegate here.

    Progress reports real ``DATA`` bytes written against the range's real
    ``DATA`` total (never holes/zeros), computed once via
    ``count_planned_bytes`` and skipped entirely when nobody's listening.

    ``max_concurrent_reads``/``max_concurrent_opens`` only govern the
    **fallback** path now (this repository's store isn't describable — see
    ``executor``'s own paragraph below) — the real, unconditional
    multiprocess path's cross-bucket concurrency is governed by
    ``concurrency.default_worker_count()`` instead. ``max_concurrent_reads``
    (default 1: serial) is the single knob for every kind of read
    concurrency ``exec_chunks`` can produce on that fallback path,
    cross-bucket or in-bucket — see ``exec_chunks`` for the semaphore
    hand-off mechanics.

    ``max_concurrent_opens`` (default ``None``: auto-derived as ``1 +
    max_concurrent_reads``; an explicit integer, e.g. ``1`` to disable
    prefetch, overrides the derivation) prefetches upcoming buckets'
    headers ahead of the main loop instead of overlapping full
    bucket-group executions on the fallback path — see
    ``_prefetch_bucket_opens`` for the mechanism. Whether
    raising it helps is backend-dependent (a saturated, low-RTT pipe gains
    nothing; a higher-RTT endpoint with spare bandwidth gains the most),
    but the auto-derived default stays on regardless, since raising it is
    never meaningfully slower even when it doesn't help.

    ``export_cache`` (default ``None``: created here, bounded to
    ``DEFAULT_BUCKET_CACHE_SIZE`` and discarded at the end of this call)
    routes every fallback-path bucket-major read through a private
    ``BucketReaderCache`` instead of ``Pool``'s own shared cache — see
    ``BucketReaderCache`` for why. ``VirtualDiskContentSource`` instead
    builds **one** instance (its own separately-bounded default) shared
    across all its fragments, so bucket-locality reuse persists across them
    on that path.

    ``executor`` (default ``None``: build one here, iff this repository's store
    can be reconstructed in a fresh process, and tear it down before
    returning; given: the caller already built one and owns its lifetime
    — see ``concurrency.new_process_pool``) is the real multiprocess
    dispatch this function uses unconditionally whenever it applies. Pass
    a shared one when calling this more than once for one logical
    export (``VirtualDiskContentSource.export_to()``'s own multi-fragment
    loop does exactly this) rather than let each call spin up its own
    pool independently.

    ``dst_offset``/``create`` (default 0/``True``, the single-file
    behavior every other caller relies on) — ``dst_offset`` shifts every
    write's destination position by a constant; its one real caller,
    ``pcps_disk``, assembles several disk-absolute-addressed PC/PS
    fragments into one combined sparse disk image.
    ``create=False`` skips creating ``dst`` (the caller already created
    it at its own, larger, combined size, and calls this once per
    fragment into the *same* file — re-creating it here would destroy
    the previous fragment's writes).
    """
    if isinstance(file_like, ByteRangeView):
        base = file_like.base
        pool = base.pool
        window_start = file_like.offset
        size = file_like.size
    else:
        base = file_like
        pool = file_like.pool
        window_start = 0
        if file_like.size is None:
            raise ValueError("export_to() requires a known size")
        size = file_like.size

    if export_cache is None:
        export_cache = BucketReaderCache(maxsize=DEFAULT_BUCKET_CACHE_SIZE)

    # None (the default) auto-derives from max_concurrent_reads (see the
    # max_concurrent_opens paragraph above); an explicit
    # int (including 1, to disable prefetch) is used as-is.
    resolved_max_concurrent_opens = 1 + max_concurrent_reads if max_concurrent_opens is None else max_concurrent_opens

    window_end = window_start + size
    planned_total = await count_planned_bytes(base, window_start, window_end) if progress else 0

    if create:
        await asyncio.to_thread(create_presized, dst, size, sparse=sparse)
    fd = await asyncio.to_thread(open_destination, dst)
    sink = _ExportSink(fd, sparse=sparse, planned_total=planned_total, progress=progress, dst_offset=dst_offset)

    # A caller-supplied executor is trusted as-is (its own builder already
    # confirmed this repository's store is describable — that's the only reason
    # one exists); only when building our own do we need to probe first.
    owns_executor = executor is None
    if owns_executor:
        pool_descriptor = PoolDescriptor.from_pool(pool)
        if pool_descriptor is not None:
            executor = build_export_executor(pool_descriptor, str(dst))

    try:
        holes, zeros = await _walk_bucket_major(
            base,
            pool,
            window_start,
            window_end,
            sink,
            window_entries,
            resolved_max_concurrent_opens,
            max_concurrent_reads,
            export_cache,
            executor=executor,
            dst_offset=dst_offset,
        )
    except BaseException:
        # Give the writer thread a chance to drain and stop cleanly before
        # the fd closes underneath it, but never let a secondary error from
        # that best-effort drain mask the real failure that got us here.
        with contextlib.suppress(BaseException):
            await sink.close()
        raise
    else:
        await sink.close()
    finally:
        if owns_executor and executor is not None:
            # A cancelled in-flight group finishes its own decode/pwrite
            # rather than being torn down mid-write — the same "a
            # worker's own in-flight unit isn't split" principle
            # _walk_bucket_major's cross-window duplicate-decode cost
            # already accepts, applied here to cancellation instead of
            # windowing. shutdown()
            # itself is a plain blocking call — wait=True means it can
            # block for as long as that in-flight worker takes, which
            # would otherwise freeze this whole process's event loop
            # (every other Task, not just this export) for that whole
            # stretch, measured directly at ~1.4s for one deliberately
            # slow worker with no to_thread() hop.
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)
        # Runs on asyncio.CancelledError too, so a cancelled export still
        # closes its partial destination file.
        await asyncio.to_thread(os.close, fd)

    return ExportResult(bytes_written=sink.bytes_written, logical_size=size, holes=holes, zeros=zeros)


# -- Multiprocess worker: one bucket group, one task ---------------------
#
# This module's own multiprocess path (_dispatch_window_multiprocess above)
# dispatches one `(stream_id, bucket_id)` group per task instead of calling
# `exec_chunks()` in-process — real multi-core parallelism for the CPU-bound
# decode work `exec_chunks()`'s own in-process `asyncio` concurrency never
# actually gets past the GIL. `chunk_walk._exec_one_bucket_group` (a
# module-private cross-import, deliberate: it's factored out of
# `chunk_walk.py` specifically so a second caller like this one can drive
# it without a second, hand-written copy of the same decode/write
# sequence) is reused **unmodified** here.


@dataclasses.dataclass(frozen=True)
class ExportGroupWorkerArgs:
    """One ``(stream_id, bucket_id)`` group's worth of work for
    ``export_bucket_group_worker`` — picklable (``ChunkRun`` is already a
    plain dataclass of ints)."""

    stream_id: StreamId
    bucket_id: BucketId
    runs: list[ChunkRun]
    size: int
    dst_offset: int


_worker_store: ObjectStore | None = None
_worker_pool: Pool | None = None
_worker_dst_fd: int | None = None


def _export_worker_init(pool_descriptor: PoolDescriptor, dst_path: str) -> None:
    """``ProcessPoolExecutor(initializer=...)`` target — builds this
    worker's own ``Pool`` and opens its own long-lived destination fd once
    per worker process, not once per task. No ``os.O_TRUNC``: the parent
    has already pre-sized ``dst_path`` (this module's own
    ``_create_truncated``) before any worker starts, and every worker
    shares that one already-open file, each writing only to its own
    disjoint set of destination offsets via ``os.pwrite`` — the same
    "positional, no shared file-position state" property that already
    lets ``_ExportSink``'s single writer thread serve out-of-order
    bucket-group writes safely in the single-process path."""
    global _worker_store, _worker_pool, _worker_dst_fd
    _worker_store, _worker_pool = build_worker_pool(pool_descriptor)
    _worker_dst_fd = open_destination(dst_path)
    # Registered last, only once every worker-global above is actually set,
    # so a shutdown hook never runs against half-initialized state.
    atexit.register(_export_worker_shutdown)


def _export_worker_shutdown() -> None:
    """Runs once, at this worker process's normal exit (registered by
    ``_export_worker_init``) — releases ``_worker_store`` (see
    ``aclose_worker_store()``) before closing this
    worker's persistent event loop and its destination fd."""
    run_in_worker_loop(aclose_worker_store(_worker_store))
    close_worker_loop()
    if _worker_dst_fd is not None:
        os.close(_worker_dst_fd)


def build_export_executor(descriptor: PoolDescriptor, dst_path: str) -> ProcessPoolExecutor:
    """The one public factory for an executor this module's own worker
    functions (above) know how to serve — for a caller
    (``VirtualDiskContentSource.export_to()``'s own multi-fragment
    fan-out) that wants to share **one** executor across several
    ``export_to()`` calls instead of letting each build
    (and tear down) its own. Kept here, not re-derived by that caller, so
    ``_export_worker_init`` itself stays module-private."""
    return new_process_pool(initializer=_export_worker_init, initargs=(descriptor, dst_path))


async def _export_bucket_group_worker_async(args: ExportGroupWorkerArgs) -> int:
    assert _worker_pool is not None
    assert _worker_dst_fd is not None
    dst_fd = _worker_dst_fd
    bytes_written = 0

    async def _on_run(offset: int, payload: bytes | memoryview) -> None:
        nonlocal bytes_written
        _pwrite(dst_fd, payload, offset + args.dst_offset)
        bytes_written += len(payload)

    async def _on_bytes(_flushed: int) -> None:
        pass  # this worker's own progress is reported by its caller, from the returned byte count.

    # A fresh, per-task BucketReaderCache: unlike verify's own worker (one
    # bucket per task, so a bucket-lifetime cache would never be reused
    # anyway), export's own per-window dispatch can, in principle, submit
    # the same bucket more than once across different windows -- the
    # accepted cross-window duplicate-decode cost _walk_bucket_major
    # documents -- nothing here needs those two
    # occurrences to share a cache entry, so a fresh one per
    # task is both correct and simpler than threading a worker-lifetime
    # one through.
    await _exec_one_bucket_group(
        args.stream_id,
        args.bucket_id,
        args.runs,
        pool=_worker_pool,
        on_run=_on_run,
        size=args.size,
        on_bytes=_on_bytes,
        export_cache=BucketReaderCache(),
    )
    return bytes_written


def export_bucket_group_worker(args: ExportGroupWorkerArgs) -> int:
    """The multiprocess path's per-bucket-group work item — returns the
    real number of bytes this group actually wrote (its caller's own
    progress-reporting figure)."""
    # asyncio.run() closes its loop when this one call returns, which would
    # break _worker_pool's cached client (bound to whichever loop first ran
    # it) on the very next call in this same worker process -- run against
    # this worker's own persistent loop instead.
    return run_in_worker_loop(_export_bucket_group_worker_async(args))
