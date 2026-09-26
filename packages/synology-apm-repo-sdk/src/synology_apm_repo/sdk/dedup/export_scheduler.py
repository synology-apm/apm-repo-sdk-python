"""Export execution: turns a ``DedupFile``/``ByteRangeView``'s own extents
into bytes on a real destination file.

Groups ``DATA`` chunks by ``(stream_id, bucket_id)`` via ``chunk_walk`` and
fetches each bucket's chunks with one merged ``BucketReader.read_chunks``
call instead of one ``Pool.read_chunk`` per chunk, since dedup means
logical offset order and Pool address order rarely match. When this
repository's store can be reconstructed in a fresh process
(``dedup.pool_descriptor.PoolDescriptor.from_pool``), each ``DATA``
group's decode+write moves to a ``ProcessPoolExecutor`` for real
multi-core parallelism (single-process decode stays under CPython's GIL);
``_ExportSink`` always handles gap/zero-fill writes in-process. See
``export_to`` for the concurrency knobs and ``_walk_bucket_major`` for
this windowed path's one accepted cost.
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
"""Backpressure cap for ``_ExportSink``'s writer thread, bounding worst-case
memory if write ever falls behind decode (normally the slower side)."""


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
    """``os.pwrite()`` where available (POSIX); Windows lacks positional
    write, so this falls back to ``lseek``+``write`` — safe since every
    caller already serializes access to ``fd`` (one writer thread per
    process)."""
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
    goes through one dedicated writer thread's ``os.pwrite`` on one open
    fd — ``pwrite``'s explicit-offset semantics make concurrent,
    non-monotonic bucket-group writes safe, and decoupling write from
    decode lets the two overlap.

    ``bytes_written``/``progress`` update on hand-off, not on-disk
    confirmation; ``close`` returning without raising means every
    handed-off write has completed. No lock needed — each of
    ``bytes_written``/``_error`` is touched by exactly one side (event
    loop vs. writer thread).

    ``dst_offset`` (default 0) shifts every write's destination by a
    constant, applied at the last step before ``pwrite``. Its one real
    caller, ``pcps_disk``, assembles several disk-absolute-addressed
    PC/PS fragments into one combined sparse disk image.
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
                match item:
                    case _DataJob():
                        # offsets are chunk-aligned (FORMAT-SPEC.md: pcps-fragments)
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
        """Bookkeeping-only counterpart to ``write_data`` for the
        multiprocess path: a worker process already ``pwrite``s its own
        bytes directly, so this only updates ``bytes_written``/reports
        progress, without touching the writer thread. Still checks
        ``self._error`` first so an earlier writer-thread failure surfaces
        here too."""
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
        """Hand ``job`` to the writer thread — usually a plain
        ``put_nowait()`` (non-blocking); falls back to
        ``asyncio.to_thread()`` only when the queue (see
        ``_WRITER_QUEUE_SIZE``) is actually full, so a blocking put never
        stalls the event loop."""
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
    """One window's multiprocess dispatch: each ``(stream_id, bucket_id)``
    group becomes one ``export_bucket_group_worker`` task via
    ``concurrency.dispatch_to_pool``. The worker already ``pwrite``s its
    own bytes, so ``sink`` only records the byte count here — gap/zero-fill
    writes still go through its writer thread via ``sink.write_gap``.
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
    """Feed ``sink`` every DATA/HOLE/ZERO region in ``[window_start,
    window_end)``, DATA chunks grouped bucket-major — see ``chunk_walk``
    for the plan/exec mechanics. Returns ``(holes, zeros)`` byte totals.

    ``progress`` is left ``None`` on the underlying ``exec_chunks`` call:
    its ``planned_total`` resets every window, so progress is instead
    reported through ``sink.bytes_written``, which persists across
    windows.

    ``executor`` (``None``: no multiprocess dispatch available) selects
    the in-process ``exec_chunks`` fallback per window, or the
    multiprocess dispatch path when given. Gap/zero writes always go
    through ``sink.write_gap`` regardless.

    A known, accepted cost of the multiprocess path: the same bucket,
    independently re-referenced at two far-apart points in the file, can
    land in two different windows and be opened/decoded twice —
    impossible in the single-process fallback, which shares one
    ``BucketReaderCache`` across the whole export.
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
    """The one export entry point for a ``DedupFile`` or a
    ``ByteRangeView`` window — groups ``DATA`` chunks bucket-major via
    ``chunk_walk``. ``DedupFile.export_to``/``ByteRangeView.export_to``
    delegate here.

    Args:
        progress: Reports real ``DATA`` bytes written against the range's
            real ``DATA`` total (never holes/zeros); skipped when
            ``None``.
        max_concurrent_reads: Governs the in-process fallback path only
            (default 1: serial) — the multiprocess path's concurrency is
            set by ``concurrency.default_worker_count()`` instead. See
            ``exec_chunks`` for the semaphore mechanics.
        max_concurrent_opens: Prefetches upcoming bucket headers ahead of
            the main loop on the fallback path (default ``None``:
            auto-derived as ``1 + max_concurrent_reads``; pass ``1`` to
            disable prefetch). See ``_prefetch_bucket_opens``.
        export_cache: A private ``BucketReaderCache`` for fallback-path
            reads, kept separate from ``Pool``'s own shared cache (default
            ``None``: created here, bounded to
            ``DEFAULT_BUCKET_CACHE_SIZE``, discarded after the call). Pass
            a shared instance across multiple calls for one logical
            export to keep bucket-locality reuse.
        executor: The multiprocess dispatch, used whenever this
            repository's store can be reconstructed in a fresh process
            (default ``None``: built here and torn down after the call;
            pass one you already own — see ``concurrency.new_process_pool``
            — to share it across multiple calls for one logical export).
        dst_offset: Shifts every write's destination by a constant
            (default 0). Its real caller, ``pcps_disk``, combines several
            PC/PS fragments into one sparse disk image.
        create: Skips creating ``dst`` when ``False`` — for a caller that
            already created it at a larger combined size and calls this
            once per fragment into the same file (default ``True``).
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

    resolved_max_concurrent_opens = 1 + max_concurrent_reads if max_concurrent_opens is None else max_concurrent_opens

    window_end = window_start + size
    planned_total = await count_planned_bytes(base, window_start, window_end) if progress else 0

    if create:
        await asyncio.to_thread(create_presized, dst, size, sparse=sparse)
    fd = await asyncio.to_thread(open_destination, dst)
    sink = _ExportSink(fd, sparse=sparse, planned_total=planned_total, progress=progress, dst_offset=dst_offset)

    # A caller-supplied executor is trusted as-is; only when building our
    # own do we need to probe describability first.
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
            # rather than being torn down mid-write. shutdown(wait=True)
            # blocks the calling thread for as long as that takes (measured
            # ~1.4s for one slow worker) — wrapped in to_thread so it
            # doesn't freeze this process's event loop.
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)
        # Runs on asyncio.CancelledError too, so a cancelled export still
        # closes its partial destination file.
        await asyncio.to_thread(os.close, fd)

    return ExportResult(bytes_written=sink.bytes_written, logical_size=size, holes=holes, zeros=zeros)


# -- Multiprocess worker: one bucket group, one task ---------------------
#
# Dispatches one (stream_id, bucket_id) group per task for real multi-core
# decode parallelism, reusing chunk_walk._exec_one_bucket_group unmodified
# (a deliberate module-private cross-import, so this path doesn't need its
# own copy of the decode/write sequence).


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
    worker's own ``Pool`` and opens its long-lived destination fd once per
    process. No ``os.O_TRUNC``: the parent already pre-sized ``dst_path``
    (``presized_file.create_presized``) before any worker starts, and each
    worker writes only its own disjoint offsets via ``os.pwrite``."""
    global _worker_store, _worker_pool, _worker_dst_fd
    _worker_store, _worker_pool = build_worker_pool(pool_descriptor)
    _worker_dst_fd = open_destination(dst_path)
    # Registered last, only once every worker-global above is actually set,
    # so a shutdown hook never runs against half-initialized state.
    atexit.register(_export_worker_shutdown)


def _export_worker_shutdown() -> None:
    """Runs once at this worker process's normal exit (registered by
    ``_export_worker_init``) — releases ``_worker_store``, closes the
    persistent event loop, then the destination fd."""
    run_in_worker_loop(aclose_worker_store(_worker_store))
    close_worker_loop()
    if _worker_dst_fd is not None:
        os.close(_worker_dst_fd)


def build_export_executor(descriptor: PoolDescriptor, dst_path: str) -> ProcessPoolExecutor:
    """Factory for an executor this module's worker functions know how to
    serve — for a caller (e.g. ``VirtualDiskContentSource.export_to()``'s
    multi-fragment fan-out) sharing one executor across several
    ``export_to()`` calls instead of building its own each time."""
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

    # Fresh per-task BucketReaderCache: a bucket can be submitted more than
    # once across windows (see _walk_bucket_major's accepted duplicate-decode
    # cost), but the two occurrences never need to share a cache entry.
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
    # asyncio.run() would close its loop each call, breaking _worker_pool's
    # cached client (bound to the loop that first ran it) — use this
    # worker's persistent loop instead.
    return run_in_worker_loop(_export_bucket_group_worker_async(args))
