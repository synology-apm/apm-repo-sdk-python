"""Export execution: turns a ``DedupFile``/``ByteRangeView``'s own extents
into bytes on an ``ExportWriter``.

Groups ``DATA`` chunks by ``(stream_id, bucket_id)`` via ``chunk_walk`` and
fetches each bucket's chunks with one merged ``BucketReader.read_chunks``
call instead of one ``Pool.read_chunk`` per chunk, since dedup means
logical offset order and Pool address order rarely match. When the store
can be rebuilt in a fresh process (``PoolDescriptor.from_pool``) and the
writer offers a ``worker_target``, each ``DATA`` group's decode+write can
move to a ``ProcessPoolExecutor`` for multi-core parallelism (single-process
decode stays under CPython's GIL; ``_WorkerPoolChoice`` decides per
window; the worker side is ``export_workers``); gap/zero-fill writes always
go through the writer in-process. ``ExportTuning`` holds the concurrency
knobs.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from typing import TYPE_CHECKING

from ..concurrency import (
    common_descriptor,
    default_worker_count,
    dispatch_to_pool,
    raise_worker_failure,
)
from .chunk_walk import (
    DEFAULT_WINDOW_ENTRIES,
    ChunkPlan,
    exec_chunks,
    plan_chunks_windowed,
)
from .export_sink import (
    ExportWriter,
    OffsetWriter,
    WorkerTarget,
    WrittenBytesCallback,
    needs_zero_fill,
)
from .export_workers import ExportExecutor, ExportGroupWorkerArgs, export_bucket_group_worker
from .extent import ExportResult
from .pool import BucketReaderCache, Pool
from .pool_descriptor import PoolDescriptor

if TYPE_CHECKING:
    from .dedup_file import DedupContent, DedupFile


@dataclasses.dataclass(frozen=True, slots=True)
class ExportTuning:
    """Concurrency and caching knobs for one export; the defaults suit a
    normal one.

    Attributes:
        window_entries: Chunks per planning window (see
            ``plan_chunks_windowed``).
        max_concurrent_opens: Prefetches upcoming bucket headers ahead of
            the main loop on the in-process path (``None``: auto-derived as
            ``1 + max_concurrent_reads``; ``1`` disables prefetch). See
            ``_prefetch_bucket_opens``.
        max_concurrent_reads: Concurrency of the in-process path only
            (default 1: serial) — the multiprocess path's is
            ``concurrency.default_worker_count()``. See ``exec_chunks``.
        export_cache: A private ``BucketReaderCache`` for in-process reads,
            kept separate from ``Pool``'s shared one (``None``: created per
            call). Pass one instance across the calls of one logical export
            to keep bucket-locality reuse.
        executor: The multiprocess workers. ``None`` lets the call build
            its own when it can, and close it afterwards (see
            ``_WorkerPoolChoice``). A caller-supplied one serves every
            window, is left open, and must match both the file's repository
            and the writer's destination.
    """

    window_entries: int = DEFAULT_WINDOW_ENTRIES
    max_concurrent_opens: int | None = None
    max_concurrent_reads: int = 1
    export_cache: BucketReaderCache | None = None
    executor: ExportExecutor | None = None


class _ExportAccounting:
    """Sits between the walk and an ``ExportWriter``: counts the real ``DATA``
    bytes written and reports them as progress, and applies the sparse
    policy to gap/zero-fill writes.

    ``bytes_written``/``progress`` update on hand-off to the writer, not on
    durable confirmation.
    """

    def __init__(
        self,
        writer: ExportWriter,
        *,
        write_zeros: bool,
        progress: WrittenBytesCallback | None,
    ) -> None:
        self._writer = writer
        self._write_zeros = write_zeros
        self._progress = progress
        self.bytes_written = 0

    async def write_data(self, dest_offset: int, payload: bytes | memoryview) -> None:
        await self._writer.write_at(dest_offset, payload)
        await self.record_external_bytes(len(payload))

    async def record_external_bytes(self, nbytes: int) -> None:
        """Bookkeeping-only counterpart to ``write_data`` for the
        multiprocess path: a worker process already wrote its own bytes
        directly, so this only updates ``bytes_written`` and reports
        progress."""
        self.bytes_written += nbytes
        if self._progress is not None:
            await self._progress(nbytes)

    def note_worker_write(self) -> None:
        self._writer.note_worker_write()

    async def write_gap(self, dest_offset: int, length: int) -> None:
        if self._write_zeros:
            await self._writer.write_zero(dest_offset, length)


async def _dispatch_window_multiprocess(
    plan: ChunkPlan, *, executor: ExportExecutor, size: int, dst_offset: int, accounting: _ExportAccounting
) -> None:
    """One window's multiprocess dispatch: each ``(stream_id, bucket_id)``
    group becomes one ``export_bucket_group_worker`` task via
    ``concurrency.dispatch_to_pool``. The worker writes its own bytes, so
    ``accounting`` only records each group's byte count.
    """

    async def _on_result(_args: ExportGroupWorkerArgs, written: int) -> None:
        await accounting.record_external_bytes(written)

    items = [
        ExportGroupWorkerArgs(stream_id=key[0], bucket_id=key[1], runs=runs, size=size, dst_offset=dst_offset)
        for key, runs in sorted(plan.groups.items())
    ]
    if items:
        accounting.note_worker_write()
    await dispatch_to_pool(
        executor.process_pool,
        export_bucket_group_worker,
        items,
        max_concurrent=default_worker_count(),
        on_result=_on_result,
    )


async def _walk_bucket_major(
    base: DedupFile,
    pool: Pool,
    window_start: int,
    window_end: int,
    accounting: _ExportAccounting,
    tuning: ExportTuning,
    *,
    workers: _WorkerPoolChoice,
    dst_offset: int,
) -> tuple[int, int]:
    """Feed ``accounting`` every DATA/HOLE/ZERO region in ``[window_start,
    window_end)``, DATA chunks grouped bucket-major — see ``chunk_walk``
    for the plan/exec mechanics. Returns ``(holes, zeros)`` byte totals.

    Each window is dispatched to worker processes when ``workers`` picks an
    executor for its plan, else run in-process via ``exec_chunks``. On the
    multiprocess path a bucket referenced in two windows is opened and
    decoded twice; the in-process path shares one ``BucketReaderCache``
    across windows.
    """
    size = window_end - window_start
    export_cache = tuning.export_cache
    if export_cache is None:
        export_cache = BucketReaderCache()
    max_concurrent_opens = tuning.max_concurrent_opens
    if max_concurrent_opens is None:
        max_concurrent_opens = 1 + tuning.max_concurrent_reads
    holes = zeros = 0
    async for plan in plan_chunks_windowed(
        base,
        window_start,
        window_end,
        window_start,
        write_zero_fill=accounting.write_gap,
        max_entries=tuning.window_entries,
    ):
        holes += plan.holes
        zeros += plan.zeros
        executor = workers.for_plan(plan)
        if executor is not None:
            try:
                await _dispatch_window_multiprocess(
                    plan, executor=executor, size=size, dst_offset=dst_offset, accounting=accounting
                )
            except BaseExceptionGroup as group:
                raise_worker_failure(group, operation="export")
        else:
            await exec_chunks(
                plan,
                pool=pool,
                on_run=accounting.write_data,
                size=size,
                max_concurrent_opens=max_concurrent_opens,
                max_concurrent_reads=tuning.max_concurrent_reads,
                export_cache=export_cache,
            )
    return holes, zeros


_MIN_WORKER_GROUPS = 8
"""Fewest bucket groups (one worker task each) a window must touch for worker processes to
repay the pool's start-up; a window below it runs in-process."""


class _WorkerPoolChoice:
    """Which worker pool, if any, runs each window of one export.

    A caller-supplied executor serves every window. Otherwise a pool is built by the first
    window whose plan touches ``_MIN_WORKER_GROUPS`` bucket groups, reused by every later
    window and closed with the export; a window with fewer groups runs in-process. No pool
    is built when the store cannot be rebuilt in a worker or the writer takes no worker writes.

    Raises:
        ValueError: ``given`` was built for another repository or destination.
    """

    def __init__(
        self, pool_descriptor: PoolDescriptor | None, target: WorkerTarget | None, given: ExportExecutor | None
    ) -> None:
        if given is not None and not given.accepts(pool_descriptor, target):
            raise ValueError(
                "executor was not built for this file's own repository/vault, or for this call's own destination: "
                "its worker processes are bound to one repository and one destination when they spawn, and are "
                "never re-initialized per task"
            )
        self._pool_descriptor = pool_descriptor
        self._target = target
        self._executor = given
        self._owned = False

    def for_plan(self, plan: ChunkPlan) -> ExportExecutor | None:
        """The executor to dispatch ``plan`` to, or ``None`` to run it in-process."""
        if (
            self._executor is None
            and self._pool_descriptor is not None
            and self._target is not None
            and len(plan.groups) >= _MIN_WORKER_GROUPS
        ):
            self._executor = ExportExecutor(self._pool_descriptor, self._target.descriptor)
            self._owned = True
        return self._executor

    async def close(self) -> None:
        """Closes the pool this export built; a caller-supplied executor stays open."""
        if self._owned and self._executor is not None:
            await self._executor.close()


async def export_to_writer(
    file_like: DedupContent,
    writer: ExportWriter,
    *,
    sparse: bool = True,
    progress: WrittenBytesCallback | None = None,
    tuning: ExportTuning | None = None,
) -> ExportResult:
    """The export entry point for a ``DedupFile`` or ``ByteRangeView``:
    writes the range into ``writer``, which the caller has opened at (at least)
    the range's size and will commit or abort.
    ``DedupFile.export_range``/``ByteRangeView.export_range`` delegate here.

    A failure inside a worker process is raised as itself (a further failure
    is noted on it), and a worker that died as ``WorkerProcessError``; only the
    opt-in concurrent in-process path (``ExportTuning.max_concurrent_reads
    > 1``) raises an ``ExceptionGroup``.

    Args:
        file_like: The file or view to export.
        writer: The destination, opened by the caller.
        sparse: Leave ``ZERO``/``HOLE`` ranges unwritten. Ignored (zeros are
            written) for a writer whose ``caps.supports_sparse`` is ``False``;
            a dense export writes nothing for them either when the writer is
            ``preallocated``.
        progress: Async callback receiving each newly written count of real
            ``DATA`` bytes, never holes/zeros.
        tuning: Concurrency and caching knobs (default ``ExportTuning()``).

    Returns:
        The export's byte totals.

    Raises:
        ValueError: ``tuning.executor`` was not built for this file's
            repository and this writer's destination.
        WorkerProcessError: A worker process died.
    """
    tuning = tuning or ExportTuning()
    base, window_start, size = file_like.export_window()
    pool = base.pool

    window_end = window_start + size
    accounting = _ExportAccounting(writer, write_zeros=needs_zero_fill(writer, sparse=sparse), progress=progress)

    target = writer.worker_target()
    workers = _WorkerPoolChoice(PoolDescriptor.from_pool(pool), target, tuning.executor)
    try:
        holes, zeros = await _walk_bucket_major(
            base,
            pool,
            window_start,
            window_end,
            accounting,
            tuning,
            workers=workers,
            dst_offset=target.base_offset if target is not None else 0,
        )
    finally:
        await workers.close()

    return ExportResult(bytes_written=accounting.bytes_written, logical_size=size, holes=holes, zeros=zeros)


async def export_fragments_to_writer(
    fragments: Sequence[tuple[DedupFile, int, int]],
    writer: ExportWriter,
    *,
    span: tuple[int, int] | None = None,
    sparse: bool = True,
    progress: WrittenBytesCallback | None = None,
    tuning: ExportTuning | None = None,
) -> ExportResult:
    """Exports each ``(file, start, end)`` fragment, in order, into ``writer``
    (opened by the caller) through ``export_to_writer``, each at its own
    ``start`` offset. ``tuning``'s concurrency applies within a fragment.

    The fragments share one ``BucketReaderCache`` (``tuning.export_cache`` if
    given) and, when every fragment's file resolves to the identical
    ``PoolDescriptor`` and the writer takes worker writes, one multiprocess
    executor, built up front whatever the fragments' size (``tuning.executor``
    if given and left open); otherwise each fragment's export decides for
    itself. ``progress`` receives each newly written count of real ``DATA``
    bytes.

    Args:
        fragments: Disjoint ``(file, start, end)`` pieces, each inside ``span``.
        writer: The destination, opened by the caller.
        span: The ``[start, end)`` range being exported. Offsets written to
            ``writer`` are relative to its start, and the gaps between
            fragments are holes of the result, zero-filled where
            ``needs_zero_fill``. ``None`` exports the fragments alone, at
            their own offsets.

    Returns:
        The fragments' summed byte totals; ``logical_size`` is ``span``'s
        length, or the fragments' summed length without a ``span``.

    Raises:
        ValueError: ``tuning.executor`` was not built for a fragment's
            repository and this writer's destination, or a fragment lies
            outside ``span``.
    """
    origin = span[0] if span is not None else 0
    if span is not None and any(start < span[0] or end > span[1] for _, start, end in fragments):
        raise ValueError(f"fragments must lie inside span {span}")
    tuning = tuning or ExportTuning()
    export_cache = tuning.export_cache
    if export_cache is None:
        export_cache = BucketReaderCache()
    executor = tuning.executor
    owns_executor = executor is None
    if executor is None:
        pool_descriptor = common_descriptor([PoolDescriptor.from_pool(file.pool) for file, _, _ in fragments])
        target = writer.worker_target()
        if pool_descriptor is not None and target is not None:
            executor = ExportExecutor(pool_descriptor, target.descriptor)
    shared = dataclasses.replace(tuning, export_cache=export_cache, executor=executor)

    bytes_written = holes = zeros = size = 0
    try:
        for file, start, end in fragments:
            result = await export_to_writer(
                file.view(start, end - start),
                OffsetWriter(writer, start - origin),
                sparse=sparse,
                progress=progress,
                tuning=shared,
            )
            bytes_written += result.bytes_written
            holes += result.holes
            zeros += result.zeros
            size += result.logical_size
        if span is not None:
            gaps = _gaps_within(span, fragments)
            if gaps and needs_zero_fill(writer, sparse=sparse):
                for gap_start, gap_end in gaps:
                    await writer.write_zero(gap_start - origin, gap_end - gap_start)
            holes += sum(gap_end - gap_start for gap_start, gap_end in gaps)
            size = span[1] - span[0]
    finally:
        if owns_executor and executor is not None:
            await executor.close()
    return ExportResult(bytes_written=bytes_written, logical_size=size, holes=holes, zeros=zeros)


def _gaps_within(span: tuple[int, int], fragments: Sequence[tuple[DedupFile, int, int]]) -> list[tuple[int, int]]:
    """The ``[start, end)`` ranges of ``span`` no fragment covers."""
    gaps: list[tuple[int, int]] = []
    pos = span[0]
    for _, start, end in sorted(fragments, key=lambda fragment: fragment[1]):
        if start > pos:
            gaps.append((pos, start))
        pos = max(pos, end)
    if pos < span[1]:
        gaps.append((pos, span[1]))
    return gaps
