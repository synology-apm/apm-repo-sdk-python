"""``run_export``: the one way to export a restorable unit's content to an
``ExportSink``."""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Awaitable, Callable

from ..dedup.export_scheduler import ExportTuning
from ..dedup.export_sink import ExportSink, WrittenBytesCallback, run_sink_export
from ..dedup.export_workers import ExportExecutor
from ..dedup.extent import ExportResult
from ..dedup.pool import BucketReaderCache
from ..dedup.pool_descriptor import SupportsWorkerPool
from ..units.base import ContentSource

_SEGMENT_ALIGN = 4096


type ExportProgressCallback = Callable[[int, int], Awaitable[None]]
"""Receives an export's ``(done, total)`` byte counts as data is written."""


async def run_export(
    content: ContentSource,
    sink: ExportSink,
    *,
    sparse: bool = True,
    progress: ExportProgressCallback | None = None,
    tuning: ExportTuning | None = None,
) -> ExportResult:
    """Exports ``content`` into ``sink``: opens the sink at the content's
    size, writes every byte into it, then commits it. On any failure —
    cancellation included — the sink is aborted and the error re-raised;
    call the sink's ``abort()`` again (idempotent) for its ``AbortOutcome``.

    A sink with a ``segment_size`` is fed one segment at a time, each written
    in full and then completed before the next is requested, so the sink
    decides when the export may go on.

    Args:
        content: The content to export.
        sink: The destination; a fresh one, not yet opened.
        sparse: Leave ``ZERO``/``HOLE`` ranges unwritten where the sink
            allows it.
        progress: Receives ``(done, total)`` byte counts as data is written;
            ``total`` is ``content.planned_bytes`` over the whole content,
            which can be less than its size (dedup-backed content counts
            only ``DATA`` bytes).
        tuning: Concurrency knobs for dedup-backed content; ignored by other
            content.

    Returns:
        What was written, and how much of the content was holes or zeros.

    Raises:
        ValueError: The content's size is unknown even after a read, or the
            sink's ``segment_size`` is not a positive multiple of 4096.
    """
    size = content.size
    if size is None:
        # A lazily assembled source only knows its size once built.
        await content.read(0, 0)
        size = content.size
    if size is None:
        raise ValueError("cannot export content whose size is unknown")
    segment_size = sink.segment_size
    if segment_size is not None and (segment_size <= 0 or segment_size % _SEGMENT_ALIGN):
        raise ValueError(f"segment_size must be a positive multiple of {_SEGMENT_ALIGN}, got {segment_size}")

    on_bytes = await _counting_progress(content, size, progress) if progress is not None else None

    async def body() -> ExportResult:
        if segment_size is None:
            writer = await sink.begin_segment(0, 0, size)
            result = await content.export_range(writer, 0, size, sparse=sparse, progress=on_bytes, tuning=tuning)
            await writer.complete()
            return result
        return await _export_segments(content, sink, size, segment_size, sparse, on_bytes, tuning)

    return await run_sink_export(sink, size, sparse=sparse, body=body)


async def _export_segments(
    content: ContentSource,
    sink: ExportSink,
    size: int,
    segment_size: int,
    sparse: bool,
    progress: WrittenBytesCallback | None,
    tuning: ExportTuning | None,
) -> ExportResult:
    """Exports ``content`` one ``segment_size`` slice at a time and totals the
    results. The slices share one reader cache and, when the sink's writers
    take worker writes into one destination and the content can be rebuilt in
    a worker, one pool of worker processes, built for the export and closed
    after it: a pool per slice would spend its start-up on every one."""
    ranges = [(start, min(start + segment_size, size)) for start in range(0, size, segment_size)]
    shared = dataclasses.replace(tuning or ExportTuning(), export_cache=_cache_of(tuning))
    pool_descriptor = (
        content.pool_descriptor()
        if isinstance(content, SupportsWorkerPool) and len(ranges) > 1 and shared.executor is None
        else None
    )

    bytes_written = holes = zeros = 0
    waited = 0.0
    executor: ExportExecutor | None = None
    try:
        for index, (start, end) in enumerate(ranges):
            waiting_since = time.perf_counter()
            writer = await sink.begin_segment(index, start, end - start)
            waited += time.perf_counter() - waiting_since
            if index == 0 and pool_descriptor is not None and (target := writer.worker_target()) is not None:
                executor = ExportExecutor(pool_descriptor, target.descriptor)
                shared = dataclasses.replace(shared, executor=executor)
            result = await content.export_range(
                writer,
                start,
                end,
                sparse=sparse,
                progress=progress,
                tuning=shared,
            )
            await writer.complete()
            bytes_written += result.bytes_written
            holes += result.holes
            zeros += result.zeros
    finally:
        if executor is not None:
            await executor.close()
    return ExportResult(
        bytes_written=bytes_written, logical_size=size, holes=holes, zeros=zeros, sink_wait_seconds=waited
    )


def _cache_of(tuning: ExportTuning | None) -> BucketReaderCache:
    """The caller's reader cache, else a new one: every segment of one export
    reuses the buckets the previous one opened."""
    return tuning.export_cache if tuning is not None and tuning.export_cache is not None else BucketReaderCache()


async def _counting_progress(
    content: ContentSource, size: int, progress: ExportProgressCallback
) -> WrittenBytesCallback:
    """Turns ``export_range``'s per-write byte counts into ``progress``'s
    ``(done, total)``, the total counted once for the whole export."""
    total = await content.planned_bytes(0, size)
    done = 0

    async def on_bytes(written: int) -> None:
        nonlocal done
        done += written
        await progress(done, total)

    return on_bytes
