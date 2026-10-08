"""The destination side of an export: where decoded bytes land.

``ExportSink`` is a whole destination with a lifecycle; an ``ExportWriter``
is the write surface ``export_scheduler`` writes through, so an export can
target something other than a local file (a hypervisor disk API, say)
without touching the read/decode side. An export is cut into segments: the
sink hands out one ``SegmentWriter`` per segment and is told when each is
complete. ``RandomAccessExportSink`` (e.g. ``local_file_sink.LocalFileSink``)
is the one-segment case.

Lifecycle belongs to the caller, never to the scheduler: ``run_sink_export``
does ``open`` → body → ``commit`` and, on any failure, ``abort``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable
from typing import Protocol, override

from .._util.closing import close_preserving


@dataclasses.dataclass(frozen=True, slots=True)
class SinkCaps:
    """What a writer can and cannot do.

    Attributes:
        supports_sparse: Ranges never written read back as zero, so the
            scheduler may skip ``HOLE``/``ZERO`` regions when the caller
            asks for a sparse export.
    """

    supports_sparse: bool = True


@dataclasses.dataclass(frozen=True, slots=True)
class AbortOutcome:
    """What ``ExportSink.abort`` left behind.

    Attributes:
        kept: A partial destination was left in place.
        ever_written: At least one write reached the sink before the abort.
    """

    kept: bool
    ever_written: bool


class WorkerWriter(Protocol):
    """A write handle a worker process opens for itself."""

    def write_at(self, offset: int, data: bytes | memoryview) -> None: ...

    def close(self) -> None: ...


class SinkDescriptor(Protocol):
    """A picklable, value-comparable recipe for opening a ``WorkerWriter`` in
    another process. Two descriptors are equal exactly when their writers
    write to the same destination."""

    def open_writer(self) -> WorkerWriter: ...


type WrittenBytesCallback = Callable[[int], Awaitable[None]]
"""Receives each newly written byte count of an export as it is handed to
its writer."""


@dataclasses.dataclass(frozen=True, slots=True)
class WorkerTarget:
    """Where a worker process writes on a sink's behalf.

    Attributes:
        descriptor: How the worker opens its own writer.
        base_offset: Added to every offset the worker writes at.
    """

    descriptor: SinkDescriptor
    base_offset: int = 0


class ExportWriter(Protocol):
    """An offset-addressed write surface for exported bytes: one whole
    destination (``RandomAccessExportSink``) or one segment of it
    (``SegmentWriter``).

    Offsets are relative to the range being exported. Writes may arrive at
    any offset and in any order; a write on a writer that is not open raises
    rather than being dropped. Offsets and lengths are multiples of 4096,
    except a write that ends at the end of its exported range, which may be
    shorter; a sector-addressed sink only needs to check in ``open`` that
    ``logical_size`` is a multiple of its sector size. ``write_at`` and
    ``write_zero`` may return before the bytes are durable; ``commit``
    returning without raising means every handed-off write completed.
    ``write_zero``'s ``length`` is unbounded (a hole can be gigabytes), so
    write it in blocks rather than allocating it at once, and zero whatever
    range it is given.
    """

    @property
    def caps(self) -> SinkCaps: ...

    @property
    def preallocated(self) -> bool:
        """Every byte of the destination is already reserved and reads as
        zero, so a dense export has no zero-fill left to write. Only
        meaningful after ``open``."""
        ...

    async def write_at(self, offset: int, data: bytes | memoryview) -> None: ...

    async def write_zero(self, offset: int, length: int) -> None: ...

    def worker_target(self) -> WorkerTarget | None:
        """Where worker processes may write directly, or ``None`` when every
        write must go through this writer in the parent process."""
        ...

    def note_worker_write(self) -> None:
        """Called before work is handed to worker processes writing through
        ``worker_target``, whose writes never reach ``write_at``/``write_zero``:
        the destination may now hold data, which ``abort`` must report and
        keep accordingly."""
        ...


class SegmentWriter(ExportWriter, Protocol):
    """The writer for one segment, from ``ExportSink.begin_segment``."""

    async def complete(self) -> None:
        """Hands the segment's bytes to the sink, which may flush them after
        this returns; every write to the segment has been made."""
        ...


class ExportSink(Protocol):
    """A destination for one export, fed segment by segment.

    The caller owns the lifecycle (``run_export``): ``open`` creates the
    destination, each segment is written through the writer ``begin_segment``
    returns and then ``complete``d, ``commit`` makes the destination final,
    ``abort`` follows a failure.
    """

    @property
    def segment_size(self) -> int | None:
        """``None`` for one segment spanning the whole export, else the bytes
        per segment, a multiple of 4096. Every segment but the last is
        exactly this long."""
        ...

    async def open(self, logical_size: int, *, sparse: bool) -> None:
        """Creates the destination for an export of ``logical_size`` bytes."""
        ...

    async def begin_segment(self, index: int, start: int, length: int) -> SegmentWriter:
        """The writer for the segment covering ``[start, start + length)`` of
        the export. Waits until the sink can take another segment, which is
        what paces the export. Indices count up from 0 and only one segment
        is open at a time."""
        ...

    async def commit(self) -> None:
        """Completes every handed-off write and makes the destination final."""
        ...

    async def abort(self) -> AbortOutcome:
        """Releases the destination after a failure. Safe to call in any
        state, including before ``open``, after ``commit`` and more than
        once; a repeated call returns the first call's outcome."""
        ...


class RandomAccessExportSink(SegmentWriter, ExportSink):
    """Base for a destination that accepts writes at any offset in any order.
    Its one segment is the whole export and its writer is the sink itself, so
    a subclass implements ``open``, ``commit``, ``abort`` and the
    ``ExportWriter`` write surface. Every write stays in the parent process
    unless the subclass also overrides ``worker_target`` and
    ``note_worker_write``."""

    segment_size: int | None = None

    @override
    async def begin_segment(self, index: int, start: int, length: int) -> SegmentWriter:
        return self

    @override
    async def complete(self) -> None:
        pass

    @override
    def worker_target(self) -> WorkerTarget | None:
        return None

    @override
    def note_worker_write(self) -> None:
        pass


def needs_zero_fill(writer: ExportWriter, *, sparse: bool) -> bool:
    """Whether an export into ``writer`` must write its ``HOLE``/``ZERO`` ranges
    as zeros. A sparse export leaves them unwritten where the writer reads
    unwritten ranges as zero; a dense one writes them, unless the writer
    already reserved the whole destination as zero
    (``ExportWriter.preallocated``), which leaves the same bytes behind."""
    return (not sparse or not writer.caps.supports_sparse) and not writer.preallocated


class SinkLifecycle(Protocol):
    """The ``open``/``commit``/``abort`` an export's caller drives."""

    async def open(self, logical_size: int, *, sparse: bool) -> None: ...

    async def commit(self) -> None: ...

    async def abort(self) -> AbortOutcome: ...


async def run_sink_export[T](
    sink: SinkLifecycle, logical_size: int, *, sparse: bool, body: Callable[[], Awaitable[T]]
) -> T:
    """``open`` → ``body`` → ``commit``, calling ``abort`` and re-raising on any
    failure (cancellation included). An error from ``abort`` never masks the
    original failure.

    Args:
        sink: The destination.
        logical_size: Size passed to ``sink.open``.
        sparse: Passed to ``sink.open``.
        body: Writes into the open sink and returns its result.
    """
    try:
        await sink.open(logical_size, sparse=sparse)
        result = await body()
        await sink.commit()
    except BaseException as exc:
        await close_preserving(exc, [sink.abort])
        raise
    return result


class OffsetWriter:
    """Adds ``base`` (never negative) to every offset written through it, so
    sources that each address their own range can share one writer."""

    def __init__(self, inner: ExportWriter, base: int) -> None:
        if base < 0:
            raise ValueError(f"base must not be negative, got {base}")
        self._inner = inner
        self._base = base

    @property
    def caps(self) -> SinkCaps:
        return self._inner.caps

    @property
    def preallocated(self) -> bool:
        return self._inner.preallocated

    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        await self._inner.write_at(self._base + offset, data)

    async def write_zero(self, offset: int, length: int) -> None:
        await self._inner.write_zero(self._base + offset, length)

    def worker_target(self) -> WorkerTarget | None:
        target = self._inner.worker_target()
        if target is None:
            return None
        return WorkerTarget(target.descriptor, target.base_offset + self._base)

    def note_worker_write(self) -> None:
        self._inner.note_worker_write()
