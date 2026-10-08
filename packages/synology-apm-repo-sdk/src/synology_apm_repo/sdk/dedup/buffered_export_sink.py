"""``BufferedExportSink``: the base for a destination that accepts only linear
writes (a zip entry, an upload stream, the parts of a multipart upload).

The exporter writes in physical, not logical, order, so it cannot feed such a
destination directly. This sink takes each segment of the export in a buffer
it owns, where writes may land at any offset in any order; once the segment
is complete it is handed to ``flush_segment`` in the background, in segment
order, while the exporter fills the next one. At most ``max_buffered_segments``
segments are held at once, so a slow destination holds the exporter back in
``begin_segment`` instead of letting buffers pile up.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Literal, override

from .export_sink import AbortOutcome, ExportSink, SegmentWriter, SinkCaps, WorkerTarget
from .segment_buffers import SharedPool, Slot, SlotPool, Spool

_BLOCK = 8 << 20
"""Default size of one block ``FlushableSegment.blocks`` yields."""

_SEGMENT_ALIGN = 4096

_State = Literal["new", "open", "committed", "aborted"]

_Read = Callable[[int, int], Awaitable[bytes | memoryview]]


class FlushableSegment:
    """One completed segment, handed to ``BufferedExportSink.flush_segment``.

    The segment is readable only until ``flush_segment`` returns; anything
    kept past that must be a copy (``read_all`` returns one).

    Attributes:
        index: The segment's position in the export, counting from 0.
        start: Where the segment starts in the export.
        length: The segment's size in bytes.
    """

    def __init__(self, index: int, start: int, length: int, read: _Read) -> None:
        self.index = index
        self.start = start
        self.length = length
        self._read: _Read | None = read
        self._views: list[memoryview] = []

    async def blocks(self, block: int = _BLOCK) -> AsyncIterator[memoryview]:
        """Yields the segment's bytes in order as views of at most ``block``
        bytes, each released when ``flush_segment`` returns."""
        for offset in range(0, self.length, block):
            view = memoryview(await self._reader()(offset, min(block, self.length - offset)))
            self._views.append(view)
            yield view

    async def read_all(self) -> bytes:
        """The whole segment, copied."""
        return bytes(await self._reader()(0, self.length))

    def _reader(self) -> _Read:
        if self._read is None:
            raise RuntimeError("the segment's bytes are only valid inside flush_segment")
        return self._read


class _Segment(FlushableSegment):
    """A ``FlushableSegment`` the sink itself fills and later releases."""

    def __init__(self, index: int, start: int, length: int, slot: Slot) -> None:
        super().__init__(index, start, length, slot.read)
        self.slot = slot

    def invalidate(self) -> None:
        """Ends reads and releases every view handed out, so a reference
        left behind can't keep the slot's shared memory from closing."""
        self._read = None
        for view in self._views:
            # Still exported (a write in a thread not yet returned, or a
            # slice kept by the consumer): it pins the buffer until collected.
            with contextlib.suppress(BufferError):
                view.release()
        self._views.clear()


class _Core:
    """The sink's shared state, which its segment writers read and update."""

    def __init__(self) -> None:
        self.state: _State = "new"
        self.failure: Exception | None = None
        self.ever_written = False
        self.queue: asyncio.Queue[_Segment | None] = asyncio.Queue()

    def require_open(self) -> None:
        if self.state != "open":
            raise RuntimeError(f"the sink is {self.state}, not open")

    def raise_failure(self) -> None:
        if self.failure is not None:
            raise self.failure


class _SegmentWriter(SegmentWriter):
    """The writer ``begin_segment`` returns: segment-relative offsets into the
    segment's slot, which reads as zero until written."""

    def __init__(self, core: _Core, segment: _Segment) -> None:
        self._core = core
        self._segment = segment
        self.completed = False

    @property
    def index(self) -> int:
        return self._segment.index

    @override
    @property
    def caps(self) -> SinkCaps:
        return self._segment.slot.caps

    @override
    @property
    def preallocated(self) -> bool:
        return self._segment.slot.preallocated

    def _checked(self, offset: int, length: int) -> Slot:
        self._core.require_open()
        self._core.raise_failure()
        if self.completed:
            raise RuntimeError(f"segment {self.index} is already complete")
        if offset < 0 or length < 0 or offset + length > self._segment.length:
            raise ValueError(
                f"write of {length} bytes at {offset} is outside segment {self.index} ({self._segment.length} bytes)"
            )
        return self._segment.slot

    @override
    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        slot = self._checked(offset, len(data))
        self._core.ever_written = True
        await slot.write_at(offset, data)

    @override
    async def write_zero(self, offset: int, length: int) -> None:
        slot = self._checked(offset, length)
        self._core.ever_written = True
        await slot.write_zero(offset, length)

    @override
    def worker_target(self) -> WorkerTarget | None:
        return self._segment.slot.worker_target()

    @override
    def note_worker_write(self) -> None:
        self._core.ever_written = True
        self._segment.slot.note_worker_write()

    @override
    async def complete(self) -> None:
        self._checked(0, 0)
        self.completed = True
        self._core.queue.put_nowait(self._segment)


class BufferedExportSink(ExportSink):
    """An ``ExportSink`` over a destination that takes bytes in order only.

    A subclass implements four hooks; this class owns the lifecycle, the
    buffers, the pacing and the background flushing. A failed flush fails the
    export, surfacing from the next ``begin_segment``, write, ``complete`` or
    ``commit``.

    ``abort`` waits for the flush in progress, which cannot be interrupted
    safely, so it can take as long as one segment's flush; only then is
    ``discard_destination`` called.

    ``storage`` picks where a segment's bytes are held. ``"memory"`` keeps
    them in a block of shared memory that the export's worker processes write
    into directly, so decoding stays spread over several cores. ``"spool"``
    keeps them in a temporary file, likewise written by the workers directly;
    it holds the same number of bytes on disk instead of in memory (the
    default temporary directory may itself be memory-backed: pass
    ``spool_dir`` to choose one).

    Args:
        segment_size: Bytes per segment, a multiple of 4096. Up to
            ``segment_size * max_buffered_segments`` bytes are held at once.
        max_buffered_segments: How many segments are held at once: the one
            being filled plus those waiting for or in the middle of their
            flush. At least 1; the default of 2 overlaps one flush with the
            next segment's export.
        storage: ``"memory"`` or ``"spool"``.
        spool_dir (pathlib.Path | str | None): The directory of the spool file;
            ``None`` for the system's temporary directory. Only for ``"spool"``.

    Raises:
        ValueError: ``segment_size`` is not a positive multiple of 4096,
            ``max_buffered_segments`` is below 1, ``storage`` is unknown, or
            ``spool_dir`` is given for memory storage.
    """

    def __init__(
        self,
        segment_size: int,
        *,
        max_buffered_segments: int = 2,
        storage: Literal["memory", "spool"] = "memory",
        spool_dir: Path | str | None = None,
    ) -> None:
        if segment_size <= 0 or segment_size % _SEGMENT_ALIGN:
            raise ValueError(f"segment_size must be a positive multiple of {_SEGMENT_ALIGN}, got {segment_size}")
        if max_buffered_segments < 1:
            raise ValueError(f"max_buffered_segments must be at least 1, got {max_buffered_segments}")
        if storage not in ("memory", "spool"):
            raise ValueError(f"storage must be 'memory' or 'spool', got {storage!r}")
        if spool_dir is not None and storage != "spool":
            raise ValueError("spool_dir only applies to spool storage")
        self._segment_size = segment_size
        self._logical_size = 0
        self._max_buffered = max_buffered_segments
        self._pool: SlotPool = (
            Spool(Path(spool_dir) if spool_dir is not None else None, segment_size)
            if storage == "spool"
            else SharedPool(segment_size)
        )
        self._core = _Core()
        self._creating = False
        self._aborting = False
        self._slots = asyncio.Condition()
        self._flusher: asyncio.Task[None] | None = None
        self._next_index = 0
        self._writer: _SegmentWriter | None = None
        self._abort_outcome: AbortOutcome | None = None
        self._abort_task: asyncio.Future[AbortOutcome] | None = None
        self._finalizing: asyncio.Future[None] | None = None

    # -- the hooks ----------------------------------------------------------

    @abc.abstractmethod
    async def create_destination(self, logical_size: int, *, sparse: bool) -> None:
        """Creates the destination for an export of ``logical_size`` bytes.
        May raise to refuse it (a part-count limit, say); ``discard_destination``
        follows, so it must cope with whatever a failed or cancelled call left."""

    @abc.abstractmethod
    async def flush_segment(self, segment: FlushableSegment) -> None:
        """Writes ``segment`` to the destination. Called once per segment, in
        segment order, never concurrently with itself."""

    @abc.abstractmethod
    async def finalize_destination(self) -> None:
        """Makes the destination final, after every segment was flushed."""

    @abc.abstractmethod
    async def discard_destination(self) -> bool:
        """Releases the destination after a failure, once no flush is running.

        Returns:
            Whether something of the export is left behind.
        """

    # -- ExportSink ----------------------------------------------------------

    @override
    @property
    def segment_size(self) -> int:
        return self._segment_size

    @override
    async def open(self, logical_size: int, *, sparse: bool) -> None:
        if self._core.state != "new":
            raise RuntimeError(f"the sink is {self._core.state}, not new")
        self._logical_size = logical_size
        # First, so an unusable spool directory refuses the export before a destination exists.
        # No more slots than the export has segments: a small export does not claim a whole buffer.
        slots = max(1, min(self._max_buffered, -(-logical_size // self._segment_size)))
        try:
            await self._pool.create(slots)
        except BaseException:
            self._core.state = "aborted"
            await self._pool.close()
            raise
        self._creating = True
        await self.create_destination(logical_size, sparse=sparse)
        if self._core.state != "new":
            raise RuntimeError(f"the sink was {self._core.state} while it was opening")
        self._core.state = "open"
        self._flusher = asyncio.ensure_future(self._flush_loop())

    @override
    async def begin_segment(self, index: int, start: int, length: int) -> SegmentWriter:
        self._core.require_open()
        if self._writer is not None and not self._writer.completed:
            raise RuntimeError(f"segment {self._writer.index} is not complete")
        if index != self._next_index:
            raise ValueError(f"expected segment {self._next_index}, got {index}")
        expected_length = min(self._segment_size, self._logical_size - index * self._segment_size)
        if (start, length) != (index * self._segment_size, expected_length) or length <= 0:
            raise ValueError(
                f"segment {index} must be {expected_length} bytes at {index * self._segment_size}, "
                f"got {length} at {start}"
            )
        async with self._slots:
            await self._slots.wait_for(
                lambda: self._core.failure is not None or self._aborting or bool(self._pool.free)
            )
            self._core.require_open()
            self._core.raise_failure()
            slot = self._pool.take()
        self._next_index += 1
        writer = self._writer = _SegmentWriter(self._core, _Segment(index, start, length, slot))
        return writer

    @override
    async def commit(self) -> None:
        self._core.require_open()
        if self._writer is not None and not self._writer.completed:
            raise RuntimeError(f"segment {self._writer.index} is not complete")
        self._core.raise_failure()
        self._core.queue.put_nowait(None)
        assert self._flusher is not None
        # Shielded: a cancelled commit must not interrupt the flush in progress (see ``abort``).
        await asyncio.shield(self._flusher)
        self._core.require_open()  # an abort may have run while the flushes finished
        self._core.raise_failure()
        self._finalizing = asyncio.ensure_future(self._finalize())
        await asyncio.shield(self._finalizing)
        await self._pool.close()

    @override
    async def abort(self) -> AbortOutcome:
        if self._abort_outcome is not None:
            return self._abort_outcome
        if self._abort_task is None:
            # One run, however many callers: the destination is discarded once, and a caller
            # that is cancelled does not stop it.
            self._abort_task = asyncio.ensure_future(self._abort())
            self._abort_task.add_done_callback(lambda task: task.cancelled() or task.exception())
        return await asyncio.shield(self._abort_task)

    async def _finalize(self) -> None:
        await self.finalize_destination()
        self._core.state = "committed"

    async def _abort(self) -> AbortOutcome:
        if self._finalizing is not None:
            # Never discard a destination that is being finalized.
            await asyncio.wait({self._finalizing})
        was = self._core.state
        self._aborting = True
        if was != "committed":
            self._core.state = "aborted"
        async with self._slots:
            self._slots.notify_all()
        if self._flusher is not None:
            self._core.queue.put_nowait(None)
            # ``wait`` neither re-raises the flusher's outcome nor hides a cancel of this call.
            await asyncio.wait({self._flusher})
        with contextlib.suppress(Exception):
            await self._pool.close()
        ever_written = self._core.ever_written
        if was == "committed":
            outcome = AbortOutcome(kept=True, ever_written=ever_written)
        elif not self._creating:
            outcome = AbortOutcome(kept=False, ever_written=False)
        else:
            try:
                kept = await self.discard_destination()
            except BaseException:
                self._abort_outcome = AbortOutcome(kept=True, ever_written=ever_written)
                raise
            outcome = AbortOutcome(kept=kept, ever_written=ever_written)
        self._abort_outcome = outcome
        return outcome

    # -- internals -----------------------------------------------------------

    async def _release_slot(self) -> None:
        """Wakes a ``begin_segment`` waiting for a slot to come back."""
        async with self._slots:
            self._slots.notify_all()

    async def _flush_loop(self) -> None:
        """Flushes completed segments in order until told to stop; after a
        failure, or an abort, it only drops what is still queued."""
        while (segment := await self._core.queue.get()) is not None:
            try:
                if self._core.failure is None and not self._aborting:
                    await self.flush_segment(segment)
            except BaseException as exc:
                # Whatever stopped the flush, a ``begin_segment`` waiting for a slot must hear of it.
                self._core.failure = exc if isinstance(exc, Exception) else RuntimeError(f"flushing stopped: {exc!r}")
                if isinstance(exc, Exception):
                    continue
                raise
            finally:
                await self._free(segment)

    async def _free(self, segment: _Segment) -> None:
        """Takes ``segment``'s slot back, so the next segment can use it."""
        segment.invalidate()
        try:
            if self._core.failure is None and not self._aborting:
                await segment.slot.release()
            else:
                segment.slot.discard()
        except Exception as exc:  # noqa: BLE001
            # A slot that could not be zeroed must never be handed out again.
            self._core.failure = self._core.failure or exc
        await self._release_slot()
