"""``LocalFileSink``: the local-filesystem ``RandomAccessExportSink``, plus the
``WorkerWriter`` its worker processes open for themselves."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import queue
import threading
from pathlib import Path
from typing import override

from ..positional_io import pwrite
from ..presentation.export_target import part_path_for
from .export_sink import (
    AbortOutcome,
    RandomAccessExportSink,
    SinkCaps,
    WorkerTarget,
    WorkerWriter,
)
from .local_file_io import create_presized, open_destination, preallocate, write_zeros_at

_WRITER_QUEUE_SIZE = 8
"""Backpressure cap for ``LocalFileSink``'s writer thread, bounding memory
when writes fall behind decode."""


@dataclasses.dataclass(frozen=True, slots=True)
class _DataJob:
    offset: int
    payload: bytes | memoryview


@dataclasses.dataclass(frozen=True, slots=True)
class _GapJob:
    offset: int
    length: int


_WriteJob = _DataJob | _GapJob
_SENTINEL = object()


class _FdWriter:
    """``WorkerWriter`` over one long-lived, already-created local file."""

    def __init__(self, fd: int) -> None:
        self._fd = fd

    def write_at(self, offset: int, data: bytes | memoryview) -> None:
        pwrite(self._fd, data, offset)

    def close(self) -> None:
        os.close(self._fd)


@dataclasses.dataclass(frozen=True, slots=True)
class LocalFileDescriptor:
    """``SinkDescriptor`` for a local file the parent already created."""

    path: str

    def open_writer(self) -> WorkerWriter:
        return _FdWriter(open_destination(self.path))


class _WriterThread:
    """One thread applying queued writes to an fd it owns and closes on exit.

    After a failed write it keeps consuming and discards jobs, so a producer
    blocked on the full queue never hangs; the failure surfaces from
    ``submit`` and ``error``.
    """

    def __init__(self, fd: int) -> None:
        self._queue: queue.Queue[_WriteJob | object] = queue.Queue(maxsize=_WRITER_QUEUE_SIZE)
        self.error: BaseException | None = None
        self._closing = False
        self._sentinel_sent = False
        self._thread = threading.Thread(target=self._run, args=(fd,), name="export-writer", daemon=True)
        self._thread.start()

    @property
    def accepting(self) -> bool:
        return not self._closing

    def _run(self, fd: int) -> None:
        try:
            self._consume(fd)
        finally:
            os.close(fd)

    def _consume(self, fd: int) -> None:
        while True:
            item = self._queue.get()
            if item is _SENTINEL:
                return
            if self.error is not None:
                continue
            try:
                match item:
                    case _DataJob():
                        pwrite(fd, item.payload, item.offset)
                    case _GapJob():
                        write_zeros_at(fd, item.offset, item.length)
            except BaseException as exc:  # noqa: BLE001
                self.error = exc

    async def submit(self, job: _WriteJob) -> None:
        """Hands ``job`` over; a full queue blocks in a worker thread, never on
        the event loop.

        Raises:
            RuntimeError: ``close`` has begun.
            BaseException: The thread's earlier write failure.
        """
        if self._closing:
            raise RuntimeError("the sink is not open")
        if self.error is not None:
            raise self.error
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            await asyncio.to_thread(self._queue.put, job)

    def _send_sentinel(self) -> None:
        self._queue.put(_SENTINEL)
        self._sentinel_sent = True

    async def close(self) -> None:
        """Stops the thread once its queue is drained (it then closes the fd).
        Idempotent: a call after a cancelled one waits for the same thread."""
        self._closing = True
        if not self._sentinel_sent:
            await asyncio.to_thread(self._send_sentinel)
        await asyncio.to_thread(self._thread.join)


class LocalFileSink(RandomAccessExportSink):
    """Writes to a local file through one dedicated writer thread, so writes
    overlap decode and may land at any offset in any order.

    ``open`` creates the file at its full size (creating missing parent
    directories), so unwritten ranges of a sparse export read back as zero.
    A dense export (``sparse=False``) also reserves the whole file's space
    there where the filesystem can (``preallocated``), failing in ``open`` if
    there isn't room; the export then has no zero-fill to write
    (``needs_zero_fill``).

    With ``staged=True`` bytes go to ``<dst>.part`` (the ``path`` property)
    and ``commit`` renames it to ``dst``; with ``staged=False`` they go to
    ``dst`` directly. ``abort`` deletes a file nothing was written to; a
    file with data in it is kept when unstaged, and when staged only with
    ``keep_partial``. A sink never opened, whose ``open`` failed before
    creating the file, or already committed, leaves the path untouched.
    """

    caps = SinkCaps(supports_sparse=True)

    def __init__(self, dst: Path, *, staged: bool, keep_partial: bool = False) -> None:
        self._dst = Path(dst)
        self._staged = staged
        self._keep_partial = keep_partial
        self._path = part_path_for(self._dst) if staged else self._dst
        self._writer: _WriterThread | None = None
        self._opening: asyncio.Future[None] | None = None
        self._created = False
        self._ever_written = False
        self._committed = False
        self._preallocated = False

    @property
    def path(self) -> Path:
        return self._path

    @override
    @property
    def preallocated(self) -> bool:
        return self._preallocated

    def _mark_created(self) -> None:
        self._created = True

    def _create(self, logical_size: int, sparse: bool) -> int:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        create_presized(self._path, logical_size, sparse=sparse, on_open=self._mark_created)
        fd = open_destination(self._path)
        if not sparse:
            try:
                self._preallocated = preallocate(fd, logical_size)
            except BaseException:
                os.close(fd)
                raise
        return fd

    def _create_and_start(self, logical_size: int, sparse: bool) -> None:
        # The thread owns the fd from the moment it is opened, so a cancelled open leaks none.
        self._writer = _WriterThread(self._create(logical_size, sparse))

    @override
    async def open(self, logical_size: int, *, sparse: bool) -> None:
        opening = asyncio.ensure_future(asyncio.to_thread(self._create_and_start, logical_size, sparse))
        opening.add_done_callback(lambda f: f.cancelled() or f.exception())  # mark the error as retrieved
        self._opening = opening
        await asyncio.shield(opening)  # a cancel here leaves ``opening`` for ``abort`` to wait out

    def _open_writer(self) -> _WriterThread:
        writer = self._writer
        if writer is None or not writer.accepting:
            raise RuntimeError("the sink is not open")
        return writer

    @override
    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        writer = self._open_writer()
        self._ever_written = True
        await writer.submit(_DataJob(offset, data))

    @override
    async def write_zero(self, offset: int, length: int) -> None:
        writer = self._open_writer()
        self._ever_written = True
        await writer.submit(_GapJob(offset, length))

    @override
    def worker_target(self) -> WorkerTarget:
        return WorkerTarget(LocalFileDescriptor(str(self._path)))

    @override
    def note_worker_write(self) -> None:
        self._ever_written = True

    async def _close_writer(self) -> BaseException | None:
        """Waits out a pending ``open``, stops the writer and returns its
        write failure, if any."""
        if self._opening is not None:
            await asyncio.wait({self._opening})
        if self._writer is None:
            return None
        await self._writer.close()
        return self._writer.error

    @override
    async def commit(self) -> None:
        error = await self._close_writer()
        if error is not None:
            raise error
        if self._staged:
            await asyncio.to_thread(self._path.replace, self._dst)
        self._committed = True

    @override
    async def abort(self) -> AbortOutcome:
        if self._opening is None:
            return AbortOutcome(kept=False, ever_written=False)  # never opened: nothing here is ours
        interrupted: BaseException | None = None
        try:
            await self._close_writer()
        except BaseException as exc:  # noqa: BLE001 - the file is still cleaned up before this propagates
            interrupted = exc
        outcome = self._discard_unless_kept()
        if interrupted is not None:
            raise interrupted
        return outcome

    def _discard_unless_kept(self) -> AbortOutcome:
        if not self._created:
            return AbortOutcome(kept=False, ever_written=False)  # open failed before touching the path
        keep = self._committed or (self._ever_written and (self._keep_partial or not self._staged))
        if not keep:
            self._path.unlink(missing_ok=True)
        return AbortOutcome(kept=keep, ever_written=self._ever_written)
