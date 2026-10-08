"""Where ``BufferedExportSink`` keeps each segment while it is filled and
flushed: a ``Slot`` of a ``SlotPool`` — one shared-memory block
(``SharedPool``) or one sparse temporary file (``Spool``) holding every
slot side by side, so a worker process can write into its slot directly."""

from __future__ import annotations

import abc
import asyncio
import contextlib
import dataclasses
import os
import sys
import tempfile
from collections.abc import Callable
from multiprocessing import shared_memory
from pathlib import Path
from typing import Any, override

from ..positional_io import O_BINARY, pread, pwrite
from .export_sink import SinkCaps, SinkDescriptor, WorkerTarget, WorkerWriter
from .local_file_io import create_presized, write_zeros_at, zero_range
from .local_file_sink import LocalFileDescriptor

_ZERO_BLOCK = bytes(1 << 20)


class SlotPool(abc.ABC):
    """Every slot of one export side by side: slot ``k`` is the bytes
    ``[k * segment_size, (k + 1) * segment_size)``. Offsets below are
    pool-relative; a ``Slot`` adds its own base."""

    def __init__(self, segment_size: int) -> None:
        self.segment_size = segment_size
        self._slots = 0
        self.free: list[int] = []
        self._creation: asyncio.Future[None] | None = None

    async def create(self, slots: int) -> None:
        self._slots = slots
        self.free = list(range(slots))
        self._creation = asyncio.ensure_future(asyncio.to_thread(self._create))
        await asyncio.shield(self._creation)

    def take(self) -> Slot:
        """Hands out the lowest free slot; the caller checked one is free."""
        return Slot(self, self.free.pop(0))

    async def close(self) -> None:
        """Releases the storage, once a cancelled ``create``'s thread is done
        (the storage it made is ours to release)."""
        if self._creation is not None:
            await asyncio.wait({self._creation})
        await self._close()

    @abc.abstractmethod
    def _create(self) -> None:
        """Allocates ``segment_size * _slots`` bytes reading as zero; runs in a thread."""

    @abc.abstractmethod
    async def _close(self) -> None: ...

    @abc.abstractmethod
    async def write_at(self, offset: int, data: bytes | memoryview) -> None: ...

    @abc.abstractmethod
    async def write_zero(self, offset: int, length: int) -> None: ...

    @abc.abstractmethod
    async def read(self, offset: int, length: int) -> bytes | memoryview: ...

    @abc.abstractmethod
    async def clear(self, offset: int, length: int) -> None:
        """Zeroes a whole slot again for its next segment."""

    @abc.abstractmethod
    def descriptor(self) -> SinkDescriptor:
        """How a worker process opens the pool for writing."""


class Slot:
    """Where one segment's bytes live while it is filled and flushed. A slot
    reads as zero until written, and is zero again when it is handed out."""

    caps = SinkCaps(supports_sparse=True)
    preallocated = True

    def __init__(self, pool: SlotPool, index: int) -> None:
        self._pool = pool
        self._index = index
        self._base = index * pool.segment_size
        self._touched = False

    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self._touched = True
        await self._pool.write_at(self._base + offset, data)

    async def write_zero(self, offset: int, length: int) -> None:
        self._touched = True
        await self._pool.write_zero(self._base + offset, length)

    async def read(self, offset: int, length: int) -> bytes | memoryview:
        return await self._pool.read(self._base + offset, length)

    def worker_target(self) -> WorkerTarget:
        return WorkerTarget(self._pool.descriptor(), self._base)

    def note_worker_write(self) -> None:
        self._touched = True

    async def release(self) -> None:
        """Takes the slot back, leaving it zero for the next segment."""
        if self._touched:
            await self._pool.clear(self._base, self._pool.segment_size)
        self._pool.free.append(self._index)

    def discard(self) -> None:
        """Takes the slot back without caring what it holds (the export is over)."""
        self._pool.free.append(self._index)


@dataclasses.dataclass(frozen=True, slots=True)
class SharedMemoryDescriptor:
    """``SinkDescriptor`` for a block of shared memory the parent created: a worker
    process attaches to it by name and writes into it directly."""

    name: str

    def open_writer(self) -> WorkerWriter:
        return _SharedMemoryWriter(self.name)


class _SharedMemoryWriter:
    def __init__(self, name: str) -> None:
        if sys.version_info >= (3, 13):
            self._shm = shared_memory.SharedMemory(name=name, track=False)
        else:
            # Spawned workers share the parent's resource tracker, which already holds the block: attaching
            # is harmless, and unregistering here would make the parent's own unlink fail.
            self._shm = shared_memory.SharedMemory(name=name)

    def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self._shm.buf[offset : offset + len(data)] = data  # type: ignore[index]

    def close(self) -> None:
        self._shm.close()


def _zero(buf: memoryview, start: int, end: int) -> None:
    """Zeroes ``buf[start:end]`` in blocks, without allocating the range."""
    for block_start in range(start, end, len(_ZERO_BLOCK)):
        block_end = min(block_start + len(_ZERO_BLOCK), end)
        buf[block_start:block_end] = _ZERO_BLOCK[: block_end - block_start]


class SharedPool(SlotPool):
    """Slots in one block of shared memory, which worker processes attach to
    by name and write into directly."""

    def __init__(self, segment_size: int) -> None:
        super().__init__(segment_size)
        self._shm: shared_memory.SharedMemory | None = None

    @override
    def _create(self) -> None:
        self._shm = shared_memory.SharedMemory(create=True, size=self.segment_size * self._slots)

    @property
    def name(self) -> str:
        assert self._shm is not None
        return self._shm.name

    @property
    def buf(self) -> memoryview:
        assert self._shm is not None and self._shm.buf is not None
        return self._shm.buf

    @override
    async def _close(self) -> None:
        shm, self._shm = self._shm, None
        if shm is not None:
            await asyncio.to_thread(self._release, shm)

    @staticmethod
    def _release(shm: shared_memory.SharedMemory) -> None:
        # A view a flush kept past its hook would block closing; the block is still unlinked.
        with contextlib.suppress(BufferError):
            shm.close()
        with contextlib.suppress(FileNotFoundError):
            shm.unlink()

    @override
    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self.buf[offset : offset + len(data)] = data

    @override
    async def write_zero(self, offset: int, length: int) -> None:
        # A hole can be gigabytes: not on the event loop.
        await asyncio.to_thread(_zero, self.buf, offset, offset + length)

    @override
    async def read(self, offset: int, length: int) -> memoryview:
        return self.buf[offset : offset + length]

    @override
    async def clear(self, offset: int, length: int) -> None:
        await self.write_zero(offset, length)

    @override
    def descriptor(self) -> SharedMemoryDescriptor:
        return SharedMemoryDescriptor(self.name)


class Spool(SlotPool):
    """Slots in one sparse temporary file, which worker processes open by
    path and write into directly."""

    def __init__(self, directory: Path | None, segment_size: int) -> None:
        super().__init__(segment_size)
        self._directory = directory
        self.path: Path | None = None
        self._fd: int | None = None
        self._pending: set[asyncio.Future[Any]] = set()

    @override
    def _create(self) -> None:
        handle, name = tempfile.mkstemp(prefix="apm-export-", suffix=".spool", dir=self._directory)
        os.close(handle)
        self.path = Path(name)
        create_presized(self.path, self.segment_size * self._slots, sparse=True)
        self._fd = os.open(self.path, os.O_RDWR | O_BINARY)

    async def run[**P, R](self, call: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """Runs a blocking call on the spool's fd in a thread that ``close``
        waits for, so a cancelled caller can never leave one writing to a
        closed (and perhaps reused) descriptor."""
        future = asyncio.ensure_future(asyncio.to_thread(call, *args, **kwargs))
        self._pending.add(future)
        future.add_done_callback(self._pending.discard)
        return await asyncio.shield(future)

    @property
    def fd(self) -> int:
        assert self._fd is not None
        return self._fd

    @override
    async def _close(self) -> None:
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)
        await asyncio.to_thread(self._close_file)

    def _close_file(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        if self.path is not None:
            self.path.unlink(missing_ok=True)
            self.path = None

    @override
    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        await self.run(pwrite, self.fd, data, offset)

    @override
    async def write_zero(self, offset: int, length: int) -> None:
        await self.run(write_zeros_at, self.fd, offset, length)

    @override
    async def read(self, offset: int, length: int) -> bytes:
        return await self.run(pread, self.fd, length, offset)

    @override
    async def clear(self, offset: int, length: int) -> None:
        # The whole slot, so the range stays aligned for a deallocating zero.
        await self.run(zero_range, self.fd, offset, length)

    @override
    def descriptor(self) -> LocalFileDescriptor:
        assert self.path is not None
        return LocalFileDescriptor(str(self.path))
