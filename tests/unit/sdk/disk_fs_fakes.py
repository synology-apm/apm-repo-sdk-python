"""The ``ContentSource`` fakes the ``disk_fs`` unit tests open disk images
through, and the loaders for the committed ``tests/fixtures/tiny_*`` images,
each decompressed or extracted at most once per test process."""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import gzip
import os
import shutil
import sys
import tarfile
import tempfile
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from functools import cache
from pathlib import Path

from support.fakes import faithful_to
from synology_apm_repo.sdk.dedup.export_sink import ExportWriter
from synology_apm_repo.sdk.dedup.extent import ExportResult
from synology_apm_repo.sdk.units.base import ContentSource

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"

_O_BINARY = getattr(os, "O_BINARY", 0)

_PREAD_LOCK = threading.Lock()


def _pread(fd: int, length: int, offset: int) -> bytes:
    """``os.pread()``, or ``lseek``+``read`` under a lock where it is unavailable (Windows)."""
    if sys.platform != "win32":
        return os.pread(fd, length, offset)
    with _PREAD_LOCK:
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)


@cache
def raw_image(name: str) -> bytes:
    """A gzip-compressed ``tests/fixtures/<name>`` image's bytes, decompressed
    once per process (callers slice it; ``bytes`` is immutable)."""
    return gzip.decompress((_FIXTURES / name).read_bytes())


@cache
def extracted_image(name: str) -> tuple[Path, int]:
    """A sparse ``.raw.tar.gz`` ``tests/fixtures/<name>`` image's one member,
    extracted once per process to a temp file removed at exit, as ``(path,
    size)``; extraction keeps its holes, so the full logical size is never
    materialized."""
    tmp_dir = Path(tempfile.mkdtemp())
    atexit.register(shutil.rmtree, tmp_dir, ignore_errors=True)
    with tarfile.open(_FIXTURES / name, "r:gz") as tf:
        (member,) = tf.getmembers()
        tf.extract(member, path=tmp_dir, filter="data")
    return tmp_dir / member.name, member.size


@faithful_to(ContentSource)
class MemoryContent:
    """In-memory ``ContentSource`` whose ``read()`` awaits, so
    ``DiskFilesystem``'s sync-to-async bridge round-trips the event loop."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    @property
    def size(self) -> int | None:
        return len(self._data)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        await asyncio.sleep(0)
        n = length if length is not None else len(self._data) - offset
        return self._data[offset : offset + n]

    def stream(self, block: int = 8 << 20) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: Callable[[int], Awaitable[None]] | None = None,
        tuning: object = None,
    ) -> ExportResult:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")


@faithful_to(ContentSource)
class FileBackedContent:
    """``ContentSource`` over a file fd, keeping a large image out of the heap."""

    def __init__(self, path: Path, size: int) -> None:
        self._fd = os.open(path, os.O_RDONLY | _O_BINARY)
        self._size = size

    def __del__(self) -> None:
        # Best-effort: no test closes this fake, so fds would otherwise pile up.
        with contextlib.suppress(OSError):
            os.close(self._fd)

    @property
    def size(self) -> int | None:
        return self._size

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        n = length if length is not None else self._size - offset
        return await asyncio.to_thread(_pread, self._fd, n, offset)

    def stream(self, block: int = 8 << 20) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: Callable[[int], Awaitable[None]] | None = None,
        tuning: object = None,
    ) -> ExportResult:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")
