"""``LocalFsStore`` — the ``ObjectStore`` implementation for a local (or
locally-mounted) filesystem tree."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import struct
import sys
from pathlib import Path, PurePosixPath
from typing import override

from ..errors import ApmRepoError, NotFoundError, PermissionDeniedError, StorageBackendError
from ..positional_io import O_BINARY, pread
from .base import Entry

# Readahead hint (``posix_fadvise(WILLNEED)`` on Linux, ``F_RDADVISE`` on
# macOS) for the merged multi-chunk reads ``dedup/pool/_bucket_reader.py``
# issues, gated on a minimum length so routine small reads pay no extra syscall.
_FADVISE_MIN_LENGTH = 64 << 10  # 64 KiB
_DARWIN_F_RDADVISE = 44  # <fcntl.h>'s F_RDADVISE — not exposed by Python's fcntl module


def _entry_size(entry: os.DirEntry[str]) -> int | None:
    """A regular file's size in bytes; ``None`` for a directory, or for an
    entry that vanished or cannot be stat'ed between listing and stat."""
    try:
        return entry.stat().st_size if entry.is_file() else None
    except OSError:
        return None


def _mapped_os_error(exc: OSError, path: str, *, missing: str = "no such path") -> ApmRepoError:
    """The ``ObjectStore`` error an ``OSError`` on ``path`` means; ``missing``
    is the message for an absent path."""
    if isinstance(exc, FileNotFoundError):
        return NotFoundError(missing, ref=path)
    if isinstance(exc, NotADirectoryError):
        return NotFoundError("not a directory", ref=path)
    if isinstance(exc, IsADirectoryError):
        return NotFoundError("is a directory, not a file", ref=path)
    if isinstance(exc, PermissionError):
        return PermissionDeniedError("permission denied", ref=path)
    return StorageBackendError(f"I/O error: {exc}", ref=path)


def _hint_willneed(fd: int, offset: int, length: int) -> None:
    """Best-effort readahead hint; any failure is swallowed, since it must
    never fail a working read. No-op off Linux and macOS.
    """
    try:
        if sys.platform == "linux":
            os.posix_fadvise(fd, offset, length, os.POSIX_FADV_WILLNEED)
        elif sys.platform == "darwin":
            import fcntl

            # struct radvisory { off_t ra_offset; int ra_count; }, padded to 16 bytes.
            fcntl.fcntl(fd, _DARWIN_F_RDADVISE, struct.pack("qi4x", offset, length))
    except (OSError, ValueError):
        # fcntl.fcntl() raises ValueError, not OSError, for e.g. a negative fd.
        pass


@dataclasses.dataclass(frozen=True, slots=True)
class LocalFsStoreDescriptor:
    """Picklable recipe for rebuilding an equivalent ``LocalFsStore``."""

    root: str

    def build(self) -> LocalFsStore:
        return LocalFsStore(self.root)


class LocalFsStore:
    """A local (or locally-mounted) filesystem tree, addressed by
    ``"/"``-separated paths relative to ``root``.

    ``read()`` opens a fresh read-only fd per call and closes it, keeping
    no per-path state; a caller wanting fd reuse owns that itself.

    Raises:
        NotFoundError: ``root`` is not a directory.
        PermissionDeniedError: ``root`` cannot be accessed.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()
        try:
            is_dir = self._root.is_dir()
        except PermissionError as exc:
            raise PermissionDeniedError("permission denied", ref=str(self._root)) from exc
        if not is_dir:
            raise NotFoundError("store root is not a directory", ref=str(self._root))

    async def close(self) -> None:
        """Nothing to release: every call opens and closes its own fd."""

    def descriptor(self) -> LocalFsStoreDescriptor:
        """How a worker process rebuilds this store."""
        return LocalFsStoreDescriptor(str(self.root))

    @override
    def __repr__(self) -> str:
        return f"LocalFsStore({self._root!r})"

    @property
    def root(self) -> Path:
        return self._root

    def local_path(self, path: str) -> Path:
        """The OS path of store-relative ``path``, for a caller that opens
        the file itself (``storage/sqlite.py``'s read-only fast path).

        Raises:
            NotFoundError: ``path`` is absolute or escapes the store root.
        """
        rel = PurePosixPath(path)
        if rel.is_absolute() or ".." in rel.parts:
            raise NotFoundError("path escapes store root", ref=path)
        return self._root.joinpath(*rel.parts)

    # -- ObjectStore ------------------------------------------------------

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return await asyncio.to_thread(self.read_sync, path, offset, length)

    async def size(self, path: str) -> int:
        return await asyncio.to_thread(self._size_sync, path)

    async def exists(self, path: str) -> bool:
        return await asyncio.to_thread(self._exists_sync, path)

    async def listdir(self, path: str) -> list[Entry]:
        return await asyncio.to_thread(self._listdir_sync, path)

    def read_sync(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        """``read``, blocking the calling thread (``storage.base.SyncReadable``)."""
        p = self.local_path(path)
        try:
            # O_BINARY (0 off Windows) is not optional: Windows opens in text
            # mode by default, which collapses CRLF and stops the read dead at
            # the first 0x1A byte -- silently short, mangled repository bytes.
            fd = os.open(p, os.O_RDONLY | O_BINARY)
        except PermissionError as exc:
            # Windows raises PermissionError from os.open() on a directory.
            # os.path.isdir(), not Path.is_dir(), which can re-raise here.
            if os.path.isdir(p):
                raise NotFoundError("is a directory, not a file", ref=path) from exc
            raise _mapped_os_error(exc, path) from exc
        except OSError as exc:
            raise _mapped_os_error(exc, path, missing="no such file") from exc
        try:
            if length is None:
                length = max(0, os.fstat(fd).st_size - offset)
            if length <= 0:
                return b""
            if length >= _FADVISE_MIN_LENGTH:
                _hint_willneed(fd, offset, length)
            return pread(fd, length, offset)
        except OSError as exc:
            # IsADirectoryError lands here: os.open() on a directory succeeds
            # on both Linux and macOS (it's the read that fails).
            raise _mapped_os_error(exc, path) from exc
        finally:
            os.close(fd)

    def _size_sync(self, path: str) -> int:
        p = self.local_path(path)
        try:
            return p.stat().st_size
        except OSError as exc:
            raise _mapped_os_error(exc, path) from exc

    def _exists_sync(self, path: str) -> bool:
        try:
            p = self.local_path(path)
        except NotFoundError:
            # An escaping path does not exist; exists() never raises.
            return False
        try:
            return p.exists()
        except PermissionError:
            # An inaccessible path reads as absent.
            return False
        except OSError as exc:
            raise _mapped_os_error(exc, path) from exc

    def _listdir_sync(self, path: str) -> list[Entry]:
        p = self.local_path(path)
        try:
            with os.scandir(p) as it:
                entries = [Entry(entry.name, _entry_size(entry)) for entry in it]
        except OSError as exc:
            raise _mapped_os_error(exc, path, missing="no such directory") from exc
        return sorted(entries, key=lambda entry: entry.name)
