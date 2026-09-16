"""``LocalFsStore`` — the ``ObjectStore`` implementation for a local (or
locally-mounted) filesystem tree."""

from __future__ import annotations

import asyncio
import os
import struct
import sys
from pathlib import Path, PurePosixPath

from ..errors import NotFoundError, PermissionDeniedError

# Readahead hint (``posix_fadvise(WILLNEED)`` on Linux, ``F_RDADVISE`` on macOS)
# for the merged multi-chunk reads ``dedup/pool.py::BucketReader``
# issues — gated on a minimum length so it fires for those
# genuinely-merged reads without adding a syscall to every routine small
# read this store also serves (SQLite page reads, single-chunk-locator
# reads, header reads, ...), which would cost more than the hint could ever
# save on this store's overwhelming majority of callers.
_FADVISE_MIN_LENGTH = 64 << 10  # 64 KiB — matches the merge gap tolerance
_HAS_POSIX_FADVISE = hasattr(os, "posix_fadvise")
_DARWIN_F_RDADVISE = 44  # <fcntl.h>'s F_RDADVISE — not exposed by Python's fcntl module


def _hint_willneed(fd: int, offset: int, length: int) -> None:
    """Best-effort readahead hint — purely advisory, so any failure
    (unsupported platform, an fd type the call doesn't like) is
    swallowed: an optimization that can't fire must never turn a working
    read into a failure. No-op on any platform that is neither Linux
    (has ``os.posix_fadvise``) nor macOS.
    """
    try:
        if _HAS_POSIX_FADVISE:
            os.posix_fadvise(fd, offset, length, os.POSIX_FADV_WILLNEED)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            import fcntl

            # struct radvisory { off_t ra_offset; int ra_count; } — 8-byte
            # off_t + 4-byte int, padded to 16 bytes for 8-byte struct
            # alignment on both x86_64 and arm64 macOS.
            fcntl.fcntl(fd, _DARWIN_F_RDADVISE, struct.pack("qi4x", offset, length))
    except (OSError, ValueError):
        # OSError: the platform/fd genuinely doesn't support the call.
        # ValueError: fcntl.fcntl() raises this (not OSError) for some
        # malformed-argument cases, e.g. a negative fd.
        pass


class LocalFsStore:
    """A local (or locally-mounted) filesystem tree, addressed by
    ``"/"``-separated paths relative to ``root``.

    ``read()`` opens a fresh read-only fd per call, reads, and closes it —
    no per-path state of its own, the same shape ``SmbStore`` already has.
    Any caller whose own access pattern genuinely benefits from reusing an
    open fd across several reads to the same path owns that reuse decision
    itself, rather than this store deciding on its own, opaquely, whether
    or how long to keep something cached.

    **On the async interface**: see ``ObjectStore``'s docstring for why
    this store's four methods are ``asyncio.to_thread()`` wrappers around
    a synchronous body.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()
        try:
            is_dir = self._root.is_dir()
        except PermissionError as exc:
            raise PermissionDeniedError("permission denied", ref=str(self._root)) from exc
        if not is_dir:
            raise NotFoundError("store root is not a directory", ref=str(self._root))

    def __repr__(self) -> str:
        return f"LocalFsStore({self._root!r})"

    @property
    def root(self) -> Path:
        return self._root

    def _resolve(self, path: str) -> Path:
        rel = PurePosixPath(path)
        if rel.is_absolute() or ".." in rel.parts:
            raise NotFoundError("path escapes store root", ref=path)
        return self._root.joinpath(*rel.parts)

    # -- ObjectStore ------------------------------------------------------

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return await asyncio.to_thread(self._read_sync, path, offset, length)

    async def size(self, path: str) -> int:
        return await asyncio.to_thread(self._size_sync, path)

    async def exists(self, path: str) -> bool:
        return await asyncio.to_thread(self._exists_sync, path)

    async def listdir(self, path: str) -> list[str]:
        return await asyncio.to_thread(self._listdir_sync, path)

    def _read_sync(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        p = self._resolve(path)
        try:
            fd = os.open(p, os.O_RDONLY)
        except FileNotFoundError as exc:
            raise NotFoundError("no such file", ref=path) from exc
        except IsADirectoryError as exc:
            raise NotFoundError("is a directory, not a file", ref=path) from exc
        except PermissionError as exc:
            raise PermissionDeniedError("permission denied", ref=path) from exc
        try:
            if length is None:
                length = max(0, os.fstat(fd).st_size - offset)
            if length <= 0:
                return b""
            if length >= _FADVISE_MIN_LENGTH:
                _hint_willneed(fd, offset, length)
            return os.pread(fd, length, offset)
        except IsADirectoryError as exc:
            # os.open() on a directory succeeds on both Linux and macOS
            # (it's the read that fails) — unlike a missing file, which
            # the os.open() above already caught.
            raise NotFoundError("is a directory, not a file", ref=path) from exc
        finally:
            os.close(fd)

    def _size_sync(self, path: str) -> int:
        p = self._resolve(path)
        try:
            return p.stat().st_size
        except FileNotFoundError as exc:
            raise NotFoundError("no such path", ref=path) from exc
        except PermissionError as exc:
            raise PermissionDeniedError("permission denied", ref=path) from exc

    def _exists_sync(self, path: str) -> bool:
        try:
            p = self._resolve(path)
        except NotFoundError:
            # an escaping path simply "does not exist" from the caller's
            # point of view; exists() never raises.
            return False
        try:
            return p.exists()
        except PermissionError:
            # See ObjectStore.exists()'s own docstring: a probe that can
            # raise would abort a caller's whole candidate search over one
            # irrelevant, inaccessible sibling — this reads as "not found"
            # the same as pathlib's own ENOENT/ENOTDIR/EBADF/ELOOP handling
            # inside Path.exists(), just extended to cover EACCES too.
            return False

    def _listdir_sync(self, path: str) -> list[str]:
        p = self._resolve(path)
        try:
            return sorted(entry.name for entry in p.iterdir())
        except FileNotFoundError as exc:
            raise NotFoundError("no such directory", ref=path) from exc
        except NotADirectoryError as exc:
            raise NotFoundError("not a directory", ref=path) from exc
        except PermissionError as exc:
            raise PermissionDeniedError("permission denied", ref=path) from exc
