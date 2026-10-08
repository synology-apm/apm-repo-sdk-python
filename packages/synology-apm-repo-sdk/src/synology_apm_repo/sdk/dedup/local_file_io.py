"""Primitives for a local-file export destination: create it at full size,
reserve its space, open it, and zero a range of it.

Export writes land at bucket-major, non-monotonic offsets, so the destination
is created at its full logical size first and every write is positional
(``positional_io.pwrite``).

On POSIX creating the file is one ``ftruncate``. On Windows it is up to three
Win32 calls, because the CRT's file-extending ``truncate`` writes the zeros
for real, at a cost proportional to the whole logical size.
"""

from __future__ import annotations

import ctypes
import errno
import functools
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

from ..positional_io import O_BINARY, pwrite

_PREALLOCATE_UNSUPPORTED = {errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS, errno.EINVAL}
"""``preallocate`` failures that mean "this filesystem can't", not "it
failed": the caller falls back to writing the zeros."""

if sys.platform == "win32":
    import msvcrt
    from ctypes import wintypes

    _FSCTL_SET_SPARSE = 0x000900C4
    _FILE_BEGIN = 0

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.DeviceIoControl.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPVOID,
    ]
    _kernel32.DeviceIoControl.restype = wintypes.BOOL
    _kernel32.SetFilePointerEx.argtypes = [
        wintypes.HANDLE,
        ctypes.c_longlong,
        ctypes.POINTER(ctypes.c_longlong),
        wintypes.DWORD,
    ]
    _kernel32.SetFilePointerEx.restype = wintypes.BOOL
    _kernel32.SetEndOfFile.argtypes = [wintypes.HANDLE]
    _kernel32.SetEndOfFile.restype = wintypes.BOOL

    def _presize_windows(fileno: int, size: int, *, sparse: bool) -> None:
        """Set ``fileno``'s length to ``size`` without writing its bytes.

        ``SetEndOfFile`` alone only defers the cost: NTFS zeros everything
        between its *valid data length* and the first write past it, which a
        bucket-major export triggers at once. Marking the file sparse first
        avoids that zeroing and makes ``sparse=True``'s "unwritten regions
        cost nothing" contract hold on NTFS.
        """
        handle = wintypes.HANDLE(msvcrt.get_osfhandle(fileno))
        if sparse:
            # Best-effort: FAT32/exFAT and some network targets support no
            # sparse files at all, and a dense file is still correct here.
            returned = wintypes.DWORD()
            _kernel32.DeviceIoControl(handle, _FSCTL_SET_SPARSE, None, 0, None, 0, ctypes.byref(returned), None)
        if not _kernel32.SetFilePointerEx(handle, ctypes.c_longlong(size), None, _FILE_BEGIN):
            raise ctypes.WinError(ctypes.get_last_error())
        if not _kernel32.SetEndOfFile(handle):
            raise ctypes.WinError(ctypes.get_last_error())


def create_presized(dst: Path, size: int, *, sparse: bool, on_open: Callable[[], None] | None = None) -> None:
    """Create or replace ``dst`` as an all-zero file of exactly ``size`` bytes.

    Synchronous; call it through one ``asyncio.to_thread``.

    Args:
        dst: Destination path. Replaced outright if it already exists.
        size: Final logical size, in bytes.
        sparse: Whether unwritten regions may stay unallocated. Only affects
            Windows, where marking the file sparse stops NTFS zero-filling
            behind an out-of-order write; POSIX runs the same ``ftruncate``
            either way. Neither branch reserves space (see ``preallocate``).
        on_open: Called once ``dst`` is created or truncated, before it is
            sized; not called if opening it fails, which leaves ``dst`` as it
            was.
    """
    with Path(dst).open("wb") as f:
        if on_open is not None:
            on_open()
        if sys.platform == "win32":
            _presize_windows(f.fileno(), size, sparse=sparse)
        else:
            f.truncate(size)


if sys.platform == "darwin":
    import fcntl
    import struct

    _F_PREALLOCATE = 42  # <fcntl.h>; the fcntl module does not export it
    _F_ALLOCATEALL = 0x4
    _F_PEOFPOSMODE = 3
    _FSTORE = struct.Struct("IiQQq")  # struct fstore: flags, posmode, offset, length, bytesalloc

    def _reserve_on_macos(fd: int, size: int) -> bool:
        """``fcntl(F_PREALLOCATE)`` reserves space *past the current end of
        file*, so the file is emptied first and re-extended afterwards, making
        the reservation back the file's own bytes."""
        os.ftruncate(fd, 0)
        try:
            fcntl.fcntl(fd, _F_PREALLOCATE, _FSTORE.pack(_F_ALLOCATEALL, _F_PEOFPOSMODE, 0, size, 0))
        except OSError as exc:
            if exc.errno in _PREALLOCATE_UNSUPPORTED:
                return False
            raise
        finally:
            os.ftruncate(fd, size)
        return True


def preallocate(fd: int, size: int) -> bool:
    """Reserve disk space for all ``size`` bytes of the file behind ``fd``, so
    a dense export fails before decoding if there is no room. Reserved,
    never-written ranges read as zero, so on ``True`` no zero-fill is needed.

    Uses ``posix_fallocate`` where the platform has one and
    ``fcntl(F_PREALLOCATE)`` on macOS.

    Args:
        fd: A file opened for writing whose content is all zero (a file
            ``create_presized`` just made); on macOS it is emptied and
            re-extended to ``size`` in the process.
        size: The file's logical size, in bytes.

    Returns:
        ``True`` once the space is reserved (or there is nothing to reserve);
        ``False`` when this platform or filesystem cannot reserve it, in which
        case nothing was changed and the zeros must be written instead.

    Raises:
        OSError: Out of space or quota, or any other real failure — only
            "cannot reserve on this platform/filesystem" is reported as
            ``False``.
    """
    if size <= 0:
        return True
    if sys.platform == "darwin":
        return _reserve_on_macos(fd, size)
    allocate = getattr(os, "posix_fallocate", None)
    if allocate is None:
        return False
    try:
        allocate(fd, 0, size)
    except OSError as exc:
        if exc.errno in _PREALLOCATE_UNSUPPORTED:
            return False
        raise
    return True


def open_destination(dst: Path | str) -> int:
    r"""Open an already-created ``dst`` for positional writes, returning its fd.

    ``O_BINARY`` (0 off Windows) is required: Windows' default text mode
    rewrites ``\n`` as ``\r\n`` in the exported bytes.

    Args:
        dst: Path to an existing file, normally one ``create_presized``
            just sized.

    Returns:
        A writable file descriptor the caller owns and must close.
    """
    return os.open(dst, os.O_WRONLY | O_BINARY)


ZERO_FILL_BLOCK = 1 << 20
"""Size of one zero-fill ``pwrite``."""


@functools.cache
def _zero_block() -> memoryview:
    return memoryview(bytes(ZERO_FILL_BLOCK))


def write_zeros_at(fd: int, offset: int, length: int) -> None:
    """Writes ``length`` zero bytes starting at ``offset``, in
    ``ZERO_FILL_BLOCK``-sized ``pwrite`` calls."""
    block = _zero_block()
    remaining = length
    pos = offset
    while remaining > 0:
        take = min(remaining, ZERO_FILL_BLOCK)
        pwrite(fd, block[:take], pos)
        pos += take
        remaining -= take


_PUNCH_UNSUPPORTED = {errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS}
"""``_punch`` failures that mean "this filesystem can't", after which it is
not tried again."""

_punch_disabled = False

_FALLOC_FL_KEEP_SIZE = 0x01
_FALLOC_FL_PUNCH_HOLE = 0x02
_F_PUNCHHOLE = 99
"""macOS's ``fcntl`` command, which ``fcntl`` does not export."""


@functools.cache
def _linux_fallocate() -> Callable[[int, int, int, int], int]:
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "fallocate64", None) or libc.fallocate
    function.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_longlong, ctypes.c_longlong]
    function.restype = ctypes.c_int
    return cast("Callable[[int, int, int, int], int]", function)


def _punch(fd: int, offset: int, length: int) -> None:
    """Deallocates ``[offset, offset + length)``, which then reads as zero.

    Raises:
        OSError: The platform or filesystem cannot, or the range is not
            aligned as it needs (4096 bytes on macOS).
    """
    if sys.platform == "darwin":
        import fcntl
        import struct

        fcntl.fcntl(fd, _F_PUNCHHOLE, struct.pack("=IIqq", 0, 0, offset, length))
    elif sys.platform.startswith("linux"):
        try:
            fallocate = _linux_fallocate()
        except (AttributeError, OSError) as exc:
            raise OSError(errno.ENOSYS, "this libc has no fallocate") from exc
        if fallocate(fd, _FALLOC_FL_PUNCH_HOLE | _FALLOC_FL_KEEP_SIZE, offset, length) != 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))
    else:
        raise OSError(errno.ENOTSUP, "no way to deallocate a range here")


def zero_range(fd: int, offset: int, length: int) -> None:
    """Makes ``[offset, offset + length)`` of the file behind ``fd`` read as
    zero: deallocated where the platform and filesystem can (nothing is
    written), else overwritten in ``ZERO_FILL_BLOCK``-sized ``pwrite`` calls.
    macOS deallocates only a 4096-aligned range; another is written."""
    global _punch_disabled
    if length <= 0:
        return
    if not _punch_disabled:
        try:
            _punch(fd, offset, length)
        except OSError as exc:
            if exc.errno in _PUNCH_UNSUPPORTED:
                _punch_disabled = True
        else:
            return
    write_zeros_at(fd, offset, length)
