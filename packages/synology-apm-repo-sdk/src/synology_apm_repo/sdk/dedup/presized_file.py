"""Creating and opening the destination file an out-of-order export writes into.

Export writes land at bucket-major, non-monotonic offsets
(``export_scheduler.py``), so the destination is created at its full
logical size before any of them run and every write is positional.

On POSIX that is one ``ftruncate``. On Windows it is up to three Win32
calls (two when not marking the file sparse), because the CRT's own
file-extending ``truncate`` writes the zeros for real rather than
recording a size — a cost proportional to the export's whole logical
size, paid before the first byte of real data.
"""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

_O_BINARY = getattr(os, "O_BINARY", 0)

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

        ``SetEndOfFile`` records the length as metadata, where the CRT's
        ``truncate`` would write ``size`` bytes of zeros. That alone only
        moves the cost rather than removing it: NTFS tracks a *valid data
        length* behind the file length and, on the first write past it,
        synchronously zeros everything in between — which a bucket-major
        export triggers almost immediately, its first write landing at an
        effectively arbitrary offset. Marking the file sparse first is
        what removes the zeroing altogether, and is also what makes
        ``sparse=True``'s "unwritten regions cost nothing" contract true
        on NTFS at all; POSIX gets both properties from ``ftruncate``
        alone.
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


def create_presized(dst: Path, size: int, *, sparse: bool) -> None:
    """Create or replace ``dst`` as an all-zero file of exactly ``size`` bytes.

    Kept as one synchronous helper so a single :func:`asyncio.to_thread`
    covers the whole open/size/close, rather than three separate thread hops.

    Args:
        dst: Destination path. Replaced outright if it already exists.
        size: Final logical size, in bytes.
        sparse: Whether unwritten regions may stay unallocated, mirroring the
            caller's own ``sparse`` export option. This only ever affects
            whether the file is *marked* sparse on Windows, where that mark is
            what stops NTFS zero-filling behind an out-of-order write; POSIX
            gets an ordinary ``ftruncate`` either way. Neither branch reserves
            space up front — with ``sparse=False`` the allocation matches the
            logical size only once the export has written every hole's zeros
            itself.
    """
    with Path(dst).open("wb") as f:
        if sys.platform == "win32":
            _presize_windows(f.fileno(), size, sparse=sparse)
        else:
            f.truncate(size)


def open_destination(dst: Path | str) -> int:
    r"""Open an already-created ``dst`` for positional writes, returning its fd.

    The one place the export's destination fd is opened, so its flags are
    stated once. ``O_BINARY`` (0 off Windows) is not optional: Windows opens
    in text mode by default, which rewrites every ``\n`` in the exported
    bytes as ``\r\n``.

    Args:
        dst: Path to an existing file, normally one :func:`create_presized`
            just sized.

    Returns:
        A writable file descriptor the caller owns and must close.
    """
    return os.open(dst, os.O_WRONLY | _O_BINARY)
