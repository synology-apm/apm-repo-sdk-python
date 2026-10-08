"""Positional read and write on a raw file descriptor, shared by the local
store's reads and the local-file export's writes.

POSIX uses ``os.pread``/``os.pwrite``. Windows has neither, so there each call
is a ``ReadFile``/``WriteFile`` carrying its offset in an ``OVERLAPPED``.
Neither path depends on the file pointer, so several threads can share one
``fd`` on every platform.
"""

from __future__ import annotations

import errno
import os
import sys

O_BINARY: int = getattr(os, "O_BINARY", 0)
"""``os.O_BINARY`` on Windows, where an ``os.open`` without it is in text
mode and translates line endings; ``0`` elsewhere. Every raw-fd open of
repository or export data includes it."""

if sys.platform != "win32":
    _read_once = os.pread
    _write_once = os.pwrite
else:
    import ctypes
    import msvcrt
    from ctypes import wintypes

    _MAX_IO = 1 << 30
    """Cap on one call's length, which Win32 takes as a ``DWORD``."""

    _DISK_FULL_WINERRORS = frozenset({39, 112})
    """``ERROR_HANDLE_DISK_FULL`` and ``ERROR_DISK_FULL``; ``ctypes.WinError``
    maps only 112 to ``ENOSPC``."""

    _ERROR_HANDLE_EOF = 38

    class _OVERLAPPED(ctypes.Structure):
        _fields_ = (
            ("Internal", ctypes.c_size_t),
            ("InternalHigh", ctypes.c_size_t),
            ("Offset", wintypes.DWORD),
            ("OffsetHigh", wintypes.DWORD),
            ("hEvent", wintypes.HANDLE),
        )

    class _Py_buffer(ctypes.Structure):
        _fields_ = (
            ("buf", ctypes.c_void_p),
            ("obj", ctypes.c_void_p),
            ("len", ctypes.c_ssize_t),
            ("itemsize", ctypes.c_ssize_t),
            ("readonly", ctypes.c_int),
            ("ndim", ctypes.c_int),
            ("format", ctypes.c_char_p),
            ("shape", ctypes.POINTER(ctypes.c_ssize_t)),
            ("strides", ctypes.POINTER(ctypes.c_ssize_t)),
            ("suboffsets", ctypes.POINTER(ctypes.c_ssize_t)),
            ("internal", ctypes.c_void_p),
        )

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.WriteFile.argtypes = (
        wintypes.HANDLE,
        wintypes.LPCVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(_OVERLAPPED),
    )
    _kernel32.WriteFile.restype = wintypes.BOOL
    _kernel32.ReadFile.argtypes = (
        wintypes.HANDLE,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(_OVERLAPPED),
    )
    _kernel32.ReadFile.restype = wintypes.BOOL

    _pythonapi = ctypes.pythonapi
    _pythonapi.PyObject_GetBuffer.argtypes = (ctypes.py_object, ctypes.POINTER(_Py_buffer), ctypes.c_int)
    _pythonapi.PyObject_GetBuffer.restype = ctypes.c_int
    _pythonapi.PyBuffer_Release.argtypes = (ctypes.POINTER(_Py_buffer),)
    _pythonapi.PyBuffer_Release.restype = None
    _pythonapi.PyBytes_FromStringAndSize.argtypes = (ctypes.c_void_p, ctypes.c_ssize_t)
    _pythonapi.PyBytes_FromStringAndSize.restype = ctypes.py_object

    def _overlapped_at(offset: int) -> _OVERLAPPED:
        """Rejects a negative ``offset`` as ``os.pread``/``os.pwrite`` do: the
        ``DWORD`` fields would wrap it, and ``-1`` means "append" to ``WriteFile``."""
        if offset < 0:
            raise OSError(errno.EINVAL, os.strerror(errno.EINVAL))
        return _OVERLAPPED(Offset=offset & 0xFFFFFFFF, OffsetHigh=offset >> 32)

    def _winerror(code: int) -> OSError:
        """The ``OSError`` for a failed call, with a full disk always reported
        as ``ENOSPC``. Set after construction: given a ``winerror``, ``OSError``
        derives ``errno`` from it, overriding one passed alongside."""
        exc = ctypes.WinError(code)
        if code in _DISK_FULL_WINERRORS:
            exc.errno = errno.ENOSPC
        return exc

    def _read_once(fd: int, length: int, offset: int) -> bytes:
        """Reads straight into a new ``bytes``, so a full read costs no extra
        copy."""
        size = min(length, _MAX_IO)
        if size <= 0:
            return b""
        out: bytes = _pythonapi.PyBytes_FromStringAndSize(None, size)
        done = wintypes.DWORD()
        ok = _kernel32.ReadFile(
            msvcrt.get_osfhandle(fd),
            ctypes.c_char_p(out),
            size,
            ctypes.byref(done),
            ctypes.byref(_overlapped_at(offset)),
        )
        if not ok:
            code = ctypes.get_last_error()
            if code == _ERROR_HANDLE_EOF:
                return b""
            raise _winerror(code)
        return out if done.value == size else out[: done.value]

    def _write_once(fd: int, view: memoryview, offset: int) -> int:
        """Addresses ``view`` through the C buffer protocol: ``ctypes`` alone
        can only address a writable buffer, and every export payload is a
        read-only view."""
        buf = _Py_buffer()
        _pythonapi.PyObject_GetBuffer(view, ctypes.byref(buf), 0)
        try:
            done = wintypes.DWORD()
            ok = _kernel32.WriteFile(
                msvcrt.get_osfhandle(fd),
                buf.buf,
                min(buf.len, _MAX_IO),
                ctypes.byref(done),
                ctypes.byref(_overlapped_at(offset)),
            )
        finally:
            _pythonapi.PyBuffer_Release(ctypes.byref(buf))
        if not ok:
            raise _winerror(ctypes.get_last_error())
        return done.value


def pread(fd: int, length: int, offset: int) -> bytes:
    """Reads ``length`` bytes at ``offset``, continuing after a short read:
    fewer only when the file ends first, ``b""`` at or past its end.

    Raises:
        OSError: The read failed.
    """
    data = _read_once(fd, length, offset)
    if len(data) >= length or not data:
        return data
    parts = [data]
    got = len(data)
    while got < length:
        more = _read_once(fd, length - got, offset + got)
        if not more:
            break
        parts.append(more)
        got += len(more)
    return b"".join(parts)


def pwrite(fd: int, data: bytes | memoryview, offset: int) -> None:
    """Writes all of ``data`` at ``offset``, continuing after a short write.

    Raises:
        OSError: The write failed, or made no progress.
    """
    view = memoryview(data).cast("B")
    while view:
        written = _write_once(fd, view, offset)
        if written <= 0:
            raise OSError(errno.EIO, "write made no progress")
        view = view[written:]
        offset += written
