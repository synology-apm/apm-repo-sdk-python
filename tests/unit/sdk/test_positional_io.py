"""Unit tests for ``synology_apm_repo.sdk.positional_io``.

Short writes and no-progress writes are simulated by patching the module's
per-platform ``_write_once``, so the same tests run on every platform.
"""

from __future__ import annotations

import errno
import os
import sys
import threading
from pathlib import Path

import pytest

from synology_apm_repo.sdk import positional_io
from synology_apm_repo.sdk.dedup.local_file_io import create_presized
from synology_apm_repo.sdk.positional_io import pread, pwrite

_O_BINARY = getattr(os, "O_BINARY", 0)


def _open(path: Path, flags: int = os.O_WRONLY) -> int:
    return os.open(path, flags | _O_BINARY)


class TestPwrite:
    def test_lands_bytes_at_the_given_offset(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(10))
        fd = _open(dst)
        try:
            pwrite(fd, b"hello", 3)
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(3) + b"hello" + bytes(2)

    def test_accepts_a_memoryview(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(6))
        fd = _open(dst)
        try:
            pwrite(fd, memoryview(b"abcdef")[2:5], 1)
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(1) + b"cde" + bytes(2)

    def test_continues_after_a_short_write(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        real_write_once = positional_io._write_once

        def short_write_once(fd: int, view: memoryview, offset: int) -> int:
            return real_write_once(fd, view[:2], offset)

        monkeypatch.setattr(positional_io, "_write_once", short_write_once)
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(10))
        fd = _open(dst)
        try:
            pwrite(fd, b"hello", 3)
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(3) + b"hello" + bytes(2)

    def test_a_write_that_makes_no_progress_raises_instead_of_looping(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(positional_io, "_write_once", lambda fd, view, offset: 0)
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(1))
        fd = _open(dst)
        try:
            with pytest.raises(OSError, match="no progress"):
                pwrite(fd, b"x", 0)
        finally:
            os.close(fd)

    def test_empty_data_writes_nothing(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"abc")
        fd = _open(dst)
        try:
            pwrite(fd, b"", 1)
        finally:
            os.close(fd)
        assert dst.read_bytes() == b"abc"


def test_a_negative_offset_raises_einval(tmp_path: Path) -> None:
    dst = tmp_path / "out.bin"
    dst.write_bytes(b"abc")
    fd = _open(dst, os.O_RDWR)
    try:
        with pytest.raises(OSError) as write_exc:
            pwrite(fd, b"x", -1)
        with pytest.raises(OSError) as read_exc:
            pread(fd, 1, -1)
    finally:
        os.close(fd)
    assert write_exc.value.errno == errno.EINVAL
    assert read_exc.value.errno == errno.EINVAL
    assert dst.read_bytes() == b"abc"


class TestPread:
    @pytest.fixture
    def src(self, tmp_path: Path) -> Path:
        path = tmp_path / "in.bin"
        path.write_bytes(b"0123456789")
        return path

    def _pread(self, src: Path, length: int, offset: int) -> bytes:
        fd = _open(src, os.O_RDONLY)
        try:
            return pread(fd, length, offset)
        finally:
            os.close(fd)

    def test_reads_the_bytes_at_the_given_offset(self, src: Path) -> None:
        data = self._pread(src, 4, 3)
        assert data == b"3456"
        assert type(data) is bytes

    def test_a_read_past_the_end_is_short(self, src: Path) -> None:
        assert self._pread(src, 8, 6) == b"6789"

    def test_a_read_at_or_past_the_end_is_empty(self, src: Path) -> None:
        assert self._pread(src, 4, 10) == b""
        assert self._pread(src, 4, 50) == b""

    def test_continues_after_a_short_read(self, src: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A primitive capped below ``length`` (1 GiB per ``ReadFile``, ~2 GiB
        per Linux ``pread``) must not look like the end of the file."""
        real_read_once = positional_io._read_once

        def short_read_once(fd: int, length: int, offset: int) -> bytes:
            return real_read_once(fd, min(length, 3), offset)

        monkeypatch.setattr(positional_io, "_read_once", short_read_once)
        assert self._pread(src, 8, 1) == b"12345678"
        assert self._pread(src, 20, 4) == b"456789"


@pytest.mark.skipif(sys.platform != "win32", reason="the Win32 ReadFile/WriteFile path exists only on Windows")
class TestWin32:
    @pytest.mark.parametrize(
        "data",
        [
            b"WXYZ",
            bytearray(b"WXYZ"),
            memoryview(bytearray(b"WXYZ")),
            memoryview(b"WXYZ").toreadonly(),
            memoryview(bytearray(b"..WXYZ..")).toreadonly()[2:6],
        ],
        ids=["bytes", "bytearray", "writable-view", "readonly-view", "readonly-slice"],
    )
    def test_every_buffer_kind_lands_at_its_offset(self, tmp_path: Path, data: bytes | memoryview) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(10))
        fd = _open(dst)
        try:
            pwrite(fd, data, 3)
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(3) + b"WXYZ" + bytes(3)

    def test_threads_sharing_one_fd_each_land_their_own_blocks(self, tmp_path: Path) -> None:
        threads, blocks, block = 8, 200, 4096
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(threads * blocks * block))

        def offset(t: int, i: int) -> int:
            return (i * threads + t) * block

        def write(fd: int, t: int) -> None:
            for i in range(blocks):
                pwrite(fd, bytes([t]) * block, offset(t, i))

        mismatches: list[tuple[int, int]] = []

        def read(fd: int, t: int) -> None:
            mismatches.extend((t, i) for i in range(blocks) if pread(fd, block, offset(t, i)) != bytes([t]) * block)

        for flags, work in ((os.O_WRONLY, write), (os.O_RDONLY, read)):
            fd = _open(dst, flags)
            try:
                workers = [threading.Thread(target=work, args=(fd, t)) for t in range(threads)]
                for w in workers:
                    w.start()
                for w in workers:
                    w.join()
            finally:
                os.close(fd)
        assert mismatches == []
        data = dst.read_bytes()
        assert all(data[offset(t, i)] == t for t in range(threads) for i in range(blocks))

    def test_writing_a_read_only_fd_raises_eacces(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(4))
        fd = _open(dst, os.O_RDONLY)
        try:
            with pytest.raises(OSError) as excinfo:
                pwrite(fd, b"x", 0)
        finally:
            os.close(fd)
        assert excinfo.value.errno == errno.EACCES

    def test_reading_a_write_only_fd_raises_eacces(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(4))
        fd = _open(dst)
        try:
            with pytest.raises(OSError) as excinfo:
                pread(fd, 1, 0)
        finally:
            os.close(fd)
        assert excinfo.value.errno == errno.EACCES

    @pytest.mark.parametrize("code", [39, 112])
    def test_a_full_disk_is_reported_as_enospc(self, code: int) -> None:
        if sys.platform == "win32":  # narrows for mypy: _winerror exists only on win32
            exc = positional_io._winerror(code)
            assert exc.errno == errno.ENOSPC
            assert exc.winerror == code

    def test_any_other_error_keeps_its_own_errno(self) -> None:
        if sys.platform == "win32":
            assert positional_io._winerror(5).errno == errno.EACCES

    def test_an_offset_past_4_gib_lands_in_the_high_dword(self, tmp_path: Path) -> None:
        dst = tmp_path / "big.bin"
        create_presized(dst, 5 << 30, sparse=True)
        offset = (4 << 30) + 12345
        fd = _open(dst, os.O_RDWR)
        try:
            pwrite(fd, b"HIGH", offset)
            assert pread(fd, 4, offset) == b"HIGH"
            assert pread(fd, 4, offset & 0xFFFFFFFF) == bytes(4)
        finally:
            os.close(fd)
