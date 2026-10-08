"""Unit tests for ``synology_apm_repo.sdk.dedup.local_file_io``."""

from __future__ import annotations

import errno
import os
import stat
import sys
from pathlib import Path

import pytest

from synology_apm_repo.sdk.dedup import local_file_io
from synology_apm_repo.sdk.dedup.local_file_io import (
    ZERO_FILL_BLOCK,
    create_presized,
    preallocate,
    write_zeros_at,
    zero_range,
)
from synology_apm_repo.sdk.positional_io import pwrite

_O_BINARY = getattr(os, "O_BINARY", 0)

_SIZE = 1 << 20


class TestSizeAndContents:
    @pytest.mark.parametrize("sparse", [True, False])
    def test_file_is_exactly_the_requested_size_and_reads_back_as_zeros(self, tmp_path: Path, *, sparse: bool) -> None:
        dst = tmp_path / "out.bin"
        create_presized(dst, _SIZE, sparse=sparse)
        assert dst.stat().st_size == _SIZE
        assert dst.read_bytes() == bytes(_SIZE)

    def test_zero_size_is_allowed(self, tmp_path: Path) -> None:
        dst = tmp_path / "empty.bin"
        create_presized(dst, 0, sparse=True)
        assert dst.stat().st_size == 0

    def test_an_existing_longer_file_is_replaced_not_extended(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * (_SIZE * 2))
        create_presized(dst, _SIZE, sparse=True)
        assert dst.stat().st_size == _SIZE
        assert dst.read_bytes() == bytes(_SIZE)


class TestOnOpen:
    def test_it_fires_once_the_file_exists_and_before_it_is_sized(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 10)
        seen: list[int] = []
        create_presized(dst, _SIZE, sparse=True, on_open=lambda: seen.append(dst.stat().st_size))
        assert seen == [0]

    def test_it_does_not_fire_when_opening_fails(self, tmp_path: Path) -> None:
        seen: list[None] = []
        with pytest.raises(FileNotFoundError):  # the parent directory is missing
            create_presized(tmp_path / "missing" / "out.bin", _SIZE, sparse=True, on_open=lambda: seen.append(None))
        assert seen == []


class TestWritesLandWhereExportPutsThem:
    def test_a_write_past_the_end_of_written_data_reads_back_correctly(self, tmp_path: Path) -> None:
        """A high-offset write into a pre-sized file leaves everything before it zero."""
        dst = tmp_path / "out.bin"
        create_presized(dst, _SIZE, sparse=True)

        fd = os.open(dst, os.O_WRONLY | _O_BINARY)
        try:
            os.lseek(fd, _SIZE - 8, os.SEEK_SET)
            os.write(fd, b"trailing")
        finally:
            os.close(fd)

        assert dst.stat().st_size == _SIZE
        assert dst.read_bytes() == bytes(_SIZE - 8) + b"trailing"


@pytest.mark.skipif(sys.platform != "win32", reason="sparse files are an NTFS/Win32 concept")
class TestWindowsSparseAttribute:
    """Creating a sparse file must not write its zeros, which a plain
    ``truncate`` does on Windows. The inner ``sys.platform`` checks narrow
    for mypy: ``st_file_attributes`` exists only under ``--platform win32``."""

    def test_sparse_true_marks_the_file_sparse(self, tmp_path: Path) -> None:
        dst = tmp_path / "sparse.bin"
        create_presized(dst, _SIZE, sparse=True)
        if sys.platform == "win32":
            assert dst.stat().st_file_attributes & stat.FILE_ATTRIBUTE_SPARSE_FILE

    def test_sparse_false_leaves_the_file_dense(self, tmp_path: Path) -> None:
        dst = tmp_path / "dense.bin"
        create_presized(dst, _SIZE, sparse=False)
        if sys.platform == "win32":
            assert not dst.stat().st_file_attributes & stat.FILE_ATTRIBUTE_SPARSE_FILE


class TestPreallocate:
    """``preallocate`` reserves a dense export's whole file, reporting
    ``False`` only for "this platform/filesystem can't"."""

    @staticmethod
    def _install(monkeypatch: pytest.MonkeyPatch, *, error: int | None = None) -> list[tuple[int, int, int]]:
        calls: list[tuple[int, int, int]] = []

        def fake_posix_fallocate(fd: int, offset: int, length: int) -> None:
            calls.append((fd, offset, length))
            if error is not None:
                raise OSError(error, os.strerror(error))

        monkeypatch.setattr(sys, "platform", "linux")  # the posix_fallocate path, whatever the host
        monkeypatch.setattr(os, "posix_fallocate", fake_posix_fallocate, raising=False)
        return calls

    def test_reserves_the_whole_file_from_offset_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = self._install(monkeypatch)
        assert preallocate(7, 1 << 20) is True
        assert calls == [(7, 0, 1 << 20)]

    def test_an_empty_file_needs_no_reservation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = self._install(monkeypatch)
        assert preallocate(7, 0) is True
        assert calls == []

    def test_a_platform_without_posix_fallocate_reports_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.delattr(os, "posix_fallocate", raising=False)
        assert preallocate(7, 4096) is False

    @pytest.mark.parametrize("code", [errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS, errno.EINVAL])
    def test_a_filesystem_that_cannot_reserve_reports_false(self, monkeypatch: pytest.MonkeyPatch, code: int) -> None:
        self._install(monkeypatch, error=code)
        assert preallocate(7, 4096) is False

    @pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT, errno.EFBIG, errno.EIO])
    def test_running_out_of_space_or_any_real_failure_is_raised(
        self, monkeypatch: pytest.MonkeyPatch, code: int
    ) -> None:
        self._install(monkeypatch, error=code)
        with pytest.raises(OSError) as excinfo:
            preallocate(7, 4096)
        assert excinfo.value.errno == code

    @pytest.mark.skipif(not hasattr(os, "posix_fallocate"), reason="needs a real os.posix_fallocate (Linux)")
    def test_really_reserves_the_space_and_the_file_still_reads_as_zero(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        create_presized(dst, _SIZE, sparse=False)
        fd = os.open(dst, os.O_WRONLY | _O_BINARY)
        try:
            if not preallocate(fd, _SIZE):
                pytest.skip("this filesystem cannot preallocate")
        finally:
            os.close(fd)
        if sys.platform != "win32":  # st_blocks is POSIX only
            assert os.stat(dst).st_blocks * 512 >= _SIZE  # in 512-byte units
        assert dst.read_bytes() == bytes(_SIZE)


def _open(path: Path) -> int:
    return os.open(path, os.O_WRONLY | _O_BINARY)


class TestWriteZerosAt:
    def test_overwrites_the_range_with_zeros_and_leaves_the_rest(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 10)
        fd = _open(dst)
        try:
            write_zeros_at(fd, 2, 5)
        finally:
            os.close(fd)
        assert dst.read_bytes() == b"\xff\xff" + bytes(5) + b"\xff\xff\xff"

    def test_spans_more_than_one_block_including_a_partial_tail(self, tmp_path: Path) -> None:
        length = ZERO_FILL_BLOCK * 2 + 123
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * (length + 4))
        fd = _open(dst)
        try:
            write_zeros_at(fd, 1, length)
        finally:
            os.close(fd)
        data = dst.read_bytes()
        assert data[0] == 0xFF
        assert data[1 : 1 + length] == bytes(length)
        assert data[1 + length :] == b"\xff" * 3

    def test_every_write_is_a_view_of_one_shared_zero_block(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A small gap must not allocate or copy a whole block."""
        seen: list[tuple[object, int]] = []

        def recording_pwrite(fd: int, data: bytes | memoryview, offset: int) -> None:
            assert isinstance(data, memoryview)
            seen.append((data.obj, len(data)))

        monkeypatch.setattr(local_file_io, "pwrite", recording_pwrite)
        write_zeros_at(0, 0, 5)
        write_zeros_at(0, 0, ZERO_FILL_BLOCK + 3)
        assert [size for _, size in seen] == [5, ZERO_FILL_BLOCK, 3]
        assert len({id(obj) for obj, _ in seen}) == 1

    def test_zero_length_writes_nothing(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 4)
        fd = _open(dst)
        try:
            write_zeros_at(fd, 1, 0)
        finally:
            os.close(fd)
        assert dst.read_bytes() == b"\xff" * 4


@pytest.mark.skipif(sys.platform != "darwin", reason="fcntl(F_PREALLOCATE) is macOS-specific")
class TestPreallocateOnMacos:
    def test_reserves_the_files_own_bytes_and_keeps_it_zero_and_full_size(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        create_presized(dst, _SIZE, sparse=False)
        fd = _open(dst)
        try:
            assert preallocate(fd, _SIZE) is True
            assert os.fstat(fd).st_size == _SIZE
            if sys.platform != "win32":  # narrows for mypy: st_blocks is not on win32
                assert os.fstat(fd).st_blocks * 512 >= _SIZE
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(_SIZE)

    def test_writing_the_whole_file_does_not_allocate_a_second_copy(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        create_presized(dst, _SIZE, sparse=False)
        fd = _open(dst)
        try:
            assert preallocate(fd, _SIZE) is True
            pwrite(fd, b"\xff" * _SIZE, 0)
            os.fsync(fd)
            if sys.platform != "win32":
                assert os.fstat(fd).st_blocks * 512 < 2 * _SIZE
        finally:
            os.close(fd)

    @pytest.mark.parametrize("code", [errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS, errno.EINVAL])
    def test_a_filesystem_that_cannot_reserve_reports_false_and_keeps_the_size(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
    ) -> None:
        def refuse(fd: int, cmd: int, arg: bytes) -> bytes:
            raise OSError(code, os.strerror(code))

        monkeypatch.setattr(pytest.importorskip("fcntl"), "fcntl", refuse)
        dst = tmp_path / "out.bin"
        create_presized(dst, _SIZE, sparse=False)
        fd = _open(dst)
        try:
            assert preallocate(fd, _SIZE) is False
            assert os.fstat(fd).st_size == _SIZE
        finally:
            os.close(fd)

    def test_no_space_is_raised_and_the_size_is_restored(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def full(fd: int, cmd: int, arg: bytes) -> bytes:
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

        monkeypatch.setattr(pytest.importorskip("fcntl"), "fcntl", full)
        dst = tmp_path / "out.bin"
        create_presized(dst, _SIZE, sparse=False)
        fd = _open(dst)
        try:
            with pytest.raises(OSError) as excinfo:
                preallocate(fd, _SIZE)
            assert excinfo.value.errno == errno.ENOSPC
            assert os.fstat(fd).st_size == _SIZE
        finally:
            os.close(fd)


def _open_rw(path: Path) -> int:
    return os.open(path, os.O_RDWR | _O_BINARY)


@pytest.fixture(autouse=True)
def _punching_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test starts with deallocation enabled, whatever an earlier one learned."""
    monkeypatch.setattr(local_file_io, "_punch_disabled", False)


class TestZeroRange:
    @pytest.mark.parametrize(("offset", "length"), [(4096, 8192), (0, 4096), (3, 5000), (1, 1)])
    def test_the_range_reads_as_zero_and_the_rest_is_untouched(self, tmp_path: Path, offset: int, length: int) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 16384)
        fd = _open_rw(dst)
        try:
            zero_range(fd, offset, length)
        finally:
            os.close(fd)
        data = dst.read_bytes()
        assert data[:offset] == b"\xff" * offset
        assert data[offset : offset + length] == bytes(length)
        assert data[offset + length :] == b"\xff" * (16384 - offset - length)
        assert len(data) == 16384  # the file keeps its size

    def test_a_range_of_several_blocks_is_zeroed(self, tmp_path: Path) -> None:
        length = ZERO_FILL_BLOCK * 2 + 4096
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * (length + 4096))
        fd = _open_rw(dst)
        try:
            zero_range(fd, 4096, length)
        finally:
            os.close(fd)
        data = dst.read_bytes()
        assert data[:4096] == b"\xff" * 4096
        assert data[4096:] == bytes(length)

    def test_a_zero_length_range_changes_nothing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def no_punch(fd: int, offset: int, length: int) -> None:
            raise AssertionError("must not be tried")

        monkeypatch.setattr(local_file_io, "_punch", no_punch)
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 4)
        fd = _open_rw(dst)
        try:
            zero_range(fd, 1, 0)
        finally:
            os.close(fd)
        assert dst.read_bytes() == b"\xff" * 4

    def test_a_filesystem_that_cannot_deallocate_gets_the_zeros_written_and_is_not_asked_again(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        attempts: list[int] = []

        def unsupported(fd: int, offset: int, length: int) -> None:
            attempts.append(offset)
            raise OSError(errno.EOPNOTSUPP, "no hole punching here")

        monkeypatch.setattr(local_file_io, "_punch", unsupported)
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 8192)
        fd = _open_rw(dst)
        try:
            zero_range(fd, 0, 4096)
            zero_range(fd, 4096, 4096)
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(8192)
        assert attempts == [0]  # the second call went straight to writing

    def test_a_range_the_platform_rejects_as_misaligned_is_written_and_deallocation_stays_on(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        attempts: list[int] = []

        def misaligned(fd: int, offset: int, length: int) -> None:
            attempts.append(offset)
            raise OSError(errno.EINVAL, "not aligned")

        monkeypatch.setattr(local_file_io, "_punch", misaligned)
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 8192)
        fd = _open_rw(dst)
        try:
            zero_range(fd, 1, 100)
            zero_range(fd, 4096, 100)
        finally:
            os.close(fd)
        data = dst.read_bytes()
        assert data[1:101] == bytes(100) and data[4096:4196] == bytes(100)
        assert attempts == [1, 4096]  # still tried each time

    def test_a_disabled_deallocation_is_not_tried_and_stays_disabled(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(local_file_io, "_punch_disabled", True)
        attempts: list[int] = []

        def punch(fd: int, offset: int, length: int) -> None:
            attempts.append(offset)
            raise OSError(errno.EINVAL, "not aligned")

        monkeypatch.setattr(local_file_io, "_punch", punch)
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 4)
        fd = _open_rw(dst)
        try:
            zero_range(fd, 0, 4)
            assert local_file_io._punch_disabled
        finally:
            os.close(fd)
        assert attempts == []

    def test_a_successful_deallocation_writes_no_zeros(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[tuple[str, int, int]] = []

        def punched(fd: int, offset: int, length: int) -> None:
            calls.append(("punch", offset, length))

        def write(fd: int, offset: int, length: int) -> None:
            calls.append(("write", offset, length))

        monkeypatch.setattr(local_file_io, "_punch_disabled", False)
        monkeypatch.setattr(local_file_io, "_punch", punched)
        monkeypatch.setattr(local_file_io, "write_zeros_at", write)
        zero_range(0, 0, 4096)
        assert calls == [("punch", 0, 4096)]
