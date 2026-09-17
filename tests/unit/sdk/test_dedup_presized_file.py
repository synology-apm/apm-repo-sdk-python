"""Unit tests for ``synology_apm_repo.sdk.dedup.presized_file``.

The property that matters here isn't the resulting file's contents (an
all-zero file of a given length is easy either way) but that creating it
*doesn't write those zeros* — on Windows the obvious ``truncate`` does,
at a cost proportional to the whole export's logical size. The sparse
attribute assertion below is what pins that down, so it is deliberately
platform-specific rather than skipped as an implementation detail.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

import pytest

from synology_apm_repo.sdk.dedup.presized_file import create_presized

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


class TestWritesLandWhereExportPutsThem:
    def test_a_write_past_the_end_of_written_data_reads_back_correctly(self, tmp_path: Path) -> None:
        """The export's own access shape: pre-size, then write at a high
        offset first, leaving everything before it untouched."""
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
    """``st_file_attributes`` doesn't exist under ``mypy --platform linux``/
    ``darwin`` (two of the three platforms ``make test`` runs mypy against —
    see ``pyproject.toml``'s ``[tool.mypy]``), only under ``--platform win32``
    — hence the inner ``sys.platform`` narrowing on top of the class-level
    skip."""

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
