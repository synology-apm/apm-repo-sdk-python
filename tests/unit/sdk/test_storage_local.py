"""Unit tests for ``synology_apm_repo.sdk.storage.local``'s
``LocalFsStore``-specific internals (the readahead hint, ``..``-escape
prevention, OS error mapping). The generic ``ObjectStore`` contract is tested
across all four backends in ``test_storage_object_store_contract.py``."""

from __future__ import annotations

import errno
import os
import sys
from pathlib import Path

import pytest

from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError, StorageBackendError
from synology_apm_repo.sdk.storage import local as local_mod
from synology_apm_repo.sdk.storage.base import Entry
from synology_apm_repo.sdk.storage.local import LocalFsStore


@pytest.fixture
def store(tmp_path: Path) -> LocalFsStore:
    (tmp_path / "a.txt").write_bytes(b"0123456789")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"hello world")
    (tmp_path / "sub" / "nested").mkdir()
    return LocalFsStore(tmp_path)


async def test_read_offset_at_eof_with_no_length_returns_empty_bytes(store: LocalFsStore) -> None:
    # With no length, _read_sync computes one; at EOF it is 0, which skips the positional read.
    assert await store.read("a.txt", offset=10) == b""


def test_init_raises_permission_denied_when_root_is_unstattable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``Path.is_dir()`` lets ``PermissionError`` through (it swallows only
    absence-like errors such as ``ENOENT``/``ENOTDIR``), so ``__init__`` must
    map it."""

    def raising_is_dir(self: Path) -> bool:
        raise PermissionError()

    monkeypatch.setattr(Path, "is_dir", raising_is_dir)
    with pytest.raises(PermissionDeniedError, match="permission denied"):
        LocalFsStore(tmp_path)


async def test_read_directory_raises_not_found(store: LocalFsStore) -> None:
    with pytest.raises(NotFoundError, match="is a directory, not a file"):
        await store.read("sub")


async def test_listdir_on_file_raises(store: LocalFsStore) -> None:
    with pytest.raises(NotFoundError, match="not a directory"):
        await store.listdir("a.txt")


@pytest.mark.parametrize("escaping", ["../evil", "sub/../../evil", "/etc/passwd"])
async def test_path_escape_prevented(store: LocalFsStore, escaping: str) -> None:
    assert await store.exists(escaping) is False
    with pytest.raises(NotFoundError, match="path escapes store root"):
        await store.read(escaping)


def test_repr_does_not_crash(store: LocalFsStore) -> None:
    assert "LocalFsStore" in repr(store)


async def test_open_raising_isadirectoryerror_raises_not_found(
    store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``os.open()`` succeeds on a directory on Linux and macOS (the read is
    what fails), so ``_read_sync()``'s catch around ``os.open()`` is reached
    only by forcing it to raise."""

    def raising_open(path: object, *a: object, **k: object) -> int:
        raise IsADirectoryError()

    monkeypatch.setattr(os, "open", raising_open)
    with pytest.raises(NotFoundError, match="is a directory"):
        await store.read("a.txt")


async def test_open_raising_permissionerror_on_a_real_directory_raises_not_found(
    store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows raises ``PermissionError`` from ``os.open()`` on a directory;
    ``os.path.isdir()`` tells that apart from a real denial (``Path.is_dir()``
    would re-raise ``PermissionError`` itself). Forced, since Linux/macOS never
    raise it here."""

    def raising_open(path: object, *a: object, **k: object) -> int:
        raise PermissionError()

    monkeypatch.setattr(os, "open", raising_open)
    with pytest.raises(NotFoundError, match="is a directory"):
        await store.read("sub")  # "sub" is a real directory in the store fixture


class TestPermissionDenied:
    """``PermissionError`` at each of the four ``ObjectStore`` methods, forced
    via ``monkeypatch``: a real ``chmod`` is ignored when the suite runs as root."""

    async def test_read_raises_permission_denied(self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch) -> None:
        def raising_open(path: object, *a: object, **k: object) -> int:
            raise PermissionError()

        monkeypatch.setattr(os, "open", raising_open)
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.read("a.txt")

    async def test_size_raises_permission_denied(self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch) -> None:
        def raising_stat(self: Path, *a: object, **k: object) -> object:
            raise PermissionError()

        monkeypatch.setattr(Path, "stat", raising_stat)
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.size("a.txt")

    async def test_listdir_raises_permission_denied(self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch) -> None:
        def raising_scandir(path: object) -> object:
            raise PermissionError()

        monkeypatch.setattr(os, "scandir", raising_scandir)
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.listdir("sub")

    async def test_exists_returns_false_rather_than_raising(
        self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Access denied is ``False``, the same as an absent path."""

        def raising_exists(self: Path) -> bool:
            raise PermissionError()

        monkeypatch.setattr(Path, "exists", raising_exists)
        assert await store.exists("sub") is False


class TestIoError:
    """An ``OSError`` that is neither absence nor denied access is
    ``StorageBackendError`` at each method, the original kept as ``__cause__``."""

    @staticmethod
    def _eio() -> OSError:
        return OSError(errno.EIO, "Input/output error")

    async def test_read_raises_storage_backend_error(
        self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        eio = self._eio()

        def raising_pread(fd: int, length: int, offset: int) -> bytes:
            raise eio

        monkeypatch.setattr("synology_apm_repo.sdk.storage.local.pread", raising_pread)
        with pytest.raises(StorageBackendError, match="Input/output error") as excinfo:
            await store.read("a.txt")
        assert excinfo.value.__cause__ is eio

    async def test_size_listdir_and_exists_raise_storage_backend_error(
        self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        eio = self._eio()

        def raising(*a: object, **k: object) -> object:
            raise eio

        monkeypatch.setattr(Path, "stat", raising)
        monkeypatch.setattr(Path, "exists", raising)
        monkeypatch.setattr(os, "scandir", raising)
        for method, path in (("size", "a.txt"), ("listdir", "sub"), ("exists", "sub")):
            with pytest.raises(StorageBackendError, match="Input/output error"):
                await getattr(store, method)(path)


class TestReadaheadHint:
    """``read()``'s readahead hint (``posix_fadvise(WILLNEED)`` on Linux,
    ``F_RDADVISE`` on macOS) for the merged multi-chunk reads
    ``BucketReader.read_chunks`` issues."""

    async def test_fires_for_a_read_at_or_above_the_threshold(
        self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (store.root / "big.bin").write_bytes(b"\x00" * (128 << 10))
        calls: list[tuple[int, int]] = []
        monkeypatch.setattr(local_mod, "_hint_willneed", lambda fd, offset, length: calls.append((offset, length)))

        await store.read("big.bin", 0, 64 << 10)

        assert calls == [(0, 64 << 10)]

    async def test_does_not_fire_for_a_small_read(self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[tuple[int, int]] = []
        monkeypatch.setattr(local_mod, "_hint_willneed", lambda fd, offset, length: calls.append((offset, length)))

        await store.read("a.txt")  # 10 bytes, far below the threshold

        assert calls == []

    @pytest.mark.parametrize("error", [OSError(9, "bad fd"), ValueError("negative fd")])
    def test_hint_willneed_itself_swallows_a_bad_fd(self, monkeypatch: pytest.MonkeyPatch, error: Exception) -> None:
        # A hint that can't fire must never turn a working read into a failure;
        # read() always passes a valid fd, so this calls _hint_willneed() directly.
        calls: list[int] = []

        def failing_fadvise(fd: int, offset: int, length: int, advice: object) -> None:
            calls.append(fd)
            raise error

        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(os, "POSIX_FADV_WILLNEED", "WILLNEED-sentinel", raising=False)
        monkeypatch.setattr(os, "posix_fadvise", failing_fadvise, raising=False)
        local_mod._hint_willneed(-1, 0, 4096)  # must not raise
        assert calls == [-1]  # the hint was attempted, and its failure swallowed

    async def test_a_real_large_read_still_succeeds_with_the_real_hint_wired_in(self, store: LocalFsStore) -> None:
        # Not monkeypatched: the real hint fires on Linux/macOS.
        payload = b"\xcd" * (128 << 10)
        (store.root / "big.bin").write_bytes(payload)
        assert await store.read("big.bin", 0, len(payload)) == payload

    def test_posix_fadvise_branch_on_a_platform_that_has_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``os.posix_fadvise`` doesn't exist on macOS, so this forces
        ``sys.platform`` to ``"linux"`` and patches fake attributes in."""
        calls: list[tuple[int, int, int, object]] = []
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(os, "POSIX_FADV_WILLNEED", "WILLNEED-sentinel", raising=False)
        monkeypatch.setattr(
            os,
            "posix_fadvise",
            lambda fd, offset, length, advice: calls.append((fd, offset, length, advice)),
            raising=False,
        )
        local_mod._hint_willneed(7, 100, 200)
        assert calls == [(7, 100, 200, "WILLNEED-sentinel")]


class TestListdirSizes:
    async def test_files_carry_their_size_and_directories_none_sorted_by_name(self, store: LocalFsStore) -> None:
        assert await store.listdir("") == [Entry("a.txt", 10), Entry("sub", None)]
        assert await store.listdir("sub") == [Entry("b.txt", 11), Entry("nested", None)]

    @pytest.mark.parametrize(
        ("path", "match"),
        [
            pytest.param("nope", "no such directory", id="a_missing_directory"),
            pytest.param("a.txt", "not a directory", id="a_file"),
        ],
    )
    async def test_raises_not_found(self, store: LocalFsStore, path: str, match: str) -> None:
        with pytest.raises(NotFoundError, match=match):
            await store.listdir(path)

    async def test_an_entry_that_cannot_be_stat_ed_reports_no_size(
        self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_scandir = os.scandir

        class _VanishingEntry:
            def __init__(self, entry: os.DirEntry[str]) -> None:
                self.name = entry.name

            def is_file(self) -> bool:
                return True

            def stat(self) -> object:
                raise FileNotFoundError()

        class _Scan:
            def __init__(self, path: object) -> None:
                self._inner = real_scandir(path)  # type: ignore[call-overload]

            def __enter__(self) -> list[_VanishingEntry]:
                return [_VanishingEntry(e) for e in self._inner]

            def __exit__(self, *exc: object) -> None:
                self._inner.close()

        monkeypatch.setattr(os, "scandir", _Scan)
        assert await store.listdir("sub") == [Entry("b.txt", None), Entry("nested", None)]
