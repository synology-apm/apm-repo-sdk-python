"""Unit tests for ``synology_apm_repo.sdk.storage.local`` — synthetic
files only, no sample repositories required.

Only ``LocalFsStore``-specific internals live here (the readahead hint,
``..``-escape prevention) — the generic ``read``/``size``/``exists``/
``listdir`` contract every ``ObjectStore`` backend shares is tested once,
parametrized across all three backends, in
``test_storage_object_store_contract.py``."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError
from synology_apm_repo.sdk.storage import local as local_mod
from synology_apm_repo.sdk.storage.local import LocalFsStore


@pytest.fixture
def store(tmp_path: Path) -> LocalFsStore:
    (tmp_path / "a.txt").write_bytes(b"0123456789")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"hello world")
    (tmp_path / "sub" / "nested").mkdir()
    return LocalFsStore(tmp_path)


async def test_read_offset_at_eof_with_no_length_returns_empty_bytes(store: LocalFsStore) -> None:
    # Distinct from test_read_short_at_eof_does_not_raise above: no
    # explicit length means _read_sync computes one itself, and an
    # offset exactly at EOF computes to 0 rather than a short positive
    # read — the length <= 0 fast path skips os.pread() entirely.
    assert await store.read("a.txt", offset=10) == b""


def test_init_raises_permission_denied_when_root_is_unstattable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real ``Path.is_dir()`` lets ``PermissionError`` through uncaught (it
    only swallows ``ENOENT``/``ENOTDIR``), so an unlistable root's parent
    must not crash ``__init__`` with a raw traceback either."""

    def raising_is_dir(self: Path) -> bool:
        raise PermissionError()

    monkeypatch.setattr(Path, "is_dir", raising_is_dir)
    with pytest.raises(PermissionDeniedError):
        LocalFsStore(tmp_path)


async def test_read_directory_raises_not_found(store: LocalFsStore) -> None:
    with pytest.raises(NotFoundError):
        await store.read("sub")


async def test_listdir_on_file_raises(store: LocalFsStore) -> None:
    with pytest.raises(NotFoundError):
        await store.listdir("a.txt")


@pytest.mark.parametrize("escaping", ["../evil", "sub/../../evil", "/etc/passwd"])
async def test_path_escape_prevented(store: LocalFsStore, escaping: str) -> None:
    assert await store.exists(escaping) is False
    with pytest.raises(NotFoundError):
        await store.read(escaping)


def test_repr_does_not_crash(store: LocalFsStore) -> None:
    assert "LocalFsStore" in repr(store)


async def test_open_raising_isadirectoryerror_raises_not_found(
    store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real ``os.open()`` succeeds on a directory on both Linux and macOS
    (it's the later read that fails — see ``test_read_directory_raises_
    not_found`` above), so ``_read_sync()``'s own catch clause around
    ``os.open()`` can only be exercised by forcing the call to raise
    directly."""

    def raising_open(path: object, *a: object, **k: object) -> int:
        raise IsADirectoryError()

    monkeypatch.setattr(os, "open", raising_open)
    with pytest.raises(NotFoundError, match="is a directory"):
        await store.read("a.txt")


async def test_open_raising_permissionerror_on_a_real_directory_raises_not_found(
    store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows raises PermissionError (not IsADirectoryError) from
    os.open() when the target is a directory -- os.path.isdir()
    disambiguates this from a genuine permission denial (see
    _read_sync's own comment). Forced here via monkeypatch since this
    project's own CI runs on Linux/macOS, where os.open() succeeds on a
    directory instead (see test_open_raising_isadirectoryerror_raises_
    not_found above), never raising PermissionError for one at all."""

    def raising_open(path: object, *a: object, **k: object) -> int:
        raise PermissionError()

    monkeypatch.setattr(os, "open", raising_open)
    with pytest.raises(NotFoundError, match="is a directory"):
        await store.read("sub")  # "sub" is a real directory in the store fixture


class TestPermissionDenied:
    """``PermissionError`` from the underlying OS call, at each of the four
    ``ObjectStore`` methods — forced via ``monkeypatch`` rather than a real
    ``chmod``, since a real permission check is unreliable when the suite
    runs as root (root ignores permission bits entirely)."""

    async def test_get_fd_raises_permission_denied(self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch) -> None:
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
        def raising_iterdir(self: Path) -> object:
            raise PermissionError()

        monkeypatch.setattr(Path, "iterdir", raising_iterdir)
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.listdir("sub")

    async def test_exists_returns_false_rather_than_raising(
        self, store: LocalFsStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unlike ``read``/``size``/``listdir``, ``exists()`` never raises —
        see ``ObjectStore.exists()``'s own docstring for why a probe over
        many candidates must not abort on one inaccessible sibling."""

        def raising_exists(self: Path) -> bool:
            raise PermissionError()

        monkeypatch.setattr(Path, "exists", raising_exists)
        assert await store.exists("sub") is False


class TestReadaheadHint:
    """``read()``'s readahead hint (``posix_fadvise(WILLNEED)`` on Linux,
    ``F_RDADVISE`` on macOS) for the merged multi-chunk reads
    ``dedup/pool.py::BucketReader.read_chunks`` now issues."""

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

    def test_hint_willneed_itself_swallows_a_bad_fd(self) -> None:
        # Direct test of the real _hint_willneed() against an fd that
        # can't possibly support a readahead hint — proves the actual
        # contract read() relies on (see its own docstring: "an
        # optimization that can't fire must never turn a working read
        # into a failure") without needing to first get a real read()
        # call into a state where the hint would fail, which isn't
        # realistically constructible (the fd read() passes it always
        # just came from a successful os.open()).
        local_mod._hint_willneed(-1, 0, 4096)  # must not raise

    async def test_a_real_large_read_still_succeeds_with_the_real_hint_wired_in(self, store: LocalFsStore) -> None:
        # End-to-end, not monkeypatched: the real _hint_willneed() fires
        # for real (this test's whole environment is real Linux/macOS)
        # and the actual read() call still returns the right bytes.
        payload = b"\xcd" * (128 << 10)
        (store.root / "big.bin").write_bytes(payload)
        assert await store.read("big.bin", 0, len(payload)) == payload

    def test_posix_fadvise_branch_on_a_platform_that_has_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Neither ``os.posix_fadvise`` nor ``os.POSIX_FADV_WILLNEED``
        exist on macOS at all (only ``hasattr(os, "posix_fadvise")``-gated
        Linux code reaches either) — this project's own CI runs on
        Linux, where this branch is exercised for real; here it's
        exercised by patching in fake attributes and forcing
        ``_HAS_POSIX_FADVISE`` on, to test the branch's own logic
        independent of which platform this happens to run on."""
        calls: list[tuple[int, int, int, object]] = []
        monkeypatch.setattr(local_mod, "_HAS_POSIX_FADVISE", True)
        monkeypatch.setattr(os, "POSIX_FADV_WILLNEED", "WILLNEED-sentinel", raising=False)
        monkeypatch.setattr(
            os,
            "posix_fadvise",
            lambda fd, offset, length, advice: calls.append((fd, offset, length, advice)),
            raising=False,
        )
        local_mod._hint_willneed(7, 100, 200)
        assert calls == [(7, 100, 200, "WILLNEED-sentinel")]

    def test_pread_fallback_reads_the_same_bytes_os_pread_would(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``os.pread`` doesn't exist on Windows — this project's own CI
        runs on Linux/macOS, where ``_HAS_PREAD`` is always on; here the
        ``lseek``+``read`` fallback is exercised directly by forcing
        ``_HAS_PREAD`` off, proving it reads the same bytes at the same
        offset ``os.pread`` would."""
        monkeypatch.setattr(local_mod, "_HAS_PREAD", False)
        payload = b"0123456789"
        (tmp_path / "f.bin").write_bytes(payload)
        fd = os.open(tmp_path / "f.bin", os.O_RDONLY)
        try:
            assert local_mod._pread(fd, 4, 3) == payload[3:7]
        finally:
            os.close(fd)
