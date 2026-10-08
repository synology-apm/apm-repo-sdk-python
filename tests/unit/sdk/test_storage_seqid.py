"""Unit tests for ``synology_apm_repo.sdk.storage.seqid``."""

from __future__ import annotations

from pathlib import Path

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.seqid import resolve_seq_file, resolve_seq_path, resolve_seq_size


@faithful_to(ObjectStore)
class _FakeStore:
    """A minimal, listdir-only ``ObjectStore`` fake — ``resolve_seq_path``
    only ever drives ``DirCache`` through ``listdir()``."""

    def __init__(self, entries: dict[str, list[str]]) -> None:
        self._entries = entries

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        raise NotImplementedError

    async def size(self, path: str) -> int:
        raise NotImplementedError

    async def exists(self, path: str) -> bool:
        raise NotImplementedError

    async def close(self) -> None:
        pass

    async def listdir(self, path: str) -> list[Entry]:
        return sorted(Entry(name, None) for name in self._entries[path])


def test_largest_suffix_wins() -> None:
    index = {"0.buk": ["0.buk.1", "0.buk.3", "0.buk.2"]}
    assert resolve_seq_file(index, "0.buk") == "0.buk.3"


def test_bare_name_wins_when_alone() -> None:
    index = {"0.buk": ["0.buk"]}
    assert resolve_seq_file(index, "0.buk") == "0.buk"


def test_suffixed_wins_over_bare() -> None:
    # A bare name coexisting with suffixed generations is a stale artifact:
    # the largest suffix wins (FORMAT-SPEC.md: Sequence-id suffix mechanism).
    index = {"0.buk": ["0.buk", "0.buk.5"]}
    assert resolve_seq_file(index, "0.buk") == "0.buk.5"


def test_missing_logical_name_raises_not_found() -> None:
    index: dict[str, list[str]] = {"0.buk": ["0.buk.1"]}
    with pytest.raises(NotFoundError, match="no file matches logical name"):
        resolve_seq_file(index, "9.buk")


def test_empty_candidate_list_raises_not_found() -> None:
    index: dict[str, list[str]] = {"0.buk": []}
    with pytest.raises(NotFoundError, match="no file matches logical name"):
        resolve_seq_file(index, "0.buk")


def test_ref_override_is_used_verbatim_in_the_raised_not_found() -> None:
    index: dict[str, list[str]] = {"0.buk": ["0.buk.1"]}
    with pytest.raises(NotFoundError, match="no file matches logical name") as exc_info:
        resolve_seq_file(index, "9.buk", ref="custom/ref/path")
    assert exc_info.value.ref == "custom/ref/path"


def test_ref_defaults_to_the_logical_name_when_omitted() -> None:
    index: dict[str, list[str]] = {"0.buk": ["0.buk.1"]}
    with pytest.raises(NotFoundError, match="no file matches logical name") as exc_info:
        resolve_seq_file(index, "9.buk")
    assert exc_info.value.ref == "9.buk"


class TestResolveSeqPath:
    async def test_picks_the_largest_suffix_and_joins_with_dir_path(self) -> None:
        store = _FakeStore({"dir": ["0.buk.1", "0.buk.3", "0.buk.2"]})
        dir_cache = DirCache(store)
        assert await resolve_seq_path(dir_cache, "dir", "0.buk") == "dir/0.buk.3"

    async def test_bare_name_resolves_when_alone(self) -> None:
        store = _FakeStore({"dir": ["0.buk"]})
        dir_cache = DirCache(store)
        assert await resolve_seq_path(dir_cache, "dir", "0.buk") == "dir/0.buk"

    async def test_missing_logical_name_raises_not_found_with_joined_ref(self) -> None:
        store = _FakeStore({"dir": ["0.buk.1"]})
        dir_cache = DirCache(store)
        with pytest.raises(NotFoundError, match="no file matches logical name") as exc_info:
            await resolve_seq_path(dir_cache, "dir", "9.buk")
        assert exc_info.value.ref == "dir/9.buk"

    async def test_empty_dir_path_still_joins_correctly(self) -> None:
        # A repository root with no prefix: no leading "/".
        store = _FakeStore({"": ["0.buk.2"]})
        dir_cache = DirCache(store)
        assert await resolve_seq_path(dir_cache, "", "0.buk") == "0.buk.2"


class TestResolveSeqSize:
    async def test_returns_the_listed_size_of_the_file_resolve_seq_path_picks(self, tmp_path: Path) -> None:
        (tmp_path / "d").mkdir()
        (tmp_path / "d" / "7.buk").write_bytes(b"x" * 10)
        (tmp_path / "d" / "7.buk.1").write_bytes(b"x" * 20)
        (tmp_path / "d" / "7.buk.3").write_bytes(b"x" * 30)
        cache = DirCache(LocalFsStore(tmp_path))

        assert await resolve_seq_path(cache, "d", "7.buk") == "d/7.buk.3"
        assert await resolve_seq_size(cache, "d", "7.buk") == 30

    async def test_no_match_raises_not_found(self, tmp_path: Path) -> None:
        (tmp_path / "d").mkdir()

        with pytest.raises(NotFoundError, match="no file matches logical name"):
            await resolve_seq_size(DirCache(LocalFsStore(tmp_path)), "d", "7.buk")
