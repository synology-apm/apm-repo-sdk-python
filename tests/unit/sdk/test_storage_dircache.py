"""Unit tests for ``synology_apm_repo.sdk.storage.dircache``."""

from __future__ import annotations

from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.dircache import DirCache, split_seq_suffix


def test_split_seq_suffix_with_suffix() -> None:
    assert split_seq_suffix("132.buk.1") == ("132.buk", 1)


def test_split_seq_suffix_without_suffix() -> None:
    assert split_seq_suffix("132.buk") == ("132.buk", None)


def test_split_seq_suffix_composition_subfile() -> None:
    assert split_seq_suffix("c0.8") == ("c0", 8)
    assert split_seq_suffix("c0") == ("c0", None)


def test_split_seq_suffix_db_generation() -> None:
    assert split_seq_suffix("file_map.73") == ("file_map", 73)
    assert split_seq_suffix("file_map") == ("file_map", None)


def test_split_seq_suffix_does_not_false_positive_on_non_numeric_extension() -> None:
    # ".inf"/".fgp" segment-index files must NOT be mistaken for sequence
    # suffixes — the extension itself is not all-digits.
    assert split_seq_suffix("0.inf") == ("0.inf", None)
    assert split_seq_suffix("0_0.fgp") == ("0_0.fgp", None)


class _CountingStore:
    """Minimal ``ObjectStore`` stub that counts ``listdir()`` calls.

    All four methods are ``async def`` — ``ObjectStore`` is an async
    Protocol now, and a sync stub would both fail the ``runtime_checkable``
    shape check and hand ``DirCache`` un-awaited coroutines.
    """

    def __init__(self, entries: dict[str, list[str]]) -> None:
        self._entries = entries
        self.listdir_calls: list[str] = []

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        raise NotImplementedError

    async def size(self, path: str) -> int:
        raise NotImplementedError

    async def exists(self, path: str) -> bool:
        return path in self._entries

    async def listdir(self, path: str) -> list[str]:
        self.listdir_calls.append(path)
        return list(self._entries[path])


async def test_dircache_is_an_object_store_shape() -> None:
    # sanity: the stub satisfies the Protocol structurally
    store: ObjectStore = _CountingStore({"": []})
    assert isinstance(store, ObjectStore)
    assert await store.exists("")


async def test_listdir_is_cached() -> None:
    backing = _CountingStore({"Pool/132": ["0.buk.1", "1.buk.1"]})
    cache = DirCache(backing)

    first = await cache.listdir("Pool/132")
    second = await cache.listdir("Pool/132")

    assert first == second == ["0.buk.1", "1.buk.1"]
    assert backing.listdir_calls == ["Pool/132"]  # only one real listdir() call


async def test_grouped_builds_index_from_one_listdir_call() -> None:
    backing = _CountingStore(
        {
            "Pool/132": [
                "0.buk.1",
                "0.buk.3",
                "1.buk.1",
                "0.inf",
                "0_0.fgp",
            ]
        }
    )
    cache = DirCache(backing)

    index = await cache.grouped("Pool/132")

    assert index == {
        "0.buk": ["0.buk.1", "0.buk.3"],
        "1.buk": ["1.buk.1"],
        "0.inf": ["0.inf"],
        "0_0.fgp": ["0_0.fgp"],
    }
    assert backing.listdir_calls == ["Pool/132"]

    # calling grouped() again does not re-scan
    await cache.grouped("Pool/132")
    assert backing.listdir_calls == ["Pool/132"]


async def test_invalidate_specific_dir() -> None:
    backing = _CountingStore({"a": ["x"], "b": ["y"]})
    cache = DirCache(backing)
    await cache.listdir("a")
    await cache.listdir("b")
    assert backing.listdir_calls == ["a", "b"]

    await cache.invalidate("a")
    await cache.listdir("a")
    await cache.listdir("b")
    assert backing.listdir_calls == ["a", "b", "a"]  # "b" still cached, "a" re-scanned


async def test_invalidate_all() -> None:
    backing = _CountingStore({"a": ["x"], "b": ["y"]})
    cache = DirCache(backing)
    await cache.listdir("a")
    await cache.listdir("b")

    await cache.invalidate()
    await cache.listdir("a")
    await cache.listdir("b")
    assert backing.listdir_calls == ["a", "b", "a", "b"]


async def test_invalidate_specific_dir_also_drops_the_grouped_cache() -> None:
    # invalidate() drops both self._raw and self._grouped -- the existing
    # test_invalidate_specific_dir only re-checks listdir()/_raw; a bug
    # that left _grouped.invalidate() a no-op would still pass that one.
    backing = _CountingStore({"a": ["0.buk.1"], "b": ["1.buk.1"]})
    cache = DirCache(backing)
    await cache.grouped("a")
    await cache.grouped("b")
    assert backing.listdir_calls == ["a", "b"]

    await cache.invalidate("a")
    await cache.grouped("a")
    await cache.grouped("b")
    assert backing.listdir_calls == ["a", "b", "a"]  # "b"'s grouped index still cached, "a" rebuilt


async def test_invalidate_all_also_drops_the_grouped_cache() -> None:
    backing = _CountingStore({"a": ["0.buk.1"], "b": ["1.buk.1"]})
    cache = DirCache(backing)
    await cache.grouped("a")
    await cache.grouped("b")

    await cache.invalidate()
    await cache.grouped("a")
    await cache.grouped("b")
    assert backing.listdir_calls == ["a", "b", "a", "b"]
