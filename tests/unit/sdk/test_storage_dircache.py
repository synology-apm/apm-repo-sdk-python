"""Unit tests for ``synology_apm_repo.sdk.storage.dircache``."""

from __future__ import annotations

from typing import cast

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk.cachemanager import DEFAULT_LIMITS
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore
from synology_apm_repo.sdk.storage.dircache import DirCache, split_seq_suffix


@pytest.mark.parametrize(
    "expected",
    [
        pytest.param({"132.buk.1": ("132.buk", 1)}, id="with_suffix"),
        pytest.param({"132.buk": ("132.buk", None)}, id="without_suffix"),
        pytest.param({"c0.8": ("c0", 8), "c0": ("c0", None)}, id="composition_subfile"),
        pytest.param({"file_map.73": ("file_map", 73), "file_map": ("file_map", None)}, id="db_generation"),
        # ".inf"/".fgp" segment-index files are not sequence suffixes: the
        # extension itself is not all-digits.
        pytest.param(
            {"0.inf": ("0.inf", None), "0_0.fgp": ("0_0.fgp", None)},
            id="does_not_false_positive_on_non_numeric_extension",
        ),
    ],
)
def test_split_seq_suffix(expected: dict[str, tuple[str, int | None]]) -> None:
    assert {name: split_seq_suffix(name) for name in expected} == expected


@faithful_to(ObjectStore)
class _CountingStore:
    """Minimal ``ObjectStore`` stub that counts ``listdir()`` calls, listing
    each directory's entries in the given order with no sizes."""

    def __init__(self, entries: dict[str, list[str]]) -> None:
        self._entries = {path: [Entry(name, None) for name in names] for path, names in entries.items()}
        self.listdir_calls: list[str] = []

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        raise NotImplementedError

    async def size(self, path: str) -> int:
        raise NotImplementedError

    async def exists(self, path: str) -> bool:
        return path in self._entries

    async def close(self) -> None:
        pass

    async def listdir(self, path: str) -> list[Entry]:
        self.listdir_calls.append(path)
        return list(self._entries[path])


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

    cache.invalidate("a")
    await cache.listdir("a")
    await cache.listdir("b")
    assert backing.listdir_calls == ["a", "b", "a"]  # "b" still cached, "a" re-scanned


async def test_invalidate_all() -> None:
    backing = _CountingStore({"a": ["x"], "b": ["y"]})
    cache = DirCache(backing)
    await cache.listdir("a")
    await cache.listdir("b")

    cache.invalidate()
    await cache.listdir("a")
    await cache.listdir("b")
    assert backing.listdir_calls == ["a", "b", "a", "b"]


async def test_invalidate_specific_dir_also_drops_the_grouped_cache() -> None:
    backing = _CountingStore({"a": ["0.buk.1"], "b": ["1.buk.1"]})
    cache = DirCache(backing)
    await cache.grouped("a")
    await cache.grouped("b")
    assert backing.listdir_calls == ["a", "b"]

    cache.invalidate("a")
    await cache.grouped("a")
    await cache.grouped("b")
    assert backing.listdir_calls == ["a", "b", "a"]  # "b"'s grouped index still cached, "a" rebuilt


async def test_invalidate_all_also_drops_the_grouped_cache() -> None:
    backing = _CountingStore({"a": ["0.buk.1"], "b": ["1.buk.1"]})
    cache = DirCache(backing)
    await cache.grouped("a")
    await cache.grouped("b")

    cache.invalidate()
    await cache.grouped("a")
    await cache.grouped("b")
    assert backing.listdir_calls == ["a", "b", "a", "b"]


async def test_listdir_is_derived_from_the_grouped_entry_and_sorted() -> None:
    store = _CountingStore({"d": ["b.buk.2", "a.buk", "b.buk"]})
    cache = DirCache(cast(ObjectStore, store))

    assert await cache.listdir("d") == ["a.buk", "b.buk", "b.buk.2"]
    assert await cache.grouped("d") == {"a.buk": ["a.buk"], "b.buk": ["b.buk.2", "b.buk"]}
    assert store.listdir_calls == ["d"]  # one listing serves both views


async def test_the_cache_is_bounded_and_evicts_the_least_recently_used_directory() -> None:
    store = _CountingStore({"a": ["1"], "b": ["2"], "c": ["3"]})
    cache = DirCache(cast(ObjectStore, store), maxsize=2)

    await cache.grouped("a")
    await cache.grouped("b")
    await cache.grouped("a")  # touch: "b" is now the least recently used
    await cache.grouped("c")  # evicts "b"
    await cache.grouped("a")  # still cached
    await cache.grouped("b")  # re-listed

    assert store.listdir_calls == ["a", "b", "c", "b"]
    stats = cache.stats()
    assert (stats.size, stats.maxsize, stats.evictions) == (2, 2, 2)


async def test_default_bound_comes_from_cache_limits() -> None:
    cache = DirCache(cast(ObjectStore, _CountingStore({})))

    assert cache.stats().maxsize == DEFAULT_LIMITS.dir_scan


class _SizedStore(_CountingStore):
    """A ``_CountingStore`` whose listing carries the given sizes."""

    def __init__(self, entries: dict[str, list[Entry]]) -> None:
        super().__init__({})
        self._entries = entries


async def test_size_of_answers_from_the_listing_without_asking_the_store_again() -> None:
    store = _SizedStore({"d": [Entry("1.buk", 100), Entry("1.buk.2", 250), Entry("sub", None)]})
    cache = DirCache(cast(ObjectStore, store))

    assert await cache.size_of("d", "1.buk.2") == 250
    assert await cache.size_of("d", "1.buk") == 100
    assert await cache.size_of("d", "sub") is None  # a directory has no size
    assert await cache.size_of("d", "missing") is None
    assert await cache.grouped("d") == {"1.buk": ["1.buk", "1.buk.2"], "sub": ["sub"]}
    assert store.listdir_calls == ["d"]  # one listing serves every size


async def test_a_listing_without_sizes_gives_no_sizes_and_still_lists_once() -> None:
    store = _CountingStore({"d": ["a", "b"]})
    cache = DirCache(cast(ObjectStore, store))

    assert await cache.size_of("d", "a") is None
    assert await cache.listdir("d") == ["a", "b"]
    assert store.listdir_calls == ["d"]


async def test_invalidate_drops_the_sizes_with_the_listing() -> None:
    store = _SizedStore({"d": [Entry("a", 1)]})
    cache = DirCache(cast(ObjectStore, store))
    await cache.size_of("d", "a")

    cache.invalidate("d")
    store._entries["d"] = [Entry("a", 2)]

    assert await cache.size_of("d", "a") == 2
    assert store.listdir_calls == ["d", "d"]
