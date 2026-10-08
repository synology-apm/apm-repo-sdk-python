"""Unit tests for ``synology_apm_repo.sdk.cachemanager`` — the registry that
creates caches, orders their invalidation, and reports their counters. The
cache mechanics themselves are covered once in ``test_asynccache.py``."""

from __future__ import annotations

import asyncio

import pytest

from synology_apm_repo.sdk.asynccache import AsyncKeyedCache, CacheStats
from synology_apm_repo.sdk.cachemanager import DEFAULT_LIMITS, CacheLimits, CacheManager


async def _fetch(key: int) -> str:
    return f"value-{key}"


def _stats(name: str) -> dict[str, CacheStats]:
    return {name: CacheStats(size=0, maxsize=1, hits=0, misses=0, evictions=0)}


class TestKeyed:
    async def test_creates_a_working_bounded_cache_and_registers_it(self) -> None:
        manager = CacheManager()
        cache: AsyncKeyedCache[int, str] = manager.keyed("numbers", _fetch, maxsize=2)

        assert await cache.resolve(1) == "value-1"
        assert manager.names() == ["numbers"]
        assert manager.stats()["numbers"].maxsize == 2
        assert manager.stats()["numbers"].misses == 1

    def test_an_unbounded_cache_must_say_what_bounds_it(self) -> None:
        manager = CacheManager()

        with pytest.raises(ValueError, match="bounded_by"):
            manager.keyed("loose", maxsize=None)

        manager.keyed("closed", maxsize=None, bounded_by="a closed key set")
        assert manager.bounds() == {"closed": "a closed key set"}

    def test_a_name_can_only_be_registered_once(self) -> None:
        manager = CacheManager()
        manager.keyed("a", maxsize=1)

        with pytest.raises(ValueError, match="already registered"):
            manager.keyed("a", maxsize=1)
        with pytest.raises(ValueError, match="already registered"):
            manager.register("a", lambda: None, dict, bounded_by="x")

    async def test_invalidate_drops_the_entries_but_keeps_the_counters(self) -> None:
        manager = CacheManager()
        cache: AsyncKeyedCache[int, str] = manager.keyed("numbers", _fetch, maxsize=4)
        await cache.resolve(1)

        await manager.invalidate("numbers")

        assert len(cache) == 0
        assert manager.stats()["numbers"].misses == 1

    async def test_invalidate_waits_for_a_fetch_in_flight_and_never_stores_its_result(self) -> None:
        gate = asyncio.Event()

        async def slow(key: int) -> str:
            await gate.wait()
            return f"value-{key}"

        manager = CacheManager()
        cache: AsyncKeyedCache[int, str] = manager.keyed("slow", slow, maxsize=4)
        resolving = asyncio.create_task(cache.resolve(1))
        await asyncio.sleep(0)

        invalidating = asyncio.create_task(manager.invalidate("slow"))
        await asyncio.sleep(0)
        assert not invalidating.done()  # blocked on the in-flight fetch
        gate.set()
        await invalidating

        assert await resolving == "value-1"  # the waiter still gets its value
        assert len(cache) == 0  # but a result from before the invalidation is not kept

    async def test_a_failed_fetch_in_flight_is_reported_as_an_exception_group(self) -> None:
        gate = asyncio.Event()

        async def failing(key: int) -> str:
            await gate.wait()
            raise OSError("boom")

        manager = CacheManager()
        cache: AsyncKeyedCache[int, str] = manager.keyed("failing", failing, maxsize=4)
        resolving = asyncio.create_task(cache.resolve(1))
        await asyncio.sleep(0)
        invalidating = asyncio.create_task(manager.invalidate("failing"))
        await asyncio.sleep(0)
        gate.set()

        with pytest.raises(OSError, match="boom"):
            await resolving
        with pytest.raises(
            ExceptionGroup, match=r"CacheManager\.invalidate\(\) failed for one or more caches"
        ) as exc_info:
            await invalidating
        assert isinstance(exc_info.value.exceptions[0], ExceptionGroup)

    async def test_on_invalidate_replaces_the_default_hook(self) -> None:
        closed: list[str] = []

        async def close_all(cache: AsyncKeyedCache[int, str]) -> None:
            closed.extend(cache.values())
            cache.invalidate()

        manager = CacheManager()
        cache: AsyncKeyedCache[int, str] = manager.keyed("owned", _fetch, maxsize=4, on_invalidate=close_all)
        await cache.resolve(7)

        await manager.invalidate_all()

        assert closed == ["value-7"]
        assert len(cache) == 0


class TestRegister:
    def test_bounded_by_is_required(self) -> None:
        with pytest.raises(ValueError, match="bounded_by"):
            CacheManager().register("owner", lambda: None, dict, bounded_by="")

    async def test_accepts_a_sync_or_an_async_invalidate(self) -> None:
        calls: list[str] = []

        async def async_hook() -> None:
            calls.append("async")

        manager = CacheManager()
        manager.register("sync", lambda: calls.append("sync"), lambda: _stats("sync"), bounded_by="x")
        manager.register("async", async_hook, lambda: _stats("async"), bounded_by="x")

        await manager.invalidate_all()

        assert sorted(calls) == ["async", "sync"]

    def test_stats_merges_what_every_owner_reports(self) -> None:
        manager = CacheManager()
        manager.register("pool", lambda: None, lambda: {**_stats("pool.a"), **_stats("pool.b")}, bounded_by="x")
        manager.keyed("keyed", maxsize=1)

        assert sorted(manager.stats()) == ["keyed", "pool.a", "pool.b"]


class TestInvalidateOrder:
    async def test_runs_last_registered_first_so_dependents_go_before_what_they_depend_on(self) -> None:
        calls: list[str] = []
        manager = CacheManager()
        for name in ("connections", "tables", "streams"):
            manager.register(name, lambda n=name: calls.append(n), dict, bounded_by="x")  # type: ignore[misc]

        await manager.invalidate_all()

        assert calls == ["streams", "tables", "connections"]

    async def test_a_named_subset_keeps_that_order(self) -> None:
        calls: list[str] = []
        manager = CacheManager()
        for name in ("a", "b", "c"):
            manager.register(name, lambda n=name: calls.append(n), dict, bounded_by="x")  # type: ignore[misc]

        await manager.invalidate("a", "c")

        assert calls == ["c", "a"]

    async def test_an_unknown_name_raises_before_anything_is_invalidated(self) -> None:
        calls: list[str] = []
        manager = CacheManager()
        manager.register("a", lambda: calls.append("a"), dict, bounded_by="x")

        with pytest.raises(KeyError, match="nope"):
            await manager.invalidate("a", "nope")

        assert calls == []

    async def test_every_owner_is_attempted_and_the_failures_reported_together(self) -> None:
        calls: list[str] = []

        def boom() -> None:
            raise OSError("boom")

        manager = CacheManager()
        manager.register("first", lambda: calls.append("first"), dict, bounded_by="x")
        manager.register("bad", boom, dict, bounded_by="x")
        manager.register("last", lambda: calls.append("last"), dict, bounded_by="x")

        with pytest.raises(
            ExceptionGroup, match=r"CacheManager\.invalidate\(\) failed for one or more caches"
        ) as exc_info:
            await manager.invalidate_all()

        assert calls == ["last", "first"]  # the failure in the middle did not stop the rest
        assert [type(e) for e in exc_info.value.exceptions] == [OSError]


def test_default_limits_match_the_documented_bounds() -> None:
    assert CacheLimits() == DEFAULT_LIMITS
    assert DEFAULT_LIMITS.bucket_readers == 16
    assert DEFAULT_LIMITS.bucket_readers_interactive == 64
    assert DEFAULT_LIMITS.chunks == 4096
    assert DEFAULT_LIMITS.composition_records == 64
    assert DEFAULT_LIMITS.composition_pages == 128
    assert DEFAULT_LIMITS.saas_streams == 8
    assert DEFAULT_LIMITS.verify_bucket_readers == 16
