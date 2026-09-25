"""Unit tests for ``synology_apm_repo.sdk.asynccache`` — the generic
memoizing cache every hand-rolled cache in this SDK (``Pool``'s two caches,
``BucketReaderCache``, ``DirCache``, ``CompositionRecord``'s page cache,
``verify``'s ``_CompositionReaderFactory``) is built from. Each of those only
needs to test its own wiring (right ``maxsize``, right ``fetch``); the
actual cache mechanics — hits, misses, eviction, in-flight dedup, error
propagation — are tested exactly once, here.
"""

from __future__ import annotations

import asyncio

import pytest

from synology_apm_repo.sdk.asynccache import AsyncKeyedCache


class _CountingFetcher:
    def __init__(self) -> None:
        self.calls: list[int] = []

    async def __call__(self, key: int) -> str:
        self.calls.append(key)
        return f"value-{key}"


class TestBasicResolve:
    async def test_first_resolve_fetches_and_caches(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        result = await cache.resolve(1)
        assert result == "value-1"
        assert fetcher.calls == [1]
        assert 1 in cache
        assert cache[1] == "value-1"

    async def test_second_resolve_of_the_same_key_hits_the_cache(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        await cache.resolve(1)
        await cache.resolve(1)
        assert fetcher.calls == [1]  # fetched only once

    async def test_different_keys_each_fetch_independently(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        assert await cache.resolve(1) == "value-1"
        assert await cache.resolve(2) == "value-2"
        assert fetcher.calls == [1, 2]

    async def test_a_fetch_that_resolves_to_none_is_still_a_real_cache_hit(self) -> None:
        # resolve() checks ``key in self._store``, not
        # ``self._store.get(key) is not None`` -- a fetch legitimately
        # resolving to None (a "checked, found nothing" outcome some
        # callers cache on purpose) must still count as a hit, not
        # silently re-fetch forever.
        calls: list[int] = []

        async def fetch_none(key: int) -> str | None:
            calls.append(key)
            return None

        cache: AsyncKeyedCache[int, str | None] = AsyncKeyedCache(fetch_none)
        assert await cache.resolve(1) is None
        assert await cache.resolve(1) is None
        assert calls == [1]  # fetched only once
        assert 1 in cache
        assert cache[1] is None


class TestPut:
    """``put()`` — a direct, synchronous insert for a caller that already
    has a value in hand (no fetch involved), as opposed to ``resolve()``'s
    await-a-fetch-on-miss shape."""

    async def test_put_inserts_without_fetching(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        cache.put(1, "value-1")
        assert cache[1] == "value-1"
        assert fetcher.calls == []

    async def test_resolve_after_put_is_a_cache_hit(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        cache.put(1, "value-1")
        assert await cache.resolve(1) == "value-1"
        assert fetcher.calls == []  # never fetched -- put() already settled it

    async def test_put_overwrites_an_existing_key(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        cache.put(1, "first")
        cache.put(1, "second")
        assert cache[1] == "second"

    async def test_put_respects_maxsize_eviction(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher(), maxsize=2)
        cache.put(1, "value-1")
        cache.put(2, "value-2")
        cache.put(3, "value-3")  # evicts 1 (LRU), cache stays at 2
        assert len(cache) == 2
        assert 1 not in cache
        assert 2 in cache
        assert 3 in cache

    async def test_put_refreshes_recency_the_same_way_resolve_does(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher(), maxsize=2)
        cache.put(1, "value-1")
        cache.put(2, "value-2")
        cache.put(1, "value-1-again")  # touches 1 again -> 2 is now the LRU one
        cache.put(3, "value-3")  # evicts 2, not 1
        assert 1 in cache
        assert 2 not in cache
        assert 3 in cache


class TestMappingInterface:
    async def test_len_reflects_resolved_keys_only(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        assert len(cache) == 0
        await cache.resolve(1)
        await cache.resolve(2)
        assert len(cache) == 2

    async def test_contains_is_a_pure_snapshot_check_never_fetches(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        assert (1 in cache) is False
        assert fetcher.calls == []  # ``in`` never triggered a fetch

    async def test_dict_conversion_matches_resolved_entries(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        await cache.resolve(1)
        await cache.resolve(2)
        assert dict(cache) == {1: "value-1", 2: "value-2"}

    async def test_sync_get_default_never_fetches(self) -> None:
        """``Mapping.get(key, default)`` — inherited, synchronous — must
        stay a pure lookup; ``resolve()`` is the only way to trigger a
        fetch."""
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        assert cache.get(1, "missing") == "missing"
        assert fetcher.calls == []


class TestEviction:
    async def test_unbounded_by_default_never_evicts(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        for i in range(50):
            await cache.resolve(i)
        assert len(cache) == 50

    async def test_maxsize_evicts_least_recently_used(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher(), maxsize=2)
        await cache.resolve(1)
        await cache.resolve(2)
        await cache.resolve(3)  # evicts 1 (LRU), cache stays at 2
        assert len(cache) == 2
        assert 1 not in cache
        assert 2 in cache
        assert 3 in cache

    async def test_resolving_an_existing_key_refreshes_its_recency(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher, maxsize=2)
        await cache.resolve(1)
        await cache.resolve(2)
        await cache.resolve(1)  # touches 1 again -> 2 is now the LRU one
        await cache.resolve(3)  # evicts 2, not 1
        assert 1 in cache
        assert 2 not in cache
        assert 3 in cache

    async def test_maxsize_is_a_live_mutable_attribute(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher(), maxsize=10)
        await cache.resolve(1)
        await cache.resolve(2)
        assert len(cache) == 2
        cache.maxsize = 1
        await cache.resolve(3)  # eviction now enforces the *new*, smaller cap
        assert len(cache) == 1
        assert 3 in cache

    async def test_evicted_entry_is_refetched_as_a_genuinely_new_value(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher, maxsize=1)
        await cache.resolve(1)
        await cache.resolve(2)  # evicts 1
        await cache.resolve(1)  # must be fetched again, not silently missing
        assert fetcher.calls == [1, 2, 1]


class TestPerCallFetchOverride:
    """``BucketReaderCache`` is constructed at a layer that doesn't yet know
    which ``Pool`` will resolve its misses — this is the mechanism that
    makes that possible."""

    async def test_no_fetch_bound_anywhere_raises_type_error(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache()  # no fetch at construction
        with pytest.raises(TypeError, match="no fetch function"):
            await cache.resolve(1)

    async def test_per_call_fetch_is_used_when_none_bound_at_construction(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache()
        result = await cache.resolve(1, lambda k: _CountingFetcher()(k))
        assert result == "value-1"
        assert 1 in cache

    async def test_per_call_fetch_overrides_the_one_bound_at_construction(self) -> None:
        async def constructed_fetch(key: int) -> str:
            return "from-constructor"

        async def override_fetch(key: int) -> str:
            return "from-override"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(constructed_fetch)
        result = await cache.resolve(1, override_fetch)
        assert result == "from-override"

    async def test_cache_hit_never_calls_the_per_call_fetch_at_all(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        await cache.resolve(1)

        async def should_never_run(key: int) -> str:
            raise AssertionError("must not be called on a cache hit")

        assert await cache.resolve(1, should_never_run) == "value-1"

    async def test_a_waiter_joining_an_in_flight_fetch_ignores_its_own_fetch_argument(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        owner_calls: list[int] = []

        async def owner_fetch(key: int) -> str:
            owner_calls.append(key)
            started.set()
            await release.wait()
            return "from-owner"

        async def waiter_fetch(key: int) -> str:
            raise AssertionError("the waiter's own fetch must never run")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache()
        task_a = asyncio.ensure_future(cache.resolve(1, owner_fetch))
        await started.wait()
        task_b = asyncio.ensure_future(cache.resolve(1, waiter_fetch))
        await asyncio.sleep(0)
        release.set()
        result_a, result_b = await asyncio.gather(task_a, task_b)
        assert result_a == result_b == "from-owner"
        assert owner_calls == [1]


class TestInvalidate:
    async def test_invalidate_one_key(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        await cache.resolve(1)
        await cache.resolve(2)
        cache.invalidate(1)
        assert 1 not in cache
        assert 2 in cache
        await cache.resolve(1)
        assert fetcher.calls == [1, 2, 1]  # 1 had to be fetched again

    async def test_invalidate_all(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        await cache.resolve(1)
        await cache.resolve(2)
        cache.invalidate()
        assert len(cache) == 0

    async def test_invalidate_a_never_cached_key_is_a_no_op(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        cache.invalidate(999)  # must not raise
        assert len(cache) == 0

    async def test_invalidate_during_an_in_flight_fetch_is_not_undone_by_that_fetchs_own_completion(
        self,
    ) -> None:
        """A ``resolve()`` already past the "miss" check (fetching, not
        holding the lock) when ``invalidate()`` is called for the same
        key must not get to commit its now-stale result afterward —
        that would silently undo the invalidation the moment the
        in-flight fetch finishes."""
        started = asyncio.Event()
        release = asyncio.Event()
        calls: list[int] = []

        async def fetch(key: int) -> str:
            calls.append(key)
            started.set()
            await release.wait()
            return f"stale-value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        task = asyncio.ensure_future(cache.resolve(1))
        await started.wait()  # the fetch is now genuinely in flight

        cache.invalidate(1)  # races the still-running fetch above
        release.set()
        result = await task

        assert result == "stale-value-1"  # the in-flight caller still gets its own answer
        assert 1 not in cache  # but that answer must not have landed in the cache
        assert calls == [1]

        # The next resolve() must trigger a genuinely fresh fetch, not
        # silently return the discarded stale value from the cache.
        assert await cache.resolve(1) == "stale-value-1"
        assert calls == [1, 1]


class TestInFlightDeduplication:
    async def test_concurrent_resolves_of_the_same_key_fetch_only_once(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        calls: list[int] = []

        async def fetch(key: int) -> str:
            calls.append(key)
            started.set()
            await release.wait()
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        task_a = asyncio.ensure_future(cache.resolve(1))
        await started.wait()
        task_b = asyncio.ensure_future(cache.resolve(1))  # joins the in-flight fetch
        await asyncio.sleep(0)  # let task_b actually reach the "wait on the owner's future" point
        release.set()

        result_a, result_b = await asyncio.gather(task_a, task_b)
        assert result_a == result_b == "value-1"
        assert calls == [1]  # only the owner ever called fetch

    async def test_different_keys_never_join_each_others_in_flight_fetch(self) -> None:
        release = asyncio.Event()
        calls: list[int] = []

        async def fetch(key: int) -> str:
            calls.append(key)
            await release.wait()
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        task_a = asyncio.ensure_future(cache.resolve(1))
        task_b = asyncio.ensure_future(cache.resolve(2))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(task_a, task_b)
        assert sorted(calls) == [1, 2]  # both keys genuinely fetched, independently

    async def test_an_exception_during_fetch_propagates_to_every_waiter(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def failing_fetch(key: int) -> str:
            started.set()
            await release.wait()
            raise ValueError("boom")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(failing_fetch)
        task_a = asyncio.ensure_future(cache.resolve(1))
        await started.wait()
        task_b = asyncio.ensure_future(cache.resolve(1))
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(ValueError, match="boom"):
            await task_a
        with pytest.raises(ValueError, match="boom"):
            await task_b
        assert len(cache) == 0  # a failed fetch must never be cached

    async def test_a_failed_fetch_with_no_waiters_does_not_warn_about_an_unretrieved_exception(self) -> None:
        # Regression guard for the "Future exception was never retrieved"
        # asyncio warning: the owner's own ``except`` clause already re-raises
        # the same exception, so nobody ever awaits the future when there
        # was no contention — resolve() must still mark it retrieved itself.
        async def failing_fetch(key: int) -> str:
            raise ValueError("boom")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(failing_fetch)
        with pytest.raises(ValueError, match="boom"):
            await cache.resolve(1)
        # Force a GC pass; asyncio logs "exception was never retrieved" when
        # a Future holding an exception is garbage-collected unretrieved.
        import gc

        gc.collect()

    async def test_key_can_be_resolved_again_after_a_failed_fetch(self) -> None:
        attempts = {"count": 0}

        async def flaky_fetch(key: int) -> str:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise ValueError("first attempt fails")
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(flaky_fetch)
        with pytest.raises(ValueError):
            await cache.resolve(1)
        assert await cache.resolve(1) == "value-1"  # retried, this time succeeds


class TestKnownKeys:
    async def test_empty_cache_has_no_known_keys(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        assert cache.known_keys() == []

    async def test_settled_keys_are_known(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        await cache.resolve(1)
        await cache.resolve(2)
        assert sorted(cache.known_keys()) == [1, 2]

    async def test_in_flight_keys_are_known_before_they_settle(self) -> None:
        """The one gap ``.keys()`` (the ``Mapping`` interface, settled
        entries only) has that this closes: a fetch already in flight when
        a caller like ``Repository.close()`` wants to settle everything
        outstanding must still be found, not missed because it hasn't
        landed in the ``Mapping`` view yet."""
        started = asyncio.Event()
        release = asyncio.Event()

        async def fetch(key: int) -> str:
            started.set()
            await release.wait()
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        task = asyncio.ensure_future(cache.resolve(1))
        await started.wait()

        assert cache.known_keys() == [1]
        assert list(cache) == []  # not yet in the Mapping view

        release.set()
        await task
        assert cache.known_keys() == [1]  # still known, now also settled
