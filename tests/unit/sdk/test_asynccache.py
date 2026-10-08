"""Unit tests for ``synology_apm_repo.sdk.asynccache`` — the generic
memoizing cache the SDK's caches (``Pool``'s, ``BucketReaderCache``,
``DirCache``, ``CompositionRecord``'s page cache, ``DedupRepo``'s, ...) are
built from. Each of those only needs to test its own wiring (right ``maxsize``, right
``fetch``); the actual cache mechanics — hits, misses, eviction, in-flight
dedup, error propagation — are tested exactly once, here.
"""

from __future__ import annotations

import asyncio
import gc
import logging

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
        assert fetcher.calls == [1]

    async def test_different_keys_each_fetch_independently(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)
        assert await cache.resolve(1) == "value-1"
        assert await cache.resolve(2) == "value-2"
        assert fetcher.calls == [1, 2]

    async def test_a_fetch_that_resolves_to_none_is_still_a_real_cache_hit(self) -> None:
        # A cached ``None`` is a "checked, found nothing" outcome some callers cache on purpose.
        calls: list[int] = []

        async def fetch_none(key: int) -> str | None:
            calls.append(key)
            return None

        cache: AsyncKeyedCache[int, str | None] = AsyncKeyedCache(fetch_none)
        assert await cache.resolve(1) is None
        assert await cache.resolve(1) is None
        assert calls == [1]
        assert 1 in cache
        assert cache[1] is None


class TestPut:
    """``put()``: a synchronous insert of a value already in hand, no fetch."""

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


class TestPutMany:
    async def test_stores_every_item_in_order_and_evicts_the_oldest_once_at_the_end(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(maxsize=2)
        cache.put(0, "a")
        cache.put_many([(1, "b"), (2, "c"), (3, "d")])
        assert dict(cache) == {2: "c", 3: "d"}
        assert cache.evictions == 2

    async def test_an_existing_key_is_overwritten_and_made_most_recent(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(maxsize=2)
        cache.put_many([(0, "a"), (1, "b")])
        cache.put_many([(0, "a2"), (2, "c")])
        assert dict(cache) == {0: "a2", 2: "c"}


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
        assert fetcher.calls == []

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


class TestLookup:
    async def test_a_hit_counts_and_becomes_most_recently_used(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher(), maxsize=2)
        await cache.resolve(1)
        await cache.resolve(2)
        hits_before = cache.hits

        assert cache.lookup(1, None) == "value-1"
        cache.put(3, "value-3")  # evicts the least recently used: 2, not 1

        assert cache.hits == hits_before + 1
        assert list(cache) == [1, 3]

    async def test_a_miss_returns_the_default_counts_and_never_fetches(self) -> None:
        fetcher = _CountingFetcher()
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetcher)

        assert cache.lookup(1, "missing") == "missing"
        assert cache.misses == 1
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

    async def test_no_fetch_needed_at_all_for_an_already_settled_key(self) -> None:
        """A cache with no fetch bound (``DedupRepo._composition_records``)
        still serves a settled key."""
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache()
        cache.put(1, "value-1")
        assert await cache.resolve(1) == "value-1"

    async def test_no_fetch_needed_at_all_to_join_an_in_flight_owner(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def owner_fetch(key: int) -> str:
            started.set()
            await release.wait()
            return "from-owner"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache()
        task_a = asyncio.ensure_future(cache.resolve(1, owner_fetch))
        await started.wait()
        task_b = asyncio.ensure_future(cache.resolve(1))  # no fetch at all
        await asyncio.sleep(0)
        release.set()
        result_a, result_b = await asyncio.gather(task_a, task_b)
        assert result_a == result_b == "from-owner"


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
        cache.invalidate(999)
        assert len(cache) == 0

    async def test_invalidate_during_an_in_flight_fetch_is_not_undone_by_that_fetchs_own_completion(
        self,
    ) -> None:
        """A fetch already in flight when its key is invalidated must not commit
        its now-stale result afterward."""
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
        await started.wait()

        cache.invalidate(1)
        release.set()
        result = await task

        assert result == "stale-value-1"  # the in-flight caller still gets its own answer
        assert 1 not in cache  # but that answer must not have landed in the cache
        assert calls == [1]

        # The next resolve() must trigger a genuinely fresh fetch, not
        # silently return the discarded stale value from the cache.
        assert await cache.resolve(1) == "stale-value-1"
        assert calls == [1, 1]

    async def test_invalidate_all_during_an_in_flight_fetch_is_not_undone_either(self) -> None:
        release = asyncio.Event()

        async def fetch(key: int) -> str:
            await release.wait()
            return f"stale-value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        task = asyncio.ensure_future(cache.resolve(1))
        await asyncio.sleep(0)
        cache.invalidate()
        release.set()
        assert await task == "stale-value-1"
        assert 1 not in cache

    async def test_bookkeeping_does_not_grow_with_every_key_ever_fetched(self) -> None:
        """An evicting cache sees an unbounded key space over a long session
        (one key per chunk read); its per-key invalidation bookkeeping must
        stay bounded by what is in flight, not by every key ever fetched."""
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher(), maxsize=4)
        for key in range(1000):
            await cache.resolve(key)
            cache.invalidate(key - 1)
        cache.invalidate()
        assert len(cache) == 0
        assert not cache._stale


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

    async def test_a_cancelled_owner_hands_the_fetch_to_a_joiner_that_was_not_cancelled(self) -> None:
        """One TUI preview cancelled mid-fetch must not fail another reader
        waiting on the same shared entry: the joiner fetches it anew."""
        calls: list[int] = []
        first_started = asyncio.Event()

        async def fetch(key: int) -> str:
            calls.append(key)
            if len(calls) == 1:
                first_started.set()
                await asyncio.Event().wait()
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        owner = asyncio.ensure_future(cache.resolve(1))
        await first_started.wait()
        joiner = asyncio.ensure_future(cache.resolve(1))
        await asyncio.sleep(0)  # the joiner is now awaiting the owner's fetch

        owner.cancel()

        assert await joiner == "value-1"
        assert calls == [1, 1]
        assert cache[1] == "value-1"
        with pytest.raises(asyncio.CancelledError):
            await owner

    async def test_a_cancelled_joiner_leaves_the_fetch_and_the_other_joiners_alone(self) -> None:
        release = asyncio.Event()
        calls: list[int] = []

        async def fetch(key: int) -> str:
            calls.append(key)
            await release.wait()
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        owner = asyncio.ensure_future(cache.resolve(1))
        await asyncio.sleep(0)
        cancelled_joiner = asyncio.ensure_future(cache.resolve(1))
        other_joiner = asyncio.ensure_future(cache.resolve(1))
        await asyncio.sleep(0)

        cancelled_joiner.cancel()
        await asyncio.sleep(0)
        release.set()

        assert await owner == "value-1"
        assert await other_joiner == "value-1"
        assert calls == [1]
        with pytest.raises(asyncio.CancelledError):
            await cancelled_joiner

    async def test_a_failed_fetch_with_no_waiters_does_not_warn_about_an_unretrieved_exception(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # With no joiner nobody awaits the owner's future, so resolve() must mark its exception retrieved itself.
        async def failing_fetch(key: int) -> str:
            raise ValueError("boom")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(failing_fetch)
        with caplog.at_level(logging.ERROR, logger="asyncio"):
            with pytest.raises(ValueError, match="boom"):
                await cache.resolve(1)
            # asyncio logs "exception was never retrieved" when a Future
            # holding an exception is garbage-collected unretrieved.
            gc.collect()
        assert [r for r in caplog.records if "never retrieved" in r.getMessage()] == []

    async def test_key_can_be_resolved_again_after_a_failed_fetch(self) -> None:
        attempts = {"count": 0}

        async def flaky_fetch(key: int) -> str:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise ValueError("first attempt fails")
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(flaky_fetch)
        with pytest.raises(ValueError, match="first attempt fails"):
            await cache.resolve(1)
        assert await cache.resolve(1) == "value-1"  # retried, this time succeeds


class TestSettleAll:
    async def test_a_fetch_raising_type_error_is_reported_not_mistaken_for_no_fetch_bound(self) -> None:
        release = asyncio.Event()

        async def buggy_fetch(key: int) -> str:
            await release.wait()
            raise TypeError("a bug inside the fetch")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache()
        owner = asyncio.ensure_future(cache.resolve(1, buggy_fetch))
        await asyncio.sleep(0)
        settle = asyncio.ensure_future(cache.settle_all())
        await asyncio.sleep(0)
        release.set()

        settled, errors = await settle
        assert settled == {}
        assert [str(e) for e in errors] == ["a bug inside the fetch"]
        with pytest.raises(TypeError, match="a bug inside the fetch"):
            await owner

    async def test_never_starts_a_fetch_even_with_one_bound(self) -> None:
        calls: list[int] = []

        async def fetch(key: int) -> str:
            calls.append(key)
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        await cache.resolve(1)

        settled, errors = await cache.settle_all()

        assert settled == {1: "value-1"}
        assert errors == []
        assert calls == [1]

    async def test_settles_both_an_already_resolved_and_a_still_in_flight_key(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def fetch(key: int) -> str:
            if key == 2:
                started.set()
                await release.wait()
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(fetch)
        await cache.resolve(1)
        task = asyncio.ensure_future(cache.resolve(2))
        await started.wait()

        settle_task = asyncio.ensure_future(cache.settle_all())
        await asyncio.sleep(0)
        release.set()
        settled, errors = await settle_task
        await task

        assert settled == {1: "value-1", 2: "value-2"}
        assert errors == []

    async def test_a_genuine_fetch_failure_is_reported_as_its_own_error(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def failing_fetch(key: int) -> str:
            started.set()
            await release.wait()
            raise ValueError("boom")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(failing_fetch)
        task = asyncio.ensure_future(cache.resolve(1))
        await started.wait()

        settle_task = asyncio.ensure_future(cache.settle_all())
        await asyncio.sleep(0)  # let settle_all() start awaiting the still-in-flight key
        release.set()
        settled, errors = await settle_task
        with pytest.raises(ValueError, match="boom"):
            await task

        assert settled == {}
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)

    async def test_a_fetch_whose_caller_was_cancelled_is_skipped_not_propagated(self) -> None:
        """The caller that owns an in-flight fetch is cancelled (a TUI worker
        at shutdown) while ``settle_all`` waits on it: that cancellation is
        not ``settle_all``'s, so it returns instead of raising it into a
        closer that still has resources to close."""
        started = asyncio.Event()

        async def blocking_fetch(key: int) -> str:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(blocking_fetch)
        await cache.resolve(0, fetch=lambda key: _value(key))
        owner = asyncio.ensure_future(cache.resolve(1))
        await started.wait()

        settle_task = asyncio.ensure_future(cache.settle_all())
        await asyncio.sleep(0)  # settle_all() is now awaiting key 1's in-flight fetch
        owner.cancel()
        settled, errors = await settle_task

        assert settled == {0: "value-0"}
        assert errors == []
        with pytest.raises(asyncio.CancelledError):
            await owner

    async def test_cancelling_settle_all_itself_still_propagates(self) -> None:
        started = asyncio.Event()

        async def blocking_fetch(key: int) -> str:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(blocking_fetch)
        owner = asyncio.ensure_future(cache.resolve(1))
        await started.wait()

        settle_task = asyncio.ensure_future(cache.settle_all())
        await asyncio.sleep(0)
        settle_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await settle_task
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner

    async def test_a_key_whose_in_flight_fetch_already_failed_and_cleaned_up_is_skipped_not_misreported(
        self,
    ) -> None:
        """A key whose fetch fails while ``settle_all()`` waits on an earlier
        key is skipped: its failure already went to its own caller."""
        release_ok = asyncio.Event()
        release_bad = asyncio.Event()

        async def slow_ok_fetch(key: int) -> str:
            await release_ok.wait()
            return "value-ok"

        async def failing_fetch(key: int) -> str:
            await release_bad.wait()
            raise ValueError("real corruption error")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache()
        task_ok = asyncio.ensure_future(cache.resolve(1, slow_ok_fetch))
        task_bad = asyncio.ensure_future(cache.resolve(2, failing_fetch))
        await asyncio.sleep(0)  # both registered as owners, in flight
        # settle_all() waits on fetches in the order they started: key 1
        # (slow) first, so key 2 fails and cleans itself up during that wait.

        settle_task = asyncio.ensure_future(cache.settle_all())
        await asyncio.sleep(0)  # settle_all() is now awaiting key 1's still-pending future
        release_bad.set()  # key 2 fails and cleans up in the background
        with pytest.raises(ValueError, match="real corruption error"):
            await task_bad  # the real failure already went to its own direct caller
        release_ok.set()  # let key 1 (and therefore settle_all()) proceed

        settled, errors = await settle_task
        await task_ok

        assert settled == {1: "value-ok"}
        assert errors == []


class TestStats:
    async def test_counts_hits_misses_and_size(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        await cache.resolve(1)
        await cache.resolve(1)
        await cache.resolve(2)
        stats = cache.stats()
        assert (stats.size, stats.hits, stats.misses, stats.evictions, stats.maxsize) == (2, 1, 2, 0, None)

    async def test_counts_evictions_against_maxsize(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher(), maxsize=2)
        for key in (1, 2, 3, 4):
            await cache.resolve(key)
        stats = cache.stats()
        assert (stats.size, stats.misses, stats.evictions, stats.maxsize) == (2, 4, 2, 2)

    async def test_a_joined_in_flight_fetch_counts_as_neither_hit_nor_miss(self) -> None:
        gate = asyncio.Event()

        async def slow(key: int) -> str:
            await gate.wait()
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(slow)
        first = asyncio.create_task(cache.resolve(1))
        await asyncio.sleep(0)
        second = asyncio.create_task(cache.resolve(1))
        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(first, second)
        stats = cache.stats()
        assert (stats.hits, stats.misses) == (0, 1)

    async def test_invalidate_keeps_the_counters(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())
        await cache.resolve(1)
        cache.invalidate()
        stats = cache.stats()
        assert (stats.size, stats.misses) == (0, 1)


class TestQuiesce:
    async def test_waits_for_in_flight_fetches_without_counting_hits_for_settled_keys(self) -> None:
        gate = asyncio.Event()

        async def slow(key: int) -> str:
            if key == 2:
                await gate.wait()
            return f"value-{key}"

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(slow)
        await cache.resolve(1)
        pending = asyncio.create_task(cache.resolve(2))
        await asyncio.sleep(0)

        waiting = asyncio.create_task(cache.quiesce())
        await asyncio.sleep(0)
        assert not waiting.done()
        gate.set()

        assert await waiting == []
        await pending
        assert cache.stats().hits == 0  # key 1 was never resolved again

    async def test_a_fetch_whose_caller_was_cancelled_is_not_raised_into_quiesce(self) -> None:
        started = asyncio.Event()

        async def blocking_fetch(key: int) -> str:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(blocking_fetch)
        owner = asyncio.create_task(cache.resolve(1))
        await started.wait()

        waiting = asyncio.create_task(cache.quiesce())
        await asyncio.sleep(0)
        owner.cancel()

        assert await waiting == []
        with pytest.raises(asyncio.CancelledError):
            await owner

    async def test_returns_the_exceptions_of_failed_fetches(self) -> None:
        gate = asyncio.Event()

        async def failing(key: int) -> str:
            await gate.wait()
            raise OSError("boom")

        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(failing)
        pending = asyncio.create_task(cache.resolve(1))
        await asyncio.sleep(0)
        waiting = asyncio.create_task(cache.quiesce())
        await asyncio.sleep(0)
        gate.set()

        errors = await waiting
        assert [type(e) for e in errors] == [OSError]
        with pytest.raises(OSError, match="boom"):
            await pending

    async def test_with_nothing_in_flight_returns_immediately(self) -> None:
        cache: AsyncKeyedCache[int, str] = AsyncKeyedCache(_CountingFetcher())

        assert await cache.quiesce() == []


async def _value(key: int) -> str:
    return f"value-{key}"
