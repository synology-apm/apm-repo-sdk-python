"""``AsyncKeyedCache``: the SDK's one ``key -> value`` memoizing cache
(``await`` a fetch on a miss, store it, optionally evict LRU entries past a
cap), so each cache only supplies its fetch function. A stdlib-only leaf
that every layer may import."""

from __future__ import annotations

import asyncio
import dataclasses
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from typing import override


@dataclasses.dataclass(frozen=True, slots=True)
class CacheStats:
    """A point-in-time snapshot of one ``AsyncKeyedCache``.

    Attributes:
        size: Entries currently settled in the cache.
        maxsize: The cache's bound, or ``None`` if unbounded.
        hits: ``resolve``/``lookup`` calls served from a settled entry.
        misses: ``resolve`` calls that started a fetch, and ``lookup`` calls
            that found nothing; a caller that joins an in-flight fetch counts
            as neither.
        evictions: Entries dropped to stay within ``maxsize``.
    """

    size: int
    maxsize: int | None
    hits: int
    misses: int
    evictions: int


class AsyncKeyedCache[K, V](Mapping[K, V]):
    """A ``key -> value`` cache backed by an async ``fetch`` callback;
    ``resolve`` is the fetch-and-store operation.

    The ``Mapping`` interface is a live, read-only view of the settled
    entries; it never fetches or blocks. ``maxsize`` (``None`` =
    unbounded) may be reassigned; the new cap applies at the next store.
    """

    def __init__(
        self,
        fetch: Callable[[K], Awaitable[V]] | None = None,
        *,
        maxsize: int | None = None,
    ) -> None:
        self._fetch = fetch
        self.maxsize = maxsize
        self._store: OrderedDict[K, V] = OrderedDict()
        self._inflight: dict[K, asyncio.Future[V]] = {}
        # In-flight keys that invalidate() hit before their fetch finished:
        # the owner skips writing that stale result to _store. Holds only
        # in-flight keys, so it never outgrows _inflight.
        self._stale: set[K] = set()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    # -- Mapping (sync, read-only, never fetches) ---------------------------

    @override
    def __len__(self) -> int:
        return len(self._store)

    @override
    def __iter__(self) -> Iterator[K]:
        return iter(self._store)

    @override
    def __getitem__(self, key: K) -> V:
        return self._store[key]

    def stats(self) -> CacheStats:
        """Counters since construction; ``invalidate`` does not reset them."""
        return CacheStats(
            size=len(self._store),
            maxsize=self.maxsize,
            hits=self.hits,
            misses=self.misses,
            evictions=self.evictions,
        )

    # -- the actual cache operation ------------------------------------------

    def lookup[D](self, key: K, default: D) -> V | D:
        """``resolve`` without the fetch: the settled value for ``key``,
        counted as a hit and marked most recently used, or ``default``,
        counted as a miss, for a caller that fetches misses itself. Unlike
        the ``Mapping`` view, this touches recency and the counters."""
        store = self._store
        if key in store:
            self.hits += 1
            store.move_to_end(key)
            return store[key]
        self.misses += 1
        return default

    def put(self, key: K, value: V) -> None:
        """Store ``value`` for ``key`` without a fetch, overwriting any
        existing entry."""
        self._store_and_evict(key, value)

    def put_many(self, items: Iterable[tuple[K, V]]) -> None:
        """``put`` for each ``(key, value)`` in order, evicting once at the end."""
        store = self._store
        for key, value in items:
            store[key] = value
            store.move_to_end(key)
        self._evict()

    def _store_and_evict(self, key: K, value: V) -> None:
        self._store[key] = value
        self._store.move_to_end(key)
        self._evict()

    def _evict(self) -> None:
        if self.maxsize is not None:
            while len(self._store) > self.maxsize:
                self._store.popitem(last=False)
                self.evictions += 1

    async def resolve(self, key: K, fetch: Callable[[K], Awaitable[V]] | None = None) -> V:
        """The cached value for ``key``, fetched and stored first on a miss;
        ``fetch`` overrides the construction-time fetch for this call. A
        caller arriving while ``key`` is being fetched joins that fetch and
        gets the same value or exception — except when the caller running
        that fetch is cancelled: a joiner that isn't cancelled itself then
        fetches ``key`` anew, and a joiner's own cancellation never reaches
        the fetch.

        Raises:
            TypeError: A fetch is needed and none was bound or passed.
        """
        # No lock: on one event loop nothing below runs concurrently with
        # another resolve() except across the awaits, and the in-flight
        # future is what joins callers there.
        #
        # A fetch may legitimately resolve to None (a cached "checked, found
        # nothing"), so presence is checked by key membership, not truthiness.
        while True:
            if key in self._store:
                self.hits += 1
                self._store.move_to_end(key)
                return self._store[key]
            joined = self._inflight.get(key)
            if joined is None:
                return await self._fetch_and_store(key, fetch)
            try:
                # Shielded: awaiting the bare future would cancel it, and with
                # it every other joiner, when this caller is cancelled.
                return await asyncio.shield(joined)
            except asyncio.CancelledError:
                _reraise_if_this_task_is_cancelled()
                # The fetch's own caller was cancelled, not this one.

    async def _fetch_and_store(self, key: K, fetch: Callable[[K], Awaitable[V]] | None) -> V:
        effective_fetch = fetch if fetch is not None else self._fetch
        self.misses += 1
        if effective_fetch is None:
            raise TypeError(f"resolve({key!r}): no fetch function bound at construction or passed here")
        future: asyncio.Future[V] = asyncio.get_running_loop().create_future()
        self._inflight[key] = future

        try:
            value = await effective_fetch(key)
        except BaseException as exc:
            del self._inflight[key]
            self._stale.discard(key)
            if not future.done():
                if isinstance(exc, asyncio.CancelledError):
                    future.cancel()
                else:
                    future.set_exception(exc)
                    future.exception()  # mark retrieved: nothing else may ever await it
            raise

        # A concurrent invalidate() on this key wins: the stale fetch result
        # is not written back (though waiters already in flight still get it).
        if key in self._stale:
            self._stale.discard(key)
        else:
            self._store_and_evict(key, value)
        del self._inflight[key]
        if not future.done():
            future.set_result(value)
        return value

    def invalidate(self, key: K | None = None) -> None:
        """Drop ``key`` (every entry when ``None``); a fetch already in
        flight for it still answers its waiters but is not stored."""
        if key is None:
            self._store.clear()
            self._stale.update(self._inflight)
        else:
            self._store.pop(key, None)
            if key in self._inflight:
                self._stale.add(key)

    async def quiesce(self) -> list[Exception]:
        """Wait for every fetch in flight right now and return the exceptions
        they raised; use it when the values need no closing."""
        errors: list[Exception] = []
        for future in list(self._inflight.values()):
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                _reraise_if_this_task_is_cancelled()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
        return errors

    async def settle_all(self) -> tuple[dict[K, V], list[Exception]]:
        """Wait out every fetch in flight, e.g. before closing the values.

        Returns every settled value plus every in-flight fetch's value —
        including one an ``invalidate()`` keeps out of the cache, which still
        needs closing — and the exceptions of the fetches that failed while
        this waited. A fetch that already failed when this reaches it is
        skipped (its owner already got the error). Never starts a fetch; only
        this task's own cancellation propagates.
        """
        settled: dict[K, V] = dict(self._store)
        errors: list[Exception] = []
        for key, future in list(self._inflight.items()):
            if future.done() and (future.cancelled() or future.exception() is not None):
                continue
            try:
                settled[key] = await asyncio.shield(future)
            except asyncio.CancelledError:
                _reraise_if_this_task_is_cancelled()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
        return settled, errors


def _reraise_if_this_task_is_cancelled() -> None:
    """Inside ``except CancelledError``: re-raise when the current task is the
    one being cancelled. Otherwise the error came from awaiting another
    caller's fetch, whose task was cancelled, and the caller carries on."""
    task = asyncio.current_task()
    if task is None or task.cancelling():
        raise
