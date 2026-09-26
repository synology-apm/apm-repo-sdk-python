"""``AsyncKeyedCache``: a ``key -> value`` memoizing cache — check a dict,
``await`` a fetch on miss, store the result, optionally evict the oldest
entry once over a cap — shared by every cache in this SDK
(``dedup.pool``'s bucket/chunk caches, ``storage.dircache.DirCache``,
``dedup.composition_reader``'s page cache, and others) so each only has
to get its own fetch function right.

At the SDK package root, not under ``storage/``/``dedup/``: both packages
depend on it, and it has zero dependencies beyond the stdlib.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterator, Mapping
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class AsyncKeyedCache(Mapping[K, V], Generic[K, V]):
    """A ``key -> value`` cache backed by an async ``fetch`` callback —
    ``resolve`` is the fetch-and-store operation.

    The ``Mapping`` interface (``len()``, ``in``, iteration,
    ``.keys()``/``.items()``/``.values()``, sync ``.get()``) is a live,
    read-only snapshot for introspection and tests — it never triggers a
    fetch and never blocks.

    ``maxsize`` (``None`` = unbounded) is a plain mutable attribute,
    not fixed at construction — assigning a new value takes effect on the
    next ``resolve`` call past the new cap.
    """

    def __init__(
        self,
        fetch: Callable[[K], Awaitable[V]] | None = None,
        *,
        maxsize: int | None = None,
    ) -> None:
        self._fetch = fetch
        self.maxsize = maxsize
        self._lock = asyncio.Lock()
        self._store: OrderedDict[K, V] = OrderedDict()
        self._inflight: dict[K, asyncio.Future[V]] = {}
        # One counter per key that's ever started a fetch, bumped by
        # invalidate() -- lets resolve()'s owner path notice "this key
        # was invalidated while my fetch was still running" and skip
        # writing a stale result to _store.
        self._generation: dict[K, int] = {}

    # -- Mapping (sync, read-only, never fetches) ---------------------------

    def __len__(self) -> int:
        return len(self._store)

    def __iter__(self) -> Iterator[K]:
        return iter(self._store)

    def __getitem__(self, key: K) -> V:
        return self._store[key]

    def known_keys(self) -> list[K]:
        """Every key with either a settled value or a fetch in flight right
        now, deduplicated — unlike ``.keys()``, includes in-flight fetches.
        Doesn't trigger a fetch, doesn't block."""
        return list({*self._store.keys(), *self._inflight.keys()})

    # -- the actual cache operation ------------------------------------------

    def put(self, key: K, value: V) -> None:
        """Insert ``key`` -> ``value`` directly, no fetch involved, for a
        caller that already has a value in hand. Overwrites an existing
        entry for ``key``."""
        self._store_and_evict(key, value)

    def _store_and_evict(self, key: K, value: V) -> None:
        """The maxsize-eviction bookkeeping shared by ``resolve()``'s own
        write-back and ``put()``."""
        self._store[key] = value
        self._store.move_to_end(key)
        if self.maxsize is not None:
            while len(self._store) > self.maxsize:
                self._store.popitem(last=False)

    async def resolve(self, key: K, fetch: Callable[[K], Awaitable[V]] | None = None) -> V:
        """Return the cached value for ``key``, fetching and storing it
        first if this is a miss. ``fetch`` overrides whatever was bound at
        construction for this one call; passing neither raises
        ``TypeError``.

        In-flight de-duplication: if another caller is already fetching
        this exact ``key``, this call awaits that caller's own in-progress
        fetch instead of starting a second one — both get the same value
        (or exception).
        """
        effective_fetch = fetch if fetch is not None else self._fetch
        if effective_fetch is None:
            raise TypeError(f"resolve({key!r}): no fetch function bound at construction or passed here")

        async with self._lock:
            # A fetch may legitimately resolve to None (a cached
            # "checked, found nothing"), so presence is checked by key
            # membership, not truthiness.
            if key in self._store:
                self._store.move_to_end(key)
                return self._store[key]
            future = self._inflight.get(key)
            if future is None:
                future = asyncio.get_running_loop().create_future()
                self._inflight[key] = future
                epoch = self._generation.setdefault(key, 0)
                is_owner = True
            else:
                is_owner = False

        if not is_owner:
            return await future

        try:
            value = await effective_fetch(key)
        except BaseException as exc:
            async with self._lock:
                del self._inflight[key]
            if not future.done():
                future.set_exception(exc)
                future.exception()  # mark retrieved: nothing else may ever await it
            raise

        async with self._lock:
            # A concurrent invalidate() on this key already bumped
            # _generation past what this owner captured — that
            # invalidation wins, so the stale fetch result is not written
            # back (though waiters already in flight still get it).
            if self._generation.get(key, 0) == epoch:
                self._store_and_evict(key, value)
            del self._inflight[key]
        if not future.done():
            future.set_result(value)
        return value

    def invalidate(self, key: K | None = None) -> None:
        """Drop ``key`` (or every cached entry, if ``key`` is ``None``),
        including the result of any fetch already in flight for it — a
        ``resolve()`` started before this call must not land a stale
        result afterward.
        """
        if key is None:
            self._store.clear()
            for k in self._generation.keys() | self._inflight.keys():
                self._generation[k] = self._generation.get(k, 0) + 1
        else:
            self._store.pop(key, None)
            self._generation[key] = self._generation.get(key, 0) + 1

    async def settle_all(self) -> tuple[dict[K, V], list[Exception]]:
        """Resolve every key ``known_keys()`` reports right now — settled or
        still fetching — so a caller can quiesce the cache before doing
        something that depends on nothing still being in flight (e.g.
        before closing underlying resources). Returns the values that
        resolved successfully, keyed the same as ``known_keys()``, plus the
        exceptions raised by every key that didn't.
        """
        settled: dict[K, V] = {}
        errors: list[Exception] = []
        for key in self.known_keys():
            try:
                settled[key] = await self.resolve(key)
            except Exception as exc:
                errors.append(exc)
        return settled, errors
