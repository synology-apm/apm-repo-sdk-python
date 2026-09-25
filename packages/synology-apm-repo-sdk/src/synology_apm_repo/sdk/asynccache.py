"""``AsyncKeyedCache`` — the one memoizing-cache shape this SDK keeps
reinventing by hand.

``Pool._buckets``/``._chunks`` (bounded LRU, locked, session-wide shared),
``dedup.pool.BucketReaderCache`` (unbounded by default, private to one
bulk sweep so it doesn't evict ``Pool``'s own shared cache),
``storage.dircache.DirCache`` (unbounded, session-wide shared),
``dedup.composition_reader.CompositionRecord``'s page cache (bounded LRU,
scoped to one record), and ``units.verify_reachable._ReachabilityWalker
._composition_records`` (bounded LRU, one run's worth of shared records)
are all the exact same operation —
check a dict, ``await`` a fetch on miss, store the result, optionally
evict the oldest entry once over a cap — each written independently with
its own, slightly different correctness properties (some lock, some
don't; some accept "two callers miss the same key at once and both pay
for a redundant fetch" as a deliberately-accepted race, some don't even
consider it). This module factors that one operation out once, with one
well-tested concurrency contract, so every caller only has to get *its
own* fetch function right.

Deliberately at the SDK package root, not inside ``storage/`` or
``dedup/``: it has zero dependencies beyond the stdlib, and both of those
packages need to depend on it (``storage.dircache.DirCache`, several
places under ``dedup/``) — the same reason ``errors.py``/``identifiers.py``
live here instead of under a specific layer.
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
        """Every key with either a settled value or a fetch in flight
        right now, deduplicated — for a caller (e.g. ``Repository.close()``)
        that needs to account for everything ever asked for, not just what
        ``.keys()`` (the ``Mapping`` interface, settled entries only) can
        see. Doesn't trigger a fetch, doesn't block; a key not yet asked
        for by the time this is taken is still invisible to it."""
        return list({*self._store.keys(), *self._inflight.keys()})

    # -- the actual cache operation ------------------------------------------

    def put(self, key: K, value: V) -> None:
        """Insert ``key`` -> ``value`` directly, no fetch involved — for a
        caller that already has a value in hand (e.g. decoded as a
        byproduct of a batched fetch elsewhere) and wants it remembered
        here too, without paying for the ``Future``/in-flight-dedup
        machinery ``resolve()`` needs to await a real fetch on miss. Safe
        with no lock, the same reason ``invalidate()`` needs none: plain
        sync code can't be preempted mid-call on asyncio's single-threaded
        event loop. Overwrites an existing entry for ``key`` rather than
        leaving it — fine for every current caller, where the same key
        always maps to the same value."""
        self._store_and_evict(key, value)

    def _store_and_evict(self, key: K, value: V) -> None:
        """The maxsize-eviction bookkeeping shared by ``resolve()``'s own
        write-back and ``put()`` — one home for it instead of two copies."""
        self._store[key] = value
        self._store.move_to_end(key)
        if self.maxsize is not None:
            while len(self._store) > self.maxsize:
                self._store.popitem(last=False)

    async def resolve(self, key: K, fetch: Callable[[K], Awaitable[V]] | None = None) -> V:
        """Return the cached value for ``key``, fetching and storing it
        first if this is a miss.

        ``fetch`` overrides whatever was bound at construction for this
        one call. Passing neither is a caller bug: raises ``TypeError``
        immediately, same as Python would for any other missing required
        argument.

        In-flight de-duplication: if another caller is already fetching
        this exact ``key``, this call awaits that caller's own in-progress
        fetch instead of starting a second, redundant one — both callers
        get the same value (or exception) for the cost of one fetch. The
        first caller to miss (the "owner") is the only one that actually
        calls ``fetch``; later callers become waiters, never seeing their
        own ``fetch`` argument even if one was given.
        """
        # fetch overrides the bound one for this call only — needed by a
        # cache built at a layer that doesn't yet know how to fetch a miss
        # (dedup.pool.BucketReaderCache is constructed at the units layer,
        # which has no Pool reference; only the later resolver does).
        effective_fetch = fetch if fetch is not None else self._fetch
        if effective_fetch is None:
            raise TypeError(f"resolve({key!r}): no fetch function bound at construction or passed here")

        # Locking and in-flight dedup are unconditional, with no
        # per-instance escape hatch: both are cheap when uncontended, and
        # every consumer either genuinely needs them (real concurrent,
        # unrelated callers racing one key) or is never contended in
        # practice (a private, one-sweep cache with disjoint keys per
        # task) — no case where paying for one path costs more than
        # maintaining two.
        async with self._lock:
            # ``key in self._store``, not ``self._store.get(key) is not None``:
            # a fetch is free to legitimately resolve to ``None`` (a "checked,
            # found nothing" outcome some callers cache on purpose, e.g.
            # dedup.repository.DedupRepo's encryption-probe and
            # file_meta-table caches) — that must still count as a cache
            # hit, not silently re-fetch forever.
            if key in self._store:
                self._store.move_to_end(key)
                return self._store[key]
            future = self._inflight.get(key)
            if future is None:
                future = asyncio.get_running_loop().create_future()
                self._inflight[key] = future
                # Captured now, under the lock, so a concurrent
                # invalidate() can't land between this read and the
                # fetch starting -- it either happens before (this owner
                # sees the bumped generation, below) or after (caught by
                # the write-back check).
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
            # A concurrent invalidate() targeting this exact key while
            # this fetch was in flight already bumped _generation past
            # what this owner captured above -- that invalidation must
            # win: writing this now-stale value to _store would silently
            # undo it, defeating the very call that asked for a fresh
            # fetch next time. The in-flight future is still resolved
            # with it below, though: every waiter asked before the
            # invalidation, so handing them the answer that was already
            # committed to fetching is still the right value for *them*.
            if self._generation.get(key, 0) == epoch:
                self._store_and_evict(key, value)
            del self._inflight[key]
        if not future.done():
            future.set_result(value)
        return value

    def invalidate(self, key: K | None = None) -> None:
        """Drop ``key`` (or every cached entry, if ``key`` is ``None``).

        Also discards the result of any fetch already in flight for the
        affected key(s): without this, a ``resolve()`` that started
        before this call could still land its now-stale result in the
        cache afterward, silently undoing the invalidation.
        """
        if key is None:
            self._store.clear()
            for k in self._generation.keys() | self._inflight.keys():
                self._generation[k] = self._generation.get(k, 0) + 1
        else:
            self._store.pop(key, None)
            self._generation[key] = self._generation.get(key, 0) + 1

    async def settle_all(self) -> tuple[dict[K, V], list[Exception]]:
        """Resolve every key ``known_keys()`` reports right now — settled
        or still fetching — for a caller that needs the whole cache
        quiesced before it does something that depends on nothing still
        being in flight (``Repository.close()``/``set_key()`` both drain
        this way before touching every open ``DedupRepo``, since a
        ``dict(self._store)``/``.values()`` snapshot only sees entries
        already settled: a fetch started by a concurrent caller — racing
        ``close()``/``set_key()`` itself — would otherwise stay invisible
        to either method's own cleanup, and (for ``set_key()`` specifically)
        go on to land in the cache after the fact, permanently pinned to
        whatever key was active when it started. A key nobody has asked
        for yet at the moment ``known_keys()`` is taken is an unavoidably
        narrower, residual race neither method can retroactively account
        for — it's racing the drain itself, not something already in
        flight when the drain began.

        Returns the values that resolved successfully, keyed the same as
        ``known_keys()``, plus the exceptions raised by every key that
        didn't — a caller with nothing further to do for a failed key
        (``set_key()``'s own drain) can simply discard the second element.
        """
        settled: dict[K, V] = {}
        errors: list[Exception] = []
        for key in self.known_keys():
            try:
                settled[key] = await self.resolve(key)
            except Exception as exc:
                errors.append(exc)
        return settled, errors
