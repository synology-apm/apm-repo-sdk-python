"""``CacheLimits`` and ``CacheManager``: where every SDK cache's bound is
declared, and the one place a caller drops caches by name or all at once.

Storage is treated as immutable while a repository is open, so a cache is never
revalidated; ``CacheManager.invalidate_all()`` is the deliberate way to pick up
changed storage without reopening the ``Session``.

At the SDK package root beside ``asynccache``: stdlib plus ``asynccache`` only,
so every layer can depend on it.
"""

from __future__ import annotations

import dataclasses
import inspect
from collections.abc import Awaitable, Callable, Mapping

from .asynccache import AsyncKeyedCache, CacheStats


@dataclasses.dataclass(frozen=True, slots=True)
class CacheLimits:
    """The bound of every cache this SDK sizes by count. Each field says what
    one entry costs, since that is what the number trades against.

    Attributes:
        bucket_readers: ``Pool``'s bucket-reader LRU, and the default bound of
            every private scan cache. A reader holds its parsed header and
            ``BucketIndex`` (9 bytes per chunk, about 72 KiB for a full
            bucket), plus its ChunkCrcStore values once a CRC check loads
            them; no open file.
        bucket_readers_interactive: The same LRU for the ``Pool`` that
            ``Repository`` shares among interactive consumers; one directory
            listing under a PC/VM disk image can touch several dozen buckets.
        chunks: ``Pool``'s decoded-chunk LRU; 4 KiB per entry, 16 MiB total.
        composition_records: The repo-wide ``CompositionRecord`` LRU. Each
            record's page cache is bounded by ``composition_pages``, so the
            worst case is records * pages * 2048 * 20 bytes (about 320 MiB at
            the defaults); a miss just cold-refetches.
        composition_pages: Chunk-map pages one record keeps; 2048 entries of
            20 bytes (about 40 KiB) per page.
        saas_streams: ``SaasStreamCache`` LRU; each stream holds 2 sqlite
            connections (snapshot and version files), 4 when a bare placeholder
            sits beside a numbered generation, so this caps open connections at
            2-4x the value.
        verify_bucket_readers: The private reader cache of one verify run; at
            most 8 buckets are ever mid-flight, doubled here as headroom.
        dir_scan: Directory listings of the cache serving bulk-scannable
            directories (Pool leaves, Composition). A Pool leaf holds up to
            ~1,030 names, about 265 KiB with the sizes a listing carries, so 256
            entries is roughly 66 MiB and covers up to ~8 TiB of unique data
            when buckets are full.
        dir_fixed: Directory listings of the small, fixed set of metadata
            directories (``db``, repo root, ``suppl_transaction_ids``,
            ``repo_transactions``); kept apart so a scan cannot evict them.
        allocation_tables: ``.inf`` allocation tables, 8 KiB each; only
            fingerprint verification fills it.
        forward_resolutions: ``SaasStream``'s requested-version resolutions;
            one tiny tuple per entry.
    """

    bucket_readers: int = 16
    bucket_readers_interactive: int = 64
    chunks: int = 4096
    composition_records: int = 64
    composition_pages: int = 128
    saas_streams: int = 8
    verify_bucket_readers: int = 16
    dir_scan: int = 256
    dir_fixed: int = 64
    allocation_tables: int = 256
    forward_resolutions: int = 4096


DEFAULT_LIMITS = CacheLimits()

InvalidateHook = Callable[[], Awaitable[None] | None]
StatsHook = Callable[[], Mapping[str, CacheStats]]


@dataclasses.dataclass(frozen=True, slots=True)
class _Entry:
    invalidate: InvalidateHook
    stats: StatsHook
    bounded_by: str


class CacheManager:
    """A named registry of cache owners with ordered, collective invalidation.

    ``keyed`` creates an ``AsyncKeyedCache`` and registers it in one step, so a
    cache made this way cannot be left out; ``register`` adds an owner that
    manages several caches itself (``Pool``, ``DirCache``, ``SaasStreamCache``).
    Invalidating runs in reverse registration order, so register a cache after
    whatever it depends on (a ``Table`` after the connection it is bound to).

    A cache whose values need closing passes ``on_invalidate`` to close what it
    drops; no reader may be mid-call while that runs.
    """

    def __init__(self) -> None:
        self._entries: dict[str, _Entry] = {}

    def keyed[K, V](
        self,
        name: str,
        fetch: Callable[[K], Awaitable[V]] | None = None,
        *,
        maxsize: int | None,
        bounded_by: str = "",
        on_invalidate: Callable[[AsyncKeyedCache[K, V]], Awaitable[None]] | None = None,
    ) -> AsyncKeyedCache[K, V]:
        """Create, register and return an ``AsyncKeyedCache``.

        Args:
            name: Unique registry name.
            fetch: The cache's fetch callback.
            maxsize: Entry bound. ``None`` is allowed only with ``bounded_by``.
            bounded_by: Why a ``maxsize=None`` cache cannot grow without limit
                (for example a closed key set).
            on_invalidate: Async hook run instead of a plain ``invalidate()``,
                for a cache whose values need closing.

        Raises:
            ValueError: ``name`` is taken, or ``maxsize`` is ``None`` without
                ``bounded_by``.
        """
        if maxsize is None and not bounded_by:
            raise ValueError(f"cache {name!r}: an unbounded cache must say what bounds it (bounded_by=...)")
        cache: AsyncKeyedCache[K, V] = AsyncKeyedCache(fetch, maxsize=maxsize)

        async def _invalidate() -> None:
            if on_invalidate is not None:
                await on_invalidate(cache)
                return
            # Wait for fetches in flight; a result from before the invalidation
            # is never stored.
            errors = await cache.quiesce()
            cache.invalidate()
            if errors:
                raise ExceptionGroup(f"cache {name!r}: a fetch in flight failed", errors)

        self._add(name, _Entry(_invalidate, lambda: {name: cache.stats()}, bounded_by or f"maxsize={maxsize}"))
        return cache

    def register(self, name: str, invalidate: InvalidateHook, stats: StatsHook, *, bounded_by: str) -> None:
        """Register an owner that manages its own cache(s).

        Args:
            name: Unique registry name.
            invalidate: Drops the owner's caches; may be sync or async.
            stats: Snapshot of the owner's caches, keyed by cache name.
            bounded_by: What bounds the owner's caches.

        Raises:
            ValueError: ``name`` is taken or ``bounded_by`` is empty.
        """
        if not bounded_by:
            raise ValueError(f"cache {name!r}: say what bounds it (bounded_by=...)")
        self._add(name, _Entry(invalidate, stats, bounded_by))

    def _add(self, name: str, entry: _Entry) -> None:
        if name in self._entries:
            raise ValueError(f"cache {name!r} is already registered")
        self._entries[name] = entry

    def names(self) -> list[str]:
        """Registered names, in registration order."""
        return list(self._entries)

    def bounds(self) -> dict[str, str]:
        """``{name: what bounds it}``, in registration order."""
        return {name: entry.bounded_by for name, entry in self._entries.items()}

    def stats(self) -> dict[str, CacheStats]:
        """A snapshot of every registered cache, keyed by cache name."""
        snapshot: dict[str, CacheStats] = {}
        for entry in self._entries.values():
            snapshot.update(entry.stats())
        return snapshot

    async def invalidate(self, *names: str) -> None:
        """Invalidate the named owners, last registered first.

        Every named owner is attempted even if an earlier one fails.

        Raises:
            KeyError: A name is not registered (nothing was invalidated).
            ExceptionGroup: One or more owners failed to invalidate.
        """
        unknown = [name for name in names if name not in self._entries]
        if unknown:
            raise KeyError(f"unknown cache name(s): {', '.join(unknown)}")
        wanted = set(names)
        errors: list[Exception] = []
        for name in reversed(list(self._entries)):
            if name not in wanted:
                continue
            try:
                result = self._entries[name].invalidate()
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
        if errors:
            raise ExceptionGroup("CacheManager.invalidate() failed for one or more caches", errors)

    async def invalidate_all(self) -> None:
        """Invalidate every registered owner, last registered first."""
        await self.invalidate(*self._entries)
