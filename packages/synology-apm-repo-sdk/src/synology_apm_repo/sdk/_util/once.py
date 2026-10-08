"""``AsyncOnce``: a lazily opened single resource whose concurrent first
uses share one open, and whose close waits for an open still in flight."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from ..asynccache import AsyncKeyedCache


class AsyncOnce[T]:
    """Opens its value on the first ``get()``; a concurrent ``get()`` joins
    that open rather than starting a second one, and a failed open is
    retried by the next ``get()``. ``close()`` resets it, so a later
    ``get()`` opens a fresh value."""

    def __init__(self, open_value: Callable[[], Awaitable[T]]) -> None:
        self._open_value = open_value
        self._cache: AsyncKeyedCache[None, T] = AsyncKeyedCache(self._fetch, maxsize=None)

    async def _fetch(self, _key: None) -> T:
        return await self._open_value()

    async def get(self) -> T:
        """The value, opening it first if needed."""
        return await self._cache.resolve(None)

    @property
    def opened(self) -> bool:
        """Whether a value is currently held (an open still in flight counts as not yet)."""
        return None in self._cache

    async def close(self, close_value: Callable[[T], Awaitable[object]]) -> None:
        """Wait for any open in flight, then release the held value (if any)
        with ``close_value``. A failed in-flight open is not re-raised here:
        its caller already received it."""
        settled, _ = await self._cache.settle_all()
        self._cache.invalidate()
        for value in settled.values():
            await close_value(value)
