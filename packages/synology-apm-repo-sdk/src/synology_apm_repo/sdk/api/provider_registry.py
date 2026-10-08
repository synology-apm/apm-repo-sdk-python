"""``ProviderRegistry``: every provider one ``Repository`` handed out, so
its close releases the SQLite connections they hold."""

from __future__ import annotations

import asyncio
import functools

from .._util.closing import RESOURCE_CLOSE_TIMEOUT, close_each, close_preserving
from ..units.base import ClosableUnitProvider


class ProviderRegistry:
    """The providers a ``Repository`` and its ``Catalog``\\ s handed out
    and haven't released yet. An unclosed provider can block interpreter
    exit (its SQLite connections run on non-daemon threads)."""

    def __init__(self) -> None:
        # Keyed by identity: a provider need not be hashable.
        self._providers: dict[int, ClosableUnitProvider] = {}
        # release() closes still running after their caller was cancelled.
        self._pending_closes: set[asyncio.Task[None]] = set()

    def track[P: ClosableUnitProvider](self, provider: P) -> P:
        """Remember ``provider`` until it is released or the registry closes."""
        self._providers[id(provider)] = provider
        return provider

    async def release(self, provider: ClosableUnitProvider) -> None:
        """Close ``provider`` and stop tracking it. The close runs to
        completion even if this call is cancelled, and ``close()`` waits
        for it."""
        self._providers.pop(id(provider), None)
        task = asyncio.ensure_future(asyncio.wait_for(provider.close(), timeout=RESOURCE_CLOSE_TIMEOUT))
        self._pending_closes.add(task)
        task.add_done_callback(self._pending_closes.discard)
        await asyncio.shield(task)

    async def release_after(self, primary: BaseException, provider: ClosableUnitProvider) -> None:
        """``release`` on a failure path whose caller never saw ``provider``:
        a release failure is noted on ``primary`` rather than raised."""
        await close_preserving(primary, [functools.partial(self.release, provider)])

    async def close(self) -> list[Exception]:
        """Close every tracked provider and wait for pending releases,
        attempting each; returns the failures instead of raising."""
        providers, self._providers = list(self._providers.values()), {}
        errors = await close_each((p.close for p in providers), per_close_timeout=RESOURCE_CLOSE_TIMEOUT)
        for outcome in await asyncio.gather(*self._pending_closes, return_exceptions=True):
            if isinstance(outcome, Exception):
                errors.append(outcome)
        return errors
