"""``LoadGate``: lets any number of repository loads run together, but an
invalidation run alone.

``Repository.invalidate_caches()`` closes and replaces db connections, so no
load on that repository may be mid-call while it runs. Cancelling the loads to
make room would leave their model slots ``Loading`` forever (a cancelled worker
never dispatches its result), so the invalidation waits for them instead, and
loads that start meanwhile wait for it.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable


class LoadGate:
    """A writer-preferring shared/exclusive gate.

    All state changes are synchronous (asyncio is single-threaded), so a holder
    cancelled at any await releases the gate: nothing awaits while releasing.
    """

    def __init__(self) -> None:
        self._shared = 0
        self._exclusive_pending = 0
        self._exclusive_running = False
        self._waiters: list[asyncio.Future[None]] = []

    def _wake(self) -> None:
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def _wait_until(self, ready: Callable[[], bool]) -> None:
        while not ready():
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._waiters.append(waiter)
            try:
                await waiter
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)

    @contextlib.asynccontextmanager
    async def shared(self) -> AsyncIterator[None]:
        """Hold the gate alongside other loads; waits while an invalidation is
        waiting for, or holding, the gate."""
        await self._wait_until(lambda: self._exclusive_pending == 0)
        self._shared += 1
        try:
            yield
        finally:
            self._shared -= 1
            self._wake()

    @contextlib.asynccontextmanager
    async def exclusive(self) -> AsyncIterator[None]:
        """Hold the gate alone: new loads wait, running ones finish first."""
        self._exclusive_pending += 1
        try:
            await self._wait_until(lambda: self._shared == 0 and not self._exclusive_running)
        except BaseException:
            self._exclusive_pending -= 1
            self._wake()
            raise
        self._exclusive_running = True
        try:
            yield
        finally:
            self._exclusive_running = False
            self._exclusive_pending -= 1
            self._wake()
