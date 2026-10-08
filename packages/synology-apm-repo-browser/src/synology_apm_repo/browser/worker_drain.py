"""Waiting out cancelled or finished workers with a bound, for app and
screen teardown."""

from __future__ import annotations

import asyncio
import contextlib

from textual.worker import Worker

#: Upper bound on waiting out cancelled workers. Cancellation only lands at a
#: worker's next await, and one parked in an already-started ``to_thread`` call
#: does not come back until that call returns -- without a bound, teardown
#: would block the UI for as long as a single large read takes.
DRAIN_TIMEOUT_SECONDS = 2.0


async def drain(workers: list[Worker[None]]) -> None:
    """Wait out ``workers``, giving up after ``DRAIN_TIMEOUT_SECONDS``.
    A cancelled, failed or finished worker is equally expected; none is
    reported."""
    if not workers:
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(
            asyncio.gather(*(worker.wait() for worker in workers), return_exceptions=True),
            timeout=DRAIN_TIMEOUT_SECONDS,
        )
