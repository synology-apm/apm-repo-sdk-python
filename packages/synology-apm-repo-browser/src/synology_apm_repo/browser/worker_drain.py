"""Waiting out cancelled/finished workers with a bound -- shared by every
screen/app-level teardown that needs it (``app.py``'s own job registry,
``UnitScreen``'s own workers), since both need the identical
cancel-then-bounded-wait shape and a per-copy timeout constant would let
them silently drift apart.
"""

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

    Whether they were cancelled first is the caller's business — a cancelled,
    failed, or cleanly finished worker is an equally expected outcome here, and
    none of them is something to report.
    """
    if not workers:
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(
            asyncio.gather(*(worker.wait() for worker in workers), return_exceptions=True),
            timeout=DRAIN_TIMEOUT_SECONDS,
        )
