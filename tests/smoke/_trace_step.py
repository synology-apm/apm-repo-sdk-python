"""The smoke step a store call belongs to, so ``store_trace.jsonl`` events
can be attributed to ``<domain>.<step>`` instead of guessed from call order.

``SmokeContext.call`` sets it around each awaited step; a tool sets it with
``trace_step`` for store work outside one, phase-level fallbacks included.
The trace writer reads it in the task that made the store call, which
inherits it through asyncio's per-task and ``asyncio.to_thread`` context
copies.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from contextvars import ContextVar

_current_step: ContextVar[str] = ContextVar("smoke_current_step", default="")


def current_step() -> str:
    """The active ``<domain>.<step>`` label, or ``""`` outside any step."""
    return _current_step.get()


@contextlib.contextmanager
def trace_step(domain: str, step: str) -> Iterator[None]:
    """Label store calls made inside the block with ``domain``/``step``."""
    token = _current_step.set(f"{domain}.{step}")
    try:
        yield
    finally:
        _current_step.reset(token)
