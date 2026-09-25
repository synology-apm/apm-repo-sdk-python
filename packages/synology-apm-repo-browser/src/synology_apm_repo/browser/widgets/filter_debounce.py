"""``Debouncer``: restarts a delay-second timer on every ``trigger()`` call,
running its callback only once no further ``trigger()`` arrives within
``delay`` — the classic UI debounce for a fast-repeating event (every
keystroke while a filter box is open), distinct from
``widgets/progress_hint.py``'s ``DebouncedProgress`` (which arms a timer
*once* and never restarts it, pacing a spinner's own appearance rather
than a repeated user action).

Must be constructed/used on the App's event loop (arms timers via
``screen.set_timer``) — safe because every worker in this package runs as
a native async task on that same loop, never a thread: a thread-hosted
worker would have to marshal back onto the loop before it could touch a
timer at all.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Protocol

from textual.css.query import NoMatches
from textual.timer import Timer

_DEFAULT_DELAY = 0.3


class _TimerHost(Protocol):
    """Structural minimum this class needs from its host: just enough to
    arm a timer — same rationale as ``widgets/progress_hint.py``'s own
    ``_TimerHost`` (not reused directly: that one is module-private, and
    also declares ``set_interval``, which nothing here needs). Lets a
    non-``NavigableScreen`` modal (``WorklistScreen``) pass itself
    directly, with no ``# type: ignore``."""

    def set_timer(self, delay: float, callback: Callable[[], None]) -> Timer: ...


class Debouncer:
    """(Re)arms a ``delay``-second timer on every ``trigger()`` — ``callback``
    runs only once ``delay`` seconds pass with no further ``trigger()``
    call. Swallows ``NoMatches`` from ``callback`` the same way
    ``DebouncedProgress``'s own delayed callback does (see
    ``_shared.py``'s ``_update_breadcrumb_text``): unlike a filter
    rebuild triggered synchronously inside the same keystroke's event
    handler, this fires after a real delay, during which the screen it
    was meant for may have been popped/replaced."""

    def __init__(self, screen: _TimerHost, callback: Callable[[], None], *, delay: float | None = None) -> None:
        self._screen = screen
        self._callback = callback
        # Resolved here, not as `delay: float = _DEFAULT_DELAY` -- a
        # default-argument expression is bound once at function-definition
        # time, so a module-level `_DEFAULT_DELAY` override (e.g. a test's
        # own autouse fixture) could never reach it that way.
        self._delay = _DEFAULT_DELAY if delay is None else delay
        self._timer: Timer | None = None

    def trigger(self) -> None:
        """Call on every event (e.g. every keystroke) — restarts the delay."""
        if self._timer is not None:
            self._timer.stop()
        self._timer = self._screen.set_timer(self._delay, self._fire)

    def _fire(self) -> None:
        self._timer = None
        with contextlib.suppress(NoMatches):
            self._callback()

    def cancel(self) -> None:
        """Drop any pending fire without running ``callback`` — call when
        the filter session closes."""
        if self._timer is not None:
            self._timer.stop()
            self._timer = None
