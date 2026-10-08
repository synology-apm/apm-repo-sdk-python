"""``Debouncer``: runs a callback once 300 ms pass with no further
``trigger()``, for a fast-repeating event such as filter keystrokes. Use it
on the App's event loop, since it arms timers.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Protocol

from textual.css.query import NoMatches
from textual.timer import Timer

_DELAY = 0.3


class _TimerHost(Protocol):
    """What ``Debouncer`` needs from its host: a timer."""

    def set_timer(self, delay: float, callback: Callable[[], None]) -> Timer: ...


class Debouncer:
    """(Re)arms a 300 ms timer on every ``trigger()``; ``callback`` runs
    once it passes with no further ``trigger()``. A
    ``NoMatches`` from ``callback`` is swallowed: the screen it was meant
    for may have gone during the delay."""

    def __init__(self, screen: _TimerHost, callback: Callable[[], None]) -> None:
        self._screen = screen
        self._callback = callback
        self._timer: Timer | None = None

    def trigger(self) -> None:
        """Call on every event (e.g. keystroke); restarts the delay."""
        if self._timer is not None:
            self._timer.stop()
        self._timer = self._screen.set_timer(_DELAY, self._fire)

    def _fire(self) -> None:
        self._timer = None
        with contextlib.suppress(NoMatches):
            self._callback()

    def cancel(self) -> None:
        """Drop any pending fire without running ``callback``."""
        if self._timer is not None:
            self._timer.stop()
            self._timer = None
