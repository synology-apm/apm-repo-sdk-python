"""``DebouncedProgress`` (below): debounces the loading indicator for an
expensive call behind a 300 ms delay, so a call that finishes quickly never
touches the screen. See its own class docstring for the mechanism.

The breadcrumb text itself is owned by the screen, not this module — see
``NavigableScreen._set_loading_indicator``'s own docstring for why the
split is there.

Must be constructed/stopped on the App's event loop (arms timers) — safe
because every worker in this package is async; see browser/README.md.
"""

from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Self

from textual.timer import Timer

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens._shared import NavigableScreen

_DEFAULT_DELAY = 0.3
_FRAME_INTERVAL = 0.1
#: A Braille dot spinner — smooth-looking at 100ms/frame, and (unlike
#: ASCII fallbacks such as ``|/-\``) never mistaken for real content.
_SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
#: Deliberately loud — this is the one signal telling the user something
#: is still running, not a subtle status-bar tone.
_LOADING_STYLE = "bold bright_yellow"


class DebouncedProgress:
    """Owns one loading-indicator's debounced start/stop for the lifetime
    of a single expensive call. Constructing this arms a ``delay``-second
    timer; if ``stop`` is called before it fires, the screen is never
    touched at all — only a call still running past ``delay`` ever
    animates anything, via ``NavigableScreen._set_loading_indicator``.

    A context manager over that same lifetime — ``with
    DebouncedProgress(self): ...`` calls ``stop()`` on the way out
    regardless of how the block exits, the shape every call site needs."""

    def __init__(self, screen: NavigableScreen, *, delay: float = _DEFAULT_DELAY) -> None:
        self._screen = screen
        self._stopped = False
        self._frame = 0
        self._anim_timer: Timer | None = None
        self._timer: Timer = screen.set_timer(delay, self._start_animating)

    def _start_animating(self) -> None:
        if self._stopped:  # pragma: no cover - defensive: stop() already cancels the timer
            return
        self._tick()  # render the first frame immediately, not one _FRAME_INTERVAL late
        self._anim_timer = self._screen.set_interval(_FRAME_INTERVAL, self._tick)

    def _tick(self) -> None:
        frame = _SPINNER_FRAMES[self._frame % len(_SPINNER_FRAMES)]
        self._frame += 1
        self._screen._set_loading_indicator(  # noqa: SLF001 - the documented cross-module contract, see module docstring
            f"[{_LOADING_STYLE}]{frame} Loading...[/{_LOADING_STYLE}]"
        )

    def stop(self) -> None:
        self._stopped = True
        self._timer.stop()
        if self._anim_timer is not None:
            self._anim_timer.stop()
            self._anim_timer = None
            self._screen._set_loading_indicator(None)  # noqa: SLF001 - see _tick's own comment

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.stop()
