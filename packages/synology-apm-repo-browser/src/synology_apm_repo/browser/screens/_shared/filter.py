"""The shared ``/`` filter box (``#filter-input``)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from textual.screen import Screen
from textual.widgets import Input

from synology_apm_repo.browser.widgets.filter_debounce import Debouncer


def show_filter_input(screen: Screen[Any]) -> None:
    """Clears, shows (CSS class ``active``) and focuses ``#filter-input``."""
    filter_input = screen.query_one("#filter-input", Input)
    filter_input.value = ""
    filter_input.add_class("active")
    filter_input.focus()


def close_filter_debounce(screen: Screen[Any], debounce: Debouncer | None, dispatch_closed: Callable[[], None]) -> None:
    """Cancels ``debounce``, hides ``#filter-input``, then calls
    ``dispatch_closed`` (the screen's ``*Closed`` message)."""
    if debounce is not None:
        debounce.cancel()
    screen.query_one("#filter-input", Input).remove_class("active")
    dispatch_closed()


class FilterFieldController:
    """One filter's pending text and ``Debouncer``. The screen decides
    whether a filter opens and dispatches its ``*Opened`` message, then
    calls ``open()``; it calls ``on_text_changed()`` from
    ``on_input_changed`` and ``close()`` when the filter closes."""

    def __init__(
        self,
        screen: Screen[Any],
        dispatch_text_changed: Callable[[str], None],
        dispatch_closed: Callable[[], None],
        *,
        post_close: Callable[[], None] | None = None,
    ) -> None:
        self._screen = screen
        self._dispatch_text_changed = dispatch_text_changed
        self._dispatch_closed = dispatch_closed
        self._post_close = post_close
        self.pending_text = ""
        self._debounce: Debouncer | None = None

    def open(self) -> None:
        """Starts a filter session and shows ``#filter-input``."""
        self.pending_text = ""
        self._debounce = Debouncer(self._screen, self._commit)
        show_filter_input(self._screen)

    def _commit(self) -> None:
        self._dispatch_text_changed(self.pending_text)

    def on_text_changed(self, value: str) -> None:
        self.pending_text = value
        assert self._debounce is not None  # set by open()
        self._debounce.trigger()

    def close(self) -> None:
        close_filter_debounce(self._screen, self._debounce, self._dispatch_closed)
        self._debounce = None
        if self._post_close is not None:
            self._post_close()
