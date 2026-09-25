"""Shared filter-box mechanics."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from textual.screen import Screen
from textual.widgets import Input

from synology_apm_repo.browser.widgets.filter_debounce import Debouncer


def show_filter_input(screen: Screen[Any]) -> None:
    """Opens the shared ``#filter-input`` widget for ``/`` filtering:
    clears its value, marks it ``active`` (the CSS class that actually
    shows it), and focuses it — the identical three-line sequence
    ``BrowseScreen``'s tree/version filters and ``UnitScreen``'s own
    filter each open with. See ``close_filter_debounce`` below for the
    matching close-half mechanics."""
    filter_input = screen.query_one("#filter-input", Input)
    filter_input.value = ""
    filter_input.add_class("active")
    filter_input.focus()


def close_filter_debounce(screen: Screen[Any], debounce: Debouncer | None, dispatch_closed: Callable[[], None]) -> None:
    """Closes the shared ``#filter-input`` widget back down: cancels
    ``debounce`` (if one was ever armed), removes the ``active`` class,
    then calls ``dispatch_closed`` — the identical mechanics
    ``BrowseScreen``'s tree/version filters and ``UnitScreen``'s own
    filter each close with. Each
    screen still dispatches its own ``*Closed`` action ("empty filter
    text restores the full list" differs by what's being filtered) via
    ``dispatch_closed``, and still owns clearing its own debounce field
    to ``None`` afterward — this function holds no reference to it, only
    the ``Debouncer`` instance passed in."""
    if debounce is not None:
        debounce.cancel()
    screen.query_one("#filter-input", Input).remove_class("active")
    dispatch_closed()


class FilterFieldController:
    """Owns one filter box's own pending-text buffer and ``Debouncer``,
    shared by ``BrowseScreen``'s tree and version filters and
    ``UnitScreen``'s own filter instead of each holding its own
    ``_pending_*_text``/``_*_debounce`` instance field pair plus a
    ``_commit_*``/``_close_*`` method pair. *Opening* a filter stays each screen's own job — deciding
    whether opening even applies (a workload-group node vs. a leaf, say)
    and which domain ``*Opened`` message to dispatch differ too much
    between call sites to share — only the commit/close half, which
    never varies, moves here. Call ``open()`` right after dispatching
    that domain-specific ``*Opened`` message, ``on_text_changed()`` from
    ``on_input_changed``, and ``close()`` from the screen's own
    ``_close_*`` binding."""

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
        """Call once the screen's own domain-specific ``*Opened`` message
        has been dispatched -- resets the pending-text buffer, arms a
        fresh debounce for this filter session, and shows the shared
        ``#filter-input`` widget (``show_filter_input``, the identical
        three-line sequence every call site used to open with too)."""
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
