"""``DetailPane``: renders a ``DetailView`` (``core/unit/detail.py``) into
``UnitScreen``'s ``#detail``/``#detail-scroll`` widgets."""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING

from textual.containers import VerticalScroll
from textual.css.query import NoMatches
from textual.widgets import Static

from synology_apm_repo.browser.core.unit.detail import DetailView, header_text
from synology_apm_repo.browser.core.unit.model import (
    DetailBody,
    DetailError,
    DetailIdle,
    DetailLoading,
    DetailNote,
    DetailOverview,
    DetailPreview,
)
from synology_apm_repo.browser.list_overview_table import render_overview_table
from synology_apm_repo.sdk.presentation import safe

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen

_PREVIEW_SEPARATOR = "─" * 40


class DetailPane:
    def __init__(self, screen: UnitScreen) -> None:
        self._screen = screen
        self._view = DetailView(node=None, body=DetailIdle(), verbose=False)

    def render(self, view: DetailView) -> None:
        """The ``Store`` subscription's render: replaces the pane's content
        and layout with ``view``. A no-op once the widgets are gone."""
        self._view = view
        static = self._static_if_present()
        if static is None:
            return
        static.update(self.text(view))
        static.set_class(view.wide, "wide-preview")
        with contextlib.suppress(NoMatches):
            self._screen.query_one("#detail-scroll", VerticalScroll).set_class(view.wide, "wide-preview")

    def restore(self) -> None:
        """Re-renders the current view, dropping any loading cue. A no-op
        once the widgets are gone."""
        static = self._static_if_present()
        if static is not None:
            static.update(self.text(self._view))

    def show_loading(self, frame: str) -> None:
        """Appends an animated cue below the current header."""
        static = self._static_if_present()
        if static is not None:
            static.update(_compose(self._header(self._view), f"({frame} loading)"))

    def text(self, view: DetailView) -> str:
        return _body_text(self._header(view), view.body)

    @staticmethod
    def _header(view: DetailView) -> str:
        return header_text(view.node, verbose=view.verbose) if view.node is not None else ""

    def _static_if_present(self) -> Static | None:
        with contextlib.suppress(NoMatches):
            return self._screen.query_one("#detail", Static)
        return None


def _compose(header: str, body: str, *, separator: str = "") -> str:
    """``body`` below ``header``; an empty header means ``body`` is the
    pane's whole text, no separator above it."""
    if not header:
        return body
    return f"{header}\n\n{separator}\n{body}" if separator else f"{header}\n\n{body}"


def _body_text(header: str, body: DetailBody) -> str:
    match body:
        case DetailIdle() | DetailLoading():
            return header
        case DetailPreview(text=text):
            return _compose(header, safe(text), separator=_PREVIEW_SEPARATOR)
        case DetailNote(message=message):
            return _compose(header, f"[dim]note:[/dim] {safe(message)}")
        case DetailError(message=message):
            return _compose(header, f"[red]error:[/red] {safe(message)}")
        case DetailOverview(rows=rows, truncated=truncated):
            if not rows:
                return f"{header}\n\n(no items)"
            return render_overview_table(header, [dict(row) for row in rows], truncated=truncated)
    raise AssertionError(body)


class DetailLoadingSink:
    """``DebouncedProgress``'s sink for a detail fetch: animates below the
    header only while ``is_current()`` says the fetch is still the awaited
    one, and restores the pane's real view on ``hide()``."""

    def __init__(self, pane: DetailPane, is_current: Callable[[], bool]) -> None:
        self._pane = pane
        self._is_current = is_current

    def show(self, frame: str) -> None:
        if self._is_current():
            self._pane.show_loading(frame)

    def hide(self) -> None:
        self._pane.restore()
