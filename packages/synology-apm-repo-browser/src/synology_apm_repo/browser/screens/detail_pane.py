"""``DetailPane``: owns ``UnitScreen``'s ``#detail``/``#detail-scroll``
widgets — which node they're currently showing (``_render_if_current``
discards a late-arriving render for a node the user has since navigated
away from), and the header/preview/List-overview text rendered into
them. Held by ``UnitScreen`` as a private collaborator, reaching back
into it only through ``query_one``/``app_state``.

The ``@work``-decorated preview/List-overview loading stays on
``UnitScreen`` itself, since Textual's ``@work`` requires a ``DOMNode``
``self``, which this plain collaborator isn't — this class owns only the
rendering that runs once bytes are already in hand.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING

from textual.containers import VerticalScroll
from textual.css.query import NoMatches
from textual.widgets import Static

from synology_apm_repo.browser.core.unit.select import is_content_only_preview
from synology_apm_repo.browser.list_overview import render_overview_table
from synology_apm_repo.browser.widgets.progress_hint import _ConditionalResetSink
from synology_apm_repo.sdk.presentation.format import format_bytes, format_timestamp
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import FileState, Node, node_file_state, node_kind_label, node_modified_time

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen


class DetailPane:
    def __init__(self, screen: UnitScreen) -> None:
        self._screen = screen
        # Which node this pane is currently showing — set synchronously by
        # show() before its preview worker starts, so a late-arriving
        # preview from a node the user navigated away from can be told
        # apart from a current one.
        self._node: Node | None = None

    @property
    def node(self) -> Node | None:
        """Whichever node this pane is currently showing -- ``None``
        before anything has been selected. Read by
        ``UnitScreen._selected_node()`` as its fallback when neither the
        tree nor the file table has focus."""
        return self._node

    def header_text(self, node: Node) -> str:
        # node.name/attrs are real backup content — must be escaped before
        # reaching Static. node.ref is a NodeRef string (percent-encoded,
        # so never contains []) and is left unescaped.
        if is_content_only_preview(node):
            # Mail/calendar/contact/Teams-chat previews already state
            # their own identity, so a generic header would repeat it.
            # Verbose mode still gets ref/attrs, just without the
            # redundant header lines.
            if not self._screen.app_state.verbose:
                return ""
            lines = [f"ref: {node.ref}"]
            for key, value in node.attrs.items():
                lines.append(f"{safe(key)}: {safe(value)}")
            return "\n".join(lines)
        lines = [f"[b]{safe(node.name)}[/b]", f"kind: {node_kind_label(node)}"]
        if node.size is not None:
            size_line = f"size: {format_bytes(node.size)}"
            if node_file_state(node) is FileState.CLOUD_ONLY:
                # A cloud-sync placeholder has no real data resident
                # locally — node.size is the guest OS's declared size, not
                # what's on disk. An EFS-encrypted file has no such
                # caveat: its bytes are on disk, just undecryptable.
                size_line += " (0 Byte on disk)"
            lines.append(size_line)
        modified = node_modified_time(node)
        if modified is not None:
            # Shown outside verbose mode too — the header stays the single
            # source of truth for the current node even when the file
            # table isn't in view (e.g. a goto-ref landing on a leaf).
            lines.append(f"modified: {format_timestamp(modified)}")
        if self._screen.app_state.verbose:
            lines.append(f"ref: {node.ref}")
            for key, value in node.attrs.items():
                lines.append(f"{safe(key)}: {safe(value)}")
        return "\n".join(lines)

    def show(self, node: Node) -> None:
        self._node = node
        self._static().update(self.header_text(node))

    def clear(self) -> None:
        """Resets to the empty pre-selection state -- called when the
        screen's root/provider is torn down so a node from the closed
        provider generation can't resurface via ``_selected_node()``'s
        fallback."""
        self._node = None
        self._static().update("")

    def set_wide(self, wide: bool) -> None:
        """Toggles the ``wide-preview`` CSS class so a List overview's
        table sizes to its real content width and pans horizontally,
        instead of wrapping. Reset on every other selection so a later
        preview doesn't inherit an oversized pane."""
        self._static().set_class(wide, "wide-preview")
        self._screen.query_one("#detail-scroll", VerticalScroll).set_class(wide, "wide-preview")

    def _append_after_header(self, node: Node, body: str, *, separator: str = "") -> None:
        """Shared by every ``append_*``/``show_loading`` method: combines
        ``body`` with whatever header ``show(node)`` put in place — an
        empty header means ``body`` is the pane's whole text, no
        separator above it."""

        def render(header: str) -> str:
            if not header:
                return body
            return f"{header}\n\n{separator}\n{body}" if separator else f"{header}\n\n{body}"

        self._render_if_current(node, render)

    def append_preview(self, node: Node, preview: str) -> None:
        self._append_after_header(node, safe(preview), separator="─" * 40)

    def append_list_overview(self, node: Node, rows: list[dict[str, object]], *, truncated: bool) -> None:
        if not rows:
            self._render_if_current(node, lambda header: f"{header}\n\n(no items)")
            return
        # Pre-rendered to plain text: Console(record=True) also writes to
        # the real stdout on every .print(), which would corrupt this
        # running Textual app's own terminal.
        self._render_if_current(node, lambda header: render_overview_table(header, rows, truncated=truncated))

    def append_list_overview_error(self, node: Node, message: str) -> None:
        self._append_after_header(node, f"[red]error:[/red] {safe(message)}")

    def append_preview_error(self, node: Node, message: object) -> None:
        self._append_after_header(node, f"[red]error:[/red] {safe(message)}")

    def append_preview_note(self, node: Node, message: object) -> None:
        """Same shape as ``append_preview_error``, styled as an
        informational hint instead of a failure (a cloud-sync
        placeholder, an EFS-encrypted file), not a genuine rendering
        failure."""
        self._append_after_header(node, f"[dim]note:[/dim] {safe(message)}")

    def show_loading(self, node: Node, frame: str) -> None:
        """``DetailPaneLoadingSink``'s ``show`` -- appends an animated cue
        below the current header, discarded by ``_render_if_current``'s
        staleness guard the same as a real preview would be."""
        self._append_after_header(node, f"({frame} loading)")

    def clear_loading(self, node: Node) -> None:
        self._render_if_current(node, lambda header: header)

    def current_text(self) -> str:
        """The pane's current plain-text rendering -- lets
        ``DetailPaneLoadingSink`` tell "nothing written since the loading
        cue" from "a real preview already landed," compared only for
        equality, never re-parsed."""
        static = self._static_if_present()
        return str(static.render()) if static is not None else ""

    def _render_if_current(self, node: Node, render: Callable[[str], str]) -> None:
        """Staleness guard shared by every ``append_*`` method -- discards
        a render for a node the user has since navigated away from."""
        if node is not self._node:
            return  # the user has since selected a different node — discard
        static = self._static_if_present()
        if static is None:
            return  # the screen itself is gone -- nothing left to render into
        static.update(render(self.header_text(node)))

    def _static(self) -> Static:
        return self._screen.query_one("#detail", Static)

    def _static_if_present(self) -> Static | None:
        """Tolerates the screen already being gone: a background worker's
        result can still arrive after its host screen is torn down. Only
        ``current_text``/``_render_if_current`` need this --
        ``show``/``clear``/``set_wide`` run synchronously while the
        screen is guaranteed present."""
        with contextlib.suppress(NoMatches):
            return self._screen.query_one("#detail", Static)
        return None


class DetailPaneLoadingSink:
    """Adapts ``DetailPane.show_loading``/``clear_loading`` to
    ``_LoadingSink``'s ``show(frame)``/``hide()`` shape, closing over the
    node a given fetch was for since ``DebouncedProgress`` only passes
    the frame.

    Delegates to ``widgets/progress_hint.py``'s ``_ConditionalResetSink``:
    ``hide()`` only clears back to a bare header when the pane still
    shows exactly what the last loading frame wrote, since a slow fetch
    can have already rendered the real result by the time ``hide()``
    runs."""

    def __init__(self, pane: DetailPane, node: Node) -> None:
        self._core = _ConditionalResetSink(
            write_loading=lambda frame: pane.show_loading(node, frame),
            write_reset=lambda: pane.clear_loading(node),
            read=pane.current_text,
        )

    def show(self, frame: str) -> None:
        self._core.show(frame)

    def hide(self) -> None:
        self._core.hide()
