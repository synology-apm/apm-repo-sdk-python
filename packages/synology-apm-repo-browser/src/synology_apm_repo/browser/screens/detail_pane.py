"""``DetailPane``: owns ``UnitScreen``'s ``#detail``/``#detail-scroll``
widgets — which node they're currently showing (``_render_if_current``
compares against this to discard a late-arriving preview/overview render
for a node the user has since navigated away from), and the
header/preview/List-overview text rendered into them. Held by
``UnitScreen`` as a private collaborator,
reaching back into it only through the small surface every ``Screen``
already exposes for this (``query_one``, ``app_state``) — the same
convention ``GotoChainWalker`` establishes for a ``UnitScreen``
collaborator.

The ``@work``-decorated preview/List-overview *loading* stays on
``UnitScreen`` itself: Textual's ``@work`` requires a ``DOMNode`` ``self``
(a ``Widget``/``Screen``/``App``), which this plain collaborator isn't.
This class owns only the rendering that runs once bytes are already in
hand, called from those workers.
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
        # show() before its preview worker even starts, so a late-arriving
        # preview for a node the user has since navigated away from can be
        # told apart from a genuinely current one.
        self._node: Node | None = None

    @property
    def node(self) -> Node | None:
        """Whichever node this pane is currently showing -- ``None``
        before anything has ever been selected. Read by
        ``UnitScreen._selected_node()`` as its own fallback when neither
        the folder tree nor the file table currently has focus (e.g. the
        user scrolled/clicked into ``#detail-scroll``, itself focusable),
        since this is the last real selection that produced whatever
        content is currently on screen either way."""
        return self._node

    def header_text(self, node: Node) -> str:
        # node.name/attrs values are real backup content (mail subjects,
        # filenames, ...) — must be escaped before reaching Static, same
        # reasoning as sdk/presentation/markup.py's docstring. node.ref is a
        # NodeRef string, which by construction (its own percent-
        # encoding) never contains ``[``/``]``, so it's left unescaped as a
        # matter of not hiding what's actually copyable from this screen.
        if is_content_only_preview(node):
            # Mail/calendar-event/contact/Teams-chat previews already
            # state their own identity, so a generic Name/kind/size/
            # modified header would just repeat it -- dropped entirely
            # here. Verbose mode still gets ref/attrs (the internal-
            # identifier exposure it exists for), just without the now-
            # redundant header lines above them.
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
                # locally — node.size is still the guest OS's own
                # declared/logical size, not what's actually on this
                # backup's disk. An EFS-encrypted file (FileState.
                # ENCRYPTED) has no equivalent caveat here: its real
                # bytes genuinely are on disk, just undecryptable.
                size_line += " (0 Byte on disk)"
            lines.append(size_line)
        modified = node_modified_time(node)
        if modified is not None:
            # Shown even outside verbose mode, like size/kind above --
            # redundant with the file table's own Modified column when a
            # leaf row was selected from there, but the header stays the
            # single source of truth for "everything known about the
            # currently-shown node" even when the file table isn't in
            # view (a goto-ref landing straight on a leaf).
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
        screen's own root/provider is torn down (a refresh, a
        verbose-mode toggle) so a node from the just-closed provider
        generation can never resurface via ``UnitScreen._selected_node()``'s
        own fallback to this pane once nothing has been selected again."""
        self._node = None
        self._static().update("")

    def set_wide(self, wide: bool) -> None:
        """Toggles the ``wide-preview`` CSS class (``theme.tcss``) that
        lets a List overview's own table size to its real content width
        and pan horizontally, instead of getting force-wrapped to the
        pane's width the way ordinary preview text should be. Reset
        (``wide=False``) on every other selection so a later, ordinary
        preview doesn't inherit an oversized/pannable pane from a
        previous List overview."""
        self._static().set_class(wide, "wide-preview")
        self._screen.query_one("#detail-scroll", VerticalScroll).set_class(wide, "wide-preview")

    def _append_after_header(self, node: Node, body: str, *, separator: str = "") -> None:
        """Shared by ``append_preview``/``append_preview_error``/
        ``append_list_overview_error``/``show_loading``: combines
        ``body`` with whatever header
        ``show(node)`` already put in place -- an empty header (a
        content-only node outside verbose mode -- see ``header_text``)
        means ``body`` *is* the pane's whole text, with no separator/
        blank line above it dividing it from a header that isn't
        there."""

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
        # Pre-rendered to plain text, not a live Rich renderable: a
        # Console(record=True) also writes straight to the real stdout on
        # every .print(), which would corrupt this running Textual app's
        # own terminal control.
        self._render_if_current(node, lambda header: render_overview_table(header, rows, truncated=truncated))

    def append_list_overview_error(self, node: Node, message: str) -> None:
        self._append_after_header(node, f"[red]error:[/red] {safe(message)}")

    def append_preview_error(self, node: Node, message: object) -> None:
        self._append_after_header(node, f"[red]error:[/red] {safe(message)}")

    def append_preview_note(self, node: Node, message: object) -> None:
        """Same shape as ``append_preview_error``, styled as an
        informational hint instead of a failure -- for a case
        ``_load_preview`` already expects and fully explains (a
        cloud-sync placeholder, an EFS-encrypted file), not a genuine
        rendering failure."""
        self._append_after_header(node, f"[dim]note:[/dim] {safe(message)}")

    def show_loading(self, node: Node, frame: str) -> None:
        """``DetailPaneLoadingSink``'s ``show`` -- appends an animated
        cue below whatever ``show(node)`` already put in place, discarded
        (via ``_render_if_current``'s own staleness guard) the same way a
        real preview/overview would be if the user has since selected a
        different node."""
        self._append_after_header(node, f"({frame} loading)")

    def clear_loading(self, node: Node) -> None:
        self._render_if_current(node, lambda header: header)

    def current_text(self) -> str:
        """The pane's own current plain-text rendering — lets
        ``DetailPaneLoadingSink`` tell "nothing has written here since I
        last showed the loading cue" apart from "a real preview/overview
        already landed," the same way ``progress_hint.py``'s
        ``StaticTextSink`` does for its own ``Static``. Never used to
        strip/re-parse content back into a new render — only compared for
        equality against a value this class itself already computed."""
        static = self._static_if_present()
        return str(static.render()) if static is not None else ""

    def _render_if_current(self, node: Node, render: Callable[[str], str]) -> None:
        """Staleness guard (discard for a node the user has since navigated
        away from) shared by every ``append_*`` method above — each
        supplies only how to turn this node's already-computed header into
        its own final text."""
        if node is not self._node:
            return  # the user has since selected a different node — discard
        static = self._static_if_present()
        if static is None:
            return  # the screen itself is gone -- nothing left to render into
        static.update(render(self.header_text(node)))

    def _static(self) -> Static:
        return self._screen.query_one("#detail", Static)

    def _static_if_present(self) -> Static | None:
        """Tolerates the screen already being gone -- same rationale as
        ``widgets/progress_hint.py``'s ``StaticTextSink._static()``: a
        background worker's own result (or ``DebouncedProgress.stop()``'s
        ``hide()``) can still be arriving after its host screen has already
        been torn down. Only ``current_text()``/``_render_if_current()`` use
        this -- ``show()``/``clear()``/``set_wide()`` all run synchronously
        while the screen is guaranteed present, so they keep using
        ``_static()`` unguarded."""
        with contextlib.suppress(NoMatches):
            return self._screen.query_one("#detail", Static)
        return None


class DetailPaneLoadingSink:
    """Adapts ``DetailPane.show_loading``/``clear_loading`` to
    ``_LoadingSink``'s ``show(frame)``/``hide()`` shape for one node --
    ``DebouncedProgress`` calls those with just the current frame, but
    ``DetailPane``'s own staleness guard (``_render_if_current``) needs
    the node a given fetch was for, which this sink closes over at
    construction instead of threading through every call.

    Delegates the actual show/hide bookkeeping to
    ``widgets/progress_hint.py``'s ``_ConditionalResetSink``: ``hide()``
    only clears back to a bare header when the pane still shows exactly
    what the last loading frame wrote, since ``_load_preview``'s/
    ``_load_list_overview``'s own ``append_preview``/
    ``append_list_overview``/``append_list_overview_error`` calls, once a
    slow-enough fetch resolves, can already have rendered the real
    preview/overview by the time ``hide()`` runs -- clearing
    unconditionally there would clobber that result right after showing
    it."""

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
