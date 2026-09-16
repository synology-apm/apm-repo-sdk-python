"""``DetailPane``: owns ``UnitScreen``'s ``#detail``/``#detail-scroll``
widgets — which node they're currently showing, the header/preview/List-
overview text rendered into them, and the staleness guard that discards a
late-arriving render for a node the user has since navigated away from.
Held by ``UnitScreen`` as a private collaborator, reaching back into it
only through the small surface every ``Screen`` already exposes for this
(``query_one``, ``app_state``) — the same convention ``GotoChainWalker``
establishes for a ``UnitScreen`` collaborator.

The ``@work``-decorated preview/List-overview *loading* stays on
``UnitScreen`` itself: Textual's ``@work`` requires a ``DOMNode`` ``self``
(a ``Widget``/``Screen``/``App``), which this plain collaborator isn't.
This class owns only the rendering that runs once bytes are already in
hand, called from those workers.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from textual.containers import VerticalScroll
from textual.widgets import Static

from synology_apm_repo.browser.list_overview import render_overview_table
from synology_apm_repo.sdk.presentation.format import format_bytes
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import Node

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

    def header_text(self, node: Node) -> str:
        # node.name/attrs values are real backup content (mail subjects,
        # filenames, ...) — must be escaped before reaching Static, same
        # reasoning as sdk/presentation/markup.py's docstring. node.ref is a
        # NodeRef string, which by construction (its own percent-
        # encoding) never contains ``[``/``]``, so it's left unescaped as a
        # matter of not hiding what's actually copyable from this screen.
        lines = [f"[b]{safe(node.name)}[/b]"]
        if node.kind is not None:
            lines.append(f"kind: {node.kind.value}")
        if node.size is not None:
            lines.append(f"size: {format_bytes(node.size)}")
        if self._screen.app_state.verbose:
            lines.append(f"ref: {node.ref}")
            for key, value in node.attrs.items():
                lines.append(f"{safe(key)}: {safe(value)}")
        return "\n".join(lines)

    def show(self, node: Node) -> None:
        self._node = node
        self._static().update(self.header_text(node))

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

    def append_preview(self, node: Node, preview: str) -> None:
        self._render_if_current(node, lambda header: f"{header}\n\n{'─' * 40}\n{safe(preview)}")

    def append_list_overview(self, node: Node, rows: list[dict[str, object]], *, truncated: bool) -> None:
        if not rows:
            self._render_if_current(node, lambda header: f"{header}\n\n(no items)")
            return
        # Pre-rendered to plain text, not a live Rich renderable — see
        # list_overview.render_overview_table's own docstring for why.
        self._render_if_current(node, lambda header: render_overview_table(header, rows, truncated=truncated))

    def append_list_overview_error(self, node: Node, message: str) -> None:
        self._render_if_current(node, lambda header: f"{header}\n\n[red]error:[/red] {safe(message)}")

    def _render_if_current(self, node: Node, render: Callable[[str], str]) -> None:
        """Staleness guard (discard for a node the user has since navigated
        away from) shared by every ``append_*`` method above — each
        supplies only how to turn this node's already-computed header into
        its own final text."""
        if node is not self._node:
            return  # the user has since selected a different node — discard
        self._static().update(render(self.header_text(node)))

    def _static(self) -> Static:
        return self._screen.query_one("#detail", Static)
