"""Shared base class for screens whose primary widget is a
``DataTable``/``Tree`` (vim-style ``j``/``k``/``l``/Enter bindings, kept
generic here instead of re-implemented per screen) —
private to the ``screens`` package (leading underscore), not part of the
browser's own public surface.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from textual.css.query import NoMatches
from textual.screen import Screen
from textual.widgets import Input, Static, Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.strings import GOTO_REF_NOT_CANONICAL_WARNING, GOTO_REF_PARSE_ERROR_WARNING
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.node_ref import NodeRef, RefKind

if TYPE_CHECKING:
    from synology_apm_repo.browser.app import ApmRepoBrowserApp
    from synology_apm_repo.sdk.api import Catalog, Repository, Version


def force_tree_line_cache(tree: Tree[Any]) -> None:
    """Force Textual's lazy line-cache rebuild before touching
    ``move_cursor()``/``scroll_to_node()``: ``Tree._build()`` (the
    thing that assigns each ``TreeNode`` a real ``.line``) only runs
    lazily, either on the next ``on_idle`` tick or whenever something
    reads the private ``_tree_lines`` property; ``.add()``/``.expand()``
    only *invalidate* the cache (``_tree_lines_cached = None``), they
    don't rebuild it. ``Tree.move_cursor(node)`` reads ``node._line``
    directly without forcing a rebuild first, so a freshly ``.add()``ed
    node (whose ``_line`` is still its never-updated constructor
    default of -1) makes ``cursor_line`` become -1, which
    ``validate_cursor_line`` then clamps to 0 — i.e. the cursor
    silently lands on the root instead of raising or visibly failing.
    Touching ``_tree_lines`` here forces the rebuild synchronously so
    ``node.line`` is correct by the time ``move_cursor()``/
    ``scroll_to_node()`` reads it."""
    _ = tree._tree_lines  # noqa: SLF001 - Textual's own private attr, forces Tree._build(), see docstring above


def move_cursor_to_parent(tree: Tree[Any]) -> None:
    """Backspace's shared behavior across every ``Tree`` in this app
    (``BrowseScreen``'s/``UnitScreen``'s via ``NavigableScreen`` below,
    ``ConnectDialog``'s own local directory tree directly since it isn't
    one): jumps the cursor to the current node's parent and
    collapses it, reaching a different branch in one keystroke instead
    of walking back up one line at a time with plain ``Tree``'s own Up.

    Never collapses the tree's own root, even when the cursor's parent
    *is* the root — ``BrowseScreen``'s two roots are permanent
    containers no one should collapse, and there's nothing to gain by
    collapsing a root with no sibling to jump to anyway. The cursor
    still moves there; only the collapse is skipped, so root stays a
    normal "nothing higher" landing spot rather than a special case
    every caller has to know about."""
    node = tree.cursor_node
    if node is None or node.parent is None:
        return  # nothing selected, or already at the tree's own root — nowhere further up
    parent = node.parent
    if parent is not tree.root:
        parent.collapse()
    force_tree_line_cache(tree)
    tree.move_cursor(parent)


def current_listing_tree_node(tree: Tree[Any]) -> TreeNode[Any]:
    """The tree node whose own children the cursor is currently
    browsing: the cursor's parent, or the tree's own root when the
    cursor sits on the root itself (no parent) or nothing is focused
    yet. Shared by ``BrowseScreen``'s tree-filter and ``UnitScreen``'s
    "load more"/filter, both resolving the same "which level is the
    cursor inside" question."""
    cursor = tree.cursor_node
    return cursor.parent if cursor is not None and cursor.parent is not None else tree.root


def show_filter_input(screen: Screen[Any]) -> None:
    """Opens the shared ``#filter-input`` widget for ``/`` filtering:
    clears its value, marks it ``active`` (the CSS class that actually
    shows it), and focuses it — the identical three-line sequence
    ``BrowseScreen``'s tree/version filters and ``UnitScreen``'s own
    filter each open with. Closing it back down stays each screen's own
    job (the "empty filter text restores the full list" step differs by
    what's being filtered), so only the open half is shared here."""
    filter_input = screen.query_one("#filter-input", Input)
    filter_input.value = ""
    filter_input.add_class("active")
    filter_input.focus()


def parse_canonical_ref(text: str, *, notify: Callable[..., None]) -> NodeRef | None:
    """Shared by ``BrowseScreen``/``UnitScreen``'s own ``g`` handling:
    parses ``text`` and validates it's a *canonical* ref (the only
    shape ``g`` accepts — the same shape ``y`` copies), notifying and
    returning ``None`` on either failure so both call sites get identical
    error messages for identical mistakes."""
    try:
        node_ref = NodeRef.parse(text.strip())
    except ValueError:
        notify(GOTO_REF_PARSE_ERROR_WARNING, severity="warning")
        return None
    if node_ref.kind is not RefKind.CANONICAL or node_ref.canonical_ids is None:
        notify(GOTO_REF_NOT_CANONICAL_WARNING, severity="warning")
        return None
    return node_ref


async def resolve_goto_version(
    notify: Callable[..., None], repo: Repository, node_ref: NodeRef
) -> tuple[Catalog, Version] | None:
    """Shared by ``BrowseScreen``/``UnitScreen``'s own ``_submit_goto``:
    resolves an already-``parse_canonical_ref``-validated ``node_ref`` to
    its owning ``Catalog`` and ``Version``, notifying and returning
    ``None`` on an ``ApmRepoError`` so both call sites report an
    unresolvable ref identically. Each caller keeps its own
    short-circuit/push behavior around this — ``UnitScreen``'s
    same-version fast path in particular has no shared equivalent here."""
    try:
        return await repo.version_for_ref(node_ref)
    except ApmRepoError as exc:
        notify(str(exc), severity="warning")
        return None


def show_error(screen: Screen[Any], widget_id: str, message: object) -> None:
    """Write ``[red]error:[/red] {message}`` into ``screen``'s
    ``widget_id`` ``Static`` — shared by ``DiagnosticsScreen``/
    ``UnitScreen`` (``#diag-status`` and ``#detail`` respectively) and
    ``KeyDialog``/``ConnectDialog``.
    ``screen: Screen[Any]``, not ``Screen[None]``: a ``ModalScreen``
    (``KeyDialog``/``ConnectDialog``) is generic over its own dismiss
    result, not ``None`` — this function only ever calls ``query_one``
    on it, so the dismiss-result type is irrelevant here. Deliberately
    not the *only* way an error reaches a screen: ``notify`` toasts,
    tree-leaf errors, and ``DetailPane.append_list_overview_error``'s
    own list-overview format are distinct UI contexts on purpose (see
    ``sdk/presentation/markup.py``) and stay separate from this."""
    screen.query_one(widget_id, Static).update(f"[red]error:[/red] {safe(message)}")


def modal_box_css(name: str, *, width: int, guard_child_horizontal: bool = False) -> str:
    """The shared "centered dialog box" CSS every ``ModalScreen``
    subclass in this package needs (``KeyDialog``/``ConnectDialog``/
    ``ExportScreen``) — ``align: center middle`` on the screen itself,
    plus its direct-child ``Vertical`` sized to ``width`` with the same
    border/background/padding. Meant to be concatenated with each
    screen's own remaining, genuinely screen-specific ``DEFAULT_CSS``
    rules (widget ids, tab visibility, ...), not used standalone — these
    three screens' CSS was never fully identical, only this shared
    prefix was.

    ``guard_child_horizontal`` adds the same ``> Vertical > Horizontal
    { height: auto; }`` override two of the three screens need: Textual's
    own ``Horizontal``/``Vertical`` containers default to
    ``height: 1fr`` (fill remaining space) — harmless inside a screen
    that already fills the terminal, but fatal inside this
    ``height: auto`` dialog box, since with nothing pinning a
    direct-child ``Horizontal`` down, it resolves its ``1fr`` against
    the ``Screen`` itself and silently eats *all* of the dialog's
    remaining height, stretching the whole box to fill the terminal."""
    guard = (
        f"""
    {name} > Vertical > Horizontal {{
        height: auto;
    }}
    """
        if guard_child_horizontal
        else ""
    )
    return f"""
    {name} {{
        align: center middle;
    }}

    {name} > Vertical {{
        width: {width};
        height: auto;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }}
    {guard}"""


class NavigableScreen(Screen[None]):
    """``j``/``k`` move, ``l``/Enter select — forwarded to whichever
    focused widget already implements ``action_cursor_down``/``_up``/
    ``action_select_cursor`` (``DataTable`` and ``Tree`` both do); a
    focused widget without those (e.g. an ``Input``) simply ignores the
    forward, letting its own bindings (if any) take the key instead.
    """

    def action_cursor_down(self) -> None:
        self._forward("action_cursor_down")

    def action_cursor_up(self) -> None:
        self._forward("action_cursor_up")

    def action_select(self) -> None:
        self._forward("action_select_cursor")

    def action_cursor_to_parent(self) -> None:
        # Not a plain _forward(): Tree has no built-in "jump to parent"
        # action of its own to forward to (unlike cursor_down/_up/
        # select_cursor, which Tree/DataTable already implement) — see
        # move_cursor_to_parent's own docstring for the behavior itself.
        if isinstance(self.focused, Tree):
            move_cursor_to_parent(self.focused)

    def action_go_back(self) -> None:
        """Plain pop, the default for a screen with nothing of its own to
        close first — ``BrowseScreen``/``UnitScreen`` override this to
        close an open filter/goto box before popping instead."""
        self.app.pop_screen()

    def action_show_diagnostics(self) -> None:
        # Local import: DiagnosticsScreen itself imports NavigableScreen
        # from this module, so a module-level import here would be
        # circular.
        from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

        self.app.push_screen(DiagnosticsScreen())

    def _forward(self, action_name: str) -> None:
        action = getattr(self.focused, action_name, None)
        if callable(action):
            action()

    # -- goto ref (``g``) -------------------------------------------------
    # Shared by every screen with a #goto-input Input (BrowseScreen/
    # UnitScreen): both methods just show/hide that same Input. Each
    # screen's own _submit_goto stays on the screen itself since it
    # isn't identical between them.

    def action_goto_ref(self) -> None:
        goto_input = self.query_one("#goto-input", Input)
        goto_input.value = ""
        goto_input.add_class("active")
        goto_input.focus()

    def _close_goto(self) -> None:
        self.query_one("#goto-input", Input).remove_class("active")

    def _set_loading_indicator(self, markup: str | None) -> None:
        """Hook ``DebouncedProgress`` calls once its debounce delay fires,
        and once more with ``None`` when the call it's timing finishes —
        appending ``markup`` (an already-styled "Loading" suffix) to this
        screen's own breadcrumb, or restoring the plain breadcrumb when
        ``markup`` is ``None``.

        A safe no-op by default: only this base class knows when
        loading starts/stops, but only each subclass knows its own
        breadcrumb's current *base* text (a live, multi-part path vs. a
        fixed version name) — a screen that actually uses
        ``DebouncedProgress`` must override this."""

    def _update_breadcrumb_text(self, text: str) -> None:
        """Writes ``text`` into this screen's own ``#breadcrumb``
        ``Static``, tolerating the widget already being gone: a
        ``DebouncedProgress``'s final ``stop()`` can fire after this
        screen has been popped (its timed call keeps running as a
        background ``Task`` no one awaited), so ``query_one()`` raising
        ``NoMatches`` here must not crash what should be an entirely
        harmless final write."""
        with contextlib.suppress(NoMatches):
            self.query_one("#breadcrumb", Static).update(text)

    @property
    def app_state(self) -> ApmRepoBrowserApp:
        return self.app  # type: ignore[return-value]
