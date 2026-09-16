"""Shared key bindings — centralized here so every screen offers the same
muscle-memory, and so the whole keymap is a single place to check against
rather than re-derived per screen.

Several handlers/actions across this package (lifecycle handlers like
``on_input_submitted``/``on_unmount``, actions like
``action_export_selected``) are declared ``async def`` purely so they can
``await`` an SDK call. This is legal because Textual dispatches both
through ``textual._callback.invoke()``, which does ``result =
callback(...)`` then ``if isawaitable(result): result = await result`` —
so a coroutine function is awaited to completion rather than left as an
un-awaited coroutine, exactly like any other handler.
"""

from __future__ import annotations

from textual.binding import Binding, BindingType

#: Bindings every screen shares (footer/help, quit, verbose-mode
#: toggle) — pushed onto each ``Screen.BINDINGS`` in addition to that
#: screen's own navigation-specific ones. Typed as ``list[BindingType]``
#: (not the narrower, inferred ``list[Binding]``) so assigning
#: ``Screen.BINDINGS = [*COMMON_BINDINGS, ...]`` matches the base
#: class's own (invariant-list) annotation.
#:
#: No key-entry binding — ``BrowseScreen`` pushes ``KeyDialog``
#: automatically when ``Repository.key_status`` says one is needed.
COMMON_BINDINGS: list[BindingType] = [
    Binding("q", "quit_app", "Quit"),
    Binding("d", "toggle_verbose", "Verbose"),
    Binding("question_mark", "show_help", "Help", key_display="?"),
]

#: Navigation within a list/tree/table — ↑↓/jk + Enter/l + Esc/h
#: (vim-style left/right doubling as back/forward). "backspace" ->
#: "jump to parent node" is a ``Tree``-only concept (a no-op forward on
#: ``DataTable``, which has no such action) — see `NavigableScreen.
#: action_cursor_to_parent``/``_shared.move_cursor_to_parent`'s own
#: docstrings for why this exists and how it behaves.
NAV_BINDINGS: list[BindingType] = [
    Binding("j", "cursor_down", "Down", show=False),
    Binding("k", "cursor_up", "Up", show=False),
    Binding("l", "select", "Open", show=False),
    Binding("h", "go_back", "Back", show=False),
    Binding("enter", "select", "Open", show=False),
    Binding("escape", "go_back", "Back", show=False),
    Binding("backspace", "cursor_to_parent", "To parent", show=False),
]

#: Item-tree/export-specific actions (``e`` export, ``i`` detail, ``x`` hex,
#: ``+`` load more — the same "plus, and equals-sign as its shiftless
#: alias" pair ``HexPreviewScreen``'s own paging keys already use).
UNIT_BINDINGS: list[BindingType] = [
    Binding("e", "export_selected", "Export"),
    Binding("i", "show_detail", "Detail"),
    Binding("x", "hex_preview", "Hex", show=False),  # verbose-mode only
    Binding("plus", "load_more", "Load more", show=False),
    Binding("equals_sign", "load_more", "Load more", show=False),  # '+' without shift on most layouts
]

#: Background-work status panel toggle (``t``).
WORKLIST_BINDING = Binding("t", "toggle_worklist", "Tasks")

#: Diagnostics screen entry (``v``, for verify findings).
VERIFY_BINDING = Binding("v", "show_diagnostics", "Verify", show=False)

#: Refresh (``r``) and filter (``/``).
REFRESH_BINDING = Binding("r", "refresh", "Refresh")
FILTER_BINDING = Binding("slash", "filter", "Filter", key_display="/")

#: Copy canonical NodeRef (``y``) and jump to a pasted one (``g``).
COPY_REF_BINDING = Binding("y", "copy_ref", "Copy ref", show=False)
GOTO_REF_BINDING = Binding("g", "goto_ref", "Goto ref", show=False)
