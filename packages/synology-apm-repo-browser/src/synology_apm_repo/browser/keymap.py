"""Key bindings more than one screen shares, so every screen binds the same
key to the same action."""

from __future__ import annotations

import dataclasses

from textual.binding import Binding, BindingType

#: Bindings every screen shares (quit, verbose toggle, help). Typed
#: ``list[BindingType]`` so ``BINDINGS = [*COMMON_BINDINGS, ...]`` matches
#: ``Screen.BINDINGS``' invariant-list annotation.
COMMON_BINDINGS: list[BindingType] = [
    Binding("q", "quit_app", "Quit"),
    Binding("d", "toggle_verbose", "Verbose mode"),
    Binding("question_mark", "show_help", "Help", key_display="?"),
]

#: Navigation within a list/tree/table: jk, Enter/l to open, Esc/h to go
#: back. Backspace (to parent node) applies to a ``Tree`` only.
NAV_BINDINGS: list[BindingType] = [
    Binding("j", "cursor_down", "Down", show=False),
    Binding("k", "cursor_up", "Up", show=False),
    Binding("l", "select", "Open", show=False),
    Binding("h", "go_back", "Back", show=False),
    Binding("enter", "select", "Open", show=False),
    Binding("escape", "go_back", "Back", show=False),
    Binding("backspace", "cursor_to_parent", "To parent", show=False),
]

#: Item-tree actions: ``e`` export, ``i`` detail, ``x`` hex, ``+``/``=``
#: load more.
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


def hidden(bindings: list[BindingType], *keys: str) -> list[BindingType]:
    """``bindings`` with each binding for one of ``keys`` kept bound (and in
    ``?``'s help) but hidden from the footer."""
    return _with_show(bindings, keys, show=False)


def shown(bindings: list[BindingType], *keys: str) -> list[BindingType]:
    """``bindings`` with each binding for one of ``keys`` shown in the footer."""
    return _with_show(bindings, keys, show=True)


def _with_show(bindings: list[BindingType], keys: tuple[str, ...], *, show: bool) -> list[BindingType]:
    return [dataclasses.replace(b, show=show) if isinstance(b, Binding) and b.key in keys else b for b in bindings]
