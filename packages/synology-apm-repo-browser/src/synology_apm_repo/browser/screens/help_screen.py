"""``HelpScreen``: the ``?`` modal, listing the bindings active on the
screen it was opened from, grouped by function, with one row per action.

It shows only actions in ``_ACTION_CATEGORY`` (a widget's generic
built-ins, e.g. a ``Tree``'s scrolling keys, stay out), so a new action
needs an entry there; ``test_browser_screens_help_screen.py`` checks every declared
``Binding``.
"""

from __future__ import annotations

from typing import ClassVar, override

from textual.app import ComposeResult
from textual.binding import ActiveBinding, Binding, BindingType
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Static

from synology_apm_repo.browser.screens._shared import modal_box_css
from synology_apm_repo.browser.strings import HELP_TITLE

#: ``action name -> (category, row_key, description)``, in display order; a
#: category's entries stay contiguous. Actions sharing a ``row_key`` merge
#: into one row. The description here is used rather than
#: ``Binding.description``, which can differ between an action's keys.
_ACTION_CATEGORY: dict[str, tuple[str, str, str]] = {
    # Navigate -- including HexPreviewScreen's paging.
    "cursor_down": ("Navigate", "cursor_down", "Down"),
    "cursor_up": ("Navigate", "cursor_up", "Up"),
    "select": ("Navigate", "select", "Open"),
    "select_cursor": ("Navigate", "select", "Open"),  # Tree's/DataTable's own native Enter -- same as "select"
    "go_back": ("Navigate", "go_back", "Back"),
    "cancel": ("Navigate", "cancel", "Cancel"),  # ConnectDialog's/KeyDialog's own Esc
    "cursor_to_parent": ("Navigate", "cursor_to_parent", "To parent"),
    "page_forward": ("Navigate", "page_forward", "Page +"),
    "page_back": ("Navigate", "page_back", "Page -"),
    # Export & jobs -- the worklist's t/x/Esc are absent on purpose: the
    # breadcrumb's "(t)" suffix and WORKLIST_HINT already show them.
    "export_selected": ("Export & jobs", "export_selected", "Export"),
    "background": ("Export & jobs", "background", "Background"),
    "cancel_or_back": ("Export & jobs", "cancel_or_back", "Cancel"),  # ExportScreen's Esc
    # Verify
    "show_diagnostics": ("Verify", "show_diagnostics", "Verify"),
    "run_full": ("Verify", "run_full", "Full check"),
    "verify": ("Verify", "verify", "Verify key"),  # KeyDialog's own Enter, unrelated to Repository.verify
    # Browse & view
    "show_detail": ("Browse & view", "show_detail", "Detail"),
    "hex_preview": ("Browse & view", "hex_preview", "Hex"),
    "load_more": ("Browse & view", "load_more", "Load more"),
    "refresh": ("Browse & view", "refresh", "Refresh"),
    "filter": ("Browse & view", "filter", "Filter"),
    "copy_ref": ("Browse & view", "copy_ref", "Copy ref"),
    "goto_ref": ("Browse & view", "goto_ref", "Goto ref"),
    # Connect
    "connect_remote": ("Connect", "connect_remote", "Connect"),
    # Global -- reachable from (almost) anywhere.
    "quit_app": ("Global", "quit_app", "Quit"),
    "toggle_verbose": ("Global", "toggle_verbose", "Verbose mode"),
    "show_help": ("Global", "show_help", "Help"),
    "command_palette": ("Global", "command_palette", "Command palette"),
    "dismiss_help": ("Global", "dismiss_help", "Close"),
}


class HelpScreen(ModalScreen[None]):
    """Binds only Esc; the App's ``q``/``d``/``?`` don't reach past a
    modal."""

    DEFAULT_CSS = (
        modal_box_css("HelpScreen", width=56)
        + """
    HelpScreen #help-body {
        height: auto;
        max-height: 24;
    }
    """
    )

    BINDINGS: ClassVar[list[BindingType]] = [Binding("escape", "dismiss_help", "Close", show=False)]

    def __init__(self, active_bindings: dict[str, ActiveBinding]) -> None:
        # Not ``_bindings``: DOMNode owns that name, and shadowing it breaks
        # Textual's binding resolution.
        super().__init__()
        self._active_bindings = active_bindings

    @override
    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(HELP_TITLE, id="help-title")
            with VerticalScroll(id="help-body"):
                yield Static(self._body_text(), id="help-text")

    def _body_text(self) -> str:
        # Keys grouped by (category, row_key); unlisted actions are skipped.
        merged: dict[tuple[str, str], set[str]] = {}
        for key, active in self._active_bindings.items():
            binding = active.binding
            entry = _ACTION_CATEGORY.get(binding.action)
            if entry is None:
                continue
            category, row_key, _description = entry
            merged.setdefault((category, row_key), set()).add(binding.key_display or key)

        # In _ACTION_CATEGORY's order; rendered_rows skips a merged row's
        # second entry.
        lines: list[str] = []
        rendered_rows: set[tuple[str, str]] = set()
        current_category: str | None = None
        for category, row_key, description in _ACTION_CATEGORY.values():
            row = (category, row_key)
            keys = merged.get(row)
            if keys is None or row in rendered_rows:
                continue
            rendered_rows.add(row)
            if category != current_category:
                if current_category is not None:
                    lines.append("")
                lines.append(f"[b]{category}[/b]")
                current_category = category
            joined_keys = "/".join(sorted(keys, key=lambda k: (len(k), k)))
            lines.append(f"  {joined_keys:<12} {description}")
        return "\n".join(lines).rstrip()

    def action_dismiss_help(self) -> None:
        self.dismiss(None)
