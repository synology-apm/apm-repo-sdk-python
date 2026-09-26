"""``HelpScreen``: the ``?`` key's own modal, showing every one of this
app's own bindings active on whichever screen was showing when it was
opened — grouped by function, with same-concept multi-key bindings
merged into one row.

Built from Textual's own ``Screen.active_bindings``, filtered to
``_ACTION_CATEGORY``'s explicit allowlist (a focused widget's own generic
built-ins, e.g. a ``Tree``'s scrolling keys, aren't part of this app's
keymap). A new action added later needs a matching entry here or it
silently doesn't show — ``test_browser_help_screen.py`` enforces that by
walking every real ``Binding`` this package declares and asserting each
action is in either ``_ACTION_CATEGORY`` or
``_SELF_DOCUMENTING_ELSEWHERE``.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import ActiveBinding, Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Static

from synology_apm_repo.browser.screens._shared import modal_box_css
from synology_apm_repo.browser.strings import HELP_TITLE

#: ``action name -> (category, row_key, description)`` -- this screen's
#: one hand-maintained content table; section and row order follow this
#: dict's own declaration order. Two action names sharing one
#: ``row_key`` (only ``select``/``select_cursor``, below) merge into a
#: single row; every other entry uses its own action name as its
#: row_key. This table's description always wins over Textual's own
#: ``Binding.description``, since which of two descriptions for a
#: multi-bound action (e.g. ``cursor_down``'s both this app's ``j`` and
#: a focused widget's native ``down``) appears "first" in
#: ``active_bindings`` depends on Textual's internal order, not this
#: app's wording.
_ACTION_CATEGORY: dict[str, tuple[str, str, str]] = {
    # Navigate -- movement, opening/selecting, leaving/cancelling a
    # screen or dialog, and HexPreviewScreen's own byte-window paging (a
    # form of movement through one screen's content, not a distinct
    # concept worth its own section).
    "cursor_down": ("Navigate", "cursor_down", "Down"),
    "cursor_up": ("Navigate", "cursor_up", "Up"),
    "select": ("Navigate", "select", "Open"),
    "select_cursor": ("Navigate", "select", "Open"),  # Tree's/DataTable's own native Enter -- same as "select"
    "go_back": ("Navigate", "go_back", "Back"),
    "cancel": ("Navigate", "cancel", "Cancel"),  # ConnectDialog's/KeyDialog's own Esc
    "cursor_to_parent": ("Navigate", "cursor_to_parent", "To parent"),
    "page_forward": ("Navigate", "page_forward", "Page +"),
    "page_back": ("Navigate", "page_back", "Page -"),
    # Export & jobs -- "toggle_worklist"/"cancel_selected"/"dismiss_worklist"
    # (t/x/Esc) are deliberately absent: see _SELF_DOCUMENTING_ELSEWHERE below.
    "export_selected": ("Export & jobs", "export_selected", "Export"),
    "background": ("Export & jobs", "background", "Background"),
    "cancel_or_back": ("Export & jobs", "cancel_or_back", "Cancel"),  # ExportScreen's own Esc -- distinct row
    #                                                                   from "background", both genuinely
    #                                                                   active on ExportScreen at once
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

#: Declared in this package's own keymap.py/screen BINDINGS but
#: deliberately absent from _ACTION_CATEGORY above -- each is already
#: self-documenting through different UI the moment it's reachable:
#: "toggle_worklist" (t) via the breadcrumb's own "N Task(s) (t)" suffix
#: (NavigableScreen._render_breadcrumb); "cancel_selected"/
#: "dismiss_worklist" (x/Esc) via WorklistScreen's own in-dialog
#: WORKLIST_HINT status-bar text. test_browser_help_screen.py's
#: completeness test verifies this rather than merely asserting it.
_SELF_DOCUMENTING_ELSEWHERE = frozenset({"toggle_worklist", "cancel_selected", "dismiss_worklist"})


class HelpScreen(ModalScreen[None]):
    """Only declares the one action it actually needs (``Esc`` to close)
    rather than inheriting ``COMMON_BINDINGS``:
    ``ModalScreen`` truncates the App-level binding chain at itself, so a
    help screen doesn't need ``d``/``q``/``?`` reachable from inside
    itself."""

    DEFAULT_CSS = (
        modal_box_css("HelpScreen", width=56)
        + """
    HelpScreen #help-body {
        height: auto;
        max-height: 24;
    }
    """
    )

    BINDINGS = [Binding("escape", "dismiss_help", "Close", show=False)]

    def __init__(self, active_bindings: dict[str, ActiveBinding]) -> None:
        # Named ``_active_bindings``, not ``_bindings`` -- ``DOMNode``
        # already owns ``_bindings`` (its compiled ``BindingsMap``);
        # shadowing it breaks Textual's own binding-chain resolution on
        # the first keypress.
        super().__init__()
        self._active_bindings = active_bindings

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(HELP_TITLE, id="help-title")
            with VerticalScroll(id="help-body"):
                yield Static(self._body_text(), id="help-text")

    def _body_text(self) -> str:
        # Grouped by (category, row_key), not by key -- merges every
        # same-action multi-key binding and same-row different-action
        # pair into one row. An action not in _ACTION_CATEGORY at all is
        # skipped entirely, not bucketed into a catch-all;
        # test_browser_help_screen.py's completeness test is the safety
        # net for that.
        merged: dict[tuple[str, str], set[str]] = {}
        for key, active in self._active_bindings.items():
            binding = active.binding
            entry = _ACTION_CATEGORY.get(binding.action)
            if entry is None:
                continue
            category, row_key, _description = entry
            merged.setdefault((category, row_key), set()).add(binding.key_display or key)

        # Walked in _ACTION_CATEGORY's own declaration order -- every
        # category's entries must stay contiguous, or this emits two
        # headers for one category. rendered_rows skips a merged
        # pair's second entry.
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
