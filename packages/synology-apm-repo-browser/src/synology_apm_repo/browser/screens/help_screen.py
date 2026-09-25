"""``HelpScreen``: the ``?`` key's own modal, showing every one of *this
app's own* bindings active on whichever screen was showing when it was
opened — grouped by function, with same-concept multi-key bindings
(``l``/``Enter`` both meaning "open", say) merged into one row, rather
than one flat, alphabetical-by-key list.

Built from Textual's own public ``Screen.active_bindings`` property (the
same source ``Footer`` reads from, ``Footer``'s own command-palette hint
included), filtered down to ``_ACTION_CATEGORY``'s own explicit allowlist
of this app's real action names -- ``active_bindings`` also carries every
focused widget's own generic built-ins (a ``Tree``'s scrolling/clipboard/
focus-cycling keys, none of them part of this app's documented keymap at
all), which would otherwise swamp a screen meant to stay concise. The
trade-off: a genuinely new action this app adds later needs a matching
entry here too, or it silently doesn't show -- ``test_browser_help_screen.py``
enforces that by walking every real ``Binding`` this package declares
(``keymap.py`` plus every screen's own ``BINDINGS``) and asserting each
action is present in either ``_ACTION_CATEGORY`` or
``_SELF_DOCUMENTING_ELSEWHERE`` (the actions deliberately left out of
the help text because a different piece of UI already names them), so
that drift fails a test instead of silently thinning the help text.
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
#: one hand-maintained piece of content. Both the section order and each row's
#: order within its section are simply this dict's own definition order
#: below -- Python dicts preserve insertion order, and since this is a
#: hand-written literal, that's exactly the order this module's author
#: chose, with no separate ordering list or position number to keep in
#: sync with it. ``row_key`` is what decides whether two entries share
#: one row: two *different* action names deliberately sharing one (only
#: ``select``/``select_cursor``, below) get the same ``row_key`` and are
#: merged into a single row, since they're the same concept reached
#: through different underlying Textual mechanics
#: (``NavigableScreen.action_select`` forwarding to whichever widget is
#: focused, vs that widget's own native ``enter`` binding winning the
#: same key first) -- every other entry uses its own action name as its
#: row_key, so two genuinely different actions never merge by accident.
#: The description here always wins over whatever Textual's own
#: ``Binding.description`` happens to say, deliberately: for an action
#: reachable through more than one underlying binding (``cursor_down`` is
#: both this app's own ``j`` on the ``Screen`` *and* a focused
#: ``Tree``/``DataTable``'s own native ``down`` arrow key), which of
#: those two descriptions ends up "first" in ``active_bindings`` depends
#: on Textual's own internal binding-chain order, not this app's wording
#: -- so this table's own description is used unconditionally instead of
#: ever trusting whichever one happened to arrive first.
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
#: self-documenting through a different piece of UI the moment it's
#: actually reachable, so repeating it here would just be a second,
#: redundant place for the same fact to drift out of sync:
#: "toggle_worklist" (t) is the exact key the breadcrumb's own "N
#: Task(s) (t)" suffix (NavigableScreen._render_breadcrumb) names, and
#: only appears once a job exists to open the dialog for;
#: "cancel_selected"/"dismiss_worklist" (x/Esc) are
#: WorklistScreen's own in-dialog WORKLIST_HINT status-bar text, visible
#: the moment either key is actually reachable. See
#: test_browser_help_screen.py's completeness test for how this is
#: verified rather than merely asserted here.
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
        # Named ``_active_bindings``, not ``_bindings`` -- ``DOMNode`` (a
        # ``Screen``'s own base class) already owns an attribute called
        # ``_bindings`` (its compiled ``BindingsMap``); shadowing it with
        # this plain dict breaks Textual's own binding-chain resolution the
        # moment any key is pressed (``AttributeError: 'dict' object has no
        # attribute 'key_to_bindings'``).
        super().__init__()
        self._active_bindings = active_bindings

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(HELP_TITLE, id="help-title")
            with VerticalScroll(id="help-body"):
                yield Static(self._body_text(), id="help-text")

    def _body_text(self) -> str:
        # Grouped by (category, row_key), not by key: this is what
        # merges every same-action, multiple-key binding (l/Enter both
        # "select", h/Esc both "go_back", +/= both "load_more", ...) and
        # every same-row, different-action pair ("select"/"select_cursor",
        # deliberately sharing one row_key up in _ACTION_CATEGORY) into
        # one row. Every action
        # not in _ACTION_CATEGORY at all -- a focused widget's own generic
        # built-in (Tree's scrolling/clipboard/focus-cycling keys, none of
        # it part of this app's own keymap) -- is skipped entirely, not
        # bucketed into a catch-all section; test_browser_help_screen.py's
        # completeness test is the safety net traded for that conciseness.
        merged: dict[tuple[str, str], set[str]] = {}
        for key, active in self._active_bindings.items():
            binding = active.binding
            entry = _ACTION_CATEGORY.get(binding.action)
            if entry is None:
                continue
            category, row_key, _description = entry
            merged.setdefault((category, row_key), set()).add(binding.key_display or key)

        # Walked in _ACTION_CATEGORY's own declaration order -- every
        # category's entries must stay contiguous there, or this would
        # emit two separate headers for one category instead of one.
        # rendered_rows skips the second of
        # a merged pair ("select_cursor" after "select" already rendered
        # its shared row).
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
