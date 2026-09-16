"""``HelpScreen``: the ``?`` key's own modal, listing every binding actually
active on whichever screen was showing when it was opened.

Built from Textual's own public ``Screen.active_bindings`` property (which
merges screen- and app-level bindings, the same source ``Footer`` reads from)
rather than a hand-maintained string, so the list can never drift from
``keymap.py``'s real ``BINDINGS`` the way a separately-spelled-out help text
could.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import ActiveBinding, Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.screens._shared import modal_box_css
from synology_apm_repo.browser.strings import HELP_COLUMNS, HELP_TITLE, HELP_VERBOSE_NOTE


class HelpScreen(ModalScreen[None]):
    """See module docstring. Only declares the one action it actually
    needs (``Esc`` to close) rather than inheriting ``COMMON_BINDINGS`` —
    same reasoning as ``KeyDialog``'s own docstring: a help screen doesn't
    need ``d``/``q``/``?`` reachable from inside itself."""

    DEFAULT_CSS = modal_box_css("HelpScreen", width=60)

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
            yield DataTable(id="help-table")
            yield Static(HELP_VERBOSE_NOTE, id="help-note")

    def on_mount(self) -> None:
        table = self.query_one("#help-table", DataTable)
        table.add_columns(*HELP_COLUMNS)
        table.cursor_type = "none"
        for key, active in sorted(self._active_bindings.items()):
            binding = active.binding
            table.add_row(binding.key_display or key, binding.description)

    def action_dismiss_help(self) -> None:
        self.dismiss(None)
