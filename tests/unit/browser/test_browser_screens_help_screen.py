"""Unit tests for ``HelpScreen`` against hand-built ``active_bindings`` dicts.
``test_declared_actions_all_have_a_category`` walks every ``Binding`` this
package declares, so a new action can't silently go missing from ``?`` help.
"""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.binding import ActiveBinding, Binding

from support.pilot import wait_for_screen, wait_until
from synology_apm_repo.browser import keymap
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.browser.screens.help_screen import _ACTION_CATEGORY, HelpScreen
from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen

#: Actions in ``_ACTION_CATEGORY`` that come from a widget's native binding
#: (Tree's/DataTable's Enter) or Textual's command palette, not from this
#: package's keymap.py/BINDINGS, so the completeness walk can't find them.
_NOT_DECLARED_IN_THIS_PACKAGE = {"select_cursor", "command_palette"}

#: Bound actions help leaves out because other UI shows them: ``t`` in the
#: breadcrumb's "N Tasks (t)" suffix, ``x``/Esc in ``WorklistScreen``'s
#: ``WORKLIST_HINT``.
_SHOWN_ELSEWHERE = {"toggle_worklist", "cancel_selected", "dismiss_worklist"}


def _actions_in(value: object) -> set[str]:
    if isinstance(value, Binding):
        return {value.action}
    if isinstance(value, list):
        return {b.action for b in value if isinstance(b, Binding)}
    return set()


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        # HelpScreen reads only ``.binding``; the node is there for typing.
        active_bindings = {
            "q": ActiveBinding(self, Binding("q", "quit_app", "Quit"), True, ""),
            "d": ActiveBinding(self, Binding("d", "toggle_verbose", "Verbose mode"), True, ""),
        }
        self.push_screen(HelpScreen(active_bindings))


def test_declared_actions_all_have_a_category() -> None:
    declared: set[str] = set()
    for name in dir(keymap):
        if name.startswith("_"):
            continue
        declared |= _actions_in(getattr(keymap, name))
    for screen_cls in (
        BrowseScreen,
        UnitScreen,
        DiagnosticsScreen,
        HexPreviewScreen,
        ExportScreen,
        WorklistScreen,
        ConnectDialog,
        KeyDialog,
        HelpScreen,
        ApmRepoBrowserApp,
    ):
        declared |= _actions_in(screen_cls.BINDINGS)

    missing = declared - set(_ACTION_CATEGORY) - _NOT_DECLARED_IN_THIS_PACKAGE - _SHOWN_ELSEWHERE
    assert not missing, f"actions with no HelpScreen category: {sorted(missing)}"

    # A _SHOWN_ELSEWHERE entry no longer declared would excuse nothing.
    assert declared >= _SHOWN_ELSEWHERE


def test_every_not_declared_exception_is_still_covered() -> None:
    assert set(_ACTION_CATEGORY) >= _NOT_DECLARED_IN_THIS_PACKAGE


def test_actions_shown_elsewhere_are_not_also_in_action_category() -> None:
    assert not (_SHOWN_ELSEWHERE & set(_ACTION_CATEGORY))


async def test_help_screen_renders_one_row_per_action_grouped_by_category() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        screen = await wait_for_screen(pilot, HelpScreen)
        text = screen._body_text()
        assert "[b]Global[/b]" in text
        assert "q            Quit" in text
        assert "d            Verbose mode" in text


def test_help_screen_merges_same_action_reached_through_two_keys() -> None:
    active_bindings = {
        "j": ActiveBinding(App(), Binding("j", "cursor_down", "Down"), True, ""),
        "down": ActiveBinding(App(), Binding("down", "cursor_down", "Cursor Down"), True, ""),
    }
    text = HelpScreen(active_bindings)._body_text()
    assert text.count("Down") == 1
    assert "down/j" in text or "j/down" in text


def test_help_screen_merges_select_and_select_cursor_into_one_row() -> None:
    active_bindings = {
        "l": ActiveBinding(App(), Binding("l", "select", "Open"), True, ""),
        "enter": ActiveBinding(App(), Binding("enter", "select_cursor", "Select"), True, ""),
    }
    text = HelpScreen(active_bindings)._body_text()
    lines = [line for line in text.splitlines() if "Open" in line]
    assert len(lines) == 1
    assert "enter" in lines[0] and "l" in lines[0]


def test_help_screen_orders_equal_length_keys_deterministically() -> None:
    # Equal-length keys must tiebreak alphabetically, not on set iteration
    # order (randomized per process by PYTHONHASHSEED).
    active_bindings = {
        "plus": ActiveBinding(App(), Binding("plus", "load_more", "Load more", key_display="+"), True, ""),
        "equals_sign": ActiveBinding(
            App(), Binding("equals_sign", "load_more", "Load more", key_display="="), True, ""
        ),
    }
    text = HelpScreen(active_bindings)._body_text()
    assert "+/=" in text


def test_help_screen_skips_actions_outside_the_allowlist() -> None:
    active_bindings = {
        "space": ActiveBinding(App(), Binding("space", "toggle_node", "Toggle"), True, ""),
    }
    text = HelpScreen(active_bindings)._body_text()
    assert text == ""


async def test_action_dismiss_help_dismisses_it() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        screen = await wait_for_screen(pilot, HelpScreen)
        screen.action_dismiss_help()
        await wait_until(pilot, lambda: app.screen is not screen)


async def test_escape_dismisses_it() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        screen = await wait_for_screen(pilot, HelpScreen)
        await pilot.press("escape")
        await wait_until(pilot, lambda: app.screen is not screen)
