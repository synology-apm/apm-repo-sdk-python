"""Unit tests for ``HelpScreen`` — driven through a real Textual ``Pilot``
against small, hand-built ``active_bindings`` dicts, so this needs no real
Session/sample data and lives in ``tests/unit/``: it's pure Textual bindings
introspection, not anything ``HelpScreen`` reads from a repository.

The completeness test walks every real ``Binding`` this package declares
(``keymap.py`` plus every screen's own ``BINDINGS``) and asserts each
action is present in either ``help_screen._ACTION_CATEGORY`` or
``help_screen._SELF_DOCUMENTING_ELSEWHERE`` — the mechanism the module
docstring promises, guarding against a new action silently thinning the
``?`` help text instead of failing a test.
"""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.binding import ActiveBinding, Binding

from synology_apm_repo.browser import keymap
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.browser.screens.help_screen import _ACTION_CATEGORY, _SELF_DOCUMENTING_ELSEWHERE, HelpScreen
from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen

#: Actions reachable only through a focused widget's own native binding
#: (Tree's/DataTable's Enter) or Textual's own App-level command palette --
#: never declared in this package's own keymap.py/screen BINDINGS, so the
#: completeness walk below can't find them there, but each is still
#: deliberately present in _ACTION_CATEGORY since that allowlist is built
#: from Textual's own active_bindings (which also carries every focused
#: widget's native bindings and App-level built-ins, not just this
#: package's own keymap.py/BINDINGS) and covered separately by
#: test_help_screen_merges_select_and_select_cursor_into_one_row.
_NOT_DECLARED_IN_THIS_PACKAGE = {"select_cursor", "command_palette"}


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
        # ``ActiveBinding.node`` must be a real ``DOMNode`` -- ``HelpScreen``
        # itself only ever reads ``.binding`` off each value, so which node
        # is attributed doesn't matter functionally, only for typing.
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

    missing = declared - set(_ACTION_CATEGORY) - _NOT_DECLARED_IN_THIS_PACKAGE - _SELF_DOCUMENTING_ELSEWHERE
    assert not missing, f"actions with no HelpScreen category: {sorted(missing)}"

    # Every _SELF_DOCUMENTING_ELSEWHERE entry must actually be one of this
    # package's own declared actions -- otherwise it's excusing nothing.
    assert declared >= _SELF_DOCUMENTING_ELSEWHERE


def test_every_not_declared_exception_is_still_covered() -> None:
    # Guards the allowlist itself against going stale in the other
    # direction -- an entry that no longer needs the exception (because a
    # screen now declares it directly) should be removed from it.
    assert set(_ACTION_CATEGORY) >= _NOT_DECLARED_IN_THIS_PACKAGE


def test_self_documenting_elsewhere_actions_are_not_also_in_action_category() -> None:
    # Each is deliberately absent from the help text, not merely
    # redundant -- if one ever gets added back to _ACTION_CATEGORY, this
    # exception set should shrink to match, not grow stale alongside it.
    assert not (_SELF_DOCUMENTING_ELSEWHERE & set(_ACTION_CATEGORY))


async def test_help_screen_renders_one_row_per_action_grouped_by_category() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, HelpScreen)
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
    # Real repro: "+"/"=" (load_more's two keys) are both length 1, so a
    # plain `sorted(keys, key=len)` tiebreaks on set iteration order --
    # which Python randomizes across processes (PYTHONHASHSEED) -- unless
    # the sort key also breaks ties alphabetically.
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
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, HelpScreen)
        screen.action_dismiss_help()
        await pilot.pause()
        assert app.screen is not screen


async def test_escape_dismisses_it() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, HelpScreen)
        await pilot.press("escape")
        await pilot.pause()
        assert app.screen is not screen


__all__: list[str] = []
