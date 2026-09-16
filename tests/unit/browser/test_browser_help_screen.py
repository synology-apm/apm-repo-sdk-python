"""Unit tests for ``HelpScreen`` — driven through a real Textual ``Pilot``
against a small, hand-built ``bindings`` dict, so this needs no real
Session/sample data and lives in ``tests/unit/``: it's pure Textual bindings
introspection, not anything ``HelpScreen`` reads from a repository."""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.binding import ActiveBinding, Binding
from textual.widgets import DataTable

from synology_apm_repo.browser.screens.help_screen import HelpScreen


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        # ``ActiveBinding.node`` must be a real ``DOMNode`` -- ``HelpScreen``
        # itself only ever reads ``.binding`` off each value, so which node
        # is attributed doesn't matter functionally, only for typing.
        active_bindings = {
            "q": ActiveBinding(self, Binding("q", "quit_app", "Quit"), True, ""),
            "d": ActiveBinding(self, Binding("d", "toggle_verbose", "Verbose"), True, ""),
        }
        self.push_screen(HelpScreen(active_bindings))


async def test_help_screen_renders_a_row_per_binding() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, HelpScreen)
        table = screen.query_one("#help-table", DataTable)
        assert table.row_count == 2
        rendered = {str(table.get_row_at(row)[0]): str(table.get_row_at(row)[1]) for row in range(table.row_count)}
        assert rendered == {"q": "Quit", "d": "Verbose"}


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
