"""Unit tests for ``browser.screens._shared``'s own branches none of the
screens built on it happen to exercise:
``move_cursor_to_parent``'s no-selection guard, ``parse_canonical_ref``'s
parse-failure branch, and ``NavigableScreen.action_cursor_down``/
``action_cursor_up`` (every other browser test navigates a ``Tree``
directly via ``move_cursor()``/``expand()`` rather than pressing
``j``/``k``, per ``open_browser_pilot``'s own docstring)."""

from __future__ import annotations

from typing import Any

from textual.app import App, ComposeResult
from textual.widgets import Tree

from synology_apm_repo.browser.screens._shared import NavigableScreen, move_cursor_to_parent, parse_canonical_ref
from synology_apm_repo.browser.strings import GOTO_REF_NOT_CANONICAL_WARNING, GOTO_REF_PARSE_ERROR_WARNING


def test_move_cursor_to_parent_with_nothing_selected_is_a_no_op() -> None:
    tree: Tree[str] = Tree("root")  # unmounted -- cursor_node starts None, not tree.root
    move_cursor_to_parent(tree)  # must not raise
    assert tree.cursor_node is None


def test_parse_canonical_ref_text_with_no_hash_notifies_a_parse_error() -> None:
    """``NodeRef.parse`` raises ``ValueError`` only when ``text`` has no
    ``#`` at all — its own docstring."""
    warnings: list[tuple[str, str]] = []

    def notify(message: str, *, severity: str = "information") -> None:
        warnings.append((message, severity))

    result = parse_canonical_ref("/some/path", notify=notify)
    assert result is None
    assert warnings == [(GOTO_REF_PARSE_ERROR_WARNING, "warning")]


def test_parse_canonical_ref_a_human_ref_is_rejected_as_not_canonical() -> None:
    warnings: list[tuple[str, str]] = []

    def notify(message: str, *, severity: str = "information") -> None:
        warnings.append((message, severity))

    result = parse_canonical_ref("/some/path#not-canonical", notify=notify)
    assert result is None
    assert warnings == [(GOTO_REF_NOT_CANONICAL_WARNING, "warning")]


class _NavScreen(NavigableScreen):
    def compose(self) -> ComposeResult:
        yield Tree[str]("root", id="nav-tree")

    def on_mount(self) -> None:
        tree = self.query_one("#nav-tree", Tree)
        tree.root.add_leaf("a")
        tree.root.add_leaf("b")
        tree.root.expand()
        tree.focus()


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(_NavScreen())


async def test_action_cursor_down_and_up_forward_to_the_focused_tree(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#nav-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) == 2, timeout=0.6, interval=0.02)
        screen = app.screen
        assert isinstance(screen, NavigableScreen)
        start_line = tree.cursor_line

        screen.action_cursor_down()
        await pilot.pause()
        assert tree.cursor_line == start_line + 1

        screen.action_cursor_up()
        await pilot.pause()
        assert tree.cursor_line == start_line


__all__: list[str] = []
