"""Unit tests for ``browser.screens._shared``'s own branches none of the
screens built on it happen to exercise:
``move_cursor_to_parent``'s no-selection guard, ``parse_canonical_ref``'s
parse-failure branch, and ``NavigableScreen.action_cursor_down``/
``action_cursor_up`` (every other browser test navigates a ``Tree``
directly via ``move_cursor()``/``expand()`` rather than pressing
``j``/``k``)."""

from __future__ import annotations

from typing import Any

from textual.app import App, ComposeResult
from textual.reactive import var
from textual.widgets import Static, Tree

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens._shared import (
    NavigableScreen,
    _breadcrumb_with_tasks_hint,
    move_cursor_to_parent,
    parse_canonical_ref,
)
from synology_apm_repo.browser.strings import GOTO_REF_NOT_CANONICAL_WARNING, GOTO_REF_PARSE_ERROR_WARNING


def test_move_cursor_to_parent_with_nothing_selected_is_a_no_op() -> None:
    tree: Tree[str] = Tree("root")  # unmounted -- cursor_node starts None, not tree.root
    move_cursor_to_parent(tree)  # must not raise
    assert tree.cursor_node is None


def test_parse_canonical_ref_text_with_no_hash_notifies_a_parse_error() -> None:
    """``NodeRef.parse`` raises ``ValueError`` only when ``text`` has no
    ``#`` at all (not a ref)."""
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


async def test_action_cursor_down_and_up_forward_to_the_focused_tree(wait_until: Any, ui_timeout: float) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#nav-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) == 2, timeout=ui_timeout, interval=0.02)
        screen = app.screen
        assert isinstance(screen, NavigableScreen)
        start_line = tree.cursor_line

        screen.action_cursor_down()
        await pilot.pause()
        assert tree.cursor_line == start_line + 1

        screen.action_cursor_up()
        await pilot.pause()
        assert tree.cursor_line == start_line


class TestBreadcrumbWithTasksHint:
    def test_no_jobs_returns_the_text_unchanged(self) -> None:
        assert _breadcrumb_with_tasks_hint("apv-sample-1", 0, 80) == "apv-sample-1"

    def test_one_job_appends_a_right_aligned_singular_hint(self) -> None:
        result = _breadcrumb_with_tasks_hint("apv-sample-1", 1, 30)
        assert result == "apv-sample-1" + " " * 8 + "1 Task (t)"

    def test_multiple_jobs_pluralizes(self) -> None:
        assert _breadcrumb_with_tasks_hint("apv-sample-1", 3, 30).endswith("3 Tasks (t)")

    def test_markup_in_text_is_measured_by_its_plain_length_not_raw_length(self) -> None:
        # "[b]x[/b]" is 8 raw characters but renders as a single "x" --
        # padding must be computed against that plain length, or the
        # hint would land too far to the right.
        result = _breadcrumb_with_tasks_hint("[b]x[/b]", 1, 15)
        assert result == "[b]x[/b]" + " " * 4 + "1 Task (t)"

    def test_not_enough_room_returns_the_text_unchanged_rather_than_overlapping(self) -> None:
        # width is deliberately smaller than len(text) + len(hint) --
        # showing nothing extra beats a garbled overlap.
        assert _breadcrumb_with_tasks_hint("a fairly long breadcrumb path here", 1, 20) == (
            "a fairly long breadcrumb path here"
        )

    def test_exactly_zero_room_returns_the_text_unchanged(self) -> None:
        text = "x"
        hint_width = len("1 Task (t)")
        assert _breadcrumb_with_tasks_hint(text, 1, len(text) + hint_width) == text


class _BreadcrumbScreen(NavigableScreen):
    def compose(self) -> ComposeResult:
        yield Static("", id="breadcrumb")

    def on_mount(self) -> None:
        super().on_mount()
        self._update_breadcrumb_text("apv-sample-1")


class _BreadcrumbApp(App[None]):
    # The real theme.tcss, not a bare Static -- #breadcrumb's own real
    # "padding: 0 1" is exactly what a real, reported bug involved (the
    # hint's own "(t)" silently clipped off the right edge because
    # _render_breadcrumb used the *screen's* raw width, 2 columns wider
    # than the padded widget's own usable one).
    CSS_PATH = ApmRepoBrowserApp.CSS_PATH
    jobs: var[dict[object, object]] = var(dict)

    def on_mount(self) -> None:
        self.push_screen(_BreadcrumbScreen())


async def test_breadcrumb_tasks_hint_fits_within_the_real_padded_breadcrumb_width(wait_until: Any) -> None:
    """The "N Task(s) (t)" suffix's own trailing "(t)" must survive the
    real, padded ``#breadcrumb`` widget's paint, not just be present in
    the *string* ``Static`` was given -- a plain ``in``/equality check
    on that string (as ``test_browser_pilot.py``'s own cross-screen sync
    test already does) can't catch a *painted* line overflowing the
    widget's own usable width (2 columns narrower than its full width,
    ``theme.tcss``'s own "padding: 0 1"), which clips the tail at paint
    time. Asserts the rendered line actually fits that usable width."""
    app = _BreadcrumbApp()
    async with app.run_test(size=(40, 10)) as pilot:
        await pilot.pause()
        app.jobs = {"job-1": object()}
        await wait_until(pilot, lambda: "Task" in str(app.screen.query_one("#breadcrumb", Static).render()))
        breadcrumb = app.screen.query_one("#breadcrumb", Static)
        rendered = str(breadcrumb.render())
        assert "1 Task (t)" in rendered
        usable_width = app.screen.size.width - breadcrumb.styles.padding.width
        assert len(rendered) <= usable_width, (rendered, usable_width)


async def test_a_covered_screens_breadcrumb_is_skipped_then_catches_up_on_resume(wait_until: Any) -> None:
    """A background job tick must not re-render a screen still mounted
    but covered by another screen on top of it -- wasted work that
    would otherwise scale with navigation depth. The covered screen's
    own ``#breadcrumb`` must stay untouched while covered, then
    immediately reflect the real, current job count the moment it
    becomes visible again (``on_screen_resume``), never a stale count
    left over from before it was covered."""
    app = _BreadcrumbApp()
    async with app.run_test(size=(40, 10)) as pilot:
        await pilot.pause()
        bottom_screen = app.screen
        assert isinstance(bottom_screen, _BreadcrumbScreen)
        bottom_breadcrumb = bottom_screen.query_one("#breadcrumb", Static)

        await app.push_screen(_BreadcrumbScreen())
        await pilot.pause()
        assert app.screen is not bottom_screen  # a second screen now covers it

        before = str(bottom_breadcrumb.render())
        app.jobs = {"job-1": object()}
        await pilot.pause()
        # Still exactly what it was before the job count changed -- the
        # covered screen's own render was skipped, not just late.
        assert str(bottom_breadcrumb.render()) == before
        assert "Task" not in before

        app.pop_screen()
        await wait_until(pilot, lambda: app.screen is bottom_screen)
        await pilot.pause()
        assert "1 Task (t)" in str(bottom_breadcrumb.render())


__all__: list[str] = []
