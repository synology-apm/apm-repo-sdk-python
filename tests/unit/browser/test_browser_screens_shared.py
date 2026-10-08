"""Unit tests for ``browser.screens._shared`` branches the screens built on
it don't exercise: ``move_cursor_to_parent``'s no-selection guard,
``parse_canonical_ref``'s failure branches, ``NavigableScreen``'s
``action_cursor_down``/``action_cursor_up``, and the breadcrumb tasks
hint."""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.reactive import var
from textual.widgets import Static, Tree

from support.pilot import UI_TIMEOUT, settle, wait_for_screen, wait_until
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens._shared import (
    NavigableScreen,
    breadcrumb_with_tasks_hint,
    move_cursor_to_parent,
    parse_canonical_ref,
)
from synology_apm_repo.browser.strings import GOTO_REF_NOT_CANONICAL_WARNING, GOTO_REF_PARSE_ERROR_WARNING


def test_move_cursor_to_parent_with_nothing_selected_is_a_no_op() -> None:
    tree: Tree[str] = Tree("root")  # unmounted -- cursor_node starts None, not tree.root
    move_cursor_to_parent(tree)  # must not raise
    assert tree.cursor_node is None


def test_parse_canonical_ref_text_with_no_hash_notifies_a_parse_error() -> None:
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


async def test_action_cursor_down_and_up_forward_to_the_focused_tree() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#nav-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) == 2, timeout=UI_TIMEOUT, interval=0.02)
        screen = app.screen
        assert isinstance(screen, NavigableScreen)
        start_line = tree.cursor_line

        screen.action_cursor_down()
        await wait_until(pilot, lambda: tree.cursor_line == start_line + 1)

        screen.action_cursor_up()
        await wait_until(pilot, lambda: tree.cursor_line == start_line)


class TestBreadcrumbWithTasksHint:
    def test_no_jobs_returns_the_text_unchanged(self) -> None:
        assert breadcrumb_with_tasks_hint("alice-backup", 0, 80) == "alice-backup"

    def test_one_job_appends_a_right_aligned_singular_hint(self) -> None:
        result = breadcrumb_with_tasks_hint("alice-backup", 1, 30)
        assert result == "alice-backup" + " " * 8 + "1 Task (t)"

    def test_multiple_jobs_pluralizes(self) -> None:
        assert breadcrumb_with_tasks_hint("alice-backup", 3, 30).endswith("3 Tasks (t)")

    def test_markup_in_text_is_measured_by_its_plain_length_not_raw_length(self) -> None:
        # "[b]x[/b]" renders as one character, so padding uses the plain length.
        result = breadcrumb_with_tasks_hint("[b]x[/b]", 1, 15)
        assert result == "[b]x[/b]" + " " * 4 + "1 Task (t)"

    def test_not_enough_room_returns_the_text_unchanged_rather_than_overlapping(self) -> None:
        assert breadcrumb_with_tasks_hint("a fairly long breadcrumb path here", 1, 20) == (
            "a fairly long breadcrumb path here"
        )

    def test_exactly_zero_room_returns_the_text_unchanged(self) -> None:
        text = "x"
        hint_width = len("1 Task (t)")
        assert breadcrumb_with_tasks_hint(text, 1, len(text) + hint_width) == text


class _BreadcrumbScreen(NavigableScreen):
    def compose(self) -> ComposeResult:
        yield Static("", id="breadcrumb")

    def on_mount(self) -> None:
        super().on_mount()
        self._update_breadcrumb_text("alice-backup")


class _BreadcrumbApp(App[None]):
    # The real theme.tcss: #breadcrumb's "padding: 0 1" narrows its usable
    # width by 2 columns.
    CSS_PATH = ApmRepoBrowserApp.CSS_PATH
    jobs: var[dict[object, object]] = var(dict)

    def on_mount(self) -> None:
        self.push_screen(_BreadcrumbScreen())


async def test_breadcrumb_tasks_hint_fits_within_the_real_padded_breadcrumb_width() -> None:
    """The rendered breadcrumb, hint included, fits the widget's padded
    usable width, so the trailing "(t)" isn't clipped at paint time."""
    app = _BreadcrumbApp()
    async with app.run_test(size=(40, 10)) as pilot:
        await wait_for_screen(pilot, _BreadcrumbScreen)
        app.jobs = {"job-1": object()}
        await wait_until(pilot, lambda: "Task" in str(app.screen.query_one("#breadcrumb", Static).render()))
        breadcrumb = app.screen.query_one("#breadcrumb", Static)
        rendered = str(breadcrumb.render())
        assert "1 Task (t)" in rendered
        usable_width = app.screen.size.width - breadcrumb.styles.padding.width
        assert len(rendered) <= usable_width, (rendered, usable_width)


async def test_a_covered_screens_breadcrumb_is_skipped_then_catches_up_on_resume() -> None:
    """A job tick doesn't re-render a covered screen's ``#breadcrumb``;
    it shows the current job count once the screen resumes."""
    app = _BreadcrumbApp()
    async with app.run_test(size=(40, 10)) as pilot:
        bottom_screen = await wait_for_screen(pilot, _BreadcrumbScreen)
        bottom_breadcrumb = bottom_screen.query_one("#breadcrumb", Static)

        await app.push_screen(_BreadcrumbScreen())
        await wait_until(pilot, lambda: app.screen is not bottom_screen)  # a second screen now covers it

        before = str(bottom_breadcrumb.render())
        app.jobs = {"job-1": object()}
        await settle(pilot)
        assert str(bottom_breadcrumb.render()) == before
        assert "Task" not in before

        app.pop_screen()
        await wait_until(pilot, lambda: app.screen is bottom_screen)
        await wait_until(pilot, lambda: "1 Task (t)" in str(bottom_breadcrumb.render()))
