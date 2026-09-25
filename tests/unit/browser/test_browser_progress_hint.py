"""Unit tests for ``DebouncedProgress``'s two sinks: the default
``_BreadcrumbSink`` (unchanged behavior, exercised here as a self-contained
module test alongside its sibling) and ``TreeNodeLoadingSink`` (targets a
specific ``TreeNode``'s own label instead, for a genuine tree-node-expand
call site — see ``browse_screen.py``'s ``_load_catalogs_for``/
``unit_screen.py``'s ``_load_children``).

Each test calls the private ``_start_animating()`` directly rather than
waiting out the real 300ms debounce delay — the same "reach past the
timer" shape every other precisely-timed detail in this suite avoids
sleeping for real."""

from __future__ import annotations

from typing import Any, cast

from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static, Tree

from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.detail_pane import DetailPane, DetailPaneLoadingSink
from synology_apm_repo.browser.widgets.progress_hint import (
    DataTableLoadingRowSink,
    DebouncedProgress,
    StaticTextSink,
    TreeNodeLoadingSink,
)
from synology_apm_repo.sdk.api import Session
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _node(name: str) -> Node:
    return Node(ref=NodeRef("repo", ("root", name)), name=name, is_leaf=True)


class _FakeApp(App[None]):
    """A bare ``App`` (not ``ApmRepoBrowserApp``) is enough here, plus
    ``resources``/``session``, since ``BrowseScreen`` routes every
    ``Repository`` through ``ResourceTable``."""

    def __init__(self) -> None:
        super().__init__()
        self.jobs: dict[JobId, Job] = {}
        self.verbose = False
        self.session = Session()
        self.resources = ResourceTable(self.session)

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(BrowseScreen())


async def test_breadcrumb_sink_shows_and_hides_on_the_screens_breadcrumb() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        progress = DebouncedProgress(screen)
        progress._start_animating()
        assert "Loading" in str(screen.query_one("#breadcrumb", Static).render())
        progress.stop()
        assert "Loading" not in str(screen.query_one("#breadcrumb", Static).render())


async def test_tree_node_sink_shows_and_hides_on_the_nodes_own_label_not_the_breadcrumb() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        node = tree.root.add("original-label")

        progress = DebouncedProgress(screen, TreeNodeLoadingSink(node))
        progress._start_animating()
        assert "original-label" in str(node.label)
        assert "Loading" in str(node.label)
        # A tree-node-targeted sink must never also touch the breadcrumb.
        assert "Loading" not in str(screen.query_one("#breadcrumb", Static).render())

        progress.stop()
        assert str(node.label) == "original-label"


async def test_tree_node_sink_preserves_an_external_relabel_that_happens_mid_animation() -> None:
    """Regression test: an external caller relabeling the same node while
    this sink is animating (e.g. ``BrowseScreen._refresh_repo_labels``,
    run when the user toggles verbose mode mid-load) must not be
    clobbered by a stale label snapshot -- the sink strips only its own
    previously-appended suffix each tick, so it picks up the new real
    label as its base instead of overwriting it with the old one."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        node = tree.root.add("original-label")

        progress = DebouncedProgress(screen, TreeNodeLoadingSink(node))
        progress._start_animating()  # calls the private starter directly, skipping the real 300ms debounce delay
        assert "original-label" in str(node.label)

        # Something else relabels the node mid-load -- e.g. a verbose-mode
        # toggle -- while the sink is still animating.
        node.set_label("relabeled-while-loading")

        progress._tick()
        assert "relabeled-while-loading" in str(node.label)
        assert "original-label" not in str(node.label)

        progress.stop()
        assert str(node.label) == "relabeled-while-loading"


async def test_tree_node_sink_restores_the_label_correctly_even_if_real_children_were_added_meanwhile() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        node = tree.root.add("original-label")

        progress = DebouncedProgress(screen, TreeNodeLoadingSink(node))
        progress._start_animating()
        # Real children arriving mid-load (the actual ordering both
        # _load_catalogs_for and _load_children follow) must not disturb
        # the parent's own label restore -- children are separate
        # TreeNodes, never encoded into the parent's own label text.
        node.add_leaf("a-real-child")
        progress.stop()

        assert str(node.label) == "original-label"
        assert [str(child.label) for child in node.children] == ["a-real-child"]


async def test_static_text_sink_shows_and_hides_relative_to_a_supplied_base() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        screen.query_one("#open-status", Static).update("found 2 repositories")

        sink = StaticTextSink(screen, "#open-status", base=lambda: "found 2 repositories")
        progress = DebouncedProgress(screen, sink)
        progress._start_animating()
        text = str(screen.query_one("#open-status", Static).render())
        assert "found 2 repositories" in text
        assert "Loading" in text

        progress.stop()
        assert str(screen.query_one("#open-status", Static).render()) == "found 2 repositories"


async def test_static_text_sink_hide_never_clobbers_a_real_result_written_after_show() -> None:
    """Regression test: ``work()`` wraps a decorated method's *entire*
    body, so a real final result (``KeyDialog._verify``'s
    ``status.update(text)``, ``DiagnosticsScreen._run``'s
    ``_show_findings``/``show_error``) can already be on screen by the
    time ``hide()`` runs, if the call was slow enough to trigger the
    sink at all. ``hide()`` must not reset to ``base()`` once that's
    happened -- only when nothing else touched the widget since ``show()``."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        status = screen.query_one("#open-status", Static)

        sink = StaticTextSink(screen, "#open-status", base=lambda: "")
        sink.show("⠹")
        assert "Loading" in str(status.render())

        # Simulates the decorated method's own tail writing its real
        # result while still inside work()'s wrap, before hide() runs.
        status.update("[green]verified[/green]")

        sink.hide()
        assert str(status.render()) == "verified"


async def test_static_text_sink_hide_still_resets_to_base_when_nothing_else_wrote() -> None:
    """The other half of the same fix: when nothing superseded the
    loading text (e.g. ``RemoteOptionsBrowser.browse()``, whose own
    ``with DebouncedProgress(...)`` wraps only the fetch, not the later
    message-triggered status write), ``hide()`` must still reset to
    ``base()`` as before."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        status = screen.query_one("#open-status", Static)

        sink = StaticTextSink(screen, "#open-status", base=lambda: "")
        sink.show("⠹")
        sink.hide()
        assert str(status.render()) == ""


async def test_static_text_sink_tolerates_the_widget_already_being_gone() -> None:
    """Regression guard mirroring ``NavigableScreen._update_breadcrumb_text``'s
    own tolerance: a ``DebouncedProgress``'s final ``stop()`` can fire after
    its host screen has already been popped/dismissed."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        sink = StaticTextSink(screen, "#does-not-exist", base=lambda: "")

        sink.show("⠹")  # must not raise NoMatches
        sink.hide()  # must not raise NoMatches


async def test_data_table_loading_row_sink_appends_and_removes_its_own_row_only() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        table = screen.query_one("#col-versions", DataTable)
        # BrowseScreen's own on_mount already populated the table with its
        # own empty-state placeholder row -- cleared here so this test
        # controls the "stale-but-real row already on screen" state itself.
        table.clear()
        table.add_row("2026-01-01 00:00")  # a stale-but-real row already on screen

        sink = DataTableLoadingRowSink(table)
        sink.show("⠹")
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows[0] == "2026-01-01 00:00"
        assert "Loading" in rows[1]

        # A second tick must replace its own row, not append a third one.
        sink.show("⠼")
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows[0] == "2026-01-01 00:00"
        assert len(rows) == 2
        assert "⠼" in rows[1]

        sink.hide()
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["2026-01-01 00:00"]


async def test_data_table_loading_row_sink_pads_blank_cells_on_a_multi_column_table() -> None:
    """``add_row()`` fills a missing trailing cell with ``None``, and
    ``default_cell_formatter(None)`` renders the literal string ``"None"``
    -- harmless for ``BrowseScreen``'s own single-column version table
    (this sink's original call site), but a multi-column table like
    ``UnitScreen``'s own file table would otherwise show
    ``"{frame} Loading" | "None" | "None" | ...`` instead of blank
    cells."""

    class _App(App[None]):
        def compose(self) -> ComposeResult:
            yield DataTable(id="dt")

    app = _App()
    async with app.run_test() as pilot:
        table = app.query_one("#dt", DataTable)
        table.add_column("Name")
        table.add_column("Modified")
        table.add_column("Size")
        await pilot.pause()

        sink = DataTableLoadingRowSink(table)
        sink.show("⠹")
        row = table.get_row_at(0)
        assert "Loading" in str(row[0])
        assert row[1] == ""
        assert row[2] == ""


async def test_data_table_loading_row_sink_hide_tolerates_the_table_already_having_been_cleared() -> None:
    """Regression test: ``DebouncedProgress.stop()`` (-> ``sink.hide()``)
    runs only once the whole worker body -- the fetch *and* its own
    dispatch -- has returned (see ``run_worker_with_progress``'s own
    ``wrapped()``), and a successful/failed fetch's dispatch
    (``VersionsLoaded``/``VersionsLoadFailed``) triggers
    ``BrowseScreen._render_versions()`` synchronously, which clears and
    repopulates the table *before* ``hide()`` ever runs. ``hide()`` must
    not raise ``RowDoesNotExist`` when that's already happened -- this is
    the common case (any fetch slower than the debounce delay that
    actually completes), not an edge one."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        table = screen.query_one("#col-versions", DataTable)
        table.clear()
        table.add_row("stale")

        sink = DataTableLoadingRowSink(table)
        sink.show("⠹")

        # Simulates the real re-render a VersionsLoaded/VersionsLoadFailed
        # dispatch performs before this sink's own hide() ever runs.
        table.clear()
        table.add_row("2026-01-01 00:00")

        sink.hide()  # must not raise RowDoesNotExist
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["2026-01-01 00:00"]


async def test_data_table_loading_row_sink_show_falls_back_to_add_row_if_its_own_row_is_already_gone() -> None:
    """The same tolerance as ``hide()``'s, for ``show()``'s own repeat-tick
    in-place ``update_cell`` -- if something else already removed this
    sink's own row (e.g. an external clear between ticks), a second
    ``show()`` must add a fresh row instead of raising ``RowDoesNotExist``."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        table = screen.query_one("#col-versions", DataTable)
        table.clear()

        sink = DataTableLoadingRowSink(table)
        sink.show("⠹")

        table.clear()  # something else removed this sink's own row

        sink.show("⠼")  # must not raise RowDoesNotExist
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert len(rows) == 1
        assert "⠼" in rows[0]


async def test_data_table_loading_row_sink_show_removes_its_own_row_once_stale_after_an_external_clear() -> None:
    """Regression test: switching to a different, already-loaded folder/
    workload mid-fetch clears the table for the newly-shown content --
    the old fetch's own sink must not re-add a row for it on its next
    tick, the bug this ``is_current`` guard exists to close."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        table = screen.query_one("#col-versions", DataTable)
        table.clear()

        current = True
        sink = DataTableLoadingRowSink(table, is_current=lambda: current)
        sink.show("⠹")
        assert "Loading" in str(table.get_row_at(0)[0])

        # Simulates the real re-render a folder/workload switch performs
        # for whatever's now selected, then the switch itself.
        table.clear()
        table.add_row("2026-01-01 00:00")
        current = False

        sink.show("⠼")  # must not re-add a row for the abandoned fetch
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["2026-01-01 00:00"]


async def test_data_table_loading_row_sink_show_removes_its_own_row_once_stale_with_no_external_clear() -> None:
    """The empty-to-empty case: nothing else ever clears the table for a
    switch between two folders/workloads that are both empty, so a stale
    ``show()`` tick has to remove its own still-present row itself --
    not just skip silently, which would leave it behind indefinitely."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        table = screen.query_one("#col-versions", DataTable)
        table.clear()

        current = True
        sink = DataTableLoadingRowSink(table, is_current=lambda: current)
        sink.show("⠹")
        assert table.row_count == 1

        current = False  # the user has since moved on; nothing cleared the table

        sink.show("⠼")
        assert table.row_count == 0


class _FakeDetailApp(App[None]):
    """Minimal stand-in for ``UnitScreen``'s own ``#detail``/``#detail-scroll``
    pair -- ``DetailPane`` only ever needs ``query_one()`` and
    ``app_state.verbose``, so a full ``UnitScreen`` (with its own
    ``Catalog``/``Version`` construction) isn't needed just to test
    ``DetailPaneLoadingSink``'s node-scoped show/hide."""

    verbose = False

    @property
    def app_state(self) -> _FakeDetailApp:
        return self

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="detail-scroll"):
            yield Static("", id="detail")


async def test_detail_pane_loading_sink_shows_and_hides_below_the_current_header() -> None:
    app = _FakeDetailApp()
    async with app.run_test():
        pane = DetailPane(cast(Any, app))
        node = _node("a.txt")
        pane.show(node)

        sink = DetailPaneLoadingSink(pane, node)
        sink.show("⠹")
        text = str(app.query_one("#detail", Static).render())
        assert "a.txt" in text
        assert "⠹ loading" in text

        sink.hide()
        text = str(app.query_one("#detail", Static).render())
        assert "a.txt" in text
        assert "loading" not in text


async def test_detail_pane_loading_sink_hide_never_clobbers_a_real_preview_written_after_show() -> None:
    """Regression test: ``work()`` wraps ``_load_preview``'s/
    ``_load_list_overview``'s *entire* body, so a real preview
    (``append_preview``) can already be rendered by the time ``hide()``
    runs, if the fetch was slow enough to trigger the sink at all --
    ``clear_loading`` must not wipe it back to a bare header in that case."""
    app = _FakeDetailApp()
    async with app.run_test():
        pane = DetailPane(cast(Any, app))
        node = _node("a.txt")
        pane.show(node)

        sink = DetailPaneLoadingSink(pane, node)
        sink.show("⠹")

        # Simulates _load_preview's own tail, still inside work()'s wrap,
        # rendering the real preview before hide() ever runs.
        pane.append_preview(node, "real preview content")

        sink.hide()
        text = str(app.query_one("#detail", Static).render())
        assert "real preview content" in text
        assert "loading" not in text


async def test_detail_pane_loading_sink_discards_a_stale_tick_after_reselection() -> None:
    """The same staleness guard ``append_preview``/etc already rely on
    (``_render_if_current``) must also cover a loading tick: a sink built
    for a node the user has since navigated away from must not touch
    ``#detail`` at all."""
    app = _FakeDetailApp()
    async with app.run_test():
        pane = DetailPane(cast(Any, app))
        first = _node("a.txt")
        pane.show(first)
        stale_sink = DetailPaneLoadingSink(pane, first)

        second = _node("b.txt")
        pane.show(second)  # user has since selected a different node

        stale_sink.show("⠹")
        text = str(app.query_one("#detail", Static).render())
        assert "b.txt" in text
        assert "loading" not in text
