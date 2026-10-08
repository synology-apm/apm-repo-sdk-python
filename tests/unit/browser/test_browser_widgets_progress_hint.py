"""Unit tests for ``DebouncedProgress`` and its loading sinks
(``_BreadcrumbSink``, ``TreeNodeLoadingSink``, ``StaticTextSink``,
``DataTableLoadingRowSink``, ``DetailLoadingSink``).

Tests call the private ``_start_animating()`` directly instead of waiting
out the debounce delay."""

from __future__ import annotations

from typing import Any, cast

from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static, Tree

from support.pilot import wait_until
from synology_apm_repo.browser.core.unit.detail import DetailView
from synology_apm_repo.browser.core.unit.model import DetailBody, DetailIdle, DetailLoading, DetailPreview
from synology_apm_repo.browser.screens.detail_pane import DetailLoadingSink, DetailPane
from synology_apm_repo.browser.widgets.progress_hint import (
    DataTableLoadingRowSink,
    DebouncedProgress,
    StaticTextSink,
    TreeNodeLoadingSink,
)
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.browse_screen_fakes import open_browse_screen


def _node(name: str) -> Node:
    return Node(ref=NodeRef("repo", ("root", name)), name=name, is_leaf=True)


async def test_breadcrumb_sink_shows_and_hides_on_the_screens_breadcrumb() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        progress = DebouncedProgress(screen)
        progress._start_animating()
        assert "Loading" in str(screen.query_one("#breadcrumb", Static).render())
        progress.stop()
        assert "Loading" not in str(screen.query_one("#breadcrumb", Static).render())


async def test_tree_node_sink_shows_and_hides_on_the_nodes_own_label_not_the_breadcrumb() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        tree = screen.query_one("#col-catalogs", Tree)
        node = tree.root.add("original-label")

        progress = DebouncedProgress(screen, TreeNodeLoadingSink(node))
        progress._start_animating()
        assert "original-label" in str(node.label)
        assert "Loading" in str(node.label)
        assert "Loading" not in str(screen.query_one("#breadcrumb", Static).render())

        progress.stop()
        assert str(node.label) == "original-label"


async def test_tree_node_sink_preserves_an_external_relabel_that_happens_mid_animation() -> None:
    """A label set externally while the sink animates becomes the new base,
    not overwritten by a stale snapshot."""
    async with open_browse_screen() as (_app, _pilot, screen):
        tree = screen.query_one("#col-catalogs", Tree)
        node = tree.root.add("original-label")

        progress = DebouncedProgress(screen, TreeNodeLoadingSink(node))
        progress._start_animating()
        assert "original-label" in str(node.label)

        node.set_label("relabeled-while-loading")

        progress._tick()
        assert "relabeled-while-loading" in str(node.label)
        assert "original-label" not in str(node.label)

        progress.stop()
        assert str(node.label) == "relabeled-while-loading"


async def test_tree_node_sink_restores_the_label_correctly_even_if_real_children_were_added_meanwhile() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        tree = screen.query_one("#col-catalogs", Tree)
        node = tree.root.add("original-label")

        progress = DebouncedProgress(screen, TreeNodeLoadingSink(node))
        progress._start_animating()
        node.add_leaf("a-real-child")
        progress.stop()

        assert str(node.label) == "original-label"
        assert [str(child.label) for child in node.children] == ["a-real-child"]


async def test_static_text_sink_shows_and_hides_relative_to_a_supplied_base() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
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
    async with open_browse_screen() as (_app, _pilot, screen):
        status = screen.query_one("#open-status", Static)

        sink = StaticTextSink(screen, "#open-status", base=lambda: "")
        sink.show("⠹")
        assert "Loading" in str(status.render())

        status.update("[green]verified[/green]")

        sink.hide()
        assert str(status.render()) == "verified"


async def test_static_text_sink_hide_still_resets_to_base_when_nothing_else_wrote() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        status = screen.query_one("#open-status", Static)

        sink = StaticTextSink(screen, "#open-status", base=lambda: "")
        sink.show("⠹")
        sink.hide()
        assert str(status.render()) == ""


async def test_static_text_sink_tolerates_the_widget_already_being_gone() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        sink = StaticTextSink(screen, "#does-not-exist", base=lambda: "")

        sink.show("⠹")  # must not raise NoMatches
        sink.hide()  # must not raise NoMatches


async def test_data_table_loading_row_sink_appends_and_removes_its_own_row_only() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        table = screen.query_one("#col-versions", DataTable)
        table.clear()  # drop BrowseScreen's empty-state placeholder row
        table.add_row("2026-01-01 00:00")

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

    class _App(App[None]):
        def compose(self) -> ComposeResult:
            yield DataTable(id="dt")

    app = _App()
    async with app.run_test() as pilot:
        table = app.query_one("#dt", DataTable)
        table.add_column("Name")
        table.add_column("Modified")
        table.add_column("Size")
        await wait_until(pilot, lambda: len(table.columns) == 3)

        sink = DataTableLoadingRowSink(table)
        sink.show("⠹")
        row = table.get_row_at(0)
        assert "Loading" in str(row[0])
        assert row[1] == ""
        assert row[2] == ""


async def test_data_table_loading_row_sink_hide_tolerates_the_table_already_having_been_cleared() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        table = screen.query_one("#col-versions", DataTable)
        table.clear()
        table.add_row("stale")

        sink = DataTableLoadingRowSink(table)
        sink.show("⠹")

        table.clear()
        table.add_row("2026-01-01 00:00")

        sink.hide()  # must not raise RowDoesNotExist
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["2026-01-01 00:00"]


async def test_data_table_loading_row_sink_show_falls_back_to_add_row_if_its_own_row_is_already_gone() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
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
    async with open_browse_screen() as (_app, _pilot, screen):
        table = screen.query_one("#col-versions", DataTable)
        table.clear()

        current = True
        sink = DataTableLoadingRowSink(table, is_current=lambda: current)
        sink.show("⠹")
        assert "Loading" in str(table.get_row_at(0)[0])

        table.clear()
        table.add_row("2026-01-01 00:00")
        current = False

        sink.show("⠼")  # must not re-add a row for the abandoned fetch
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["2026-01-01 00:00"]


async def test_data_table_loading_row_sink_show_removes_its_own_row_once_stale_with_no_external_clear() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
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
    """The ``#detail``/``#detail-scroll`` pair ``DetailPane`` queries."""

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="detail-scroll"):
            yield Static("", id="detail")


def _view(node: Node, body: DetailBody | None = None) -> DetailView:
    return DetailView(node=node, body=body if body is not None else DetailLoading(), verbose=False)


async def test_detail_loading_sink_shows_and_hides_below_the_current_header() -> None:
    app = _FakeDetailApp()
    async with app.run_test():
        pane = DetailPane(cast(Any, app))
        pane.render(_view(_node("a.txt")))

        sink = DetailLoadingSink(pane, lambda: True)
        sink.show("⠹")
        text = str(app.query_one("#detail", Static).render())
        assert "a.txt" in text
        assert "⠹ loading" in text

        sink.hide()
        text = str(app.query_one("#detail", Static).render())
        assert "a.txt" in text
        assert "loading" not in text


async def test_detail_loading_sink_hide_restores_the_real_view_rather_than_a_bare_header() -> None:
    app = _FakeDetailApp()
    async with app.run_test():
        pane = DetailPane(cast(Any, app))
        node = _node("a.txt")
        pane.render(_view(node))

        sink = DetailLoadingSink(pane, lambda: True)
        sink.show("⠹")
        pane.render(_view(node, DetailPreview("real preview content")))

        sink.hide()
        text = str(app.query_one("#detail", Static).render())
        assert "real preview content" in text
        assert "loading" not in text


async def test_detail_loading_sink_ignores_a_tick_once_its_fetch_is_no_longer_current() -> None:
    app = _FakeDetailApp()
    async with app.run_test():
        pane = DetailPane(cast(Any, app))
        pane.render(_view(_node("b.txt"), DetailIdle()))

        DetailLoadingSink(pane, lambda: False).show("⠹")

        text = str(app.query_one("#detail", Static).render())
        assert "b.txt" in text
        assert "loading" not in text
