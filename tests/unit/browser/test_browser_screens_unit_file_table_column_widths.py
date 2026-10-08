"""Unit tests for ``unit_file_table.py``'s column widths
(``FileTableView``, ``FileTable.on_resize``, ``_flexible_widths``) against a
real, mounted ``FileTable``. Column width is driven purely by available
screen space, never by row content."""

from __future__ import annotations

from typing import Any

import pytest
from textual.app import App, ComposeResult

from support.pilot import wait_until
from synology_apm_repo.browser.core.unit.select import FileRow, FixedColumnWidth, FlexibleColumnWidth
from synology_apm_repo.browser.screens.unit_file_table import FileTable, FileTableView, _flexible_widths


class _FakeScreen:
    """The ``UnitScreen`` surface ``FileTableView`` uses: ``file_table``."""

    def __init__(self, table: FileTable) -> None:
        self.file_table = table


class _App(App[None]):
    def compose(self) -> ComposeResult:
        yield FileTable(id="file-table")


@pytest.fixture
async def view_and_table(request: pytest.FixtureRequest) -> Any:
    """A mounted ``FileTable`` and its ``FileTableView``; an indirect param
    overrides the terminal ``size`` (default 200x20)."""
    size = getattr(request, "param", (200, 20))
    app = _App()
    async with app.run_test(size=size) as pilot:
        table = app.query_one("#file-table", FileTable)
        # scrollable_content_region is meaningful only after the first layout.
        await wait_until(pilot, lambda: table.scrollable_content_region.width > 0)
        yield FileTableView(_FakeScreen(table)), table, pilot  # type: ignore[arg-type]


def _rows(*names: str) -> tuple[FileRow, ...]:
    return tuple(FileRow(node=None, cells=(name, "extra")) for name in names)


class TestFlexibleWidths:
    def test_a_single_column_gets_everything_available(self) -> None:
        assert _flexible_widths(100, [1], [0]) == [100]

    def test_weighted_split_matches_the_declared_ratio(self) -> None:
        assert _flexible_widths(100, [1, 3], [0, 0]) == [25, 75]

    def test_a_floor_that_exceeds_its_own_share_does_not_overflow_the_total(self) -> None:
        widths = _flexible_widths(100, [1, 1], [80, 0])
        assert widths[0] == 80
        assert sum(widths) <= 100

    def test_every_floor_exceeding_available_still_returns_without_crashing(self) -> None:
        widths = _flexible_widths(10, [1, 1], [80, 80])
        assert widths == [80, 80]

    def test_a_middle_columns_floor_overrun_comes_out_of_a_later_columns_share_too(self) -> None:
        """An earlier column's floor overrun shrinks a later non-last
        column's share too, not just the last column's remainder."""
        widths = _flexible_widths(100, [1, 1, 1], [80, 0, 0])
        assert widths[0] == 80
        assert sum(widths) <= 100


class TestDistributeFlexibleColumns:
    async def test_single_flexible_column_fills_all_remaining_width(self, view_and_table: Any) -> None:
        view, table, _pilot = view_and_table
        view.configure_columns(("Name", "Size"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10)))
        columns = {str(c.label): c for c in table.ordered_columns}
        assert columns["Name"].auto_width is False
        assert columns["Size"].width == 10
        other_render_width = columns["Size"].get_render_width(table)
        assert columns["Name"].get_render_width(table) == table.scrollable_content_region.width - other_render_width

    async def test_weighted_split_matches_the_declared_ratio(self, view_and_table: Any) -> None:
        view, table, _pilot = view_and_table
        view.configure_columns(("Sender", "Subject", "Date"))
        view.configure_column_widths((FlexibleColumnWidth(1), FlexibleColumnWidth(3), FixedColumnWidth(20)))
        view.render(_rows("a", "b"))
        columns = {str(c.label): c for c in table.ordered_columns}
        sender, subject = columns["Sender"].width, columns["Subject"].width
        # Approximate: integer division.
        assert abs(subject - 3 * sender) <= 3

    @pytest.mark.parametrize("view_and_table", [(60, 15)], indirect=True)
    async def test_a_floor_that_exceeds_its_own_share_does_not_overflow_the_table(self, view_and_table: Any) -> None:
        """A column floors at its header width; when that exceeds its share,
        the overrun comes out of the sibling flexible column."""
        view, table, _pilot = view_and_table
        view.configure_columns(("A Longish Sender Header", "Subject", "Date"))
        view.configure_column_widths((FlexibleColumnWidth(1), FlexibleColumnWidth(3), FixedColumnWidth(10)))
        columns = table.ordered_columns
        total_render_width = sum(c.get_render_width(table) for c in columns)
        assert total_render_width <= table.scrollable_content_region.width

    @pytest.mark.parametrize("view_and_table", [(40, 15)], indirect=True)
    async def test_a_long_value_in_a_narrow_terminal_does_not_grow_the_column_past_its_share(
        self, view_and_table: Any
    ) -> None:
        view, table, _pilot = view_and_table
        view.configure_columns(("Name", "Size"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10)))
        share_before = next(c for c in table.ordered_columns if str(c.label) == "Name").width
        long_name = "a-name-much-longer-than-this-narrow-terminal-can-comfortably-show.docx"
        view.render(_rows(long_name))
        name_column = next(c for c in table.ordered_columns if str(c.label) == "Name")
        assert name_column.width == share_before
        assert name_column.width < len(long_name)

    async def test_no_flexible_column_leaves_every_column_untouched(self, view_and_table: Any) -> None:
        view, table, _pilot = view_and_table
        view.configure_columns(("Full Name", "Email"))
        view.configure_column_widths((FixedColumnWidth(10), FixedColumnWidth(10)))
        columns = table.ordered_columns
        assert all(c.auto_width is False and c.width == 10 for c in columns)


class TestAppendPath:
    async def test_appending_a_page_never_changes_a_flexible_columns_width(self, view_and_table: Any) -> None:
        view, table, _pilot = view_and_table
        view.configure_columns(("Name", "Size"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10)))
        page1 = _rows("a-fairly-long-filename-already-on-screen.docx")
        view.render(page1)
        before = next(c for c in table.ordered_columns if str(c.label) == "Name").width

        wider_page = page1 + _rows("a-much-longer-filename-than-anything-seen-so-far.docx")
        view.render(wider_page)
        after_wider = next(c for c in table.ordered_columns if str(c.label) == "Name").width
        assert after_wider == before

        narrower_page = wider_page + _rows("x.txt")
        view.render(narrower_page)
        after_narrower = next(c for c in table.ordered_columns if str(c.label) == "Name").width
        assert after_narrower == before


class TestFirstPopulationAndEmptyToEmptySwitch:
    async def test_a_folders_first_ever_page_is_correctly_distributed_not_just_appended(
        self, view_and_table: Any
    ) -> None:
        """An empty ``_nodes`` prefix-matches any first page, which must still
        take the rebuild path, not the append path."""
        view, table, _pilot = view_and_table
        view.configure_columns(("Name", "Size"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10)))
        view.render(_rows("first-item.txt"))
        name_column = next(c for c in table.ordered_columns if str(c.label) == "Name")
        assert name_column.auto_width is False
        other_render_width = next(c for c in table.ordered_columns if str(c.label) == "Size").get_render_width(table)
        assert name_column.get_render_width(table) == table.scrollable_content_region.width - other_render_width

    async def test_switching_between_two_empty_specs_still_distributes_the_new_one(self, view_and_table: Any) -> None:
        """``file_table_rows()`` stays ``()`` across two empty folders, so the
        ``Store`` never re-invokes ``render()``: ``configure_column_widths``
        alone must size the new spec."""
        view, table, _pilot = view_and_table
        view.configure_columns(("Name", "Size", "Modified"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10), FixedColumnWidth(20)))
        view.render(())

        view.configure_columns(("Sender", "Subject", "Date"))
        view.configure_column_widths((FlexibleColumnWidth(1), FlexibleColumnWidth(3), FixedColumnWidth(20)))

        columns = {str(c.label): c for c in table.ordered_columns}
        assert columns["Sender"].auto_width is False
        assert columns["Subject"].auto_width is False
        assert columns["Subject"].width > columns["Sender"].width


class TestOnResize:
    async def test_resize_redistributes_flexible_columns_to_the_new_size(self, view_and_table: Any) -> None:
        view, table, pilot = view_and_table
        view.configure_columns(("Name", "Size"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10)))
        view.render(_rows("x.txt"))
        wide_width = next(c for c in table.ordered_columns if str(c.label) == "Name").width

        await pilot.resize_terminal(60, 20)
        await wait_until(
            pilot, lambda: next(c for c in table.ordered_columns if str(c.label) == "Name").width < wide_width
        )


class TestTotalRenderWidthInvariant:
    @pytest.mark.parametrize("view_and_table", [(40, 15)], indirect=True)
    @pytest.mark.parametrize(
        "name", ["x.txt", "a-name-much-longer-than-this-narrow-terminal-can-comfortably-show.docx"]
    )
    async def test_total_render_width_never_exceeds_the_scrollable_region(self, view_and_table: Any, name: str) -> None:
        view, table, _pilot = view_and_table
        view.configure_columns(("Name", "Size"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10)))
        view.render(_rows(name))
        total_render_width = sum(c.get_render_width(table) for c in table.ordered_columns)
        assert total_render_width <= table.scrollable_content_region.width
