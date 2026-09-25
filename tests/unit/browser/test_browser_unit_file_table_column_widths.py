"""Unit tests for ``unit_file_table.py``'s column-width mechanism --
``FileTableView.configure_columns``/``configure_column_widths``/``render``,
``FileTable.on_resize``, and the module-private ``_flexible_widths``/
``_distribute_flexible_columns`` they call -- driven directly against a real,
mounted ``FileTable`` widget rather than through the full ``UnitScreen``/
``Store``/provider stack, since none of this logic depends on any of that.
Column width is driven purely by available screen space, never by row
content. Regression coverage for three bugs real code review caught in
this exact mechanism: ``_flexible_widths``'s own running budget/weight
pool not accounting for an earlier column's own floor overrun (an
overrun could otherwise push the total past the table's own available
width -- ``TestFlexibleWidths`` exercises that arithmetic directly, with
no ``App``/``Pilot`` needed), ``render()``'s append-vs-rebuild check
being vacuously true for a folder's own first-ever page (routing it
through the append path, which never establishes a flexible column's
width in the first place), and an empty-to-empty folder-kind switch
never calling ``render()`` at all (leaving the new spec's flexible
columns undistributed since only ``render()`` used to size them)."""

from __future__ import annotations

from typing import Any

import pytest
from textual.app import App, ComposeResult

from synology_apm_repo.browser.core.unit.select import FileRow, FixedColumnWidth, FlexibleColumnWidth
from synology_apm_repo.browser.screens.unit_file_table import FileTable, FileTableView, _flexible_widths


class _FakeScreen:
    """The narrow ``UnitScreen`` surface ``FileTableView`` actually reaches
    through (``self._screen.file_table``)."""

    def __init__(self, table: FileTable) -> None:
        self.file_table = table


class _App(App[None]):
    def compose(self) -> ComposeResult:
        yield FileTable(id="file-table")


@pytest.fixture
async def view_and_table(request: pytest.FixtureRequest) -> Any:
    """A real, mounted ``FileTable`` plus the ``FileTableView`` collaborator
    driving it -- ``size`` (default a generous width) is a fixture param so
    a test can ask for a narrow terminal to force the header-floor path."""
    size = getattr(request, "param", (200, 20))
    app = _App()
    async with app.run_test(size=size) as pilot:
        table = app.query_one("#file-table", FileTable)
        # One pause so the widget's own first layout pass has actually run
        # -- table.scrollable_content_region/cell_padding aren't meaningful
        # before that.
        await pilot.pause()
        yield FileTableView(_FakeScreen(table)), table, pilot  # type: ignore[arg-type]


def _rows(*names: str) -> tuple[FileRow, ...]:
    return tuple(FileRow(node=None, cells=(name, "extra")) for name in names)


class TestFlexibleWidths:
    def test_a_single_column_gets_everything_available(self) -> None:
        assert _flexible_widths(100, [1], [0]) == [100]

    def test_weighted_split_matches_the_declared_ratio(self) -> None:
        assert _flexible_widths(100, [1, 3], [0, 0]) == [25, 75]

    def test_a_floor_that_exceeds_its_own_share_does_not_overflow_the_total(self) -> None:
        """Regression test: a column whose own floor is wider than its
        theoretical weighted share must not silently push the sum of
        every column's own assigned width past ``available`` -- the
        running budget has to account for the overrun, not the
        theoretical share it never actually used."""
        widths = _flexible_widths(100, [1, 1], [80, 0])
        assert widths[0] == 80
        assert sum(widths) <= 100

    def test_every_floor_exceeding_available_still_returns_without_crashing(self) -> None:
        """Degenerate case, not engineered around: a terminal so narrow
        that even every column's own floor together exceeds ``available``
        still returns a plain per-column width, not an error."""
        widths = _flexible_widths(10, [1, 1], [80, 80])
        assert widths == [80, 80]

    def test_a_middle_columns_floor_overrun_comes_out_of_a_later_columns_share_too(self) -> None:
        """Regression test: with three or more flexible columns, an
        earlier (non-last) column's own floor overrun must still reduce
        what a *later*, still-non-last column gets -- not just the final
        column, which absorbs whatever's left regardless. Both the
        budget and the weight pool a later column's own share is
        computed against have to shrink by the earlier column's actual
        assigned width, or the sum silently exceeds ``available``."""
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
        # Name gets everything the fixed Size column (plus its own
        # padding) doesn't use.
        other_render_width = columns["Size"].get_render_width(table)
        assert columns["Name"].get_render_width(table) == table.scrollable_content_region.width - other_render_width

    async def test_weighted_split_matches_the_declared_ratio(self, view_and_table: Any) -> None:
        view, table, _pilot = view_and_table
        view.configure_columns(("Sender", "Subject", "Date"))
        view.configure_column_widths((FlexibleColumnWidth(1), FlexibleColumnWidth(3), FixedColumnWidth(20)))
        view.render(_rows("a", "b"))
        columns = {str(c.label): c for c in table.ordered_columns}
        sender, subject = columns["Sender"].width, columns["Subject"].width
        # Integer division means this is approximate, not exact, to
        # within rounding.
        assert abs(subject - 3 * sender) <= 3

    @pytest.mark.parametrize("view_and_table", [(60, 15)], indirect=True)
    async def test_a_floor_that_exceeds_its_own_share_does_not_overflow_the_table(self, view_and_table: Any) -> None:
        """Regression test: one flexible column's own header text (its
        floor -- each column floors at its own header width so it never
        goes to zero or negative) is wider than
        its theoretical weighted share of a narrow terminal -- the sum of
        every column's own assigned width must still not exceed what the
        table can actually show; the overrun comes out of the sibling
        flexible column's own share instead."""
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
        """Column width is driven purely by available screen space, never
        by row content -- a value far wider than the column stays clipped
        by DataTable's own plain truncation instead of forcing the column
        (and so the whole table) wider than the terminal."""
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
        """Width is only ever driven by available screen space
        (``configure_column_widths``/``on_resize``), never by row content
        -- a page whose own values are wider or narrower than what's
        already on screen must leave an already-distributed flexible
        column's own width exactly as it was, in either direction."""
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
        """Regression test: ``FileTableView._nodes`` starts at ``[]``, so
        the append check's own prefix match (``new_nodes[:0] == []``) is
        vacuously true the first time any folder gets real rows --
        without the ``old_nodes`` truthiness guard, that first page would
        route through the append path instead of the rebuild path. Now a
        plain sanity check rather than proof the guard is width-load-
        bearing: ``configure_column_widths`` already establishes the
        correct share-based width against zero rows before this render()
        call, so a misrouted first page wouldn't actually leave the
        column unsized -- the guard is still worth keeping as a cheap,
        general invariant."""
        view, table, _pilot = view_and_table
        view.configure_columns(("Name", "Size"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10)))
        view.render(_rows("first-item.txt"))
        name_column = next(c for c in table.ordered_columns if str(c.label) == "Name")
        assert name_column.auto_width is False
        other_render_width = next(c for c in table.ordered_columns if str(c.label) == "Size").get_render_width(table)
        assert name_column.get_render_width(table) == table.scrollable_content_region.width - other_render_width

    async def test_switching_between_two_empty_specs_still_distributes_the_new_one(self, view_and_table: Any) -> None:
        """Regression test: ``file_table_rows()`` returns ``()`` for any
        empty folder regardless of kind, so a real ``Store`` never
        re-invokes ``render()`` when switching between two folders that
        are *both* empty -- ``configure_column_widths`` is the only
        subscriber left to size the new spec's own flexible columns for
        that transition."""
        view, table, _pilot = view_and_table
        view.configure_columns(("Name", "Size", "Modified"))
        view.configure_column_widths((FlexibleColumnWidth(), FixedColumnWidth(10), FixedColumnWidth(20)))
        view.render(())

        view.configure_columns(("Sender", "Subject", "Date"))
        view.configure_column_widths((FlexibleColumnWidth(1), FlexibleColumnWidth(3), FixedColumnWidth(20)))
        # Deliberately no view.render(()) call here -- matches file_table_rows()
        # staying () across the switch in the real Store-driven flow.

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
        await pilot.pause()
        narrow_width = next(c for c in table.ordered_columns if str(c.label) == "Name").width
        assert narrow_width < wide_width


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
