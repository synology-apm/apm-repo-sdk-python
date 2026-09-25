"""Unit tests for ``browser.list_overview``'s SharePoint List overview
column/width logic — pure, widget-free code with
no Textual/screen state involved at all: ``truncate_cell``,
``overview_console_width``, and ``overview_columns``. The rest of the
feature (``UnitScreen._load_list_overview``/``DetailPane.append_list_overview``/
``DetailPane.append_list_overview_error``) is real screen glue (``self._node``,
``self._screen.query_one``, a real Rich ``Console`` render via
``render_overview_table``) covered instead by
``tests/integration/browser/test_browser_pilot_preview.py``'s own List
overview test against real recorded sample data."""

from __future__ import annotations

from synology_apm_repo.browser.list_overview import (
    OVERVIEW_CELL_MAX_CHARS,
    overview_columns,
    overview_console_width,
    truncate_cell,
)


class TestTruncateCell:
    def test_short_value_passes_through_unchanged(self) -> None:
        assert truncate_cell("hello") == "hello"

    def test_none_becomes_empty_string(self) -> None:
        assert truncate_cell(None) == ""

    def test_non_string_value_is_stringified_first(self) -> None:
        assert truncate_cell(42) == "42"
        assert truncate_cell(True) == "True"

    def test_value_at_exactly_the_cap_is_not_truncated(self) -> None:
        value = "x" * OVERVIEW_CELL_MAX_CHARS
        result = truncate_cell(value)
        assert result == value
        assert "…" not in result

    def test_value_one_over_the_cap_is_truncated_with_an_ellipsis(self) -> None:
        value = "x" * (OVERVIEW_CELL_MAX_CHARS + 1)
        result = truncate_cell(value)
        assert len(result) == OVERVIEW_CELL_MAX_CHARS
        assert result.endswith("…")
        assert result == "x" * (OVERVIEW_CELL_MAX_CHARS - 1) + "…"

    def test_a_much_longer_value_is_still_capped_to_the_same_length(self) -> None:
        value = "y" * 1000
        result = truncate_cell(value)
        assert len(result) == OVERVIEW_CELL_MAX_CHARS


class TestOverviewColumns:
    def test_columns_appear_in_first_seen_order_across_rows(self) -> None:
        rows: list[dict[str, object]] = [{"B": 1, "A": 2}, {"C": 3}]
        assert overview_columns(rows) == ["B", "A", "C"]

    def test_a_column_only_present_in_a_later_row_is_still_included(self) -> None:
        rows: list[dict[str, object]] = [{"A": 1}, {"A": 2, "B": 3}, {"A": 4, "C": 5}]
        assert overview_columns(rows) == ["A", "B", "C"]

    def test_title_is_pinned_first_when_present_anywhere(self) -> None:
        rows: list[dict[str, object]] = [{"B": 1, "A": 2}, {"Title": "x", "C": 3}]
        assert overview_columns(rows) == ["Title", "B", "A", "C"]

    def test_no_title_anywhere_leaves_order_unchanged(self) -> None:
        rows: list[dict[str, object]] = [{"B": 1}, {"A": 2}]
        assert overview_columns(rows) == ["B", "A"]

    def test_empty_rows_yields_no_columns(self) -> None:
        assert overview_columns([]) == []

    def test_a_column_is_never_duplicated_across_rows(self) -> None:
        rows: list[dict[str, object]] = [{"A": 1}, {"A": 2}, {"A": 3}]
        assert overview_columns(rows) == ["A"]


class TestOverviewConsoleWidth:
    def test_single_short_column_sizes_to_the_cell_cap_plus_overhead(self) -> None:
        # Cell content (capped at OVERVIEW_CELL_MAX_CHARS) dominates a
        # short header, so width is driven by the cap, not the header.
        width = overview_console_width(["A"])
        assert width == (OVERVIEW_CELL_MAX_CHARS + 3) + 4

    def test_a_header_longer_than_the_cell_cap_drives_the_width_instead(self) -> None:
        long_header = "X" * (OVERVIEW_CELL_MAX_CHARS + 20)
        width = overview_console_width([long_header])
        assert width == (len(long_header) + 3) + 4

    def test_width_scales_with_column_count(self) -> None:
        one = overview_console_width(["A"])
        three = overview_console_width(["A", "B", "C"])
        # Each column contributes the same per-column width once past the
        # shared "+4" outer-border term.
        assert three - 4 == 3 * (one - 4)

    def test_no_columns_is_just_the_outer_border_term(self) -> None:
        """``columns`` can genuinely be empty even when the caller's own
        fetched item list isn't -- a real List item's JSON body can itself
        be ``{}``, or every one of its keys can get filtered away by
        ``visible_site_fields``; this must degrade gracefully rather than
        raising ``ValueError: max() arg is an empty sequence`` (``max()``
        over an empty generator with no ``default=``)."""
        assert overview_console_width([]) == 4


__all__: list[str] = []
