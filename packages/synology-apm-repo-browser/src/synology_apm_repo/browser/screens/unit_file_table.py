"""``FileTableView``: ``UnitScreen``'s file ``DataTable`` -- column and row
rendering, the row -> ``Node`` index, and cursor placement by ref. It
reaches the screen only through ``file_table``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from rich.cells import cell_len
from textual import events
from textual.coordinate import Coordinate
from textual.widgets import DataTable

from synology_apm_repo.browser.core.unit.select import ColumnWidth, FileRow, FixedColumnWidth, FlexibleColumnWidth
from synology_apm_repo.sdk import Node, NodeRef

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen


def _force_dimension_recompute(table: DataTable[object]) -> None:
    """Recomputes ``virtual_size`` from the columns' widths now, not on
    Textual's next idle tick (a private Textual call)."""
    table._update_dimensions(())  # noqa: SLF001


def _flexible_widths(available: int, weights: Sequence[int], floors: Sequence[int]) -> list[int]:
    """Splits ``available`` cells across ``len(weights)`` columns by
    relative weight, each floored at its own ``floors[i]``. A column
    whose floor exceeds its share has the overrun deducted from what's
    left for later columns; the last column absorbs any remainder."""
    remaining = available
    remaining_weight = sum(weights)
    widths: list[int] = []
    for index, weight in enumerate(weights):
        share = remaining if index == len(weights) - 1 else remaining * weight // remaining_weight
        width = max(share, floors[index])
        widths.append(width)
        remaining -= width
        remaining_weight -= weight
    return widths


def _distribute_flexible_columns(table: DataTable[object], widths: tuple[ColumnWidth, ...]) -> None:
    """Sizes every ``FlexibleColumnWidth`` column (``widths`` aligned with
    ``table.ordered_columns``) to share, by weight, the width the other
    columns leave; ``DataTable`` has no flex columns. Each is floored at
    its header's width; a longer value is truncated."""
    flexible = [
        (column, width)
        for column, width in zip(table.ordered_columns, widths, strict=True)
        if isinstance(width, FlexibleColumnWidth)
    ]
    if not flexible:
        return
    flexible_ids = {id(column) for column, _ in flexible}
    fixed_render_width = sum(
        column.get_render_width(table) for column in table.ordered_columns if id(column) not in flexible_ids
    )
    raw_available = table.scrollable_content_region.width - fixed_render_width - 2 * table.cell_padding * len(flexible)
    flexible_widths = _flexible_widths(
        raw_available,
        [width.weight for _, width in flexible],
        [cell_len(str(column.label)) for column, _ in flexible],
    )
    for (column, _), width in zip(flexible, flexible_widths, strict=True):
        column.auto_width = False
        column.width = width
    _force_dimension_recompute(table)


class FileTable(DataTable[object]):
    """A ``DataTable`` that re-sizes its flexible columns on resize.
    ``column_widths`` is the current folder's width policy, set by
    ``FileTableView.configure_column_widths``."""

    column_widths: tuple[ColumnWidth, ...] = ()

    def on_resize(self, event: events.Resize) -> None:
        _distribute_flexible_columns(self, self.column_widths)


def _nearest_surviving_ref(old_nodes: list[Node | None], from_index: int, new_refs: set[NodeRef]) -> NodeRef | None:
    """The ref of the nearest node after ``from_index``, then before it,
    whose ref is in ``new_refs``; ``None`` if none survived."""
    for node in old_nodes[from_index + 1 :]:
        if node is not None and node.ref in new_refs:
            return node.ref
    for node in reversed(old_nodes[:from_index]):
        if node is not None and node.ref in new_refs:
            return node.ref
    return None


class FileTableView:
    def __init__(self, screen: UnitScreen) -> None:
        self._screen = screen
        # Row -> Node; None marks the error row.
        self._nodes: list[Node | None] = []

    def configure_columns(self, headers: tuple[str, ...]) -> None:
        """Rebuilds the columns, all auto-width until
        ``configure_column_widths`` (subscribed next) runs."""
        table = self._screen.file_table
        table.clear(columns=True)
        for header in headers:
            table.add_column(header)

    def configure_column_widths(self, widths: tuple[ColumnWidth, ...]) -> None:
        """Applies the width policy (``column_widths_for``). Flexible
        columns are distributed here too, since two empty folders can differ
        in columns without ``render`` firing."""
        table = self._screen.file_table
        for column, width in zip(table.ordered_columns, widths, strict=True):
            if isinstance(width, FixedColumnWidth):
                column.auto_width = False
                column.width = width.cells
        table.column_widths = widths
        _distribute_flexible_columns(table, widths)

    def render(self, rows: tuple[FileRow, ...]) -> None:
        new_nodes = [row.node for row in rows]
        old_nodes = self._nodes
        table = self._screen.file_table
        if old_nodes and len(new_nodes) > len(old_nodes) and new_nodes[: len(old_nodes)] == old_nodes:
            # A pure append (load-more): no clear() and scroll reset. An
            # empty `old_nodes` would match any first page, hence the check.
            self._nodes = new_nodes
            new_rows = rows[len(old_nodes) :]
            table.add_rows(row.cells for row in new_rows)
            return
        old_cursor_row = table.cursor_row
        cursor_ref: NodeRef | None = None
        if 0 <= old_cursor_row < len(old_nodes):
            prior = old_nodes[old_cursor_row]
            cursor_ref = prior.ref if prior is not None else None
        new_refs = {node.ref for node in new_nodes if node is not None}
        if cursor_ref is not None and cursor_ref not in new_refs:
            # The cursor's row is gone (e.g. filtered out): nearest survivor.
            cursor_ref = _nearest_surviving_ref(old_nodes, old_cursor_row, new_refs)
        table.clear()
        self._nodes = new_nodes
        cursor_row = next((i for i, node in enumerate(new_nodes) if node is not None and node.ref == cursor_ref), 0)
        table.add_rows(row.cells for row in rows)
        _distribute_flexible_columns(table, table.column_widths)
        if cursor_row:  # already (0, 0) from clear() above otherwise
            table.cursor_coordinate = Coordinate(cursor_row, 0)

    def node_at(self, row: int) -> Node | None:
        """The ``Node`` at ``row``; ``None`` out of range or for the error
        row."""
        return self._nodes[row] if 0 <= row < len(self._nodes) else None

    def locate_and_focus_row(self, ref: NodeRef) -> None:
        """Moves the cursor to ``ref``'s row and focuses the table; a no-op
        if the row isn't rendered. Matched by ref: the row's ``Node`` may
        come from a different fetch than the caller's."""
        row_index = next((i for i, n in enumerate(self._nodes) if n is not None and n.ref == ref), None)
        if row_index is None:
            return
        table = self._screen.file_table
        table.cursor_coordinate = Coordinate(row_index, 0)
        table.focus()
