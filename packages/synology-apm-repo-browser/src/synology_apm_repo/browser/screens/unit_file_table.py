"""``FileTableView``: owns ``UnitScreen``'s file ``DataTable`` widget --
column/row rendering, the row -> ``Node`` index, and cursor placement by
ref. Held by ``UnitScreen`` as a private collaborator, reaching back into
it only through the small surface every ``Screen`` already exposes
(``file_table``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from rich.cells import cell_len
from textual import events
from textual.coordinate import Coordinate
from textual.widgets import DataTable

from synology_apm_repo.browser.core.unit.select import ColumnWidth, FileRow, FixedColumnWidth, FlexibleColumnWidth
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen


def _force_dimension_recompute(table: DataTable[object]) -> None:
    """Forces ``virtual_size`` to recompute from the columns' current
    widths right now, rather than on Textual's next idle tick -- the one
    place this reaches into Textual's private ``_update_dimensions``."""
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
    """Grows every ``FlexibleColumnWidth`` column in ``widths``
    (positionally aligned with ``table.ordered_columns``) to share
    whatever width the table's fixed-width columns don't already use, by
    relative weight -- ``DataTable`` has no native flex-column concept.
    A no-op when ``widths`` holds no ``FlexibleColumnWidth``.

    Each column floors at its own header text's width, not the longest
    value on screen; a value wider than its column is left to
    ``DataTable``'s own plain truncation (full value stays in the detail
    pane)."""
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
    """Plain ``DataTable`` subclass whose only addition is keeping its
    flexible columns filled on a terminal resize -- the one trigger
    ``FileTableView``'s ``configure_column_widths``/``render`` don't
    already cover.

    ``column_widths`` is the current folder's ``ColumnSpec.widths``
    (``configure_column_widths`` stashes it here); ``on_resize`` needs its
    own copy since it has no reference to ``FileTableView``."""

    column_widths: tuple[ColumnWidth, ...] = ()

    def on_resize(self, event: events.Resize) -> None:
        _distribute_flexible_columns(self, self.column_widths)


def _nearest_surviving_ref(old_nodes: list[Node | None], from_index: int, new_refs: set[NodeRef]) -> NodeRef | None:
    """The file table's counterpart of ``view/reconcile.py``'s
    ``next_cursor_key``: searches forward through ``old_nodes`` from just
    past ``from_index``, then backward, for the first node whose ref
    survives into ``new_refs``. ``None`` when nothing survived (a folder
    switch); the caller falls back to row 0. A deliberate duplicate, not
    shared code -- this operates on a flat ``list[Node | None]``, not a
    ``Diff``'s before/after key tuples."""
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
        # The file table's row -> Node index, rebuilt every render() call
        # -- None marks the synthetic error row.
        self._nodes: list[Node | None] = []

    def configure_columns(self, headers: tuple[str, ...]) -> None:
        """Rebuilds the file table's column headers for whichever kind of
        folder is now selected. Only called when ``column_headers_for``'s
        return actually changed. Every column starts auto-width;
        ``configure_column_widths``, registered right after this, applies
        the current folder's ``ColumnSpec.widths`` next."""
        table = self._screen.file_table
        table.clear(columns=True)
        for header in headers:
            table.add_column(header)

    def configure_column_widths(self, widths: tuple[ColumnWidth, ...]) -> None:
        """Applies ``select.py``'s per-column width policy
        (``column_widths_for``) to whichever columns ``configure_columns``
        just (re)built.

        Distributes the flexible columns against whatever's in the table
        right now (zero rows, the common case -- ``render`` redoes it once
        rows land). Needed even when ``render`` won't fire for this
        switch: two consecutive empty folders can still change
        ``ColumnSpec`` without changing ``file_table_rows()``'s return."""
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
            # A pure append onto what's on screen (e.g. "+" load-more) --
            # avoids an O(total rows) clear()+rebuild and the scroll-reset
            # that comes with it. `old_nodes` must be non-empty: an empty
            # one prefix-matches any `new_nodes`, including a folder's
            # first-ever page, which must go through the rebuild branch.
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
            # The prior cursor's row didn't survive (e.g. a filter
            # keystroke narrowing it out) -- find the nearest still-present
            # row instead of snapping to row 0.
            cursor_ref = _nearest_surviving_ref(old_nodes, old_cursor_row, new_refs)
        table.clear()
        self._nodes = new_nodes
        cursor_row = next((i for i, node in enumerate(new_nodes) if node is not None and node.ref == cursor_ref), 0)
        table.add_rows(row.cells for row in rows)
        _distribute_flexible_columns(table, table.column_widths)
        if cursor_row:  # already (0, 0) from clear() above otherwise
            table.cursor_coordinate = Coordinate(cursor_row, 0)

    def node_at(self, row: int) -> Node | None:
        """The ``Node`` currently rendered at ``row``, or ``None`` for an
        out-of-range row (defensive) or the synthetic error row (a real,
        in-range row with no ``Node`` behind it)."""
        return self._nodes[row] if 0 <= row < len(self._nodes) else None

    def locate_and_focus_row(self, ref: NodeRef) -> None:
        """Moves the cursor to ``ref``'s row and focuses the table, when
        ``ref`` is among the currently-rendered rows -- a silent no-op
        otherwise, since the caller shows the target's detail either way.
        Matched by ref, not node equality: the target ``Node`` and its
        entry here can come from two independent fetches, so only the
        shared ref is guaranteed to agree."""
        row_index = next((i for i, n in enumerate(self._nodes) if n is not None and n.ref == ref), None)
        if row_index is None:
            return
        table = self._screen.file_table
        table.cursor_coordinate = Coordinate(row_index, 0)
        table.focus()
