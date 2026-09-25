"""``FileTableView``: owns ``UnitScreen``'s file ``DataTable`` widget --
column/row rendering, the row -> ``Node`` index, and cursor placement by
ref. Held by ``UnitScreen`` as a private collaborator, reaching back into
it only through the small surface every ``Screen`` already exposes for
this (``file_table``) — the same convention ``GotoChainWalker``/
``DetailPane``/``FolderTreeView`` establish for a ``UnitScreen``
collaborator.
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
    widths right now -- ``add_column``/``add_row`` trigger the same
    recompute, but only via ``_require_update_dimensions`` on the next
    idle; both column-width functions below need the table's own scroll
    region correct before this frame ends, not on some later idle tick.
    The one place either of them reaches into Textual's own private
    ``_update_dimensions``, so a future Textual upgrade that changes its
    signature/semantics only needs a fix here."""
    table._update_dimensions(())  # noqa: SLF001


def _flexible_widths(available: int, weights: Sequence[int], floors: Sequence[int]) -> list[int]:
    """Splits ``available`` cells across ``len(weights)`` columns by
    relative weight, each floored at its own ``floors[i]`` so a column
    never goes to zero or negative in a pathologically narrow terminal.
    Both the running budget (``remaining``) and the weight pool it's
    split by (``remaining_weight``) shrink by each column's own *actual*
    assigned width/weight, not its theoretical share, as each column is
    resolved -- so a column whose floor exceeds its share doesn't
    silently push the total past ``available``: whatever it overruns by
    comes out of what's left for the columns after it, still split among
    them by their own relative weight. The last column absorbs both the
    integer-division remainder and any earlier column's own floor
    overrun, instead of losing either to rounding."""
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
    (positionally aligned with ``table.ordered_columns`` -- both derive
    from the same ``ColumnSpec``) to share whatever width the table's
    fixed-width columns don't already use, by relative weight --
    ``DataTable`` has no native flex-column concept of its own. A no-op
    when ``widths`` holds no ``FlexibleColumnWidth`` at all -- every
    ``ColumnSpec`` in ``select.py`` has at least one today, but a future
    all-fixed/all-``None`` spec would just keep every column auto-width,
    untouched by this function.

    Width is driven by available screen space alone, never by row
    content: each column floors at its own header text's width (just
    enough to avoid a degenerate zero/negative column), not the longest
    value currently on screen. A value wider than its column is left to
    ``DataTable``'s own plain truncation -- no ellipsis, and the full
    value is still available in the detail pane."""
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
    own flexible columns filled the way ``_distribute_flexible_columns``
    describes. ``FileTableView``'s own ``configure_column_widths``/
    ``render`` already keep them filled for their own triggers (a new
    column set, a rebuilt row set); ``on_resize`` covers the one
    remaining trigger neither of those reacts to -- a real terminal
    resize, the only thing that ever changes this widget's own outer
    size (``#browser``'s own Tree/DataTable split is a fixed 30%/1fr
    ratio, ``theme.tcss``, untouched by anything else on this screen).

    ``column_widths`` is the current folder's own ``ColumnSpec.widths``
    (``configure_column_widths`` stashes it here) -- ``on_resize`` needs
    its own copy since it has no reference to ``FileTableView``, the
    collaborator that otherwise owns this state."""

    column_widths: tuple[ColumnWidth, ...] = ()

    def on_resize(self, event: events.Resize) -> None:
        # Width no longer depends on row content, so this no longer
        # touches table.get_column() at all -- a resize tick is now
        # O(number of flexible columns), not O(rows).
        _distribute_flexible_columns(self, self.column_widths)


def _nearest_surviving_ref(old_nodes: list[Node | None], from_index: int, new_refs: set[NodeRef]) -> NodeRef | None:
    """The file table's own counterpart of ``view/reconcile.py``'s
    ``next_cursor_key`` -- forward through ``old_nodes`` from just past
    ``from_index`` first, then backward from it, for the first node
    whose own ref also survives into ``new_refs``. ``None`` when nothing
    in ``old_nodes`` survived at all (a folder switch, say) -- unlike
    ``next_cursor_key``, there's no ancestor chain here to climb looking
    for a still-present node, so the caller just falls back to row 0
    instead. A deliberate duplicate, not shared code: ``next_cursor_key`` operates on a
    ``Diff``'s own before/after key tuples, this on a flat ``list[Node |
    None]`` (the file table's own synthetic error row has no key-shaped
    equivalent to a tree's ``error_leaf_ref``) -- if a future change
    revisits ``next_cursor_key``'s own tie-break/fallback policy, check
    whether this should follow."""
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
        # The file table's own row -> Node index, rebuilt every render()
        # call -- None marks the synthetic error row (mirrors select.py's
        # FileRow.node convention).
        self._nodes: list[Node | None] = []

    def configure_columns(self, headers: tuple[str, ...]) -> None:
        """Rebuilds the file table's own column headers for whichever
        kind of folder is now selected (Mail/Contact/Calendar/File-style/
        a synthetic category grouping, each defined by ``select.py``'s
        ``ColumnSpec``). Only ever called when ``column_headers_for``'s
        return actually changed (``Store.subscribe``'s own comparison),
        so this never runs on an ordinary same-kind folder-to-folder
        navigation. A column change only ever accompanies a real folder
        switch (a different ``Node.ref`` throughout, or the empty-to-empty
        case), so nothing here needs to touch ``_nodes`` itself — the
        very next subscriber in this same notify pass (``render``, when
        it fires -- skipped when two consecutive empty folders share the
        same ``()`` from ``file_table_rows()`` even though their
        ``ColumnSpec`` differs, per ``Store.subscribe``'s own
        unchanged-value skip) rebuilds it fresh. Every column
        starts auto-width here -- ``configure_column_widths``, registered
        right after this in the same ``UnitScreen.on_mount`` subscription
        block, applies the current folder's own ``ColumnSpec.widths``
        next."""
        table = self._screen.file_table
        table.clear(columns=True)
        for header in headers:
            table.add_column(header)

    def configure_column_widths(self, widths: tuple[ColumnWidth, ...]) -> None:
        """Applies ``select.py``'s own per-column width policy
        (``column_widths_for``) to whichever columns ``configure_columns``
        just (re)built -- registered immediately after it, and
        ``Store._notify()`` calls every subscriber in registration
        order, so this always lines up positionally with the *current*
        column set, never a stale one from a prior spec.

        Distributes the flexible columns here too, against whatever's in
        the table right now (zero rows, the common case -- ``render``,
        registered right after this, then redoes it for real once its
        own rows land, which is cheap to repeat since there's nothing to
        scan yet). This call can't be skipped as "render will handle it"
        the way it looks: switching between two folders that are *both*
        empty changes this folder's own ``ColumnSpec`` (a different kind)
        without changing ``file_table_rows()``'s return (``()`` either
        way), so ``render`` doesn't fire at all for that switch (``Store.
        subscribe``'s own unchanged-value skip) -- this call is the only
        one left to size that folder's own flexible columns."""
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
            # A pure append onto what's already on screen -- "+" (load
            # more) against a large, already-rendered folder is the case
            # this matters for: rebuilding every prior row via clear()
            # costs O(total rows) per page instead of O(page size), and
            # clear() unconditionally resets scroll position too, which
            # would otherwise snap the view back to the top on every
            # further page. Cursor position needs no attention here --
            # nothing about any already-rendered row moved.
            #
            # `old_nodes` (not just the length/prefix check) must be
            # non-empty: an empty `old_nodes` prefix-matches *any*
            # `new_nodes`, including a folder's own first-ever page --
            # that page must go through the rebuild branch below instead.
            # Column width itself no longer hinges on this guard either
            # way (configure_column_widths already establishes it against
            # zero rows before any render() call), but it's still a
            # cheap, correct general invariant worth keeping.
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
            # The prior cursor's own row didn't survive (typically a
            # filter keystroke narrowing it out) -- looks for the
            # nearest still-present row by original position (forward
            # first, then backward) instead of snapping to row 0.
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
        """Moves the cursor to ``ref``'s own row and focuses the table,
        when ``ref`` is actually among the currently-rendered rows —
        a silent no-op otherwise (filtered out, or never table-navigable
        to begin with), since the caller shows the target's detail either
        way. Matched by ref, not node equality: the target ``Node`` and
        its own entry here can come from two independent fetches (e.g.
        Drive's own ``resolve_extra()`` vs. an ordinary ``children()``
        page), so only their shared ref is guaranteed to agree, not every
        field."""
        row_index = next((i for i, n in enumerate(self._nodes) if n is not None and n.ref == ref), None)
        if row_index is None:
            return
        table = self._screen.file_table
        table.cursor_coordinate = Coordinate(row_index, 0)
        table.focus()
