"""Pure, widget-free logic for a SharePoint List's spreadsheet-style
overview table: column selection, per-cell truncation, and rendering the
whole thing to plain text. Extracted out of
``browser/screens/unit_screen.py`` because none of it touches Textual
widget state — ``DetailPane.append_list_overview`` is the only caller."""

from __future__ import annotations

import io

from rich.console import Console
from rich.table import Table as RichTable

from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.presentation.markup import safe

#: Per-cell truncation for the List overview table — keeps a wide-schema
#: List's table scannable in a terminal-width detail pane; the full value
#: is still available by selecting that one item individually (its own
#: Field/Value preview isn't cell-width-bounded this way).
OVERVIEW_CELL_MAX_CHARS = 60

#: Per-column overhead (border + one space of padding on each side) Rich
#: adds around a column's own content width — used by
#: ``overview_console_width`` to size the Console comfortably larger than
#: any column could possibly need, so Rich never has a reason to
#: compress/truncate a header to make everything fit.
_OVERVIEW_COLUMN_OVERHEAD = 3


def truncate_cell(value: object) -> str:
    text = "" if value is None else str(value)
    return text if len(text) <= OVERVIEW_CELL_MAX_CHARS else text[: OVERVIEW_CELL_MAX_CHARS - 1] + "…"


def overview_columns(rows: list[dict[str, object]]) -> list[str]:
    # First-seen order across every fetched row, "Title" pinned first
    # when present anywhere — real SharePoint Lists don't share one
    # fixed schema, so the column set itself must come from the data.
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    if "Title" in columns:
        columns.remove("Title")
        columns.insert(0, "Title")
    return columns


def overview_console_width(columns: list[str]) -> int:
    """A cell's own content is already bounded by ``OVERVIEW_CELL_MAX_CHARS``
    (``truncate_cell``), but a column *header* (a real SharePoint field
    name) isn't — so each column's own worst case is whichever of the
    two is actually larger. ``+ 4`` covers the table's own outer border.

    ``columns`` can genuinely be empty even when ``rows`` (the caller's own
    fetched items) isn't -- a real List item's JSON body can itself be
    ``{}``, or ``visible_site_fields`` can filter every one of its keys away
    -- so ``max(..., default=0)`` avoids ``max()``'s own "arg is an empty
    sequence" ``ValueError`` for that real case, rather than crashing this
    preview."""
    per_column = max(((max(OVERVIEW_CELL_MAX_CHARS, len(c)) + _OVERVIEW_COLUMN_OVERHEAD) for c in columns), default=0)
    return len(columns) * per_column + 4


def render_overview_table(header: str, rows: list[dict[str, object]], *, truncated: bool) -> str:
    """Pre-renders ``header`` plus a Rich table of ``rows`` to plain text via
    an in-memory, file-redirected Console — not handed back as a live
    Rich renderable, since a plain ``Console(record=True)`` *also* writes
    straight to the real stdout on every ``.print()``, which would
    corrupt a running Textual app's own terminal control, and every other
    preview ``UnitScreen`` shows is already a plain string.

    Laid out at each column's own natural content width, not the
    pane's visible width: a real SharePoint List row easily has 10+
    visible columns even after ``visible_site_fields`` filtering, and
    handing Rich a Console narrower than that many columns need makes it
    *compress* — truncating column headers, then (worse) the detail
    pane's own line-wrapping hard-wraps the resulting already-fixed-width
    box-drawing rows mid-cell, visibly misaligning the table.
    ``overview_console_width`` instead sizes the Console comfortably larger
    than any column could possibly need, so Rich never compresses
    anything; the caller's own ``wide-preview`` CSS class is what lets the
    detail pane show the result at its real width and pan right instead
    of wrapping it back down.

    ``rows`` must be non-empty — the caller shows a plain "(no items)"
    message itself rather than calling this for that case."""
    columns = overview_columns(rows)
    title = f"showing first {len(rows)} items" if truncated else f"{len(rows)} {pluralize(len(rows), 'item')}"
    table = RichTable(title=title, title_justify="left")
    for column in columns:
        table.add_column(safe(column))
    for row in rows:
        table.add_row(*(safe(truncate_cell(row.get(column))) for column in columns))
    buffer = io.StringIO()
    # force_terminal=False, not Console()'s own "auto" default: an ambient
    # FORCE_COLOR in the launching shell (checked ahead of an isatty()
    # probe -- see Console.is_terminal's own docstring) would otherwise
    # make Rich emit real ANSI escapes into this buffer even though it's
    # never a real terminal, corrupting the plain string this function
    # promises -- the box-drawing rows would carry a different number of
    # embedded escape codes per line depending on each cell's own styling,
    # so a caller measuring "is every row the same width" would see
    # different raw lengths despite identical visible width.
    console = Console(file=buffer, width=overview_console_width(columns), force_terminal=False)
    console.print(header)
    console.print()
    console.print(table)
    return buffer.getvalue()
