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
    """Each column's width is the larger of ``OVERVIEW_CELL_MAX_CHARS`` and
    its header length; ``+ 4`` covers the table's outer border. ``columns``
    can be empty (a real List item's JSON body can be ``{}``), so
    ``max(..., default=0)`` avoids ``max()``'s empty-sequence ``ValueError``."""
    per_column = max(((max(OVERVIEW_CELL_MAX_CHARS, len(c)) + _OVERVIEW_COLUMN_OVERHEAD) for c in columns), default=0)
    return len(columns) * per_column + 4


def render_overview_table(header: str, rows: list[dict[str, object]], *, truncated: bool) -> str:
    """Pre-renders ``header`` plus a Rich table of ``rows`` to a plain string
    via an in-memory, file-redirected Console — a plain ``Console(record=True)``
    would also write to the real stdout, corrupting the running TUI.

    Sized to each column's natural width via ``overview_console_width``,
    wide enough that Rich never compresses/truncates headers; the caller's
    ``wide-preview`` CSS class lets the detail pane pan right instead of
    wrapping the result back down.

    ``rows`` must be non-empty — the caller shows "(no items)" itself."""
    columns = overview_columns(rows)
    title = f"showing first {len(rows)} items" if truncated else f"{len(rows)} {pluralize(len(rows), 'item')}"
    table = RichTable(title=title, title_justify="left")
    for column in columns:
        table.add_column(safe(column))
    for row in rows:
        table.add_row(*(safe(truncate_cell(row.get(column))) for column in columns))
    buffer = io.StringIO()
    # force_terminal=False, not Console's "auto" default: an ambient
    # FORCE_COLOR would otherwise make Rich emit real ANSI escapes into
    # this buffer, corrupting the plain string this function promises.
    console = Console(file=buffer, width=overview_console_width(columns), force_terminal=False)
    console.print(header)
    console.print()
    console.print(table)
    return buffer.getvalue()
