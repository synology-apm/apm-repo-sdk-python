"""Pure, widget-free logic for a SharePoint List's spreadsheet-style
overview table: column selection, per-cell truncation, and rendering the
whole thing to plain text. ``DetailPane`` is the only caller."""

from __future__ import annotations

import io

from rich.console import Console
from rich.table import Table as RichTable

from synology_apm_repo.sdk.presentation import pluralize, safe

#: Per-cell truncation, so a wide-schema List stays scannable; selecting an
#: item shows its full values.
OVERVIEW_CELL_MAX_CHARS = 60

#: Per-column overhead Rich adds around a column's content (border plus one
#: space of padding each side).
_OVERVIEW_COLUMN_OVERHEAD = 3


def truncate_cell(value: object) -> str:
    text = "" if value is None else str(value)
    return text if len(text) <= OVERVIEW_CELL_MAX_CHARS else text[: OVERVIEW_CELL_MAX_CHARS - 1] + "…"


def overview_columns(rows: list[dict[str, object]]) -> list[str]:
    # Lists have no fixed schema: first-seen order across rows, "Title"
    # pinned first.
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
    """A console width no column needs to be compressed in: each column the
    larger of ``OVERVIEW_CELL_MAX_CHARS`` and its header, plus the outer
    border. ``columns`` may be empty."""
    per_column = max(((max(OVERVIEW_CELL_MAX_CHARS, len(c)) + _OVERVIEW_COLUMN_OVERHEAD) for c in columns), default=0)
    return len(columns) * per_column + 4


def render_overview_table(header: str, rows: list[dict[str, object]], *, truncated: bool) -> str:
    """``header`` plus a Rich table of ``rows``, rendered to a plain string
    at ``overview_console_width`` (the detail pane scrolls it horizontally).
    Rendered into an in-memory file, since a ``Console(record=True)`` would
    also write to the running TUI's stdout. ``rows`` must be non-empty."""
    columns = overview_columns(rows)
    title = f"showing first {len(rows)} items" if truncated else f"{len(rows)} {pluralize(len(rows), 'item')}"
    table = RichTable(title=title, title_justify="left")
    for column in columns:
        table.add_column(safe(column))
    for row in rows:
        table.add_row(*(safe(truncate_cell(row.get(column))) for column in columns))
    buffer = io.StringIO()
    # force_terminal=False: an ambient FORCE_COLOR would otherwise put ANSI
    # escapes into the plain string.
    console = Console(file=buffer, width=overview_console_width(columns), force_terminal=False)
    console.print(header)
    console.print()
    console.print(table)
    return buffer.getvalue()
