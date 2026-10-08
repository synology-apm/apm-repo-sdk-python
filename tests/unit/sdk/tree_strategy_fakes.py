"""The provider fake and SQLite builder the SQL-backed
``units.saas.tree_strategy`` unit tests share: every table a test names
lives in one SQLite database, served through ``SupportsTable``, the
only provider surface a strategy uses."""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from support.fakes import faithful_to
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.units.saas.tree_strategy import SupportsTable

TableSpec = tuple[str, Sequence[tuple[object, ...]]]
"""A table's ``CREATE TABLE`` column definitions and its rows."""


def tables_db_bytes(tables: dict[str, TableSpec]) -> bytes:
    """A SQLite file holding each ``{name: (column definitions, rows)}`` table."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "tables.db"
        conn = sqlite3.connect(path)
        for name, (columns, rows) in tables.items():
            conn.execute(f"CREATE TABLE {name}({columns})")
            for row in rows:
                conn.execute(f"INSERT INTO {name} VALUES ({', '.join('?' * len(row))})", row)
        conn.commit()
        conn.close()
        return path.read_bytes()


@faithful_to(SupportsTable)
class FakeTableProvider:
    """Serves every table name from one ``SqliteSource``; ``requested``
    records each name asked for, in order."""

    def __init__(self, source: SqliteSource) -> None:
        self._source = source
        self.requested: list[str] = []

    def table(self, name: str) -> aiosqlite.Connection:
        self.requested.append(name)
        return self._source.connection


@asynccontextmanager
async def table_provider(tables: dict[str, TableSpec]) -> AsyncIterator[FakeTableProvider]:
    """A ``FakeTableProvider`` over ``tables``; its source is closed on exit."""
    source = await SqliteSource.from_bytes(tables_db_bytes(tables))
    try:
        yield FakeTableProvider(source)
    finally:
        await source.close()
