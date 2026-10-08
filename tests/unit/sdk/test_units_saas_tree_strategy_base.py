"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy._base``'s
``_resolve_order_by``, against a small real SQLite table."""

from __future__ import annotations

from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.storage.table import Column, Table
from synology_apm_repo.sdk.units.saas.tree_strategy._base import _resolve_order_by
from unit.sdk.tree_strategy_fakes import tables_db_bytes

_COLUMNS = [Column("item_id"), Column("name"), Column("folder_id", required=False)]


class TestResolveOrderBy:
    """``descending`` must put ``DESC`` on every column: ``ORDER BY a, b
    DESC`` reverses only ``b``."""

    async def _table(self) -> tuple[Table, SqliteSource]:
        rows = [("item-1", "Item One", "folder-a")]
        source = await SqliteSource.from_bytes(
            tables_db_bytes({"items": ("item_id TEXT PRIMARY KEY, name TEXT, folder_id TEXT", rows)})
        )
        table = await Table.create(source.connection, "items", _COLUMNS)
        return table, source

    async def test_ascending_is_unchanged(self) -> None:
        table, source = await self._table()
        try:
            assert _resolve_order_by(table, ["name"]) == "name, rowid"
        finally:
            await source.close()

    async def test_descending_applies_desc_to_every_column_not_just_the_last(self) -> None:
        table, source = await self._table()
        try:
            assert _resolve_order_by(table, ["name"], descending=True) == "name DESC, rowid DESC"
        finally:
            await source.close()

    async def test_a_missing_preferred_column_is_filtered_out_before_desc_is_applied(self) -> None:
        table, source = await self._table()
        try:
            assert _resolve_order_by(table, ["nonexistent", "name"], descending=True) == "name DESC, rowid DESC"
        finally:
            await source.close()

    async def test_dir_first_sql_wraps_the_resolved_clause_in_a_leading_case(self) -> None:
        table, source = await self._table()
        try:
            resolved = _resolve_order_by(table, ["name"], dir_first_sql="is_folder = 1")
            assert resolved == "(CASE WHEN is_folder = 1 THEN 0 ELSE 1 END), name, rowid"
        finally:
            await source.close()
