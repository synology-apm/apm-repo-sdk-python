"""Unit tests for ``synology_apm_repo.sdk.storage.table``."""

from __future__ import annotations

from collections.abc import AsyncIterator

import aiosqlite
import pytest

from synology_apm_repo.sdk.errors import DataCorruptError
from synology_apm_repo.sdk.storage.table import Column, Table, as_int, as_str, sql_placeholders


@pytest.fixture
async def conn() -> AsyncIterator[aiosqlite.Connection]:
    c = await aiosqlite.connect(":memory:")
    await c.execute("CREATE TABLE t(a INTEGER, b TEXT)")
    await c.executemany("INSERT INTO t VALUES (?, ?)", [(1, "x"), (2, "y")])
    await c.commit()
    try:
        yield c
    finally:
        await c.close()


class TestConstruction:
    async def test_all_columns_present(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        assert table.name == "t"

    async def test_missing_optional_column_does_not_raise(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("nope", required=False)])
        assert "nope" not in table.columns_present

    async def test_missing_required_column_raises_data_corrupt(self, conn: aiosqlite.Connection) -> None:
        with pytest.raises(DataCorruptError, match="missing required"):
            await Table.create(conn, "t", [Column("a"), Column("nope")])

    async def test_nonexistent_table_raises_data_corrupt(self, conn: aiosqlite.Connection) -> None:
        with pytest.raises(DataCorruptError, match="does not exist"):
            await Table.create(conn, "no_such_table", [Column("a")])

    async def test_index_hints_are_forwarded_to_apply_index_hint(self, conn: aiosqlite.Connection) -> None:
        await Table.create(conn, "t", [Column("a"), Column("b")], index_hints=[["a"], ["a", "b"]])
        cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 't'")
        index_names = {row[0] for row in await cursor.fetchall()}
        assert index_names == {"_synology_apm_repo_idx_t_a", "_synology_apm_repo_idx_t_a_b"}


class TestSelect:
    async def test_returns_all_rows_as_dicts(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        rows = [row async for row in table.select()]
        assert rows == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    async def test_missing_optional_column_backfilled_as_none(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("missing", required=False)])
        rows = [row async for row in table.select()]
        assert rows == [{"a": 1, "missing": None}, {"a": 2, "missing": None}]

    async def test_where_clause_with_params(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        rows = [row async for row in table.select("a = ?", (2,))]
        assert rows == [{"a": 2, "b": "y"}]

    async def test_select_one_returns_first_match(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        row = await table.select_one("a = ?", (1,))
        assert row == {"a": 1, "b": "x"}

    async def test_select_one_returns_none_when_no_match(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        assert await table.select_one("a = ?", (999,)) is None

    async def test_column_order_in_declaration_does_not_matter_for_correctness(
        self, conn: aiosqlite.Connection
    ) -> None:
        table = await Table.create(conn, "t", [Column("b"), Column("a")])
        rows = [row async for row in table.select()]
        assert rows == [{"b": "x", "a": 1}, {"b": "y", "a": 2}]


class TestSelectPagination:
    async def test_limit_caps_the_number_of_rows(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        rows = [row async for row in table.select(order_by="a", limit=1)]
        assert rows == [{"a": 1, "b": "x"}]

    async def test_offset_without_limit_skips_leading_rows(self, conn: aiosqlite.Connection) -> None:
        # SQLite needs a LIMIT before OFFSET: the ``LIMIT -1 OFFSET ?`` form.
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        rows = [row async for row in table.select(order_by="a", offset=1)]
        assert rows == [{"a": 2, "b": "y"}]

    async def test_limit_and_offset_together(self, conn: aiosqlite.Connection) -> None:
        await conn.execute("INSERT INTO t VALUES (3, 'z')")
        await conn.commit()
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        rows = [row async for row in table.select(order_by="a", limit=1, offset=1)]
        assert rows == [{"a": 2, "b": "y"}]

    async def test_order_by_controls_row_order(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        rows = [row async for row in table.select(order_by="a DESC")]
        assert rows == [{"a": 2, "b": "y"}, {"a": 1, "b": "x"}]

    async def test_no_limit_no_offset_omits_clause_entirely(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a"), Column("b")])
        rows = [row async for row in table.select(order_by="a")]
        assert rows == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]


class TestExistsIn:
    async def test_true_for_a_real_table(self, conn: aiosqlite.Connection) -> None:
        assert await Table.exists_in(conn, "t") is True

    async def test_false_for_a_missing_table(self, conn: aiosqlite.Connection) -> None:
        assert await Table.exists_in(conn, "no_such_table") is False


class TestColumnsPresent:
    async def test_reflects_the_real_schema_regardless_of_what_was_declared(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [Column("a")])
        assert table.columns_present == frozenset({"a", "b"})

    async def test_empty_declaration_still_reports_the_full_real_schema(self, conn: aiosqlite.Connection) -> None:
        table = await Table.create(conn, "t", [])
        assert table.columns_present == frozenset({"a", "b"})


class TestAsInt:
    def test_returns_an_int_value_unchanged(self) -> None:
        assert as_int(5) == 5

    @pytest.mark.parametrize("value", [pytest.param("5", id="non_int_value"), pytest.param(None, id="none")])
    def test_raises_for_a_wrong_type(self, value: object) -> None:
        with pytest.raises(DataCorruptError, match="expected an int column value, got"):
            as_int(value)


class TestAsStr:
    def test_returns_a_str_value_unchanged(self) -> None:
        assert as_str("hello") == "hello"

    @pytest.mark.parametrize("value", [pytest.param(5, id="non_str_value"), pytest.param(None, id="none")])
    def test_raises_for_a_wrong_type(self, value: object) -> None:
        with pytest.raises(DataCorruptError, match="expected a str column value, got"):
            as_str(value)


class TestSqlPlaceholders:
    @pytest.mark.parametrize(
        ("count", "expected"),
        [
            pytest.param(3, "?,?,?", id="generates_one_placeholder_per_item"),
            pytest.param(1, "?", id="single_item"),
            pytest.param(0, "", id="zero_items_yields_an_empty_string"),
        ],
    )
    def test_sql_placeholders(self, count: int, expected: str) -> None:
        assert sql_placeholders(count) == expected
