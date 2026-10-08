"""Schema-tolerant SQLite table access: declare the columns a query needs and
whether each is required, and ``Table.create`` introspects the real table
(``PRAGMA table_info``) once. A missing *optional* column reads back as
``None`` for every row instead of raising ``no such column``, which differs
across connector versions.
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Sequence
from typing import Self

import aiosqlite

from ..errors import DataCorruptError
from .sqlite import apply_index_hint


@dataclasses.dataclass(frozen=True, slots=True)
class Column:
    """One column a ``Table`` expects; ``required=False`` for a column only
    some connector-version schemas have."""

    name: str
    required: bool = True


class Table:
    """A schema-tolerant view of one real SQLite table.

    Introspects ``PRAGMA table_info(name)`` once, in ``create``. A missing
    *required* column raises ``DataCorruptError``; a missing optional one is
    left out of the ``SELECT`` and backfilled as ``None`` in every row.

    Build with ``await Table.create(conn, name, columns)``, not
    ``Table(...)``: introspection is a query and ``__init__`` can't be async.

    Attributes:
        name: The table name.
        columns: The declared ``Column``\\ s.
    """

    #: Populated by ``create``, declared here for the type checker.
    _present_columns: list[str]
    _all_columns: frozenset[str]
    _conn: aiosqlite.Connection

    def __init__(self, name: str, columns: Sequence[Column]) -> None:
        self.name = name
        self.columns = tuple(columns)

    @classmethod
    async def create(
        cls,
        conn: aiosqlite.Connection,
        name: str,
        columns: Sequence[Column],
        *,
        index_hints: Sequence[Sequence[str]] = (),
    ) -> Self:
        """Introspect ``name``'s real schema and return a ``Table`` bound to
        ``conn``.

        Args:
            conn: Connection to query through.
            name: Table name.
            columns: The columns callers need.
            index_hints: Column sequences callers will filter or sort by
                (e.g. ``[["parent_folder_id"]]``), each passed to
                ``apply_index_hint``, which copes with a read-only ``conn``.

        Raises:
            DataCorruptError: The table is missing or lacks a required column.
        """
        self = cls(name, columns)
        cursor = await conn.execute(f"PRAGMA table_info({name})")
        present = {row[1] for row in await cursor.fetchall()}
        if not present:
            raise DataCorruptError(f"table {name!r} does not exist (or has no columns)", ref=name)
        missing_required = [c.name for c in self.columns if c.required and c.name not in present]
        if missing_required:
            raise DataCorruptError(f"table {name!r} is missing required column(s) {missing_required}", ref=name)
        self._present_columns = [c.name for c in self.columns if c.name in present]
        self._all_columns = frozenset(present)
        self._conn = conn
        for hint in index_hints:
            await apply_index_hint(conn, name, hint)
        return self

    @staticmethod
    async def exists_in(conn: aiosqlite.Connection, name: str) -> bool:
        """Whether ``name`` exists with at least one column. Call before
        ``create`` for a table some repository shapes lack entirely (e.g.
        ``file_meta``); ``create`` raises for a missing table."""
        cursor = await conn.execute(f"PRAGMA table_info({name})")
        return bool({row[1] for row in await cursor.fetchall()})

    @property
    def columns_present(self) -> frozenset[str]:
        """The table's actual column set, independent of the declared columns,
        for callers that pick a column by heuristic because its name varies
        across repositories."""
        return self._all_columns

    async def select(
        self,
        where: str = "",
        params: Sequence[object] = (),
        *,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> AsyncIterator[dict[str, object | None]]:
        """Run ``SELECT <present columns> FROM <name> [WHERE <where>]
        [ORDER BY <order_by>] [LIMIT ? OFFSET ?]`` and yield each row as
        ``{column_name: value}``, with every declared-but-absent optional
        column backfilled as ``None``.

        Args:
            where: Literal SQL written by the caller, never untrusted input;
                pass values through ``params``.
            params: Values for the ``?`` placeholders in ``where``.
            order_by: Literal SQL ``ORDER BY`` fragment.
            limit: Row cap; ``None`` is uncapped but still honors ``offset``
                (via ``LIMIT -1``).
            offset: Rows to skip.
        """
        select_list = ", ".join(self._present_columns)
        query = f"SELECT {select_list} FROM {self.name}"
        query_params: list[object] = list(params)
        if where:
            query += f" WHERE {where}"
        if order_by is not None:
            query += f" ORDER BY {order_by}"
        if limit is not None or offset:
            query += " LIMIT ? OFFSET ?"
            query_params.extend((limit if limit is not None else -1, offset))
        cursor = await self._conn.execute(query, query_params)
        async for row in cursor:
            record: dict[str, object | None] = dict(zip(self._present_columns, row, strict=True))
            for col in self.columns:
                if col.name not in record:
                    record[col.name] = None
            yield record

    async def select_one(self, where: str = "", params: Sequence[object] = ()) -> dict[str, object | None] | None:
        """The first row ``select(where, params)`` yields, or ``None``."""
        async for row in self.select(where, params):
            return row
        return None


def as_int(value: object) -> int:
    """Narrow one of ``Table.select``'s ``object | None`` values to ``int``.

    Raises:
        DataCorruptError: ``value`` is not an ``int``.
    """
    if not isinstance(value, int):
        raise DataCorruptError(f"expected an int column value, got {value!r}")
    return value


def as_str(value: object) -> str:
    """Narrow one of ``Table.select``'s values to ``str``.

    Raises:
        DataCorruptError: ``value`` is not a ``str``.
    """
    if not isinstance(value, str):
        raise DataCorruptError(f"expected a str column value, got {value!r}")
    return value


def sql_placeholders(n: int) -> str:
    """A comma-joined list of ``n`` ``"?"`` placeholders for ``IN (...)``.
    ``n`` must be positive: an empty ``IN ()`` is invalid SQL."""
    return ",".join("?" * n)
