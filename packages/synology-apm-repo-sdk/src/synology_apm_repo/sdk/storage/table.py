"""Schema-tolerant SQLite table access — declare the columns
a query needs and whether each is required, and this introspects the real
table once (``PRAGMA table_info``) to work out which optional columns
this particular connector version's schema actually has. A missing
*optional* column reads back as ``None`` for every row instead of raising
``sqlite3.OperationalError: no such column`` — the failure mode this
project hits repeatedly across every workload DB, not just those two.
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Sequence
from typing import Self

import aiosqlite

from ..errors import DataCorruptError
from .sqlite import apply_index_hint


@dataclasses.dataclass(frozen=True)
class Column:
    """One column a ``Table`` expects — ``required=False`` for a column
    only some connector-version schemas have."""

    name: str
    required: bool = True


class Table:
    """A schema-tolerant view of one real SQLite table.

    Introspects ``PRAGMA table_info(name)`` once at construction time —
    cheap, and the schema can't change under a read-only connection mid
    ``Table`` lifetime. Raises ``DataCorruptError`` immediately if any
    *required* column is absent; missing optional columns are simply
    left out of the ``SELECT`` and backfilled as ``None`` in every row.

    **Constructed via** ``await Table.create(conn, name, columns)``, never
    ``Table(conn, ...)`` directly: introspecting ``PRAGMA table_info`` is
    a database query, and ``__init__`` cannot be ``async def`` — the same
    async-classmethod-factory idiom ``aiosqlite.connect()`` uses.
    """

    #: Populated by ``create``, which is the only supported way to build
    #: one of these — declared here so the type checker sees them without
    #: ``__init__`` having to invent placeholder values it would immediately
    #: overwrite.
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
        """Introspect ``name``'s real schema and return a ``Table`` bound
        to ``conn``.

        ``index_hints``: each entry is a column-name sequence this
        table's callers intend to filter/sort by (e.g.
        ``[["parent_folder_id"]]``, or a composite), declared here
        alongside the schema introspection rather than scattered into
        query logic later. Each is passed straight to
        ``apply_index_hint``, which doesn't need to know or care whether
        ``conn`` allows writing.
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
        """Whether ``name`` exists at all (and has at least one column) —
        a non-raising presence check, for the genuinely different
        situation from a missing *column*: some repository shapes don't have a
        given table at all (e.g. ``file_meta`` may not exist in every
        repository shape). Callers needing that distinction check this
        *before* constructing a ``Table`` — the constructor's own
        "table must exist" contract stays intact for the normal
        "table exists but drifted a column" case."""
        cursor = await conn.execute(f"PRAGMA table_info({name})")
        return bool({row[1] for row in await cursor.fetchall()})

    @property
    def columns_present(self) -> frozenset[str]:
        """The table's *actual* full column set, as introspected —
        independent of which columns this ``Table`` was asked to
        declare. For call sites that must pick a real column name via a
        heuristic rather than a fixed declaration, because the table's
        schema varies across repositories and so there is no fixed name to
        declare."""
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

        ``where``/``order_by`` are literal SQL fragments written by the
        caller (not untrusted input) — parameterize actual *values* via
        ``params``; this only ever interpolates column/table names this
        module itself introspected.

        ``limit``/``offset`` are the pagination primitives: ``limit=None``
        (default) means "no cap" but still lets ``offset`` apply via
        SQLite's own ``LIMIT -1`` idiom, since ``OFFSET`` alone isn't
        valid syntax without some ``LIMIT`` present. Both are
        parameterized (``?``), since they're plain integers from the
        caller, not schema-derived text.
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
        async for row in self.select(where, params):
            return row
        return None


def as_int(value: object) -> int:
    """Narrow one of ``Table.select``'s ``object | None`` values to
    ``int`` — a thin, explicit boundary between a caller's typed
    dataclasses and the untyped ``dict`` rows SQLite hands back, rather
    than scattering ``# type: ignore`` at every call site."""
    assert isinstance(value, int)
    return value


def as_str(value: object) -> str:
    assert isinstance(value, str)
    return value


def sql_placeholders(n: int) -> str:
    """A comma-joined ``n``-item ``"?"`` placeholder list for a batched
    ``WHERE <column> IN (...)`` query — every caller building one of
    these still owns its own "nothing to look up" early return (an empty
    ``IN ()`` is invalid SQL), just not the placeholder string itself."""
    return ",".join("?" * n)
