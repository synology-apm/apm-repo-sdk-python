"""Shared plumbing for this package's ``TreeStrategy`` implementations:
the protocol, the lazily-created tables (``_LazyTable``,
``_NamedGroupTable``) and the ``ORDER BY``/leaf-listing helpers.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, NamedTuple, Protocol

from ....storage.table import Column, Table
from ....units.provider_kit import dir_first_order_by, paginate

if TYPE_CHECKING:
    import aiosqlite


class SupportsTable(Protocol):
    """The one thing a strategy reads from its workload provider: the open
    connection for a named table (``SaasWorkloadProvider.table``)."""

    def table(self, name: str) -> aiosqlite.Connection: ...


Row = dict[str, object | None]
"""One service-DB table row, by column name."""
Key = tuple[str, ...]
"""A tree node's opaque ref-segment path; ``()`` is the provider root."""


class TreeEntry(NamedTuple):
    """One child ``TreeStrategy.children_of`` lists. ``row`` is the service-DB
    row it was read from: always set for a leaf, set for a folder only when
    the folder is a row of the leaf table itself (``RecursiveTree``,
    ``NamedGroupRecursiveTree``), ``None`` for any other group."""

    key: Key
    name: str
    is_leaf: bool
    row: Row | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class FolderPredicate:
    """A folder-vs-leaf rule in both forms ``RecursiveTree``/
    ``NamedGroupRecursiveTree`` need: ``is_folder(row)`` for a fetched row,
    ``sql`` for folders-first ``ORDER BY``. Callers keep the two in
    agreement."""

    is_folder: Callable[[Row], bool]
    sql: str


def _resolve_order_by(
    table: Table, preferred: Sequence[str], *, descending: bool = False, dir_first_sql: str | None = None
) -> str:
    """An ``ORDER BY`` over the ``preferred`` columns ``table`` has, then
    ``rowid`` as the stable tiebreaker. ``descending`` applies ``DESC`` to
    every column; ``dir_first_sql``, when given, sorts containers
    first."""
    cols = [c for c in preferred if c in table.columns_present]
    cols.append("rowid")
    body = ", ".join(f"{c} DESC" for c in cols) if descending else ", ".join(cols)
    return dir_first_order_by(dir_first_sql, body) if dir_first_sql is not None else body


async def _flat_leaf_entries(
    table: Table,
    *,
    where: str,
    params: tuple[object, ...],
    id_column: str,
    display_name: Callable[[Row], str],
    order_by: Sequence[str],
    descending: bool,
    offset: int,
    limit: int | None,
    key_prefix: Key,
) -> list[TreeEntry]:
    """List one page of a flat leaf table's ``where`` rows as leaf entries
    keyed ``key_prefix + (id,)``."""
    resolved_order_by = _resolve_order_by(table, order_by, descending=descending)
    return [
        TreeEntry((*key_prefix, str(row[id_column])), display_name(row), True, row)
        async for row in table.select(where, params, order_by=resolved_order_by, limit=limit, offset=offset)
    ]


class _LazyTable:
    """A ``Table`` created on first use and cached."""

    def __init__(
        self,
        provider: SupportsTable,
        *,
        table: str,
        columns: list[Column],
        index_hints: list[list[str]] | None = None,
    ) -> None:
        self._provider = provider
        self._table = table
        self._columns = columns
        self._index_hints = index_hints or []
        self._table_obj: Table | None = None

    async def get(self) -> Table:
        if self._table_obj is None:
            self._table_obj = await Table.create(
                self._provider.table(self._table), self._table, self._columns, index_hints=self._index_hints
            )
        return self._table_obj


class TreeStrategy(Protocol):
    """A provider's tree: ``children_of`` returns one page of ``key``'s
    children as ``TreeEntry``s."""

    async def children_of(self, key: Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]: ...


class _NamedGroupTable:
    """The group table of ``NamedGroupFlatTree``/``NamedGroupRecursiveTree``:
    created lazily, listed in full by name and paginated in Python.
    ``name_override`` may replace a row's ``name_column`` value."""

    def __init__(
        self,
        provider: SupportsTable,
        *,
        table: str,
        columns: list[Column],
        id_column: str,
        name_column: str,
        name_override: Callable[[Row], str | None] | None = None,
    ) -> None:
        self._provider = provider
        self._table = table
        self._columns = columns
        self._id_column = id_column
        self._name_column = name_column
        self._name_override = name_override
        self._table_obj: Table | None = None

    async def get(self) -> Table:
        if self._table_obj is None:
            self._table_obj = await Table.create(self._provider.table(self._table), self._table, self._columns)
        return self._table_obj

    def _name_for(self, row: Row) -> str:
        if self._name_override is not None:
            override = self._name_override(row)
            if override is not None:
                return override
        return str(row[self._name_column])

    async def list_top_level(self, *, offset: int, limit: int | None) -> list[TreeEntry]:
        table = await self.get()
        order_by = _resolve_order_by(table, [self._name_column])
        groups = [
            TreeEntry((str(row[self._id_column]),), self._name_for(row), False)
            async for row in table.select(order_by=order_by)
        ]
        return paginate(groups, offset, limit)
