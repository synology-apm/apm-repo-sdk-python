"""Shared plumbing every concrete ``TreeStrategy`` implementation in this
package builds on: the ``TreeStrategy`` protocol itself, the
lazy-create-and-cache table shapes (``_LazyTable``, and the outer
"groups are rows of their own table" half, ``_NamedGroupTable``), and the
``ORDER BY``/leaf-listing helpers every implementation but
``CategorizedGroupTree`` needs. Six concrete strategies build on this —
``SyntheticGroupedTree``, ``NamedGroupFlatTree``, ``RecursiveTree``,
``NamedGroupRecursiveTree``, ``RecursiveGroupFlatTree``, and
``CategorizedGroupTree`` — covering the two shapes every service-level
DB in this project's real schemas expands into (parent-pointer
recursion, or a flat list optionally grouped by one key), plus
``CategorizedGroupTree``'s further synthetic split on top of either.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Protocol

from ....storage.table import Column, Table
from ...base import dir_first_order_by, paginate

if TYPE_CHECKING:
    from ..provider import SaasWorkloadProvider

_Row = dict[str, object | None]
_Key = tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class FolderPredicate:
    """A directory-vs-leaf rule, stated once in both forms
    ``RecursiveTree``/``NamedGroupRecursiveTree`` need: ``is_folder(row)``
    classifies an already-fetched row (used for ``Node.is_leaf``); ``sql``
    is the equivalent SQL fragment pushed into ``ORDER BY`` for dir-first
    paging (see ``dir_first_order_by``). Callers must keep the two forms
    agreeing — there is no way to derive one from the other generically."""

    is_folder: Callable[[_Row], bool]
    sql: str


def _resolve_order_by(
    table: Table, preferred: Sequence[str], *, descending: bool = False, dir_first_sql: str | None = None
) -> str:
    """Builds an ``ORDER BY`` expression from ``preferred`` column
    names, filtered down to the table's actual columns present (the
    schema-drift tolerance ``storage/table.py`` describes, traps
    #19/#20) — always with SQLite's implicit ``rowid`` appended last,
    unconditionally, as the one sort key present and stable across
    every schema version. ``descending`` applies ``DESC`` to every
    column individually (``ORDER BY a, b DESC`` in SQL only reverses
    the last column, not the whole clause), rather than appending one
    trailing suffix. ``dir_first_sql``, when given, wraps the result
    with ``dir_first_order_by`` so containers sort before leaves."""
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
    display_name: Callable[[_Row], str],
    order_by: Sequence[str],
    descending: bool,
    offset: int,
    limit: int | None,
    key_prefix: _Key,
    rows: dict[_Key, _Row],
) -> list[tuple[_Key, str, bool]]:
    """Shared leaf-listing loop for a flat (non-recursive) leaf table,
    keyed by appending each row's own id to ``key_prefix`` —
    ``SyntheticGroupedTree``, ``NamedGroupFlatTree``, and
    ``RecursiveGroupFlatTree`` all query, cache, and emit leaf entries this
    same way; only what the ``WHERE``/``key_prefix`` are built from differs
    between them. ``RecursiveTree``/``NamedGroupRecursiveTree`` don't use
    this: their own rows can also be folders (``is_folder(row)``), which
    every caller of this helper's own leaf table never has."""
    resolved_order_by = _resolve_order_by(table, order_by, descending=descending)
    out: list[tuple[_Key, str, bool]] = []
    async for row in table.select(where, params, order_by=resolved_order_by, limit=limit, offset=offset):
        leaf_key = key_prefix + (str(row[id_column]),)
        rows[leaf_key] = row
        out.append((leaf_key, display_name(row), True))
    return out


class _LazyTable:
    """``Table.create()`` called once, on first use, and cached — the
    lazy-create-and-cache shape every leaf/main table in this package's
    ``TreeStrategy`` implementations shares (the outer group-table half of
    the same shape is ``_NamedGroupTable.get()`` below, kept separate
    since it also needs ``list_top_level``'s own full-scan listing, not
    just the bare ``Table``)."""

    def __init__(
        self,
        provider: SaasWorkloadProvider,
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
    """The shared interface each of this package's concrete classes
    implements, and the only thing ``SaasWorkloadProvider`` drives.
    ``children_of`` is ``async``: every call is a real one-page SQL fetch
    (``WHERE``/``ORDER BY``/``LIMIT``/``OFFSET``), never a full-table
    scan. ``row_for`` stays synchronous: a lookup into the per-key cache
    ``children_of`` populates as it goes, valid only for a key some prior
    ``children_of`` call actually returned."""

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]: ...
    def row_for(self, key: _Key) -> _Row | None: ...


class _NamedGroupTable:
    """The outer "groups are rows of their own table" half shared by
    ``NamedGroupFlatTree`` (Calendar) and ``NamedGroupRecursiveTree``
    (Site): a lazily-created ``Table`` plus a one-shot full-scan ``+``
    ``paginate`` top-level listing — identical in both, since only what
    each does with a *leaf* (flat ``WHERE`` vs. parent-pointer
    recursion) actually differs between them. ``name_override``, when
    given, can replace a specific row's own ``name_column`` value —
    Calendar's own use (a user's real display name standing in for
    their primary calendar's own ``summary``, which Google's API
    defaults to the bare account email) is the only current caller."""

    def __init__(
        self,
        provider: SaasWorkloadProvider,
        *,
        table: str,
        columns: list[Column],
        id_column: str,
        name_column: str,
        name_override: Callable[[_Row], str | None] | None = None,
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

    def _name_for(self, row: _Row) -> str:
        if self._name_override is not None:
            override = self._name_override(row)
            if override is not None:
                return override
        return str(row[self._name_column])

    async def list_top_level(self, *, offset: int, limit: int | None) -> list[tuple[_Key, str, bool]]:
        table = await self.get()
        order_by = _resolve_order_by(table, [self._name_column])
        groups = [
            ((str(row[self._id_column]),), self._name_for(row), False) async for row in table.select(order_by=order_by)
        ]
        return paginate(groups, offset, limit)
