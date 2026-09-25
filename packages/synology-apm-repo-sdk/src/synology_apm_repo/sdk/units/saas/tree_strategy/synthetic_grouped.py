"""``SyntheticGroupedTree``: Mail/Contact's shape — a flat list,
optionally grouped by one key, one of the two schema shapes every
service-level DB in this project expands into (the other being
parent-pointer recursion).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from ....storage.table import Column, Table
from ...base import paginate
from ._base import _flat_leaf_entries, _Key, _LazyTable, _Row

if TYPE_CHECKING:
    from ..provider import SaasWorkloadProvider


class SyntheticGroupedTree:
    """Mail/Contact's shape: one table, grouped by a *value* of its own
    group column; there is no separate group-naming table, so a
    group's display name defaults to the group-key value itself unless
    ``group_display_name`` resolves it.

    ``group_column`` is the real column to filter by (e.g.
    ``"parent_folder_id"``). GWS passes ``None`` because it genuinely
    has no such column — Gmail has no folder hierarchy at all, only
    many-to-many labels surfaced as an ``extra_attrs`` field rather than
    a tree grouping, and GWS Contact's groups are themselves M:N — so
    every row then falls into one synthetic ``root_name`` group
    instead."""

    def __init__(
        self,
        provider: SaasWorkloadProvider,
        *,
        table: str,
        columns: list[Column],
        id_column: str,
        group_column: str | None,
        display_name: Callable[[_Row], str],
        root_name: str,
        order_by: Sequence[str],
        descending: bool = False,
        group_display_name: Callable[[str], str] | None = None,
    ) -> None:
        self._provider = provider
        self._table = table
        self._id_column = id_column
        self._group_column = group_column
        self._display_name = display_name
        self._root_name = root_name
        self._order_by = order_by
        self._descending = descending
        self._group_display_name = group_display_name
        hints = [[group_column]] if group_column is not None else []
        self._lazy_table = _LazyTable(provider, table=table, columns=columns, index_hints=hints)
        self._rows: dict[_Key, _Row] = {}

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]:
        table = await self._lazy_table.get()
        if key == ():
            return await self._list_groups(offset=offset, limit=limit)
        if len(key) != 1:
            # A leaf's own key (2 segments) genuinely has no children, but
            # no real caller ever reaches this branch: children_of() is only
            # ever called on a key by a caller expanding a container, and a
            # leaf node (built with is_leaf=True) is never expanded.
            return []  # pragma: no cover
        (group,) = key
        return await self._list_members(table, group, offset=offset, limit=limit)

    async def _list_groups(self, *, offset: int, limit: int | None) -> list[tuple[_Key, str, bool]]:
        if self._group_column is None:
            groups = [self._root_name]
        else:
            conn = self._provider.table(self._table)
            cursor = await conn.execute(
                f"SELECT DISTINCT {self._group_column} FROM {self._table} ORDER BY {self._group_column}"
            )
            groups = [str(value) if value is not None else self._root_name for (value,) in await cursor.fetchall()]
        entries = [((group,), self._display_name_for_group(group), False) for group in groups]
        return paginate(entries, offset, limit)

    async def _list_members(
        self, table: Table, group: str, *, offset: int, limit: int | None
    ) -> list[tuple[_Key, str, bool]]:
        where: str
        params: tuple[object, ...]
        if self._group_column is None:
            # Nothing to filter by, but children_of() below still costs
            # only one page's I/O via ORDER BY ... LIMIT ? OFFSET ?, not
            # a full-table load.
            where, params = "", ()
        elif group == self._root_name:
            # IS NULL, never a literal "= root_name" — that string
            # would never match a real column value and would silently
            # hide these rows.
            where, params = f"{self._group_column} IS NULL", ()
        else:
            where, params = f"{self._group_column} = ?", (group,)
        return await _flat_leaf_entries(
            table,
            where=where,
            params=params,
            id_column=self._id_column,
            display_name=self._display_name,
            order_by=self._order_by,
            descending=self._descending,
            offset=offset,
            limit=limit,
            key_prefix=(group,),
            rows=self._rows,
        )

    def row_for(self, key: _Key) -> _Row | None:
        return self._rows.get(key) if len(key) == 2 else None

    def _display_name_for_group(self, group: str) -> str:
        if group == self._root_name:
            return self._root_name
        if self._group_display_name is not None:
            return self._group_display_name(group)
        return group
