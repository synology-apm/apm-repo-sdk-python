"""``SyntheticGroupedTree``: one table, optionally grouped by a column's
values (Mail, Contact).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from ....storage.table import Column, Table
from ....units.provider_kit import paginate
from ._base import Key, Row, SupportsTable, TreeEntry, _flat_leaf_entries, _LazyTable


class SyntheticGroupedTree:
    """One group per distinct ``group_column`` value, named by
    ``group_display_name`` (default: the value itself); rows whose value is
    NULL fall in the ``root_name`` group. With ``group_column=None`` every
    row is in the single ``root_name`` group."""

    def __init__(
        self,
        provider: SupportsTable,
        *,
        table: str,
        columns: list[Column],
        id_column: str,
        group_column: str | None,
        display_name: Callable[[Row], str],
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

    async def children_of(self, key: Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        table = await self._lazy_table.get()
        if key == ():
            return await self._list_groups(offset=offset, limit=limit)
        if len(key) != 1:
            # A leaf key (2 segments) has no children; never expanded in practice.
            return []  # pragma: no cover
        (group,) = key
        return await self._list_members(table, group, offset=offset, limit=limit)

    async def _list_groups(self, *, offset: int, limit: int | None) -> list[TreeEntry]:
        if self._group_column is None:
            groups = [self._root_name]
        else:
            conn = self._provider.table(self._table)
            cursor = await conn.execute(
                f"SELECT DISTINCT {self._group_column} FROM {self._table} ORDER BY {self._group_column}"
            )
            groups = [str(value) if value is not None else self._root_name for (value,) in await cursor.fetchall()]
        entries = [TreeEntry((group,), self._display_name_for_group(group), False) for group in groups]
        return paginate(entries, offset, limit)

    async def _list_members(self, table: Table, group: str, *, offset: int, limit: int | None) -> list[TreeEntry]:
        where: str
        params: tuple[object, ...]
        if self._group_column is None:
            # Nothing to filter by; the page is still bounded by LIMIT/OFFSET.
            where, params = "", ()
        elif group == self._root_name:
            # IS NULL: "= root_name" would never match, hiding these rows.
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
        )

    def _display_name_for_group(self, group: str) -> str:
        if group == self._root_name:
            return self._root_name
        if self._group_display_name is not None:
            return self._group_display_name(group)
        return group
