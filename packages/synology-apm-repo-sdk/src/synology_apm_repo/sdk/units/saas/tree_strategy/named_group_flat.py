"""``NamedGroupFlatTree``: named groups from their own table, each holding
flat leaves (Calendar).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from ....storage.table import Column
from ._base import Key, Row, SupportsTable, TreeEntry, _flat_leaf_entries, _LazyTable, _NamedGroupTable


class NamedGroupFlatTree:
    """Groups are the rows of ``group_table``; leaves are the rows of
    ``leaf_table`` whose ``leaf_group_column`` names the group. The group
    level is read in full and paginated in Python; each group's leaves
    are paged in SQL. ``group_name_override`` may replace a group's
    ``group_name_column`` value."""

    def __init__(
        self,
        provider: SupportsTable,
        *,
        group_table: str,
        group_columns: list[Column],
        group_id_column: str,
        group_name_column: str,
        leaf_table: str,
        leaf_columns: list[Column],
        leaf_id_column: str,
        leaf_group_column: str,
        display_name: Callable[[Row], str],
        order_by: Sequence[str],
        group_name_override: Callable[[Row], str | None] | None = None,
    ) -> None:
        self._provider = provider
        self._groups = _NamedGroupTable(
            provider,
            table=group_table,
            columns=group_columns,
            id_column=group_id_column,
            name_column=group_name_column,
            name_override=group_name_override,
        )
        self._leaf_id_column = leaf_id_column
        self._leaf_group_column = leaf_group_column
        self._display_name = display_name
        self._order_by = order_by
        self._leaf_lazy_table = _LazyTable(
            provider, table=leaf_table, columns=leaf_columns, index_hints=[[leaf_group_column]]
        )

    async def children_of(self, key: Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        if key == ():
            return await self._groups.list_top_level(offset=offset, limit=limit)
        if len(key) != 1:
            return []  # a leaf's key
        (group_id,) = key
        leaf_table = await self._leaf_lazy_table.get()
        return await _flat_leaf_entries(
            leaf_table,
            where=f"{self._leaf_group_column} = ?",
            params=(group_id,),
            id_column=self._leaf_id_column,
            display_name=self._display_name,
            order_by=self._order_by,
            descending=False,
            offset=offset,
            limit=limit,
            key_prefix=(group_id,),
        )
