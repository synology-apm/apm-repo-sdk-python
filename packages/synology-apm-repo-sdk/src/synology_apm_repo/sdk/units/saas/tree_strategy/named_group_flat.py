"""``NamedGroupFlatTree``: Calendar's shape — the flat-list variant of
this package's two tree shapes: an outer table whose rows are the named
groups, with a separately-queried flat leaf table grouped by a foreign
key, rather than parent-pointer recursion (Drive, Site's document
libraries, Contact folders).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from ....storage.table import Column
from ._base import _flat_leaf_entries, _Key, _LazyTable, _NamedGroupTable, _Row

if TYPE_CHECKING:
    from ..provider import SaasWorkloadProvider


class NamedGroupFlatTree:
    """Calendar's shape: an outer table whose rows *are* the groups
    (with their own display-name column); an inner table holds flat
    leaves grouped by a foreign key. The outer listing is a one-shot
    full scan + ``paginate`` slice (small, bounded — every real "level"
    is the event count within one calendar, not the calendar count
    itself); the leaf level gets a real ``WHERE``/``ORDER BY``/
    ``LIMIT``/``OFFSET`` query. ``group_name_override`` lets a caller
    replace a specific group's own name; unused (``None``) means every group
    shows its ``group_name_column`` value verbatim."""

    def __init__(
        self,
        provider: SaasWorkloadProvider,
        *,
        group_table: str,
        group_columns: list[Column],
        group_id_column: str,
        group_name_column: str,
        leaf_table: str,
        leaf_columns: list[Column],
        leaf_id_column: str,
        leaf_group_column: str,
        display_name: Callable[[_Row], str],
        order_by: Sequence[str],
        group_name_override: Callable[[_Row], str | None] | None = None,
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
        self._rows: dict[_Key, _Row] = {}

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]:
        if key == ():
            return await self._groups.list_top_level(offset=offset, limit=limit)
        if len(key) != 1:
            return []  # a leaf's own key (2 segments) genuinely has no children
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
            rows=self._rows,
        )

    def row_for(self, key: _Key) -> _Row | None:
        return self._rows.get(key) if len(key) == 2 else None
