"""``RecursiveGroupFlatTree``: a recursive folder table whose folders hold
flat leaves from another table (M365 Mail).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from ....storage.table import Column, Table
from ....units.provider_kit import paginate
from ._base import Key, Row, SupportsTable, TreeEntry, _flat_leaf_entries, _LazyTable, _resolve_order_by


class RecursiveGroupFlatTree:
    """Folders are ``group_table`` rows recursing by
    ``group_parent_column`` from ``group_root_id``; a folder's leaves are
    the ``leaf_table`` rows whose ``leaf_group_column`` is its id.

    Keys are the path of folder ids (``("inbox", "sub")``), a leaf adding
    its own id, so ``units/resolve.py`` can descend by prefix. A leaf's
    key has no children."""

    def __init__(
        self,
        provider: SupportsTable,
        *,
        group_table: str,
        group_columns: list[Column],
        group_id_column: str,
        group_name_column: str,
        group_parent_column: str,
        group_root_id: str,
        leaf_table: str,
        leaf_columns: list[Column],
        leaf_id_column: str,
        leaf_group_column: str,
        display_name: Callable[[Row], str],
        order_by: Sequence[str],
        descending: bool = False,
    ) -> None:
        self._group_id_column = group_id_column
        self._group_name_column = group_name_column
        self._group_parent_column = group_parent_column
        self._group_root_id = group_root_id
        self._leaf_id_column = leaf_id_column
        self._leaf_group_column = leaf_group_column
        self._display_name = display_name
        self._order_by = order_by
        self._descending = descending
        self._group_lazy_table = _LazyTable(
            provider, table=group_table, columns=group_columns, index_hints=[[group_parent_column]]
        )
        self._leaf_lazy_table = _LazyTable(
            provider, table=leaf_table, columns=leaf_columns, index_hints=[[leaf_group_column]]
        )

    async def children_of(self, key: Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        """One folder's subfolders, then its leaves. Subfolders are read in
        full and paginated in Python; leaves are paged in SQL for the rest
        of the window."""
        folder_id = key[-1] if key else self._group_root_id
        group_table = await self._group_lazy_table.get()
        subfolders = await self._list_subfolders(group_table, key, folder_id)

        # Folders sort before leaves, so this slice is already exact.
        folder_entries = paginate(subfolders, offset, limit)
        remaining = None if limit is None else max(limit - len(folder_entries), 0)
        if limit is not None and remaining == 0:
            # A full page from folders alone: skip the leaf query.
            return folder_entries

        # How far into the leaf rows this window starts.
        leaf_offset = max(offset - len(subfolders), 0)
        leaf_table = await self._leaf_lazy_table.get()
        leaf_entries = await _flat_leaf_entries(
            leaf_table,
            where=f"{self._leaf_group_column} = ?",
            params=(folder_id,),
            id_column=self._leaf_id_column,
            display_name=self._display_name,
            order_by=self._order_by,
            descending=self._descending,
            offset=leaf_offset,
            limit=remaining,
            key_prefix=key,
        )
        return folder_entries + leaf_entries

    async def _list_subfolders(self, group_table: Table, key: Key, folder_id: str) -> list[TreeEntry]:
        order_by = _resolve_order_by(group_table, [self._group_name_column])
        entries: list[TreeEntry] = []
        async for row in group_table.select(f"{self._group_parent_column} = ?", (folder_id,), order_by=order_by):
            sub_id = str(row[self._group_id_column])
            name = str(row[self._group_name_column]) or sub_id
            entries.append(TreeEntry((*key, sub_id), name, False))
        return entries
