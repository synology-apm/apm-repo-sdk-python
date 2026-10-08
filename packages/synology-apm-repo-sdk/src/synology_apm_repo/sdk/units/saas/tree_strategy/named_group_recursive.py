"""``NamedGroupRecursiveTree``: named groups whose leaves recurse by
parent pointer (Site).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from ....storage.table import Column
from ....units.provider_kit import dir_first_order_by
from ._base import FolderPredicate, Key, Row, SupportsTable, TreeEntry, _LazyTable, _NamedGroupTable, _resolve_order_by


class NamedGroupRecursiveTree:
    """Like ``NamedGroupFlatTree``, but within each group the leaf table
    recurses by ``leaf_parent_column`` (a document library's folders),
    folders listed first.

    ``self_id_of`` gives a row's id, which its children's
    ``leaf_parent_column`` holds and which becomes its key segment.
    ``root_folder_id_of`` gives a group's top-level parent value
    (default ``""``). ``order_by_sql``, when given, is a raw ``ORDER BY``
    expression used instead of ``order_by``."""

    _ROOT_FOLDER = ""

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
        self_id_of: Callable[[Row], str],
        leaf_group_column: str,
        leaf_parent_column: str,
        folder: FolderPredicate,
        display_name: Callable[[Row], str],
        order_by: Sequence[str] = (),
        root_folder_id_of: Callable[[str], str] | None = None,
        order_by_sql: str | None = None,
    ) -> None:
        self._provider = provider
        self._groups = _NamedGroupTable(
            provider, table=group_table, columns=group_columns, id_column=group_id_column, name_column=group_name_column
        )
        self._self_id_of = self_id_of
        self._leaf_group_column = leaf_group_column
        self._leaf_parent_column = leaf_parent_column
        self._folder = folder
        self._display_name = display_name
        self._order_by = order_by
        self._order_by_sql = order_by_sql
        self._root_folder_id_of: Callable[[str], str] = (
            root_folder_id_of if root_folder_id_of is not None else lambda _group_id: self._ROOT_FOLDER
        )
        self._leaf_lazy_table = _LazyTable(
            provider,
            table=leaf_table,
            columns=leaf_columns,
            # Site's item_version_table has no index for this WHERE; the hint
            # builds one in the private temp copy, never the repository file.
            index_hints=[[leaf_group_column, leaf_parent_column]],
        )

    async def children_of(self, key: Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        if key == ():
            return await self._groups.list_top_level(offset=offset, limit=limit)
        group_id = key[0]
        parent_self_id = key[-1] if len(key) > 1 else self._root_folder_id_of(group_id)
        leaf_table = await self._leaf_lazy_table.get()
        # order_by_sql is a raw expression, not a column name, so it bypasses
        # _resolve_order_by's column filtering and is used verbatim.
        if self._order_by_sql is not None:
            order_by = dir_first_order_by(self._folder.sql, f"{self._order_by_sql}, rowid")
        else:
            order_by = _resolve_order_by(leaf_table, self._order_by, dir_first_sql=self._folder.sql)
        return [
            TreeEntry((*key, self._self_id_of(row)), self._display_name(row), not self._folder.is_folder(row), row)
            async for row in leaf_table.select(
                f"{self._leaf_group_column} = ? AND {self._leaf_parent_column} = ?",
                (group_id, parent_self_id),
                order_by=order_by,
                limit=limit,
                offset=offset,
            )
        ]
