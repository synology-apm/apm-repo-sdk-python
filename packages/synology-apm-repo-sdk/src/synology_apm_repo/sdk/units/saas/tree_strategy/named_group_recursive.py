"""``NamedGroupRecursiveTree``: Site's shape — parent-pointer recursion
(``parent_folder_id`` + a root id) nested within named groups, combining
the two schema shapes every service-level DB in this project expands
into (parent-pointer recursion; a flat list optionally grouped by one
key).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from ....storage.table import Column
from ...base import dir_first_order_by
from ._base import FolderPredicate, _Key, _LazyTable, _NamedGroupTable, _resolve_order_by, _Row

if TYPE_CHECKING:
    from ..provider import SaasWorkloadProvider


class NamedGroupRecursiveTree:
    """Site's shape: like ``NamedGroupFlatTree``, but the inner table
    recurses via parent-pointer *within* each outer group instead of
    being flat (a document library's nested folders).

    ``self_id_of`` supplies each row's own id (Site's own
    ``file_id``-or-``item_id`` rule, not a plain column) — used only to
    build an outgoing child's key, never to filter; the ``WHERE``
    clause always compares against the *stored* ``leaf_parent_column``
    value directly.

    ``root_folder_id_of`` resolves each group's own top-level anchor.
    Assuming every group's top level is the empty string is wrong: a
    general List's top level is ``parent_folder_id = ""``, but a
    document library's top-level items instead share one non-empty
    ``list_version_table.root_folder_id`` value — without this, those
    top-level rows never match the ``WHERE`` clause and the library
    appears empty. Defaults to the empty string when a caller has no
    per-group anchor."""

    _ROOT_FOLDER = ""

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
        self_id_of: Callable[[_Row], str],
        leaf_group_column: str,
        leaf_parent_column: str,
        folder: FolderPredicate,
        display_name: Callable[[_Row], str],
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
            # Site's item_version_table is the one real schema with no
            # real index for this WHERE — this hint builds one, once,
            # into the private per-version temp copy this SDK already
            # reads the table from, never a write to the real repository file.
            index_hints=[[leaf_group_column, leaf_parent_column]],
        )
        self._rows: dict[_Key, _Row] = {}

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]:
        if key == ():
            return await self._groups.list_top_level(offset=offset, limit=limit)
        group_id = key[0]
        parent_self_id = key[-1] if len(key) > 1 else self._root_folder_id_of(group_id)
        leaf_table = await self._leaf_lazy_table.get()
        # order_by_sql is a raw expression (e.g. site.py's title-vs-url_path
        # CASE), not a plain column name -- _resolve_order_by's column-list
        # path can't carry it (it filters each entry against
        # table.columns_present), so it bypasses that resolution entirely
        # and is used verbatim, with dir-first wrapping applied directly.
        if self._order_by_sql is not None:
            order_by = dir_first_order_by(self._folder.sql, f"{self._order_by_sql}, rowid")
        else:
            order_by = _resolve_order_by(leaf_table, self._order_by, dir_first_sql=self._folder.sql)
        out: list[tuple[_Key, str, bool]] = []
        async for row in leaf_table.select(
            f"{self._leaf_group_column} = ? AND {self._leaf_parent_column} = ?",
            (group_id, parent_self_id),
            order_by=order_by,
            limit=limit,
            offset=offset,
        ):
            self_id = self._self_id_of(row)
            self._rows[(group_id, self_id)] = row
            out.append((key + (self_id,), self._display_name(row), not self._folder.is_folder(row)))
        return out

    def row_for(self, key: _Key) -> _Row | None:
        """``None`` for a folder's own key too, not just a missing one —
        a folder is never a restorable unit, so ``assemble()`` must never
        see its row even though the cache holds it for traversal."""
        if len(key) < 2:
            return None
        row = self._rows.get((key[0], key[-1]))
        if row is None or self._folder.is_folder(row):
            return None
        return row
