"""``RecursiveTree``: Drive's shape — parent-pointer recursion
(``parent_folder_id`` + a root id), one of the two schema shapes every
service-level DB in this project expands into (the other being a flat
list, optionally grouped by one key).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from ....storage.table import Column
from ._base import FolderPredicate, _Key, _LazyTable, _resolve_order_by, _Row

if TYPE_CHECKING:
    from ..provider import SaasWorkloadProvider


class RecursiveTree:
    """Drive's shape: one table, no group layer — the root is the top
    of one parent-pointer recursion rooted at ``root_id``. ``root_id``
    itself never has a row (a synthetic anchor, not a browsable item),
    but it *is* a real value ``parent_folder_id`` stores for top-level
    items, so the same ``WHERE parent_folder_id = ?`` query handles the
    root level like any other folder."""

    def __init__(
        self,
        provider: SaasWorkloadProvider,
        *,
        table: str,
        columns: list[Column],
        id_column: str,
        parent_column: str,
        root_id: str,
        folder: FolderPredicate,
        display_name: Callable[[_Row], str],
        order_by: Sequence[str],
    ) -> None:
        self._provider = provider
        self._id_column = id_column
        self._parent_column = parent_column
        self._root_id = root_id
        self._folder = folder
        self._display_name = display_name
        self._order_by = order_by
        self._lazy_table = _LazyTable(
            provider, table=table, columns=columns, index_hints=[[parent_column], [id_column]]
        )
        self._rows: dict[str, _Row] = {}

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]:
        table = await self._lazy_table.get()
        parent_id = key[0] if key else self._root_id
        order_by = _resolve_order_by(table, self._order_by, dir_first_sql=self._folder.sql)
        out: list[tuple[_Key, str, bool]] = []
        async for row in table.select(
            f"{self._parent_column} = ?", (parent_id,), order_by=order_by, limit=limit, offset=offset
        ):
            item_id = str(row[self._id_column])
            self._rows[item_id] = row
            out.append(((item_id,), self._display_name(row), not self._folder.is_folder(row)))
        return out

    def row_for(self, key: _Key) -> _Row | None:
        """``None`` for a folder's key too, not just a missing one — a
        folder is never a restorable unit (``UnitProvider``: ``unit()``
        is for leaves only), so ``assemble()`` must never see its row
        even though the cache holds it for traversal."""
        if len(key) != 1:
            return None
        row = self._rows.get(key[0])
        if row is None or self._folder.is_folder(row):
            return None
        return row

    async def resolve_id(self, item_id: str) -> tuple[_Key, str, bool] | None:
        """Direct ``WHERE id_column = ?`` lookup for ``item_id``,
        bypassing ``children_of``'s parent-scoped scan entirely — the
        mechanism ``SupportsDirectRefLookup`` needs, since a Drive
        item's key is a single, depth-independent id with no
        parent-scoped ``children_of`` call that would find it
        otherwise. Populates ``self._rows`` exactly like
        ``children_of`` does, so a later ``row_for``/``unit()`` call on
        the same key behaves identically to one reached through
        ordinary traversal. ``None`` if no such row exists."""
        table = await self._lazy_table.get()
        row = await table.select_one(f"{self._id_column} = ?", (item_id,))
        if row is None:
            return None
        self._rows[item_id] = row
        return (item_id,), self._display_name(row), not self._folder.is_folder(row)

    def parent_id_of(self, key: _Key) -> str | None:
        """The immediate parent's own id for ``key`` — unlike
        ``row_for``, does not hide a folder's row, since an ancestor
        is a folder by definition. ``None`` when ``key``'s row isn't
        cached yet, or its parent is the synthetic root (no further
        ancestor to walk to)."""
        row = self._rows.get(key[0])
        if row is None:
            return None
        parent_id = str(row[self._parent_column])
        return None if parent_id == self._root_id else parent_id
