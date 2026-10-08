"""``RecursiveTree``: parent-pointer recursion over one table (Drive)."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from ....storage.table import Column
from ._base import FolderPredicate, Key, Row, SupportsTable, TreeEntry, _LazyTable, _resolve_order_by


class RecursiveTree:
    """One table recursing by ``parent_column`` from ``root_id``, the
    parent value of top-level items (it has no row of its own); folders
    listed first. Every key is a single, depth-independent item id."""

    def __init__(
        self,
        provider: SupportsTable,
        *,
        table: str,
        columns: list[Column],
        id_column: str,
        parent_column: str,
        root_id: str,
        folder: FolderPredicate,
        display_name: Callable[[Row], str],
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

    async def children_of(self, key: Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        table = await self._lazy_table.get()
        parent_id = key[0] if key else self._root_id
        order_by = _resolve_order_by(table, self._order_by, dir_first_sql=self._folder.sql)
        return [
            self._entry(row)
            async for row in table.select(
                f"{self._parent_column} = ?", (parent_id,), order_by=order_by, limit=limit, offset=offset
            )
        ]

    def _entry(self, row: Row) -> TreeEntry:
        """Folders carry their row too, for ``parent_id_of``."""
        return TreeEntry((str(row[self._id_column]),), self._display_name(row), not self._folder.is_folder(row), row)

    async def resolve_id(self, item_id: str) -> TreeEntry | None:
        """Look ``item_id`` up directly; ``None`` if no such row exists."""
        table = await self._lazy_table.get()
        row = await table.select_one(f"{self._id_column} = ?", (item_id,))
        return self._entry(row) if row is not None else None

    def parent_id_of(self, row: Row) -> str | None:
        """The parent id of the item ``row`` (folder or not); ``None`` when
        its parent is ``root_id``."""
        parent_id = str(row[self._parent_column])
        return None if parent_id == self._root_id else parent_id
