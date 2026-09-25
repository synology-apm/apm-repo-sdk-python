"""``RecursiveGroupFlatTree``: M365 Mail's real folder hierarchy —
groups (folders) recurse via parent-pointer, like ``RecursiveTree``, but
each group's leaves (messages) live in a separate, flat, non-recursive
table instead of recursing themselves, combining the two schema shapes
every service-level DB in this project expands into (parent-pointer
recursion; a flat list optionally grouped by one key).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from ....storage.table import Column, Table
from ...base import paginate
from ._base import _flat_leaf_entries, _Key, _LazyTable, _resolve_order_by, _Row

if TYPE_CHECKING:
    from ..provider import SaasWorkloadProvider


class RecursiveGroupFlatTree:
    """M365 Mail's real folder hierarchy: ``mail_folder_table`` is a
    real, named, self-referencing table (``folder_id``/``folder_name``/
    ``parent_folder_id``, rooted at a synthetic anchor id exactly like
    ``RecursiveTree``'s own ``root_id`` convention) whose rows are
    *always* folders — unlike ``RecursiveTree``/``NamedGroupRecursiveTree``'s
    own mixed tables, no ``is_folder()`` check is needed. Its leaves live
    in a separate, flat, non-recursive table keyed by its own group-fk
    column — a message never nests and never has children of its own.
    "Group" here means what it means for ``NamedGroupFlatTree``/
    ``NamedGroupRecursiveTree`` (a real definitions table with its own
    display-name column), just recursive rather than flat, and with one
    id-space serving as both this table's own recursion key and the leaf
    table's own foreign key.

    Every key is a growing-prefix chain, one real folder id per level
    (e.g. ``("inbox",)``, ``("inbox", "haha")``), not a bare id the way
    ``RecursiveTree`` keys Drive: Mail stays a plain ``SaasWorkloadProvider``
    (not ``RecursiveTreeSaasProvider``), so ``units/resolve.py``'s generic
    ref-descent needs every non-leaf child's key to be a genuine prefix of
    any deeper target's key. A leaf's key extends its own containing
    folder's key by one more segment (its own leaf id).

    ``children_of()`` for one folder lists its real subfolders — always,
    even with zero backed-up messages, the entire point of this class —
    before that folder's own leaves. See ``children_of()`` itself for how
    each half is fetched and windowed.

    A genuine leaf's own key naturally returns ``[]`` from
    ``children_of()`` with no explicit key-shape guard (unlike every
    sibling class in this package): a leaf's own id never appears as any
    row's ``parent_folder_id`` in either table. A ``NULL``
    ``leaf_group_column`` value is a documented non-concern the same way:
    it never matches ``WHERE ... = ?``, and real M365 rows always
    populate it.

    ``contact_folder_table`` has this exact same shape and could reuse
    this class later — out of scope for now, ``contact.py`` is untouched."""

    def __init__(
        self,
        provider: SaasWorkloadProvider,
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
        display_name: Callable[[_Row], str],
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
        #: Leaf rows only — a folder key is never inserted here, so
        #: row_for()'s "None for a folder" contract holds with no
        #: explicit is_folder filtering at all, unlike
        #: RecursiveTree/NamedGroupRecursiveTree's shared row cache.
        self._rows: dict[_Key, _Row] = {}

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]:
        """One folder's real subfolders (always, even with zero backed-up
        messages) before that folder's own leaves. A folder's own
        subfolder count is small (this mailbox's own folder fan-out, never
        its message count), so ``_list_subfolders`` fully materializes and
        paginates it in Python, the same small-bounded-scan precedent
        ``_NamedGroupTable.list_top_level`` already establishes; only the
        leaf half issues a real ``WHERE``/``ORDER BY``/``LIMIT``/``OFFSET``
        query, windowed to cover only whatever part of
        ``[offset, offset+limit)`` the subfolders didn't already satisfy."""
        folder_id = key[-1] if key else self._group_root_id
        group_table = await self._group_lazy_table.get()
        subfolders = await self._list_subfolders(group_table, key, folder_id)

        # Folders always sort before leaves. paginate() on the folders
        # list alone already gives the exact right slice for however
        # much of [offset, offset+limit) folders can satisfy — Python
        # slicing on a list stops at its own length, so this is correct
        # even when offset/limit run past the end of `subfolders`.
        folder_entries = paginate(subfolders, offset, limit)
        remaining = None if limit is None else max(limit - len(folder_entries), 0)
        if limit is not None and remaining == 0:
            # A full page came entirely from folders — never query
            # leaves at all for this call.
            return folder_entries

        # How far into the *leaf* rows this window reaches: 0 while any
        # part of [offset, offset+limit) still overlaps the folder list
        # (folder_entries already covers that part), else however far
        # past every folder this window starts.
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
            rows=self._rows,
        )
        return folder_entries + leaf_entries

    async def _list_subfolders(self, group_table: Table, key: _Key, folder_id: str) -> list[tuple[_Key, str, bool]]:
        order_by = _resolve_order_by(group_table, [self._group_name_column])
        entries: list[tuple[_Key, str, bool]] = []
        async for row in group_table.select(f"{self._group_parent_column} = ?", (folder_id,), order_by=order_by):
            sub_id = str(row[self._group_id_column])
            name = str(row[self._group_name_column]) or sub_id
            entries.append((key + (sub_id,), name, False))
        return entries

    def row_for(self, key: _Key) -> _Row | None:
        return self._rows.get(key)
