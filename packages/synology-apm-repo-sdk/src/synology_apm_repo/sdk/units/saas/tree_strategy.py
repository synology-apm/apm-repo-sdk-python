"""Shared tree-expansion helpers for SaaS application-layer
providers. Every service-level DB in this project's real schemas
expands into a browsable tree one of two shapes: parent-pointer
recursion (``parent_folder_id`` + a root id — Drive, Site's document
libraries, Contact folders) or a flat list, optionally grouped by one
key (Calendar events by ``calendar_id``, Mail by folder, Site's lists
ungrouped). FS's own third strategy (absolute-path lookup) is
FS-specific and lives in ``units/fs.py``, not duplicated here.

``TreeStrategy`` is the interface ``SaasWorkloadProvider``'s
``children()``/``unit()`` drive (see its own docstring for the shared
async/sync contract); every key is an opaque ref-segment tuple, ``()``
meaning the provider root. Its five concrete implementations —
``SyntheticGroupedTree`` (Contact, and M365 Mail's own degrade path),
``NamedGroupFlatTree`` (Calendar), ``RecursiveTree`` (Drive),
``NamedGroupRecursiveTree`` (Site), and ``RecursiveGroupFlatTree`` (M365
Mail's real folder hierarchy) — cover exactly those shapes; see each
class's own docstring for its specifics.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Protocol

from ...storage.table import Column, Table
from ..base import paginate

if TYPE_CHECKING:
    from .provider import SaasWorkloadProvider

_Row = dict[str, object | None]
_Key = tuple[str, ...]


def _resolve_order_by(table: Table, preferred: Sequence[str], *, descending: bool = False) -> str:
    """Builds an ``ORDER BY`` expression from ``preferred`` column
    names, filtered down to the table's actual columns present (the
    schema-drift tolerance ``storage/table.py`` describes, traps
    #19/#20) — always with SQLite's implicit ``rowid`` appended last,
    unconditionally, as the one sort key present and stable across
    every schema version. ``descending`` applies ``DESC`` to every
    column individually (``ORDER BY a, b DESC`` in SQL only reverses
    the last column, not the whole clause), rather than appending one
    trailing suffix."""
    cols = [c for c in preferred if c in table.columns_present]
    cols.append("rowid")
    if descending:
        return ", ".join(f"{c} DESC" for c in cols)
    return ", ".join(cols)


async def _flat_leaf_entries(
    table: Table,
    *,
    where: str,
    params: tuple[object, ...],
    id_column: str,
    display_name: Callable[[_Row], str],
    order_by: Sequence[str],
    descending: bool,
    offset: int,
    limit: int | None,
    key_prefix: _Key,
    rows: dict[_Key, _Row],
) -> list[tuple[_Key, str, bool]]:
    """Shared leaf-listing loop for a flat (non-recursive) leaf table,
    keyed by appending each row's own id to ``key_prefix`` —
    ``SyntheticGroupedTree``, ``NamedGroupFlatTree``, and
    ``RecursiveGroupFlatTree`` all query, cache, and emit leaf entries this
    same way; only what the ``WHERE``/``key_prefix`` are built from differs
    between them. ``RecursiveTree``/``NamedGroupRecursiveTree`` don't use
    this: their own rows can also be folders (``is_folder(row)``), which
    every caller of this helper's own leaf table never has."""
    resolved_order_by = _resolve_order_by(table, order_by, descending=descending)
    out: list[tuple[_Key, str, bool]] = []
    async for row in table.select(where, params, order_by=resolved_order_by, limit=limit, offset=offset):
        leaf_key = key_prefix + (str(row[id_column]),)
        rows[leaf_key] = row
        out.append((leaf_key, display_name(row), True))
    return out


class _LazyTable:
    """``Table.create()`` called once, on first use, and cached — the
    lazy-create-and-cache shape every leaf/main table in this module's
    five ``TreeStrategy`` implementations shares (the outer group-table
    half of the same shape is ``_NamedGroupTable.get()`` below, kept
    separate since it also needs ``list_top_level``'s own full-scan
    listing, not just the bare ``Table``)."""

    def __init__(
        self,
        provider: SaasWorkloadProvider,
        *,
        table: str,
        columns: list[Column],
        index_hints: list[list[str]] | None = None,
    ) -> None:
        self._provider = provider
        self._table = table
        self._columns = columns
        self._index_hints = index_hints or []
        self._table_obj: Table | None = None

    async def get(self) -> Table:
        if self._table_obj is None:
            self._table_obj = await Table.create(
                self._provider.table(self._table), self._table, self._columns, index_hints=self._index_hints
            )
        return self._table_obj


class TreeStrategy(Protocol):
    """The shared interface each of this module's five concrete
    classes implements, and the only thing ``SaasWorkloadProvider``
    drives. ``children_of`` is ``async``: every call is a real one-page
    SQL fetch (``WHERE``/``ORDER BY``/``LIMIT``/``OFFSET``), never a
    full-table scan. ``row_for`` stays synchronous: a lookup into the
    per-key cache ``children_of`` populates as it goes, valid only for
    a key some prior ``children_of`` call actually returned."""

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]: ...
    def row_for(self, key: _Key) -> _Row | None: ...


class SyntheticGroupedTree:
    """Mail/Contact's shape: one table, grouped by a *value* of its own
    group column; there is no separate group-naming table, so a
    group's display name defaults to the group-key value itself unless
    ``group_display_name`` resolves it.

    ``group_column`` is the real column to filter by (e.g.
    ``"parent_folder_id"``). GWS passes ``None`` because it genuinely
    has no such column — Gmail has no folder hierarchy at all (see
    ``mail.py``'s own module docstring), and GWS Contact's groups are
    themselves M:N — so every row then falls into one synthetic
    ``root_name`` group instead."""

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
            # A leaf's own key (2 segments) genuinely has no children.
            # Confirmed executed via direct sys.settrace (see
            # test_units_saas_tree_strategy.py's own
            # test_children_of_a_leaf_key_has_no_children_of_its_own) --
            # coverage.py itself still reports this line missing, the
            # same false negative CONTRIBUTING.md documents for
            # units/device.py's own pragma'd line.
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


class _NamedGroupTable:
    """The outer "groups are rows of their own table" half shared by
    ``NamedGroupFlatTree`` (Calendar) and ``NamedGroupRecursiveTree``
    (Site): a lazily-created ``Table`` plus a one-shot full-scan ``+``
    ``paginate`` top-level listing — identical in both, since only what
    each does with a *leaf* (flat ``WHERE`` vs. parent-pointer
    recursion) actually differs between them."""

    def __init__(
        self,
        provider: SaasWorkloadProvider,
        *,
        table: str,
        columns: list[Column],
        id_column: str,
        name_column: str,
    ) -> None:
        self._provider = provider
        self._table = table
        self._columns = columns
        self._id_column = id_column
        self._name_column = name_column
        self._table_obj: Table | None = None

    async def get(self) -> Table:
        if self._table_obj is None:
            self._table_obj = await Table.create(self._provider.table(self._table), self._table, self._columns)
        return self._table_obj

    async def list_top_level(self, *, offset: int, limit: int | None) -> list[tuple[_Key, str, bool]]:
        table = await self.get()
        order_by = _resolve_order_by(table, [self._name_column])
        groups = [
            ((str(row[self._id_column]),), str(row[self._name_column]), False)
            async for row in table.select(order_by=order_by)
        ]
        return paginate(groups, offset, limit)


class NamedGroupFlatTree:
    """Calendar's shape: an outer table whose rows *are* the groups
    (with their own display-name column); an inner table holds flat
    leaves grouped by a foreign key. The outer listing is a one-shot
    full scan + ``paginate`` slice (small, bounded — every real "level"
    is the event count within one calendar, not the calendar count
    itself); the leaf level gets a real ``WHERE``/``ORDER BY``/
    ``LIMIT``/``OFFSET`` query."""

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
    ) -> None:
        self._provider = provider
        self._groups = _NamedGroupTable(
            provider, table=group_table, columns=group_columns, id_column=group_id_column, name_column=group_name_column
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
        is_folder: Callable[[_Row], bool],
        display_name: Callable[[_Row], str],
        order_by: Sequence[str],
    ) -> None:
        self._provider = provider
        self._id_column = id_column
        self._parent_column = parent_column
        self._root_id = root_id
        self._is_folder = is_folder
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
        order_by = _resolve_order_by(table, self._order_by)
        out: list[tuple[_Key, str, bool]] = []
        async for row in table.select(
            f"{self._parent_column} = ?", (parent_id,), order_by=order_by, limit=limit, offset=offset
        ):
            item_id = str(row[self._id_column])
            self._rows[item_id] = row
            out.append(((item_id,), self._display_name(row), not self._is_folder(row)))
        return out

    def row_for(self, key: _Key) -> _Row | None:
        """``None`` for a folder's key too, not just a missing one — a
        folder is never a restorable unit (``UnitProvider``: ``unit()``
        is for leaves only), so ``assemble()`` must never see its row
        even though the cache holds it for traversal."""
        if len(key) != 1:
            return None
        row = self._rows.get(key[0])
        if row is None or self._is_folder(row):
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
        return (item_id,), self._display_name(row), not self._is_folder(row)

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
        is_folder: Callable[[_Row], bool],
        display_name: Callable[[_Row], str],
        order_by: Sequence[str],
        root_folder_id_of: Callable[[str], str] | None = None,
    ) -> None:
        self._provider = provider
        self._groups = _NamedGroupTable(
            provider, table=group_table, columns=group_columns, id_column=group_id_column, name_column=group_name_column
        )
        self._self_id_of = self_id_of
        self._leaf_group_column = leaf_group_column
        self._leaf_parent_column = leaf_parent_column
        self._is_folder = is_folder
        self._display_name = display_name
        self._order_by = order_by
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
        order_by = _resolve_order_by(leaf_table, self._order_by)
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
            out.append((key + (self_id,), self._display_name(row), not self._is_folder(row)))
        return out

    def row_for(self, key: _Key) -> _Row | None:
        """``None`` for a folder's own key too — see
        ``RecursiveTree.row_for``'s docstring for why (same reasoning,
        applied to this shape's own per-group row cache)."""
        if len(key) < 2:
            return None
        row = self._rows.get((key[0], key[-1]))
        if row is None or self._is_folder(row):
            return None
        return row


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
    sibling class in this module): a leaf's own id never appears as any
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
