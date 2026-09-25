"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy``'s
``SyntheticGroupedTree`` and ``RecursiveGroupFlatTree`` (plus their shared
``_resolve_order_by`` helper) — a few of ``SyntheticGroupedTree``'s own
branches are genuinely unreachable through either real caller
(``mail.py``/``contact.py``): both always supply a ``group_display_name``
resolver, so the class's own documented "falls back to the raw group
value" default never runs via them; and neither ever calls
``children_of()`` with a leaf's own (already-2-segment) key directly.
Exercised here via a minimal fake provider and a real, small SQLite table
(through ``SqliteSource``), bypassing the full SaaS-provider/repository
machinery this class's own real callers need only for unrelated reasons
(META resolution, object-name indexing, ...) that don't affect this
class's own listing logic."""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path
from typing import cast

import pytest

from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.storage.table import Column, Table
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.tree_strategy import RecursiveGroupFlatTree, SyntheticGroupedTree
from synology_apm_repo.sdk.units.saas.tree_strategy._base import _resolve_order_by

_COLUMNS = [Column("item_id"), Column("name"), Column("folder_id", required=False)]


def _build_table_bytes(rows: list[tuple[str, str, str | None]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE items(item_id TEXT PRIMARY KEY, name TEXT, folder_id TEXT)")
        conn.executemany("INSERT INTO items VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        return path.read_bytes()


class _FakeProvider:
    """Just enough of ``SaasWorkloadProvider``'s surface for
    ``SyntheticGroupedTree``'s own ``table()`` accessor — every other
    method it needs (META resolution, index lookups, ...) belongs to
    ``_assemble``, never to the listing logic under test here."""

    def __init__(self, source: SqliteSource) -> None:
        self._source = source

    def table(self, name: str) -> object:
        return self._source.connection


async def _build_tree(
    rows: list[tuple[str, str, str | None]],
    *,
    group_display_name: Callable[[str], str] | None = None,
    descending: bool = False,
) -> tuple[SyntheticGroupedTree, SqliteSource]:
    source = await SqliteSource.from_bytes(_build_table_bytes(rows))
    tree = SyntheticGroupedTree(
        cast(SaasWorkloadProvider, _FakeProvider(source)),
        table="items",
        columns=_COLUMNS,
        id_column="item_id",
        group_column="folder_id",
        display_name=lambda row: str(row["name"]),
        root_name="(no folder)",
        order_by=["name"],
        descending=descending,
        group_display_name=group_display_name,
    )
    return tree, source


async def test_children_of_a_leaf_key_has_no_children_of_its_own() -> None:
    # A leaf's own key is already 2 segments (group, item_id) -- a third
    # segment can only come from a stale/malformed pasted ref, never a
    # real listing call this class itself makes.
    tree, source = await _build_tree([("item-1", "Item One", "folder-a")])
    try:
        assert await tree.children_of(("folder-a", "item-1", "extra")) == []
    finally:
        await source.close()


async def test_members_with_no_group_value_are_listed_under_the_root_group() -> None:
    # folder_id IS NULL, not the literal string "(no folder)" -- a real
    # column value would never match that synthetic root name, so this
    # must filter by IS NULL specifically to find these rows at all.
    tree, source = await _build_tree([("item-1", "Item One", None), ("item-2", "Item Two", "folder-a")])
    try:
        groups = await tree.children_of(())
        assert {name for _key, name, _leaf in groups} == {"(no folder)", "folder-a"}
        root_members = await tree.children_of(("(no folder)",))
        assert [name for _key, name, _leaf in root_members] == ["Item One"]
    finally:
        await source.close()


async def test_group_display_name_falls_back_to_the_raw_group_value_without_a_resolver() -> None:
    tree, source = await _build_tree([("item-1", "Item One", "folder-a")])
    try:
        groups = await tree.children_of(())
        [(_key, name, _leaf)] = [g for g in groups if g[0] != ("(no folder)",)]
        assert name == "folder-a"
    finally:
        await source.close()


async def test_group_display_name_resolver_overrides_the_raw_group_value_when_given() -> None:
    tree, source = await _build_tree(
        [("item-1", "Item One", "folder-a")], group_display_name=lambda group: f"Pretty {group}"
    )
    try:
        groups = await tree.children_of(())
        [(_key, name, _leaf)] = [g for g in groups if g[0] != ("(no folder)",)]
        assert name == "Pretty folder-a"
    finally:
        await source.close()


async def test_members_are_listed_newest_first_when_descending_is_set() -> None:
    tree, source = await _build_tree([("item-1", "Aaa", "folder-a"), ("item-2", "Zzz", "folder-a")], descending=True)
    try:
        members = await tree.children_of(("folder-a",))
        assert [name for _key, name, _leaf in members] == ["Zzz", "Aaa"]
    finally:
        await source.close()


class TestResolveOrderBy:
    """Direct unit tests for ``_resolve_order_by`` -- no existing caller
    test exercises it in isolation, and its own ``descending`` branch is
    the one place a subtle SQL mistake is easy to make: ``ORDER BY a, b
    DESC`` only reverses the last column, not the whole clause."""

    async def _table(self) -> tuple[Table, SqliteSource]:
        source = await SqliteSource.from_bytes(_build_table_bytes([("item-1", "Item One", "folder-a")]))
        table = await Table.create(source.connection, "items", _COLUMNS)
        return table, source

    async def test_ascending_is_unchanged(self) -> None:
        table, source = await self._table()
        try:
            assert _resolve_order_by(table, ["name"]) == "name, rowid"
        finally:
            await source.close()

    async def test_descending_applies_desc_to_every_column_not_just_the_last(self) -> None:
        table, source = await self._table()
        try:
            assert _resolve_order_by(table, ["name"], descending=True) == "name DESC, rowid DESC"
        finally:
            await source.close()

    async def test_a_missing_preferred_column_is_filtered_out_before_desc_is_applied(self) -> None:
        table, source = await self._table()
        try:
            assert _resolve_order_by(table, ["nonexistent", "name"], descending=True) == "name DESC, rowid DESC"
        finally:
            await source.close()

    async def test_dir_first_sql_wraps_the_resolved_clause_in_a_leading_case(self) -> None:
        table, source = await self._table()
        try:
            resolved = _resolve_order_by(table, ["name"], dir_first_sql="is_folder = 1")
            assert resolved == "(CASE WHEN is_folder = 1 THEN 0 ELSE 1 END), name, rowid"
        finally:
            await source.close()


_FOLDER_COLUMNS = [Column("folder_id"), Column("folder_name"), Column("parent_folder_id")]
_ITEM_COLUMNS = [Column("item_id"), Column("name"), Column("folder_id")]

# The synthetic root anchor -- no real folder row has this as its own
# folder_id, matching RecursiveTree's own root_id convention (see
# tree_strategy/recursive.py's RecursiveTree docstring).
_ROOT = ""


def _build_recursive_table_bytes(folders: list[tuple[str, str, str]], items: list[tuple[str, str, str]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE folders(folder_id TEXT PRIMARY KEY, folder_name TEXT, parent_folder_id TEXT)")
        conn.executemany("INSERT INTO folders VALUES (?, ?, ?)", folders)
        conn.execute("CREATE TABLE items(item_id TEXT PRIMARY KEY, name TEXT, folder_id TEXT)")
        conn.executemany("INSERT INTO items VALUES (?, ?, ?)", items)
        conn.commit()
        conn.close()
        return path.read_bytes()


async def _build_recursive_tree(
    folders: list[tuple[str, str, str]], items: list[tuple[str, str, str]], *, descending: bool = False
) -> tuple[RecursiveGroupFlatTree, SqliteSource]:
    source = await SqliteSource.from_bytes(_build_recursive_table_bytes(folders, items))
    tree = RecursiveGroupFlatTree(
        cast(SaasWorkloadProvider, _FakeProvider(source)),
        group_table="folders",
        group_columns=_FOLDER_COLUMNS,
        group_id_column="folder_id",
        group_name_column="folder_name",
        group_parent_column="parent_folder_id",
        group_root_id=_ROOT,
        leaf_table="items",
        leaf_columns=_ITEM_COLUMNS,
        leaf_id_column="item_id",
        leaf_group_column="folder_id",
        display_name=lambda row: str(row["name"]),
        order_by=["name"],
        descending=descending,
    )
    return tree, source


# inbox/sent at root; haha nested under inbox (inbox itself has no direct
# mail); mail directly under sent, and under the nested haha.
_HIERARCHY_FOLDERS = [("inbox", "Inbox", _ROOT), ("sent", "Sent", _ROOT), ("haha", "haha", "inbox")]
_HIERARCHY_ITEMS = [
    ("mail-1", "Under Sent", "sent"),
    ("mail-2", "Aaa Under Haha", "haha"),
    ("mail-3", "Zzz Under Haha", "haha"),
]


class TestRecursiveGroupFlatTree:
    """Mail's real folder hierarchy shape -- a real, named, recursive
    group table (folders, always shown even with zero direct leaves) with
    a separate flat leaf table."""

    async def test_root_lists_real_folders_even_with_no_direct_mail(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            # inbox has no direct mail at all -- only its own subfolder
            # haha does -- yet it must still appear at the root.
            groups = await tree.children_of(())
            assert [name for _key, name, _leaf in groups] == ["Inbox", "Sent"]
        finally:
            await source.close()

    async def test_folder_with_no_direct_mail_still_shows_its_subfolder(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            entries = await tree.children_of(("inbox",))
            assert entries == [(("inbox", "haha"), "haha", False)]
        finally:
            await source.close()

    async def test_nested_subfolder_lists_its_own_mail(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            entries = await tree.children_of(("inbox", "haha"))
            assert [(key, name) for key, name, _leaf in entries] == [
                (("inbox", "haha", "mail-2"), "Aaa Under Haha"),
                (("inbox", "haha", "mail-3"), "Zzz Under Haha"),
            ]
        finally:
            await source.close()

    async def test_folder_with_direct_mail_lists_it(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            entries = await tree.children_of(("sent",))
            assert entries == [(("sent", "mail-1"), "Under Sent", True)]
        finally:
            await source.close()

    async def test_leaves_are_ordered_newest_first_when_descending_is_set(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS, descending=True)
        try:
            entries = await tree.children_of(("inbox", "haha"))
            assert [name for _key, name, _leaf in entries] == ["Zzz Under Haha", "Aaa Under Haha"]
        finally:
            await source.close()

    async def test_a_leaf_key_has_no_children_of_its_own(self) -> None:
        # No explicit key-shape guard is needed: a mail_id never appears
        # as any row's parent_folder_id in either table.
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            assert await tree.children_of(("sent", "mail-1")) == []
        finally:
            await source.close()

    async def test_row_for_is_none_for_a_top_level_folder_key(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            await tree.children_of(())
            assert tree.row_for(("inbox",)) is None
        finally:
            await source.close()

    async def test_row_for_is_none_for_a_nested_subfolder_key(self) -> None:
        # A 2-segment subfolder key must not be mistaken for a leaf key
        # of the same length.
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            await tree.children_of(("inbox",))
            assert tree.row_for(("inbox", "haha")) is None
        finally:
            await source.close()

    async def test_row_for_returns_the_real_row_for_a_leaf_key(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            await tree.children_of(("sent",))
            row = tree.row_for(("sent", "mail-1"))
            assert row is not None
            assert row["name"] == "Under Sent"
        finally:
            await source.close()

    async def test_children_of_issues_exactly_two_queries_regardless_of_row_counts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[object] = []
        original_select = Table.select

        def counting_select(
            self: Table,
            where: str = "",
            params: Sequence[object] = (),
            *,
            order_by: str | None = None,
            limit: int | None = None,
            offset: int = 0,
        ) -> AsyncIterator[dict[str, object | None]]:
            calls.append(1)
            return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

        monkeypatch.setattr(Table, "select", counting_select)

        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            calls.clear()
            await tree.children_of(("inbox",))
            assert len(calls) == 2
        finally:
            await source.close()


# A folder with 2 subfolders and 3 direct leaves -- enough to exercise
# every side of the folder/leaf pagination boundary.
_BOUNDARY_FOLDERS = [("mixed", "Mixed", _ROOT), ("mixed-a", "A", "mixed"), ("mixed-b", "B", "mixed")]
_BOUNDARY_ITEMS = [("leaf-1", "L1", "mixed"), ("leaf-2", "L2", "mixed"), ("leaf-3", "L3", "mixed")]


class TestRecursiveGroupFlatTreePagination:
    """Hand-verified against the exact merge arithmetic
    ``RecursiveGroupFlatTree.children_of`` uses: 2 subfolders ("A", "B")
    then 3 leaves ("L1", "L2", "L3"), as one virtual concatenated
    sequence — offset/limit must slice across that boundary correctly."""

    async def test_page_starting_inside_the_folder_range(self) -> None:
        tree, source = await _build_recursive_tree(_BOUNDARY_FOLDERS, _BOUNDARY_ITEMS)
        try:
            entries = await tree.children_of(("mixed",), offset=1, limit=3)
            assert [name for _key, name, _leaf in entries] == ["B", "L1", "L2"]
        finally:
            await source.close()

    async def test_page_starting_exactly_at_the_boundary(self) -> None:
        tree, source = await _build_recursive_tree(_BOUNDARY_FOLDERS, _BOUNDARY_ITEMS)
        try:
            entries = await tree.children_of(("mixed",), offset=2, limit=2)
            assert [name for _key, name, _leaf in entries] == ["L1", "L2"]
        finally:
            await source.close()

    async def test_page_starting_past_all_folders(self) -> None:
        tree, source = await _build_recursive_tree(_BOUNDARY_FOLDERS, _BOUNDARY_ITEMS)
        try:
            entries = await tree.children_of(("mixed",), offset=3, limit=2)
            assert [name for _key, name, _leaf in entries] == ["L2", "L3"]
        finally:
            await source.close()

    async def test_page_that_exactly_fills_from_folders_alone_skips_the_leaf_query(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[object] = []
        original_select = Table.select

        def counting_select(
            self: Table,
            where: str = "",
            params: Sequence[object] = (),
            *,
            order_by: str | None = None,
            limit: int | None = None,
            offset: int = 0,
        ) -> AsyncIterator[dict[str, object | None]]:
            calls.append(1)
            return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

        monkeypatch.setattr(Table, "select", counting_select)

        tree, source = await _build_recursive_tree(_BOUNDARY_FOLDERS, _BOUNDARY_ITEMS)
        try:
            calls.clear()
            entries = await tree.children_of(("mixed",), offset=0, limit=2)
            assert [name for _key, name, _leaf in entries] == ["A", "B"]
            # Only the subfolder scan ran -- a full page came entirely
            # from folders, so the leaf query is never issued at all.
            assert len(calls) == 1
        finally:
            await source.close()


__all__: list[str] = []
