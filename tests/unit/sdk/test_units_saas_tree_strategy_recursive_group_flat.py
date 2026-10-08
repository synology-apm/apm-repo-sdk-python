"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy.recursive_group_flat``'s
``RecursiveGroupFlatTree`` (Mail's folder shape), over
``tree_strategy_fakes.FakeTableProvider`` and small real SQLite tables."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

import pytest

from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.storage.table import Column, Table
from synology_apm_repo.sdk.units.saas.tree_strategy import RecursiveGroupFlatTree
from unit.sdk.tree_strategy_fakes import FakeTableProvider, tables_db_bytes

_FOLDER_COLUMNS = [Column("folder_id"), Column("folder_name"), Column("parent_folder_id")]
_ITEM_COLUMNS = [Column("item_id"), Column("name"), Column("folder_id")]

# The parent value of top-level folders; no folder row has it as its id.
_ROOT = ""


def _build_recursive_table_bytes(folders: list[tuple[str, str, str]], items: list[tuple[str, str, str]]) -> bytes:
    return tables_db_bytes(
        {
            "folders": ("folder_id TEXT PRIMARY KEY, folder_name TEXT, parent_folder_id TEXT", folders),
            "items": ("item_id TEXT PRIMARY KEY, name TEXT, folder_id TEXT", items),
        }
    )


async def _build_recursive_tree(
    folders: list[tuple[str, str, str]], items: list[tuple[str, str, str]], *, descending: bool = False
) -> tuple[RecursiveGroupFlatTree, SqliteSource]:
    source = await SqliteSource.from_bytes(_build_recursive_table_bytes(folders, items))
    tree = RecursiveGroupFlatTree(
        FakeTableProvider(source),
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
    """Mail's folder shape: a named, recursive group table (folders shown
    even with no direct leaves) and a separate flat leaf table."""

    async def test_root_lists_real_folders_even_with_no_direct_mail(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            # inbox has no direct mail, only its subfolder haha does.
            groups = await tree.children_of(())
            assert [name for _key, name, _leaf, _row in groups] == ["Inbox", "Sent"]
        finally:
            await source.close()

    async def test_folder_with_no_direct_mail_still_shows_its_subfolder(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            entries = await tree.children_of(("inbox",))
            assert [entry[:3] for entry in entries] == [(("inbox", "haha"), "haha", False)]
        finally:
            await source.close()

    async def test_nested_subfolder_lists_its_own_mail(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            entries = await tree.children_of(("inbox", "haha"))
            assert [(key, name) for key, name, _leaf, _row in entries] == [
                (("inbox", "haha", "mail-2"), "Aaa Under Haha"),
                (("inbox", "haha", "mail-3"), "Zzz Under Haha"),
            ]
        finally:
            await source.close()

    async def test_folder_with_direct_mail_lists_it(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            entries = await tree.children_of(("sent",))
            assert [entry[:3] for entry in entries] == [(("sent", "mail-1"), "Under Sent", True)]
        finally:
            await source.close()

    async def test_leaves_are_ordered_newest_first_when_descending_is_set(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS, descending=True)
        try:
            entries = await tree.children_of(("inbox", "haha"))
            assert [name for _key, name, _leaf, _row in entries] == ["Zzz Under Haha", "Aaa Under Haha"]
        finally:
            await source.close()

    async def test_a_leaf_key_has_no_children_of_its_own(self) -> None:
        # A mail_id never appears as a parent_folder_id, so no key-shape
        # guard is needed.
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            assert await tree.children_of(("sent", "mail-1")) == []
        finally:
            await source.close()

    async def test_a_top_level_folder_entry_carries_no_row(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            assert [entry.row for entry in await tree.children_of(())] == [None, None]
        finally:
            await source.close()

    async def test_a_nested_subfolder_entry_carries_no_row(self) -> None:
        # A 2-segment subfolder key must not be mistaken for a leaf of the same length.
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            [subfolder] = await tree.children_of(("inbox",))
            assert (subfolder.is_leaf, subfolder.row) == (False, None)
        finally:
            await source.close()

    async def test_a_leaf_entry_carries_its_real_row(self) -> None:
        tree, source = await _build_recursive_tree(_HIERARCHY_FOLDERS, _HIERARCHY_ITEMS)
        try:
            [leaf] = await tree.children_of(("sent",))
            assert leaf.row is not None
            assert leaf.row["name"] == "Under Sent"
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


# A folder with 2 subfolders and 3 direct leaves.
_BOUNDARY_FOLDERS = [("mixed", "Mixed", _ROOT), ("mixed-a", "A", "mixed"), ("mixed-b", "B", "mixed")]
_BOUNDARY_ITEMS = [("leaf-1", "L1", "mixed"), ("leaf-2", "L2", "mixed"), ("leaf-3", "L3", "mixed")]


class TestRecursiveGroupFlatTreePagination:
    """``children_of`` pages over subfolders ("A", "B") then leaves ("L1",
    "L2", "L3") as one sequence; offset/limit slice across the boundary."""

    @pytest.mark.parametrize(
        ("offset", "limit", "expected"),
        [
            pytest.param(1, 3, ["B", "L1", "L2"], id="inside_the_folder_range"),
            pytest.param(2, 2, ["L1", "L2"], id="exactly_at_the_boundary"),
            pytest.param(3, 2, ["L2", "L3"], id="past_all_folders"),
        ],
    )
    async def test_page_starting(self, offset: int, limit: int, expected: list[str]) -> None:
        tree, source = await _build_recursive_tree(_BOUNDARY_FOLDERS, _BOUNDARY_ITEMS)
        try:
            entries = await tree.children_of(("mixed",), offset=offset, limit=limit)
            assert [name for _key, name, _leaf, _row in entries] == expected
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
            assert [name for _key, name, _leaf, _row in entries] == ["A", "B"]
            assert len(calls) == 1
        finally:
            await source.close()
