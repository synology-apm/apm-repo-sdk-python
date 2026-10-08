"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy``'s
``NamedGroupRecursiveTree`` (Site's shape): named groups from their own
table, whose leaves recurse by parent pointer, over a small real SQLite
database."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable

import pytest

from synology_apm_repo.sdk.storage.table import Column
from synology_apm_repo.sdk.units.saas.tree_strategy import FolderPredicate, NamedGroupRecursiveTree, TreeEntry
from unit.sdk.tree_strategy_fakes import FakeTableProvider, TableSpec, table_provider

_LISTS: TableSpec = (
    "list_id TEXT PRIMARY KEY, list_title TEXT",
    [("list-docs", "Documents"), ("list-assets", "Assets")],
)
# Documents: two top-level files and a folder (parent ""), which holds a
# subfolder (itself holding a file) and a file. Assets' top level hangs off
# its own root id.
_ITEMS: TableSpec = (
    "item_id TEXT PRIMARY KEY, name TEXT, list_id TEXT, parent_folder_id TEXT, is_folder INTEGER",
    [
        ("doc-b", "b.docx", "list-docs", "", 0),
        ("doc-a", "a.docx", "list-docs", "", 0),
        ("dir-1", "Reports", "list-docs", "", 1),
        ("dir-2", "2026", "list-docs", "dir-1", 1),
        ("doc-c", "c.xlsx", "list-docs", "dir-1", 0),
        ("doc-d", "d.pdf", "list-docs", "dir-2", 0),
        ("logo", "logo.png", "list-assets", "assets-root", 0),
    ],
)
_FOLDER = FolderPredicate(is_folder=lambda row: row["is_folder"] == 1, sql="is_folder = 1")


@pytest.fixture
async def provider() -> AsyncIterator[FakeTableProvider]:
    async with table_provider({"lists": _LISTS, "items": _ITEMS}) as p:
        yield p


def _tree(
    provider: FakeTableProvider,
    *,
    order_by: tuple[str, ...] = ("name",),
    order_by_sql: str | None = None,
    root_folder_id_of: Callable[[str], str] | None = None,
) -> NamedGroupRecursiveTree:
    return NamedGroupRecursiveTree(
        provider,
        group_table="lists",
        group_columns=[Column("list_id"), Column("list_title")],
        group_id_column="list_id",
        group_name_column="list_title",
        leaf_table="items",
        leaf_columns=[
            Column("item_id"),
            Column("name"),
            Column("list_id"),
            Column("parent_folder_id"),
            Column("is_folder"),
        ],
        self_id_of=lambda row: str(row["item_id"]),
        leaf_group_column="list_id",
        leaf_parent_column="parent_folder_id",
        folder=_FOLDER,
        display_name=lambda row: str(row["name"]),
        order_by=order_by,
        root_folder_id_of=root_folder_id_of,
        order_by_sql=order_by_sql,
    )


def _shape(entries: list[TreeEntry]) -> list[tuple[tuple[str, ...], str, bool]]:
    return [(entry.key, entry.name, entry.is_leaf) for entry in entries]


async def test_the_root_lists_the_groups_by_name(provider: FakeTableProvider) -> None:
    assert await _tree(provider).children_of(()) == [
        TreeEntry(("list-assets",), "Assets", False),
        TreeEntry(("list-docs",), "Documents", False),
    ]


async def test_the_root_pages_the_groups(provider: FakeTableProvider) -> None:
    assert [entry.name for entry in await _tree(provider).children_of((), offset=1, limit=1)] == ["Documents"]


async def test_a_group_lists_its_top_level_items_folders_first(provider: FakeTableProvider) -> None:
    assert _shape(await _tree(provider).children_of(("list-docs",))) == [
        (("list-docs", "dir-1"), "Reports", False),
        (("list-docs", "doc-a"), "a.docx", True),
        (("list-docs", "doc-b"), "b.docx", True),
    ]


async def test_a_folder_lists_its_children_under_its_full_key_path(provider: FakeTableProvider) -> None:
    assert _shape(await _tree(provider).children_of(("list-docs", "dir-1"))) == [
        (("list-docs", "dir-1", "dir-2"), "2026", False),
        (("list-docs", "dir-1", "doc-c"), "c.xlsx", True),
    ]


async def test_a_nested_folder_is_looked_up_by_its_last_key_segment(provider: FakeTableProvider) -> None:
    assert _shape(await _tree(provider).children_of(("list-docs", "dir-1", "dir-2"))) == [
        (("list-docs", "dir-1", "dir-2", "doc-d"), "d.pdf", True),
    ]


async def test_folders_and_leaves_both_carry_their_row(provider: FakeTableProvider) -> None:
    entries = await _tree(provider).children_of(("list-docs",))
    assert [entry.row["item_id"] for entry in entries if entry.row is not None] == ["dir-1", "doc-a", "doc-b"]


async def test_the_default_root_folder_id_misses_a_group_rooted_elsewhere(provider: FakeTableProvider) -> None:
    assert await _tree(provider).children_of(("list-assets",)) == []


async def test_root_folder_id_of_gives_each_groups_top_level_parent(provider: FakeTableProvider) -> None:
    roots = {"list-assets": "assets-root"}
    tree = _tree(provider, root_folder_id_of=lambda list_id: roots.get(list_id, ""))
    assert _shape(await tree.children_of(("list-assets",))) == [(("list-assets", "logo"), "logo.png", True)]
    assert len(await tree.children_of(("list-docs",))) == 3


async def test_order_by_sql_is_used_verbatim_after_folders_first(provider: FakeTableProvider) -> None:
    tree = _tree(provider, order_by=("no_such_column",), order_by_sql="name DESC")
    assert [entry.name for entry in await tree.children_of(("list-docs",))] == ["Reports", "b.docx", "a.docx"]


async def test_an_order_by_column_the_table_lacks_is_skipped(provider: FakeTableProvider) -> None:
    # Within each folder/leaf group, rowid then orders: b.docx was inserted first.
    tree = _tree(provider, order_by=("no_such_column",))
    assert [entry.name for entry in await tree.children_of(("list-docs",))] == ["Reports", "b.docx", "a.docx"]


@pytest.mark.parametrize(
    ("offset", "limit", "expected"),
    [
        pytest.param(1, None, ["a.docx", "b.docx"], id="offset_only"),
        pytest.param(0, 1, ["Reports"], id="limit_only"),
        pytest.param(1, 1, ["a.docx"], id="offset_and_limit"),
    ],
)
async def test_a_group_pages_its_items(
    provider: FakeTableProvider, offset: int, limit: int | None, expected: list[str]
) -> None:
    entries = await _tree(provider).children_of(("list-docs",), offset=offset, limit=limit)
    assert [entry.name for entry in entries] == expected
