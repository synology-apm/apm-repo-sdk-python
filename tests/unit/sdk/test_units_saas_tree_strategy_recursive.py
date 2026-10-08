"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy``'s
``RecursiveTree`` (Drive's shape): parent-pointer recursion over one table,
over a small real SQLite database."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from synology_apm_repo.sdk.storage.table import Column
from synology_apm_repo.sdk.units.saas.tree_strategy import FolderPredicate, RecursiveTree, TreeEntry
from unit.sdk.tree_strategy_fakes import FakeTableProvider, TableSpec, table_provider

_ROOT = "root-folder"
_TYPE_FOLDER = 1
# The root folder has no row of its own. "Shared" holds a subfolder and a
# file; the subfolder holds one more file.
_ITEMS: TableSpec = (
    "item_id TEXT PRIMARY KEY, name TEXT, parent_folder_id TEXT, type INTEGER",
    [
        ("f-notes", "notes.txt", _ROOT, 0),
        ("d-shared", "Shared", _ROOT, _TYPE_FOLDER),
        ("f-budget", "budget.xlsx", _ROOT, 0),
        ("d-2026", "2026", "d-shared", _TYPE_FOLDER),
        ("f-plan", "plan.docx", "d-shared", 0),
        ("f-q1", "q1.pdf", "d-2026", 0),
    ],
)


@pytest.fixture
async def provider() -> AsyncIterator[FakeTableProvider]:
    async with table_provider({"items": _ITEMS}) as p:
        yield p


def _tree(provider: FakeTableProvider, *, order_by: tuple[str, ...] = ("name",)) -> RecursiveTree:
    return RecursiveTree(
        provider,
        table="items",
        columns=[Column("item_id"), Column("name"), Column("parent_folder_id"), Column("type")],
        id_column="item_id",
        parent_column="parent_folder_id",
        root_id=_ROOT,
        folder=FolderPredicate(is_folder=lambda row: row["type"] == _TYPE_FOLDER, sql=f"type = {_TYPE_FOLDER}"),
        display_name=lambda row: str(row["name"]),
        order_by=order_by,
    )


def _shape(entries: list[TreeEntry]) -> list[tuple[tuple[str, ...], str, bool]]:
    return [(entry.key, entry.name, entry.is_leaf) for entry in entries]


class TestChildrenOf:
    async def test_the_root_lists_the_root_ids_children_folders_first(self, provider: FakeTableProvider) -> None:
        assert _shape(await _tree(provider).children_of(())) == [
            (("d-shared",), "Shared", False),
            (("f-budget",), "budget.xlsx", True),
            (("f-notes",), "notes.txt", True),
        ]

    async def test_a_nested_folders_children_are_keyed_by_their_own_id_alone(self, provider: FakeTableProvider) -> None:
        tree = _tree(provider)
        assert _shape(await tree.children_of(("d-shared",))) == [
            (("d-2026",), "2026", False),
            (("f-plan",), "plan.docx", True),
        ]
        assert _shape(await tree.children_of(("d-2026",))) == [(("f-q1",), "q1.pdf", True)]

    async def test_folders_and_leaves_both_carry_their_row(self, provider: FakeTableProvider) -> None:
        entries = await _tree(provider).children_of(())
        assert [entry.row["item_id"] for entry in entries if entry.row is not None] == [
            "d-shared",
            "f-budget",
            "f-notes",
        ]

    async def test_a_leaf_lists_nothing(self, provider: FakeTableProvider) -> None:
        assert await _tree(provider).children_of(("f-notes",)) == []

    async def test_an_order_by_column_the_table_lacks_is_skipped(self, provider: FakeTableProvider) -> None:
        # Within the leaves, rowid then orders: notes.txt was inserted first.
        tree = _tree(provider, order_by=("no_such_column",))
        assert [entry.name for entry in await tree.children_of(())] == ["Shared", "notes.txt", "budget.xlsx"]

    @pytest.mark.parametrize(
        ("offset", "limit", "expected"),
        [
            pytest.param(1, None, ["budget.xlsx", "notes.txt"], id="offset_only"),
            pytest.param(0, 1, ["Shared"], id="limit_only"),
            pytest.param(2, 5, ["notes.txt"], id="limit_past_the_end"),
        ],
    )
    async def test_pages_the_children(
        self, provider: FakeTableProvider, offset: int, limit: int | None, expected: list[str]
    ) -> None:
        entries = await _tree(provider).children_of((), offset=offset, limit=limit)
        assert [entry.name for entry in entries] == expected

    async def test_the_table_is_created_once(self, provider: FakeTableProvider) -> None:
        tree = _tree(provider)
        await tree.children_of(())
        await tree.children_of(("d-shared",))
        await tree.resolve_id("f-plan")
        assert provider.requested == ["items"]


class TestResolveId:
    async def test_a_nested_leaf_resolves_to_its_entry(self, provider: FakeTableProvider) -> None:
        entry = await _tree(provider).resolve_id("f-q1")
        assert entry is not None
        assert (entry.key, entry.name, entry.is_leaf) == (("f-q1",), "q1.pdf", True)
        assert entry.row is not None and entry.row["parent_folder_id"] == "d-2026"

    async def test_a_folder_resolves_as_a_non_leaf(self, provider: FakeTableProvider) -> None:
        entry = await _tree(provider).resolve_id("d-shared")
        assert entry is not None
        assert (entry.key, entry.is_leaf) == (("d-shared",), False)

    @pytest.mark.parametrize("item_id", [pytest.param("f-missing", id="unknown_id"), pytest.param(_ROOT, id="root_id")])
    async def test_an_id_with_no_row_resolves_to_none(self, provider: FakeTableProvider, item_id: str) -> None:
        assert await _tree(provider).resolve_id(item_id) is None


class TestParentIdOf:
    async def test_a_top_level_items_parent_is_none(self, provider: FakeTableProvider) -> None:
        tree = _tree(provider)
        entry = await tree.resolve_id("d-shared")
        assert entry is not None and entry.row is not None
        assert tree.parent_id_of(entry.row) is None

    async def test_a_nested_items_parent_is_its_folders_id(self, provider: FakeTableProvider) -> None:
        tree = _tree(provider)
        entry = await tree.resolve_id("f-q1")
        assert entry is not None and entry.row is not None
        assert tree.parent_id_of(entry.row) == "d-2026"

    async def test_a_non_string_parent_value_is_returned_as_a_string(self, provider: FakeTableProvider) -> None:
        assert _tree(provider).parent_id_of({"parent_folder_id": 42}) == "42"
