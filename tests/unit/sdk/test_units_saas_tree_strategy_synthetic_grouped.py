"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy.synthetic_grouped``'s
``SyntheticGroupedTree``, over ``tree_strategy_fakes.FakeTableProvider`` and
a small real SQLite table, covering branches Mail/Contact never reach: the
raw-group-value fallback when no ``group_display_name`` is given, and
``children_of()`` on a malformed key below a leaf."""

from __future__ import annotations

from collections.abc import Callable

from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.storage.table import Column
from synology_apm_repo.sdk.units.saas.tree_strategy import SyntheticGroupedTree
from unit.sdk.tree_strategy_fakes import FakeTableProvider, tables_db_bytes

_COLUMNS = [Column("item_id"), Column("name"), Column("folder_id", required=False)]


def _build_table_bytes(rows: list[tuple[str, str, str | None]]) -> bytes:
    return tables_db_bytes({"items": ("item_id TEXT PRIMARY KEY, name TEXT, folder_id TEXT", rows)})


async def _build_tree(
    rows: list[tuple[str, str, str | None]],
    *,
    group_display_name: Callable[[str], str] | None = None,
    descending: bool = False,
) -> tuple[SyntheticGroupedTree, SqliteSource]:
    source = await SqliteSource.from_bytes(_build_table_bytes(rows))
    tree = SyntheticGroupedTree(
        FakeTableProvider(source),
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
    # A leaf's key is (group, item_id); a third segment comes only from a
    # malformed pasted ref.
    tree, source = await _build_tree([("item-1", "Item One", "folder-a")])
    try:
        assert await tree.children_of(("folder-a", "item-1", "extra")) == []
    finally:
        await source.close()


async def test_members_with_no_group_value_are_listed_under_the_root_group() -> None:
    # The root group's members are found by folder_id IS NULL, not by the
    # synthetic root name.
    tree, source = await _build_tree([("item-1", "Item One", None), ("item-2", "Item Two", "folder-a")])
    try:
        groups = await tree.children_of(())
        assert {name for _key, name, _leaf, _row in groups} == {"(no folder)", "folder-a"}
        root_members = await tree.children_of(("(no folder)",))
        assert [name for _key, name, _leaf, _row in root_members] == ["Item One"]
    finally:
        await source.close()


async def test_group_display_name_falls_back_to_the_raw_group_value_without_a_resolver() -> None:
    tree, source = await _build_tree([("item-1", "Item One", "folder-a")])
    try:
        groups = await tree.children_of(())
        [(_key, name, _leaf, _row)] = [g for g in groups if g[0] != ("(no folder)",)]
        assert name == "folder-a"
    finally:
        await source.close()


async def test_group_display_name_resolver_overrides_the_raw_group_value_when_given() -> None:
    tree, source = await _build_tree(
        [("item-1", "Item One", "folder-a")], group_display_name=lambda group: f"Pretty {group}"
    )
    try:
        groups = await tree.children_of(())
        [(_key, name, _leaf, _row)] = [g for g in groups if g[0] != ("(no folder)",)]
        assert name == "Pretty folder-a"
    finally:
        await source.close()


async def test_members_are_listed_newest_first_when_descending_is_set() -> None:
    tree, source = await _build_tree([("item-1", "Aaa", "folder-a"), ("item-2", "Zzz", "folder-a")], descending=True)
    try:
        members = await tree.children_of(("folder-a",))
        assert [name for _key, name, _leaf, _row in members] == ["Zzz", "Aaa"]
    finally:
        await source.close()
