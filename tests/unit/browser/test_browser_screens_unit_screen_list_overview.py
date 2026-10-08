"""UnitScreen SharePoint List overview: selecting and activating a List group, and loading, failing and discarding its overview."""

from __future__ import annotations

import asyncio
import json

import pytest
from textual.coordinate import Coordinate
from textual.widgets import Static, Tree

from support.model_factories import make_version
from support.pilot import SDK_TIMEOUT, move_cursor_to, settle, wait_for_workers, wait_until
from synology_apm_repo.browser.core.unit.model import DETAIL_GROUP, DetailIdle
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.browser.view.reconcile import find_node
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.units.base import Node, NodeRole, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    ConfigurableProvider,
    FakeApp,
    FakeContentSource,
    FakeRepo,
    leaf_node,
)


def _list_overview_node() -> Node:
    return Node(ref=NodeRef("repo", ("root", "list")), name="MyList", is_leaf=False, role=NodeRole.LIST_OVERVIEW)


async def test_selecting_a_list_overview_group_does_not_change_model_selected() -> None:
    """A List-overview group has no file-table contents, so ``model.selected`` stays on the previous folder."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = ConfigurableProvider(root, {str(root_ref): [overview_node]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = screen.unit_tree
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        assert screen.store.model.selected == root_ref

        overview_tree_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, overview_tree_node)
        await pilot.press("enter")

        assert screen.store.model.selected == root_ref  # unchanged -- never the group's own ref
        detail = screen.query_one("#detail", Static)
        assert "MyList" in str(detail.render())  # the detail pane did update to the group


async def test_activating_a_list_overview_row_in_the_file_table_never_expands_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Activating a List-overview group's file-table row never expands its
    ``TreeNode`` (``expand()`` ignores ``allow_expand=False``), so no
    ``ChildrenRequested`` is dispatched for it."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = ConfigurableProvider(root, {str(root_ref): [overview_node]})
    children_calls: list[str] = []
    real_children = provider.children

    async def _counting_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        children_calls.append(str(node.ref))
        return await real_children(node, offset=offset, limit=limit)

    monkeypatch.setattr(provider, "children", _counting_children)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        children_calls.clear()  # drop root's auto-expand fetch

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        row = next(i for i, n in enumerate(screen._file_table._nodes) if n is overview_node)
        table.cursor_coordinate = Coordinate(row, 0)
        await pilot.press("enter")

        # One call: the overview fetch's own.
        await wait_until(pilot, lambda: children_calls.count(str(overview_node.ref)) == 1)
        assert overview_node.ref not in screen.store.model.loaded, (
            "a List-overview group's items must never be routed through the store's own "
            "ChildrenRequested/model.loaded path"
        )

        tree_node = find_node(screen.unit_tree.root, overview_node.ref)
        assert tree_node is not None
        assert not tree_node.is_expanded


async def test_list_overview_is_a_no_op_without_a_provider() -> None:
    app = FakeApp(make_version(), FakeRepo(None, provider_error=ApmRepoError("boom")))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: screen.store.model.root_error is not None)
        assert screen.store.model.provider is None
        screen._show_detail(_list_overview_node())
        header_only = str(screen.query_one("#detail", Static).render())
        await settle(pilot)
        # No fetch was started, so the pane stays on the header.
        assert isinstance(screen.store.model.detail.body, DetailIdle)  # type: ignore[union-attr]
        assert str(screen.query_one("#detail", Static).render()) == header_only


async def test_list_overview_children_error_shows_in_detail_pane() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = ConfigurableProvider(root, {str(root_ref): [overview_node]}, raise_children_for={str(overview_node.ref)})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        detail = screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "error:" in str(detail.render()))
        assert f"boom at {overview_node.ref}" in str(detail.render())


async def test_list_overview_error_for_a_node_the_user_moved_away_from_is_discarded() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = ConfigurableProvider(root, {str(root_ref): [overview_node]}, raise_children_for={str(overview_node.ref)})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        screen._show_detail(root)  # the user moves on before the worker resolves
        header_only = str(screen.query_one("#detail", Static).render())
        await wait_for_workers(pilot, group=DETAIL_GROUP)  # the abandoned fetch is over
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "error:" not in header_only


async def test_list_overview_success_for_a_node_the_user_moved_away_from_is_discarded() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = ConfigurableProvider(root, {str(root_ref): [overview_node]})  # empty children -> "(no items)"
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        screen._show_detail(root)  # the user moves on before the worker resolves
        header_only = str(screen.query_one("#detail", Static).render())
        await wait_for_workers(pilot, group=DETAIL_GROUP)  # the abandoned fetch is over
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "(no items)" not in header_only


async def test_list_overview_skips_nested_folders_and_malformed_items_and_reports_empty() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    nested_folder = Node(ref=NodeRef("repo", ("root", "list", "folder")), name="folder", is_leaf=False)
    malformed_item = leaf_node("bad", "bad")
    malformed_unit = RestorableUnit(
        ref=malformed_item.ref,
        name=malformed_item.name,
        is_leaf=True,
        content=FakeContentSource(b"not json"),  # type: ignore[arg-type]
    )
    provider = ConfigurableProvider(
        root,
        {str(root_ref): [overview_node], str(overview_node.ref): [nested_folder, malformed_item]},
        units_by_ref={str(malformed_item.ref): malformed_unit},
    )
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        detail = screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "(no items)" in str(detail.render()))
        assert "(no items)" in str(detail.render())  # the folder was skipped, the malformed item discarded


async def test_list_overview_fetches_every_items_own_content_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    """All items' ``unit()`` calls are in flight at once (``entered`` fires
    only then); a sequential fetch times out."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    items = [leaf_node(f"item-{i}", f"item-{i}") for i in range(3)]
    units_by_ref = {
        str(item.ref): RestorableUnit(
            ref=item.ref,
            name=item.name,
            is_leaf=True,
            content=FakeContentSource(json.dumps({"Title": item.name}).encode()),  # type: ignore[arg-type]
        )
        for item in items
    }
    provider = ConfigurableProvider(
        root, {str(root_ref): [overview_node], str(overview_node.ref): items}, units_by_ref=units_by_ref
    )
    real_unit = provider.unit
    entered = asyncio.Event()
    release = asyncio.Event()
    in_flight = 0

    async def _gated_unit(node: Node) -> RestorableUnit:
        nonlocal in_flight
        in_flight += 1
        if in_flight == len(items):
            entered.set()
        await release.wait()
        return await real_unit(node)

    monkeypatch.setattr(provider, "unit", _gated_unit)

    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        await wait_until(pilot, lambda: entered.is_set(), timeout=SDK_TIMEOUT, message="items never ran concurrently")
        release.set()

        detail = screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "item-0" in str(detail.render()))
        assert all(f"item-{i}" in str(detail.render()) for i in range(3))
