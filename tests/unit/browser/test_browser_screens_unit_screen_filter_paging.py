"""UnitScreen load-more paging and the per-level filter."""

from __future__ import annotations

import asyncio

import pytest
from textual.widgets import Input, Tree

from support.model_factories import make_version
from support.pilot import SDK_TIMEOUT, move_cursor_to, settle, wait_for_filter_closed, wait_until
from synology_apm_repo.browser.core.unit.update import CHILDREN_PAGE_SIZE
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    ConfigurableProvider,
    FakeApp,
    FakeRepo,
    leaf_node,
)


def _filter_text(screen: UnitScreen) -> str | None:
    state = screen.store.model.filter
    return state.text if state is not None else None


async def test_load_more_error_notifies_and_load_more_with_active_filter_rerenders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A load-more ``children()`` failure notifies; with a filter active, a
    successful load-more re-renders through the filter."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    bulk = [leaf_node(f"bulk-{i}", f"bulk-{i}") for i in range(CHILDREN_PAGE_SIZE + 1)]
    provider = ConfigurableProvider(root, {str(root_ref): bulk})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: len(screen._file_table._nodes) == CHILDREN_PAGE_SIZE)
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        provider._raise_children_for = {str(root_ref)}
        screen.action_load_more()
        await wait_until(pilot, lambda: bool(warnings))
        assert f"boom at {root_ref}" in warnings[0]

        # "bulk-500", the one item this load adds, is the only filter match.
        provider._raise_children_for = set()
        screen.action_filter()
        await wait_until(pilot, lambda: screen.store.model.filter is not None)
        screen._filter.pending_text = "bulk-500"
        screen._filter._commit()
        await wait_until(pilot, lambda: _filter_text(screen) == "bulk-500")
        warnings.clear()
        screen.action_load_more()
        await wait_until(pilot, lambda: bool(warnings))  # load-more's own "loaded" notice
        assert [n.name for n in screen._file_table._nodes if n is not None] == ["bulk-500"]


async def test_filtering_a_level_preserves_a_surviving_childs_own_widget_and_does_not_refetch_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``reconcile_children`` keeps a surviving child's ``TreeNode`` across a
    filter re-render, so the folder's ``children()`` runs once."""
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    other = leaf_node("other.bin", "other")
    inner = leaf_node("inner.bin", "inner")
    provider = ConfigurableProvider(root, {str(root_ref): [folder, other], str(folder_ref): [inner]})
    children_calls: list[str] = []
    real_children = provider.children

    async def _counting_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        children_calls.append(str(node.ref))
        return await real_children(node, offset=offset, limit=limit)

    monkeypatch.setattr(provider, "children", _counting_children)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        children_calls.clear()  # drop the root's auto-expand fetch

        folder_node = next(n for n in tree.root.children if n.data is not None and n.data.key == folder_ref)
        await move_cursor_to(pilot, tree, folder_node)
        await pilot.press("enter")  # selects+expands folder -- also sets model.selected = folder_ref
        await wait_until(pilot, lambda: folder_ref in screen.store.model.loaded, timeout=SDK_TIMEOUT)
        assert children_calls == [str(folder_ref)]

        # Re-select root by dispatch: enter on the expanded root would collapse it.
        screen._select_folder_ref(root)
        screen.action_filter()
        await wait_until(pilot, lambda: screen.store.model.filter is not None)
        screen._filter.pending_text = "folder"
        screen._filter._commit()
        await wait_until(pilot, lambda: _filter_text(screen) == "folder")

        new_folder_node = next(n for n in tree.root.children if n.data is not None and n.data.key == folder_ref)
        assert new_folder_node is folder_node, "the reconciler must keep a surviving child's own TreeNode"
        screen._close_filter()

        # _close_filter() leaves focus on #filter-input, so refocus the tree.
        tree.focus()
        await wait_until(pilot, lambda: tree.has_focus)
        await move_cursor_to(pilot, tree, new_folder_node)
        await pilot.press("enter")
        await wait_until(pilot, lambda: screen.store.model.selected == folder_ref)
        assert [n.name for n in screen._file_table._nodes if n is not None] == ["inner.bin"]
        assert children_calls == [str(folder_ref)], "a filter round-trip must not re-fetch an already-loaded level"


async def test_action_filter_is_a_no_op_for_an_unloaded_level(monkeypatch: pytest.MonkeyPatch) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {str(root_ref): []})
    gate = asyncio.Event()
    real_children = provider.children

    async def _blocked_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        await gate.wait()
        return await real_children(node, offset=offset, limit=limit)

    monkeypatch.setattr(provider, "children", _blocked_children)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        assert root_ref not in screen.store.model.loaded

        screen.action_filter()
        await settle(pilot)
        assert not screen.query_one("#filter-input", Input).has_class("active")

        gate.set()  # let the level load so unmount has nothing stuck
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)


async def test_enter_on_the_filter_input_closes_it() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen.action_filter()
        await wait_until(pilot, lambda: screen.query_one("#filter-input", Input).has_class("active"))

        await pilot.press("enter")
        await wait_for_filter_closed(pilot, screen)
