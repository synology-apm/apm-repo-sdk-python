"""UnitScreen navigation between the folder tree and the file table: selection syncing, the loading spinner's anchor, and which node actions apply to."""

from __future__ import annotations

import asyncio

import pytest
from textual.coordinate import Coordinate
from textual.widgets import Tree

from support.fakes import faithful_to
from support.model_factories import make_version
from support.pilot import move_cursor_to, wait_until
from synology_apm_repo.browser.screens.unit_file_table import FileTable
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.units.base import Node, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    ConfigurableProvider,
    FakeApp,
    FakeRepo,
    leaf_node,
)


async def test_selecting_a_subfolder_row_in_the_file_table_syncs_the_folder_tree() -> None:
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [folder, leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        table.cursor_coordinate = Coordinate(0, 0)  # the folder row
        await pilot.press("enter")

        await wait_until(pilot, lambda: screen.store.model.selected == folder_ref)
        tree_node = screen.unit_tree.cursor_node
        assert tree_node is not None and tree_node.data is not None
        assert tree_node.data.key == folder_ref
        assert tree_node.is_expanded


async def test_selecting_a_file_table_row_syncs_the_tree_cursor_even_through_a_collapsed_ancestor() -> None:
    """Activating a file-table row beneath a collapsed ancestor re-expands
    the ancestors so the tree cursor follows (``move_cursor_keyed`` no-ops
    otherwise)."""
    root_ref = NodeRef("repo", ("root",))
    b_ref = NodeRef("repo", ("root", "b"))
    c_ref = NodeRef("repo", ("root", "b", "c"))
    d_ref = NodeRef("repo", ("root", "b", "c", "d"))
    e_ref = NodeRef("repo", ("root", "b", "c", "e"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    b = Node(ref=b_ref, name="b", is_leaf=False)
    c = Node(ref=c_ref, name="c", is_leaf=False)
    d = Node(ref=d_ref, name="d", is_leaf=False)
    e = Node(ref=e_ref, name="e", is_leaf=False)
    provider = ConfigurableProvider(
        root, {str(root_ref): [b], str(b_ref): [c], str(c_ref): [d, e], str(d_ref): [], str(e_ref): []}
    )
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = screen.unit_tree
        await wait_until(pilot, lambda: len(tree.root.children) > 0)

        b_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, b_node)
        await pilot.press("enter")  # selects+expands b -- c appears
        await wait_until(pilot, lambda: b_ref in screen.store.model.loaded)

        c_node = next(n for n in b_node.children if n.data is not None and n.data.key == c_ref)
        await move_cursor_to(pilot, tree, c_node)
        await pilot.press("enter")  # selects+expands c -- d/e appear in both tree and file table
        await wait_until(pilot, lambda: c_ref in screen.store.model.loaded)

        # Collapse b, an ancestor of c, leaving model.selected untouched.
        b_node.collapse()
        await wait_until(pilot, lambda: not b_node.is_expanded)

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        e_row = next(i for i, n in enumerate(screen._file_table._nodes) if n is not None and n.ref == e_ref)
        table.cursor_coordinate = Coordinate(e_row, 0)
        await pilot.press("enter")

        await wait_until(pilot, lambda: screen.store.model.selected == e_ref)
        assert b_node.is_expanded, "an ancestor above the target must be re-expanded too, not just the target itself"
        tree_node = tree.cursor_node
        assert tree_node is not None and tree_node.data is not None
        assert tree_node.data.key == e_ref, "the tree cursor must actually follow the file-table selection"


async def test_first_time_enter_on_an_unloaded_folder_anchors_the_spinner_on_the_file_table() -> None:
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    gate = asyncio.Event()

    @faithful_to(UnitProvider)
    class _GatedProvider(ConfigurableProvider):
        async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
            if node.ref == folder_ref:
                await gate.wait()
            return await super().children(node, offset=offset, limit=limit)

    provider = _GatedProvider(root, {str(root_ref): [folder], str(folder_ref): []})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) >= 1)
        folder_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, folder_node)
        await pilot.press("enter")  # first-ever expand of this folder
        await wait_until(pilot, lambda: screen.store.model.selected == folder_ref)

        table = screen.file_table
        await wait_until(pilot, lambda: table.row_count > 0 and "Loading" in str(table.get_row_at(0)[0]))
        assert "Loading" not in str(folder_node.label), "the tree node itself must show no spinner suffix"

        gate.set()
        await wait_until(pilot, lambda: folder_ref in screen.store.model.loaded)


async def test_loading_a_sibling_folder_does_not_rebuild_the_currently_selected_file_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrelated sibling's fetch does not rebuild the selected folder's
    file table (``DataTable.clear()`` would reset its scroll position)."""
    root_ref = NodeRef("repo", ("root",))
    folder_a_ref = NodeRef("repo", ("root", "a"))
    folder_b_ref = NodeRef("repo", ("root", "b"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder_a = Node(ref=folder_a_ref, name="a", is_leaf=False)
    folder_b = Node(ref=folder_b_ref, name="b", is_leaf=False)
    provider = ConfigurableProvider(
        root, {str(root_ref): [folder_a, folder_b], str(folder_a_ref): [], str(folder_b_ref): []}
    )
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) >= 2)

        a_node, b_node = tree.root.children[0], tree.root.children[1]
        await move_cursor_to(pilot, tree, a_node)
        await pilot.press("enter")
        await wait_until(pilot, lambda: screen.store.model.selected == folder_a_ref)

        table = screen.file_table
        clear_calls = 0
        real_clear = table.clear

        def _counting_clear(columns: bool = False) -> FileTable:
            nonlocal clear_calls
            clear_calls += 1
            return real_clear(columns)

        monkeypatch.setattr(table, "clear", _counting_clear)

        await move_cursor_to(pilot, tree, b_node)
        await pilot.press("space")  # expand (not select) folder b -- a background fetch only
        await wait_until(pilot, lambda: folder_b_ref in screen.store.model.loaded)

        assert screen.store.model.selected == folder_a_ref
        assert clear_calls == 0, "an unrelated sibling's own fetch must not rebuild this table at all"


async def test_selected_node_reads_the_currently_focused_widget() -> None:
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [folder, leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        tree = screen.unit_tree
        tree.focus()
        await wait_until(pilot, lambda: tree.has_focus)
        tree_node = next(n for n in tree.root.children if n.data is not None and n.data.key == folder_ref)
        _ = tree._tree_lines  # rebuilds the line map, as support.pilot.move_cursor_to does
        tree.move_cursor(tree_node)
        await wait_until(pilot, lambda: tree.cursor_node is tree_node)
        assert screen._selected_node() is folder

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        row_index = screen._file_table._nodes.index(leaf)
        table.cursor_coordinate = Coordinate(row_index, 0)
        assert screen._selected_node() is leaf


async def test_selected_node_falls_back_to_the_detail_pane_when_focus_is_elsewhere() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        table.cursor_coordinate = Coordinate(screen._file_table._nodes.index(leaf), 0)
        await pilot.press("enter")  # fires on_data_table_row_selected -> _show_detail(leaf)
        assert screen._selected_node() is leaf  # sanity: the table itself still resolves it

        detail_scroll = screen.query_one("#detail-scroll")
        detail_scroll.focus()
        await wait_until(pilot, lambda: detail_scroll.has_focus)

        assert screen._selected_node() is leaf


async def test_refresh_clears_the_detail_panes_stale_fallback_node() -> None:
    """``RootRequested`` (refresh/verbose reload) clears the model's detail,
    so ``_selected_node()`` does not fall back to a node from the closed provider."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        table.cursor_coordinate = Coordinate(screen._file_table._nodes.index(leaf), 0)
        await pilot.press("enter")  # fires on_data_table_row_selected -> _show_detail(leaf)
        assert screen.store.model.detail is not None  # sanity: the model is tracking it
        assert screen.store.model.detail.node is leaf

        screen.action_refresh()
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        detail_scroll = screen.query_one("#detail-scroll")
        detail_scroll.focus()
        await wait_until(pilot, lambda: detail_scroll.has_focus)

        assert screen.store.model.detail is None
        assert screen._selected_node() is None
