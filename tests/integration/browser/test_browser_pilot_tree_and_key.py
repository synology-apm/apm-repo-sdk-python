"""``textual`` ``Pilot``-driven coverage against this file's own dedicated
``tui_tree_and_key_apv1_pilot.json.gz`` (recorded via each test's own
``record_target()`` call — see ``tests/conftest.py`` and
``tests/CLAUDE.md``'s "Recording a fixture" section): the second-level
tree expand double-toggle guard and stale-tree-node-bookkeeping purge,
with **no real ``samples_dir`` dependency**.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from textual.widgets import DataTable, Input, Tree
from textual.widgets.tree import TreeNode

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen, CatalogEntry
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Workload
from synology_apm_repo.sdk.storage.base import ObjectStore


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[..., Awaitable[ObjectStore]]
) -> None:
    store = await record_target("tui_tree_and_key_apv1_pilot.json.gz", allow_content=True)

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, "apv-sample-1"

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


def _select_matching_connection(tree: Tree[object], connection_config_id: int) -> None:
    for repo_node in tree.root.children:
        for connection_node in repo_node.children:
            data = connection_node.data
            if isinstance(data, CatalogEntry) and data.catalog.connection.connection_config_id == connection_config_id:
                tree.move_cursor(connection_node)
                return
    raise AssertionError(f"no connection with connection_config_id {connection_config_id!r} found")


def _select_matching_workload(tree: Tree[object], type_hint: str) -> None:
    def _find_path(node: TreeNode[object], path: list[TreeNode[object]]) -> list[TreeNode[object]] | None:
        for child in node.children:
            data = child.data
            if isinstance(data, Workload) and data.type_hint == type_hint:
                return [*path, child]
            found = _find_path(child, [*path, child])
            if found is not None:
                return found
        return None

    path = _find_path(tree.root, [])
    if path is None:
        raise AssertionError(f"no workload with type_hint {type_hint!r} found")
    for ancestor in path[:-1]:
        if not ancestor.is_expanded:
            ancestor.expand()
    _ = tree._tree_lines
    tree.move_cursor(path[-1])


async def _drill_to_a_real_mail_units_screen(
    app: ApmRepoBrowserApp, pilot: Any, wait_until: Any, *, connection_config_id: int
) -> None:
    assert isinstance(app.screen, BrowseScreen), app.screen
    cat_tree = app.screen.query_one("#col-catalogs", Tree)
    _select_matching_connection(cat_tree, connection_config_id)
    cat_tree.focus()
    await pilot.press("enter")

    wl_tree = app.screen.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: any(wl_tree.root.children), timeout=3.0, interval=0.03)
    _select_matching_workload(wl_tree, "MAIL")
    wl_tree.focus()
    await pilot.press("enter")

    ver_table = app.screen.query_one("#col-versions", DataTable)
    await wait_until(pilot, lambda: ver_table.row_count, timeout=3.0, interval=0.03)
    ver_table.focus()
    await pilot.press("enter")

    await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=0.6, interval=0.03)
    assert isinstance(app.screen, UnitScreen), app.screen


async def _first_non_leaf_root_child(
    unit_screen: UnitScreen, pilot: Any, wait_until: Any, focus_widget: Any
) -> TreeNode[Any]:
    tree = unit_screen.query_one("#unit-tree", Tree)
    await wait_until(pilot, lambda: tree.root.children, timeout=3.0, interval=0.03)
    await focus_widget(pilot, tree)
    folder = next((n for n in tree.root.children if n.data is not None and not n.data.is_leaf), None)
    assert folder is not None, "Mail root has no non-leaf folder child to expand"
    return folder


def test_l_expands_a_second_level_tree_node_and_it_stays_expanded_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    move_cursor_to: Any,
    focus_widget: Any,
) -> None:
    async def scenario() -> tuple[bool, int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_a_real_mail_units_screen(app, pilot, wait_until, connection_config_id=3)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#unit-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot, wait_until, focus_widget)

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("l")
            await wait_until(pilot, lambda: folder.children, timeout=3.0, interval=0.03)
            return folder.is_expanded, len(folder.children)

    is_expanded, child_count = asyncio.run(scenario())
    assert is_expanded, "folder collapsed again after l — the double-toggle bug"
    assert child_count > 0, "l did not load the folder's messages"


def test_enter_expands_a_second_level_tree_node_and_it_stays_expanded_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    move_cursor_to: Any,
    focus_widget: Any,
) -> None:
    async def scenario() -> tuple[bool, int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_a_real_mail_units_screen(app, pilot, wait_until, connection_config_id=3)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#unit-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot, wait_until, focus_widget)

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("enter")
            await wait_until(pilot, lambda: folder.children, timeout=3.0, interval=0.03)
            return folder.is_expanded, len(folder.children)

    is_expanded, child_count = asyncio.run(scenario())
    assert is_expanded, "folder collapsed again after enter — the double-toggle bug"
    assert child_count > 0, "enter did not load the folder's messages"


def test_filtering_purges_stale_tree_node_bookkeeping_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    move_cursor_to: Any,
    focus_widget: Any,
) -> None:
    async def scenario() -> tuple[bool, bool, int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_a_real_mail_units_screen(app, pilot, wait_until, connection_config_id=3)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#unit-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot, wait_until, focus_widget)

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("l")
            await wait_until(pilot, lambda: folder.children, timeout=3.0, interval=0.03)
            assert folder.children, "setup: folder never loaded its messages"
            old_folder_id = id(folder)
            assert old_folder_id in unit_screen._loaded_tree_node_ids

            needle = str(folder.label)[:3]
            await move_cursor_to(pilot, tree, tree.root)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#filter-input"),
                timeout=0.4,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = unit_screen.query_one("#filter-input", Input)
            filter_input.value = needle
            # Filtering rebuilds the root's children, so the readiness signal
            # is the tree no longer holding the pre-filter node objects.
            await wait_until(
                pilot,
                lambda: folder not in tree.root.children,
                timeout=0.4,
                interval=0.02,
                message="filter never narrowed the tree",
            )
            await pilot.press("escape")
            await wait_until(
                pilot,
                lambda: not unit_screen.query("#filter-input.active"),
                timeout=0.4,
                interval=0.02,
                message="filter never closed",
            )

            old_id_still_marked_loaded = old_folder_id in unit_screen._loaded_tree_node_ids

            new_folder = next((n for n in tree.root.children if n.data is not None and not n.data.is_leaf), None)
            assert new_folder is not None, "folder missing after filter restore"
            assert new_folder is not folder, "test invariant: filtering must produce a new TreeNode object"

            await move_cursor_to(pilot, tree, new_folder)
            await pilot.press("l")
            await wait_until(
                pilot, lambda: new_folder.children or not new_folder.allow_expand, timeout=3.0, interval=0.03
            )
            return old_id_still_marked_loaded, new_folder.is_expanded, len(new_folder.children)

    old_id_still_marked_loaded, is_expanded, child_count = asyncio.run(scenario())
    assert not old_id_still_marked_loaded, "stale TreeNode id was not purged from _loaded_tree_node_ids"
    assert is_expanded and child_count > 0, "the post-filter folder node failed to (re)load its real children"


__all__: list[str] = []
