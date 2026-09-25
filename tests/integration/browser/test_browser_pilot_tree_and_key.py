"""``textual`` ``Pilot``-driven coverage against this file's own dedicated
``tui_tree_and_key_apv1_pilot.json.gz`` (recorded via each test's own
``record_target()`` call — see ``tests/conftest.py`` and
``tests/CLAUDE.md``'s "Recording a fixture" section): the second-level
tree expand double-toggle guard and a filter re-render's own
ref-keyed bookkeeping survival, with **no real ``samples_dir``
dependency**.
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
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Catalog, Workload
from synology_apm_repo.sdk.storage.base import ObjectStore


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[..., Awaitable[ObjectStore]]
) -> None:
    store = await record_target("tui_tree_and_key_apv1_pilot.json.gz", allow_content=True)

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, "apv-sample-1"

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


def _select_matching_connection(tree: Tree[Any], connection_config_id: int) -> None:
    _ = tree._tree_lines  # forces the line map to rebuild; see move_cursor_to's docstring
    for repo_node in tree.root.children:
        for connection_node in repo_node.children:
            data = connection_node.data
            payload = data.payload if data is not None else None
            if isinstance(payload, Catalog) and payload.connection.connection_config_id == connection_config_id:
                tree.move_cursor(connection_node)
                return
    raise AssertionError(f"no connection with connection_config_id {connection_config_id!r} found")


def _select_matching_workload(tree: Tree[Any], type_hint: str) -> None:
    def _find_path(node: TreeNode[Any], path: list[TreeNode[Any]]) -> list[TreeNode[Any]] | None:
        for child in node.children:
            data = child.data
            payload = data.payload if data is not None else None
            if isinstance(payload, Workload) and payload.type_hint == type_hint:
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
    app: ApmRepoBrowserApp,
    pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    ui_timeout: float,
    sdk_timeout: float,
    *,
    connection_config_id: int,
) -> None:
    assert isinstance(app.screen, BrowseScreen), app.screen
    cat_tree = app.screen.query_one("#col-catalogs", Tree)
    _select_matching_connection(cat_tree, connection_config_id)
    await focus_widget(pilot, cat_tree)
    await pilot.press("enter")

    wl_tree = app.screen.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: any(wl_tree.root.children), timeout=sdk_timeout, interval=0.03)
    _select_matching_workload(wl_tree, "MAIL")
    await focus_widget(pilot, wl_tree)
    await pilot.press("enter")

    ver_table = app.screen.query_one("#col-versions", DataTable)
    # row_count alone can be stale; wait for _visible_version_indices too
    # (see tests/conftest.py's drill_to_unit_screen_via_fs_device).
    await wait_until(
        pilot,
        lambda: ver_table.row_count and app.screen._visible_version_indices,
        timeout=sdk_timeout,
        interval=0.03,
    )
    await focus_widget(pilot, ver_table)
    await pilot.press("enter")

    await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=ui_timeout, interval=0.03)
    assert isinstance(app.screen, UnitScreen), app.screen


async def _first_non_leaf_root_child(
    unit_screen: UnitScreen, pilot: Any, wait_until: Any, focus_widget: Any, sdk_timeout: float
) -> TreeNode[Any]:
    """The folder tree only ever shows containers, so every one of root's
    own tree children already satisfies "non-leaf" -- the name is kept
    (rather than renamed to ``_first_root_child``) since this helper's own
    callers still care specifically that it's a folder whose own children
    (the real mail messages) can be loaded and inspected."""
    tree = unit_screen.query_one("#folder-tree", Tree)
    await wait_until(pilot, lambda: tree.root.children, timeout=sdk_timeout, interval=0.03)
    await focus_widget(pilot, tree)
    folder = next((n for n in tree.root.children if n.data is not None and not n.data.payload.is_leaf), None)
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
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[bool, int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_a_real_mail_units_screen(
                app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout, connection_config_id=3
            )
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot, wait_until, focus_widget, sdk_timeout)
            assert folder.data is not None
            folder_ref = folder.data.key

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("l")
            await wait_until(
                pilot, lambda: folder_ref in unit_screen.store.model.loaded, timeout=sdk_timeout, interval=0.03
            )
            # The folder's own real messages are ordinary leaves, so they
            # list in the file table, never the folder tree (which
            # excludes leaves entirely).
            return folder.is_expanded, len(unit_screen.store.model.loaded[folder_ref].children)

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
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[bool, int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_a_real_mail_units_screen(
                app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout, connection_config_id=3
            )
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot, wait_until, focus_widget, sdk_timeout)
            assert folder.data is not None
            folder_ref = folder.data.key

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("enter")
            await wait_until(
                pilot, lambda: folder_ref in unit_screen.store.model.loaded, timeout=sdk_timeout, interval=0.03
            )
            return folder.is_expanded, len(unit_screen.store.model.loaded[folder_ref].children)

    is_expanded, child_count = asyncio.run(scenario())
    assert is_expanded, "folder collapsed again after enter — the double-toggle bug"
    assert child_count > 0, "enter did not load the folder's messages"


def test_filtering_preserves_a_surviving_folders_own_widget_and_its_loaded_messages_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    move_cursor_to: Any,
    focus_widget: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    """The keyed reconciler (``view/reconcile.py``'s ``reconcile_children``)
    keeps a still-matching child's own ``TreeNode`` object across a
    filter re-render instead of destroying and recreating it -- unlike
    the pre-reconciler ``id(TreeNode)``-keyed bookkeeping this replaced
    (see ``tests/unit/browser/test_browser_unit_screen_gaps.py``'s own
    sibling test for the synthetic-data version of this same proof), so
    a folder already expanded and loaded before a ``/`` filter keystroke
    stays expanded with its own already-fetched messages, without any
    re-fetch through the provider -- proven here against real recorded
    Mail data end to end, not just a fake provider."""

    async def scenario() -> tuple[bool, bool, int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_a_real_mail_units_screen(
                app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout, connection_config_id=3
            )
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot, wait_until, focus_widget, sdk_timeout)
            assert folder.data is not None
            folder_ref = folder.data.key
            assert tree.root.data is not None

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("l")
            await wait_until(
                pilot, lambda: folder_ref in unit_screen.store.model.loaded, timeout=sdk_timeout, interval=0.03
            )
            loaded_before = folder_ref in unit_screen.store.model.loaded
            # The folder's own real messages are ordinary leaves, so they
            # list in the file table, never the folder tree.
            child_count_before = len(unit_screen.store.model.loaded[folder_ref].children)

            needle = str(folder.label)[:3]
            # Re-select root before filtering -- the "l" press above moved
            # model.selected to folder_ref, and action_filter() resolves
            # its target from model.selected directly (not the tree
            # cursor), so filtering root's own children requires root to
            # be model.selected again. Done via the screen's own narrow
            # dispatch helper, not a real cursor move + key press -- press
            # would also toggle root's own already-expanded state.
            unit_screen._select_folder_ref(tree.root.data.payload)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#filter-input"),
                timeout=ui_timeout,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = unit_screen.query_one("#filter-input", Input)
            filter_input.value = needle
            # The reconciler keeps a still-matching survivor's own
            # TreeNode object -- the readiness signal is the model's own
            # committed filter text, not the tree changing shape at all
            # under this node.
            await wait_until(
                pilot,
                lambda: unit_screen.store.model.filter is not None and unit_screen.store.model.filter.text == needle,
                timeout=ui_timeout,
                interval=0.02,
                message="filter text never committed",
            )

            loaded_after_filter = folder_ref in unit_screen.store.model.loaded
            still_the_same_node = folder in tree.root.children
            still_expanded = folder.is_expanded
            child_count_after_filter = len(unit_screen.store.model.loaded[folder_ref].children)

            await pilot.press("escape")
            await wait_until(
                pilot,
                lambda: not unit_screen.query("#filter-input.active"),
                timeout=ui_timeout,
                interval=0.02,
                message="filter never closed",
            )

            return (
                loaded_before and loaded_after_filter and still_the_same_node and still_expanded,
                child_count_before == child_count_after_filter,
                child_count_after_filter,
            )

    survived, child_count_unchanged, child_count = asyncio.run(scenario())
    assert survived, "a still-matching folder's own TreeNode/expansion/loaded-state must survive a filter keystroke"
    assert child_count_unchanged and child_count > 0, (
        "the folder's already-loaded messages must not be dropped or re-fetched"
    )


__all__: list[str] = []
