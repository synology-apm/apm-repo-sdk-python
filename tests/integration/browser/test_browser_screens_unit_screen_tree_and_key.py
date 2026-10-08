"""``Pilot`` tests against a real Mail workload: a second-level folder stays
expanded after ``l``/Enter, and a filter re-render keeps a surviving
folder's ``TreeNode`` and loaded children.

Fixture: ``tui_tree_and_key_vault_plain_pilot.json.gz``, recorded against
``vault-plain/@ActiveProtectVault``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from textual.widgets import Input, Tree
from textual.widgets.tree import TreeNode

from integration.browser.pilot_drivers import ReplayLocalStore, drill_to_first_workload_of_type, open_browser_pilot
from support.pilot import (
    RUN_TEST_SIZE,
    SDK_TIMEOUT,
    UI_TIMEOUT,
    focus_widget,
    move_cursor_to,
    wait_for_workers,
    wait_until,
)
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.units.base import Node


async def _first_non_leaf_root_child(
    unit_screen: UnitScreen,
    pilot: Any,
) -> TreeNode[Any]:
    """The first folder under root (the folder tree shows only containers, so
    every root child is non-leaf); callers load and inspect its children."""
    tree = unit_screen.query_one("#folder-tree", Tree)
    await wait_until(pilot, lambda: tree.root.children, timeout=SDK_TIMEOUT, interval=0.03)
    await focus_widget(pilot, tree)
    folder = next((n for n in tree.root.children if n.data is not None and not n.data.payload.is_leaf), None)
    assert folder is not None, "Mail root has no non-leaf folder child to expand"
    return folder


def test_l_expands_a_second_level_tree_node_and_it_stays_expanded_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[bool, int]:
        await replay_local_store("tui_tree_and_key_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_workload_of_type(app, pilot, connection_config_id=3, type_hint="MAIL")
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot)
            assert folder.data is not None
            folder_ref = folder.data.key

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("l")
            await wait_until(
                pilot, lambda: folder_ref in unit_screen.store.model.loaded, timeout=SDK_TIMEOUT, interval=0.03
            )
            # Messages are leaves (file table, not tree): count them in the model.
            return folder.is_expanded, len(unit_screen.store.model.loaded[folder_ref].children)

    is_expanded, child_count = asyncio.run(scenario())
    assert is_expanded, "folder collapsed again after l — the double-toggle bug"
    assert child_count > 0, "l did not load the folder's messages"


def test_enter_expands_a_second_level_tree_node_and_it_stays_expanded_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[bool, int]:
        await replay_local_store("tui_tree_and_key_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_workload_of_type(app, pilot, connection_config_id=3, type_hint="MAIL")
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot)
            assert folder.data is not None
            folder_ref = folder.data.key

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("enter")
            await wait_until(
                pilot, lambda: folder_ref in unit_screen.store.model.loaded, timeout=SDK_TIMEOUT, interval=0.03
            )
            return folder.is_expanded, len(unit_screen.store.model.loaded[folder_ref].children)

    is_expanded, child_count = asyncio.run(scenario())
    assert is_expanded, "folder collapsed again after enter — the double-toggle bug"
    assert child_count > 0, "enter did not load the folder's messages"


def test_filtering_preserves_a_surviving_folders_own_widget_and_its_loaded_messages_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A folder expanded and loaded before a ``/`` filter keeps its
    ``TreeNode`` (``view/reconcile.py``'s ``reconcile_children``), stays
    expanded, and is not re-fetched, against real Mail data. The synthetic
    counterpart is ``tests/unit/browser/test_browser_screens_unit_screen_filter_paging.py``'s
    ``test_filtering_a_level_preserves_a_surviving_childs_own_widget_and_does_not_refetch_it``."""

    children_calls: list[object] = []

    async def scenario() -> tuple[bool, bool, int]:
        await replay_local_store("tui_tree_and_key_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_workload_of_type(app, pilot, connection_config_id=3, type_hint="MAIL")
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            folder = await _first_non_leaf_root_child(unit_screen, pilot)
            assert folder.data is not None
            folder_ref = folder.data.key
            assert tree.root.data is not None

            await move_cursor_to(pilot, tree, folder)
            await pilot.press("l")
            await wait_until(
                pilot, lambda: folder_ref in unit_screen.store.model.loaded, timeout=SDK_TIMEOUT, interval=0.03
            )
            loaded_before = folder_ref in unit_screen.store.model.loaded
            # Messages are leaves (file table, not tree): count them in the model.
            child_count_before = len(unit_screen.store.model.loaded[folder_ref].children)

            handle = unit_screen.store.model.provider
            assert handle is not None
            provider = unit_screen.app_state.resources.provider(handle)
            assert provider is not None
            fetch_children = provider.children

            async def counting_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
                children_calls.append(node.ref)
                return await fetch_children(node, offset=offset, limit=limit)

            monkeypatch.setattr(provider, "children", counting_children)

            needle = str(folder.label)[:3]
            # action_filter() targets model.selected, which "l" moved to
            # folder_ref; re-select root by dispatch, since a key press on
            # root would also collapse it.
            unit_screen._select_folder_ref(tree.root.data.payload)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#filter-input"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = unit_screen.query_one("#filter-input", Input)
            filter_input.value = needle
            # The surviving node doesn't change shape, so wait on the
            # model's committed filter text instead.
            await wait_until(
                pilot,
                lambda: unit_screen.store.model.filter is not None and unit_screen.store.model.filter.text == needle,
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="filter text never committed",
            )

            # A re-fetch would run in a worker the filter started.
            await wait_for_workers(pilot)
            loaded_after_filter = folder_ref in unit_screen.store.model.loaded
            still_the_same_node = folder in tree.root.children
            still_expanded = folder.is_expanded
            child_count_after_filter = len(unit_screen.store.model.loaded[folder_ref].children)

            await pilot.press("escape")
            await wait_until(
                pilot,
                lambda: not unit_screen.query("#filter-input.active"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="filter never closed",
            )

            return (
                loaded_before and loaded_after_filter and still_the_same_node and still_expanded,
                child_count_before == child_count_after_filter,
                child_count_after_filter,
            )

    survived, child_count_unchanged, child_count = asyncio.run(scenario())
    assert children_calls == [], "a filter keystroke must not re-fetch any level's children"
    assert survived, "a still-matching folder's own TreeNode/expansion/loaded-state must survive a filter keystroke"
    assert child_count_unchanged and child_count > 0, (
        "the folder's already-loaded messages must not be dropped or re-fetched"
    )
