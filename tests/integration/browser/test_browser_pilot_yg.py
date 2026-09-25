"""``textual`` ``Pilot``-driven coverage against this file's own dedicated
``tui_yg_apv1_pilot.json.gz``: ``y`` (copy canonical ``NodeRef``) and ``g``
(jump to a pasted one), with **no real ``samples_dir`` dependency**.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from textual.coordinate import Coordinate
from textual.widgets import DataTable, Input, Tree

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.unit.msg import ChildrenRequested
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import Node


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[[str], Awaitable[ObjectStore]]
) -> None:
    store = await record_target("tui_yg_apv1_pilot.json.gz")

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, "apv-sample-1"

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


async def _drill_to_unit_screen(
    app: ApmRepoBrowserApp,
    pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    assert isinstance(app.screen, BrowseScreen), app.screen
    await focus_widget(pilot, app.screen.query_one("#col-catalogs", Tree))
    await pilot.press("enter")
    workloads_tree = app.screen.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: workloads_tree.root.children, timeout=sdk_timeout, interval=0.02)
    await focus_widget(pilot, app.screen.query_one("#col-workloads", Tree))
    await pilot.press("enter")
    versions_table = app.screen.query_one("#col-versions", DataTable)
    # row_count alone can be stale; wait for _visible_version_indices too
    # (see tests/conftest.py's drill_to_unit_screen_via_fs_device).
    await wait_until(
        pilot,
        lambda: versions_table.row_count and app.screen._visible_version_indices,
        timeout=sdk_timeout,
        interval=0.02,
    )
    await focus_widget(pilot, app.screen.query_one("#col-versions", DataTable))
    await pilot.press("enter")
    await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=ui_timeout, interval=0.03)
    assert isinstance(app.screen, UnitScreen), app.screen


async def _first_leaf(app: ApmRepoBrowserApp, pilot: Any, wait_until: Any, sdk_timeout: float) -> tuple[Node, Node]:
    """DFS with backtracking, not a blind ``children[0]`` walk: since
    ``units/fs.py`` sorts directories before files, a real
    directory's own *first* child (alphabetically first among its
    subdirectories) can be an empty one at any depth -- an ordinary real
    filesystem fact, not a fixture quirk -- which a greedy walk would
    dead-end into instead of finding one of this version's real leaves
    elsewhere.

    Driven straight through the store (``ChildrenRequested`` + real
    replayed provider fetches), not the folder-tree widget: the tree
    only ever shows containers now, so a leaf can never be found by
    walking it. Returns the leaf's own ``Node`` plus its own parent
    ``Node``, for a caller to select through the real file-table UI (see
    ``_select_leaf_row``)."""
    unit_screen = app.screen
    assert isinstance(unit_screen, UnitScreen)
    await wait_until(pilot, lambda: unit_screen.store.model.root is not None, timeout=sdk_timeout, interval=0.02)
    root = unit_screen.store.model.root
    assert root is not None and not root.is_leaf, "version root is itself a leaf -- no parent to select it under"

    async def _search(node: Node, depth: int) -> tuple[Node, Node] | None:
        if depth >= 10:
            return None
        level = unit_screen.store.model.loaded.get(node.ref)
        if level is None:
            unit_screen.store.dispatch(ChildrenRequested(node=node))
            await wait_until(
                pilot,
                lambda ref=node.ref: ref in unit_screen.store.model.loaded or ref in unit_screen.store.model.errors,
                timeout=sdk_timeout,
                interval=0.02,
            )
            level = unit_screen.store.model.loaded.get(node.ref)
        if level is None:
            return None  # a children() failure for this one branch -- not fatal, just try elsewhere
        leaf = next((c for c in level.children if c.is_leaf), None)
        if leaf is not None:
            return leaf, node
        for child in level.children:
            if not child.is_leaf:
                found = await _search(child, depth + 1)
                if found is not None:
                    return found
        return None

    found = await _search(root, 0)
    assert found is not None, "no leaf found"
    return found


async def _select_leaf_row(
    unit_screen: UnitScreen,
    pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    leaf: Node,
    parent: Node,
    ui_timeout: float,
) -> None:
    """Points the file table's own cursor at ``leaf`` -- the file-table
    counterpart of moving the folder tree's cursor onto a node: no Enter
    press, cursor position plus focus alone is what
    ``UnitScreen._selected_node()`` reads."""
    unit_screen._select_folder_ref(parent)
    await wait_until(pilot, lambda: leaf in unit_screen._file_table._nodes, timeout=ui_timeout, interval=0.02)
    row_index = unit_screen._file_table._nodes.index(leaf)
    table = unit_screen.query_one("#file-table", DataTable)
    await focus_widget(pilot, table)
    table.cursor_coordinate = Coordinate(row_index, 0)


def _landed_on_target(unit_screen: UnitScreen, target_ref: str) -> bool:
    """Whether the file table's own cursor currently sits on the node
    named by ``target_ref`` -- the folder tree only ever shows containers,
    so a goto landing on a leaf stops descent one level early, at the
    leaf's parent folder, and the file table is where the real landing
    spot shows up."""
    table = unit_screen.query_one("#file-table", DataTable)
    row = table.cursor_row
    if not (0 <= row < len(unit_screen._file_table._nodes)):
        return False
    node = unit_screen._file_table._nodes[row]
    return node is not None and str(node.ref) == target_ref


def test_copy_ref_puts_the_cursor_nodes_canonical_ref_on_the_clipboard_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[str, str]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            leaf, parent = await _first_leaf(app, pilot, wait_until, sdk_timeout)
            await _select_leaf_row(unit_screen, pilot, wait_until, focus_widget, leaf, parent, ui_timeout)
            expected_ref = str(leaf.ref)

            await pilot.press("y")
            await wait_until(
                pilot, lambda: app.clipboard, timeout=ui_timeout, interval=0.02, message="clipboard never set"
            )
            return app.clipboard or "", expected_ref

    clipboard_text, expected_ref = asyncio.run(scenario())
    assert clipboard_text == expected_ref
    assert "#cat:" in clipboard_text


def test_copy_ref_with_nothing_selected_warns_not_crashes_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            unit_screen.notify = recording_notify  # type: ignore[method-assign]
            # Forces the "nothing selected" branch this test targets --
            # same pattern
            # test_export_and_copy_ref_and_hex_preview_warn_when_nothing_is_selected
            # (tests/unit/browser/test_browser_unit_screen_gaps.py) uses.
            unit_screen._selected_node = lambda: None  # type: ignore[method-assign]

            await pilot.press("y")
            await wait_until(pilot, lambda: notifications, timeout=ui_timeout, interval=0.03)
            return isinstance(app.screen, UnitScreen), notifications

    still_on_unit_screen, notifications = asyncio.run(scenario())
    assert still_on_unit_screen
    # The real warning text unit_screen.py's action_copy_ref actually
    # fires when nothing is selected -- matching the sibling
    # goto_ref-not-found test's own pattern of capturing and asserting
    # the real notification, not just "didn't crash".
    assert notifications == ["select an item to copy its ref first"], notifications


def test_goto_ref_within_the_same_version_expands_and_selects_the_target_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[bool, str]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout)
            leaf, _parent = await _first_leaf(app, pilot, wait_until, sdk_timeout)
            target_ref = str(leaf.ref)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            root_ref = unit_screen.store.model.root.ref if unit_screen.store.model.root is not None else None
            unit_screen.action_refresh()
            await wait_until(
                pilot,
                lambda: root_ref is not None and root_ref in unit_screen.store.model.loaded,
                timeout=sdk_timeout,
                interval=0.03,
            )

            await pilot.press("g")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#goto-input"),
                timeout=ui_timeout,
                interval=0.02,
                message="goto input never opened",
            )
            goto_input = unit_screen.query_one("#goto-input", Input)
            goto_input.value = target_ref
            await pilot.press("enter")

            await wait_until(
                pilot, lambda: _landed_on_target(unit_screen, target_ref), timeout=sdk_timeout, interval=0.02
            )
            return _landed_on_target(unit_screen, target_ref), target_ref

    landed, target_ref = asyncio.run(scenario())
    assert landed, target_ref


def test_goto_ref_from_browse_screen_opens_the_right_version_and_node_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[bool, str]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout)
            leaf, _parent = await _first_leaf(app, pilot, wait_until, sdk_timeout)
            target_ref = str(leaf.ref)

            app.pop_screen()
            await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=ui_timeout, interval=0.03)
            assert isinstance(app.screen, BrowseScreen), app.screen

            await pilot.press("g")
            await wait_until(
                pilot,
                lambda: app.screen.query("#goto-input"),
                timeout=ui_timeout,
                interval=0.02,
                message="goto input never opened",
            )
            goto_input = app.screen.query_one("#goto-input", Input)
            goto_input.value = target_ref
            await pilot.press("enter")

            def _landed() -> bool:
                return isinstance(app.screen, UnitScreen) and _landed_on_target(app.screen, target_ref)

            await wait_until(pilot, _landed, timeout=sdk_timeout, interval=0.03)

            assert isinstance(app.screen, UnitScreen), app.screen
            return _landed_on_target(app.screen, target_ref), target_ref

    landed, target_ref = asyncio.run(scenario())
    assert landed, target_ref


def test_goto_ref_rejects_a_human_ref_with_a_clear_warning_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            unit_screen.notify = recording_notify  # type: ignore[method-assign]

            await pilot.press("g")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#goto-input"),
                timeout=ui_timeout,
                interval=0.02,
                message="goto input never opened",
            )
            goto_input = unit_screen.query_one("#goto-input", Input)
            # This scenario only proves a human-shaped ref gets rejected as
            # such -- it never needs to resolve, so any syntactically
            # plausible fragment works; no real workload/device identity
            # needed here at all.
            goto_input.value = "/some/path#Test-Workload-0000/CORP-PC-0000"
            await pilot.press("enter")
            # The rejection notification is what says the submission was
            # processed; without it there is nothing to assert on yet.
            await wait_until(
                pilot, lambda: notifications, timeout=ui_timeout, interval=0.02, message="ref was never rejected"
            )
            return isinstance(app.screen, UnitScreen), notifications

    still_on_unit_screen, notifications = asyncio.run(scenario())
    assert still_on_unit_screen
    assert any("canonical" in n.lower() for n in notifications), notifications


def test_goto_ref_not_found_warns_not_crashes_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout)
            leaf, _parent = await _first_leaf(app, pilot, wait_until, sdk_timeout)
            real_ref = str(leaf.ref)
            bogus_ref = real_ref + "-does-not-exist-at-all"

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            unit_screen.notify = recording_notify  # type: ignore[method-assign]

            await pilot.press("g")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#goto-input"),
                timeout=ui_timeout,
                interval=0.02,
                message="goto input never opened",
            )
            goto_input = unit_screen.query_one("#goto-input", Input)
            goto_input.value = bogus_ref
            await pilot.press("enter")
            await wait_until(pilot, lambda: notifications, timeout=sdk_timeout, interval=0.03)
            return isinstance(app.screen, UnitScreen), notifications

    still_on_unit_screen, notifications = asyncio.run(scenario())
    assert still_on_unit_screen
    assert any("not found" in n.lower() for n in notifications), notifications


__all__: list[str] = []
