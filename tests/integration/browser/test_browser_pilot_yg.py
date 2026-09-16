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
from textual.widgets import DataTable, Input, Tree

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.storage.base import ObjectStore


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[[str], Awaitable[ObjectStore]]
) -> None:
    store = await record_target("tui_yg_apv1_pilot.json.gz")

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, "apv-sample-1"

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


async def _drill_to_unit_screen(app: ApmRepoBrowserApp, pilot: Any, wait_until: Any) -> None:
    assert isinstance(app.screen, BrowseScreen), app.screen
    app.screen.query_one("#col-catalogs", Tree).focus()
    await pilot.press("enter")
    workloads_tree = app.screen.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: workloads_tree.root.children, timeout=0.8, interval=0.02)
    app.screen.query_one("#col-workloads", Tree).focus()
    await pilot.press("enter")
    versions_table = app.screen.query_one("#col-versions", DataTable)
    await wait_until(pilot, lambda: versions_table.row_count, timeout=0.8, interval=0.02)
    app.screen.query_one("#col-versions", DataTable).focus()
    await pilot.press("enter")
    await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=0.6, interval=0.03)
    assert isinstance(app.screen, UnitScreen), app.screen


async def _first_leaf(app: ApmRepoBrowserApp, pilot: Any, wait_until: Any) -> Any:
    unit_screen = app.screen
    assert isinstance(unit_screen, UnitScreen)
    tree = unit_screen.query_one("#unit-tree", Tree)
    await wait_until(pilot, lambda: tree.root.data is not None, timeout=1.0, interval=0.02)
    node = tree.root
    depth = 0
    while node is not None and node.data is not None and not node.data.is_leaf and depth < 6:
        node.expand()
        await wait_until(pilot, lambda n=node: n.children, timeout=0.4, interval=0.02)
        if not node.children:
            break
        node = node.children[0]
        depth += 1
    assert node is not None and node.data is not None and node.data.is_leaf, "no leaf found"
    tree.move_cursor(node)
    await pilot.pause(0.03)
    return node


def test_copy_ref_puts_the_cursor_nodes_canonical_ref_on_the_clipboard_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> tuple[str, str]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)
            node = await _first_leaf(app, pilot, wait_until)
            expected_ref = str(node.data.ref)

            await pilot.press("y")
            await pilot.pause(0.02)
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
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            unit_screen.notify = recording_notify  # type: ignore[method-assign]
            # #unit-tree's cursor defaults to a real node after drilling
            # in (Textual's own Tree always starts with a cursor line),
            # so "nothing selected" -- the branch this test targets --
            # needs to be forced the same way
            # test_export_and_copy_ref_and_hex_preview_warn_when_nothing_is_selected
            # (tests/unit/browser/test_browser_unit_screen_gaps.py) does.
            unit_screen._selected_node = lambda: None  # type: ignore[method-assign]

            await pilot.press("y")
            await wait_until(pilot, lambda: notifications, timeout=0.6, interval=0.03)
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
) -> None:
    async def scenario() -> tuple[str, str]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)
            node = await _first_leaf(app, pilot, wait_until)
            target_ref = str(node.data.ref)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            unit_screen.action_refresh()
            await wait_until(
                pilot, lambda: unit_screen.query_one("#unit-tree", Tree).root.children, timeout=0.6, interval=0.03
            )

            await pilot.press("g")
            await pilot.pause(0.02)
            goto_input = unit_screen.query_one("#goto-input", Input)
            goto_input.value = target_ref
            await pilot.press("enter")

            def _landed() -> bool:
                cursor = unit_screen.query_one("#unit-tree", Tree).cursor_node
                return cursor is not None and cursor.data is not None and str(cursor.data.ref) == target_ref

            await wait_until(pilot, _landed, timeout=0.6, interval=0.02)

            tree = unit_screen.query_one("#unit-tree", Tree)
            landed_ref = (
                str(tree.cursor_node.data.ref) if tree.cursor_node is not None and tree.cursor_node.data else ""
            )
            return landed_ref, target_ref

    landed_ref, target_ref = asyncio.run(scenario())
    assert landed_ref == target_ref


def test_goto_ref_from_browse_screen_opens_the_right_version_and_node_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> tuple[str, str]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)
            node = await _first_leaf(app, pilot, wait_until)
            target_ref = str(node.data.ref)

            app.pop_screen()
            await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=0.6, interval=0.03)
            assert isinstance(app.screen, BrowseScreen), app.screen

            await pilot.press("g")
            await pilot.pause(0.02)
            goto_input = app.screen.query_one("#goto-input", Input)
            goto_input.value = target_ref
            await pilot.press("enter")

            def _landed() -> bool:
                if not isinstance(app.screen, UnitScreen):
                    return False
                cursor = app.screen.query_one("#unit-tree", Tree).cursor_node
                return cursor is not None and cursor.data is not None and str(cursor.data.ref) == target_ref

            await wait_until(pilot, _landed, timeout=0.9, interval=0.03)

            assert isinstance(app.screen, UnitScreen), app.screen
            tree = app.screen.query_one("#unit-tree", Tree)
            landed_ref = (
                str(tree.cursor_node.data.ref) if tree.cursor_node is not None and tree.cursor_node.data else ""
            )
            return landed_ref, target_ref

    landed_ref, target_ref = asyncio.run(scenario())
    assert landed_ref == target_ref


def test_goto_ref_rejects_a_human_ref_with_a_clear_warning_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            unit_screen.notify = recording_notify  # type: ignore[method-assign]

            await pilot.press("g")
            await pilot.pause(0.02)
            goto_input = unit_screen.query_one("#goto-input", Input)
            # This scenario only proves a human-shaped ref gets rejected as
            # such -- it never needs to resolve, so any syntactically
            # plausible fragment works; no real workload/device identity
            # needed here at all.
            goto_input.value = "/some/path#Test-Workload-0000/CORP-PC-0000"
            await pilot.press("enter")
            await pilot.pause(0.03)
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
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)
            node = await _first_leaf(app, pilot, wait_until)
            real_ref = str(node.data.ref)
            bogus_ref = real_ref + "-does-not-exist-at-all"

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            unit_screen.notify = recording_notify  # type: ignore[method-assign]

            await pilot.press("g")
            await pilot.pause(0.02)
            goto_input = unit_screen.query_one("#goto-input", Input)
            goto_input.value = bogus_ref
            await pilot.press("enter")
            await wait_until(pilot, lambda: notifications, timeout=0.6, interval=0.03)
            return isinstance(app.screen, UnitScreen), notifications

    still_on_unit_screen, notifications = asyncio.run(scenario())
    assert still_on_unit_screen
    assert any("not found" in n.lower() for n in notifications), notifications


__all__: list[str] = []
