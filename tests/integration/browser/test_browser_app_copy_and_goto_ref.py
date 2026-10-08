"""``Pilot`` tests for ``y`` (copy a node's canonical ``NodeRef``) and ``g``
(jump to a pasted one).

Fixture: ``tui_yg_vault_plain_pilot.json.gz``, recorded against ``vault-plain``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from textual.widgets import DataTable, Input

from integration.browser.pilot_drivers import (
    ReplayLocalStore,
    drill_to_first_unit_screen,
    find_first_leaf,
    open_browser_pilot,
    select_leaf_row,
)
from support.pilot import RUN_TEST_SIZE, SDK_TIMEOUT, UI_TIMEOUT, wait_until
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.unit_screen import UnitScreen


def _landed_on_target(unit_screen: UnitScreen, target_ref: str) -> bool:
    """Whether the file table's cursor is on ``target_ref``'s node: a goto
    to a leaf stops the tree at its parent folder and lands in the file
    table."""
    table = unit_screen.query_one("#file-table", DataTable)
    row = table.cursor_row
    if not (0 <= row < len(unit_screen._file_table._nodes)):
        return False
    node = unit_screen._file_table._nodes[row]
    return node is not None and str(node.ref) == target_ref


def test_copy_ref_puts_the_cursor_nodes_canonical_ref_on_the_clipboard_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[str, str]:
        await replay_local_store("tui_yg_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_unit_screen(app, pilot)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            leaf, parent = await find_first_leaf(app, pilot)
            await select_leaf_row(unit_screen, pilot, leaf, parent)
            expected_ref = str(leaf.ref)

            await pilot.press("y")
            await wait_until(
                pilot, lambda: app.clipboard, timeout=UI_TIMEOUT, interval=0.02, message="clipboard never set"
            )
            return app.clipboard or "", expected_ref

    clipboard_text, expected_ref = asyncio.run(scenario())
    assert clipboard_text == expected_ref
    assert "#cat:" in clipboard_text


def test_copy_ref_with_nothing_selected_warns_not_crashes_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await replay_local_store("tui_yg_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_unit_screen(app, pilot)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            monkeypatch.setattr(unit_screen, "notify", recording_notify)
            # Forces the "nothing selected" branch.
            monkeypatch.setattr(unit_screen, "_selected_node", lambda: None)

            await pilot.press("y")
            await wait_until(pilot, lambda: notifications, timeout=UI_TIMEOUT, interval=0.03)
            return isinstance(app.screen, UnitScreen), notifications

    still_on_unit_screen, notifications = asyncio.run(scenario())
    assert still_on_unit_screen
    assert notifications == ["select an item to copy its ref first"], notifications


def test_goto_ref_within_the_same_version_expands_and_selects_the_target_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[bool, str]:
        await replay_local_store("tui_yg_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_unit_screen(app, pilot)
            leaf, _parent = await find_first_leaf(app, pilot)
            target_ref = str(leaf.ref)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            # A refresh reloads the tree from its root, so the goto has to
            # expand the path down to the leaf itself.
            root_ref = unit_screen.store.model.root.ref if unit_screen.store.model.root is not None else None
            unit_screen.action_refresh()
            await wait_until(
                pilot,
                lambda: root_ref is not None and root_ref in unit_screen.store.model.loaded,
                timeout=SDK_TIMEOUT,
                interval=0.03,
            )

            await pilot.press("g")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#goto-input"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="goto input never opened",
            )
            goto_input = unit_screen.query_one("#goto-input", Input)
            goto_input.value = target_ref
            await pilot.press("enter")

            await wait_until(
                pilot, lambda: _landed_on_target(unit_screen, target_ref), timeout=SDK_TIMEOUT, interval=0.02
            )
            return _landed_on_target(unit_screen, target_ref), target_ref

    landed, target_ref = asyncio.run(scenario())
    assert landed, target_ref


def test_goto_ref_from_browse_screen_opens_the_right_version_and_node_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[bool, str]:
        await replay_local_store("tui_yg_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_unit_screen(app, pilot)
            leaf, _parent = await find_first_leaf(app, pilot)
            target_ref = str(leaf.ref)

            app.pop_screen()
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.03,
            )
            assert isinstance(app.screen, BrowseScreen), app.screen

            await pilot.press("g")
            await wait_until(
                pilot,
                lambda: app.screen.query("#goto-input"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="goto input never opened",
            )
            goto_input = app.screen.query_one("#goto-input", Input)
            goto_input.value = target_ref
            await pilot.press("enter")

            def _landed() -> bool:
                return isinstance(app.screen, UnitScreen) and _landed_on_target(app.screen, target_ref)

            await wait_until(pilot, _landed, timeout=SDK_TIMEOUT, interval=0.03)

            assert isinstance(app.screen, UnitScreen), app.screen
            return _landed_on_target(app.screen, target_ref), target_ref

    landed, target_ref = asyncio.run(scenario())
    assert landed, target_ref


def test_goto_ref_rejects_a_human_ref_with_a_clear_warning_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await replay_local_store("tui_yg_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_unit_screen(app, pilot)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            monkeypatch.setattr(unit_screen, "notify", recording_notify)

            await pilot.press("g")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#goto-input"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="goto input never opened",
            )
            goto_input = unit_screen.query_one("#goto-input", Input)
            # Never resolved, so any human-shaped fragment works.
            goto_input.value = "/some/path#Test-Workload-0000/CORP-PC-0000"
            await pilot.press("enter")
            # The rejection notification is what says the submission was
            # processed; without it there is nothing to assert on yet.
            await wait_until(
                pilot, lambda: notifications, timeout=UI_TIMEOUT, interval=0.02, message="ref was never rejected"
            )
            return isinstance(app.screen, UnitScreen), notifications

    still_on_unit_screen, notifications = asyncio.run(scenario())
    assert still_on_unit_screen
    assert any("canonical" in n.lower() for n in notifications), notifications


def test_goto_ref_not_found_warns_not_crashes_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> tuple[bool, list[str]]:
        await replay_local_store("tui_yg_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        notifications: list[str] = []
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_unit_screen(app, pilot)
            leaf, _parent = await find_first_leaf(app, pilot)
            real_ref = str(leaf.ref)
            bogus_ref = real_ref + "-does-not-exist-at-all"

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            original_notify = unit_screen.notify

            def recording_notify(message: str, **kwargs: object) -> None:
                notifications.append(message)
                original_notify(message, **kwargs)  # type: ignore[arg-type]

            monkeypatch.setattr(unit_screen, "notify", recording_notify)

            await pilot.press("g")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#goto-input"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="goto input never opened",
            )
            goto_input = unit_screen.query_one("#goto-input", Input)
            goto_input.value = bogus_ref
            await pilot.press("enter")
            await wait_until(pilot, lambda: notifications, timeout=SDK_TIMEOUT, interval=0.03)
            return isinstance(app.screen, UnitScreen), notifications

    still_on_unit_screen, notifications = asyncio.run(scenario())
    assert still_on_unit_screen
    assert any("not found" in n.lower() for n in notifications), notifications
