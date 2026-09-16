"""``textual`` ``Pilot``-driven coverage against this file's own dedicated
``tui_preview_apv1_pilot.json.gz`` — the TUI's Site-list/wide-table/
disk-image preview panes, plus a thread-lifecycle check on Teams-channel
preview, against real apv-sample-1 data, with **no real ``samples_dir``
dependency**.

Deliberately doesn't cover Mail/Contact/Calendar/Teams-message preview
rendering against real content: a message's body, a contact's own name/
email, an event's own fields, and a channel's own messages *are* content,
and loading one to preview it would make ``RecordingStore`` capture that
real content into the committed fixture regardless of what the test then
asserts. ``tests/unit/browser/test_browser_content_preview.py`` covers
every one of those render functions directly, against synthetic input.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from textual.widgets import DataTable, Static, Tree
from textual.widgets.tree import TreeNode

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen, CatalogEntry
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.storage.base import ObjectStore


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[..., Awaitable[ObjectStore]]
) -> None:
    store = await record_target("tui_preview_apv1_pilot.json.gz", allow_content=True)

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, "apv-sample-1"

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


def _find_group_node_path(
    node: TreeNode[object], type_hint: str, path: list[TreeNode[object]]
) -> list[TreeNode[object]] | None:
    for child in node.children:
        if child.data == type_hint:
            return [*path, child]
        found = _find_group_node_path(child, type_hint, [*path, child])
        if found is not None:
            return found
    return None


async def _drill_to_first_workload_of_type(
    app: ApmRepoBrowserApp, pilot: Any, wait_until: Any, *, connection_config_id: int, type_hint: str
) -> None:
    assert isinstance(app.screen, BrowseScreen), app.screen
    cat_tree = app.screen.query_one("#col-catalogs", Tree)
    repo_node = cat_tree.root.children[0]
    connection_node = next(
        n
        for n in repo_node.children
        if isinstance(n.data, CatalogEntry) and n.data.catalog.connection.connection_config_id == connection_config_id
    )
    cat_tree.move_cursor(connection_node)
    cat_tree.focus()
    await pilot.press("enter")

    wl_tree = app.screen.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: any(wl_tree.root.children), timeout=0.9, interval=0.03)
    path = _find_group_node_path(wl_tree.root, type_hint, [])
    assert path is not None, f"no group node with data == {type_hint!r} found"
    for ancestor in path:
        if not ancestor.is_expanded:
            ancestor.expand()
    group_node = path[-1]
    _ = wl_tree._tree_lines
    workload_node = group_node.children[0]
    wl_tree.move_cursor(workload_node)
    wl_tree.focus()
    await pilot.press("enter")

    ver_table = app.screen.query_one("#col-versions", DataTable)
    await wait_until(pilot, lambda: ver_table.row_count, timeout=0.9, interval=0.03)
    ver_table.focus()
    await pilot.press("enter")

    await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=0.6, interval=0.03)
    assert isinstance(app.screen, UnitScreen), app.screen


async def _wait_for_detail_text(unit_screen: UnitScreen, pilot: Any, *, contains: str, attempts: int = 40) -> str:
    text = ""
    for _ in range(attempts):
        await pilot.pause(0.03)
        text = str(unit_screen.query_one("#detail", Static).render())
        if contains in text:
            break
    return text


def test_previewing_a_teams_channel_then_quitting_does_not_leak_threads_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> None:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_first_workload_of_type(
                app,
                pilot,
                wait_until,
                connection_config_id=1,
                type_hint="TEAMS",
            )

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#unit-tree", Tree)
            await wait_until(pilot, lambda: tree.root.children, timeout=0.9, interval=0.03)
            channel = tree.root.children[0]
            tree.move_cursor(channel)
            await pilot.pause(0.02)
            await pilot.press("enter")
            # A Teams channel's own node carries UnitKind.RAW_OBJECT (see
            # teams_chat.py) -- structural, not a rendered message's own
            # content, but still proof the preview actually loaded before
            # quitting.
            await _wait_for_detail_text(unit_screen, pilot, contains="kind: raw_object")

            await pilot.press("q")
            await pilot.pause(0.5)

    before = {t.ident for t in threading.enumerate()}
    asyncio.run(scenario())
    leaked = [t for t in threading.enumerate() if t.ident not in before and t.is_alive() and not t.daemon]
    assert not leaked, [t.name for t in leaked]


async def _drill_to_site_list_category(
    app: ApmRepoBrowserApp, pilot: Any, wait_until: Any, *, connection_config_id: int
) -> tuple[UnitScreen, TreeNode[object]]:
    await _drill_to_first_workload_of_type(
        app, pilot, wait_until, connection_config_id=connection_config_id, type_hint="SITE"
    )
    unit_screen = app.screen
    assert isinstance(unit_screen, UnitScreen)
    tree = unit_screen.query_one("#unit-tree", Tree)
    await wait_until(pilot, lambda: tree.root.children, timeout=0.9, interval=0.03)
    list_category = next((c for c in tree.root.children if str(c.label) == "List"), None)
    assert list_category is not None, [str(c.label) for c in tree.root.children]
    tree.move_cursor(list_category)
    await pilot.pause(0.02)
    await pilot.press("l")
    await wait_until(pilot, lambda: list_category.children, timeout=0.9, interval=0.03)
    assert list_category.children, "List category never loaded any real lists"
    return unit_screen, list_category


def test_site_list_group_has_no_expand_arrow_and_loads_no_tree_children_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> bool:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, list_category = await _drill_to_site_list_category(
                app, pilot, wait_until, connection_config_id=1
            )
            tree = unit_screen.query_one("#unit-tree", Tree)

            access_requests = next((c for c in list_category.children if str(c.label) == "Access Requests"), None)
            assert access_requests is not None, [str(c.label) for c in list_category.children]
            tree.move_cursor(access_requests)
            await pilot.pause(0.02)
            await pilot.press("l")  # a no-op "expand" attempt — there is nothing to expand
            await pilot.pause(0.5)

            return access_requests.allow_expand

    allow_expand = asyncio.run(scenario())
    assert allow_expand is False


def test_site_list_group_shows_a_spreadsheet_overview_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> str:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, list_category = await _drill_to_site_list_category(
                app, pilot, wait_until, connection_config_id=1
            )
            tree = unit_screen.query_one("#unit-tree", Tree)

            access_requests = next((c for c in list_category.children if str(c.label) == "Access Requests"), None)
            assert access_requests is not None, [str(c.label) for c in list_category.children]
            tree.move_cursor(access_requests)
            await pilot.pause(0.02)
            await pilot.press("enter")

            return await _wait_for_detail_text(unit_screen, pilot, contains="I'd like")

    detail_text = asyncio.run(scenario())
    assert "I'd like access, please." in detail_text, detail_text
    assert "Conversation" in detail_text, detail_text
    assert "1 item" in detail_text, detail_text


def test_wide_list_overview_gets_a_pannable_pane_not_a_wrapped_one_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> tuple[str, frozenset[str], bool]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, list_category = await _drill_to_site_list_category(
                app, pilot, wait_until, connection_config_id=1
            )
            tree = unit_screen.query_one("#unit-tree", Tree)

            composed_looks = next((c for c in list_category.children if str(c.label) == "Composed Looks"), None)
            assert composed_looks is not None, [str(c.label) for c in list_category.children]
            tree.move_cursor(composed_looks)
            await pilot.pause(0.02)
            await pilot.press("enter")

            detail = unit_screen.query_one("#detail", Static)
            detail_scroll = unit_screen.query_one("#detail-scroll")
            text = await _wait_for_detail_text(unit_screen, pilot, contains="item")
            await pilot.pause(0.03)
            return text, detail.classes, detail_scroll.virtual_size.width > detail_scroll.size.width

    detail_text, detail_classes, is_wider_than_viewport = asyncio.run(scenario())
    assert "wide-preview" in detail_classes, detail_classes
    assert is_wider_than_viewport, "a table this wide should need horizontal scrolling"
    box_drawing_lines = [line for line in detail_text.splitlines() if line and line[0] in "┏┡└│┃┗┓┛├┤┬┴┼"]
    assert box_drawing_lines, detail_text
    assert len({len(line) for line in box_drawing_lines}) == 1, detail_text


def test_disk_image_leaf_shows_no_preview_just_the_header_block_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> str:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_first_workload_of_type(app, pilot, wait_until, connection_config_id=1, type_hint="VM")

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#unit-tree", Tree)
            await wait_until(
                pilot,
                lambda: tree.root.children or (tree.root.data is not None and tree.root.data.is_leaf),
                timeout=0.9,
                interval=0.03,
            )
            node = tree.root
            depth = 0
            while node is not None and node.data is not None and not node.data.is_leaf and depth < 6:
                node.expand()
                await pilot.pause(0.4)
                if not node.children:
                    break
                node = node.children[0]
                depth += 1
            assert node is not None and node.data is not None and node.data.is_leaf, "no disk image leaf found"
            tree.move_cursor(node)
            await pilot.pause(0.02)
            await pilot.press("enter")
            await pilot.pause(1.0)

            return str(unit_screen.query_one("#detail", Static).render())

    detail_text = asyncio.run(scenario())
    assert "kind: disk_image" in detail_text, detail_text
    assert "─" not in detail_text, "no preview separator should appear for a disk image leaf"


__all__: list[str] = []
