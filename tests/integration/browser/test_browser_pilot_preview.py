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
from textual.coordinate import Coordinate
from textual.widgets import DataTable, Static, Tree
from textual.widgets.tree import TreeNode

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.unit.msg import ChildrenRequested
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Catalog
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import UnitKind


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[..., Awaitable[ObjectStore]]
) -> None:
    store = await record_target("tui_preview_apv1_pilot.json.gz", allow_content=True)

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, "apv-sample-1"

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


def _find_group_node_path(node: TreeNode[Any], type_hint: str, path: list[TreeNode[Any]]) -> list[TreeNode[Any]] | None:
    for child in node.children:
        if child.data is not None and child.data.payload == type_hint:
            return [*path, child]
        found = _find_group_node_path(child, type_hint, [*path, child])
        if found is not None:
            return found
    return None


async def _drill_to_first_workload_of_type(
    app: ApmRepoBrowserApp,
    pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    ui_timeout: float,
    sdk_timeout: float,
    *,
    connection_config_id: int,
    type_hint: str,
) -> None:
    assert isinstance(app.screen, BrowseScreen), app.screen
    cat_tree = app.screen.query_one("#col-catalogs", Tree)
    repo_node = cat_tree.root.children[0]
    connection_node = next(
        n
        for n in repo_node.children
        if n.data is not None
        and isinstance(n.data.payload, Catalog)
        and n.data.payload.connection.connection_config_id == connection_config_id
    )
    _ = cat_tree._tree_lines  # forces the line map to rebuild; see move_cursor_to's docstring
    cat_tree.move_cursor(connection_node)
    await focus_widget(pilot, cat_tree)
    await pilot.press("enter")

    wl_tree = app.screen.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: any(wl_tree.root.children), timeout=sdk_timeout, interval=0.03)
    path = _find_group_node_path(wl_tree.root, type_hint, [])
    assert path is not None, f"no group node with data == {type_hint!r} found"
    for ancestor in path:
        if not ancestor.is_expanded:
            ancestor.expand()
    group_node = path[-1]
    _ = wl_tree._tree_lines
    workload_node = group_node.children[0]
    wl_tree.move_cursor(workload_node)
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


async def _select_first_file_table_row(
    unit_screen: UnitScreen, pilot: Any, wait_until: Any, focus_widget: Any, sdk_timeout: float
) -> None:
    """Selects row 0 of the file table -- for a leaf whose parent folder
    is root itself (never tree-shown), this is the only way to reach it:
    the folder tree only ever shows containers."""
    await wait_until(pilot, lambda: unit_screen._file_table._nodes, timeout=sdk_timeout, interval=0.03)
    table = unit_screen.query_one("#file-table", DataTable)
    await focus_widget(pilot, table)
    table.cursor_coordinate = Coordinate(0, 0)
    await pilot.press("enter")


def test_previewing_a_teams_channel_then_quitting_does_not_leak_threads_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    move_cursor_to: Any,
    focus_widget: Any,
    wait_for_detail_content: Any,
    ui_timeout: float,
    sdk_timeout: float,
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
                focus_widget,
                ui_timeout,
                sdk_timeout,
                connection_config_id=1,
                type_hint="TEAMS",
            )

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            # A Teams channel's own node is a leaf (UnitKind.TEAMS_CHAT_MESSAGE,
            # see teams_chat.py), so it lists in the file table, never the
            # folder tree.
            await _select_first_file_table_row(unit_screen, pilot, wait_until, focus_widget, sdk_timeout)
            # A Teams/Chat message page is a content-only preview (see
            # detail_pane.py's own header_text) -- the pane shows nothing
            # at all until the async preview lands, so this (not a bare
            # `text.strip() != ""` check, which can pass on
            # DebouncedProgress's own transient "(⠋ loading)" cue alone
            # for a content-only-preview node, whose header is empty) is
            # the real proof the preview loaded before quitting.
            await wait_for_detail_content(pilot, unit_screen)

            await pilot.press("q")
            await wait_until(
                pilot, lambda: not app.is_running, timeout=sdk_timeout, interval=0.02, message="app never shut down"
            )

    before = {t.ident for t in threading.enumerate()}
    asyncio.run(scenario())
    candidates = [t for t in threading.enumerate() if t.ident not in before and not t.daemon]
    # aiosqlite.Connection.close() awaits a future its own worker thread
    # resolves via call_soon_threadsafe *before* that thread's run() loop
    # actually returns (aiosqlite/core.py's _connection_worker_thread: the
    # callback is scheduled, then the loop breaks) -- so is_alive() can
    # still read True for a few milliseconds after our own await returns,
    # independent of anything this app/test controls. join() with a short
    # timeout gives that real, bounded OS-level exit the moment it needs.
    for t in candidates:
        t.join(timeout=1.0)
    leaked = [t for t in candidates if t.is_alive()]
    assert not leaked, [t.name for t in leaked]


async def _drill_to_site_list_category(
    app: ApmRepoBrowserApp,
    pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    ui_timeout: float,
    sdk_timeout: float,
    *,
    connection_config_id: int,
    move_cursor_to: Any,
) -> tuple[UnitScreen, TreeNode[object]]:
    """Selects the site root's own "List" category tree node -- never
    expanded (see ``test_site_list_category_has_no_expand_arrow_and_
    loads_no_tree_children_replayed``): its own individual Lists are
    reached as file-table rows, not tree children, via
    ``_select_file_table_row_by_name``."""
    await _drill_to_first_workload_of_type(
        app,
        pilot,
        wait_until,
        focus_widget,
        ui_timeout,
        sdk_timeout,
        connection_config_id=connection_config_id,
        type_hint="SITE",
    )
    unit_screen = app.screen
    assert isinstance(unit_screen, UnitScreen)
    tree = unit_screen.query_one("#folder-tree", Tree)
    await wait_until(pilot, lambda: tree.root.children, timeout=sdk_timeout, interval=0.03)
    list_category = next((c for c in tree.root.children if str(c.label) == "List"), None)
    assert list_category is not None, [str(c.label) for c in tree.root.children]
    await move_cursor_to(pilot, tree, list_category)
    await pilot.press("enter")
    await wait_until(pilot, lambda: unit_screen._file_table._nodes, timeout=sdk_timeout, interval=0.03)
    return unit_screen, list_category


async def _select_file_table_row_by_name(
    unit_screen: UnitScreen, pilot: Any, wait_until: Any, focus_widget: Any, name: str, sdk_timeout: float
) -> None:
    """Selects whichever visible file-table row's own ``Node.name``
    equals ``name`` -- the site's individual Lists live only here now,
    never as folder-tree children (see ``_drill_to_site_list_category``)."""
    await wait_until(pilot, lambda: unit_screen._file_table._nodes, timeout=sdk_timeout, interval=0.03)
    table = unit_screen.query_one("#file-table", DataTable)
    await focus_widget(pilot, table)
    names = [n.name if n is not None else None for n in unit_screen._file_table._nodes]
    row_index = next(i for i, n in enumerate(names) if n == name)
    table.cursor_coordinate = Coordinate(row_index, 0)
    await pilot.press("enter")


def test_site_list_category_has_no_expand_arrow_and_loads_no_tree_children_replayed(
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
    async def scenario() -> tuple[bool, list[str]]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, list_category = await _drill_to_site_list_category(
                app,
                pilot,
                wait_until,
                focus_widget,
                ui_timeout,
                sdk_timeout,
                connection_config_id=1,
                move_cursor_to=move_cursor_to,
            )
            tree = unit_screen.query_one("#folder-tree", Tree)
            await move_cursor_to(pilot, tree, list_category)
            await pilot.press("l")  # a no-op "expand" attempt — there is nothing to expand
            # Deliberately a fixed wait, not a wait_until: this asserts an
            # *absence* (nothing expands), and there is no readiness signal
            # for something that must never happen.
            await pilot.pause(0.5)

            # The category's own individual Lists (Access Requests,
            # Composed Looks, ...) are real file-table rows instead.
            names = [n.name for n in unit_screen._file_table._nodes if n is not None]
            return list_category.allow_expand, names

    allow_expand, names = asyncio.run(scenario())
    assert allow_expand is False
    assert "Access Requests" in names, names


def test_site_list_group_shows_a_spreadsheet_overview_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    move_cursor_to: Any,
    focus_widget: Any,
    wait_for_detail_content: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> str:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, _list_category = await _drill_to_site_list_category(
                app,
                pilot,
                wait_until,
                focus_widget,
                ui_timeout,
                sdk_timeout,
                connection_config_id=1,
                move_cursor_to=move_cursor_to,
            )
            await _select_file_table_row_by_name(
                unit_screen, pilot, wait_until, focus_widget, "Access Requests", sdk_timeout
            )

            await wait_for_detail_content(pilot, unit_screen, contains="I'd like")
            return str(unit_screen.query_one("#detail", Static).render())

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
    move_cursor_to: Any,
    focus_widget: Any,
    wait_for_detail_content: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[str, frozenset[str], bool]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, _list_category = await _drill_to_site_list_category(
                app,
                pilot,
                wait_until,
                focus_widget,
                ui_timeout,
                sdk_timeout,
                connection_config_id=1,
                move_cursor_to=move_cursor_to,
            )
            await _select_file_table_row_by_name(
                unit_screen, pilot, wait_until, focus_widget, "Composed Looks", sdk_timeout
            )

            detail = unit_screen.query_one("#detail", Static)
            detail_scroll = unit_screen.query_one("#detail-scroll")
            # set_wide(True) runs synchronously in _show_detail(), before
            # the list-overview fetch this waits on even starts -- once the
            # real content has landed, the wide-preview class is already
            # long since applied, so no extra pause is needed after this.
            await wait_for_detail_content(pilot, unit_screen, contains="item")
            text = str(unit_screen.query_one("#detail", Static).render())
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
    move_cursor_to: Any,
    focus_widget: Any,
    wait_for_detail_content: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> str:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_first_workload_of_type(
                app, pilot, wait_until, focus_widget, ui_timeout, sdk_timeout, connection_config_id=1, type_hint="VM"
            )

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            await wait_until(
                pilot,
                lambda: tree.root.children or (tree.root.data is not None and tree.root.data.payload.is_leaf),
                timeout=sdk_timeout,
                interval=0.03,
            )
            assert tree.root.data is not None
            if tree.root.data.payload.is_leaf:
                # The version root itself is the (only) disk image -- no
                # parent folder to list it in at all, so select it
                # directly rather than searching a file table that will
                # never exist for it.
                await move_cursor_to(pilot, tree, tree.root)
                await pilot.press("enter")
                await wait_for_detail_content(pilot, unit_screen, contains="kind: disk_image")
                return str(unit_screen.query_one("#detail", Static).render())

            # The folder tree only ever shows containers, so a disk image
            # (a leaf) is never reachable by descending node.children[0]
            # alone. Search breadth-first straight through the store
            # (ChildrenRequested + real replayed
            # provider fetches), bypassing the tree widget's own
            # key-press/focus/expand-toggle mechanics entirely -- those
            # are proven separately by this file's sibling tests; this
            # search only needs to locate a real disk image leaf's own
            # parent folder to then drive through the UI for the actual
            # assertion below.
            assert tree.root.data is not None
            root_node = tree.root.data.payload
            queue = [root_node]
            visited = 0
            disk_image_node = None
            found_parent = None
            while queue and disk_image_node is None and visited < 60:
                parent = queue.pop(0)
                visited += 1
                level = unit_screen.store.model.loaded.get(parent.ref)
                if level is None:
                    unit_screen.store.dispatch(ChildrenRequested(node=parent))
                    await wait_until(
                        pilot,
                        lambda ref=parent.ref: (
                            ref in unit_screen.store.model.loaded or ref in unit_screen.store.model.errors
                        ),
                        timeout=5.0,
                        interval=0.03,
                    )
                    level = unit_screen.store.model.loaded.get(parent.ref)
                if level is None:  # a children() failure for this one branch -- skip it, not fatal
                    continue
                disk_image_node = next((c for c in level.children if c.kind == UnitKind.DISK_IMAGE), None)
                if disk_image_node is not None:
                    found_parent = parent
                    break
                # Never descend into a "(filesystem)" sibling itself --
                # its own children are real parsed partitions/files (real
                # dissect parsing, genuinely slow against a real disk
                # image), not another disk image to find; a disk image
                # only ever sits beside one, in the same parent's own
                # listing already just checked above.
                queue.extend(c for c in level.children if not c.is_leaf and c.kind != UnitKind.DISK_FILESYSTEM)
            assert disk_image_node is not None and found_parent is not None, (
                f"no disk image leaf found; visited={visited}"
            )

            # Drive the real UI from here: select the parent folder (as a
            # real folder-tree selection would), then the disk image's
            # own file-table row.
            unit_screen._select_folder_ref(found_parent)
            await wait_until(pilot, lambda: disk_image_node in unit_screen._file_table._nodes, timeout=ui_timeout)
            row_index = unit_screen._file_table._nodes.index(disk_image_node)
            table = unit_screen.query_one("#file-table", DataTable)
            await focus_widget(pilot, table)
            table.cursor_coordinate = Coordinate(row_index, 0)
            await pilot.press("enter")
            await wait_for_detail_content(pilot, unit_screen, contains="kind: disk_image")

            return str(unit_screen.query_one("#detail", Static).render())

    detail_text = asyncio.run(scenario())
    assert "kind: disk_image" in detail_text, detail_text
    assert "─" not in detail_text, "no preview separator should appear for a disk image leaf"


__all__: list[str] = []
