"""``Pilot`` tests of the TUI's Site-list/wide-table/disk-image preview
panes, plus a thread-lifecycle check on Teams-message preview.

Mail/Contact/Calendar/Teams-message preview rendering isn't asserted here;
``tests/unit/browser/test_browser_content_preview.py`` covers those render
functions against synthetic input.

Fixture: ``tui_preview_vault_plain_pilot.json.gz``, recorded against
``vault-plain``.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from functools import partial
from pathlib import Path
from typing import Any

from textual.coordinate import Coordinate
from textual.widgets import DataTable, Static, Tree
from textual.widgets.tree import TreeNode

from integration.browser.pilot_drivers import (
    ReplayLocalStore,
    children_settled,
    drill_to_first_workload_of_type,
    open_browser_pilot,
)
from support.pilot import (
    RUN_TEST_SIZE,
    SDK_TIMEOUT,
    UI_TIMEOUT,
    focus_widget,
    move_cursor_to,
    wait_for_detail_content,
    wait_for_workers,
    wait_until,
)
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.unit.msg import ChildrenRequested
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.units.base import UnitKind


async def _select_first_file_table_row(
    unit_screen: UnitScreen,
    pilot: Any,
) -> None:
    """Selects row 0 of the file table -- the only way to reach a leaf whose
    parent is the root, since the folder tree shows only containers."""
    await wait_until(pilot, lambda: unit_screen._file_table._nodes, timeout=SDK_TIMEOUT, interval=0.03)
    table = unit_screen.query_one("#file-table", DataTable)
    await focus_widget(pilot, table)
    table.cursor_coordinate = Coordinate(0, 0)
    await pilot.press("enter")


def test_previewing_a_teams_channel_then_quitting_does_not_leak_threads_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        await replay_local_store("tui_preview_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_workload_of_type(app, pilot, connection_config_id=1, type_hint="TEAMS")

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            # Teams messages are leaves (UnitKind.TEAMS_CHAT_MESSAGE), so they list in the file table.
            await _select_first_file_table_row(unit_screen, pilot)
            # A content-only preview has an empty header, so only real content
            # (not the transient loading cue) proves the preview loaded.
            await wait_for_detail_content(pilot, unit_screen)

            await pilot.press("q")
            await wait_until(
                pilot, lambda: not app.is_running, timeout=SDK_TIMEOUT, interval=0.02, message="app never shut down"
            )

    before = {t.ident for t in threading.enumerate()}
    asyncio.run(scenario())
    candidates = [t for t in threading.enumerate() if t.ident not in before and not t.daemon]
    # aiosqlite's worker thread can still be alive briefly after close()
    # returns; join() gives it a bounded window to exit.
    for t in candidates:
        t.join(timeout=1.0)
    leaked = [t for t in candidates if t.is_alive()]
    assert not leaked, [t.name for t in leaked]


async def _drill_to_site_list_category(
    app: ApmRepoBrowserApp,
    pilot: Any,
    *,
    connection_config_id: int,
) -> tuple[UnitScreen, TreeNode[object]]:
    """Selects the site root's "List" category tree node. Its individual
    Lists are file-table rows, reached via ``_select_file_table_row_by_name``."""
    await drill_to_first_workload_of_type(app, pilot, connection_config_id=connection_config_id, type_hint="SITE")
    unit_screen = app.screen
    assert isinstance(unit_screen, UnitScreen)
    tree = unit_screen.query_one("#folder-tree", Tree)
    await wait_until(pilot, lambda: tree.root.children, timeout=SDK_TIMEOUT, interval=0.03)
    list_category = next((c for c in tree.root.children if str(c.label) == "List"), None)
    assert list_category is not None, [str(c.label) for c in tree.root.children]
    await move_cursor_to(pilot, tree, list_category)
    await pilot.press("enter")
    await wait_until(pilot, lambda: unit_screen._file_table._nodes, timeout=SDK_TIMEOUT, interval=0.03)
    return unit_screen, list_category


async def _select_file_table_row_by_name(
    unit_screen: UnitScreen,
    pilot: Any,
    name: str,
) -> None:
    """Selects the file-table row whose ``Node.name`` equals ``name``."""
    await wait_until(pilot, lambda: unit_screen._file_table._nodes, timeout=SDK_TIMEOUT, interval=0.03)
    table = unit_screen.query_one("#file-table", DataTable)
    await focus_widget(pilot, table)
    names = [n.name if n is not None else None for n in unit_screen._file_table._nodes]
    row_index = next(i for i, n in enumerate(names) if n == name)
    table.cursor_coordinate = Coordinate(row_index, 0)
    await pilot.press("enter")


def test_site_list_category_has_no_expand_arrow_and_loads_no_tree_children_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[bool, bool, list[str]]:
        await replay_local_store("tui_preview_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, list_category = await _drill_to_site_list_category(app, pilot, connection_config_id=1)
            tree = unit_screen.query_one("#folder-tree", Tree)
            await move_cursor_to(pilot, tree, list_category)
            await pilot.press("l")  # a no-op "expand" attempt — there is nothing to expand
            await wait_for_workers(pilot, node=unit_screen)  # any load the key started is over

            names = [n.name for n in unit_screen._file_table._nodes if n is not None]
            return list_category.allow_expand, list_category.is_expanded, names

    allow_expand, expanded, names = asyncio.run(scenario())
    assert allow_expand is False
    assert expanded is False
    assert "Access Requests" in names, names


def test_site_list_group_shows_a_spreadsheet_overview_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> str:
        await replay_local_store("tui_preview_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, _list_category = await _drill_to_site_list_category(app, pilot, connection_config_id=1)
            await _select_file_table_row_by_name(unit_screen, pilot, "Access Requests")

            await wait_for_detail_content(pilot, unit_screen, contains="1 item")
            return str(unit_screen.query_one("#detail", Static).render())

    detail_text = asyncio.run(scenario())
    # Structure only: the row count and a column header, not a row's content.
    assert "1 item" in detail_text, detail_text
    assert "Conversation" in detail_text, detail_text


def test_wide_list_overview_gets_a_pannable_pane_not_a_wrapped_one_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[str, frozenset[str], bool]:
        await replay_local_store("tui_preview_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            unit_screen, _list_category = await _drill_to_site_list_category(app, pilot, connection_config_id=1)
            await _select_file_table_row_by_name(unit_screen, pilot, "Composed Looks")

            detail = unit_screen.query_one("#detail", Static)
            detail_scroll = unit_screen.query_one("#detail-scroll")
            # The wide-preview class is applied before the fetch starts, so
            # it is set once content has landed.
            await wait_for_detail_content(pilot, unit_screen, contains="item")
            # virtual_size follows on a later layout pass than the text update.
            with contextlib.suppress(TimeoutError):
                await wait_until(
                    pilot,
                    lambda: detail_scroll.virtual_size.width > detail_scroll.size.width,
                    timeout=UI_TIMEOUT,
                    interval=0.02,
                )
            text = str(unit_screen.query_one("#detail", Static).render())
            return text, detail.classes, detail_scroll.virtual_size.width > detail_scroll.size.width

    detail_text, detail_classes, is_wider_than_viewport = asyncio.run(scenario())
    assert "wide-preview" in detail_classes, detail_classes
    assert is_wider_than_viewport, "a table this wide should need horizontal scrolling"
    box_drawing_lines = [line for line in detail_text.splitlines() if line and line[0] in "┏┡└│┃┗┓┛├┤┬┴┼"]
    assert box_drawing_lines, detail_text
    assert len({len(line) for line in box_drawing_lines}) == 1, detail_text


def test_disk_image_leaf_shows_no_preview_just_the_header_block_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> str:
        await replay_local_store("tui_preview_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_first_workload_of_type(app, pilot, connection_config_id=1, type_hint="VM")

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            await wait_until(
                pilot,
                lambda: tree.root.children or (tree.root.data is not None and tree.root.data.payload.is_leaf),
                timeout=SDK_TIMEOUT,
                interval=0.03,
            )
            assert tree.root.data is not None
            if tree.root.data.payload.is_leaf:
                # The version root itself is the disk image; select it directly.
                await move_cursor_to(pilot, tree, tree.root)
                await pilot.press("enter")
                await wait_for_detail_content(pilot, unit_screen, contains="kind: disk_image")
                return str(unit_screen.query_one("#detail", Static).render())

            # The folder tree shows only containers, so search breadth-first
            # through the store (ChildrenRequested) for the disk image's parent.
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
                        partial(children_settled, unit_screen, parent.ref),
                        timeout=SDK_TIMEOUT,
                        interval=0.03,
                    )
                    level = unit_screen.store.model.loaded.get(parent.ref)
                if level is None:  # a children() failure for this one branch -- skip it, not fatal
                    continue
                disk_image_node = next((c for c in level.children if c.kind == UnitKind.DISK_IMAGE), None)
                if disk_image_node is not None:
                    found_parent = parent
                    break
                # Skip "(filesystem)" siblings: parsing them is slow and the
                # disk image sits beside them, already checked above.
                queue.extend(c for c in level.children if not c.is_leaf and c.kind != UnitKind.DISK_FILESYSTEM)
            assert disk_image_node is not None and found_parent is not None, (
                f"no disk image leaf found; visited={visited}"
            )

            # Drive the UI: select the parent folder, then the disk image's row.
            unit_screen._select_folder_ref(found_parent)
            await wait_until(pilot, lambda: disk_image_node in unit_screen._file_table._nodes, timeout=UI_TIMEOUT)
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
