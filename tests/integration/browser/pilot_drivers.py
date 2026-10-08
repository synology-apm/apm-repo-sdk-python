"""Multi-step ``Pilot`` drives over a replayed real repository, shared by the
``test_*.py`` files here."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial
from pathlib import Path
from typing import Any

from textual.coordinate import Coordinate
from textual.pilot import Pilot
from textual.widgets import Button, DataTable, Input, Tree
from textual.widgets.tree import TreeNode

from support.pilot import SDK_TIMEOUT, UI_TIMEOUT, focus_widget, move_cursor_to, wait_for_screen, wait_until
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.unit.msg import ChildrenRequested
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Catalog, Workload
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

#: ``tests/integration/browser/conftest.py``'s ``replay_local_store`` fixture.
ReplayLocalStore = Callable[..., Awaitable[None]]


async def open_browser_pilot(app: ApmRepoBrowserApp, pilot: Pilot[Any], path: Path) -> None:
    """Wait for the auto-opened ``ConnectDialog``, drive it to open the local repository at
    ``path``, then expand the first repository, so ``BrowseScreen``'s
    ``#col-catalogs`` is populated with the cursor on its first connection."""
    dialog = await wait_for_screen(pilot, ConnectDialog)
    dialog.query_one("#connect-local-path", Input).value = str(path)
    dialog.query_one("#connect-submit", Button).press()
    await wait_until(
        pilot,
        lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
        timeout=SDK_TIMEOUT,
        interval=0.2,
        message="BrowseScreen never appeared",
    )
    # BrowseScreen never auto-expands a repository (catalog fetching is
    # deferred), so expand the first one the way a user would.
    tree = app.screen.query_one("#col-catalogs", Tree)
    tree.focus()
    # The screen mounting doesn't mean the worker populating the tree is done.
    await wait_until(
        pilot, lambda: tree.root.children, timeout=SDK_TIMEOUT, interval=0.2, message="#col-catalogs never populated"
    )
    await move_cursor_to(pilot, tree, tree.root.children[0])
    await pilot.press("enter")
    await wait_until(
        pilot,
        lambda: tree.root.children[0].children,
        timeout=SDK_TIMEOUT,
        interval=0.2,
        message="first connection's children never populated",
    )


async def drill_to_unit_screen_via_fs_device(app: ApmRepoBrowserApp, pilot: Pilot[Any]) -> None:
    """From a populated ``BrowseScreen`` (see ``open_browser_pilot``), open the
    first version of connection 1's first FS workload in a ``UnitScreen``.

    FS content never goes through ``target.db``, unlike that connection's VM
    device (whose real version lacks one), and a connection id is immune to
    display-name anonymization reordering the groups.
    """
    browse = app.screen
    assert isinstance(browse, BrowseScreen), browse
    cat_tree = browse.query_one("#col-catalogs", Tree)
    repo_node = cat_tree.root.children[0]
    connection_node = next(
        n
        for n in repo_node.children
        if n.data is not None
        and isinstance(n.data.payload, Catalog)
        and n.data.payload.connection.connection_config_id == 1
    )
    await move_cursor_to(pilot, cat_tree, connection_node)
    await focus_widget(pilot, cat_tree)
    await pilot.press("enter")

    wl_tree = browse.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: wl_tree.root.children, timeout=SDK_TIMEOUT)
    fs_group = next(n for n in wl_tree.root.children if str(n.label) == "FS")
    # Only the first group auto-expands, and "FS" isn't always first.
    fs_group.expand()
    await wait_until(pilot, lambda: fs_group.children, timeout=SDK_TIMEOUT)
    await move_cursor_to(pilot, wl_tree, fs_group.children[0])
    await focus_widget(pilot, wl_tree)
    await pilot.press("enter")
    await _open_first_version(app, pilot, browse)


async def drill_to_first_unit_screen(app: ApmRepoBrowserApp, pilot: Pilot[Any]) -> None:
    """From a populated ``BrowseScreen``, open the first version of the first
    workload under the cursor's connection in a ``UnitScreen``."""
    browse = app.screen
    assert isinstance(browse, BrowseScreen), browse
    await focus_widget(pilot, browse.query_one("#col-catalogs", Tree))
    await pilot.press("enter")
    workloads_tree = browse.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: workloads_tree.root.children, timeout=SDK_TIMEOUT)
    await focus_widget(pilot, workloads_tree)
    await pilot.press("enter")
    await _open_first_version(app, pilot, browse)


async def drill_to_first_workload_of_type(
    app: ApmRepoBrowserApp, pilot: Pilot[Any], *, connection_config_id: int, type_hint: str
) -> None:
    """From a populated ``BrowseScreen``, open in a ``UnitScreen`` the first
    version of the first workload with ``type_hint`` under the connection
    with ``connection_config_id``, expanding the groups above it."""
    browse = app.screen
    assert isinstance(browse, BrowseScreen), browse
    cat_tree = browse.query_one("#col-catalogs", Tree)
    connection_node = next(
        (
            n
            for repo_node in cat_tree.root.children
            for n in repo_node.children
            if n.data is not None
            and isinstance(n.data.payload, Catalog)
            and n.data.payload.connection.connection_config_id == connection_config_id
        ),
        None,
    )
    assert connection_node is not None, f"no connection with connection_config_id {connection_config_id!r}"
    await move_cursor_to(pilot, cat_tree, connection_node)
    await focus_widget(pilot, cat_tree)
    await pilot.press("enter")

    wl_tree = browse.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: wl_tree.root.children, timeout=SDK_TIMEOUT)
    path = _path_to_workload(wl_tree.root, type_hint)
    assert path is not None, f"no workload with type_hint {type_hint!r}"
    for ancestor in path[:-1]:
        ancestor.expand()
    await move_cursor_to(pilot, wl_tree, path[-1])
    await focus_widget(pilot, wl_tree)
    await pilot.press("enter")
    await _open_first_version(app, pilot, browse)


def _path_to_workload(node: TreeNode[Any], type_hint: str) -> list[TreeNode[Any]] | None:
    """The nodes from below ``node`` down to the first ``Workload`` with ``type_hint``, depth first."""
    for child in node.children:
        payload = child.data.payload if child.data is not None else None
        if isinstance(payload, Workload) and payload.type_hint == type_hint:
            return [child]
        if (found := _path_to_workload(child, type_hint)) is not None:
            return [child, *found]
    return None


def version_rows_ready(app: ApmRepoBrowserApp) -> bool:
    """Whether ``BrowseScreen``'s versions table holds the selected workload's
    rows: ``row_count`` can still reflect the previous workload's, so the
    visible version indices, which gate a real selection, decide."""
    screen = app.screen
    return (
        isinstance(screen, BrowseScreen)
        and screen.query_one("#col-versions", DataTable).row_count > 0
        and bool(screen._visible_version_indices)
    )


async def _open_first_version(app: ApmRepoBrowserApp, pilot: Pilot[Any], browse: BrowseScreen) -> None:
    versions_table = browse.query_one("#col-versions", DataTable)
    await wait_until(pilot, lambda: version_rows_ready(app), timeout=SDK_TIMEOUT)
    await focus_widget(pilot, versions_table)
    await pilot.press("enter")
    await wait_until(
        pilot, lambda: isinstance(app.screen, UnitScreen) and app.screen.is_mounted, timeout=UI_TIMEOUT, interval=0.03
    )
    assert isinstance(app.screen, UnitScreen), app.screen


def children_settled(unit_screen: UnitScreen, ref: NodeRef) -> bool:
    """Whether ``ref``'s ``ChildrenRequested`` has loaded or failed."""
    return ref in unit_screen.store.model.loaded or ref in unit_screen.store.model.errors


async def find_first_leaf(
    app: ApmRepoBrowserApp, pilot: Pilot[Any], *, max_depth: int = 10, require_size: bool = False
) -> tuple[Node, Node]:
    """The first leaf (with a declared ``size``, if ``require_size``) of the
    current ``UnitScreen``'s version, and its parent.

    A backtracking DFS through ``ChildrenRequested`` on the store: the folder
    tree shows only containers, and a greedy ``children[0]`` walk can dead-end
    in an empty subdirectory, since directories sort before files. A branch
    whose ``children()`` fails is skipped. ``max_depth`` bounds the walk, and
    with it the replayed calls a fixture must cover.
    """
    unit_screen = app.screen
    assert isinstance(unit_screen, UnitScreen)
    await wait_until(pilot, lambda: unit_screen.store.model.root is not None, timeout=SDK_TIMEOUT)
    root = unit_screen.store.model.root
    assert root is not None and not root.is_leaf, "version root is itself a leaf -- no parent to select it under"

    async def search(node: Node, depth: int) -> tuple[Node, Node] | None:
        if depth >= max_depth:
            return None
        level = unit_screen.store.model.loaded.get(node.ref)
        if level is None:
            unit_screen.store.dispatch(ChildrenRequested(node=node))
            await wait_until(pilot, partial(children_settled, unit_screen, node.ref), timeout=SDK_TIMEOUT)
            level = unit_screen.store.model.loaded.get(node.ref)
        if level is None:
            return None
        leaf = next((c for c in level.children if c.is_leaf and (c.size is not None or not require_size)), None)
        if leaf is not None:
            return leaf, node
        for child in level.children:
            if not child.is_leaf and (found := await search(child, depth + 1)) is not None:
                return found
        return None

    found = await search(root, 0)
    assert found is not None, "no leaf found"
    return found


async def select_leaf_row(unit_screen: UnitScreen, pilot: Pilot[Any], leaf: Node, parent: Node) -> None:
    """Focus the file table with its cursor on ``leaf`` (no Enter): cursor plus
    focus is what ``UnitScreen._selected_node()`` reads."""
    unit_screen._select_folder_ref(parent)
    file_table = unit_screen._file_table
    await wait_until(pilot, lambda: leaf in file_table._nodes)
    table = unit_screen.query_one("#file-table", DataTable)
    await focus_widget(pilot, table)
    table.cursor_coordinate = Coordinate(file_table._nodes.index(leaf), 0)


async def select_first_leaf(
    app: ApmRepoBrowserApp, pilot: Pilot[Any], *, max_depth: int = 10, require_size: bool = False
) -> Node:
    """``find_first_leaf`` then ``select_leaf_row``; returns the leaf."""
    leaf, parent = await find_first_leaf(app, pilot, max_depth=max_depth, require_size=require_size)
    assert isinstance(app.screen, UnitScreen)
    await select_leaf_row(app.screen, pilot, leaf, parent)
    return leaf
