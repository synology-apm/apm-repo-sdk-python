"""``textual`` ``Pilot``-driven coverage — ``x`` hex preview, ``r`` refresh,
``/`` filter, and the debounced progress hint — against committed
fixtures recorded from real sample data, with **no real ``samples_dir``
dependency**.

Every apv-sample-1 scenario shares this file's own dedicated
``tui_hex_filter_refresh_apv1_pilot.json.gz``; the one
``s3-sample-2-encrypted`` scenario (``r`` refresh) has its own dedicated
``tui_hex_filter_refresh_s3sample2_pilot.json.gz`` — see
``tests/conftest.py``/``tests/CLAUDE.md`` for how to re-record either.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from textual.coordinate import Coordinate
from textual.widgets import Button, DataTable, Input, Static, Tree

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.unit.msg import ChildrenRequested
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Catalog, Repository, Version
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.recording import ReplayStore
from synology_apm_repo.sdk.units.base import Node, UnitProvider

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures"
_APV1_FIXTURE = _FIXTURES / "tui_hex_filter_refresh_apv1_pilot.json.gz"


def _unique_needle(names: list[str], index: int) -> str:
    """The shortest substring of ``names[index]`` that doesn't occur
    (case-insensitively) in any other entry of ``names`` -- lets a filter
    test prove real narrowing happened without hardcoding a
    fixture-specific literal that a re-recording could invalidate.
    """
    target = names[index]
    others = [name.lower() for i, name in enumerate(names) if i != index]
    for length in range(1, len(target) + 1):
        for start in range(len(target) - length + 1):
            candidate = target[start : start + length]
            if not any(candidate.lower() in other for other in others):
                return candidate
    raise AssertionError(f"no substring of {target!r} discriminates it from {names!r}")


def _fake_build_local_store_for(fixture: Path, label: str) -> Any:
    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return ReplayStore.from_path(fixture), label

    return _fake


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch,
    record_target: Callable[..., Awaitable[ObjectStore]],
    *,
    allow_content: bool = False,
) -> None:
    """``--record-against``-aware equivalent of ``_fake_build_local_store_for``
    -- used only by the tests below that also need
    ``drill_to_unit_screen_via_fs_device``'s fix of always targeting one
    specific, confirmed-good workload (the ``connection_config_id`` 1 FS
    device) rather than whatever a cursor-default "press enter" lands on
    first -- avoiding both a VM device whose ``target.db`` was never
    captured and a device whose sort/group order shifts on every
    re-anonymization; every other test in this file
    still uses the plain ``_fake_build_local_store_for(_APV1_FIXTURE,
    ...)`` helper above and doesn't yet support ``--record-against``."""
    store = await record_target("tui_hex_filter_refresh_apv1_pilot.json.gz", allow_content=allow_content)

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, "apv-sample-1"

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


async def _first_leaf(
    app: ApmRepoBrowserApp,
    pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> Node:
    """DFS with backtracking, not a blind ``children[0]`` walk: since
    ``units/fs.py`` sorts directories before files, a real directory's own
    *first* child (alphabetically first among its subdirectories) can be
    an empty one at any depth -- an ordinary real filesystem fact -- which
    a greedy walk would dead-end into. Driven straight through the store
    (``ChildrenRequested`` + real replayed provider fetches), not the
    folder-tree widget: the tree only ever shows containers now, so a
    leaf can never be found by walking it.

    Also points the file table's own cursor at the leaf found (focus +
    cursor position, no Enter press) before returning, so a caller can
    press ``x``/whatever key it needs immediately."""
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
    leaf, parent = found

    unit_screen._select_folder_ref(parent)
    await wait_until(pilot, lambda: leaf in unit_screen._file_table._nodes, timeout=ui_timeout, interval=0.02)
    row_index = unit_screen._file_table._nodes.index(leaf)
    table = unit_screen.query_one("#file-table", DataTable)
    await focus_widget(pilot, table)
    table.cursor_coordinate = Coordinate(row_index, 0)
    return leaf


def test_hex_preview_blocked_outside_diagnostic_mode_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    drill_to_unit_screen_via_fs_device: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> bool:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)
            await _first_leaf(app, pilot, wait_until, focus_widget, move_cursor_to, ui_timeout, sdk_timeout)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            # action_hex_preview's own outside-verbose-mode branch is a
            # synchronous no-op that never pushes a screen, so there's no
            # UI state to wait_until() on for "x was blocked" -- the
            # warning it does fire (the same real signal
            # test_hex_preview_outside_verbose_mode_is_a_no_op_warning
            # asserts on directly) is what this waits for, proving the
            # key was actually recognized and correctly rejected, not
            # just that the screen hadn't changed yet.
            warnings: list[str] = []
            unit_screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
            await pilot.press("x")
            await wait_until(pilot, lambda: warnings, timeout=ui_timeout, interval=0.02)
            assert warnings == ["press d to enable verbose mode first"]
            return isinstance(app.screen, UnitScreen)

    still_on_unit_screen = asyncio.run(scenario())
    assert still_on_unit_screen


def test_hex_preview_pages_forward_and_back_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    drill_to_unit_screen_via_fs_device: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    """Also covers plain "opens for a leaf in diagnostic mode" on its
    own (the ``initial`` assertion below) -- there is no separate test
    for that alone, since this one's own setup is identical."""

    async def scenario() -> tuple[str, str, str]:
        # allow_content=True: this test's whole point is the hex-preview
        # feature actually reading and rendering a real file's own bytes.
        await _patch_local_store(monkeypatch, record_target, allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)
            await _first_leaf(app, pilot, wait_until, focus_widget, move_cursor_to, ui_timeout, sdk_timeout)

            def dump() -> str:
                return str(app.screen.query_one("#hex-dump").render())

            # ``d`` re-dispatches the provider and reloads the whole tree
            # (UnitScreen.refresh_for_verbose_mode), so the leaf picked above
            # no longer exists afterwards -- re-acquire one rather than press
            # ``x`` into a tree that is still being rebuilt.
            await pilot.press("d")
            await wait_until(
                pilot, lambda: app.verbose, timeout=ui_timeout, interval=0.02, message="verbose never turned on"
            )
            await _first_leaf(app, pilot, wait_until, focus_widget, move_cursor_to, ui_timeout, sdk_timeout)

            await pilot.press("x")
            await wait_until(
                pilot, lambda: isinstance(app.screen, HexPreviewScreen), timeout=sdk_timeout, interval=0.02
            )
            assert isinstance(app.screen, HexPreviewScreen), app.screen
            initial = dump()

            # The rendered window changing *is* the readiness signal for a
            # page turn; there is no state flag that says "the next page has
            # landed", and a fixed pause would only be guessing at it.
            await pilot.press("x")  # page forward
            await wait_until(
                pilot, lambda: dump() != initial, timeout=sdk_timeout, interval=0.02, message="never paged forward"
            )
            forward = dump()

            await pilot.press("X")  # page back to the original window
            await wait_until(
                pilot, lambda: dump() != forward, timeout=sdk_timeout, interval=0.02, message="never paged back"
            )
            back = dump()

            return initial, forward, back

    initial, forward, back = asyncio.run(scenario())
    assert initial.startswith("00000000"), initial
    assert forward.startswith("00000200"), forward  # HEX_WINDOW_SIZE (512) in hex
    assert back == initial


def test_unit_screen_filter_narrows_then_esc_restores_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    drill_to_unit_screen_via_fs_device: Any,
    wait_for_filter_closed: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[int, str, list[str], int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            await wait_until(pilot, lambda: tree.root.children, timeout=sdk_timeout, interval=0.03)
            full_count = len(tree.root.children)
            assert full_count > 0, "root has no children to filter"

            needle = str(tree.root.children[0].label)[:3]
            await move_cursor_to(pilot, tree, tree.root)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#filter-input"),
                timeout=ui_timeout,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = unit_screen.query_one("#filter-input", Input)
            # The filter rebuild is debounced after the last keystroke (see
            # widgets/filter_debounce.py -- and tests/conftest.py's
            # fast_browser_debounce for why this test session's delay isn't
            # literally its 0.3s production default) -- wait for the
            # model's own committed filter text rather than a fixed pause
            # shorter than the debounce. A still-matching node (``needle``
            # is a prefix of the first child's own label, by construction
            # above) keeps its own TreeNode object across this re-render --
            # reconcile_children keys survivors by domain identity rather
            # than position -- so "every node replaced" isn't a reliable
            # signal to wait for here.
            filter_input.value = needle
            await wait_until(
                pilot,
                lambda: unit_screen.store.model.filter is not None and unit_screen.store.model.filter.text == needle,
                timeout=ui_timeout,
                interval=0.02,
                message="debounced filter never committed",
            )
            filtered_labels = [str(c.label) for c in tree.root.children]

            await pilot.press("escape")
            await wait_for_filter_closed(pilot, unit_screen)
            restored_count = len(tree.root.children)

            return full_count, needle, filtered_labels, restored_count

    full_count, needle, filtered_labels, restored_count = asyncio.run(scenario())
    assert 0 < len(filtered_labels) <= full_count
    # Every surviving node's label actually contains the needle
    # (case-insensitively, matching core/unit/select.py's own
    # ``_needle_for``/``_folder_node_spec`` ``.lower()`` matching) -- a
    # regression that narrowed to the right *count* but the wrong
    # *nodes* would still pass a count-only bound.
    assert all(needle.lower() in label.lower() for label in filtered_labels), filtered_labels
    assert restored_count == full_count


def test_browse_screen_filter_narrows_connections_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    move_cursor_to: Any,
    wait_for_filter_closed: Any,
    ui_timeout: float,
) -> None:
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )

    async def scenario() -> tuple[int, str, list[str], int]:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            tree = app.screen.query_one("#col-catalogs", Tree)
            await focus_widget(pilot, tree)
            repo_node = tree.root.children[0]
            full_count = len(repo_node.children)
            assert full_count >= 1

            # A naive `first_name[:4]`-style needle is the exact anti-pattern
            # to avoid: if every connection's anonymized display name happens
            # to share a prefix, it matches every row and the filter could
            # silently do nothing while every assertion below still passed.
            # _unique_needle picks a substring that matches only the target
            # row.
            names = [str(c.label) for c in repo_node.children]
            needle = _unique_needle(names, 0)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: app.screen.query("#filter-input"),
                timeout=ui_timeout,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = app.screen.query_one("#filter-input", Input)
            filter_input.value = needle
            # The filter rebuild is debounced (see widgets/filter_debounce.py);
            # `needle` matches exactly one row, so waiting for the tree to
            # actually narrow to that one row is itself the real completion
            # signal.
            await wait_until(
                pilot,
                lambda: len(repo_node.children) == 1,
                timeout=ui_timeout,
                interval=0.02,
                message="debounced filter never narrowed the connection list",
            )
            filtered_labels = [str(c.label) for c in repo_node.children]

            await pilot.press("escape")
            await wait_for_filter_closed(pilot, app.screen)
            restored_count = len(repo_node.children)

            return full_count, needle, filtered_labels, restored_count

    full_count, needle, filtered_labels, restored_count = asyncio.run(scenario())
    assert 0 < len(filtered_labels) <= full_count
    # Every surviving connection's label actually contains the needle
    # (case-insensitively, matching core/browse/select.py's own
    # ``.lower()`` needle match) -- a regression that narrowed to the
    # right *count* but the wrong *nodes* would still pass a
    # count-only bound.
    assert all(needle.lower() in label.lower() for label in filtered_labels), filtered_labels
    assert restored_count == full_count


def test_browse_screen_version_filter_enter_closes_it_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )

    async def scenario() -> tuple[int, str, str, list[str], int, bool, bool]:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            await focus_widget(pilot, app.screen.query_one("#col-catalogs", Tree))
            await pilot.press("enter")
            workloads_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: workloads_tree.root.children, timeout=sdk_timeout, interval=0.02)
            await focus_widget(pilot, workloads_tree)
            await pilot.press("enter")
            versions_table = app.screen.query_one("#col-versions", DataTable)
            # row_count alone can be stale; wait for _visible_version_indices
            # too (see tests/conftest.py's drill_to_unit_screen_via_fs_device).
            await wait_until(
                pilot,
                lambda: versions_table.row_count and app.screen._visible_version_indices,
                timeout=sdk_timeout,
                interval=0.02,
            )
            await focus_widget(pilot, versions_table)
            all_names = [str(versions_table.get_row_at(i)[0]) for i in range(versions_table.row_count)]
            full_count = len(all_names)
            assert full_count >= 1

            # `_unique_needle` picks a substring that matches row 0 and
            # nothing else -- this real fixture's version display names
            # all share a common date-based prefix, so a naive
            # `first_name[:4]`-style needle would match every row and the
            # filter could silently do nothing while every assertion below
            # still passed.
            expected_name = all_names[0]
            needle = _unique_needle(all_names, 0)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: app.screen.query("#filter-input"),
                timeout=ui_timeout,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = app.screen.query_one("#filter-input", Input)
            filter_input.value = needle
            # The filter rebuild is debounced (see widgets/filter_debounce.py);
            # `needle` matches exactly one row, so waiting for the table to
            # actually narrow to that one row is itself the real completion
            # signal.
            await wait_until(
                pilot,
                lambda: versions_table.row_count == 1,
                timeout=ui_timeout,
                interval=0.02,
                message="debounced filter never narrowed the table",
            )
            filtered_names = [str(versions_table.get_row_at(i)[0]) for i in range(versions_table.row_count)]

            await pilot.press("enter")
            # FilterFieldController.close() removes #filter-input's "active"
            # class, restores the full list, and (its own post_close)
            # refocuses #col-versions -- all synchronously, in that order,
            # within one call. Waiting for the last of the three (rather
            # than re-deriving the same "active" check the assertions below
            # already make) still proves the other two already happened,
            # without the wait itself being the very thing being asserted.
            await wait_until(
                pilot,
                lambda: versions_table.has_focus,
                timeout=ui_timeout,
                interval=0.02,
                message="versions table never regained focus after the filter closed",
            )
            restored_count = versions_table.row_count
            input_still_active = filter_input.has_class("active")
            table_focused = versions_table.has_focus

            return full_count, expected_name, needle, filtered_names, restored_count, input_still_active, table_focused

    full_count, expected_name, needle, filtered_names, restored_count, input_still_active, table_focused = asyncio.run(
        scenario()
    )
    # Exactly the one row `needle` was built to match -- a regression that
    # narrowed to the right *count* but the wrong *row*, or didn't narrow
    # at all, would both be caught here (unlike a shared-prefix needle,
    # which every row would match regardless of whether filtering ran).
    assert filtered_names == [expected_name], (needle, filtered_names)
    assert restored_count == full_count
    assert not input_still_active
    assert table_focused


def test_browse_screen_backspace_jumps_cursor_to_parent_and_collapses_it_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
) -> None:
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )

    async def scenario() -> tuple[bool, bool, bool, bool]:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            tree = app.screen.query_one("#col-catalogs", Tree)
            await focus_widget(pilot, tree)
            repo_node = tree.root.children[0]
            assert repo_node.children, "expected at least one connection under the repository"
            connection_node = repo_node.children[0]
            await move_cursor_to(pilot, tree, connection_node)

            await pilot.press("backspace")
            await wait_until(
                pilot, lambda: tree.cursor_node is repo_node, timeout=ui_timeout, interval=0.02, message="never went up"
            )
            landed_on_repo = tree.cursor_node is repo_node
            repo_collapsed = not repo_node.is_expanded

            await pilot.press("backspace")
            await wait_until(
                pilot, lambda: tree.cursor_node is tree.root, timeout=ui_timeout, interval=0.02, message="never went up"
            )
            landed_on_root = tree.cursor_node is tree.root
            root_still_expanded = tree.root.is_expanded

            return landed_on_repo, repo_collapsed, landed_on_root, root_still_expanded

    landed_on_repo, repo_collapsed, landed_on_root, root_still_expanded = asyncio.run(scenario())
    assert landed_on_repo
    assert repo_collapsed
    assert landed_on_root
    assert root_still_expanded, "the tree's own permanent root must never collapse"


def test_browse_screen_refresh_rescans_the_same_path_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    ui_timeout: float,
) -> None:
    """``s3-sample-2-encrypted``'s two sibling repo-ids discover as one
    ``Repository`` (holding both as ``catalogs()``), so the repository
    count this test checks before/after refresh is 1 — the behavior
    under test (a refresh rescans and finds the same thing again)
    doesn't depend on that count."""

    async def scenario() -> tuple[int, int]:
        store = await record_target("tui_hex_filter_refresh_s3sample2_pilot.json.gz")
        monkeypatch.setattr(
            connect_dialog_module.ConnectDialog,
            "_build_local_store",
            lambda self: (store, "s3-sample-2-encrypted"),
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            tree = app.screen.query_one("#col-catalogs", Tree)
            first_count = len(tree.root.children)

            await pilot.press("r")
            await wait_until(pilot, lambda: isinstance(app.screen, ConnectDialog), timeout=ui_timeout, interval=0.02)
            assert isinstance(app.screen, ConnectDialog), "refresh with nothing selected must reopen ConnectDialog"
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            second_count = len(tree.root.children)
            return first_count, second_count

    first_count, second_count = asyncio.run(scenario())
    assert first_count == second_count == 1


def test_unit_screen_refresh_reloads_the_tree_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    drill_to_unit_screen_via_fs_device: Any,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[int, int, bool]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            await wait_until(pilot, lambda: tree.root.children, timeout=sdk_timeout, interval=0.03)
            before_count = len(tree.root.children)
            provider_before = unit_screen._current_provider()

            await pilot.press("r")
            await wait_until(pilot, lambda: tree.root.children, timeout=sdk_timeout, interval=0.03)
            after_count = len(tree.root.children)
            provider_replaced = unit_screen._current_provider() is not provider_before

            return before_count, after_count, provider_replaced

    before_count, after_count, provider_replaced = asyncio.run(scenario())
    assert before_count == after_count > 0
    assert provider_replaced


def test_loading_indicator_never_appears_for_a_fast_catalog_query_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
) -> None:
    """A fast catalog-level query (real local storage) settles well under
    the 300ms debounce delay — the repo node's own label (this call site's
    loading target, since ``_load_catalogs_for`` is a genuine tree-node
    expand — see ``TreeNodeLoadingSink``) must never show a Loading suffix,
    and the breadcrumb (a different call site's target) must not either."""
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )

    async def scenario() -> tuple[bool, bool]:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            repo_node = app.screen.query_one("#col-catalogs", Tree).root.children[0]
            assert repo_node.children
            node_hint = "loading" in str(repo_node.label).lower()
            breadcrumb_hint = "loading" in str(app.screen.query_one("#breadcrumb", Static).render()).lower()
            return node_hint, breadcrumb_hint

    node_hint_appeared, breadcrumb_hint_appeared = asyncio.run(scenario())
    assert not node_hint_appeared, "the repo node's label must never show a Loading suffix for a fast query"
    assert not breadcrumb_hint_appeared, "the breadcrumb must never show a Loading suffix for a catalog-level query"


def test_loading_indicator_appears_on_the_repo_node_label_for_a_slow_catalog_query_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    sdk_timeout: float,
) -> None:
    """``_load_catalogs_for`` (a real tree-node expand) shows its loading
    hint on the expanding repo node's own label, not the breadcrumb — the
    opposite target from ``_load_root``'s own loading hint (covered by
    ``test_loading_indicator_appears_for_an_operation_slower_than_300ms_replayed``
    below)."""
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )
    original_catalogs = Repository.catalogs

    async def slow_catalogs(self: Repository) -> Any:
        # Comfortably past the debounce delay (0.3s in production, sped up
        # for this whole test session -- see tests/integration/browser/
        # conftest.py's own autouse fixture) either way, so the indicator
        # has ample real time both to appear and to still be showing by the
        # time the wait_until below checks for it, regardless of real
        # scheduling load.
        await asyncio.sleep(3.0)
        return await original_catalogs(self)

    async def scenario() -> None:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            assert isinstance(app.screen, ConnectDialog), app.screen
            dialog = app.screen
            dialog.query_one("#connect-local-path", Input).value = str(tmp_path)
            dialog.query_one("#connect-submit", Button).press()
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen),
                timeout=sdk_timeout,
                message="BrowseScreen never appeared",
            )
            tree = app.screen.query_one("#col-catalogs", Tree)
            await wait_until(
                pilot, lambda: tree.root.children, timeout=sdk_timeout, message="#col-catalogs never populated"
            )
            repo_node = tree.root.children[0]
            _ = tree._tree_lines  # forces the line map to rebuild; see move_cursor_to's docstring
            tree.move_cursor(repo_node)
            await pilot.press("enter")

            await wait_until(
                pilot,
                lambda: "loading" in str(repo_node.label).lower(),
                timeout=2.0,
                interval=0.02,
                message="the repo node's label must show a Loading suffix once its own catalog query "
                "runs past the debounce delay",
            )
            breadcrumb_hint = "loading" in str(app.screen.query_one("#breadcrumb", Static).render()).lower()
            assert not breadcrumb_hint, "a tree-node-expand's own loading hint must not also touch the breadcrumb"

    Repository.catalogs = slow_catalogs  # type: ignore[method-assign]
    try:
        asyncio.run(scenario())
    finally:
        Repository.catalogs = original_catalogs  # type: ignore[method-assign]


def test_loading_indicator_appears_for_an_operation_slower_than_300ms_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    sdk_timeout: float,
) -> None:
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )
    original_provider = Catalog.provider

    async def slow_provider(
        self: Catalog, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> UnitProvider:
        # Comfortably past the debounce delay either way -- see
        # test_loading_indicator_appears_on_the_repo_node_label_for_a_slow_catalog_query_replayed's
        # own slow_catalogs above for why.
        await asyncio.sleep(3.0)
        return await original_provider(self, version, object_db_id=object_db_id, force_raw=force_raw)

    async def scenario() -> None:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            await focus_widget(pilot, app.screen.query_one("#col-catalogs", Tree))
            await pilot.press("enter")
            workloads_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: workloads_tree.root.children, timeout=sdk_timeout, interval=0.02)
            await focus_widget(pilot, workloads_tree)
            await pilot.press("enter")
            versions_table = app.screen.query_one("#col-versions", DataTable)
            # row_count alone can be stale; wait for _visible_version_indices
            # too (see tests/conftest.py's drill_to_unit_screen_via_fs_device).
            await wait_until(
                pilot,
                lambda: versions_table.row_count and app.screen._visible_version_indices,
                timeout=sdk_timeout,
                interval=0.02,
            )
            await focus_widget(pilot, versions_table)
            await pilot.press("enter")

            def _shows_loading_hint() -> bool:
                return (
                    isinstance(app.screen, UnitScreen)
                    and "loading" in str(app.screen.query_one("#breadcrumb", Static).render()).lower()
                )

            await wait_until(
                pilot,
                _shows_loading_hint,
                timeout=2.0,
                interval=0.02,
                message="the breadcrumb must show a Loading suffix once an operation runs past the debounce delay",
            )

    Catalog.provider = slow_provider  # type: ignore[method-assign]
    try:
        asyncio.run(scenario())
    finally:
        Catalog.provider = original_provider  # type: ignore[method-assign]


__all__: list[str] = []
