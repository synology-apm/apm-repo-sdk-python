"""``Pilot`` tests for ``x`` hex preview, ``r`` refresh, ``/`` filter, and the
debounced loading hint.

Fixtures and the root each is recorded against:

- ``tui_hex_filter_refresh_vault_plain_pilot.json.gz`` — ``vault-plain``.
- ``tui_hex_filter_refresh_objstore_encrypted_pilot.json.gz`` —
  ``objstore-encrypted``.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any

import pytest
from textual.widgets import Button, DataTable, Input, Static, Tree

from integration.browser.pilot_drivers import (
    ReplayLocalStore,
    drill_to_unit_screen_via_fs_device,
    open_browser_pilot,
    select_first_leaf,
    version_rows_ready,
)
from support.pilot import (
    RUN_TEST_SIZE,
    SDK_TIMEOUT,
    UI_TIMEOUT,
    focus_widget,
    move_cursor_to,
    wait_for_filter_closed,
    wait_for_screen,
    wait_until,
)
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Catalog, RawView, Repository, Version
from synology_apm_repo.sdk.units.base import ClosableUnitProvider


def _unique_needle(names: list[str], index: int) -> str:
    """The shortest substring of ``names[index]`` that doesn't occur
    (case-insensitively) in any other entry of ``names``: a filter needle
    matching only that row, without hardcoding an anonymized name."""
    target = names[index]
    others = [name.lower() for i, name in enumerate(names) if i != index]
    for length in range(1, len(target) + 1):
        for start in range(len(target) - length + 1):
            candidate = target[start : start + length]
            if not any(candidate.lower() in other for other in others):
                return candidate
    raise AssertionError(f"no substring of {target!r} discriminates it from {names!r}")


def test_hex_preview_blocked_outside_verbose_mode_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> bool:
        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)
            await select_first_leaf(app, pilot)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            # The blocked branch pushes no screen; its warning is the signal
            # that x was handled.
            warnings: list[str] = []
            monkeypatch.setattr(unit_screen, "notify", lambda message, **kwargs: warnings.append(message))
            await pilot.press("x")
            await wait_until(pilot, lambda: warnings, timeout=UI_TIMEOUT, interval=0.02)
            assert warnings == ["press d to enable verbose mode first"]
            return isinstance(app.screen, UnitScreen)

    still_on_unit_screen = asyncio.run(scenario())
    assert still_on_unit_screen


def test_hex_preview_pages_forward_and_back_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    """``x`` in verbose mode opens the preview at offset 0; ``x``/``X`` then
    page one window forward and back."""

    async def scenario() -> tuple[str, str, str]:
        # The hex preview reads the file's bytes (asserting only offsets).
        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz", allow_content=True)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)
            await select_first_leaf(app, pilot)

            def dump() -> str:
                return str(app.screen.query_one("#hex-dump").render())

            # ``x`` opens the preview only in verbose mode. On this FS version
            # ``d`` reloads no tree (only a SaaS version's does), so the
            # selected leaf survives it.
            await pilot.press("d")
            await wait_until(
                pilot, lambda: app.verbose, timeout=UI_TIMEOUT, interval=0.02, message="verbose never turned on"
            )

            await pilot.press("x")
            # The screen is pushed before its first window is read: wait for
            # that window to render, or ``initial`` captures an empty dump.
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, HexPreviewScreen) and app.screen.is_mounted and dump(),
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="first hex window never rendered",
            )
            assert isinstance(app.screen, HexPreviewScreen), app.screen
            initial = dump()

            # No state flag marks a landed page; the dump changing does.
            await pilot.press("x")  # page forward
            await wait_until(
                pilot, lambda: dump() != initial, timeout=SDK_TIMEOUT, interval=0.02, message="never paged forward"
            )
            forward = dump()

            await pilot.press("X")  # page back to the original window
            await wait_until(
                pilot, lambda: dump() != forward, timeout=SDK_TIMEOUT, interval=0.02, message="never paged back"
            )
            back = dump()

            return initial, forward, back

    initial, forward, back = asyncio.run(scenario())
    assert initial.startswith("00000000"), initial
    assert forward.startswith("00000200"), forward  # HEX_WINDOW_SIZE (512) in hex
    assert back == initial


def test_unit_screen_filter_narrows_then_esc_restores_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[int, str, list[str], int]:
        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            await wait_until(pilot, lambda: tree.root.children, timeout=SDK_TIMEOUT, interval=0.03)
            full_count = len(tree.root.children)
            assert full_count > 0, "root has no children to filter"

            needle = str(tree.root.children[0].label)[:3]
            await move_cursor_to(pilot, tree, tree.root)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#filter-input"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = unit_screen.query_one("#filter-input", Input)
            # The rebuild is debounced and a surviving node keeps its
            # TreeNode, so wait on the model's committed filter text.
            filter_input.value = needle
            await wait_until(
                pilot,
                lambda: unit_screen.store.model.filter is not None and unit_screen.store.model.filter.text == needle,
                timeout=UI_TIMEOUT,
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
    # Case-insensitive, as core/text_filter.py's matches_filter.
    assert all(needle.lower() in label.lower() for label in filtered_labels), filtered_labels
    assert restored_count == full_count


def test_browse_screen_filter_narrows_connections_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:

    async def scenario() -> tuple[int, str, list[str], int]:

        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            tree = app.screen.query_one("#col-catalogs", Tree)
            await focus_widget(pilot, tree)
            repo_node = tree.root.children[0]
            full_count = len(repo_node.children)
            assert full_count >= 1

            names = [str(c.label) for c in repo_node.children]
            needle = _unique_needle(names, 0)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: app.screen.query("#filter-input"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = app.screen.query_one("#filter-input", Input)
            filter_input.value = needle
            # `needle` matches exactly one row, so narrowing to it is the
            # debounced rebuild's completion signal.
            await wait_until(
                pilot,
                lambda: len(repo_node.children) == 1,
                timeout=UI_TIMEOUT,
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
    # Case-insensitive, as core/text_filter.py's matches_filter.
    assert all(needle.lower() in label.lower() for label in filtered_labels), filtered_labels
    assert restored_count == full_count


def test_browse_screen_version_filter_enter_closes_it_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:

    async def scenario() -> tuple[int, str, str, list[str], int, bool, bool]:

        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            await focus_widget(pilot, app.screen.query_one("#col-catalogs", Tree))
            await pilot.press("enter")
            workloads_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: workloads_tree.root.children, timeout=SDK_TIMEOUT, interval=0.02)
            await focus_widget(pilot, workloads_tree)
            await pilot.press("enter")
            versions_table = app.screen.query_one("#col-versions", DataTable)
            await wait_until(
                pilot,
                lambda: version_rows_ready(app),
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            await focus_widget(pilot, versions_table)
            all_names = [str(versions_table.get_row_at(i)[0]) for i in range(versions_table.row_count)]
            full_count = len(all_names)
            assert full_count >= 1

            # Version names share a date prefix, so a plain prefix would
            # match every row.
            expected_name = all_names[0]
            needle = _unique_needle(all_names, 0)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: app.screen.query("#filter-input"),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = app.screen.query_one("#filter-input", Input)
            filter_input.value = needle
            # `needle` matches exactly one row, so narrowing to it is the
            # debounced rebuild's completion signal.
            await wait_until(
                pilot,
                lambda: versions_table.row_count == 1,
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="debounced filter never narrowed the table",
            )
            filtered_names = [str(versions_table.get_row_at(i)[0]) for i in range(versions_table.row_count)]

            await pilot.press("enter")
            # Closing the filter deactivates the input, restores the list,
            # then refocuses the table, in one synchronous call: waiting on
            # the last step covers the other two.
            await wait_until(
                pilot,
                lambda: versions_table.has_focus,
                timeout=UI_TIMEOUT,
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
    assert filtered_names == [expected_name], (needle, filtered_names)
    assert restored_count == full_count
    assert not input_still_active
    assert table_focused


def test_browse_screen_backspace_jumps_cursor_to_parent_and_collapses_it_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:

    async def scenario() -> tuple[bool, bool, bool, bool]:

        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
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
                pilot, lambda: tree.cursor_node is repo_node, timeout=UI_TIMEOUT, interval=0.02, message="never went up"
            )
            landed_on_repo = tree.cursor_node is repo_node
            repo_collapsed = not repo_node.is_expanded

            await pilot.press("backspace")
            await wait_until(
                pilot, lambda: tree.cursor_node is tree.root, timeout=UI_TIMEOUT, interval=0.02, message="never went up"
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
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    """A refresh rescans and finds the same repositories again;
    ``objstore-encrypted``'s two sibling repo-ids discover as one
    ``Repository``, so the count is 1."""

    async def scenario() -> tuple[int, int]:
        await replay_local_store("tui_hex_filter_refresh_objstore_encrypted_pilot.json.gz", label="objstore-encrypted")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            tree = app.screen.query_one("#col-catalogs", Tree)
            first_count = len(tree.root.children)

            await pilot.press("r")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ConnectDialog) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.02,
            )
            assert isinstance(app.screen, ConnectDialog), "refresh with nothing selected must reopen ConnectDialog"
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            second_count = len(tree.root.children)
            return first_count, second_count

    first_count, second_count = asyncio.run(scenario())
    assert first_count == second_count == 1


def test_unit_screen_refresh_reloads_the_tree_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[int, int, bool]:
        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#folder-tree", Tree)
            await wait_until(pilot, lambda: tree.root.children, timeout=SDK_TIMEOUT, interval=0.03)
            before_count = len(tree.root.children)
            provider_before = unit_screen.store.model.provider

            await pilot.press("r")
            await wait_until(pilot, lambda: tree.root.children, timeout=SDK_TIMEOUT, interval=0.03)
            after_count = len(tree.root.children)
            provider_replaced = unit_screen.store.model.provider != provider_before

            return before_count, after_count, provider_replaced

    before_count, after_count, provider_replaced = asyncio.run(scenario())
    assert before_count == after_count > 0
    assert provider_replaced


def test_loading_indicator_is_gone_once_a_catalog_query_settles_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    """Once a catalog-level query has settled, neither the repo node's label
    (this call site's loading target, a ``TreeNodeLoadingSink`` expand) nor
    the breadcrumb shows a Loading suffix."""

    async def scenario() -> tuple[bool, bool]:

        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
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
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_load_catalogs_for`` (a real tree-node expand) shows its loading
    hint on the expanding repo node's own label, not the breadcrumb — the
    opposite target from ``_load_root``'s own loading hint (covered by
    ``test_loading_indicator_appears_on_the_breadcrumb_for_a_slow_provider_load_replayed``
    below)."""
    original_catalogs = Repository.catalogs
    # Held open until both checks below have run, so the loading window
    # cannot close before polling starts on a slow machine.
    hint_seen: asyncio.Event | None = None

    async def slow_catalogs(self: Repository) -> Any:
        assert hint_seen is not None
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(hint_seen.wait(), timeout=30.0)
        return await original_catalogs(self)

    async def scenario() -> None:

        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        nonlocal hint_seen
        hint_seen = asyncio.Event()
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            dialog = await wait_for_screen(pilot, ConnectDialog)
            dialog.query_one("#connect-local-path", Input).value = str(tmp_path)
            dialog.query_one("#connect-submit", Button).press()
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                message="BrowseScreen never appeared",
            )
            tree = app.screen.query_one("#col-catalogs", Tree)
            await wait_until(
                pilot, lambda: tree.root.children, timeout=SDK_TIMEOUT, message="#col-catalogs never populated"
            )
            repo_node = tree.root.children[0]
            _ = tree._tree_lines  # forces the line map to rebuild; see move_cursor_to's docstring
            tree.move_cursor(repo_node)
            await pilot.press("enter")

            try:
                await wait_until(
                    pilot,
                    lambda: "loading" in str(repo_node.label).lower(),
                    timeout=SDK_TIMEOUT,
                    interval=0.02,
                    message="the repo node's label must show a Loading suffix once its own catalog query "
                    "runs past the debounce delay",
                )
                breadcrumb_hint = "loading" in str(app.screen.query_one("#breadcrumb", Static).render()).lower()
                assert not breadcrumb_hint, "a tree-node-expand's own loading hint must not also touch the breadcrumb"
            finally:
                hint_seen.set()

    monkeypatch.setattr(Repository, "catalogs", slow_catalogs)
    asyncio.run(scenario())


def test_loading_indicator_appears_on_the_breadcrumb_for_a_slow_provider_load_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_provider = Catalog.provider
    # Held open until the test has seen the hint, so the loading window
    # cannot close before polling starts on a slow machine.
    hint_seen: asyncio.Event | None = None

    async def slow_provider(self: Catalog, version: Version, *, raw: RawView | None = None) -> ClosableUnitProvider:
        assert hint_seen is not None
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(hint_seen.wait(), timeout=30.0)
        return await original_provider(self, version, raw=raw)

    async def scenario() -> None:

        await replay_local_store("tui_hex_filter_refresh_vault_plain_pilot.json.gz")
        nonlocal hint_seen
        hint_seen = asyncio.Event()
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            await focus_widget(pilot, app.screen.query_one("#col-catalogs", Tree))
            await pilot.press("enter")
            workloads_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: workloads_tree.root.children, timeout=SDK_TIMEOUT, interval=0.02)
            await focus_widget(pilot, workloads_tree)
            await pilot.press("enter")
            versions_table = app.screen.query_one("#col-versions", DataTable)
            await wait_until(
                pilot,
                lambda: version_rows_ready(app),
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            await focus_widget(pilot, versions_table)
            await pilot.press("enter")

            def _shows_loading_hint() -> bool:
                return (
                    isinstance(app.screen, UnitScreen)
                    and "loading" in str(app.screen.query_one("#breadcrumb", Static).render()).lower()
                )

            try:
                await wait_until(
                    pilot,
                    _shows_loading_hint,
                    timeout=SDK_TIMEOUT,
                    interval=0.02,
                    message="the breadcrumb must show a Loading suffix once an operation runs past the debounce delay",
                )
            finally:
                hint_seen.set()

    monkeypatch.setattr(Catalog, "provider", slow_provider)
    asyncio.run(scenario())
