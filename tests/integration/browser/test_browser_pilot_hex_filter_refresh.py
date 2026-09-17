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
from textual.widgets import DataTable, Input, Static, Tree

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen, CatalogEntry
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Catalog, Version
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.recording import ReplayStore
from synology_apm_repo.sdk.units.base import UnitProvider

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures"
_APV1_FIXTURE = _FIXTURES / "tui_hex_filter_refresh_apv1_pilot.json.gz"


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
    -- used only by the tests below that also need ``_drill_to_unit_screen``'s
    explicit-target fix (see its own docstring); every other test in this
    file still uses the plain ``_fake_build_local_store_for(_APV1_FIXTURE,
    ...)`` helper above and doesn't yet support ``--record-against``."""
    store = await record_target("tui_hex_filter_refresh_apv1_pilot.json.gz", allow_content=allow_content)

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, "apv-sample-1"

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


async def _drill_to_unit_screen(app: ApmRepoBrowserApp, pilot: Any, wait_until: Any) -> None:
    """Drills into a specific, confirmed-good workload — the connection
    with connection_config_id 1's FS device — rather than whatever a
    cursor-default "press enter" lands on first: a VM device under this
    same connection has a real version whose ``target.db`` was never
    captured (a genuine gap in the real sample data itself, not a
    recording or anonymization bug), and which device sorts/groups first
    shifts every time display names are re-anonymized. FS content is
    structurally immune to that whole failure mode — it never goes
    through ``target.db`` at all — so it's the more durable choice, not
    just a currently-lucky pick."""
    assert isinstance(app.screen, BrowseScreen), app.screen
    cat_tree = app.screen.query_one("#col-catalogs", Tree)
    repo_node = cat_tree.root.children[0]
    # connection_config_id 1 -- an internal catalog identifier, stable and
    # non-identifying (never touched by anonymization).
    connection_node = next(
        n
        for n in repo_node.children
        if isinstance(n.data, CatalogEntry) and n.data.catalog.connection.connection_config_id == 1
    )
    cat_tree.move_cursor(connection_node)
    cat_tree.focus()
    await pilot.press("enter")

    wl_tree = app.screen.query_one("#col-workloads", Tree)
    await wait_until(pilot, lambda: wl_tree.root.children, timeout=3.0, interval=0.02)
    fs_group = next(n for n in wl_tree.root.children if str(n.label) == "FS")
    # Only the *first* group auto-expands (BrowseScreen._set_workloads) --
    # "FS" isn't always that one, so its own children aren't visible/
    # selectable via move_cursor until expanded explicitly.
    fs_group.expand()
    await wait_until(pilot, lambda: fs_group.children, timeout=0.8, interval=0.02)
    wl_tree.move_cursor(fs_group.children[0])
    wl_tree.focus()
    await pilot.press("enter")

    versions_table = app.screen.query_one("#col-versions", DataTable)
    await wait_until(pilot, lambda: versions_table.row_count, timeout=3.0, interval=0.02)
    versions_table.focus()
    await pilot.press("enter")
    await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=0.6, interval=0.03)
    assert isinstance(app.screen, UnitScreen), app.screen


async def _first_leaf(
    app: ApmRepoBrowserApp, pilot: Any, wait_until: Any, focus_widget: Any, move_cursor_to: Any
) -> Any:
    unit_screen = app.screen
    assert isinstance(unit_screen, UnitScreen)
    tree = unit_screen.query_one("#unit-tree", Tree)
    await wait_until(pilot, lambda: tree.root.data is not None, timeout=3.0, interval=0.02)
    await focus_widget(pilot, tree)
    node = tree.root
    depth = 0
    while node is not None and node.data is not None and not node.data.is_leaf and depth < 6:
        node.expand()
        await wait_until(pilot, lambda n=node: n.children, timeout=3.0, interval=0.02)
        if not node.children:
            break
        node = node.children[0]
        depth += 1
    assert node is not None and node.data is not None and node.data.is_leaf, "no leaf found"
    # move_cursor only takes effect against an up-to-date line map; forcing it
    # here is the same idiom _select_matching_workload uses.
    _ = tree._tree_lines
    tree.move_cursor(node)
    await wait_until(
        pilot,
        lambda n=node: tree.cursor_node is n,
        timeout=0.4,
        interval=0.02,
        message="cursor never landed on the chosen leaf",
    )
    return node


def test_hex_preview_blocked_outside_diagnostic_mode_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[..., Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
) -> None:
    async def scenario() -> bool:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)
            await _first_leaf(app, pilot, wait_until, focus_widget, move_cursor_to)

            await pilot.press("x")
            await pilot.pause(0.3)
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
            await _drill_to_unit_screen(app, pilot, wait_until)
            await _first_leaf(app, pilot, wait_until, focus_widget, move_cursor_to)

            def dump() -> str:
                return str(app.screen.query_one("#hex-dump").render())

            # ``d`` re-dispatches the provider and reloads the whole tree
            # (UnitScreen.refresh_for_verbose_mode), so the leaf picked above
            # no longer exists afterwards -- re-acquire one rather than press
            # ``x`` into a tree that is still being rebuilt.
            await pilot.press("d")
            await wait_until(pilot, lambda: app.verbose, timeout=0.4, interval=0.02, message="verbose never turned on")
            await _first_leaf(app, pilot, wait_until, focus_widget, move_cursor_to)

            await pilot.press("x")
            await wait_until(pilot, lambda: isinstance(app.screen, HexPreviewScreen), timeout=0.4, interval=0.02)
            assert isinstance(app.screen, HexPreviewScreen), app.screen
            initial = dump()

            # The rendered window changing *is* the readiness signal for a
            # page turn; there is no state flag that says "the next page has
            # landed", and a fixed pause would only be guessing at it.
            await pilot.press("x")  # page forward
            await wait_until(
                pilot, lambda: dump() != initial, timeout=0.4, interval=0.02, message="never paged forward"
            )
            forward = dump()

            await pilot.press("X")  # page back to the original window
            await wait_until(pilot, lambda: dump() != forward, timeout=0.4, interval=0.02, message="never paged back")
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
) -> None:
    async def scenario() -> tuple[int, str, list[str], int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#unit-tree", Tree)
            await wait_until(pilot, lambda: tree.root.children, timeout=3.0, interval=0.03)
            full_count = len(tree.root.children)
            assert full_count > 0, "root has no children to filter"

            needle = str(tree.root.children[0].label)[:3]
            await move_cursor_to(pilot, tree, tree.root)
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: unit_screen.query("#filter-input"),
                timeout=0.4,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = unit_screen.query_one("#filter-input", Input)
            filter_input.value = needle
            await pilot.pause(0.02)
            filtered_labels = [str(c.label) for c in tree.root.children]

            await pilot.press("escape")
            await pilot.pause(0.02)
            restored_count = len(tree.root.children)

            return full_count, needle, filtered_labels, restored_count

    full_count, needle, filtered_labels, restored_count = asyncio.run(scenario())
    assert 0 < len(filtered_labels) <= full_count
    # Every surviving node's label actually contains the needle
    # (case-insensitively, matching _rerender_filtered's own ``.lower()``)
    # -- a regression that narrowed to the right *count* but the wrong
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
            tree.focus()
            repo_node = tree.root.children[0]
            full_count = len(repo_node.children)
            assert full_count >= 1

            first_name = str(repo_node.children[0].label)
            needle = first_name[:4]
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: app.screen.query("#filter-input"),
                timeout=0.4,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = app.screen.query_one("#filter-input", Input)
            filter_input.value = needle
            await pilot.pause(0.02)
            filtered_labels = [str(c.label) for c in repo_node.children]

            await pilot.press("escape")
            await pilot.pause(0.02)
            restored_count = len(repo_node.children)

            return full_count, needle, filtered_labels, restored_count

    full_count, needle, filtered_labels, restored_count = asyncio.run(scenario())
    assert 0 < len(filtered_labels) <= full_count
    # Every surviving connection's label actually contains the needle
    # (case-insensitively, matching _rerender_tree_filter's own
    # ``.lower()``) -- a regression that narrowed to the right *count* but
    # the wrong *nodes* would still pass a count-only bound.
    assert all(needle.lower() in label.lower() for label in filtered_labels), filtered_labels
    assert restored_count == full_count


def test_browse_screen_version_filter_enter_closes_it_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    move_cursor_to: Any,
) -> None:
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )

    async def scenario() -> tuple[int, str, list[str], int, bool, bool]:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            app.screen.query_one("#col-catalogs", Tree).focus()
            await pilot.press("enter")
            workloads_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: workloads_tree.root.children, timeout=0.8, interval=0.02)
            workloads_tree.focus()
            await pilot.press("enter")
            versions_table = app.screen.query_one("#col-versions", DataTable)
            await wait_until(pilot, lambda: versions_table.row_count, timeout=3.0, interval=0.02)
            versions_table.focus()
            full_count = versions_table.row_count
            assert full_count >= 1

            first_name = str(versions_table.get_row_at(0)[0])
            needle = first_name[:4]
            await pilot.press("slash")
            await wait_until(
                pilot,
                lambda: app.screen.query("#filter-input"),
                timeout=0.4,
                interval=0.02,
                message="filter input never opened",
            )
            filter_input = app.screen.query_one("#filter-input", Input)
            filter_input.value = needle
            await pilot.pause(0.02)
            filtered_names = [str(versions_table.get_row_at(i)[0]) for i in range(versions_table.row_count)]

            await pilot.press("enter")
            await pilot.pause(0.02)
            restored_count = versions_table.row_count
            input_still_active = filter_input.has_class("active")
            table_focused = versions_table.has_focus

            return full_count, needle, filtered_names, restored_count, input_still_active, table_focused

    full_count, needle, filtered_names, restored_count, input_still_active, table_focused = asyncio.run(scenario())
    assert 0 < len(filtered_names) <= full_count
    # Every surviving row's own name column actually contains the needle
    # (case-insensitively) -- a regression that narrowed to the right
    # *count* but the wrong *rows* would still pass a count-only bound.
    assert all(needle.lower() in name.lower() for name in filtered_names), filtered_names
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
            tree.focus()
            repo_node = tree.root.children[0]
            assert repo_node.children, "expected at least one connection under the repository"
            connection_node = repo_node.children[0]
            await move_cursor_to(pilot, tree, connection_node)

            await pilot.press("backspace")
            await wait_until(
                pilot, lambda: tree.cursor_node is repo_node, timeout=0.4, interval=0.02, message="never went up"
            )
            landed_on_repo = tree.cursor_node is repo_node
            repo_collapsed = not repo_node.is_expanded

            await pilot.press("backspace")
            await wait_until(
                pilot, lambda: tree.cursor_node is tree.root, timeout=0.4, interval=0.02, message="never went up"
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
) -> None:
    """``s3-sample-2-encrypted``'s two sibling repo-ids now correctly
    discover as one ``Repository`` (holding both as ``catalogs()``) under
    the Repository/Catalog architecture rename — the repository count this test
    checks before/after refresh is 1, not the pre-rename 2, but the
    behavior under test (a refresh rescans and finds the same thing
    again) is unaffected."""

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
            await wait_until(pilot, lambda: isinstance(app.screen, ConnectDialog), timeout=0.6, interval=0.02)
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
) -> None:
    async def scenario() -> tuple[int, int, bool]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await _drill_to_unit_screen(app, pilot, wait_until)

            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen)
            tree = unit_screen.query_one("#unit-tree", Tree)
            await wait_until(pilot, lambda: tree.root.children, timeout=3.0, interval=0.03)
            before_count = len(tree.root.children)
            provider_before = unit_screen._provider

            await pilot.press("r")
            await wait_until(pilot, lambda: tree.root.children, timeout=3.0, interval=0.03)
            after_count = len(tree.root.children)
            provider_replaced = unit_screen._provider is not provider_before

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
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )

    async def scenario() -> bool:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            assert app.screen.query_one("#col-catalogs", Tree).root.children
            return "loading" in str(app.screen.query_one("#breadcrumb", Static).render()).lower()

    hint_appeared = asyncio.run(scenario())
    assert not hint_appeared, "the breadcrumb must never show a Loading suffix for a catalog-level query"


def test_loading_indicator_appears_for_an_operation_slower_than_300ms_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
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
        # An order of magnitude past the real 300ms debounce delay, so
        # the indicator has ample real time both to appear and to still
        # be showing by the time the wait_until below checks for it,
        # regardless of real scheduling load.
        await asyncio.sleep(3.0)
        return await original_provider(self, version, object_db_id=object_db_id, force_raw=force_raw)

    async def scenario() -> None:
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            app.screen.query_one("#col-catalogs", Tree).focus()
            await pilot.press("enter")
            await pilot.pause(0.4)
            app.screen.query_one("#col-workloads", Tree).focus()
            await pilot.press("enter")
            await pilot.pause(0.4)
            app.screen.query_one("#col-versions", DataTable).focus()
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
                message="the breadcrumb must show a Loading suffix once an operation runs past the 300ms debounce delay",
            )

    Catalog.provider = slow_provider  # type: ignore[method-assign]
    try:
        asyncio.run(scenario())
    finally:
        Catalog.provider = original_provider  # type: ignore[method-assign]


__all__: list[str] = []
