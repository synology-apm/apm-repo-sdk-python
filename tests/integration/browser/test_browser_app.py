"""Regression tests for the browser TUI's core flows. ``replay_local_store``
hands the ``ConnectDialog`` the fixture's store, so the path
``open_browser_pilot`` types is never read.

Fixtures and the root each is recorded against:

- ``tui_vault_plain_pilot_walk.json.gz`` — ``vault-plain``.
- ``tui_browse_happy_path_vault_plain.json.gz`` —
  ``vault-plain/@ActiveProtectVault``.
- ``tui_vault_encrypted_pilot_walk.json.gz`` —
  ``vault-encrypted/@ActiveProtectVault``.
- ``tui_objstore_encrypted_pilot_walk.json.gz`` — ``objstore-encrypted``.

``BrowseScreen``'s ``#col-catalogs`` root has one child per discovered
repository; each repository's connections sit one level below that.

Real-time export-progress behavior (cancel latency, rate/ETA display,
backgrounding) isn't covered: a replayed fixture returns instantly.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Input, Static, Tree

import synology_apm_repo.browser.runtime.connect as connect_runtime_module
from integration.browser.pilot_drivers import (
    ReplayLocalStore,
    drill_to_unit_screen_via_fs_device,
    open_browser_pilot,
    select_first_leaf,
    version_rows_ready,
)
from support.content_fakes import BlockingContentSource
from support.pilot import RUN_TEST_SIZE, SDK_TIMEOUT, UI_TIMEOUT, focus_widget, settle, wait_for_screen, wait_until
from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.app.msg import CancelJobRequested, ExportProgressed, StartExport
from synology_apm_repo.browser.runtime import unit_effects
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import KeyStatus, Session
from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.presentation.format import format_bytes
from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef


def test_open_browse_and_unit_tree_happy_path_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        await replay_local_store("tui_browse_happy_path_vault_plain.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            assert len(cat_tree.root.children) == 1
            assert len(cat_tree.root.children[0].children) == 2
            await focus_widget(pilot, cat_tree)
            await pilot.press("enter")
            wl_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: wl_tree.root.children, timeout=SDK_TIMEOUT, interval=0.02)
            assert any(wl_tree.root.children), "no workload type groups populated"
            await focus_widget(pilot, wl_tree)
            await pilot.press("enter")
            ver_table = app.screen.query_one("#col-versions", DataTable)
            await wait_until(
                pilot,
                lambda: version_rows_ready(app),
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            assert ver_table.row_count > 0
            await focus_widget(pilot, ver_table)
            await pilot.press("enter")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, UnitScreen) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.03,
            )
            assert isinstance(app.screen, UnitScreen), app.screen

            tree = app.screen.query_one("#folder-tree", Tree)
            await wait_until(
                pilot,
                lambda: tree.root.children or (tree.root.data is not None and tree.root.data.payload.is_leaf),
                timeout=SDK_TIMEOUT,
                interval=0.03,
            )
            assert tree.root.data is not None

    asyncio.run(scenario())


def test_esc_on_the_main_screen_closes_the_repo_and_reopens_connect_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:

    async def scenario() -> tuple[bool, bool, bool, bool]:

        await replay_local_store("tui_vault_plain_pilot_walk.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            await pilot.press("escape")
            # The dialog reopens once the repository is closed through the Session.
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ConnectDialog) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            reopened_connect_dialog = isinstance(app.screen, ConnectDialog)

            # Esc again cancels the reopened dialog, landing on a blank
            # BrowseScreen (the same "nothing connected" state as a fresh boot).
            await pilot.press("escape")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.02,
            )
            back_on_browse_screen = isinstance(app.screen, BrowseScreen)
            connections_gone = back_on_browse_screen and not app.screen.query_one("#col-catalogs", Tree).root.children
            status_cleared = back_on_browse_screen and str(app.screen.query_one("#open-status", Static).render()) == ""
            return reopened_connect_dialog, back_on_browse_screen, connections_gone, status_cleared

    reopened_connect_dialog, back_on_browse_screen, connections_gone, status_cleared = asyncio.run(scenario())
    assert reopened_connect_dialog
    assert back_on_browse_screen
    assert connections_gone
    assert status_cleared


def test_tasks_hint_stays_in_sync_across_browse_and_unit_screens_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    """A background job's count in the breadcrumb hint stays correct across
    ``BrowseScreen``, ``UnitScreen``, a progress tick, and Esc back."""

    def _breadcrumb_text(app: ApmRepoBrowserApp) -> str:
        return str(app.screen.query_one("#breadcrumb", Static).render())

    async def scenario() -> tuple[str, str, str, str]:

        await replay_local_store("tui_vault_plain_pilot_walk.json.gz")
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name="exporting.bin",
            is_leaf=True,
            content=BlockingContentSource(),
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            idle_on_browse_screen = _breadcrumb_text(app)

            app.store.dispatch(StartExport(target=unit, dst_text=str(tmp_path / "out.bin"), sparse=True))
            job_id = next(iter(app.jobs))

            await drill_to_unit_screen_via_fs_device(app, pilot)
            on_unit_screen = _breadcrumb_text(app)

            app.store.dispatch(
                ExportProgressed(
                    job_id=job_id,
                    done=50,
                    total=100,
                    size_text="100 B",
                    rate_text="50 B/s",
                    eta_text="",
                    elapsed_text="00:01",
                )
            )
            await settle(pilot)
            after_update = _breadcrumb_text(app)

            await pilot.press("escape")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.02,
            )
            back_on_browse_screen = _breadcrumb_text(app)

            # Finish the job before returning: run_test()'s teardown would
            # otherwise cancel it mid-teardown and can raise NoMatches.
            app.store.dispatch(CancelJobRequested(job_id=job_id))
            await wait_until(pilot, lambda: not app.jobs, timeout=UI_TIMEOUT, interval=0.02)

            return idle_on_browse_screen, on_unit_screen, after_update, back_on_browse_screen

    idle_on_browse_screen, on_unit_screen, after_update, back_on_browse_screen = asyncio.run(scenario())
    assert "Task" not in idle_on_browse_screen  # no jobs yet -- no suffix at all
    assert "1 Task (t)" in on_unit_screen
    assert "1 Task (t)" in after_update  # count unaffected by a progress tick
    assert "1 Task (t)" in back_on_browse_screen


def test_detail_panel_and_export_dialog_show_human_readable_sizes_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The detail header and export dialog render the leaf's size human-readably.

    The content preview is stubbed out: it would read the real file's bytes,
    which a replay test doesn't assert on (preview rendering is covered by
    the synthetic ``test_browser_runtime_preview.py``)."""

    async def no_preview(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(unit_effects, "load_preview", no_preview)

    async def scenario() -> tuple[str, str, int]:
        await replay_local_store("tui_vault_plain_pilot_walk.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen), app.screen
            leaf = await select_first_leaf(app, pilot, require_size=True, max_depth=8)
            assert leaf.is_leaf and leaf.size is not None
            size = leaf.size

            unit_screen.action_show_detail()
            await wait_until(
                pilot,
                lambda: str(unit_screen.query_one("#detail", Static).render()),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="detail pane never rendered",
            )
            detail_text = str(unit_screen.query_one("#detail", Static).render())

            unit_screen.action_export_selected()
            # The screen is pushed before its children mount: wait for the title itself.
            await wait_until(
                pilot,
                lambda: (
                    isinstance(app.screen, ExportScreen) and app.screen.is_mounted and app.screen.query_one("Static")
                ),
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            export_title = str(app.screen.query_one("Static").render())

            return detail_text, export_title, size

    detail_text, export_title, size = asyncio.run(scenario())
    expected = format_bytes(size)
    assert "bytes" not in detail_text.lower(), detail_text
    assert expected in detail_text, (expected, detail_text)
    assert "bytes" not in export_title.lower(), export_title
    assert expected in export_title, (expected, export_title)


def test_export_screen_is_a_centered_modal_over_the_still_present_unit_screen_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[bool, bool, bool, int, int]:
        await replay_local_store("tui_vault_plain_pilot_walk.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen), app.screen
            await select_first_leaf(app, pilot, require_size=True, max_depth=8)
            unit_screen.action_export_selected()
            # The screen being pushed doesn't mean compose() has mounted
            # its children yet; wait for the query to resolve too.
            await wait_until(
                pilot,
                lambda: (
                    isinstance(app.screen, ExportScreen)
                    and app.screen.is_mounted
                    and bool(app.screen.query("Vertical"))
                ),
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )

            is_modal = isinstance(app.screen, ModalScreen)
            unit_screen_still_on_stack = unit_screen in app.screen_stack
            unit_screen_is_top = app.screen is unit_screen
            box = app.screen.query_one("Vertical")
            return is_modal, unit_screen_still_on_stack, unit_screen_is_top, box.region.height, app.screen.size.height

    is_modal, unit_screen_still_on_stack, unit_screen_is_top, box_height, screen_height = asyncio.run(scenario())
    assert is_modal, "ExportScreen must be a ModalScreen, not a full-screen replacement view"
    assert unit_screen_still_on_stack, "UnitScreen must stay on the screen stack underneath the modal"
    assert not unit_screen_is_top, "the modal, not UnitScreen, must be the topmost/active screen"
    assert box_height < screen_height, (
        f"dialog box ({box_height} rows) must be smaller than the screen ({screen_height} rows), "
        "not stretched to fill it"
    )


def test_encrypted_repo_key_flow_then_shutdown_does_not_raise_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    """Entering the key and shutting down raises no sqlite cross-thread error
    (synthetic counterpart: ``test_storage_sqlite.py``'s
    ``test_connection_opened_in_worker_thread_can_be_closed_from_main_thread``)."""
    key_string = VAULT_ENCRYPTED_KEY_STRING

    async def scenario() -> None:
        await replay_local_store("tui_vault_encrypted_pilot_walk.json.gz", label="vault-encrypted")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            await focus_widget(pilot, cat_tree)
            await pilot.press("enter")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, KeyDialog) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            assert isinstance(app.screen, KeyDialog), app.screen

            key_input = app.screen.query_one("#key-input", Input)
            key_input.value = key_string
            await focus_widget(pilot, key_input)
            await pilot.press("enter")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                interval=0.03,
            )
            assert isinstance(app.screen, BrowseScreen), app.screen

            wl_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: wl_tree.root.children, timeout=SDK_TIMEOUT, interval=0.03)
            assert wl_tree.root.children, "workload list never populated after key verification"
            await focus_widget(pilot, wl_tree)
            await pilot.press("enter")
            ver_table = app.screen.query_one("#col-versions", DataTable)
            await wait_until(
                pilot,
                lambda: version_rows_ready(app),
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            if ver_table.row_count:
                await focus_widget(pilot, ver_table)
                await pilot.press("enter")
                await wait_until(
                    pilot,
                    lambda: isinstance(app.screen, UnitScreen) and app.screen.is_mounted,
                    timeout=UI_TIMEOUT,
                    interval=0.03,
                )

    asyncio.run(scenario())  # must not raise sqlite3.ProgrammingError on shutdown


def test_connect_dialog_reports_repos_found_incrementally_while_scanning_replayed(
    replay_local_store: ReplayLocalStore,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Two sibling repo-ids sharing one bucket discover as a single
    ``Repository``: discovery reports found=1 once and the tree holds one
    repository node. Multi-repository progress is in ``test_api_session.py``."""
    found_counts: list[int] = []

    class _RecordingProgressMeter(ProgressMeter):
        async def update(self, progress: Progress) -> None:
            if progress.found is not None:
                found_counts.append(progress.found)
            await super().update(progress)

    monkeypatch.setattr(connect_runtime_module, "ProgressMeter", _RecordingProgressMeter)

    async def scenario() -> int:
        await replay_local_store("tui_objstore_encrypted_pilot_walk.json.gz", label="objstore-encrypted")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            tree = app.screen.query_one("#col-catalogs", Tree)
            return len(tree.root.children)

    final_count = asyncio.run(scenario())
    assert found_counts == [1], f"repository-found progress must report the one bucket found, got {found_counts}"
    assert final_count == 1


def test_default_repo_lands_on_the_first_discovered_one_not_the_last_without_auto_expanding_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    """The sole discovered repository becomes ``app.repo_handle`` and column 1's
    cursor stays on the root with nothing auto-expanded. One repository can't
    prove "first, not last" ordering; see
    ``tests/unit/sdk/test_storage_layout.py``'s
    ``test_repository_layout_multiple_sibling_vaults``."""

    async def scenario() -> tuple[bool, bool, int]:
        await replay_local_store("tui_objstore_encrypted_pilot_walk.json.gz", label="objstore-encrypted")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            dialog = await wait_for_screen(pilot, ConnectDialog)
            dialog.query_one("#connect-local-path", Input).value = str(tmp_path)
            dialog.query_one("#connect-submit", Button).press()
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                interval=0.03,
            )
            screen = app.screen
            assert isinstance(screen, BrowseScreen), screen
            # Can't use open_browser_pilot (it auto-expands); wait for the tree instead.
            tree = screen.query_one("#col-catalogs", Tree)
            await wait_until(
                pilot,
                lambda: tree.root.children,
                timeout=SDK_TIMEOUT,
                interval=0.03,
                message="#col-catalogs never populated",
            )
            repos = screen.store.model.repos
            repo_count = len(repos)
            cursor_on_root = tree.cursor_node is tree.root
            no_repo_expanded = not any(node.is_expanded for node in tree.root.children)
            first_handle = next(iter(repos))
            app_repo_is_first = app.repo_handle == first_handle
            return (cursor_on_root and no_repo_expanded), app_repo_is_first, repo_count

    landed_on_root_unexpanded, app_repo_is_first, repo_count = asyncio.run(scenario())
    assert repo_count == 1, "test invariant: this sample's two sibling repo-ids must discover as one Repository"
    assert landed_on_root_unexpanded, "the cursor must stay on column 1's root, with no repository auto-expanded"
    assert app_repo_is_first, "app.repo_handle must be the sole/first discovered repository"


async def test_workloads_raises_key_required_on_an_encrypted_repo_before_a_key_is_verified_replayed(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """``Repository.catalogs()`` succeeds on a locked repository, but
    ``Catalog.workloads()`` raises ``KeyRequiredError``."""
    store = await record_target("tui_objstore_encrypted_pilot_walk.json.gz")
    session = Session()
    try:
        (repo,) = await session.open(store)
        assert repo.key_status is not KeyStatus.VERIFIED, "this sample must still be locked (no key verified)"
        catalogs = await repo.catalogs()
        assert catalogs
        for catalog in catalogs:
            with pytest.raises(KeyRequiredError, match="this repository is encrypted"):
                await catalog.workloads()
    finally:
        await session.close()
