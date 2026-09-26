"""Regression tests for the browser TUI's core ``apv-sample-1`` flows —
replayed from committed fixtures recorded against real bytes, with **no
external dependency**. Most tests below use ``tui_apv1_pilot_walk.json.gz``
(see each test below for exactly which real walk it covers) — the fixture
backing ``test_open_browse_and_unit_tree_happy_path_replayed`` stays
``tui_browse_happy_path_apv1.json.gz`` on its own, recorded
independently even though it's recorded through the same
``--record-against``-aware ``_patch_local_store`` every other
``--record-against``-supporting test in this file uses: every replay
test gets its own dedicated fixture, never one merged across test
files.

Real-time export-progress behavior (cancel latency, rate/ETA display,
backgrounding) isn't covered here: it needs an actual stream to run over
wall-clock time, which a replayed fixture can't reproduce — pressing
ExportScreen's real Start button against a recorded byte stream returns
instantly rather than over the 200ms-to-few-hundred-ms window that
behavior needs to be observable in.

Unlike every ``cli.*``-based replay test in this directory, the seam
here is ``ConnectDialog._build_local_store`` — monkeypatched to return
a ``ReplayStore`` instead of building a real ``LocalFsStore``
from whatever path the user typed. The existing
``open_browser_pilot`` fixture (``tests/conftest.py``) needs no changes
at all to support this: it only ever *types* the given path into
``#connect-local-path`` and presses submit — once
``_build_local_store`` is replaced, that typed value is never actually
read, so any placeholder string (or even ``tmp_path``, unused) works.

``BrowseScreen``'s own tree structure: ``#col-catalogs``'s root has
one child *per discovered repository* (labeled by the scan label +
key status, e.g. ``"apv-sample-1 · not encrypted"``), with that
repository's own connections one level *below* that — not at the tree's
top level directly, which is easy to miss if you only skim
``open_browser_pilot``'s docstring rather than its actual node-depth
navigation.

Fixture: ``tui_browse_happy_path_apv1.json.gz`` — recorded (via
``_patch_local_store``/``record_target``, same as every other
``--record-against``-supporting test in this file) by driving this exact
Pilot scenario against real ``apv-sample-1`` bytes (connection -> workload
group -> version -> ``UnitScreen``'s own item tree) — the only reliable
way to capture the *exact* call sequence four layers of screen code
produce, rather than guessing it by reading
``browse_screen.py``/``unit_screen.py`` by hand.

**Deliberately kept separate from the ``cli.*``-command replay tests'**
own ``apv-sample-1`` fixtures in this directory's sibling ``cli/``
folder, even though it touches much of the same underlying real data:
this recording is rooted one level higher, at ``apv-sample-1`` itself
rather than ``apv-sample-1/@ActiveProtectVault`` directly (matching how
a real user points the TUI at a general folder and lets discovery find
the vault inside it, versus every CLI test's ``--profile``-scoped store
landing directly on the vault as its own root) — see
``tests/CLAUDE.md``'s "RecordingStore / ReplayStore" section for why a
mismatched real root rules out merging fixtures even when they touch
the same vault.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from textual.coordinate import Coordinate
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Input, Static, Tree

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.app.msg import CancelJobRequested, ExportProgressed, StartExport
from synology_apm_repo.browser.core.unit.msg import ChildrenRequested
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import KeyStatus
from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.presentation.format import format_bytes
from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.recording import ReplayStore
from synology_apm_repo.sdk.units.base import Node, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef

_APV1_FIXTURE = Path(__file__).parent.parent.parent / "fixtures" / "tui_apv1_pilot_walk.json.gz"
#: apv-sample-2-encrypted's real vault key — see
#: ``tests/integration/sdk/test_units_disk_fs.py``'s own
#: ``_ENCRYPTED_KEY_STRING`` for the same value/precedent (a replay test
#: must never read the real ``samples_dir`` at test time).
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="


def test_open_browse_and_unit_tree_happy_path_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> None:
        await _patch_local_store(monkeypatch, record_target, "tui_browse_happy_path_apv1.json.gz")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            # tmp_path's content is never read once _build_local_store is
            # replaced, so any placeholder value works.
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            assert len(cat_tree.root.children) == 1
            assert len(cat_tree.root.children[0].children) == 2
            await focus_widget(pilot, cat_tree)
            await pilot.press("enter")
            wl_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: wl_tree.root.children, timeout=sdk_timeout, interval=0.02)
            assert any(wl_tree.root.children), "no workload type groups populated"
            await focus_widget(pilot, wl_tree)
            await pilot.press("enter")
            ver_table = app.screen.query_one("#col-versions", DataTable)
            # row_count alone can be stale; wait for _visible_version_indices
            # too (see tests/conftest.py's drill_to_unit_screen_via_fs_device).
            await wait_until(
                pilot,
                lambda: ver_table.row_count and app.screen._visible_version_indices,
                timeout=sdk_timeout,
                interval=0.02,
            )
            assert ver_table.row_count > 0
            await focus_widget(pilot, ver_table)
            await pilot.press("enter")
            await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=ui_timeout, interval=0.03)
            assert isinstance(app.screen, UnitScreen), app.screen

            tree = app.screen.query_one("#folder-tree", Tree)
            await wait_until(
                pilot,
                lambda: tree.root.children or (tree.root.data is not None and tree.root.data.payload.is_leaf),
                timeout=sdk_timeout,
                interval=0.03,
            )
            assert tree.root.data is not None

    asyncio.run(scenario())


def _fake_build_local_store_for(fixture: Path, label: str) -> Any:
    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return ReplayStore.from_path(fixture), label

    return _fake


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    fixture_name: str = "tui_apv1_pilot_walk.json.gz",
    label: str = "apv-sample-1",
) -> None:
    """--record-against--aware equivalent of _fake_build_local_store_for --
    used by every test in this file that supports --record-against
    (defaults match the most common case, tui_apv1_pilot_walk.json.gz/
    apv-sample-1); every other test in this file still uses the plain
    _fake_build_local_store_for(_APV1_FIXTURE, ...) helper above and
    doesn't yet support --record-against."""
    store = await record_target(fixture_name)

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, label

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


def test_esc_on_the_main_screen_closes_the_repo_and_reopens_connect_replayed(
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

            await pilot.press("escape")
            await wait_until(pilot, lambda: isinstance(app.screen, ConnectDialog), timeout=ui_timeout, interval=0.02)
            reopened_connect_dialog = isinstance(app.screen, ConnectDialog)

            # Cancel that reopened dialog (Esc again, ConnectDialog's own
            # "cancel" binding) -- lands back on BrowseScreen, now blank,
            # the same acceptable "nothing connected" state as a fresh
            # app boot (c still available to retry).
            await pilot.press("escape")
            await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=ui_timeout, interval=0.02)
            back_on_browse_screen = isinstance(app.screen, BrowseScreen)
            connections_gone = back_on_browse_screen and not app.screen.query_one("#col-catalogs", Tree).root.children
            status_cleared = back_on_browse_screen and str(app.screen.query_one("#open-status", Static).render()) == ""
            return reopened_connect_dialog, back_on_browse_screen, connections_gone, status_cleared

    reopened_connect_dialog, back_on_browse_screen, connections_gone, status_cleared = asyncio.run(scenario())
    assert reopened_connect_dialog
    assert back_on_browse_screen
    assert connections_gone
    assert status_cleared


class _BlockingContentSource:
    """A fake ``ContentSource`` whose own ``export_to`` never completes
    on its own, only via cancellation -- deterministic control over
    exactly when the job this test starts actually ends, so it can be
    cancelled on the way out rather than racing this scenario's own
    teardown. Matches ``test_browser_export_screen.py``'s own fake of
    the same shape (duplicated here rather than imported — ``tests/``
    isn't a package, see ``tests/CLAUDE.md``)."""

    size = 10
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


def test_tasks_hint_stays_in_sync_across_browse_and_unit_screens_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    drill_to_unit_screen_via_fs_device: Any,
    ui_timeout: float,
) -> None:
    """A background job started on ``BrowseScreen`` must show in
    ``UnitScreen``'s breadcrumb immediately once pushed
    (``NavigableScreen._update_breadcrumb_text`` re-reads ``app.jobs``
    fresh on ``on_mount``), keep tracking while ``UnitScreen`` stays on
    top (every ``app.store.model.jobs`` dispatch re-fires every screen's
    own ``jobs`` watch regardless of stack position; ``_render_breadcrumb``'s
    covered-screen guard only skips the widget write for a screen
    underneath, not the re-read), and stay correct back on
    ``BrowseScreen`` after Esc (``on_screen_resume`` re-renders it). The
    job is synthetic — only the hint's job *count* is under test here
    (per-job detail lives in ``WorklistScreen``'s own dialog; real
    export-progress timing needs wall-clock time a replayed fixture can't
    provide)."""
    monkeypatch.setattr(
        connect_dialog_module.ConnectDialog,
        "_build_local_store",
        _fake_build_local_store_for(_APV1_FIXTURE, "apv-sample-1"),
    )

    def _breadcrumb_text(app: ApmRepoBrowserApp) -> str:
        return str(app.screen.query_one("#breadcrumb", Static).render())

    async def scenario() -> tuple[str, str, str, str]:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name="exporting.bin",
            is_leaf=True,
            content=_BlockingContentSource(),  # genuinely satisfies the ContentSource protocol
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            idle_on_browse_screen = _breadcrumb_text(app)

            app.store.dispatch(StartExport(unit=unit, dst_text=str(tmp_path / "out.bin"), sparse=True))
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
            await pilot.pause()
            after_update = _breadcrumb_text(app)

            await pilot.press("escape")
            await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=ui_timeout, interval=0.02)
            back_on_browse_screen = _breadcrumb_text(app)

            # Cancel and wait for it to actually finish before the scenario
            # returns -- otherwise run_test()'s own teardown cancels the
            # still-running job itself (on_unmount's cleanup loop), which
            # races the App's own widget teardown and can raise NoMatches
            # from a widget query during job-finish handling.
            app.store.dispatch(CancelJobRequested(job_id=job_id))
            await wait_until(pilot, lambda: not app.jobs, timeout=ui_timeout, interval=0.02)

            return idle_on_browse_screen, on_unit_screen, after_update, back_on_browse_screen

    idle_on_browse_screen, on_unit_screen, after_update, back_on_browse_screen = asyncio.run(scenario())
    assert "Task" not in idle_on_browse_screen  # no jobs yet -- no suffix at all
    assert "1 Task (t)" in on_unit_screen
    assert "1 Task (t)" in after_update  # count unaffected by a progress tick
    assert "1 Task (t)" in back_on_browse_screen


async def _first_leaf(
    app: ApmRepoBrowserApp,
    pilot: Any,
    wait_until: Any,
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> Node:
    """DFS with backtracking, not a blind ``children[0]`` walk (a real
    directory's own first child can be an empty one at any depth).
    Driven straight through the store (``ChildrenRequested`` + real
    replayed provider fetches), not the folder-tree widget: the tree
    only ever shows containers now, so a leaf -- and this one
    specifically needs a leaf with a real declared ``size`` -- can never
    be found by walking it.

    Also points the file table's own cursor at the leaf found (focus +
    cursor position, no Enter press) before returning."""
    unit_screen = app.screen
    assert isinstance(unit_screen, UnitScreen)
    await wait_until(pilot, lambda: unit_screen.store.model.root is not None, timeout=sdk_timeout, interval=0.02)
    root = unit_screen.store.model.root
    assert root is not None and not root.is_leaf, "version root is itself a leaf -- no parent to select it under"

    async def _search(node: Node, depth: int) -> tuple[Node, Node] | None:
        if depth >= 8:
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
        leaf = next((c for c in level.children if c.is_leaf and c.size is not None), None)
        if leaf is not None:
            return leaf, node
        for child in level.children:
            if not child.is_leaf:
                found = await _search(child, depth + 1)
                if found is not None:
                    return found
        return None

    found = await _search(root, 0)
    assert found is not None, "no leaf with a declared size found"
    leaf, parent = found

    unit_screen._select_folder_ref(parent)
    await wait_until(pilot, lambda: leaf in unit_screen._file_table._nodes, timeout=ui_timeout, interval=0.02)
    row_index = unit_screen._file_table._nodes.index(leaf)
    table = unit_screen.query_one("#file-table", DataTable)
    await focus_widget(pilot, table)
    table.cursor_coordinate = Coordinate(row_index, 0)
    return leaf


def test_detail_panel_and_export_dialog_show_human_readable_sizes_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    drill_to_unit_screen_via_fs_device: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[str, str, int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen), app.screen
            leaf = await _first_leaf(app, pilot, wait_until, focus_widget, move_cursor_to, ui_timeout, sdk_timeout)
            assert leaf.is_leaf and leaf.size is not None
            size = leaf.size

            # _first_leaf already left the file table focused with its
            # own cursor on this row.
            unit_screen.action_show_detail()
            await wait_until(
                pilot,
                lambda: str(unit_screen.query_one("#detail", Static).render()),
                timeout=ui_timeout,
                interval=0.02,
                message="detail pane never rendered",
            )
            detail_text = str(unit_screen.query_one("#detail", Static).render())

            unit_screen.action_export_selected()
            await wait_until(pilot, lambda: isinstance(app.screen, ExportScreen), timeout=sdk_timeout, interval=0.02)
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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    drill_to_unit_screen_via_fs_device: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    async def scenario() -> tuple[bool, bool, bool, int, int]:
        await _patch_local_store(monkeypatch, record_target)
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            await drill_to_unit_screen_via_fs_device(app, pilot)
            unit_screen = app.screen
            assert isinstance(unit_screen, UnitScreen), app.screen
            await _first_leaf(app, pilot, wait_until, focus_widget, move_cursor_to, ui_timeout, sdk_timeout)
            # _first_leaf already left the file table focused with its
            # own cursor on this row.
            unit_screen.action_export_selected()
            # isinstance(app.screen, ExportScreen) alone only proves the
            # screen has been pushed, not that its compose() has actually
            # mounted a child yet -- querying "Vertical" immediately after
            # can race an unmounted screen (NoMatches) on a slow/busy
            # runner. Wait for the query to actually resolve too.
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen) and bool(app.screen.query("Vertical")),
                timeout=sdk_timeout,
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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    ui_timeout: float,
    sdk_timeout: float,
) -> None:
    """Against ``tui_apv2_encrypted_pilot_walk.json.gz`` — guards the
    same sqlite cross-thread open/close bug
    ``tests/unit/sdk/test_storage_sqlite.py``'s
    ``test_connection_opened_in_worker_thread_can_be_closed_from_main_thread``
    isolates synthetically against the real ``open_sqlite()``/``aiosqlite``
    code path."""
    key_string = _APV2_ENCRYPTED_KEY_STRING

    async def scenario() -> None:
        await _patch_local_store(
            monkeypatch, record_target, "tui_apv2_encrypted_pilot_walk.json.gz", "apv-sample-2-encrypted"
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            await focus_widget(pilot, cat_tree)
            await pilot.press("enter")
            await wait_until(pilot, lambda: isinstance(app.screen, KeyDialog), timeout=sdk_timeout, interval=0.02)
            assert isinstance(app.screen, KeyDialog), app.screen

            key_input = app.screen.query_one("#key-input", Input)
            key_input.value = key_string
            await focus_widget(pilot, key_input)
            await pilot.press("enter")
            await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=sdk_timeout, interval=0.03)
            assert isinstance(app.screen, BrowseScreen), app.screen

            wl_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: wl_tree.root.children, timeout=sdk_timeout, interval=0.03)
            assert wl_tree.root.children, "workload list never populated after key verification"
            await focus_widget(pilot, wl_tree)
            await pilot.press("enter")
            ver_table = app.screen.query_one("#col-versions", DataTable)
            # row_count alone can be stale; wait for _visible_version_indices
            # too (see tests/conftest.py's drill_to_unit_screen_via_fs_device).
            await wait_until(
                pilot,
                lambda: ver_table.row_count and app.screen._visible_version_indices,
                timeout=sdk_timeout,
                interval=0.02,
            )
            if ver_table.row_count:
                await focus_widget(pilot, ver_table)
                await pilot.press("enter")
                await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=ui_timeout, interval=0.03)

    asyncio.run(scenario())  # must not raise sqlite3.ProgrammingError on shutdown


def test_connect_dialog_reports_repos_found_incrementally_while_scanning_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
) -> None:
    """Against ``tui_s3sample2_pilot_walk.json.gz``.

    ``s3-sample-2-encrypted`` has two sibling repo-ids sharing one bucket
    — discovery correctly yields *one* ``Repository`` (holding both as
    ``catalogs()``), never two separate ones, so this test's own
    premise — verifying progress ticks once per *repository* as several
    repositories stream in — can't be exercised by this specific fixture,
    since it never discovers more than one repository. What's left worth checking here is that
    discovery still reports found=1 exactly once and the tree ends up
    with exactly one repository node. Coverage for progress reporting across
    genuinely multiple, separate repositories (distinct buckets/vaults) lives at
    the synthetic `Session.discover` layer (`tests/unit/sdk/test_api.py`'s
    own discover-progress tests), not here."""
    found_counts: list[int] = []

    class _RecordingProgressMeter(ProgressMeter):
        async def update(self, progress: Progress) -> None:
            if progress.found is not None:
                found_counts.append(progress.found)
            await super().update(progress)

    monkeypatch.setattr(connect_dialog_module, "ProgressMeter", _RecordingProgressMeter)

    async def scenario() -> int:
        await _patch_local_store(
            monkeypatch, record_target, "tui_s3sample2_pilot_walk.json.gz", "s3-sample-2-encrypted"
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen
            tree = app.screen.query_one("#col-catalogs", Tree)
            return len(tree.root.children)

    final_count = asyncio.run(scenario())
    assert found_counts == [1], f"repository-found progress must report the one bucket found, got {found_counts}"
    assert final_count == 1


def test_default_repo_lands_on_the_first_discovered_one_not_the_last_without_auto_expanding_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
    sdk_timeout: float,
) -> None:
    """Against ``s3-sample-2-encrypted``, whose two sibling repo-ids
    sharing one bucket resolve into a single ``Repository`` (holding both
    as ``catalogs()``) — with only one discovered repository, picking it is not
    proof the "first, not last" ordering logic is right; that distinction
    needs a sample with genuinely separate repositories (distinct buckets/vaults)
    discovered together, none committed as a fixture here. What this test
    does check — and still exercises the same `_apply_discovered`/``RepoAdded``
    code path — is that the sole discovered repository becomes
    ``app.repo_handle`` and the column-1 cursor lands on the root without
    auto-expanding it,
    both real, load-bearing behaviors `_apply_discovered`/`on_mount`
    implement. The synthetic
    `test_multiple_sibling_vaults`-style coverage in
    `tests/unit/sdk/test_storage_layout.py` is the closest existing
    substitute for "first, not last" itself, at the discovery layer rather
    than this screen's own."""

    async def scenario() -> tuple[bool, bool, int]:
        await _patch_local_store(
            monkeypatch, record_target, "tui_s3sample2_pilot_walk.json.gz", "s3-sample-2-encrypted"
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            assert isinstance(app.screen, ConnectDialog), app.screen
            dialog = app.screen
            dialog.query_one("#connect-local-path", Input).value = str(tmp_path)
            dialog.query_one("#connect-submit", Button).press()
            await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=sdk_timeout, interval=0.03)
            screen = app.screen
            assert isinstance(screen, BrowseScreen), screen
            # BrowseScreen's own transition (waited for above) only means the
            # screen itself has mounted, not that ConnectDialog's discovered
            # repositories have already been applied to it -- same
            # `open_browser_pilot`-documented race, waited out here the same
            # way (``tree.root.children``) since this test can't route
            # through that fixture (it must not auto-expand/select anything).
            tree = screen.query_one("#col-catalogs", Tree)
            await wait_until(
                pilot,
                lambda: tree.root.children,
                timeout=sdk_timeout,
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


def test_workloads_raises_key_required_on_an_encrypted_repo_before_a_key_is_verified_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    focus_widget: Any,
    move_cursor_to: Any,
) -> None:
    """``Repository.catalogs()`` still succeeds on a locked repository (its
    underlying tables are genuinely unencrypted); ``Catalog.workloads()``
    raises ``KeyRequiredError`` before any catalog I/O — see
    ``api/repository.py``'s ``_require_key_verified``."""

    async def scenario() -> tuple[bool, bool]:
        await _patch_local_store(
            monkeypatch, record_target, "tui_s3sample2_pilot_walk.json.gz", "s3-sample-2-encrypted"
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            assert isinstance(app.screen, BrowseScreen), app.screen

            repo = app.current_repo
            assert repo is not None
            not_verified = repo.key_status is not KeyStatus.VERIFIED
            catalogs = await repo.catalogs()
            results = [await _raises_key_required(catalog) for catalog in catalogs]
            return not_verified, bool(results) and all(results)

    not_verified, raised_for_every_connection = asyncio.run(scenario())
    assert not_verified, "test invariant: app.repo_handle's repo must still be locked (no key verified) for this sample"
    assert raised_for_every_connection, (
        "an unverified encrypted repository's workloads() must raise KeyRequiredError, not succeed"
    )


async def _raises_key_required(catalog: Any) -> bool:
    try:
        await catalog.workloads()
    except KeyRequiredError:
        return True
    return False


__all__: list[str] = []
