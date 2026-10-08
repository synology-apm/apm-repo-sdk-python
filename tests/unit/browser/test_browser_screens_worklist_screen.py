"""``Pilot`` tests for ``WorklistScreen`` (the ``t`` panel) against plain
``Job`` instances. ``_FakeApp.jobs`` is a real reactive because
``WorklistScreen.on_mount`` watches it, and ``store`` is a real ``Store``
because ``action_cancel_selected`` dispatches ``CancelJobRequested``."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from textual.app import App, ComposeResult
from textual.reactive import var
from textual.widgets import DataTable, Static

from support.pilot import SDK_TIMEOUT, UI_TIMEOUT, settle, wait_for_screen, wait_until
from synology_apm_repo.browser.core.app.cmd import AppCmd
from synology_apm_repo.browser.core.app.model import AppModel, Job, JobStatus
from synology_apm_repo.browser.core.app.msg import AppMsg, CancelJobRequested
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.runtime.app_effects import AppEffects
from synology_apm_repo.browser.runtime.load_gate import LoadGate
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen
from synology_apm_repo.browser.strings import WORKLIST_HINT


class _FakeApp(App[None]):
    jobs: var[Mapping[JobId, Job]] = var(dict)

    def __init__(self, jobs: dict[JobId, Job]) -> None:
        super().__init__()
        self.jobs = jobs
        # Seeded with the same `jobs`: update() ignores a cancel for an
        # unknown id.
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(jobs=jobs), update, self._perform)
        self.effects = AppEffects(self, self.store, LoadGate())
        self.store.subscribe(lambda model: model.jobs, self._sync_jobs, init=False)

    def _perform(self, cmd: AppCmd) -> None:
        self.effects.perform(cmd)

    def _sync_jobs(self, jobs: Mapping[JobId, Job]) -> None:
        self.jobs = jobs

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(WorklistScreen())


def _record_cancel_group(app: _FakeApp, monkeypatch: pytest.MonkeyPatch) -> list[tuple[object, str]]:
    """Replaces ``app.workers.cancel_group`` with a recorder and returns its call log."""
    calls: list[tuple[object, str]] = []

    def _fake_cancel_group(node: object, group: str) -> list[Any]:
        calls.append((node, group))
        return []

    monkeypatch.setattr(app.workers, "cancel_group", _fake_cancel_group)
    return calls


def _record_dispatches(app: _FakeApp, monkeypatch: pytest.MonkeyPatch) -> list[AppMsg]:
    """Records every message dispatched to ``app.store`` (still dispatching
    it) and returns the log."""
    dispatched: list[AppMsg] = []
    real_dispatch = app.store.dispatch

    def _recording_dispatch(msg: AppMsg) -> None:
        dispatched.append(msg)
        real_dispatch(msg)

    monkeypatch.setattr(app.store, "dispatch", _recording_dispatch)
    return dispatched


async def test_empty_worklist_shows_the_empty_status() -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        status = app.screen.query_one("#status-bar", Static)
        await wait_until(pilot, lambda: "No background jobs" in str(status.render()), timeout=UI_TIMEOUT, interval=0.02)


async def test_jobs_render_percent_or_done_count_by_availability() -> None:
    jobs = {
        JobId(1): Job(id=JobId(1), label="export-a.bin", group="job-1", done=50, total=100),
        JobId(2): Job(id=JobId(2), label="export-b.bin", group="job-2", done=7, total=None),
    }
    app = _FakeApp(jobs)
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 2, timeout=UI_TIMEOUT, interval=0.02)
        rows = [tuple(table.get_row_at(i)) for i in range(table.row_count)]
        assert ("export-a.bin", "-", "50%", "-", "-", "-", "running") in rows
        assert ("export-b.bin", "-", "7 done", "-", "-", "-", "running") in rows


async def test_the_table_refreshes_live_when_the_apps_jobs_reactive_changes() -> None:
    # A post-mount change to `jobs` reaches the table only through the
    # screen's debounced watch.
    app = _FakeApp({})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        status = app.screen.query_one("#status-bar", Static)
        await wait_until(pilot, lambda: table.row_count == 0, timeout=UI_TIMEOUT, interval=0.02)
        await wait_until(pilot, lambda: "No background jobs" in str(status.render()), timeout=UI_TIMEOUT, interval=0.02)

        # Set directly rather than through StartExport, whose RunExport
        # command would need a real RestorableUnit.
        app.jobs = {JobId(1): Job(id=JobId(1), label="late-job.bin", group="job-1", done=1, total=None)}
        app.mutate_reactive(_FakeApp.jobs)
        await wait_until(
            pilot,
            lambda: table.row_count == 1,
            timeout=UI_TIMEOUT,
            interval=0.02,
            message="debounced worklist refresh never fired",
        )
        assert table.row_count == 1
        assert tuple(table.get_row_at(0)) == ("late-job.bin", "-", "1 done", "-", "-", "-", "running")
        assert str(status.render()) == WORKLIST_HINT


async def test_cancel_selected_cancels_the_job_under_the_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    job = Job(id=JobId(1), label="export-a.bin", group="job-1", done=10, total=100)
    app = _FakeApp({JobId(1): job})
    async with app.run_test() as pilot:
        cancel_calls = _record_cancel_group(app, monkeypatch)
        dispatched = _record_dispatches(app, monkeypatch)

        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 1, timeout=UI_TIMEOUT, interval=0.02)
        table.focus()

        await pilot.press("x")
        await wait_until(pilot, lambda: bool(cancel_calls), timeout=SDK_TIMEOUT, interval=0.02)
        assert cancel_calls == [(app, "job-1")]
        assert dispatched[0] == CancelJobRequested(job_id=JobId(1))
        assert app.store.model.jobs[JobId(1)].status is JobStatus.CANCELLING


async def test_cancel_selected_on_empty_table_is_a_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        cancel_calls = _record_cancel_group(app, monkeypatch)
        table = app.screen.query_one("#worklist-table", DataTable)
        table.focus()
        await pilot.press("x")  # must not raise
        await settle(pilot)
        assert cancel_calls == []
        assert app.store.model.jobs == {}
        assert isinstance(app.screen, WorklistScreen)


async def test_a_job_with_no_percent_and_no_done_count_shows_a_dash() -> None:
    job = Job(id=JobId(1), label="export-a.bin", group="job-1", done=0, total=None)
    app = _FakeApp({JobId(1): job})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 1, timeout=UI_TIMEOUT, interval=0.02)
        assert tuple(table.get_row_at(0)) == ("export-a.bin", "-", "-", "-", "-", "-", "running")


async def test_cancel_selected_on_a_row_with_no_key_value_is_a_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_refresh()`` always keys rows by ``str(job.id)``; this hand-added
    unkeyed row reaches ``action_cancel_selected``'s ``RowKey.value is None``
    guard."""
    app = _FakeApp({})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        cancel_calls = _record_cancel_group(app, monkeypatch)
        table.add_row("unkeyed", "-", "running")  # no key= -- RowKey.value is None
        table.focus()
        table.cursor_coordinate = table.cursor_coordinate  # forces a valid coordinate
        await pilot.press("x")  # must not raise
        await settle(pilot)
        assert cancel_calls == []
        assert app.store.model.jobs == {}
        assert table.row_count == 1


async def test_cancel_selected_for_a_row_whose_job_already_finished_is_a_no_op(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale row (the debounced refresh hasn't repainted yet) for a job
    the app no longer knows dispatches nothing."""
    app = _FakeApp({})
    async with app.run_test() as pilot:
        dispatched = _record_dispatches(app, monkeypatch)
        cancel_calls = _record_cancel_group(app, monkeypatch)
        table = app.screen.query_one("#worklist-table", DataTable)
        table.add_row("stale", "-", "running", key="999")
        table.focus()
        table.cursor_coordinate = table.cursor_coordinate
        await pilot.press("x")  # must not raise
        await settle(pilot)
        assert dispatched == []
        assert cancel_calls == []
        assert app.store.model.jobs == {}


async def test_action_dismiss_worklist_pops_the_screen() -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        screen = await wait_for_screen(pilot, WorklistScreen)
        screen.action_dismiss_worklist()
        await wait_until(pilot, lambda: app.screen is not screen)


async def test_a_folder_export_shows_its_position_beside_its_name() -> None:
    jobs = {
        JobId(1): Job(
            id=JobId(1), label="export docs", group="job-1", done=1, total=4, position="file 2 of 3", item="a/b.txt"
        ),
        JobId(2): Job(id=JobId(2), label="export media", group="job-2", position="scanning..."),
    }
    app = _FakeApp(jobs)
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 2, timeout=UI_TIMEOUT, interval=0.02)

        labels = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]

        assert labels == ["export docs (file 2 of 3)", "export media (scanning...)"]
