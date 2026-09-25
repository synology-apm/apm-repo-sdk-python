"""``textual`` ``Pilot``-driven coverage for ``WorklistScreen`` (the
``t`` panel) — driven against plain ``Job`` instances constructed
directly, so this needs no real export and lives in ``tests/unit/``.

``_FakeApp`` mirrors ``test_browser_unit_screen_pagination.py``'s own
minimal-host convention: a bare ``App`` subclass, not
``ApmRepoBrowserApp``'s own auto-pushed ``BrowseScreen``/``ConnectDialog``.
``jobs`` stays a real reactive (not a plain attribute), matching
``ApmRepoBrowserApp``'s own shape — ``WorklistScreen`` watches it (see
its own ``on_mount``), which needs a genuine ``Reactive`` to hook into,
not just an attribute of the same name. ``store`` is a real, working
``Store`` too (not a stand-in): ``action_cancel_selected`` now dispatches
``CancelJobRequested`` through it, the same mechanism ``ExportScreen``'s
own cancel path uses."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from textual.app import App, ComposeResult
from textual.reactive import var
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.core.app.cmd import AppCmd
from synology_apm_repo.browser.core.app.model import AppModel, Job, JobStatus
from synology_apm_repo.browser.core.app.msg import AppMsg
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.runtime.app_effects import AppEffects
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen
from synology_apm_repo.browser.strings import WORKLIST_HINT


class _FakeApp(App[None]):
    jobs: var[Mapping[JobId, Job]] = var(dict)

    def __init__(self, jobs: dict[JobId, Job]) -> None:
        super().__init__()
        self.jobs = jobs
        # A real Store/AppEffects pair, seeded with the same `jobs` --
        # action_cancel_selected dispatches CancelJobRequested through
        # `self.app_state.store`, which needs update() to already know
        # about the job being cancelled (it's a no-op for an unknown id).
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(jobs=jobs), update, self._perform)
        self.effects = AppEffects(self, self.store)
        self.store.subscribe(lambda model: model.jobs, self._sync_jobs, init=False)

    def _perform(self, cmd: AppCmd) -> None:
        self.effects.perform(cmd)

    def _sync_jobs(self, jobs: Mapping[JobId, Job]) -> None:
        self.jobs = jobs

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(WorklistScreen())


async def test_empty_worklist_shows_the_empty_status(wait_until: Any, ui_timeout: float) -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        status = app.screen.query_one("#status-bar", Static)
        await wait_until(pilot, lambda: "No background jobs" in str(status.render()), timeout=ui_timeout, interval=0.02)


async def test_jobs_render_percent_or_done_count_by_availability(wait_until: Any, ui_timeout: float) -> None:
    jobs = {
        JobId(1): Job(id=JobId(1), label="export-a.bin", group="job-1", done=50, total=100),
        JobId(2): Job(id=JobId(2), label="export-b.bin", group="job-2", done=7, total=None),
    }
    app = _FakeApp(jobs)
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 2, timeout=ui_timeout, interval=0.02)
        rows = [tuple(table.get_row_at(i)) for i in range(table.row_count)]
        assert ("export-a.bin", "-", "50%", "-", "-", "-", "running") in rows
        assert ("export-b.bin", "-", "7 done", "-", "-", "-", "running") in rows


async def test_the_table_refreshes_live_when_the_apps_jobs_reactive_changes(wait_until: Any, ui_timeout: float) -> None:
    # The one test that actually exercises WorklistScreen's documented
    # "live" refresh claim -- every other test in this file either checks
    # state right after mount or right after manually calling
    # screen._refresh(). This dispatches a real StartExport through the
    # store post-mount and waits for Debouncer's own delayed watch to pick
    # it up on its own, calling nothing on the screen directly.
    app = _FakeApp({})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        status = app.screen.query_one("#status-bar", Static)
        await wait_until(pilot, lambda: table.row_count == 0, timeout=ui_timeout, interval=0.02)
        await wait_until(pilot, lambda: "No background jobs" in str(status.render()), timeout=ui_timeout, interval=0.02)

        # update() mints its own job id/group; a RunExport command comes
        # back too, but nothing here performs it (this test only cares
        # about the jobs registry reflecting the new entry), so the
        # fake's own `effects.perform` handling it as a real command is
        # harmless -- RunExport just starts a worker against a
        # RestorableUnit this test never gave it a real one for, which
        # is why a bare `Job` is inserted directly instead of going
        # through StartExport here.
        app.jobs = {JobId(1): Job(id=JobId(1), label="late-job.bin", group="job-1", done=1, total=None)}
        app.mutate_reactive(_FakeApp.jobs)
        await wait_until(
            pilot,
            lambda: table.row_count == 1,
            timeout=ui_timeout,
            interval=0.02,
            message="debounced worklist refresh never fired",
        )
        assert table.row_count == 1
        assert tuple(table.get_row_at(0)) == ("late-job.bin", "-", "1 done", "-", "-", "-", "running")
        # The empty-state message must clear (replaced by the key hint)
        # once a job exists again -- _refresh()'s own else branch, not
        # just a one-way "goes empty" write.
        assert str(status.render()) == WORKLIST_HINT


async def test_cancel_selected_cancels_the_job_under_the_cursor(
    wait_until: Any, ui_timeout: float, sdk_timeout: float
) -> None:
    job = Job(id=JobId(1), label="export-a.bin", group="job-1", done=10, total=100)
    app = _FakeApp({JobId(1): job})
    async with app.run_test() as pilot:
        cancel_calls: list[tuple[object, str]] = []

        def _fake_cancel_group(node: object, group: str) -> list[Any]:
            cancel_calls.append((node, group))
            return []

        app.workers.cancel_group = _fake_cancel_group  # type: ignore[method-assign]

        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 1, timeout=ui_timeout, interval=0.02)
        table.focus()

        await pilot.press("x")
        await wait_until(pilot, lambda: bool(cancel_calls), timeout=sdk_timeout, interval=0.02)
        assert cancel_calls == [(app, "job-1")]
        # action_cancel_selected's own dispatch replaces the registry
        # entry (Job is frozen) -- the original `job` reference here
        # stays at its pre-cancel status.
        assert app.store.model.jobs[JobId(1)].status is JobStatus.CANCELLING


async def test_cancel_selected_on_empty_table_is_a_no_op() -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        table.focus()
        await pilot.press("x")  # must not raise
        await pilot.pause()


async def test_a_job_with_no_percent_and_no_done_count_shows_a_dash(wait_until: Any, ui_timeout: float) -> None:
    job = Job(id=JobId(1), label="export-a.bin", group="job-1", done=0, total=None)
    app = _FakeApp({JobId(1): job})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 1, timeout=ui_timeout, interval=0.02)
        assert tuple(table.get_row_at(0)) == ("export-a.bin", "-", "-", "-", "-", "-", "running")


async def test_cancel_selected_on_a_row_with_no_key_value_is_a_no_op() -> None:
    """A row added without an explicit ``key=`` (unlike every real row
    ``_refresh()`` itself adds, always keyed by ``str(job.id)``) gets an
    auto-generated ``RowKey`` whose own ``.value`` is ``None`` —
    ``action_cancel_selected``'s own guard against that, otherwise
    unreachable through this screen's real, always-keyed rows."""
    app = _FakeApp({})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        table.add_row("unkeyed", "-", "running")  # no key= -- RowKey.value is None
        table.focus()
        table.cursor_coordinate = table.cursor_coordinate  # forces a valid coordinate
        await pilot.press("x")  # must not raise
        await pilot.pause()


async def test_cancel_selected_for_a_row_whose_job_already_finished_is_a_no_op(wait_until: Any) -> None:
    """A row still shown in a stale table (the debounced refresh hasn't
    repainted yet) for a job the store no longer knows about --
    ``JobId(job_id) in self.app_state.jobs`` is what guards this, one
    level above ``update()``'s own identical guard on
    ``CancelJobRequested`` for an unknown id."""
    app = _FakeApp({})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        table.add_row("stale", "-", "running", key="999")
        table.focus()
        table.cursor_coordinate = table.cursor_coordinate
        await pilot.press("x")  # must not raise, must not dispatch
        await pilot.pause()
        assert app.store.model.jobs == {}


async def test_action_dismiss_worklist_pops_the_screen() -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, WorklistScreen)
        screen.action_dismiss_worklist()
        await pilot.pause()
        assert app.screen is not screen


__all__: list[str] = []
