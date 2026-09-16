"""``textual`` ``Pilot``-driven coverage for ``WorklistScreen`` (the
``t`` panel) — driven against plain ``BackgroundJob`` instances
constructed directly, so this needs no real export/session and lives in
``tests/unit/``.

``_FakeApp`` mirrors ``test_browser_unit_screen_pagination.py``'s own
minimal-host convention: ``WorklistScreen`` only ever reads
``app_state.jobs`` (``NavigableScreen.app_state`` is just ``self.app``,
duck-typed, no runtime type check), so a bare ``App`` subclass exposing
that one attribute satisfies it without going through
``ApmRepoBrowserApp``'s own auto-pushed ``BrowseScreen``/``ConnectDialog``."""

from __future__ import annotations

import dataclasses
from typing import Any

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.app import BackgroundJob, JobStatus
from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen


class _FakeApp(App[None]):
    def __init__(self, jobs: dict[int, BackgroundJob]) -> None:
        super().__init__()
        self.jobs = jobs

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(WorklistScreen())

    def mark_job_cancelling(self, job_id: int) -> None:
        """Same shape as ``ApmRepoBrowserApp.mark_job_cancelling`` — this
        fake only needs the one method ``WorklistScreen.action_cancel_selected``
        actually calls."""
        job = self.jobs.get(job_id)
        if job is not None:
            self.jobs[job_id] = dataclasses.replace(job, status=JobStatus.CANCELLING)


async def test_empty_worklist_shows_the_empty_status(wait_until: Any) -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        status = app.screen.query_one("#status-bar", Static)
        await wait_until(pilot, lambda: "No background jobs" in str(status.render()), timeout=0.6, interval=0.02)


async def test_jobs_render_percent_or_done_count_by_availability(wait_until: Any) -> None:
    jobs = {
        1: BackgroundJob(id=1, label="export-a.bin", done=50, total=100, status=JobStatus.RUNNING),
        2: BackgroundJob(id=2, label="export-b.bin", done=7, total=None, status=JobStatus.RUNNING),
    }
    app = _FakeApp(jobs)
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 2, timeout=0.6, interval=0.02)
        rows = [tuple(table.get_row_at(i)) for i in range(table.row_count)]
        assert ("export-a.bin", "50%", "running") in rows
        assert ("export-b.bin", "7 done", "running") in rows


async def test_the_table_refreshes_live_from_the_periodic_timer_not_just_on_mount(wait_until: Any) -> None:
    # WorklistScreen's own docstring claim ("refreshing live") was never
    # actually exercised over time -- every other test either checks
    # state right after mount or right after manually calling
    # screen._refresh(). This mutates app.jobs post-mount and waits for
    # the real _REFRESH_INTERVAL-driven set_interval(self._refresh) to
    # pick it up on its own, calling nothing directly.
    app = _FakeApp({})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 0, timeout=0.6, interval=0.02)

        app.jobs[1] = BackgroundJob(id=1, label="late-job.bin", done=1, total=None, status=JobStatus.RUNNING)
        # Real time, past _REFRESH_INTERVAL (0.5s) -- not a manual _refresh() call.
        await wait_until(pilot, lambda: table.row_count == 1, timeout=1.2, interval=0.02)
        assert table.row_count == 1
        assert tuple(table.get_row_at(0)) == ("late-job.bin", "1 done", "running")


async def test_cancel_selected_cancels_the_job_under_the_cursor(wait_until: Any) -> None:
    cancelled = False

    class _Job(BackgroundJob):
        def cancel(self) -> None:
            nonlocal cancelled
            cancelled = True

    job = _Job(id=1, label="export-a.bin", done=10, total=100, status=JobStatus.RUNNING)
    app = _FakeApp({1: job})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 1, timeout=0.6, interval=0.02)
        table.focus()

        await pilot.press("x")
        await wait_until(pilot, lambda: cancelled, timeout=0.6, interval=0.02)
        assert cancelled
        # action_cancel_selected's own mark_job_cancelling call replaces
        # the registry entry (BackgroundJob is frozen) — the original
        # ``job`` reference here stays at its pre-cancel status.
        assert app.jobs[1].status is JobStatus.CANCELLING


async def test_cancel_selected_on_empty_table_is_a_no_op() -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        table.focus()
        await pilot.press("x")  # must not raise
        await pilot.pause()


async def test_a_job_with_no_percent_and_no_done_count_shows_a_dash(wait_until: Any) -> None:
    job = BackgroundJob(id=1, label="export-a.bin", done=0, total=None, status=JobStatus.RUNNING)
    app = _FakeApp({1: job})
    async with app.run_test() as pilot:
        table = app.screen.query_one("#worklist-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 1, timeout=0.6, interval=0.02)
        assert tuple(table.get_row_at(0)) == ("export-a.bin", "-", "running")


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


async def test_action_go_back_pops_the_screen() -> None:
    app = _FakeApp({})
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, WorklistScreen)
        screen.action_go_back()
        await pilot.pause()
        assert app.screen is not screen


__all__: list[str] = []
