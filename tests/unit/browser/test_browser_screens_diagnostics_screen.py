"""``Pilot`` tests for ``DiagnosticsScreen`` (the ``v`` panel), against a
fake repository whose ``verify()`` returns canned ``Finding``\\ s."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from textual.screen import Screen
from textual.widgets import DataTable, Static

import synology_apm_repo.sdk.api as _sdk_api
from support.fakes import faithful_to
from support.pilot import SDK_TIMEOUT, wait_for_screen, wait_until
from synology_apm_repo.browser.core.app.cmd import AppCmd
from synology_apm_repo.browser.core.app.model import AppModel, Job
from synology_apm_repo.browser.core.app.msg import AppMsg
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen, _status_base
from synology_apm_repo.browser.strings import (
    DIAGNOSTICS_EXPORT_BUSY_WARNING,
    DIAGNOSTICS_FULL_ALREADY_RUNNING_WARNING,
    DIAGNOSTICS_FULL_RUNNING_STATUS,
    DIAGNOSTICS_QUICK_STATUS,
    DIAGNOSTICS_QUICK_STILL_RUNNING_WARNING,
)
from synology_apm_repo.sdk.api import Finding, Session, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.progress import Progress
from unit.browser.screen_host_fakes import ScreenHostApp

_QUICK_FINDING = Finding(Stage.FILE_MAP, Symptom.FILE_MISSING, "db/file_map", "file_map not found")
_FULL_FINDING = Finding(Stage.BUCKET, Symptom.CORRUPTION, "bucket 1/2", "checksum mismatch")


@faithful_to(_sdk_api.Repository)
class _FakeRepo:
    def __init__(self, verify: Callable[[VerifyLevel], list[Finding] | ApmRepoError]) -> None:
        self._verify = verify

    async def verify(self, level: VerifyLevel = VerifyLevel.QUICK, *, progress: Any = None) -> list[Finding]:
        result = self._verify(level)
        if isinstance(result, Exception):
            raise result
        return result


class _FakeApp(ScreenHostApp):
    """A real ``Store`` as ``self.store``: ``action_run_full``'s guard reads
    its model and dispatches ``VerifyFullStarted`` through it, ``_run``'s
    ``finally`` ``VerifyFullFinished``. Its effect runner is a no-op: no
    export starts here."""

    def __init__(self, repo: _FakeRepo, *, model: AppModel | None = None) -> None:
        super().__init__(repo=repo, session=cast(Session, object()))
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(
            model if model is not None else AppModel(), update, lambda cmd: None
        )

    def screens(self) -> list[Screen[Any]]:
        return [DiagnosticsScreen()]


async def test_quick_check_with_findings_populates_the_table() -> None:
    app = _FakeApp(_FakeRepo(lambda level: [_QUICK_FINDING]))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        await wait_until(pilot, lambda: table.row_count > 0, timeout=SDK_TIMEOUT, interval=0.02)
        # One group (a single finding) renders as a header row plus one
        # instance row.
        assert table.row_count == 2
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "finding" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert "1 finding " in str(status.render())
        assert "level=quick" in str(status.render())


async def test_findings_are_grouped_and_ref_only_shown_when_verbose() -> None:
    """``ref`` follows the verbose toggle live (``refresh_for_verbose_mode``), not only on the next check."""
    ref_finding = Finding(
        Stage.VERSION, Symptom.FILE_MISSING, "wl/2024-01-01 00:00:00", "boom", ref="local:/repo#cat:1/wl:2/ver:abc"
    )
    app = _FakeApp(_FakeRepo(lambda level: [_QUICK_FINDING, _QUICK_FINDING, ref_finding]))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        await wait_until(pilot, lambda: table.row_count > 0, timeout=SDK_TIMEOUT, interval=0.02)
        # Two identical _QUICK_FINDINGs collapse into one group (a header
        # row plus two instance rows); ref_finding is its own group (a
        # header row plus one instance row).
        assert table.row_count == 5

        def _ref_visible() -> bool:
            rows = (table.get_row_at(i) for i in range(table.row_count))
            return any("cat:1/wl:2/ver:abc" in str(cell) for row in rows for cell in row)

        assert not _ref_visible()

        app.verbose = True
        screen = app.screen
        assert isinstance(screen, DiagnosticsScreen)
        screen.refresh_for_verbose_mode()
        assert _ref_visible()


async def test_quick_check_clean_reports_clean_status() -> None:
    app = _FakeApp(_FakeRepo(lambda level: []))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert "press f for a full check" in str(status.render())


async def test_repaired_via_parity_only_reports_clean_not_red() -> None:
    """``Symptom.REPAIRED_VIA_PARITY`` is a successful self-heal, not counted as a finding."""
    repaired = Finding(
        Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, "bucket 1/2", "SizeStore CRC mismatch repaired via parity"
    )
    app = _FakeApp(_FakeRepo(lambda level: [repaired]))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert "1 self-repaired via parity" in str(status.render())
        assert "finding" not in str(status.render())


async def test_pressing_f_runs_a_full_check_and_replaces_the_findings() -> None:
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return [_QUICK_FINDING] if level is VerifyLevel.QUICK else [_FULL_FINDING, _FULL_FINDING]

    app = _FakeApp(_FakeRepo(verify))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        # One group (a single QUICK finding): a header row plus one instance row.
        await wait_until(pilot, lambda: table.row_count == 2, timeout=SDK_TIMEOUT, interval=0.02)

        await pilot.press("f")
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "level=full" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)

        assert calls == [VerifyLevel.QUICK, VerifyLevel.FULL]
        # Both FULL findings are identical, so they land in one group: one
        # header row plus two instance rows.
        assert table.row_count == 3
        assert "2 findings" in str(status.render())


async def test_verify_error_shows_in_the_status_line() -> None:
    app = _FakeApp(_FakeRepo(lambda level: ApmRepoError("repo is locked")))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert "repo is locked" in str(status.render())


async def test_action_go_back_pops_the_screen() -> None:
    app = _FakeApp(_FakeRepo(lambda level: []))
    async with app.run_test() as pilot:
        screen = await wait_for_screen(pilot, DiagnosticsScreen)
        screen.action_go_back()
        await wait_until(pilot, lambda: app.screen is not screen)


def test_status_base_picks_the_quick_or_full_running_text_by_level() -> None:
    assert _status_base(VerifyLevel.QUICK) == DIAGNOSTICS_QUICK_STATUS
    assert _status_base(VerifyLevel.FULL) == DIAGNOSTICS_FULL_RUNNING_STATUS


def test_verify_status_text_shows_phase_and_progress_once_a_tick_arrives() -> None:
    """``_verify_status_text`` is what the debounced spinner's ``base()`` re-reads every tick."""
    screen = DiagnosticsScreen()
    assert screen._verify_status_text(VerifyLevel.FULL) == DIAGNOSTICS_FULL_RUNNING_STATUS
    screen._verify_progress = Progress(phase="verifying", determinate=True, done=3, total=10, unit="buckets")
    assert screen._verify_status_text(VerifyLevel.FULL) == f"{DIAGNOSTICS_FULL_RUNNING_STATUS} — verifying (3/10)"
    # QUICK ignores it even if set.
    assert screen._verify_status_text(VerifyLevel.QUICK) == DIAGNOSTICS_QUICK_STATUS


async def test_action_run_full_refuses_while_an_export_is_running() -> None:
    """A FULL check must not overlap an export's process pool."""
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return []

    running_job = Job(id=JobId(1), label="export a.bin", group="job-1")
    app = _FakeApp(_FakeRepo(verify), model=AppModel(jobs={JobId(1): running_job}))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert calls == [VerifyLevel.QUICK]  # QUICK is unaffected by the guard

        await pilot.press("f")
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert DIAGNOSTICS_EXPORT_BUSY_WARNING in str(status.render())
        assert calls == [VerifyLevel.QUICK]  # FULL never actually ran
        assert app.store.model.verify_full_running is False


async def test_action_run_full_refuses_while_another_full_check_is_already_running() -> None:
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return []

    app = _FakeApp(_FakeRepo(verify), model=AppModel(verify_full_running=True))
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, DiagnosticsScreen)
        await pilot.press("f")
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert DIAGNOSTICS_FULL_ALREADY_RUNNING_WARNING in str(status.render())
        assert VerifyLevel.FULL not in calls


async def test_verify_full_running_flag_is_cleared_after_a_full_check_finishes() -> None:
    """``VerifyFullFinished`` fires from ``_run``'s ``finally``, through the real ``app.store``."""
    running_during_full: list[bool] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        if level is not VerifyLevel.FULL:
            return []
        running_during_full.append(app.store.model.verify_full_running)
        return [_FULL_FINDING]

    app = _FakeApp(_FakeRepo(verify))
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, DiagnosticsScreen)
        status = app.screen.query_one("#diag-status", Static)
        # The mount's QUICK check has finished, so "f" isn't refused as still running.
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)

        await pilot.press("f")
        await wait_until(pilot, lambda: "level=full" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert running_during_full == [True]
        assert app.store.model.verify_full_running is False


async def test_action_run_full_refuses_while_quick_is_still_running() -> None:
    """``_run``'s ``@work`` has no ``group``/``exclusive``, so ``_verify_running`` alone keeps a FULL worker
    from racing the mount's QUICK one. It is set directly: the fake QUICK check may already be done."""
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return []

    app = _FakeApp(_FakeRepo(verify))
    async with app.run_test() as pilot:
        screen = await wait_for_screen(pilot, DiagnosticsScreen)
        screen._verify_running = True
        screen.action_run_full()
        status = screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert DIAGNOSTICS_QUICK_STILL_RUNNING_WARNING in str(status.render())
        assert VerifyLevel.FULL not in calls
