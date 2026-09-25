"""``textual`` ``Pilot``-driven coverage for ``DiagnosticsScreen`` (the
``v`` panel) — driven against a fake, sample-independent repository whose
``verify()`` returns canned ``Finding``\\ s, so this lives in
``tests/unit/`` rather than needing a real sample.

``_FakeApp`` mirrors ``test_browser_unit_screen_pagination.py``'s own
minimal-host convention: ``DiagnosticsScreen`` only ever reads
``app_state.current_repo`` (``NavigableScreen.app_state`` is just
``self.app``, duck-typed, no runtime type check), so a bare ``App``
subclass exposing that property (plus ``resources``/``repo_handle``,
which it's derived from) satisfies it without going through
``ApmRepoBrowserApp``'s own auto-pushed ``BrowseScreen``/``ConnectDialog``."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.core.app.cmd import AppCmd
from synology_apm_repo.browser.core.app.model import AppModel, Job
from synology_apm_repo.browser.core.app.msg import AppMsg
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen, _status_base
from synology_apm_repo.browser.strings import (
    DIAGNOSTICS_EXPORT_BUSY_WARNING,
    DIAGNOSTICS_FULL_ALREADY_RUNNING_WARNING,
    DIAGNOSTICS_FULL_RUNNING_STATUS,
    DIAGNOSTICS_QUICK_STATUS,
    DIAGNOSTICS_QUICK_STILL_RUNNING_WARNING,
)
from synology_apm_repo.sdk.api import Finding, Repository, Session, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.progress import Progress

_QUICK_FINDING = Finding(Stage.FILE_MAP, Symptom.FILE_MISSING, "db/file_map", "file_map not found")
_FULL_FINDING = Finding(Stage.BUCKET, Symptom.CORRUPTION, "bucket 1/2", "checksum mismatch")


class _FakeRepo:
    def __init__(self, verify: Callable[[VerifyLevel], list[Finding] | ApmRepoError]) -> None:
        self._verify = verify

    async def verify(self, level: VerifyLevel, *, progress: Any = None) -> list[Finding]:
        result = self._verify(level)
        if isinstance(result, Exception):
            raise result
        return result


class _FakeApp(App[None]):
    """``self.store`` mirrors ``ApmRepoBrowserApp``'s own shape (a real,
    working ``Store``) -- ``action_run_full``'s mutual-exclusion guard
    reads ``app.store.model.export_occupied``/``.verify_full_running``,
    and dispatches ``VerifyFullStarted``/``VerifyFullFinished`` through
    it, same as ``AppEffects``'s real ``perform`` callback would for
    anything those two messages might return (nothing, in every scenario
    here -- no export is ever actually started in this file)."""

    def __init__(self, repo: _FakeRepo, *, model: AppModel | None = None) -> None:
        super().__init__()
        # NavigableScreen.app_state is just self.app (duck-typed) --
        # _show_findings reads .verbose directly, and on_mount's own
        # self.watch(self.app, "verbose", ...) call needs the attribute to
        # exist at all -- same convention every other fake app in this
        # package's tests already follows.
        self.verbose = False
        self.resources = ResourceTable(cast(Session, object()))
        self.repo_handle: RepoHandle | None = self.resources.put_repo(cast(Repository, repo))
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(
            model if model is not None else AppModel(), update, lambda cmd: None
        )

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(DiagnosticsScreen())


async def test_quick_check_with_findings_populates_the_table(wait_until: Any, sdk_timeout: float) -> None:
    app = _FakeApp(_FakeRepo(lambda level: [_QUICK_FINDING]))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        await wait_until(pilot, lambda: table.row_count > 0, timeout=sdk_timeout, interval=0.02)
        # One group (a single finding) renders as a header row plus one
        # instance row.
        assert table.row_count == 2
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "finding(s)" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert "1 finding(s)" in str(status.render())
        assert "level=quick" in str(status.render())


async def test_findings_are_grouped_and_ref_only_shown_when_verbose(wait_until: Any, sdk_timeout: float) -> None:
    """This screen groups/sorts findings like the CLI's own ``verify``
    command and gates ``ref`` behind the app's own verbose toggle,
    refreshing live on ``d`` (``refresh_for_verbose_mode``) rather than
    only on the next check."""
    ref_finding = Finding(
        Stage.VERSION, Symptom.FILE_MISSING, "wl/2024-01-01 00:00:00", "boom", ref="local:/repo#cat:1/wl:2/ver:abc"
    )
    app = _FakeApp(_FakeRepo(lambda level: [_QUICK_FINDING, _QUICK_FINDING, ref_finding]))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        await wait_until(pilot, lambda: table.row_count > 0, timeout=sdk_timeout, interval=0.02)
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


async def test_quick_check_clean_reports_clean_status(wait_until: Any, sdk_timeout: float) -> None:
    app = _FakeApp(_FakeRepo(lambda level: []))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert "press f for a full check" in str(status.render())


async def test_repaired_via_parity_only_reports_clean_not_red(wait_until: Any, sdk_timeout: float) -> None:
    """``Symptom.REPAIRED_VIA_PARITY`` is a successful self-heal, not a
    problem left unresolved -- ``_show_findings`` must not fold it into
    the red "finding(s)" count."""
    repaired = Finding(
        Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, "bucket 1/2", "SizeStore CRC mismatch repaired via parity"
    )
    app = _FakeApp(_FakeRepo(lambda level: [repaired]))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert "1 self-repaired via parity" in str(status.render())
        assert "finding(s)" not in str(status.render())


async def test_pressing_f_runs_a_full_check_and_replaces_the_findings(wait_until: Any, sdk_timeout: float) -> None:
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return [_QUICK_FINDING] if level is VerifyLevel.QUICK else [_FULL_FINDING, _FULL_FINDING]

    app = _FakeApp(_FakeRepo(verify))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        # One group (a single QUICK finding): a header row plus one instance row.
        await wait_until(pilot, lambda: table.row_count == 2, timeout=sdk_timeout, interval=0.02)

        await pilot.press("f")
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "level=full" in str(status.render()), timeout=sdk_timeout, interval=0.02)

        assert calls == [VerifyLevel.QUICK, VerifyLevel.FULL]
        # Both FULL findings are identical, so they land in one group: one
        # header row plus two instance rows.
        assert table.row_count == 3
        assert "2 finding(s)" in str(status.render())


async def test_verify_error_shows_in_the_status_line(wait_until: Any, sdk_timeout: float) -> None:
    app = _FakeApp(_FakeRepo(lambda level: ApmRepoError("repo is locked")))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert "repo is locked" in str(status.render())


async def test_action_go_back_pops_the_screen() -> None:
    app = _FakeApp(_FakeRepo(lambda level: []))
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, DiagnosticsScreen)
        screen.action_go_back()
        await pilot.pause()
        assert app.screen is not screen


def test_status_base_picks_the_quick_or_full_running_text_by_level() -> None:
    """The debounced sink's own base text for ``_run``: QUICK's is just
    its fixed status label, while FULL's carries an extra warning for a
    check long enough to still be running once the debounce window
    passes."""
    assert _status_base(VerifyLevel.QUICK) == DIAGNOSTICS_QUICK_STATUS
    assert _status_base(VerifyLevel.FULL) == DIAGNOSTICS_FULL_RUNNING_STATUS


def test_verify_status_text_shows_phase_and_progress_once_a_tick_arrives() -> None:
    """Pure logic, no mount/worker needed -- ``_verify_status_text`` is
    what the debounced spinner's own ``base()`` re-reads every tick."""
    screen = DiagnosticsScreen()
    assert screen._verify_status_text(VerifyLevel.FULL) == DIAGNOSTICS_FULL_RUNNING_STATUS
    screen._verify_progress = Progress(phase="verifying", determinate=True, done=3, total=10, unit="buckets")
    assert screen._verify_status_text(VerifyLevel.FULL) == f"{DIAGNOSTICS_FULL_RUNNING_STATUS} — verifying (3/10)"
    # QUICK never reads it, even if somehow set (defensive -- QUICK's own
    # _run branch never touches self._verify_progress at all).
    assert screen._verify_status_text(VerifyLevel.QUICK) == DIAGNOSTICS_QUICK_STATUS


async def test_action_run_full_refuses_while_an_export_is_running(wait_until: Any, sdk_timeout: float) -> None:
    """The mutual-exclusion guard -- verify FULL and export share the one
    process-pool slot -- refuses to start rather than racing an export's
    own pool, and never flips ``verify_full_running`` on in that case."""
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return []

    running_job = Job(id=JobId(1), label="export a.bin", group="job-1")
    app = _FakeApp(_FakeRepo(verify), model=AppModel(jobs={JobId(1): running_job}))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert calls == [VerifyLevel.QUICK]  # QUICK is unaffected by the guard

        await pilot.press("f")
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert DIAGNOSTICS_EXPORT_BUSY_WARNING in str(status.render())
        assert calls == [VerifyLevel.QUICK]  # FULL never actually ran
        assert app.store.model.verify_full_running is False


async def test_action_run_full_refuses_while_another_full_check_is_already_running(
    wait_until: Any, sdk_timeout: float
) -> None:
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return []

    app = _FakeApp(_FakeRepo(verify), model=AppModel(verify_full_running=True))
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("f")
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert DIAGNOSTICS_FULL_ALREADY_RUNNING_WARNING in str(status.render())
        assert VerifyLevel.FULL not in calls


async def test_verify_full_running_flag_is_cleared_after_a_full_check_finishes(
    wait_until: Any, sdk_timeout: float
) -> None:
    """``VerifyFullFinished`` always fires from ``_run``'s own
    ``finally``, so the shared flag never gets stuck ``True`` -- checked
    here via the real round-trip through ``app.store``, not just the
    pure ``update()`` branch (already covered in
    ``test_browser_core_app_update.py``)."""
    app = _FakeApp(_FakeRepo(lambda level: [_FULL_FINDING] if level is VerifyLevel.FULL else []))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 0, timeout=sdk_timeout, interval=0.02)

        await pilot.press("f")
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "level=full" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert app.store.model.verify_full_running is False


async def test_action_run_full_refuses_while_quick_is_still_running(wait_until: Any, sdk_timeout: float) -> None:
    """``_run``'s own ``@work`` has no ``group``/``exclusive`` -- without
    ``_verify_running``, pressing ``f`` while the QUICK check from
    ``on_mount`` hasn't finished yet would start a second, genuinely
    concurrent worker racing the first one's writes into
    ``#diag-status``/``#diag-table``. Set directly here (``_FakeRepo``
    has no artificial delay, so QUICK may already be done by the time a
    real race could be timed) to exercise the guard itself, not the
    timing."""
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return []

    app = _FakeApp(_FakeRepo(verify))
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, DiagnosticsScreen)
        screen._verify_running = True
        screen.action_run_full()
        status = screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert DIAGNOSTICS_QUICK_STILL_RUNNING_WARNING in str(status.render())
        assert VerifyLevel.FULL not in calls


__all__: list[str] = []
