"""``DiagnosticsScreen``: ``Repository.verify``'s ``Finding`` list —
diagnostic-mode content, reachable from anywhere via ``v``.

FULL stays screen-local (cancelled the instant this screen unmounts)
rather than joining ``AppModel.jobs`` the way export does, matching
``synology-apm-repo-cli verify --level full``'s own blocking-foreground
shape. It must never overlap an export's own ``ProcessPoolExecutor``:
``action_run_full`` refuses to start while one is running
(``AppModel.export_occupied``), and sets ``AppModel.verify_full_running``
for its duration so a `StartExport` queues instead of racing it — see
``core/app/update.py``'s ``VerifyFullStarted``/``VerifyFullFinished``
handling. QUICK doesn't use a process pool, so it re-runs fresh on every
mount.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer, Static

from synology_apm_repo.browser.core.app.msg import VerifyFullFinished, VerifyFullStarted
from synology_apm_repo.browser.keymap import COMMON_BINDINGS, NAV_BINDINGS
from synology_apm_repo.browser.screens._shared import NavigableScreen, show_error
from synology_apm_repo.browser.strings import (
    DIAGNOSTICS_COLUMNS,
    DIAGNOSTICS_EXPORT_BUSY_WARNING,
    DIAGNOSTICS_FULL_ALREADY_RUNNING_WARNING,
    DIAGNOSTICS_FULL_RUNNING_STATUS,
    DIAGNOSTICS_QUICK_STATUS,
    DIAGNOSTICS_QUICK_STILL_RUNNING_WARNING,
)
from synology_apm_repo.browser.widgets.progress_hint import StaticTextSink
from synology_apm_repo.browser.widgets.worker_progress import work
from synology_apm_repo.sdk.api import (
    Finding,
    Symptom,
    VerifyLevel,
    group_count_label,
    group_findings,
    sort_key,
)
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter


def _status_base(level: VerifyLevel) -> str:
    return DIAGNOSTICS_QUICK_STATUS if level is VerifyLevel.QUICK else DIAGNOSTICS_FULL_RUNNING_STATUS


class DiagnosticsScreen(NavigableScreen):
    BINDINGS = [*COMMON_BINDINGS, *NAV_BINDINGS, Binding("f", "run_full", "Full check")]

    def __init__(self) -> None:
        super().__init__()
        #: The most recent tick from a running FULL check's own
        #: ``ProgressMeter`` -- only set while ``level is VerifyLevel.FULL``.
        #: Read by ``_verify_status_text``.
        self._verify_progress: Progress | None = None
        #: True for the whole duration of either level's own ``_run``
        #: worker -- guards against a second concurrent worker (``_run``'s
        #: ``@work`` has no group/exclusive of its own) racing writes into
        #: ``#diag-status``/``#diag-table``.
        self._verify_running = False
        #: The most recent ``verify()`` result and level, so
        #: ``refresh_for_verbose_mode`` can re-render without re-running.
        self._last_findings: list[Finding] | None = None
        self._last_level: VerifyLevel | None = None

    def compose(self) -> ComposeResult:
        yield Static(DIAGNOSTICS_QUICK_STATUS, id="diag-status")
        yield DataTable(id="diag-table")
        yield Footer(show_command_palette=False)

    def on_mount(self) -> None:
        table = self.query_one("#diag-table", DataTable)
        table.add_columns(*DIAGNOSTICS_COLUMNS)
        # init=False: on_mount already runs a fresh QUICK check
        # unconditionally, so the watch only needs to fire on a later toggle.
        self.watch(self.app, "verbose", self.refresh_for_verbose_mode, init=False)
        self._verify_running = True
        self._run(VerifyLevel.QUICK)

    def refresh_for_verbose_mode(self) -> None:
        """Registered as a watch callback on the app's ``verbose``
        reactive (see ``on_mount``). Re-renders the last ``verify()``
        result so each instance row's own ``ref`` suffix appears/
        disappears immediately on ``d``, without re-running the check."""
        if self._last_findings is not None and self._last_level is not None:
            self._show_findings(self._last_findings, self._last_level)

    def action_run_full(self) -> None:
        # Guards against QUICK (from on_mount) or an earlier FULL still
        # running on *this* screen instance -- narrower than the
        # cross-screen checks below.
        if self._verify_running:
            show_error(self, "#diag-status", DIAGNOSTICS_QUICK_STILL_RUNNING_WARNING)
            return
        model = self.app_state.store.model
        # Checked separately so the error message names the actual blocker.
        if model.verify_full_running:
            show_error(self, "#diag-status", DIAGNOSTICS_FULL_ALREADY_RUNNING_WARNING)
            return
        if model.export_occupied:
            show_error(self, "#diag-status", DIAGNOSTICS_EXPORT_BUSY_WARNING)
            return
        # Set synchronously here, not inside _run's own worker body: _run
        # is @work-decorated, so calling it only schedules the coroutine
        # -- setting the flag inside would leave a window for a fast
        # double-`f` (or two screen instances) to both pass the guards
        # above first.
        self._verify_running = True
        self.app_state.store.dispatch(VerifyFullStarted())
        self._run(VerifyLevel.FULL)

    # The sink's base text differs by level -- FULL's own
    # DIAGNOSTICS_FULL_RUNNING_STATUS warning shows past the 300ms debounce.
    @work(sink=lambda self, level: StaticTextSink(self, "#diag-status", base=lambda: self._verify_status_text(level)))
    async def _run(self, level: VerifyLevel) -> None:
        repo = self.app_state.current_repo
        assert repo is not None
        if level is VerifyLevel.FULL:
            self._verify_progress = None
        try:
            if level is VerifyLevel.FULL:
                meter = ProgressMeter(callback=self._on_verify_progress)
                findings = sorted(await repo.verify(level, progress=meter.update), key=sort_key)
            else:
                findings = sorted(await repo.verify(level), key=sort_key)
        except ApmRepoError as exc:
            show_error(self, "#diag-status", str(exc))
            return
        finally:
            # Always fires, so neither flag gets stuck True: _verify_running
            # (so a next QUICK/FULL can run) and, FULL only,
            # AppModel.verify_full_running (so a queued export can start).
            self._verify_running = False
            if level is VerifyLevel.FULL:
                self.app_state.store.dispatch(VerifyFullFinished())
        self._last_findings = findings
        self._last_level = level
        self._show_findings(findings, level)

    async def _on_verify_progress(self, progress: Progress) -> None:
        self._verify_progress = progress

    def _verify_status_text(self, level: VerifyLevel) -> str:
        base = _status_base(level)
        if level is not VerifyLevel.FULL or self._verify_progress is None:
            return base
        p = self._verify_progress
        amount = f"{p.done}/{p.total}" if p.total is not None else str(p.done)
        return f"{base} — {p.phase} ({amount})"

    def _show_findings(self, findings: list[Finding], level: VerifyLevel) -> None:
        """Renders ``findings`` grouped/sorted the same way the CLI's own
        ``verify`` command does (``group_findings``/``sort_key`` —
        ``findings`` expected already sorted): a header row per group
        (detail = template + count) followed by an indented row per
        instance (``ref`` appended only in verbose mode). Every
        content-derived value is escaped via ``safe()`` before reaching a
        cell; stage/symptom are fixed vocabulary, left unescaped."""
        table = self.query_one("#diag-table", DataTable)
        table.clear()
        groups = group_findings(findings)
        for group in groups:
            rep = group[0].finding
            table.add_row(rep.stage, rep.symptom.value, "", f"{safe(group[0].template)} ({group_count_label(group)})")
            for aug in group:
                detail = safe(aug.variable_parts)
                if self.app_state.verbose and aug.finding.ref is not None:
                    detail += f" ({safe(aug.finding.ref)})"
                table.add_row("", "", f"  {safe(aug.finding.path)}", detail)
        status = self.query_one("#diag-status", Static)
        problems = [f for f in findings if f.symptom is not Symptom.REPAIRED_VIA_PARITY]
        group_count = f"{len(groups)} {pluralize(len(groups), 'group')}"
        if problems:
            status.update(f"[red]{len(problems)} finding(s)[/red] in {group_count} at level={level.value}")
        elif findings:
            # Every finding here already passed self-repair
            # (Symptom.REPAIRED_VIA_PARITY), just still shown as "clean".
            status.update(
                f"[green]clean[/green] ({len(findings)} self-repaired via parity) in {group_count} "
                f"at level={level.value}"
            )
        else:
            status.update(f"[green]clean[/green] at level={level.value} — press f for a full check")
