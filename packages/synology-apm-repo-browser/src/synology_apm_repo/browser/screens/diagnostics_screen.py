"""``DiagnosticsScreen``: ``Repository.verify``'s findings, opened with
``v``. A QUICK check runs on every mount; ``f`` runs FULL.

FULL is screen-local (cancelled when the screen unmounts), not a ``Job``.
It must not overlap an export's process pool: ``action_run_full`` refuses
while an export runs (``AppModel.export_occupied``) and dispatches
``VerifyFullStarted``, so an export started meanwhile queues.
"""

from __future__ import annotations

from typing import ClassVar, override

from textual.app import ComposeResult
from textual.binding import Binding, BindingType
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
from synology_apm_repo.sdk import ApmRepoError, VerifyLevel
from synology_apm_repo.sdk.presentation import (
    Progress,
    ProgressMeter,
    VerifySummary,
    group_count_label,
    safe,
    summarize_findings,
)


def _status_base(level: VerifyLevel) -> str:
    return DIAGNOSTICS_QUICK_STATUS if level is VerifyLevel.QUICK else DIAGNOSTICS_FULL_RUNNING_STATUS


class DiagnosticsScreen(NavigableScreen):
    BINDINGS: ClassVar[list[BindingType]] = [*COMMON_BINDINGS, *NAV_BINDINGS, Binding("f", "run_full", "Full check")]

    def __init__(self) -> None:
        super().__init__()
        #: A running FULL check's latest progress, for ``_verify_status_text``.
        self._verify_progress: Progress | None = None
        #: True while either level's ``_run`` worker runs, so a second
        #: worker can't race writes into ``#diag-status``/``#diag-table``.
        self._verify_running = False
        #: The most recent ``verify()`` result and level, so
        #: ``refresh_for_verbose_mode`` can re-render without re-running.
        self._last_summary: VerifySummary | None = None
        self._last_level: VerifyLevel | None = None

    @override
    def compose(self) -> ComposeResult:
        yield Static(DIAGNOSTICS_QUICK_STATUS, id="diag-status")
        yield DataTable(id="diag-table")
        yield Footer(show_command_palette=False)

    @override
    def on_mount(self) -> None:
        table = self.query_one("#diag-table", DataTable)
        table.add_columns(*DIAGNOSTICS_COLUMNS)
        # init=False: nothing is rendered yet.
        self.watch(self.app, "verbose", self.refresh_for_verbose_mode, init=False)
        self._verify_running = True
        self._run(VerifyLevel.QUICK)

    def refresh_for_verbose_mode(self) -> None:
        """Watch callback for the app's ``verbose`` reactive: re-renders the
        last result so the ``ref`` suffix toggles without re-running."""
        if self._last_summary is not None and self._last_level is not None:
            self._show_findings(self._last_summary, self._last_level)

    def action_run_full(self) -> None:
        # Same-screen guard; the checks below are cross-screen.
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
        # Set before scheduling _run: setting it inside the worker would let
        # a fast double-`f` pass the guards above twice.
        self._verify_running = True
        self.app_state.store.dispatch(VerifyFullStarted())
        self._run(VerifyLevel.FULL)

    # The sink's base text differs by level.
    @work(sink=lambda self, level: StaticTextSink(self, "#diag-status", base=lambda: self._verify_status_text(level)))
    async def _run(self, level: VerifyLevel) -> None:
        repo = self.app_state.current_repo
        assert repo is not None
        if level is VerifyLevel.FULL:
            self._verify_progress = None
        try:
            if level is VerifyLevel.FULL:
                meter = ProgressMeter(callback=self._on_verify_progress)
                summary = summarize_findings(await repo.verify(level, progress=meter.update))
            else:
                summary = summarize_findings(await repo.verify(level))
        except ApmRepoError as exc:
            show_error(self, "#diag-status", str(exc))
            return
        finally:
            # Clear both flags on every exit path so later checks and queued
            # exports aren't blocked.
            self._verify_running = False
            if level is VerifyLevel.FULL:
                self.app_state.store.dispatch(VerifyFullFinished())
        self._last_summary = summary
        self._last_level = level
        self._show_findings(summary, level)

    async def _on_verify_progress(self, progress: Progress) -> None:
        self._verify_progress = progress

    def _verify_status_text(self, level: VerifyLevel) -> str:
        base = _status_base(level)
        if level is not VerifyLevel.FULL or self._verify_progress is None:
            return base
        p = self._verify_progress
        amount = f"{p.done}/{p.total}" if p.total is not None else str(p.done)
        return f"{base} — {p.phase} ({amount})"

    def _show_findings(self, summary: VerifySummary, level: VerifyLevel) -> None:
        """Renders ``summary`` as the CLI ``verify`` does: a header row per
        group (detail = template + count), then an indented row per instance
        (``ref`` only in verbose mode). Content-derived values are escaped;
        stage/symptom are fixed vocabulary."""
        table = self.query_one("#diag-table", DataTable)
        table.clear()
        for group in summary.groups:
            rep = group[0].finding
            table.add_row(rep.stage, rep.symptom.value, "", f"{safe(group[0].template)} ({group_count_label(group)})")
            for aug in group:
                detail = safe(aug.variable_parts)
                if self.app_state.verbose and aug.finding.ref is not None:
                    detail += f" ({safe(aug.finding.ref)})"
                table.add_row("", "", f"  {safe(aug.finding.path)}", detail)
        status = self.query_one("#diag-status", Static)
        headline = summary.headline(level.value)
        status.update(headline or f"[green]clean[/green] at level={level.value} — press f for a full check")
