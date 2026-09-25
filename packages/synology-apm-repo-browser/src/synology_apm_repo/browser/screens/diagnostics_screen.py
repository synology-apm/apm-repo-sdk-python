"""``DiagnosticsScreen``: ``Repository.verify``'s ``Finding`` list —
diagnostic-mode content, reachable from anywhere via ``v``.

FULL stays screen-local (hosted on ``self``, cancelled the instant this
screen unmounts) rather than joining ``AppModel.jobs`` the way export
does — a FULL check's own usage pattern is "start it, wait, see the
result" (the same blocking-foreground shape ``synology-apm-repo-cli
verify --level full`` already has), not something a user needs to keep
running while browsing elsewhere. It still needs to never overlap an
export's own ``ProcessPoolExecutor``, though: ``action_run_full`` refuses
to start while one is running (``AppModel.export_occupied``), and sets
``AppModel.verify_full_running`` for its own duration so a `StartExport`
in the other direction queues instead of racing it — see
``core/app/update.py``'s ``VerifyFullStarted``/``VerifyFullFinished``
handling. QUICK never touches any of this: it doesn't use a process pool,
so it's re-run fresh on every mount exactly as before.
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
        #: ``ProgressMeter`` — only ever set while ``level is
        #: VerifyLevel.FULL``; QUICK never touches it. Read by
        #: ``_verify_status_text``, the debounced spinner's own ``base()``.
        self._verify_progress: Progress | None = None
        #: True for the whole duration of either level's own ``_run``
        #: worker on this screen instance -- QUICK included. ``_run``'s
        #: own ``@work`` has no ``group``/``exclusive`` of its own, so
        #: without this, pressing ``f`` while the QUICK check from
        #: ``on_mount`` is still running would start a second, genuinely
        #: concurrent worker racing the first one's writes into
        #: ``#diag-status``/``#diag-table``.
        self._verify_running = False
        #: The most recent ``verify()`` result and the level it ran at —
        #: kept so ``refresh_for_verbose_mode`` can re-render the same
        #: findings (showing/hiding each instance's own ``ref``) without
        #: re-running the check itself.
        self._last_findings: list[Finding] | None = None
        self._last_level: VerifyLevel | None = None

    def compose(self) -> ComposeResult:
        yield Static(DIAGNOSTICS_QUICK_STATUS, id="diag-status")
        yield DataTable(id="diag-table")
        yield Footer(show_command_palette=False)

    def on_mount(self) -> None:
        table = self.query_one("#diag-table", DataTable)
        table.add_columns(*DIAGNOSTICS_COLUMNS)
        # init=False: this screen's on_mount already runs a fresh QUICK
        # check unconditionally below, so the watch only needs to fire on
        # a genuine later toggle, not the initial subscribe -- same
        # reasoning as UnitScreen's/BrowseScreen's own identical call.
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
        # Checked before the cross-screen model checks below, not
        # combined with them: this one guards against QUICK (from
        # on_mount) or an earlier FULL still running on *this* screen
        # instance, a narrower and separate hazard from the
        # cross-screen/cross-kind ones the model-level checks cover.
        if self._verify_running:
            show_error(self, "#diag-status", DIAGNOSTICS_QUICK_STILL_RUNNING_WARNING)
            return
        model = self.app_state.store.model
        # Checked separately, not one combined `or`, so the message
        # actually names what's blocking -- "an export is running" would
        # be flatly wrong when the real blocker is another already-
        # running FULL check (this screen or another DiagnosticsScreen
        # instance), and vice versa.
        if model.verify_full_running:
            show_error(self, "#diag-status", DIAGNOSTICS_FULL_ALREADY_RUNNING_WARNING)
            return
        if model.export_occupied:
            show_error(self, "#diag-status", DIAGNOSTICS_EXPORT_BUSY_WARNING)
            return
        # Set/dispatched here, synchronously, immediately after every
        # check above passes -- not from inside _run's own worker body.
        # _run is a @work-decorated worker: calling it only *schedules*
        # the coroutine, so the actual repo.verify() call (and any
        # dispatch inside it) doesn't run until a later event-loop turn.
        # Setting either flag there instead would leave a window where
        # two action_run_full calls in quick succession -- a fast
        # double-`f`, or two different DiagnosticsScreen instances --
        # could both pass the guards above before either one's flag-set
        # actually lands, exactly the race these flags exist to prevent.
        # Checking and flag-setting must happen in the same synchronous
        # stretch of code, with no ``await`` in between.
        self._verify_running = True
        self.app_state.store.dispatch(VerifyFullStarted())
        self._run(VerifyLevel.FULL)

    # The sink's base text depends on level: DIAGNOSTICS_FULL_RUNNING_STATUS's
    # own "this may take a while" warning is what a still-running full
    # check shows past the 300ms debounce, rather than a separate eager,
    # undebounced write of its own outside this worker.
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
            # Always fires -- success, cancelled (Esc pops this screen),
            # or errored -- so neither flag ever gets stuck True:
            # _verify_running (for either level, so a next QUICK-on-
            # remount or FULL-on-f can actually run) and, FULL only,
            # AppModel.verify_full_running (so a queued export can
            # always eventually start).
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
        ``verify`` command does (via ``group_findings``/``sort_key`` —
        ``findings`` is expected already sorted, see ``_run``) instead of
        one flat, ungrouped row per ``Finding``: a header row per group
        (stage/symptom populated, path blank, detail = template + count)
        followed by one indented instance row per member (path/
        variable_parts, ``ref`` appended only in verbose mode) —
        the ``DataTable``-shaped equivalent of that command's own
        ``"[symptom] stage: template (count)"`` header plus per-instance
        lines. Every content-derived value (path, template, variable_parts,
        ref) is escaped via ``safe()`` before reaching a cell, matching
        every other cell-producing site in this package; stage/symptom are
        fixed vocabulary, left unescaped, the same split the CLI's own
        ``_render_human`` documents.
        """
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
            # Not a problem left unresolved -- every finding here already
            # passed self-repair (Symptom.REPAIRED_VIA_PARITY), just still
            # shown as "clean" rather than hidden.
            status.update(
                f"[green]clean[/green] ({len(findings)} self-repaired via parity) in {group_count} "
                f"at level={level.value}"
            )
        else:
            status.update(f"[green]clean[/green] at level={level.value} — press f for a full check")
