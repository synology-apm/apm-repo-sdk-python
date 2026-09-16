"""``DiagnosticsScreen``: ``Repository.verify``'s ``Finding`` list —
diagnostic-mode content, reachable from anywhere via ``v``.
"""

from __future__ import annotations

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.keymap import COMMON_BINDINGS, NAV_BINDINGS
from synology_apm_repo.browser.screens._shared import NavigableScreen, show_error
from synology_apm_repo.browser.strings import (
    DIAGNOSTICS_COLUMNS,
    DIAGNOSTICS_FULL_RUNNING_STATUS,
    DIAGNOSTICS_QUICK_STATUS,
)
from synology_apm_repo.sdk.api import Finding, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import ApmRepoError


class DiagnosticsScreen(NavigableScreen):
    BINDINGS = [*COMMON_BINDINGS, *NAV_BINDINGS, Binding("f", "run_full", "Full check")]

    def compose(self) -> ComposeResult:
        yield Static(DIAGNOSTICS_QUICK_STATUS, id="diag-status")
        yield DataTable(id="diag-table")

    def on_mount(self) -> None:
        table = self.query_one("#diag-table", DataTable)
        table.add_columns(*DIAGNOSTICS_COLUMNS)
        self._run(VerifyLevel.QUICK)

    def action_run_full(self) -> None:
        self.query_one("#diag-status", Static).update(DIAGNOSTICS_FULL_RUNNING_STATUS)
        self._run(VerifyLevel.FULL)

    # Async ``@work`` (never ``thread=True``) — see browser/README.md.
    @work
    async def _run(self, level: VerifyLevel) -> None:
        repo = self.app_state.repo
        assert repo is not None
        try:
            findings = await repo.verify(level)
        except ApmRepoError as exc:
            self._show_error(str(exc))
            return
        self._show_findings(findings, level)

    def _show_error(self, message: str) -> None:
        show_error(self, "#diag-status", message)

    def _show_findings(self, findings: list[Finding], level: VerifyLevel) -> None:
        table = self.query_one("#diag-table", DataTable)
        table.clear()
        for finding in findings:
            table.add_row(finding.stage, finding.symptom.value, finding.path, finding.detail)
        status = self.query_one("#diag-status", Static)
        problems = [f for f in findings if f.symptom is not Symptom.REPAIRED_VIA_PARITY]
        if problems:
            status.update(f"[red]{len(problems)} finding(s)[/red] at level={level.value}")
        elif findings:
            # Not a problem left unresolved -- see Symptom.REPAIRED_VIA_PARITY's
            # own docstring.
            status.update(f"[green]clean[/green] ({len(findings)} self-repaired via parity) at level={level.value}")
        else:
            status.update(f"[green]clean[/green] at level={level.value} — press f for a full check")
