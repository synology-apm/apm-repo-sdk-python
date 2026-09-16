"""``WorklistScreen``, the panel ``t`` opens when several jobs are running
at once. Lists every ``BackgroundJob`` a screen has backgrounded (currently
only ``ExportScreen``'s exports), refreshing live, with a per-job cancel.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.keymap import COMMON_BINDINGS, NAV_BINDINGS
from synology_apm_repo.browser.screens._shared import NavigableScreen
from synology_apm_repo.browser.strings import WORKLIST_COLUMNS, WORKLIST_EMPTY_STATUS, WORKLIST_STATUS_BAR

_REFRESH_INTERVAL = 0.5


class WorklistScreen(NavigableScreen):
    BINDINGS = [*COMMON_BINDINGS, *NAV_BINDINGS, Binding("x", "cancel_selected", "Cancel job")]

    def compose(self) -> ComposeResult:
        yield Static(WORKLIST_STATUS_BAR, id="status-bar")
        yield DataTable(id="worklist-table")

    def on_mount(self) -> None:
        table = self.query_one("#worklist-table", DataTable)
        table.add_columns(*WORKLIST_COLUMNS)
        table.cursor_type = "row"
        self._refresh()
        self.set_interval(_REFRESH_INTERVAL, self._refresh)

    def _refresh(self) -> None:
        table = self.query_one("#worklist-table", DataTable)
        jobs = list(self.app_state.jobs.values())
        table.clear()
        for job in jobs:
            if job.percent is not None:
                progress = f"{job.percent}%"
            elif job.done:
                progress = f"{job.done} done"
            else:
                progress = "-"
            table.add_row(job.label, progress, job.status, key=str(job.id))
        if not jobs:
            self.query_one("#status-bar", Static).update(WORKLIST_EMPTY_STATUS)

    def action_cancel_selected(self) -> None:
        table = self.query_one("#worklist-table", DataTable)
        if table.cursor_row is None or table.row_count == 0:
            return
        row_key, _column_key = table.coordinate_to_cell_key(table.cursor_coordinate)
        job_id = int(row_key.value) if row_key.value is not None else None
        if job_id is None:
            return
        job = self.app_state.jobs.get(job_id)
        if job is not None:
            job.cancel()
            self.app_state.mark_job_cancelling(job_id)
            self._refresh()
