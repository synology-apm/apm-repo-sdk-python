"""``WorklistScreen``: the modal ``t`` opens, listing every running or
queued export job live, with ``x`` to cancel one.
"""

from __future__ import annotations

from typing import ClassVar, override

from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.core.app.msg import CancelJobRequested
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.keymap import COMMON_BINDINGS
from synology_apm_repo.browser.screens._shared import (
    AppStateMixin,
    DelegatesCommonActions,
    forward_to_focused,
    modal_box_css,
)
from synology_apm_repo.browser.strings import WORKLIST_COLUMNS, WORKLIST_EMPTY_STATUS, WORKLIST_HINT
from synology_apm_repo.browser.widgets.filter_debounce import Debouncer


class WorklistScreen(AppStateMixin, DelegatesCommonActions, ModalScreen[None]):
    """``DelegatesCommonActions`` routes ``COMMON_BINDINGS``' ``q``/``d``/``?``
    to the App; ``j``/``k`` are forwarded to the ``DataTable`` here, since
    this isn't a ``NavigableScreen``. With no ``Footer``, ``WORKLIST_HINT``
    is the key hint."""

    DEFAULT_CSS = (
        modal_box_css("WorklistScreen", width=100)
        + """
    /* Overrides modal_box_css's fixed 100-cell width: the 7-column
    DataTable needs room that scales with terminal width. */
    WorklistScreen > Vertical {
        width: 92%;
        height: auto;
        max-height: 80%;
    }
    """
    )

    BINDINGS: ClassVar[list[BindingType]] = [
        *COMMON_BINDINGS,
        Binding("escape", "dismiss_worklist", "Close", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("x", "cancel_selected", "Cancel job"),
    ]

    def __init__(self) -> None:
        super().__init__()
        # Built in on_mount, once the screen can arm timers.
        self._refresh_debounce: Debouncer | None = None

    @override
    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static("", id="status-bar")
            yield DataTable(id="worklist-table")

    def on_mount(self) -> None:
        table = self.query_one("#worklist-table", DataTable)
        table.add_columns(*WORKLIST_COLUMNS)
        table.cursor_type = "row"
        self._refresh_debounce = Debouncer(self, self._refresh)
        self._refresh()  # first paint; the watch below is init=False
        # Debounced: progress ticks arrive faster than a rebuild is worth.
        self.watch(self.app, "jobs", self._refresh_debounce.trigger, init=False)

    def _refresh(self) -> None:
        app = self.app_state
        jobs = tuple(app.jobs.values())
        table = self.query_one("#worklist-table", DataTable)
        table.clear()
        for job in jobs:
            if job.percent is not None:
                progress = f"{job.percent}%"
            elif job.done:
                progress = f"{job.done} done"
            else:
                progress = "-"
            table.add_row(
                f"{job.label} ({job.position})" if job.position else job.label,
                job.size_text or "-",
                progress,
                job.rate_text or "-",
                job.eta_text or "-",
                job.elapsed_text or "-",
                job.status,
                key=str(job.id),
            )
        self.query_one("#status-bar", Static).update(WORKLIST_EMPTY_STATUS if not jobs else WORKLIST_HINT)

    def action_dismiss_worklist(self) -> None:
        self.app.pop_screen()

    def action_cursor_down(self) -> None:
        forward_to_focused(self, "action_cursor_down")

    def action_cursor_up(self) -> None:
        forward_to_focused(self, "action_cursor_up")

    def action_cancel_selected(self) -> None:
        table = self.query_one("#worklist-table", DataTable)
        if table.row_count == 0:
            return
        row_key, _column_key = table.coordinate_to_cell_key(table.cursor_coordinate)
        job_id = int(row_key.value) if row_key.value is not None else None
        if job_id is None:
            return
        app = self.app_state
        if JobId(job_id) in app.jobs:
            app.store.dispatch(CancelJobRequested(job_id=JobId(job_id)))
            # Now, rather than after the debounce.
            self._refresh()
