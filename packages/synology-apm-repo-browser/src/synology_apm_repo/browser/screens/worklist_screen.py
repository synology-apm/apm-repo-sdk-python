"""``WorklistScreen``, the modal dialog ``t`` opens (from any non-modal
screen) when several jobs are running at once. Lists every ``Job`` a
screen has backgrounded (currently only ``ExportScreen``'s exports),
refreshing live, with a per-job cancel — dispatched into ``app.store`` the
same way ``ExportScreen``'s own Esc/Cancel does, so the two can never
drift into cancelling a job differently depending on which screen
requested it.
"""

from __future__ import annotations

from typing import cast

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.app.msg import CancelJobRequested
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.keymap import COMMON_BINDINGS
from synology_apm_repo.browser.screens._shared import delegate_common_action, forward_to_focused, modal_box_css
from synology_apm_repo.browser.strings import WORKLIST_COLUMNS, WORKLIST_EMPTY_STATUS, WORKLIST_HINT
from synology_apm_repo.browser.widgets.filter_debounce import Debouncer


class WorklistScreen(ModalScreen[None]):
    """A centered dialog box, not a full-viewport screen -- ``ModalScreen``
    truncates the App-level binding chain here, so ``q``/``d``/``?``
    need explicit ``action_quit_app``/``action_toggle_verbose``/
    ``action_show_help`` delegates below, or Textual would silently
    swallow them despite ``COMMON_BINDINGS`` being inherited. ``j``/``k``
    are re-declared directly (forwarding to the ``DataTable``'s cursor
    actions) since this class doesn't inherit ``NavigableScreen``. No
    ``Footer``, matching every other modal here; ``WORKLIST_HINT`` in
    ``#status-bar`` is the visible key hint instead."""

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

    BINDINGS = [
        *COMMON_BINDINGS,
        Binding("escape", "dismiss_worklist", "Close", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("x", "cancel_selected", "Cancel job"),
    ]

    def __init__(self) -> None:
        super().__init__()
        # Armed in on_mount, once this screen has a real event loop --
        # Debouncer arms its timer via screen.set_timer.
        self._refresh_debounce: Debouncer | None = None

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static("", id="status-bar")
            yield DataTable(id="worklist-table")

    def on_mount(self) -> None:
        table = self.query_one("#worklist-table", DataTable)
        table.add_columns(*WORKLIST_COLUMNS)
        table.cursor_type = "row"
        self._refresh_debounce = Debouncer(self, self._refresh)
        self._refresh()  # immediate first paint -- see the watch below's own init=False
        # Fires only on a genuine jobs mutation (App.mutate_reactive), so
        # every firing is a real change. Still debounced since
        # ProgressMeter's 0.1s throttle can push updates faster than the
        # DataTable rebuild is worth redoing.
        self.watch(self.app, "jobs", self._refresh_debounce.trigger, init=False)

    def _refresh(self) -> None:
        app = cast(ApmRepoBrowserApp, self.app)
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
                job.label,
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

    # -- COMMON_BINDINGS delegates -- see the class docstring.
    def action_quit_app(self) -> None:
        delegate_common_action(self, "quit_app")

    def action_toggle_verbose(self) -> None:
        delegate_common_action(self, "toggle_verbose")

    def action_show_help(self) -> None:
        delegate_common_action(self, "show_help")

    def action_cursor_down(self) -> None:
        forward_to_focused(self, "action_cursor_down")

    def action_cursor_up(self) -> None:
        forward_to_focused(self, "action_cursor_up")

    def action_cancel_selected(self) -> None:
        table = self.query_one("#worklist-table", DataTable)
        if table.cursor_row is None or table.row_count == 0:
            return
        row_key, _column_key = table.coordinate_to_cell_key(table.cursor_coordinate)
        job_id = int(row_key.value) if row_key.value is not None else None
        if job_id is None:
            return
        app = cast(ApmRepoBrowserApp, self.app)
        if JobId(job_id) in app.jobs:
            app.store.dispatch(CancelJobRequested(job_id=JobId(job_id)))
            # update() applies synchronously, so this reflects the new
            # status/removal immediately rather than waiting for the
            # debounced watch above.
            self._refresh()
