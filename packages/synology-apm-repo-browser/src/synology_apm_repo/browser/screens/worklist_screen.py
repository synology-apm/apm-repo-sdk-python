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
    """A centered dialog box (same treatment as ``KeyDialog``/
    ``ExportScreen``), not a full-viewport screen — ``ModalScreen``
    truncates the App-level binding chain at itself, so only the bindings
    declared here are reachable while this dialog is open. Keeps
    ``COMMON_BINDINGS`` (``q``/``d``/``?``), matching a working screen
    with ongoing state shown as a dialog, not a one-shot form like
    ``KeyDialog``/``ConnectDialog`` (which skip it) — but including the
    ``Binding`` alone isn't enough on a modal: Textual's own action
    dispatch runs the method on whichever node's own ``BINDINGS`` the key
    was actually found on, never bubbling further once the chain is
    truncated here — without ``action_quit_app``/``action_toggle_verbose``/
    ``action_show_help`` below, each delegating explicitly to the App's
    own real implementation, ``q``/``d``/``?`` would be silently
    swallowed while this dialog is open despite the ``Binding`` being
    present. ``j``/``k`` are re-declared directly (forwarding to the
    ``DataTable``'s own cursor actions via
    ``_shared.forward_to_focused``, shared with ``NavigableScreen``'s own
    identical need) since this class doesn't inherit ``NavigableScreen``
    — plain arrow keys keep working regardless, via ``DataTable``'s own
    built-in bindings. None of ``BINDINGS``' own ``show`` flags matter
    visually here, unlike a real ``NavigableScreen`` -- this dialog has
    no ``Footer`` at all, matching every other modal in this package; the
    *actual* visible key hint is ``WORKLIST_HINT``, its own plain string in
    ``#status-bar``."""

    DEFAULT_CSS = (
        modal_box_css("WorklistScreen", width=100)
        + """
    /* Overrides modal_box_css's own fixed 100-cell width: a 7-column
    DataTable (Name/Size/Progress/Speed/ETA/Elapsed/Status) needs real
    room to breathe, especially Name for a long unit filename -- a
    percentage of the actual terminal width scales far better here than
    any single fixed cell count would. */
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
        # Armed in on_mount, once this screen actually has a real event
        # loop to arm a timer on -- Debouncer arms its timer via
        # screen.set_timer, which needs the App's event loop already
        # running.
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
        # Replaces a fixed 0.5s poll (plus the snapshot-diff it needed
        # purely to skip repainting when nothing had changed between
        # ticks): this only fires on a genuine jobs mutation
        # (App.mutate_reactive, from start/update/finish/cancel), the
        # same mechanism NavigableScreen's own breadcrumb tasks-hint
        # watch uses, so the diff is no longer needed -- every firing
        # already represents a real change. Still debounced rather than
        # calling _refresh directly: ProgressMeter's own default 0.1s
        # throttle can push jobs updates faster than the fixed-column
        # DataTable rebuild below is worth redoing, well past what the
        # old 0.5s poll ever allowed through.
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
        # No Footer on this screen (matching every other modal in this
        # package), so #status-bar is the only place a key hint can show
        # at all -- WORKLIST_HINT covers the non-empty case, the same
        # role ExportScreen's own permanent #status-bar ("b: continue in
        # background...") already plays there.
        self.query_one("#status-bar", Static).update(WORKLIST_EMPTY_STATUS if not jobs else WORKLIST_HINT)

    def action_dismiss_worklist(self) -> None:
        self.app.pop_screen()

    # -- COMMON_BINDINGS delegates -- see the class docstring for why
    # the Binding alone (inherited into this screen's own BINDINGS) isn't
    # enough on a modal; each of these must exist here too.
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
            # update()'s own CancelJobRequested handler applies synchronously
            # (before dispatch() returns), so this reflects the new
            # status/removal immediately rather than waiting for the
            # debounced watch above to pick it up.
            self._refresh()
