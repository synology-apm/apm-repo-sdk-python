"""``ExportScreen``: the modal that exports one item to a file, or a
folder's items into a directory -- destination path, progress and cancel,
with ``b`` sending the export to the background. Every export uses
``app.default_sparse`` (``--no-sparse-export``).

``_start`` dispatches ``StartExport`` into ``app.store``, which mints the
job (``QUEUED`` while another export or a verify FULL runs);
``runtime/app_effects.py`` runs it on the App, so ``b`` or leaving the
screen doesn't stop it. The screen renders its job's state
(``_my_job``/``_render_job``) and dispatches ``CancelJobRequested``.
"""

from __future__ import annotations

from typing import ClassVar, override

from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, ProgressBar, Static

from synology_apm_repo.browser.core.app.model import AppModel, ExportTarget, FinishedJob, FolderExport, Job, JobStatus
from synology_apm_repo.browser.core.app.msg import CancelJobRequested, StartExport
from synology_apm_repo.browser.core.app.select import ExportButtonSpec, export_button_spec
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.keymap import COMMON_BINDINGS
from synology_apm_repo.browser.runtime.store import Subscription
from synology_apm_repo.browser.screens._shared import AppStateMixin, DelegatesCommonActions, modal_box_css
from synology_apm_repo.browser.strings import (
    EXPORT_BACKGROUNDED_TITLE,
    EXPORT_DST_LABEL,
    EXPORT_DST_PLACEHOLDER,
    EXPORT_FOLDER_DST_LABEL,
    EXPORT_NOTHING_RUNNING_WARNING,
    EXPORT_QUEUED_MESSAGE,
    EXPORT_RUNNING_STATUS_TEXT,
    EXPORT_START_LABEL,
    EXPORT_STATUS_BAR,
)
from synology_apm_repo.sdk.export import safe_file_name
from synology_apm_repo.sdk.presentation import format_bytes, safe


class ExportScreen(AppStateMixin, DelegatesCommonActions, ModalScreen[None]):
    """``DelegatesCommonActions`` routes ``COMMON_BINDINGS``' ``q``/``d``/``?``
    to the App, which this modal's truncated binding chain wouldn't reach."""

    DEFAULT_CSS = (
        modal_box_css("ExportScreen", width=76, guard_child_horizontal=True)
        + """
    /* Export/Cancel button, right-aligned via a single-child Horizontal. */
    #export-actions {
        align: right middle;
    }

    /* Full width: ProgressBar's default bar is fixed at 32 cells. */
    #export-progress {
        width: 100%;
        /* Hidden until a job runs: with no total, ProgressBar animates
        an indeterminate bar. */
        display: none;
    }

    #export-progress.active {
        display: block;
    }

    #export-progress #bar {
        width: 1fr;
    }
    """
    )

    BINDINGS: ClassVar[list[BindingType]] = [
        *COMMON_BINDINGS,
        Binding("escape", "cancel_or_back", "Back/Cancel", show=False),
        Binding("b", "background", "Background"),
    ]

    def __init__(self, target: ExportTarget) -> None:
        super().__init__()
        self._target = target
        self._job_id: JobId | None = None
        self._subscription: Subscription | None = None
        #: The status _render_job last wrote to #export-status, so a progress
        #: tick skips the write (Static.update() always repaints).
        self._last_rendered_status: JobStatus | None = None

    @override
    def compose(self) -> ComposeResult:
        with Vertical():
            target = self._target
            if isinstance(target, FolderExport):
                yield Static(f"Export folder: [b]{safe(target.name)}[/b]")
            else:
                yield Static(
                    f"Export: [b]{safe(target.name)}[/b]" + (f" ({format_bytes(target.size)})" if target.size else "")
                )
            yield Static(EXPORT_FOLDER_DST_LABEL if isinstance(target, FolderExport) else EXPORT_DST_LABEL)
            yield Input(
                placeholder=EXPORT_DST_PLACEHOLDER,
                id="export-dst",
                value=f"./{safe_file_name(self._target.name)}",
            )
            with Horizontal(id="export-actions"):
                yield Button(EXPORT_START_LABEL, id="export-start", variant="primary")
            # show_eta=False: the ETA is ProgressMeter's (Job.eta_text), as in
            # the CLI.
            yield ProgressBar(id="export-progress", show_eta=False)
            yield Static("", id="export-rate")
            yield Static("", id="export-status")
            yield Static(EXPORT_STATUS_BAR, id="status-bar")

    def on_mount(self) -> None:
        app = self.app_state
        self._subscription = app.store.subscribe(self._my_job, self._render_job, init=False)

    def on_unmount(self) -> None:
        if self._subscription is not None:
            self._subscription.unsubscribe()

    def _my_job(self, model: AppModel) -> Job | FinishedJob | None:
        """This screen's job, live (``AppModel.jobs``) or finished
        (``AppModel.recent``); ``None`` when none is in progress."""
        if self._job_id is None:
            return None
        job = model.jobs.get(self._job_id)
        if job is not None:
            return job
        return next((finished for finished in model.recent if finished.id == self._job_id), None)

    def _render_job(self, job: Job | FinishedJob | None) -> None:
        """Renders the button, progress bar and status line from the job."""
        if isinstance(job, Job):
            # For QUEUED too, clearing a previous export's rate text.
            self._render_progress(job)
            # CANCELLING's text is written by _cancel_job.
            if job.status is not self._last_rendered_status:
                if job.status is JobStatus.QUEUED:
                    self.query_one("#export-status", Static).update(EXPORT_QUEUED_MESSAGE)
                elif job.status is JobStatus.RUNNING:
                    self.query_one("#export-status", Static).update(EXPORT_RUNNING_STATUS_TEXT)
                self._last_rendered_status = job.status
        elif isinstance(job, FinishedJob):
            self.query_one("#export-status", Static).update(job.outcome.status_text)
            self._job_id = None  # allow re-exporting from this same screen instance
            self._last_rendered_status = None
        self._render_button(export_button_spec(job))

    def _render_button(self, spec: ExportButtonSpec) -> None:
        self.query_one("#export-progress", ProgressBar).set_class(spec.progress_active, "active")
        button = self.query_one("#export-start", Button)
        button.label = spec.label
        # A plain str from core/, always a valid ButtonVariant.
        button.variant = spec.variant

    def on_button_pressed(self, event: Button.Pressed) -> None:
        # Export while idle, Cancel while a job is in progress.
        if event.button.id == "export-start":
            if self._job_id is None:
                self._start()
            else:
                self._cancel_job()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        # Enter in the destination field confirms, same as clicking the button.
        if event.input.id == "export-dst":
            self._start()

    def _start(self) -> None:
        if self._job_id is not None:
            return
        app = self.app_state
        dst_text = self.query_one("#export-dst", Input).value
        # update() validates dst_text; a new job id after the dispatch means
        # it was accepted.
        jobs_before = set(app.store.model.jobs)
        app.store.dispatch(StartExport(target=self._target, dst_text=dst_text, sparse=app.default_sparse))
        new_job_ids = set(app.store.model.jobs) - jobs_before
        if not new_job_ids:
            return
        (job_id,) = new_job_ids
        job = app.store.model.jobs[job_id]
        self._job_id = job_id
        # Unfocus the Input, or it swallows single-letter bindings like b.
        self.set_focus(None)
        # The subscription rendered during dispatch(), before _job_id was set.
        self._render_job(job)

    def _render_progress(self, job: Job) -> None:
        # A total of 0 is complete, not unknown: pass it through as is.
        self.query_one("#export-progress", ProgressBar).update(total=job.total, progress=job.done)
        parts = [
            part
            for part in (
                safe(job.file_text),
                job.rate_text,
                f"ETA {job.eta_text}" if job.eta_text else "",
                f"elapsed {job.elapsed_text}" if job.elapsed_text else "",
            )
            if part
        ]
        self.query_one("#export-rate", Static).update(" · ".join(parts))

    def action_background(self) -> None:
        if self._job_id is None:
            self.notify(EXPORT_NOTHING_RUNNING_WARNING, severity="warning")
            return
        # A QUEUED job hasn't started; only the wording differs.
        app = self.app_state
        job = app.store.model.jobs.get(self._job_id)
        if job is not None and job.status is JobStatus.QUEUED:
            self.notify(f"{self._target.name}: {EXPORT_QUEUED_MESSAGE}", title=EXPORT_BACKGROUNDED_TITLE)
        else:
            self.notify(f"{self._target.name}: continuing export in the background", title=EXPORT_BACKGROUNDED_TITLE)
        self.app.pop_screen()

    def action_cancel_or_back(self) -> None:
        if self._job_id is not None:
            self._cancel_job()
        else:
            self.app.pop_screen()

    def _cancel_job(self) -> None:
        # For the button and Esc.
        assert self._job_id is not None
        self.app_state.store.dispatch(CancelJobRequested(job_id=self._job_id))
        # A QUEUED job was removed during dispatch(), and _render_job already
        # reset _job_id and wrote its status.
        if self._job_id is not None:
            self.query_one("#export-status", Static).update("cancelling...")
