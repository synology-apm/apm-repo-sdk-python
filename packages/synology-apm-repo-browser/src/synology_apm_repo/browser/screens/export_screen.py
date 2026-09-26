"""``ExportScreen`` is a small, centered modal overlay (same treatment as
``KeyDialog``) for a destination path, progress bar, and cancel, with
``b`` sending the export to the background — always offered, no size
threshold. Output goes to ``<dst>.part`` first, renamed to ``<dst>`` only
on success: a truncated-but-plausible output file is a safety problem for
a restore tool, so a failure or cancel leaves the ``.part`` file instead.

No sparse toggle here: every export uses ``app.default_sparse`` (set once
at launch via ``synology-apm-repo-browser --no-sparse-export``), not a
per-export ``Checkbox``.

The export itself runs entirely above this screen: ``_start`` dispatches
``StartExport`` into ``app.store``, which mints the job (``QUEUED`` if
the one export/verify-FULL slot is taken, promoted to ``RUNNING`` once it
frees) and ``runtime/app_effects.py`` runs it on the *App*, never this
screen, so pressing ``b`` or navigating away never kills it. This screen
only subscribes to its own job's state (``_my_job``/``_render_job``) and
dispatches ``CancelJobRequested`` — it holds no live ``Worker`` reference;
the store is the shared place both this screen and ``WorklistScreen``
read/drive the same job through.
"""

from __future__ import annotations

import re
import sys
from typing import cast

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, ProgressBar, Static

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.app.model import AppModel, FinishedJob, Job, JobStatus
from synology_apm_repo.browser.core.app.msg import CancelJobRequested, StartExport
from synology_apm_repo.browser.core.app.select import ExportButtonSpec, export_button_spec
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.keymap import COMMON_BINDINGS
from synology_apm_repo.browser.runtime.store import Subscription
from synology_apm_repo.browser.screens._shared import delegate_common_action, modal_box_css
from synology_apm_repo.browser.strings import (
    EXPORT_BACKGROUNDED_TITLE,
    EXPORT_DST_LABEL,
    EXPORT_DST_PLACEHOLDER,
    EXPORT_NOTHING_RUNNING_WARNING,
    EXPORT_QUEUED_MESSAGE,
    EXPORT_RUNNING_STATUS_TEXT,
    EXPORT_START_LABEL,
    EXPORT_STATUS_BAR,
)
from synology_apm_repo.sdk.presentation.format import format_bytes
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import RestorableUnit

_WINDOWS_FORBIDDEN_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_WINDOWS_RESERVED_BASENAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
)


def _windows_safe_filename(name: str) -> str:
    """Sanitizes ``name`` for use as the export dialog's suggested
    destination filename. A no-op off Windows. Replaces each
    Windows-forbidden character (``< > : " / \\ | ? *`` plus ASCII
    control characters) with ``_``, strips a trailing ``.``/space, and
    guards against a reserved device basename (``CON``, ``COM1``, ...)
    regardless of any extension after it.
    """
    if sys.platform != "win32":
        return name
    sanitized = _WINDOWS_FORBIDDEN_CHARS.sub("_", name)
    sanitized = sanitized.rstrip(". ") or "_"
    if sanitized.split(".", 1)[0].upper() in _WINDOWS_RESERVED_BASENAMES:
        sanitized = f"_{sanitized}"
    return sanitized


class ExportScreen(ModalScreen[None]):
    """``ModalScreen`` truncates the App-level binding chain here, same
    as ``KeyDialog`` — ``COMMON_BINDINGS`` alone isn't enough to make
    ``q``/``d``/``?`` work, since Textual dispatches on whichever node's
    own ``BINDINGS`` the key was found on, never bubbling further. See
    ``action_quit_app``/``action_toggle_verbose``/``action_show_help``
    below, each an explicit delegate to the App's real implementation."""

    DEFAULT_CSS = (
        modal_box_css("ExportScreen", width=76, guard_child_horizontal=True)
        + """
    /* Export/Cancel button, right-aligned via a single-child Horizontal. */
    #export-actions {
        align: right middle;
    }

    /* Pulled to the destination Input's full width -- ProgressBar's
    default is width: auto (inner Bar sub-widget fixed at 32 cells). */
    #export-progress {
        width: 100%;
        /* Hidden until an export starts -- Textual's ProgressBar
        auto-animates an indeterminate bar whenever total is unset.
        _start() adds the active class once a job is accepted. */
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

    BINDINGS = [
        *COMMON_BINDINGS,
        Binding("escape", "cancel_or_back", "Back/Cancel", show=False),
        Binding("b", "background", "Background"),
    ]

    def __init__(self, unit: RestorableUnit) -> None:
        super().__init__()
        self._unit = unit
        self._job_id: JobId | None = None
        self._subscription: Subscription | None = None
        #: The status last written to #export-status by _render_job --
        #: lets a same-status re-render (every ExportProgressed tick
        #: while RUNNING, up to 10/s) skip the write, since
        #: Static.update() always repaints regardless of whether the
        #: text changed. Reset to None once a job leaves, so the next
        #: start always writes.
        self._last_rendered_status: JobStatus | None = None

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(
                f"Export: [b]{safe(self._unit.name)}[/b]"
                + (f" ({format_bytes(self._unit.size)})" if self._unit.size else "")
            )
            yield Static(EXPORT_DST_LABEL)
            yield Input(
                placeholder=EXPORT_DST_PLACEHOLDER,
                id="export-dst",
                value=f"./{_windows_safe_filename(self._unit.name)}",
            )
            with Horizontal(id="export-actions"):
                yield Button(EXPORT_START_LABEL, id="export-start", variant="primary")
            # show_eta=False: ETA comes from Job.rate_text/eta_text/
            # elapsed_text below (the same adapter export_to()'s CLI
            # counterpart uses), not ProgressBar's own built-in ETA math.
            yield ProgressBar(id="export-progress", show_eta=False)
            yield Static("", id="export-rate")
            yield Static("", id="export-status")
            yield Static(EXPORT_STATUS_BAR, id="status-bar")

    def on_mount(self) -> None:
        app = cast(ApmRepoBrowserApp, self.app)
        self._subscription = app.store.subscribe(self._my_job, self._render_job, init=False)

    def on_unmount(self) -> None:
        if self._subscription is not None:
            self._subscription.unsubscribe()

    def _my_job(self, model: AppModel) -> Job | FinishedJob | None:
        """This screen's own job, wherever it currently is -- still
        running (``AppModel.jobs``) or just finished (``AppModel.recent``,
        kept so a still-open screen can read its terminal ``outcome``).
        ``None`` before any export started, and again once one finishes."""
        if self._job_id is None:
            return None
        job = model.jobs.get(self._job_id)
        if job is not None:
            return job
        return next((finished for finished in model.recent if finished.id == self._job_id), None)

    def _render_job(self, job: Job | FinishedJob | None) -> None:
        """The single choke point every job-state change routes through,
        so the button/progress-bar/status-line never drift from the
        job's actual status."""
        if isinstance(job, Job):
            # Called for QUEUED too: its rate/eta/elapsed text is already
            # "", clearing #export-rate's leftover text from a previous
            # export.
            self._render_progress(job)
            # Written only on a QUEUED<->RUNNING transition, not every
            # render (this fires up to 10/s while RUNNING) -- caught by
            # comparing against _last_rendered_status. CANCELLING is left
            # alone: _cancel_job writes "cancelling..." itself, right
            # after this same render already ran.
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
        # None: nothing running and nothing finished to show yet -- the
        # dialog's own freshly-composed empty state already covers this.
        self._render_button(export_button_spec(job))

    def _render_button(self, spec: ExportButtonSpec) -> None:
        self.query_one("#export-progress", ProgressBar).set_class(spec.progress_active, "active")
        button = self.query_one("#export-start", Button)
        button.label = spec.label
        # ExportButtonSpec.variant is plain str (core/ stays Textual-free) --
        # a real ButtonVariant literal at every construction site above.
        button.variant = spec.variant

    def on_button_pressed(self, event: Button.Pressed) -> None:
        # Same button, two lives: [Export] while idle, [Cancel] while
        # running -- mirrors action_cancel_or_back's own branch
        # (_cancel_job is the one place that logic lives).
        if event.button.id == "export-start":
            if self._job_id is None:
                self._start()
            else:
                self._cancel_job()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        # Enter confirms, same as clicking the button — from either the
        # destination path.
        if event.input.id == "export-dst":
            self._start()

    def _start(self) -> None:
        if self._job_id is not None:
            return
        app = cast(ApmRepoBrowserApp, self.app)
        dst_text = self.query_one("#export-dst", Input).value
        # StartExport's own update() handler validates dst_text itself (a
        # Notify on empty -- see core/app/update.py); the new job id in
        # model.jobs after dispatch (at most one) tells "accepted" from
        # "rejected" without update() needing a reply channel back here.
        jobs_before = set(app.store.model.jobs)
        app.store.dispatch(StartExport(unit=self._unit, dst_text=dst_text, sparse=app.default_sparse))
        new_job_ids = set(app.store.model.jobs) - jobs_before
        if not new_job_ids:
            return
        (job_id,) = new_job_ids
        job = app.store.model.jobs[job_id]
        self._job_id = job_id
        # Move focus off the destination Input once exporting starts --
        # otherwise every single-letter binding (b included) gets
        # swallowed as typed text by the still-focused Input.
        self.set_focus(None)
        # The subscription from on_mount already re-rendered synchronously
        # inside dispatch() above, using self._job_id from before this
        # method set it -- one explicit catch-up render for this job's
        # just-minted state closes that gap.
        self._render_job(job)

    def _render_progress(self, job: Job) -> None:
        # job.total, not `job.total or None`: ProgressBar treats a known
        # total of 0 as 100% complete, not indeterminate -- `or None`
        # would collapse that into "unknown". Matches Job.percent's own
        # total==0 case.
        self.query_one("#export-progress", ProgressBar).update(total=job.total, progress=job.done)
        parts = [
            part
            for part in (
                job.rate_text,
                f"ETA {job.eta_text}" if job.eta_text else "",
                f"elapsed {job.elapsed_text}" if job.elapsed_text else "",
            )
            if part
        ]
        self.query_one("#export-rate", Static).update(" · ".join(parts))

    # -- COMMON_BINDINGS delegates -- see the class docstring for why
    # the Binding alone (inherited into this screen's own BINDINGS) isn't
    # enough on a modal; each of these must exist here too.
    def action_quit_app(self) -> None:
        delegate_common_action(self, "quit_app")

    def action_toggle_verbose(self) -> None:
        delegate_common_action(self, "toggle_verbose")

    def action_show_help(self) -> None:
        delegate_common_action(self, "show_help")

    def action_background(self) -> None:
        if self._job_id is None:
            self.notify(EXPORT_NOTHING_RUNNING_WARNING, severity="warning")
            return
        # A QUEUED job hasn't actually started, so "continuing in the
        # background" would be inaccurate -- the job lives in
        # AppModel.jobs either way, so popping is correct regardless,
        # only the wording differs.
        app = cast(ApmRepoBrowserApp, self.app)
        job = app.store.model.jobs.get(self._job_id)
        if job is not None and job.status is JobStatus.QUEUED:
            self.notify(f"{self._unit.name}: {EXPORT_QUEUED_MESSAGE}", title=EXPORT_BACKGROUNDED_TITLE)
        else:
            self.notify(f"{self._unit.name}: continuing export in the background", title=EXPORT_BACKGROUNDED_TITLE)
        self.app.pop_screen()

    def action_cancel_or_back(self) -> None:
        if self._job_id is not None:
            self._cancel_job()
        else:
            self.app.pop_screen()

    def _cancel_job(self) -> None:
        # Shared by the button and ``Esc`` — one place that requests
        # cancellation and sets the "cancelling..." status text, so the
        # two triggers can never drift apart.
        assert self._job_id is not None
        cast(ApmRepoBrowserApp, self.app).store.dispatch(CancelJobRequested(job_id=self._job_id))
        # dispatch() runs synchronously, including this screen's own
        # subscription callback -- a QUEUED job is removed immediately
        # (never CANCELLING) and _render_job already reset self._job_id
        # and wrote the real status text. Only a still-RUNNING/CANCELLING
        # job gets the generic text here.
        if self._job_id is not None:
            self.query_one("#export-status", Static).update("cancelling...")
