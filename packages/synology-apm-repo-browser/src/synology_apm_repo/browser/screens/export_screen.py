"""``ExportScreen`` is a small, centered modal overlay (same treatment as
``KeyDialog``) for a destination path, progress bar, and cancel, with ``b``
sending the export to the background -- always offered, no size
threshold -- so a long-running export never locks the user into one
screen. Output goes to ``<dst>.part`` first, renamed to ``<dst>`` only on
success: a truncated-but-plausible-looking output file is a
safety-level problem for a restore tool, not just a UX nicety, so a
failure or cancel leaves the ``.part`` file rather than one named
``<dst>`` that looks complete but isn't.

No sparse toggle here: every export uses ``app.default_sparse`` (set
once at launch via ``synology-apm-repo-browser --no-sparse-export``, see ``app.py``'s
``main()``), not a per-export ``Checkbox`` -- the rare exception to
"sparse is what you want" is a launch-time preference, not something
worth asking on every export.

The export itself runs entirely above this screen: ``_start`` dispatches
``StartExport`` into ``app.store``, which mints the job and returns a
``RunExport`` command that ``runtime/app_effects.py`` carries out via
``app.run_worker(..., group=<job's own group>)`` -- unless another export
or a running verify-FULL check already has the slot, in which case the
job starts life ``QUEUED`` and promotes to ``RUNNING`` once that slot
frees. Either way, the worker's node is the *App*, never this screen,
so pressing ``b`` (or navigating away) never kills it. This screen only
ever *subscribes* to its own job's
state (``_my_job``/``_render_job``) and dispatches ``CancelJobRequested``
to cancel it -- it holds no live ``Worker`` reference and posts no
``Message`` of its own; the store is the one shared place both this
screen and ``WorklistScreen`` read/drive the same job through.
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
    """Sanitizes ``name`` for use as the export dialog's *suggested*
    destination filename. A no-op on any platform but Windows -- this
    only affects a default the user can freely edit either way, so
    there's no reason to mangle an otherwise-valid POSIX name when not
    actually running on Windows. Replaces each Windows-forbidden
    character (``< > : " / \\ | ? *`` plus ASCII control characters)
    with ``_``, strips a trailing ``.``/space (also Windows-invalid),
    and guards against a reserved device basename (``CON``, ``COM1``,
    ...) recognized regardless of any extension after it.
    """
    if sys.platform != "win32":
        return name
    sanitized = _WINDOWS_FORBIDDEN_CHARS.sub("_", name)
    sanitized = sanitized.rstrip(". ") or "_"
    if sanitized.split(".", 1)[0].upper() in _WINDOWS_RESERVED_BASENAMES:
        sanitized = f"_{sanitized}"
    return sanitized


class ExportScreen(ModalScreen[None]):
    """``ModalScreen`` truncates the App-level binding chain at itself,
    same as ``KeyDialog`` — only the bindings
    declared here are reachable while this dialog is open. Keeping
    ``COMMON_BINDINGS`` in ``BINDINGS`` isn't enough on its own to make
    ``q``/``d``/``?`` work, though: Textual's own action dispatch runs
    the method on whichever node's own ``BINDINGS`` the key was actually
    found on, never bubbling further once the chain is truncated here —
    see ``action_quit_app``/``action_toggle_verbose``/``action_show_help``
    below, each an explicit delegate to the App's own real
    implementation (same fix/rationale as ``WorklistScreen``'s own)."""

    DEFAULT_CSS = (
        modal_box_css("ExportScreen", width=76, guard_child_horizontal=True)
        + """
    /* Export/Cancel button, right-aligned. A single-child Horizontal
    purely for its own ``align: right middle``; the button itself keeps its
    normal ``width: auto`` sizing (see Button's own DEFAULT_CSS) and sits
    right-aligned rather than flush left. */
    #export-actions {
        align: right middle;
    }

    /* Progress area pulled to the same full width as the destination
    Input — ProgressBar's own default is ``width: auto`` (its inner Bar
    sub-widget defaults to a fixed 32 cells), which renders much
    narrower than the Input above it. ``#bar`` targets that inner
    sub-widget by the id ProgressBar itself gives it. */
    #export-progress {
        width: 100%;
        /* Hidden until an export actually starts — Textual's own
        ProgressBar animates an indeterminate scanning bar automatically
        whenever ``total`` is unset (i.e. before ``.update()`` is ever
        called), so an unhidden bar in a freshly-opened dialog would spin
        for work that hasn't started and read as "export already
        running". ``_start()`` adds the ``active`` class the moment a job is
        actually accepted. */
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
        #: The status last written to #export-status by _render_job's
        #: own QUEUED/RUNNING branch, so a same-status re-render (every
        #: ExportProgressed tick while RUNNING, up to 10/s) can skip the
        #: write -- Static.update() unconditionally calls
        #: self.refresh(layout=True) regardless of whether the text
        #: actually changed, and a real repaint is this app's own
        #: documented dominant cost (see widgets/progress_hint.py's
        #: _FRAME_INTERVAL comment). Reset to None once a job leaves
        #: (FinishedJob), so the next fresh start always writes at least
        #: once regardless of what the previous job's last status was.
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
            # show_eta=False: ETA is rendered from Job.rate_text/eta_text/
            # elapsed_text below (the same rate/ETA adapter export_to()'s
            # CLI counterpart uses, computed in runtime/app_effects.py),
            # not ProgressBar's own built-in ETA math.
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
        kept around precisely so a still-open screen like this one can
        read its own job's terminal ``outcome`` after the fact). ``None``
        before any export has been started yet, and again once one
        finishes and ``_render_job`` resets ``self._job_id``."""
        if self._job_id is None:
            return None
        job = model.jobs.get(self._job_id)
        if job is not None:
            return job
        return next((finished for finished in model.recent if finished.id == self._job_id), None)

    def _render_job(self, job: Job | FinishedJob | None) -> None:
        """The single choke point every path -- a freshly-started job, a
        queued one getting promoted, an ongoing one's own progress tick,
        or one that just finished (cancelled, errored, or successful) --
        routes through, so the button/progress-bar/status-line state
        never drifts from what the job's own current status actually is."""
        if isinstance(job, Job):
            # Called for QUEUED too, not just RUNNING/CANCELLING: a
            # QUEUED job's own rate_text/eta_text/elapsed_text are all
            # still "" already, which is exactly what clears
            # #export-rate's leftover text from a previous export that
            # finished on this same screen instance -- this is the only
            # place that ever writes to it.
            self._render_progress(job)
            # Only written on an actual QUEUED<->RUNNING transition, not
            # on every render -- this fires on every ExportProgressed
            # tick while RUNNING (up to 10/s), and Static.update() always
            # repaints regardless of whether the text changed, so writing
            # the same "exporting..." on every one of those ticks would
            # be pure repaint churn. A QUEUED job promoted to RUNNING
            # (this screen never calls _start() again for that
            # transition, only the store's own subscription re-firing
            # this method) still needs exactly one write to flip the text
            # over from "queued..." to "exporting...", which comparing
            # against ``_last_rendered_status`` catches. CANCELLING is
            # left alone here on purpose either way: _cancel_job is the
            # one place that writes "cancelling...", immediately after
            # this same dispatch-triggered render already ran.
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
        # Same button, two lives: [Export] while idle, [Cancel] while an
        # export is running — which one, decided purely by whether a job
        # is currently running, mirrors ``action_cancel_or_back``'s own
        # branch below exactly (``_cancel_job`` is the one place that logic
        # lives).
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
        # StartExport's own update() handler validates dst_text itself
        # (empty -> a Notify the store's effect interpreter turns into a
        # real app.notify() -- see core/app/update.py) rather than this
        # screen re-validating before dispatching; the *new* key(s) in
        # `model.jobs` after the dispatch (there is at most one --
        # StartExport's own case adds exactly one job on acceptance, none
        # on rejection) tell "accepted" from "rejected" without update()
        # needing its own reply channel back to one specific caller, and
        # without this screen needing to know or predict how `update()`
        # itself mints a job's own id.
        jobs_before = set(app.store.model.jobs)
        app.store.dispatch(StartExport(unit=self._unit, dst_text=dst_text, sparse=app.default_sparse))
        new_job_ids = set(app.store.model.jobs) - jobs_before
        if not new_job_ids:
            return
        (job_id,) = new_job_ids
        job = app.store.model.jobs[job_id]
        self._job_id = job_id
        # Move focus off the destination Input once exporting starts —
        # otherwise every single-letter binding on this screen (``b``
        # background included) gets swallowed as ordinary typed text by
        # the still-focused Input instead of reaching the screen's
        # BINDINGS (Input intercepts printable-character keys for editing
        # before they'd ever bubble up to a binding lookup). Escape/Enter
        # aren't affected either way since Input doesn't claim those.
        self.set_focus(None)
        # The subscription registered in on_mount only re-renders on the
        # *next* dispatch's own notify pass -- which already happened,
        # synchronously, inside the dispatch() call above, using the
        # `self._job_id` value from *before* this method set it. One
        # explicit catch-up render for this job's own just-minted state
        # (0 done, no rate/eta/elapsed text yet -- or, if the one job slot
        # was already occupied, QUEUED instead of RUNNING) closes that
        # gap -- this is also the one place that writes the "exporting..."/
        # "queued..." status text, so it stays correct for either case
        # without a separate write here that a later QUEUED->RUNNING
        # promotion (a subsequent render, not this method running again)
        # would have no way to redo.
        self._render_job(job)

    def _render_progress(self, job: Job) -> None:
        # job.total, not `job.total or None`: ProgressBar's own
        # percentage math already treats a genuinely known total of 0
        # (an empty unit) as 100% complete, not indeterminate -- `or
        # None` would collapse that known-zero back into "unknown",
        # rendering an indeterminate spinner for an export that's
        # actually already done. Matches Job.percent's own total==0
        # special case (model.py) for the same reason.
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
        # A QUEUED job (self._job_id is set the same way for either
        # status -- see _start()) hasn't actually started yet, so
        # "continuing export in the background" would be inaccurate;
        # either way the job already lives in AppModel.jobs/
        # queued_requests regardless of whether this screen stays open,
        # so popping is correct either way -- only the wording differs.
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
        # dispatch() above runs synchronously to completion, including
        # this screen's own subscription callback -- for a QUEUED job,
        # CancelJobRequested removes it immediately (never routed through
        # CANCELLING, see update.py's own branch) and _render_job's
        # FinishedJob path already reset self._job_id to None and wrote
        # the real "cancelled (was queued...)" status text. Only a job
        # still RUNNING/CANCELLING after that (self._job_id still set)
        # gets the generic "cancelling..." text here -- otherwise this
        # would unconditionally clobber the correct terminal text with a
        # wrong one nothing would ever correct afterward.
        if self._job_id is not None:
            self.query_one("#export-status", Static).update("cancelling...")
