"""``ExportScreen`` is a small, centered modal overlay (same treatment as
``KeyDialog``) for a destination path, progress bar, and cancel, with ``b``
sending the export to the background -- always offered, no size
threshold -- so a long-running export never locks the user into one
screen. Output goes to ``<dst>.part`` first, renamed to ``<dst>`` only on
success — see the CLI's ``export`` command's own docstring for why.

No sparse toggle here: every export uses ``app.default_sparse`` (set
once at launch via ``synology-apm-repo-browser --no-sparse-export``, see ``app.py``'s
``main()``), not a per-export ``Checkbox`` -- the rare exception to
"sparse is what you want" is a launch-time preference, not something
worth asking on every export.

The export always runs as an ``app.BackgroundJob`` owned by the *app*
(``app.run_worker(...)``, never ``@work`` on ``self``), from the moment
Export is pressed, not only once backgrounded: a Textual worker whose
node is a ``Screen`` is cancelled automatically the instant that screen
unmounts, so tying it to ``self`` would make pressing ``b`` (or navigating
away) silently kill the export.

**``self.app``/``self.app_state`` are live parent-chain lookups, not
cached**, and raise ``NoActiveAppError`` once this screen is actually
removed (not merely suspended). ``_start`` therefore captures the live app
reference exactly once, while the screen is still guaranteed mounted, and
threads it through every closure below instead of touching ``self.app``
again; ``_touch_ui`` guards ordinary widget access the same way — see its
own docstring for why ``self._detached`` alone isn't enough.
"""

from __future__ import annotations

import asyncio
import functools
import os
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.screen import ModalScreen
from textual.widgets import Button, Input, ProgressBar, Static

from synology_apm_repo.browser.app import ApmRepoBrowserApp, BackgroundJob
from synology_apm_repo.browser.keymap import COMMON_BINDINGS
from synology_apm_repo.browser.screens._shared import modal_box_css
from synology_apm_repo.browser.strings import (
    EXPORT_BACKGROUNDED_TITLE,
    EXPORT_CANCEL_LABEL,
    EXPORT_DST_LABEL,
    EXPORT_DST_PLACEHOLDER,
    EXPORT_NO_DESTINATION_WARNING,
    EXPORT_NOTHING_RUNNING_WARNING,
    EXPORT_START_LABEL,
    EXPORT_STATUS_BAR,
)
from synology_apm_repo.sdk.api import ExportResult
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.format import format_bytes, format_duration, format_rate
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter, reading_progress_callback
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
    """See module docstring. ``ModalScreen`` truncates the App-level
    binding chain at itself, same as ``KeyDialog`` — only the bindings
    declared here are reachable while this dialog is open."""

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
        actually created. */
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
        self._job: BackgroundJob | None = None
        self._detached = False

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
            # show_eta=False: ETA is rendered from our own ProgressMeter below
            # (the same rate/ETA adapter export_to()'s CLI counterpart uses),
            # not ProgressBar's own built-in ETA math.
            yield ProgressBar(id="export-progress", show_eta=False)
            yield Static("", id="export-rate")
            yield Static("", id="export-status")
            yield Static(EXPORT_STATUS_BAR, id="status-bar")

    def on_unmount(self) -> None:
        # The export worker (app-owned, not screen-owned — see module
        # docstring) keeps running after this; it only stops touching this
        # screen's widgets from here on.
        self._detached = True

    def on_button_pressed(self, event: Button.Pressed) -> None:
        # Same button, two lives: [Export] while idle, [Cancel] while an
        # export is running — which one, decided purely by whether a job
        # is currently running, mirrors ``action_cancel_or_back``'s own
        # branch below exactly (``_cancel_job`` is the one place that logic
        # lives).
        if event.button.id == "export-start":
            if self._job is None:
                self._start()
            else:
                self._cancel_job()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        # Enter confirms, same as clicking the button — from either the
        # destination path.
        if event.input.id == "export-dst":
            self._start()

    def _read_export_inputs(self) -> Path | None:
        """Validates the destination path, ``notify()``-ing and returning
        ``None`` on a problem. Pure read + validation — no widget
        mutation, no job creation, so ``_start`` can bail out before
        touching either."""
        dst = self.query_one("#export-dst", Input).value
        if not dst:
            self.notify(EXPORT_NO_DESTINATION_WARNING, severity="warning")
            return None
        return Path(dst)

    def _start(self) -> None:
        if self._job is not None:
            return
        # Captured once, here, while this screen is still guaranteed
        # mounted — see the module docstring's live-``self.app`` note.
        # Every closure from here on takes ``app`` as an explicit parameter
        # rather than touching ``self.app`` again. Cast, not
        # ``NavigableScreen.app_state`` (this is a ModalScreen, not a
        # NavigableScreen — see the class docstring):
        # ``self.app`` is typed as the generic ``App[None]``, but
        # ``start_job``/``update_job``/``finish_job`` (all called on ``app``
        # below) are specific to ApmRepoBrowserApp.
        app = cast(ApmRepoBrowserApp, self.app)
        dst = self._read_export_inputs()
        if dst is None:
            return
        sparse = app.default_sparse
        self._job = app.start_job(f"export {self._unit.name}")
        self.query_one("#export-status", Static).update("exporting...")
        self.query_one("#export-progress", ProgressBar).add_class("active")
        button = self.query_one("#export-start", Button)
        button.label = EXPORT_CANCEL_LABEL
        button.variant = "warning"
        # Move focus off the destination Input once exporting starts —
        # otherwise every single-letter binding on this screen (``b``
        # background included) gets swallowed as ordinary typed text by
        # the still-focused Input instead of reaching the screen's
        # BINDINGS (Input intercepts printable-character keys for editing
        # before they'd ever bubble up to a binding lookup). Escape/Enter
        # aren't affected either way since Input doesn't claim those.
        self.set_focus(None)
        # ``functools.partial``, not a lambda: Textual's ``Worker._run_async``
        # decides whether it may run the work at all by asking
        # ``inspect.iscoroutinefunction(self._work)`` or
        # ``iscoroutinefunction(self._work.func)`` — a lambda *returning* a
        # coroutine satisfies neither and raises WorkerError("Request to
        # run a non-async function as an async worker"). A partial over
        # the ``async def`` itself exposes it as ``.func``, so the check
        # passes.
        self._job = app.attach_worker(
            self._job.id,
            app.run_worker(
                functools.partial(self._run_export, app, self._job, dst, sparse),
                name=f"export-{self._job.id}",
            ),
        )

    async def _run_export(
        self,
        app: ApmRepoBrowserApp,
        job: BackgroundJob,
        dst: Path,
        sparse: bool,
    ) -> None:
        content = self._unit.open()
        part_path = dst.with_name(dst.name + ".part")

        # ``meter`` is referenced by ``on_meter_update`` below before it's
        # assigned on the following line — ordinary Python closure
        # late-binding, not a bug: the name only needs to exist by the
        # time the callback actually *runs*, and ProgressMeter.__init__
        # never invokes its callback synchronously.
        async def on_meter_update(p: Progress) -> None:
            # ``update_job``/``finish_job`` (here and below) go through the
            # ``app`` reference captured in ``_start``, never ``_touch_ui``:
            # they need the App itself, not a screen widget, so
            # ``_touch_ui``'s NoMatches staleness guard doesn't apply to them.
            app.update_job(job.id, p.done, p.total)
            self._touch_ui(lambda: self._render_progress(meter, p))

        # Rate/ETA are computed here via ProgressMeter; on_progress below
        # wraps it with presentation.progress.reading_progress_callback(),
        # the same adapter the CLI's export command uses, so the two
        # surfaces can never disagree about what "187 MiB/s" or "ETA 00:08"
        # means.
        meter = ProgressMeter(callback=on_meter_update)
        on_progress = reading_progress_callback(meter)

        try:
            # ``mkdir``/``os.replace`` stay plain blocking calls even though
            # this runs on the event loop, not a worker thread: both are
            # single filesystem *metadata* operations, not the
            # bulk data path the SDK offloads with ``asyncio.to_thread``, and
            # wrapping the rename in particular would add a cancellation
            # point between "export finished" and "output is at its final
            # name" for no benefit.
            dst.parent.mkdir(parents=True, exist_ok=True)
            # No concurrency kwargs to pass here — export_to()'s own real
            # parallelism (a multiprocess dispatch, unconditional whenever
            # the repository's store supports it) isn't a caller-facing
            # knob; see export_scheduler.py's own module docstring.
            result = cast(
                ExportResult,
                await content.export_to(part_path, sparse=sparse, progress=on_progress),
            )
            os.replace(part_path, dst)
        except asyncio.CancelledError:
            # The SDK's own try/finally has already closed the partial
            # destination file on the way out, so <dst>.part is left on
            # disk.
            message = f"{self._unit.name}: cancelled (partial file kept as {part_path.name})"
            app.finish_job(job.id, message, severity="warning")
            self._touch_ui(lambda: self._finish("[yellow]cancelled[/yellow] (partial file kept as .part)"))
            self._job = None  # allow re-exporting from this same screen instance
            # Re-raised, never swallowed: swallowing it would leave this
            # worker reported as SUCCESS and could suppress a cancellation
            # that came from the event loop itself (app shutdown), which
            # asyncio requires to propagate.
            raise
        except ApmRepoError as exc:
            # ``str(exc)`` must be captured now, not referenced from inside a
            # lambda that runs later: ``except ... as exc`` implicitly
            # unbinds ``exc`` the moment this block exits (a well-known
            # Python gotcha, to avoid leaking traceback references), so a
            # deferred closure over the bare name would raise NameError.
            detail = str(exc)
            message = f"{self._unit.name}: export failed — {detail}"
            app.finish_job(job.id, message, severity="error")
            self._touch_ui(lambda: self._finish(f"[red]error:[/red] {safe(detail)}"))
            self._job = None
            return

        message = f"{self._unit.name}: exported {format_bytes(result.bytes_written)} to {dst}"
        app.finish_job(job.id, message, severity="information")
        self._touch_ui(
            lambda: self._finish(
                f"[green]done[/green] — {format_bytes(result.bytes_written)} written, "
                f"{format_bytes(result.holes)} holes, {format_bytes(result.zeros)} zero-fill"
            ),
        )
        self._job = None

    def _render_progress(self, meter: ProgressMeter, p: Progress) -> None:
        self.query_one("#export-progress", ProgressBar).update(total=p.total or None, progress=p.done)
        rate = meter.rate
        parts = []
        if rate > 0:
            parts.append(format_rate(rate, p.unit))
        eta = meter.eta
        if eta is not None:
            parts.append(f"ETA {format_duration(eta.total_seconds())}")
        parts.append(f"elapsed {format_duration(meter.elapsed.total_seconds())}")
        self.query_one("#export-rate", Static).update(" · ".join(parts))

    def _touch_ui(self, fn: Callable[[], None]) -> None:
        # No thread hop needed (the export worker runs on the App's own
        # event loop), but the screen can go stale between event-loop turns
        # before ``on_unmount()`` flips ``self._detached`` — so ``NoMatches`` (not
        # ``_detached`` alone) is the actual authority on whether the screen
        # is still touchable; ``_detached`` stays as a fast-path skip.
        if self._detached:
            return
        try:
            fn()
        except NoMatches:
            self._detached = True

    def _finish(self, message: str) -> None:
        # The single choke point every completion path (cancelled,
        # errored, or successful — see the three call sites in
        # _run_export) routes through, so the button flipping back to
        # "Export" happens exactly once, regardless of which of those
        # three ways this export ended.
        self.query_one("#export-status", Static).update(message)
        button = self.query_one("#export-start", Button)
        button.label = EXPORT_START_LABEL
        button.variant = "primary"

    def action_background(self) -> None:
        if self._job is None:
            self.notify(EXPORT_NOTHING_RUNNING_WARNING, severity="warning")
            return
        self.notify(f"{self._unit.name}: continuing export in the background", title=EXPORT_BACKGROUNDED_TITLE)
        self.app.pop_screen()

    def action_cancel_or_back(self) -> None:
        if self._job is not None:
            self._cancel_job()
        else:
            self.app.pop_screen()

    def _cancel_job(self) -> None:
        # Shared by the button and ``Esc`` — one place that requests
        # cancellation and sets the "cancelling..." status text, so the
        # two triggers can never drift apart. Cancellation semantics: see
        # BackgroundJob.cancel().
        assert self._job is not None
        self._job.cancel()
        # Cast for the same reason _start()'s own comment gives:
        # mark_job_cancelling is ApmRepoBrowserApp-specific, not on the
        # generic App[None] self.app is typed as.
        cast(ApmRepoBrowserApp, self.app).mark_job_cancelling(self._job.id)
        self.query_one("#export-status", Static).update("cancelling...")
