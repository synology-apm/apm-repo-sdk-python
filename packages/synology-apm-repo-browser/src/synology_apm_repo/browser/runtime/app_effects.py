"""``AppEffects``: the one place an ``AppCmd`` actually does anything —
starts a real Textual worker, cancels a worker group, or calls
``App.notify()``. Constructed once alongside ``ApmRepoBrowserApp``'s own
``Store``, and handed to it as that store's ``perform`` callback.

``_run_export`` is the export worker body itself, reporting progress and
completion through ``core.app.msg`` dispatches (``ExportProgressed``/
``ExportFinished``) rather than a screen-owned callback or posted
``Message``: the store and the worker both live above any one screen, so
neither needs a screen to relay through.
"""

from __future__ import annotations

import asyncio
import functools
from typing import TYPE_CHECKING, Any, assert_never, cast

from textual.app import App
from textual.worker import Worker

from synology_apm_repo.browser.core.app.cmd import AppCmd, CancelGroup, Notify, RunExport
from synology_apm_repo.browser.core.app.model import JobOutcome
from synology_apm_repo.browser.core.app.msg import AppMsg, ExportFinished, ExportProgressed
from synology_apm_repo.browser.widgets.worker_progress import run_worker_no_progress
from synology_apm_repo.sdk.api import ExportResult
from synology_apm_repo.sdk.presentation.export_target import (
    destination_available,
    finalize_export,
    part_path_for,
    resolve_cancelled_partial,
)
from synology_apm_repo.sdk.presentation.format import format_bytes, format_duration, format_rate
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter, reading_progress_callback

if TYPE_CHECKING:
    from synology_apm_repo.browser.runtime.store import Store


class AppEffects:
    def __init__(self, app: App[None], store: Store[Any, AppMsg, AppCmd]) -> None:
        self._app = app
        self._store = store

    def perform(self, cmd: AppCmd) -> None:
        match cmd:
            case RunExport():
                # functools.partial (not a lambda): Worker._run_async gates on
                # inspect.iscoroutinefunction(self._work)/.func, which only a
                # partial over the bound async method satisfies. The Worker
                # return value is unused -- cancellation goes through
                # CancelGroup below, not a live reference -- but the
                # annotation is still needed for mypy to infer run_worker's
                # generic from this otherwise-unassigned call.
                # run_worker_no_progress: an export already has its own
                # progress UI (AppModel.jobs -> ExportProgressed ->
                # ExportScreen's progress bar/rate/ETA), so a generic
                # breadcrumb spinner on top would be redundant.
                _worker: Worker[None] = run_worker_no_progress(
                    self._app, functools.partial(self._run_export, cmd), group=cmd.group, name=f"export-{cmd.job_id}"
                )
            case CancelGroup(group=group):
                self._app.workers.cancel_group(self._app, group)
            case Notify(message=message, severity=severity, title=title):
                self._app.notify(message, severity=severity, title=title or "")
            case _:  # pragma: no cover - exhaustiveness fallback; mypy proves this unreachable
                assert_never(cmd)

    async def _run_export(self, cmd: RunExport) -> None:
        dst = cmd.dst
        # Refused up front, before anything is opened -- the same safety
        # check the CLI's own export command makes, via the same shared
        # helper (sdk.presentation.export_target), so an existing
        # destination can't be silently overwritten on either surface.
        # No --force equivalent exists in this UI yet, so this always
        # refuses rather than ever asking to overwrite.
        if not destination_available(dst, force=False):
            message = f"{cmd.unit.name}: export failed — {dst} already exists"
            outcome = JobOutcome(
                notify_message=message, notify_severity="error", status_text=f"[red]error:[/red] {safe(message)}"
            )
            self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))
            return
        content = cmd.unit.open()
        part_path = part_path_for(dst)

        # `meter` is referenced by `on_meter_update` below before it's
        # assigned on the following line -- ordinary Python closure
        # late-binding, not a bug: the name only needs to exist by the
        # time the callback actually *runs*, and ProgressMeter.__init__
        # never invokes its callback synchronously.
        async def on_meter_update(p: Progress) -> None:
            # Size/rate/ETA/elapsed text, computed here (not in core/,
            # which can't hold ProgressMeter's own stateful tracking) via
            # the same presentation.progress adapter the CLI's export
            # command uses, so the two surfaces can never disagree about
            # what "4.2 GiB" or "187 MiB/s" or "ETA 00:08" means. Each is
            # left as "" when not yet knowable (e.g. rate before the
            # meter's warm-up window has a sample), so a consumer (a
            # dialog column, ExportScreen's own joined status line) can
            # fall back to "-" or simply omit it without recomputing
            # anything itself.
            size_text = format_bytes(p.total) if p.total is not None else ""
            rate = meter.rate
            rate_text = format_rate(rate, p.unit) if rate > 0 else ""
            eta = meter.eta
            eta_text = format_duration(eta.total_seconds()) if eta is not None else ""
            elapsed_text = format_duration(meter.elapsed.total_seconds())
            self._store.dispatch(
                ExportProgressed(
                    job_id=cmd.job_id,
                    done=p.done,
                    total=p.total,
                    size_text=size_text,
                    rate_text=rate_text,
                    eta_text=eta_text,
                    elapsed_text=elapsed_text,
                )
            )

        meter = ProgressMeter(callback=on_meter_update)
        on_progress = reading_progress_callback(meter)

        try:
            # mkdir stays a plain blocking call even though this runs on
            # the event loop, not a worker thread: a single filesystem
            # *metadata* operation, not the bulk data path the SDK
            # offloads with asyncio.to_thread -- same tier as
            # finalize_export()'s own replace() below.
            dst.parent.mkdir(parents=True, exist_ok=True)
            # No concurrency kwargs to pass here -- export_to()'s own real
            # parallelism (a ProcessPoolExecutor dispatch) engages
            # automatically whenever this repository's store can be
            # reconstructed in a fresh process, so it isn't a
            # caller-facing knob.
            result = cast(ExportResult, await content.export_to(part_path, sparse=cmd.sparse, progress=on_progress))
            finalize_export(part_path, dst)
        except asyncio.CancelledError:
            # The SDK's own try/finally has already closed the partial
            # destination file on the way out, so <dst>.part may be left
            # on disk -- unless export_to() itself never got past
            # deferring its own first-block-succeeds file creation (a
            # cloud-sync placeholder cancelled early), in which case
            # nothing was ever written. keep_partial=False, matching the
            # CLI's own default (no --keep-partial equivalent exists in
            # this UI) -- resolve_cancelled_partial() always reports (and
            # performs) removal regardless of whether anything was actually
            # written, since "nothing left behind" holds either way and
            # there's nothing to check -- so there is only ever this one
            # message here, unlike the CLI's own handle_cancelled(), which
            # also has a --keep-partial=True branch this worker never takes.
            resolve_cancelled_partial(part_path, keep_partial=False)
            message = f"{cmd.unit.name}: cancelled (partial file removed)"
            status_text = "[yellow]cancelled[/yellow] (partial file removed)"
            outcome = JobOutcome(notify_message=message, notify_severity="warning", status_text=status_text)
            self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))
            # Re-raised, never swallowed: swallowing it would leave this
            # worker reported as SUCCESS and could suppress a cancellation
            # that came from the event loop itself (app shutdown), which
            # asyncio requires to propagate.
            raise
        except Exception as exc:
            # Broader than ApmRepoError on purpose: a content source can
            # still surface a raw failure from a third-party dependency it
            # wraps (e.g. a dissect.* filesystem parser) that isn't an
            # ApmRepoError at all.
            #
            # str(exc) must be captured now, not referenced from inside a
            # closure that runs later: `except ... as exc` implicitly
            # unbinds `exc` the moment this block exits (a well-known
            # Python gotcha, to avoid leaking traceback references).
            detail = str(exc)
            message = f"{cmd.unit.name}: export failed — {detail}"
            outcome = JobOutcome(
                notify_message=message, notify_severity="error", status_text=f"[red]error:[/red] {safe(detail)}"
            )
            self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))
            return

        message = f"{cmd.unit.name}: exported {format_bytes(result.bytes_written)} to {dst}"
        status_text = (
            f"[green]done[/green] — {format_bytes(result.bytes_written)} written, "
            f"{format_bytes(result.holes)} holes, {format_bytes(result.zeros)} zero-fill"
        )
        outcome = JobOutcome(notify_message=message, notify_severity="information", status_text=status_text)
        self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))
