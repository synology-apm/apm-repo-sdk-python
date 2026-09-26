"""``AppEffects``: the one place an ``AppCmd`` actually does anything —
starts a real Textual worker, cancels a worker group, or calls
``App.notify()``. Constructed once alongside ``ApmRepoBrowserApp``'s
``Store``, handed to it as that store's ``perform`` callback.

``_run_export`` is the export worker body, reporting progress/completion
through ``core.app.msg`` dispatches rather than a screen-owned callback,
since the store and worker both live above any one screen.
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
                # functools.partial, not a lambda: Worker._run_async gates on
                # inspect.iscoroutinefunction, which only a partial over the
                # bound async method satisfies. run_worker_no_progress: an
                # export already has its own progress UI, so a generic
                # breadcrumb spinner would be redundant.
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
        # Refused up front, via the same shared helper the CLI's export
        # command uses. No --force equivalent exists in this UI, so this
        # always refuses rather than asking to overwrite.
        if not destination_available(dst, force=False):
            message = f"{cmd.unit.name}: export failed — {dst} already exists"
            outcome = JobOutcome(
                notify_message=message, notify_severity="error", status_text=f"[red]error:[/red] {safe(message)}"
            )
            self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))
            return
        content = cmd.unit.open()
        part_path = part_path_for(dst)

        # `meter` is referenced below before assignment -- ordinary Python
        # closure late-binding, not a bug: the name only needs to exist by
        # the time the callback runs.
        async def on_meter_update(p: Progress) -> None:
            # Computed here (not in core/, which can't hold ProgressMeter's
            # stateful tracking) via the same adapter the CLI uses, so the
            # two surfaces can never disagree. Left as "" when not yet
            # knowable, so a consumer can fall back to "-" without
            # recomputing.
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
            # A metadata-only filesystem op, not the bulk data path the
            # SDK offloads with asyncio.to_thread, so this stays blocking.
            dst.parent.mkdir(parents=True, exist_ok=True)
            # No concurrency kwargs needed -- export_to()'s own
            # ProcessPoolExecutor dispatch engages automatically whenever
            # this repository's store can be reconstructed in a fresh
            # process.
            result = cast(ExportResult, await content.export_to(part_path, sparse=cmd.sparse, progress=on_progress))
            finalize_export(part_path, dst)
        except asyncio.CancelledError:
            # keep_partial=False, matching the CLI's default (no
            # --keep-partial equivalent exists in this UI).
            resolve_cancelled_partial(part_path, keep_partial=False)
            message = f"{cmd.unit.name}: cancelled (partial file removed)"
            status_text = "[yellow]cancelled[/yellow] (partial file removed)"
            outcome = JobOutcome(notify_message=message, notify_severity="warning", status_text=status_text)
            self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))
            raise  # never swallow: would suppress an app-shutdown cancellation asyncio requires to propagate
        except Exception as exc:
            # Broader than ApmRepoError: a content source can surface a raw
            # third-party failure (e.g. a dissect.* parser) too.
            # str(exc) captured now: `except ... as exc` unbinds `exc` the
            # moment this block exits.
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
