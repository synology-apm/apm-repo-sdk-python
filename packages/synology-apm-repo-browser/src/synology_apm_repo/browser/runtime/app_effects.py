"""``AppEffects``: the app-level store's ``perform`` -- starts an export
worker, cancels a worker group, or calls ``App.notify()``.

``_run_export`` is the App-hosted export worker body (one item or a
folder); it reports progress and completion as ``core.app.msg`` dispatches.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
from typing import TYPE_CHECKING, Any, assert_never

from textual.app import App
from textual.worker import Worker

from synology_apm_repo.browser.core.app.cmd import AppCmd, CancelGroup, Notify, RunExport
from synology_apm_repo.browser.core.app.model import FolderExport, JobOutcome
from synology_apm_repo.browser.core.app.msg import AppMsg, ExportFinished, ExportProgressed
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.runtime.load_gate import LoadGate
from synology_apm_repo.browser.strings import EXPORT_SCANNING_TEXT
from synology_apm_repo.browser.widgets.worker_progress import run_worker_no_progress
from synology_apm_repo.sdk import RawView, RestorableUnit
from synology_apm_repo.sdk.export import (
    ExportProgressCallback,
    ExportResult,
    LocalFileSink,
    TreeItem,
    plan_tree_export,
    preflight_tree,
    run_export,
    run_tree_export,
)
from synology_apm_repo.sdk.presentation import (
    ExportTracker,
    Progress,
    ProgressMeter,
    folder_destination_problem,
    format_bytes,
    is_out_of_space,
    pluralize,
    reading_progress_callback,
    safe,
    single_destination_message,
    single_destination_problem,
    size_summary,
)

if TYPE_CHECKING:
    from synology_apm_repo.browser.runtime.store import Store


_NOTES_SHOWN = 5


def _error_outcome(message: str, status: str | None = None) -> JobOutcome:
    return JobOutcome(
        notify_message=message, notify_severity="error", status_text=f"[red]error:[/red] {safe(status or message)}"
    )


class AppEffects:
    def __init__(self, app: App[None], store: Store[Any, AppMsg, AppCmd], load_gate: LoadGate) -> None:
        self._app = app
        #: Held shared for a whole export: ``Repository.invalidate_caches()`` replaces the
        #: db connections an export reads through.
        self._load_gate = load_gate
        self._store = store

    def perform(self, cmd: AppCmd) -> None:
        match cmd:
            case RunExport():
                # functools.partial, not a lambda: Worker._run_async needs
                # inspect.iscoroutinefunction to hold. No breadcrumb spinner:
                # an export has its own progress UI.
                _worker: Worker[None] = run_worker_no_progress(
                    self._app, functools.partial(self._run_export, cmd), group=cmd.group, name=f"export-{cmd.job_id}"
                )
            case CancelGroup(group=group):
                self._app.workers.cancel_group(self._app, group)
            case Notify(message=message, severity=severity, title=title):
                self._app.notify(message, severity=severity, title=title or "")
            case _:
                assert_never(cmd)

    async def _run_export(self, cmd: RunExport) -> None:
        """One item or a folder; whichever way it ends, the job is reported finished exactly once."""
        target = cmd.target
        active = ExportTracker(cmd.dst)
        try:
            async with self._load_gate.shared():
                if isinstance(target, RestorableUnit):
                    outcome = await self._export_unit(cmd, target, active)
                else:
                    outcome = await self._export_folder(cmd, target, active)
        except asyncio.CancelledError:
            # A second cancel can interrupt the wrap-up below; the job must still be reported finished.
            note = ""
            try:
                # run_export already aborted the sink; abort() is idempotent and repeats what it left behind.
                if (leftover := await active.leftover()) is not None:
                    note = f" ({leftover})"
            except Exception:  # noqa: BLE001 - a failing abort only costs the leftover note; it must not end the app
                pass
            finally:
                if (files := active.files_note()) is not None:
                    note += f" — {files}"
                message = f"{target.name}: cancelled{note}"
                status_text = f"[yellow]cancelled[/yellow]{note}"
                outcome = JobOutcome(notify_message=message, notify_severity="warning", status_text=status_text)
                self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))
            raise  # a swallowed cancellation would break app shutdown
        except Exception as exc:  # noqa: BLE001
            # Broader than ApmRepoError: a content source can surface a raw
            # third-party parser failure too.
            detail = f"{active.label}: {exc}" if active.label else str(exc)
            try:
                if is_out_of_space(exc) and active.sink is not None:
                    detail = await active.out_of_space(exc)
            except Exception:  # noqa: BLE001 - a failing abort only costs the out-of-space detail; it must not end the app
                pass
            finally:  # a cancel that interrupts the abort above must not leave the job unreported
                outcome = _error_outcome(f"{target.name}: export failed — {detail}", detail)
                self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))
            return
        self._store.dispatch(ExportFinished(job_id=cmd.job_id, outcome=outcome))

    def _meter(self, job_id: JobId, position: str = "", item: str = "") -> ProgressMeter:
        """A meter whose updates become ``ExportProgressed`` for ``job_id``."""

        async def on_meter_update(p: Progress) -> None:
            # Formatted here because core/ can't hold ProgressMeter's state.
            # "" means not yet knowable.
            size_text = format_bytes(p.total) if p.total is not None else ""
            text = meter.formatted(p.unit)
            self._store.dispatch(
                ExportProgressed(
                    job_id=job_id,
                    done=p.done,
                    total=p.total,
                    size_text=size_text,
                    rate_text=text.rate,
                    eta_text=text.eta,
                    elapsed_text=text.elapsed,
                    position=position,
                    item=item,
                )
            )

        meter = ProgressMeter(callback=on_meter_update)
        return meter

    async def _export_unit(self, cmd: RunExport, unit: RestorableUnit, active: ExportTracker) -> JobOutcome:
        dst = cmd.dst
        # This UI has no --force equivalent, so an existing dst is refused.
        if (problem := single_destination_problem(dst, force=False)) is not None:
            return _error_outcome(f"{unit.name}: export failed — {single_destination_message(dst, problem)}")
        content = unit.content
        active.sink = sink = LocalFileSink(dst, staged=True)
        result = await run_export(
            content, sink, sparse=cmd.sparse, progress=reading_progress_callback(self._meter(cmd.job_id))
        )
        message = f"{unit.name}: exported {format_bytes(result.bytes_written)} to {dst}"
        if unit.degraded is not None:
            status_text = f"[yellow]done, incomplete[/yellow] — {size_summary(result)}\n{safe(unit.degraded)}"
            return JobOutcome(
                notify_message=f"{message} — incomplete: {unit.degraded}",
                notify_severity="warning",
                status_text=status_text,
            )
        status_text = f"[green]done[/green] — {size_summary(result)}"
        return JobOutcome(notify_message=message, notify_severity="information", status_text=status_text)

    async def _export_folder(self, cmd: RunExport, folder: FolderExport, active: ExportTracker) -> JobOutcome:
        dst = cmd.dst
        failed = f"{folder.name}: export failed — "
        if (folder_problem := folder_destination_problem(dst)) is not None:
            return _error_outcome(f"{failed}{folder_problem}")
        self._store.dispatch(
            ExportProgressed(
                job_id=cmd.job_id,
                done=0,
                total=None,
                size_text="",
                rate_text="",
                eta_text="",
                elapsed_text="",
                position=EXPORT_SCANNING_TEXT,
            )
        )
        provider = await folder.catalog.provider(folder.version, raw=RawView() if folder.force_raw else None)
        try:
            plan = await plan_tree_export(provider, folder.node, dst)
            preflight = await asyncio.to_thread(preflight_tree, plan, dst, force=False)
            if (problem := preflight.problem) is not None:
                return _error_outcome(f"{failed}{problem.message()}")
            total = len(plan.items)
            active.total = total

            def on_item_start(index: int, item: TreeItem, sink: LocalFileSink) -> ExportProgressCallback:
                active.item_started(sink, item.path, item.relative)
                position = f"file {index + 1} of {total}"
                self._store.dispatch(
                    ExportProgressed(
                        job_id=cmd.job_id,
                        done=0,
                        total=item.node.size,
                        size_text=format_bytes(item.node.size) if item.node.size is not None else "",
                        rate_text="",
                        eta_text="",
                        elapsed_text="",
                        position=position,
                        item=item.relative,
                    )
                )
                return reading_progress_callback(self._meter(cmd.job_id, position, item.relative))

            def on_item_done(_item: TreeItem, _result: ExportResult) -> None:
                active.item_finished()

            done = await run_tree_export(
                provider,
                plan,
                dst,
                preflight=preflight,
                sparse=cmd.sparse,
                on_item_start=on_item_start,
                on_item_done=on_item_done,
            )
        finally:
            with contextlib.suppress(Exception):  # a failing close must not turn a finished export into an error
                await folder.repo.release_provider(provider)
        files = f"{len(done.exported)} {pluralize(len(done.exported), 'file')}"
        summary = f"{files}, {size_summary(done.totals)}"
        exported = f"{folder.name}: exported {files} to {dst}"
        notes = [
            *(f"incomplete {safe(item.relative)}: {safe(item.reason)}" for item in done.incomplete),
            *(f"skipped {safe(item.relative)}: {item.reason.description}" for item in done.skipped),
        ]
        if not notes:
            return JobOutcome(
                notify_message=exported, notify_severity="information", status_text=f"[green]done[/green] — {summary}"
            )
        shown = notes[:_NOTES_SHOWN]
        if len(notes) > _NOTES_SHOWN:
            shown.append(f"... and {len(notes) - _NOTES_SHOWN} more")
        counts = ", ".join(
            f"{count} {label}"
            for count, label in ((len(done.skipped), "skipped"), (len(done.incomplete), "incomplete"))
            if count
        )
        status_text = "\n".join([f"[yellow]done with {counts}[/yellow] — {summary}", *shown])
        return JobOutcome(notify_message=f"{exported}, {counts}", notify_severity="warning", status_text=status_text)
