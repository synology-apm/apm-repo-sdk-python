"""``update(model, msg) -> (model, cmds)`` for the app-level store."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from pathlib import Path
from typing import assert_never

from synology_apm_repo.browser.core.app.cmd import AppCmd, CancelGroup, Notify, RunExport
from synology_apm_repo.browser.core.app.model import (
    AppModel,
    FinishedJob,
    Job,
    JobOutcome,
    JobStatus,
    QueuedExport,
    _cap_recent,
)
from synology_apm_repo.browser.core.app.msg import (
    AppMsg,
    CancelJobRequested,
    ExportFinished,
    ExportProgressed,
    StartExport,
    VerifyFullFinished,
    VerifyFullStarted,
)
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.strings import (
    EXPORT_NO_DESTINATION_WARNING,
    EXPORT_NOTIFY_TITLE,
    EXPORT_QUEUED_MESSAGE,
)

_Result = tuple[AppModel, tuple[AppCmd, ...]]
"""What ``update`` and each ``_on_*`` handler return: the next model and the commands to run."""


def _without[T](mapping: Mapping[JobId, T], job_id: JobId) -> dict[JobId, T]:
    return {k: v for k, v in mapping.items() if k != job_id}


def _record_finished(model: AppModel, job_id: JobId, label: str, outcome: JobOutcome) -> AppModel:
    """Appends a ``FinishedJob`` for ``job_id`` to ``model.recent`` (capped)."""
    finished = FinishedJob(id=job_id, label=label, outcome=outcome)
    return dataclasses.replace(model, recent=_cap_recent((*model.recent, finished)))


def _notify_outcome(outcome: JobOutcome) -> Notify:
    """The toast for a finished job's outcome."""
    return Notify(message=outcome.notify_message, severity=outcome.notify_severity, title=EXPORT_NOTIFY_TITLE)


def _run_cmd(job_id: JobId, group: str, request: QueuedExport) -> RunExport:
    """The command that runs ``request`` as job ``job_id``, started at once or from the queue."""
    return RunExport(job_id=job_id, group=group, target=request.target, dst=request.dst, sparse=request.sparse)


def _promote_next_queued(model: AppModel) -> tuple[AppModel, RunExport | None]:
    """Promotes the earliest still-``QUEUED`` job (FIFO, via ``model.jobs``'s
    insertion order) to ``RUNNING`` and returns the ``RunExport`` to start
    it. Returns ``model`` unchanged and ``None`` when nothing is queued."""
    for job_id, job in model.jobs.items():
        if job.status is JobStatus.QUEUED:
            request = model.queued_requests[job_id]
            new_job = dataclasses.replace(job, status=JobStatus.RUNNING)
            new_model = dataclasses.replace(
                model, jobs={**model.jobs, job_id: new_job}, queued_requests=_without(model.queued_requests, job_id)
            )
            return new_model, _run_cmd(job_id, job.group, request)
    return model, None


def _on_start_export(model: AppModel, msg: StartExport) -> _Result:
    if not msg.dst_text:
        return model, (Notify(message=EXPORT_NO_DESTINATION_WARNING, severity="warning"),)
    job_id = model.next_job_id
    group = f"job-{job_id}"
    request = QueuedExport(target=msg.target, dst=Path(msg.dst_text), sparse=msg.sparse)
    blocked = model.export_occupied or model.verify_full_running
    if blocked:
        new_job = Job(id=job_id, label=f"export {msg.target.name}", group=group, status=JobStatus.QUEUED)
        new_model = dataclasses.replace(
            model,
            jobs={**model.jobs, job_id: new_job},
            queued_requests={**model.queued_requests, job_id: request},
            next_job_id=JobId(job_id + 1),
        )
        return new_model, (Notify(message=EXPORT_QUEUED_MESSAGE, severity="information"),)
    new_job = Job(id=job_id, label=f"export {msg.target.name}", group=group)
    new_model = dataclasses.replace(model, jobs={**model.jobs, job_id: new_job}, next_job_id=JobId(job_id + 1))
    return new_model, (_run_cmd(job_id, group, request),)


def _on_export_progressed(model: AppModel, msg: ExportProgressed) -> _Result:
    job = model.jobs.get(msg.job_id)
    if job is None:  # already finished/cancelled -- a late progress tick, not an error
        return model, ()
    new_job = dataclasses.replace(
        job,
        done=msg.done,
        total=msg.total,
        size_text=msg.size_text,
        rate_text=msg.rate_text,
        eta_text=msg.eta_text,
        elapsed_text=msg.elapsed_text,
        position=msg.position,
        item=msg.item,
    )
    return dataclasses.replace(model, jobs={**model.jobs, msg.job_id: new_job}), ()


def _on_export_finished(model: AppModel, msg: ExportFinished) -> _Result:
    job = model.jobs.get(msg.job_id)
    if job is None:  # pragma: no cover - defensive; RunExport reports exactly once per job_id
        return model, ()
    new_model = _record_finished(model, msg.job_id, job.label, msg.outcome)
    new_model = dataclasses.replace(new_model, jobs=_without(new_model.jobs, msg.job_id))
    notify = _notify_outcome(msg.outcome)
    new_model, promoted = _promote_next_queued(new_model)
    return new_model, (notify, promoted) if promoted is not None else (notify,)


def _on_cancel_job_requested(model: AppModel, msg: CancelJobRequested) -> _Result:
    job = model.jobs.get(msg.job_id)
    if job is None:  # already finished/gone -- nothing to cancel
        return model, ()
    if job.status is JobStatus.QUEUED:
        # No worker started, so no ExportFinished will come: remove it
        # now rather than leave it CANCELLING forever.
        outcome = JobOutcome(
            notify_message=f"{job.label}: cancelled (was queued, never started)",
            notify_severity="information",
            status_text="[yellow]cancelled[/yellow] (was queued, never started)",
        )
        new_model = _record_finished(model, msg.job_id, job.label, outcome)
        new_model = dataclasses.replace(
            new_model,
            jobs=_without(new_model.jobs, msg.job_id),
            queued_requests=_without(new_model.queued_requests, msg.job_id),
        )
        return new_model, (_notify_outcome(outcome),)
    new_job = dataclasses.replace(job, status=JobStatus.CANCELLING)
    new_model = dataclasses.replace(model, jobs={**model.jobs, msg.job_id: new_job})
    return new_model, (CancelGroup(group=job.group),)


def update(model: AppModel, msg: AppMsg) -> _Result:
    match msg:
        case StartExport():
            return _on_start_export(model, msg)
        case ExportProgressed():
            return _on_export_progressed(model, msg)
        case ExportFinished():
            return _on_export_finished(model, msg)
        case CancelJobRequested():
            return _on_cancel_job_requested(model, msg)
        case VerifyFullStarted():
            return dataclasses.replace(model, verify_full_running=True), ()
        case VerifyFullFinished():
            new_model = dataclasses.replace(model, verify_full_running=False)
            new_model, promoted = _promote_next_queued(new_model)
            return new_model, (promoted,) if promoted is not None else ()
        case _:
            assert_never(msg)
