"""``update(model, msg) -> (model, cmds)`` for the app-level store — pure,
synchronous, exhaustive (``case _: assert_never(msg)`` makes mypy flag a
missing case at type-check time the moment a new ``AppMsg`` variant is
added but not handled here).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from pathlib import Path
from typing import TypeVar, assert_never

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

_T = TypeVar("_T")


def _without(mapping: Mapping[JobId, _T], job_id: JobId) -> dict[JobId, _T]:
    """``mapping`` with ``job_id`` dropped."""
    return {k: v for k, v in mapping.items() if k != job_id}


def _record_finished(model: AppModel, job_id: JobId, label: str, outcome: JobOutcome) -> AppModel:
    """Appends a ``FinishedJob`` for ``job_id`` to ``model.recent`` (capped)."""
    finished = FinishedJob(id=job_id, label=label, outcome=outcome)
    return dataclasses.replace(model, recent=_cap_recent((*model.recent, finished)))


def _notify_outcome(outcome: JobOutcome) -> Notify:
    """The toast for a finished job's outcome."""
    return Notify(message=outcome.notify_message, severity=outcome.notify_severity, title=EXPORT_NOTIFY_TITLE)


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
            cmd = RunExport(job_id=job_id, group=job.group, unit=request.unit, dst=request.dst, sparse=request.sparse)
            return new_model, cmd
    return model, None


def update(model: AppModel, msg: AppMsg) -> tuple[AppModel, tuple[AppCmd, ...]]:
    match msg:
        case StartExport(unit=unit, dst_text=dst_text, sparse=sparse):
            if not dst_text:
                return model, (Notify(message=EXPORT_NO_DESTINATION_WARNING, severity="warning"),)
            job_id = model.next_job_id
            group = f"job-{job_id}"
            blocked = model.export_occupied or model.verify_full_running
            if blocked:
                new_job = Job(id=job_id, label=f"export {unit.name}", group=group, status=JobStatus.QUEUED)
                request = QueuedExport(unit=unit, dst=Path(dst_text), sparse=sparse)
                new_model = dataclasses.replace(
                    model,
                    jobs={**model.jobs, job_id: new_job},
                    queued_requests={**model.queued_requests, job_id: request},
                    next_job_id=JobId(job_id + 1),
                )
                return new_model, (Notify(message=EXPORT_QUEUED_MESSAGE, severity="information"),)
            new_job = Job(id=job_id, label=f"export {unit.name}", group=group)
            new_model = dataclasses.replace(model, jobs={**model.jobs, job_id: new_job}, next_job_id=JobId(job_id + 1))
            cmd = RunExport(job_id=job_id, group=group, unit=unit, dst=Path(dst_text), sparse=sparse)
            return new_model, (cmd,)

        case ExportProgressed(
            job_id=job_id,
            done=done,
            total=total,
            size_text=size_text,
            rate_text=rate_text,
            eta_text=eta_text,
            elapsed_text=elapsed_text,
        ):
            job = model.jobs.get(job_id)
            if job is None:  # already finished/cancelled -- a late progress tick, not an error
                return model, ()
            new_job = dataclasses.replace(
                job,
                done=done,
                total=total,
                size_text=size_text,
                rate_text=rate_text,
                eta_text=eta_text,
                elapsed_text=elapsed_text,
            )
            return dataclasses.replace(model, jobs={**model.jobs, job_id: new_job}), ()

        case ExportFinished(job_id=job_id, outcome=outcome):
            job = model.jobs.get(job_id)
            if job is None:  # pragma: no cover - defensive; RunExport reports exactly once per job_id
                return model, ()
            new_model = _record_finished(model, job_id, job.label, outcome)
            new_model = dataclasses.replace(new_model, jobs=_without(new_model.jobs, job_id))
            notify = _notify_outcome(outcome)
            new_model, promoted = _promote_next_queued(new_model)
            return new_model, (notify, promoted) if promoted is not None else (notify,)

        case CancelJobRequested(job_id=job_id):
            job = model.jobs.get(job_id)
            if job is None:  # already finished/gone -- nothing to cancel
                return model, ()
            if job.status is JobStatus.QUEUED:
                # Never started a worker -- no ExportFinished will ever
                # arrive for it, so it's removed directly rather than
                # routed through CANCELLING (which would leave it stuck
                # forever waiting for a completion that can't happen). It
                # never occupied the running slot either, so nothing to
                # promote.
                outcome = JobOutcome(
                    notify_message=f"{job.label}: cancelled (was queued, never started)",
                    notify_severity="information",
                    status_text="[yellow]cancelled[/yellow] (was queued, never started)",
                )
                new_model = _record_finished(model, job_id, job.label, outcome)
                new_model = dataclasses.replace(
                    new_model,
                    jobs=_without(new_model.jobs, job_id),
                    queued_requests=_without(new_model.queued_requests, job_id),
                )
                return new_model, (_notify_outcome(outcome),)
            new_job = dataclasses.replace(job, status=JobStatus.CANCELLING)
            new_model = dataclasses.replace(model, jobs={**model.jobs, job_id: new_job})
            return new_model, (CancelGroup(group=job.group),)

        case VerifyFullStarted():
            return dataclasses.replace(model, verify_full_running=True), ()

        case VerifyFullFinished():
            new_model = dataclasses.replace(model, verify_full_running=False)
            new_model, promoted = _promote_next_queued(new_model)
            return new_model, (promoted,) if promoted is not None else ()

        case _:  # pragma: no cover - exhaustiveness fallback; mypy proves this unreachable
            assert_never(msg)
