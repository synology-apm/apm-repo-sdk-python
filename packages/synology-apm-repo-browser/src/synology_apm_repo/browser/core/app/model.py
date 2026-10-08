"""App-level state: the background-job registry (export jobs, started by
``ExportScreen``) and the value types screens render from.

A ``Job`` holds its worker-group name, not a live ``Worker``: cancelling it
is a ``CancelGroup`` command, which needs only the name.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.sdk import Catalog, Node, Repository, RestorableUnit, Version

#: Cap on ``AppModel.recent``; only a still-open ``ExportScreen`` reads it,
#: for its own job.
_MAX_RECENT = 20


class JobStatus(enum.StrEnum):
    """A background job's lifecycle: ``QUEUED`` -> ``RUNNING`` ->
    ``CANCELLING``, never back. Its value is the displayed status text."""

    RUNNING = "running"
    QUEUED = "queued"
    CANCELLING = "cancelling"


@dataclasses.dataclass(frozen=True, slots=True)
class Job:
    """One backgroundable job's live record, while still in
    ``AppModel.jobs``. A ``QUEUED`` job carries only its label/group; the
    request it'll run once promoted lives in ``AppModel.queued_requests``,
    keyed by the same ``id``.

    ``size_text``/``rate_text``/``eta_text``/``elapsed_text`` arrive
    pre-formatted from the effect driving the job, which owns the stateful
    rate/ETA tracking; each is ``""`` until the first progress tick."""

    id: JobId
    label: str
    group: str
    done: int = 0
    total: int | None = None
    status: JobStatus = JobStatus.RUNNING
    size_text: str = ""
    rate_text: str = ""
    eta_text: str = ""
    elapsed_text: str = ""
    #: A folder export's progress through its files ("file 3 of 12", or
    #: "scanning...") and the file in flight; ``""`` for a single-item export.
    #: ``done``/``total`` above are the bytes of that file.
    position: str = ""
    item: str = ""

    @property
    def file_text(self) -> str:
        """The single line the export dialog shows."""
        return " — ".join(part for part in (self.position, self.item) if part)

    @property
    def percent(self) -> int | None:
        """Whole-percent completion, or ``None`` when ``total`` isn't known
        yet. A ``total`` of ``0`` is complete by definition, not unknown."""
        if self.total is None:
            return None
        if self.total == 0:
            return 100
        return int(100 * self.done / self.total)


@dataclasses.dataclass(frozen=True, slots=True)
class JobOutcome:
    """A finished job's terminal result: ``notify_message`` is the plain
    toast, ``status_text`` the markup a screen's status line shows."""

    notify_message: str
    notify_severity: Literal["information", "warning", "error"]
    status_text: str


@dataclasses.dataclass(frozen=True, slots=True)
class FolderExport:
    """What to export when it is a folder: every item below ``node``.

    Carries what the export needs to open its *own* provider when it starts,
    because the ``UnitScreen`` that offered it releases its provider once the
    user navigates away, while an export may still be queued or running.
    ``force_raw`` is the verbose mode that screen had the provider opened with."""

    repo: Repository
    catalog: Catalog
    version: Version
    force_raw: bool
    node: Node
    name: str


ExportTarget = RestorableUnit | FolderExport
"""One resolved item, or a whole folder."""


@dataclasses.dataclass(frozen=True, slots=True)
class QueuedExport:
    """The request a ``QUEUED`` export ``Job`` runs once promoted to
    ``RUNNING``."""

    target: ExportTarget
    dst: Path
    sparse: bool


@dataclasses.dataclass(frozen=True, slots=True)
class FinishedJob:
    """A job that has left ``AppModel.jobs``, kept in ``AppModel.recent``
    so a still-open ``ExportScreen`` can read its ``outcome``."""

    id: JobId
    label: str
    outcome: JobOutcome


@dataclasses.dataclass(frozen=True, slots=True)
class AppModel:
    jobs: Mapping[JobId, Job] = dataclasses.field(default_factory=dict)
    #: Most-recently-finished last, capped at ``_MAX_RECENT``.
    recent: tuple[FinishedJob, ...] = ()
    next_job_id: JobId = JobId(1)
    #: The request of each ``QUEUED`` job in ``jobs``, until it is promoted.
    queued_requests: Mapping[JobId, QueuedExport] = dataclasses.field(default_factory=dict)
    #: Set while ``DiagnosticsScreen`` runs a verify FULL check, which is
    #: screen-local rather than a ``Job``; a new export queues behind it.
    verify_full_running: bool = False

    @property
    def export_occupied(self) -> bool:
        """Whether some export already holds the one job slot."""
        return jobs_occupy_slot(self.jobs)


def jobs_occupy_slot(jobs: Mapping[JobId, Job]) -> bool:
    """Whether any of ``jobs`` is running (or still winding down), i.e. holds the export slot."""
    return any(job.status in (JobStatus.RUNNING, JobStatus.CANCELLING) for job in jobs.values())


def _cap_recent(recent: tuple[FinishedJob, ...]) -> tuple[FinishedJob, ...]:
    return recent[-_MAX_RECENT:] if len(recent) > _MAX_RECENT else recent
