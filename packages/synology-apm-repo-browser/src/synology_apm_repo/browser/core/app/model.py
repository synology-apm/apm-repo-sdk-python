"""Pure app-level state: the background-job registry every screen's own
store subscribes to (currently only ``ExportScreen`` creates one, via
``StartExport`` — see ``core/app/msg.py``), plus the small value types
the status bar / ``WorklistScreen`` render from.

A ``Job`` holds no live Textual ``Worker`` (a frozen value can't), only
its own ``group`` — the same worker-group name ``runtime/app_effects.py`` passes
to ``run_worker(..., group=...)`` when it actually starts the job.
Cancelling one is a ``CancelGroup`` command the effect interpreter turns
into Textual's own ``workers.cancel_group(app, group)`` — group-based
cancellation needs no live reference to the worker at all, only its
name, which is exactly the kind of plain, comparable value a frozen
model can hold.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.sdk.units.base import RestorableUnit

#: ``recent`` below is capped at this many entries — a long session can
#: run many exports; nothing needs more than a handful of the most
#: recent outcomes remembered (WorklistScreen never reads a finished
#: job at all; a still-open ExportScreen only ever needs its own one).
_MAX_RECENT = 20


class JobStatus(enum.StrEnum):
    """A background job's own lifecycle. ``QUEUED`` (waiting for the one
    job slot ``AppModel.export_occupied`` tracks to free up) is the
    only status that ever moves back to ``RUNNING``; once ``RUNNING``,
    a job only ever moves on to ``CANCELLING`` (the export screen's
    Esc/cancel button, or ``WorklistScreen``'s ``x``), never back. A
    ``StrEnum`` so status bar/worklist text built from it (e.g.
    ``f"{label} ({job.status})"``) keeps reading as the plain
    "running"/"queued"/"cancelling" text it always has."""

    RUNNING = "running"
    QUEUED = "queued"
    CANCELLING = "cancelling"


@dataclasses.dataclass(frozen=True, slots=True)
class Job:
    """One backgroundable job's own live record, while it's still in
    ``AppModel.jobs``. A ``QUEUED`` job carries only its own
    label/group; the request it'll actually run once promoted lives in
    ``AppModel.queued_requests``, keyed by the same ``id``.

    ``size_text``/``rate_text``/``eta_text``/``elapsed_text`` (e.g.
    ``"4.2 GiB"``/``"187 MiB/s"``/``"00:08"``/``"00:42"``) arrive
    already formatted from whichever effect is driving the job
    (``runtime/app_effects.py``'s own ``ProgressMeter``, for an export)
    — rate/ETA tracking is inherently stateful (it needs to remember
    *when* the previous tick was), which has no place in a frozen
    model; only the text it produces does. Each is left at its default
    ``""`` until the first progress tick arrives (e.g. while still
    ``QUEUED``)."""

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

    @property
    def percent(self) -> int | None:
        """Whole-percent completion, or ``None`` when ``total`` isn't
        known yet — the same guard ``WorklistScreen`` needs before
        dividing by it. A ``total`` of ``0`` (an empty unit) is complete
        by definition, not unknown, so it's checked ahead of the
        division below."""
        if self.total is None:
            return None
        if self.total == 0:
            return 100
        return int(100 * self.done / self.total)


@dataclasses.dataclass(frozen=True, slots=True)
class JobOutcome:
    """A finished job's own terminal result. Two separate texts, not
    one: ``notify_message`` is the plain string passed to ``self.notify()``
    (the app-wide toast); ``status_text`` is the richer, markup-styled text
    ``ExportScreen`` renders into its own in-modal status line (e.g.
    ``"[green]done[/green] — 4.2 GiB written, ..."``) — always more
    detailed than the toast, and specific to whichever screen owns this
    job, so it's never appropriate as the toast text itself."""

    notify_message: str
    notify_severity: Literal["information", "warning", "error"]
    status_text: str


@dataclasses.dataclass(frozen=True, slots=True)
class QueuedExport:
    """The request a ``QUEUED`` export ``Job`` will actually run once
    promoted to ``RUNNING`` — see ``update.py``'s ``_promote_next_queued``.
    A ``Job`` itself carries none of this; it only exists once the export
    is running, so a still-queued one has nothing to show."""

    unit: RestorableUnit
    dst: Path
    sparse: bool


@dataclasses.dataclass(frozen=True, slots=True)
class FinishedJob:
    """A job that has left ``AppModel.jobs`` — kept in ``AppModel.recent``
    only so a still-open screen (``ExportScreen``) can read its own
    job's terminal ``outcome`` after the fact; ``WorklistScreen`` never
    reads this at all."""

    id: JobId
    label: str
    outcome: JobOutcome


@dataclasses.dataclass(frozen=True, slots=True)
class AppModel:
    jobs: Mapping[JobId, Job] = dataclasses.field(default_factory=dict)
    #: Most-recently-finished last; see ``_MAX_RECENT`` above.
    recent: tuple[FinishedJob, ...] = ()
    next_job_id: JobId = JobId(1)
    #: The request payload for each currently-``QUEUED`` job in ``jobs``,
    #: keyed by the same ``JobId`` — a ``QUEUED`` ``Job`` carries only its
    #: label and group, not the full export request, which lives here
    #: until it's promoted to ``RUNNING``.
    queued_requests: Mapping[JobId, QueuedExport] = dataclasses.field(default_factory=dict)
    #: Set for the whole duration of a screen-local verify FULL check
    #: (``DiagnosticsScreen`` cancels it on unmount instead of ever making
    #: it a ``Job``, since it's a blocking start/wait/see-result flow, not
    #: something to keep running in the background) so ``export_occupied``'s
    #: counterpart on the export side has something to check against
    #: without verify joining ``jobs`` itself.
    verify_full_running: bool = False

    @property
    def export_occupied(self) -> bool:
        """Whether some export already holds the one job slot — checked
        both before starting a new export (``update.py``'s ``StartExport``)
        and before starting a verify FULL check (``DiagnosticsScreen``'s
        ``action_run_full``), so the same predicate isn't written out
        twice."""
        return any(job.status in (JobStatus.RUNNING, JobStatus.CANCELLING) for job in self.jobs.values())


def _cap_recent(recent: tuple[FinishedJob, ...]) -> tuple[FinishedJob, ...]:
    return recent[-_MAX_RECENT:] if len(recent) > _MAX_RECENT else recent
