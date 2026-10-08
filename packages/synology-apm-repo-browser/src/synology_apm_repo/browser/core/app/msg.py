"""Messages the app-level ``update()`` (``core/app/update.py``) handles."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.app.model import ExportTarget, JobOutcome
from synology_apm_repo.browser.core.keys import JobId


@dataclasses.dataclass(frozen=True, slots=True)
class StartExport:
    """Dispatched by ``ExportScreen`` on Export/Enter. ``dst_text`` is the
    raw destination text; ``update()`` validates it."""

    target: ExportTarget
    dst_text: str
    sparse: bool


@dataclasses.dataclass(frozen=True, slots=True)
class ExportProgressed:
    job_id: JobId
    done: int
    total: int | None
    size_text: str
    rate_text: str
    eta_text: str
    elapsed_text: str
    position: str = ""
    item: str = ""


@dataclasses.dataclass(frozen=True, slots=True)
class ExportFinished:
    job_id: JobId
    outcome: JobOutcome


@dataclasses.dataclass(frozen=True, slots=True)
class CancelJobRequested:
    """Dispatched by ``ExportScreen``'s cancel action or ``WorklistScreen``'s
    ``x``."""

    job_id: JobId


@dataclasses.dataclass(frozen=True, slots=True)
class VerifyFullStarted:
    """Dispatched by ``DiagnosticsScreen.action_run_full`` before it starts
    the check's worker, so a second quick press already sees the run as
    busy (a ``@work`` call only schedules its worker)."""


@dataclasses.dataclass(frozen=True, slots=True)
class VerifyFullFinished:
    """Dispatched once per FULL run however it ended; a queued export
    waiting behind it then starts."""


AppMsg = StartExport | ExportProgressed | ExportFinished | CancelJobRequested | VerifyFullStarted | VerifyFullFinished
