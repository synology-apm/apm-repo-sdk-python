"""Messages the app-level store's own ``update()`` reacts to — see
``core/app/update.py``.
"""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.app.model import JobOutcome
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.sdk.units.base import RestorableUnit


@dataclasses.dataclass(frozen=True, slots=True)
class StartExport:
    """Dispatched by ``ExportScreen`` on Export/Enter. ``dst_text`` is
    the raw, unvalidated destination path text — validation happens in
    ``update()`` (pure, testable without a real filesystem), not in the
    screen, the same "screen only reads widgets, model decides" split
    every other domain's own ``update()`` follows."""

    unit: RestorableUnit
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


@dataclasses.dataclass(frozen=True, slots=True)
class ExportFinished:
    job_id: JobId
    outcome: JobOutcome


@dataclasses.dataclass(frozen=True, slots=True)
class CancelJobRequested:
    """Dispatched by ``ExportScreen``'s own cancel action or
    ``WorklistScreen``'s ``x`` — both request the same thing regardless
    of which screen currently owns the job."""

    job_id: JobId


@dataclasses.dataclass(frozen=True, slots=True)
class VerifyFullStarted:
    """Dispatched synchronously by ``DiagnosticsScreen.action_run_full``,
    before the screen-local worker that runs the check starts — a
    ``@work``-decorated call only schedules the worker, so setting this
    flag from inside the worker's body instead would let two quick
    ``action_run_full`` calls both pass the busy-check guard first.
    Verify FULL has no ``Job``/``RunX`` pair like export: its work never
    outlives the screen that started it, so there's no App-hosting
    boundary to cross."""


@dataclasses.dataclass(frozen=True, slots=True)
class VerifyFullFinished:
    """Dispatched from ``DiagnosticsScreen._run``'s ``finally``, so it
    fires exactly once per FULL run regardless of how it ended
    (success/cancelled/errored) — this is what lets a queued export
    waiting behind it auto-start."""


AppMsg = StartExport | ExportProgressed | ExportFinished | CancelJobRequested | VerifyFullStarted | VerifyFullFinished
