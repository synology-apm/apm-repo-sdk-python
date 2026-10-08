"""Commands the app-level ``update()`` returns, carried out by
``runtime/app_effects.py``.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from synology_apm_repo.browser.core.app.model import ExportTarget
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.core.notify import Notify as Notify


@dataclasses.dataclass(frozen=True, slots=True)
class RunExport:
    """Starts the export. ``group`` is the worker group its ``Job``
    carries, so a later ``CancelGroup`` reaches exactly this run."""

    job_id: JobId
    group: str
    target: ExportTarget
    dst: Path
    sparse: bool


@dataclasses.dataclass(frozen=True, slots=True)
class CancelGroup:
    """Cancels every worker in ``group``."""

    group: str


AppCmd = RunExport | CancelGroup | Notify
