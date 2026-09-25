"""Commands the app-level store's own ``update()`` returns — data, never
a callable, so a test can assert ``cmds == (SomeCmd(...),)`` directly
with no need to run or mock anything, carried out by
``runtime/app_effects.py``.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.core.notify import Notify as Notify
from synology_apm_repo.sdk.units.base import RestorableUnit


@dataclasses.dataclass(frozen=True, slots=True)
class RunExport:
    """Starts the real export — the one command that actually does I/O.
    ``group`` is the same worker-group name the ``Job`` this command was
    minted alongside already carries, so a later ``CancelGroup`` can
    reach exactly this run without either side needing a live ``Worker``
    reference."""

    job_id: JobId
    group: str
    unit: RestorableUnit
    dst: Path
    sparse: bool


@dataclasses.dataclass(frozen=True, slots=True)
class CancelGroup:
    """Cancels every worker in ``group`` — Textual's own
    ``workers.cancel_group(app, group)``, which looks workers up by
    name rather than needing a stored reference."""

    group: str


AppCmd = RunExport | CancelGroup | Notify
