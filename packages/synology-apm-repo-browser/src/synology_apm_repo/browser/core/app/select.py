"""Pure selectors deriving display text from app-level state — no
Textual, no widget access; a caller writes the result into whatever
widget it owns.
"""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.app.model import FinishedJob, Job, JobStatus
from synology_apm_repo.browser.strings import EXPORT_CANCEL_LABEL, EXPORT_START_LABEL


@dataclasses.dataclass(frozen=True, slots=True)
class ExportButtonSpec:
    """``ExportScreen``'s start/cancel button and progress-bar-active state,
    as a pure function of its job. ``variant`` is a plain ``str``, not
    Textual's ``ButtonVariant`` literal, since ``core/`` stays Textual-free."""

    label: str
    variant: str
    progress_active: bool


def export_button_spec(job: Job | FinishedJob | None) -> ExportButtonSpec:
    """Any live ``Job`` shows Cancel/warning; the progress bar is active
    only once actually running (a ``QUEUED`` job has no progress yet).
    Anything else shows Export/primary with the bar inactive."""
    if isinstance(job, Job):
        return ExportButtonSpec(
            label=EXPORT_CANCEL_LABEL, variant="warning", progress_active=job.status is not JobStatus.QUEUED
        )
    return ExportButtonSpec(label=EXPORT_START_LABEL, variant="primary", progress_active=False)
