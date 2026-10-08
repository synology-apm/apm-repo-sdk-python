"""Selectors deriving display state from the app-level model."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.app.model import FinishedJob, Job, JobStatus
from synology_apm_repo.browser.strings import EXPORT_CANCEL_LABEL, EXPORT_START_LABEL


@dataclasses.dataclass(frozen=True, slots=True)
class ExportButtonSpec:
    """``ExportScreen``'s start/cancel button and progress-bar state.
    ``variant`` is a plain ``str``, since ``core/`` imports no Textual."""

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
