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
    """``ExportScreen``'s own start/cancel button and progress-bar-active
    state, as a pure function of its own job -- ``variant`` is a plain
    ``str``, not Textual's own ``ButtonVariant`` literal, since ``core/``
    must stay import-free of Textual.
    ``Button.variant`` is itself a bare ``reactive`` with no narrower
    static type of its own, so the screen assigns this field directly,
    with no cast needed at that boundary."""

    label: str
    variant: str
    progress_active: bool


def export_button_spec(job: Job | FinishedJob | None) -> ExportButtonSpec:
    """``ExportScreen``'s single source of truth for its button/progress-bar
    state — any live ``Job`` (running, cancelling, or still queued) shows
    Cancel/warning, but the bar itself is only active once actually
    running: a still-``QUEUED`` job has no progress to show yet, so an
    indeterminate spinning bar behind its "queued" status text would be
    misleading. Anything else (nothing running yet, or a job that just
    finished) shows Export/primary with the bar inactive."""
    if isinstance(job, Job):
        return ExportButtonSpec(
            label=EXPORT_CANCEL_LABEL, variant="warning", progress_active=job.status is not JobStatus.QUEUED
        )
    return ExportButtonSpec(label=EXPORT_START_LABEL, variant="primary", progress_active=False)
