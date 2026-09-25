"""Unit tests for ``browser.core.app.select.export_button_spec`` — pure
text/state-building, no Textual/widget involved at all (unlike the Pilot
test this selector is extracted from, which still exists to prove the
*widget* side)."""

from __future__ import annotations

from synology_apm_repo.browser.core.app.model import FinishedJob, Job, JobOutcome, JobStatus
from synology_apm_repo.browser.core.app.select import ExportButtonSpec, export_button_spec
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.strings import EXPORT_CANCEL_LABEL, EXPORT_START_LABEL


def test_export_button_spec_with_no_job_shows_export_primary_inactive() -> None:
    assert export_button_spec(None) == ExportButtonSpec(
        label=EXPORT_START_LABEL, variant="primary", progress_active=False
    )


def test_export_button_spec_with_a_running_job_shows_cancel_warning_active() -> None:
    job = Job(id=JobId(1), label="a.bin", group="export-1")
    assert export_button_spec(job) == ExportButtonSpec(
        label=EXPORT_CANCEL_LABEL, variant="warning", progress_active=True
    )


def test_export_button_spec_with_a_queued_job_shows_cancel_but_progress_inactive() -> None:
    """A still-``QUEUED`` job has no progress to show yet -- Cancel is
    still offered (queueing is undoable), but an indeterminate spinning
    bar behind a "queued" status would be misleading."""
    job = Job(id=JobId(1), label="a.bin", group="export-1", status=JobStatus.QUEUED)
    assert export_button_spec(job) == ExportButtonSpec(
        label=EXPORT_CANCEL_LABEL, variant="warning", progress_active=False
    )


def test_export_button_spec_with_a_finished_job_shows_export_primary_inactive() -> None:
    """A job that just finished (cancelled, errored, or successful --
    JobOutcome doesn't distinguish which for this purpose) resets the
    button the same as no job at all."""
    outcome = JobOutcome(notify_message="done", notify_severity="information", status_text="[green]done[/green]")
    finished = FinishedJob(id=JobId(1), label="a.bin", outcome=outcome)
    assert export_button_spec(finished) == ExportButtonSpec(
        label=EXPORT_START_LABEL, variant="primary", progress_active=False
    )
