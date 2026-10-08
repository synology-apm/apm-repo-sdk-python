"""Unit tests for ``browser.core.app.model``."""

from __future__ import annotations

from synology_apm_repo.browser.core.app.model import AppModel, FinishedJob, Job, JobOutcome, JobStatus
from synology_apm_repo.browser.core.keys import JobId


def test_job_status_string_values() -> None:
    # WorklistScreen's table renders these values directly, so a change is
    # a visible wording regression. `.value`: mypy strict flags a
    # StrEnum-vs-str `==` as a non-overlapping comparison.
    assert JobStatus.RUNNING.value == "running"
    assert JobStatus.QUEUED.value == "queued"
    assert JobStatus.CANCELLING.value == "cancelling"


def test_job_percent_is_none_before_total_is_known() -> None:
    job = Job(id=JobId(1), label="x.bin", group="job-1")
    assert job.percent is None


def test_job_percent_rounds_down_to_whole_percent() -> None:
    job = Job(id=JobId(1), label="x.bin", group="job-1", done=33, total=100)
    assert job.percent == 33


def test_job_percent_is_complete_for_a_genuinely_empty_unit() -> None:
    """``total=0`` is a known total (an empty unit), unlike ``total=None``."""
    job = Job(id=JobId(1), label="empty.bin", group="job-1", done=0, total=0)
    assert job.percent == 100


def test_app_model_defaults_to_no_jobs() -> None:
    model = AppModel()
    assert model.jobs == {}
    assert model.recent == ()
    assert model.next_job_id == JobId(1)
    assert model.queued_requests == {}
    assert model.verify_full_running is False
    assert model.export_occupied is False


def test_finished_job_carries_its_outcome() -> None:
    outcome = JobOutcome(notify_message="done", notify_severity="information", status_text="[green]done[/green]")
    finished = FinishedJob(id=JobId(1), label="x.bin", outcome=outcome)
    assert finished.outcome.status_text == "[green]done[/green]"
