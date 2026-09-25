"""Unit tests for ``browser.core.app.model`` — ``JobStatus``'s own
string values (embedded directly in rendered text via
``f"...({job.status})"``, so a value typo here is a silent, user-visible
regression), ``Job``'s plain field/equality behavior, and ``AppModel``'s
own defaults."""

from __future__ import annotations

from synology_apm_repo.browser.core.app.model import AppModel, FinishedJob, Job, JobOutcome, JobStatus
from synology_apm_repo.browser.core.keys import JobId


def test_job_status_string_values() -> None:
    # These render directly into user-facing text (WorklistScreen's own
    # table) -- a changed value here is a visible wording regression,
    # not just an internal rename. Compared via `.value` rather than
    # `JobStatus.RUNNING == "running"` directly: mypy strict treats a
    # StrEnum-vs-str `==` as a non-overlapping comparison even though
    # StrEnum genuinely is a str subclass at runtime.
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
    """``total=0`` is a known, complete total (an empty unit), not the
    same as ``total=None`` (not yet known) -- both are falsy, so a naive
    ``if not self.total`` guard would wrongly report this job as having
    no known percent at all."""
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
