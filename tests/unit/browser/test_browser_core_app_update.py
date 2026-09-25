"""Unit tests for ``browser.core.app.update`` — every branch, no
Textual/App/Pilot involved at all. Each test asserts on the returned
``(model, cmds)`` pair directly -- ``Cmd`` is data, never a callable, so
this is a one-line test with no effects layer involved."""

from __future__ import annotations

from pathlib import Path

from synology_apm_repo.browser.core.app.cmd import CancelGroup, Notify, RunExport
from synology_apm_repo.browser.core.app.model import AppModel, Job, JobOutcome, JobStatus, QueuedExport
from synology_apm_repo.browser.core.app.msg import (
    CancelJobRequested,
    ExportFinished,
    ExportProgressed,
    StartExport,
    VerifyFullFinished,
    VerifyFullStarted,
)
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.strings import (
    EXPORT_NO_DESTINATION_WARNING,
    EXPORT_NOTIFY_TITLE,
    EXPORT_QUEUED_MESSAGE,
)
from synology_apm_repo.sdk.units.base import RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _unit(name: str = "file.bin") -> RestorableUnit:
    return RestorableUnit(ref=NodeRef("repo", (name,)), name=name, is_leaf=True)


def test_start_export_with_empty_destination_is_rejected() -> None:
    model = AppModel()
    new_model, cmds = update(model, StartExport(unit=_unit(), dst_text="", sparse=True))
    assert new_model is model  # nothing changed
    assert cmds == (Notify(message=EXPORT_NO_DESTINATION_WARNING, severity="warning"),)


def test_start_export_mints_a_job_and_a_run_export_command() -> None:
    unit = _unit("file.bin")
    model = AppModel()
    new_model, cmds = update(model, StartExport(unit=unit, dst_text="./out.bin", sparse=True))

    assert new_model.next_job_id == JobId(2)
    job = new_model.jobs[JobId(1)]
    assert job.label == "export file.bin"
    assert job.group == "job-1"
    assert job.status is JobStatus.RUNNING
    assert job.done == 0
    assert job.total is None

    assert len(cmds) == 1
    run_export = cmds[0]
    assert isinstance(run_export, RunExport)
    assert run_export.job_id == JobId(1)
    assert run_export.group == "job-1"
    assert run_export.unit is unit
    assert run_export.dst == Path("./out.bin")
    assert run_export.sparse is True


def test_two_exports_get_distinct_job_ids_and_groups() -> None:
    """The second export still mints its own distinct id/group even
    though the one job slot is already taken, so it's queued rather than
    started immediately -- see the queueing tests below for the rest of
    that behavior."""
    model = AppModel()
    model, _ = update(model, StartExport(unit=_unit("a.bin"), dst_text="./a.bin", sparse=True))
    model, cmds = update(model, StartExport(unit=_unit("b.bin"), dst_text="./b.bin", sparse=True))

    assert set(model.jobs) == {JobId(1), JobId(2)}
    assert model.jobs[JobId(2)].group == "job-2"
    assert model.jobs[JobId(2)].status is JobStatus.QUEUED
    assert cmds == (Notify(message=EXPORT_QUEUED_MESSAGE, severity="information"),)


def test_export_progressed_updates_the_jobs_done_total_and_progress_text() -> None:
    model = AppModel(jobs={JobId(1): Job(id=JobId(1), label="a.bin", group="job-1")})
    new_model, cmds = update(
        model,
        ExportProgressed(
            job_id=JobId(1),
            done=50,
            total=100,
            size_text="100 B",
            rate_text="187 MiB/s",
            eta_text="00:08",
            elapsed_text="00:01",
        ),
    )
    job = new_model.jobs[JobId(1)]
    assert job.done == 50
    assert job.total == 100
    assert job.size_text == "100 B"
    assert job.rate_text == "187 MiB/s"
    assert job.eta_text == "00:08"
    assert job.elapsed_text == "00:01"
    assert cmds == ()


def test_export_progressed_for_an_unknown_job_is_a_no_op() -> None:
    """A late progress tick for a job that already finished/was
    cancelled -- not an error, a silent no-op."""
    model = AppModel()
    new_model, cmds = update(
        model,
        ExportProgressed(job_id=JobId(99), done=1, total=2, size_text="", rate_text="", eta_text="", elapsed_text=""),
    )
    assert new_model is model
    assert cmds == ()


def test_export_progressed_does_not_touch_an_unrelated_jobs_identity() -> None:
    other = Job(id=JobId(2), label="b.bin", group="job-2")
    model = AppModel(jobs={JobId(1): Job(id=JobId(1), label="a.bin", group="job-1"), JobId(2): other})
    new_model, _ = update(
        model,
        ExportProgressed(job_id=JobId(1), done=1, total=10, size_text="", rate_text="", eta_text="", elapsed_text=""),
    )
    assert new_model.jobs[JobId(2)] is other
    assert new_model.recent is model.recent  # a field this message never touches


def test_export_finished_removes_the_job_and_records_the_outcome() -> None:
    model = AppModel(jobs={JobId(1): Job(id=JobId(1), label="a.bin", group="job-1")})
    outcome = JobOutcome(
        notify_message="a.bin: exported", notify_severity="information", status_text="[green]done[/green]"
    )

    new_model, cmds = update(model, ExportFinished(job_id=JobId(1), outcome=outcome))

    assert JobId(1) not in new_model.jobs
    assert len(new_model.recent) == 1
    assert new_model.recent[0].id == JobId(1)
    assert new_model.recent[0].label == "a.bin"
    assert new_model.recent[0].outcome is outcome
    assert cmds == (Notify(message="a.bin: exported", severity="information", title=EXPORT_NOTIFY_TITLE),)


def test_export_finished_does_not_touch_an_unrelated_jobs_identity() -> None:
    other = Job(id=JobId(2), label="b.bin", group="job-2")
    model = AppModel(jobs={JobId(1): Job(id=JobId(1), label="a.bin", group="job-1"), JobId(2): other})
    outcome = JobOutcome(notify_message="x", notify_severity="information", status_text="x")
    new_model, _ = update(model, ExportFinished(job_id=JobId(1), outcome=outcome))
    assert new_model.jobs[JobId(2)] is other


def test_export_finished_for_an_already_gone_job_is_a_no_op() -> None:
    model = AppModel()
    outcome = JobOutcome(notify_message="x", notify_severity="error", status_text="x")
    new_model, cmds = update(model, ExportFinished(job_id=JobId(1), outcome=outcome))
    assert new_model is model
    assert cmds == ()


def test_recent_is_capped_dropping_the_oldest_first() -> None:
    """One model, 24 sequential start-then-finish cycles -- past
    ``_MAX_RECENT`` (20) -- proves the cap trims from the front (oldest
    first), not the back."""
    model = AppModel()
    outcome = JobOutcome(notify_message="x", notify_severity="information", status_text="x")
    for i in range(1, 25):
        model, _ = update(model, StartExport(unit=_unit(f"job-{i}.bin"), dst_text=f"./{i}.bin", sparse=True))
        model, _ = update(model, ExportFinished(job_id=JobId(i), outcome=outcome))

    assert len(model.recent) == 20
    assert model.recent[0].id == JobId(5)  # jobs 1-4 fell off the front
    assert model.recent[-1].id == JobId(24)


def test_cancel_job_requested_marks_the_job_cancelling_and_emits_cancel_group() -> None:
    model = AppModel(jobs={JobId(1): Job(id=JobId(1), label="a.bin", group="job-1")})
    new_model, cmds = update(model, CancelJobRequested(job_id=JobId(1)))
    assert new_model.jobs[JobId(1)].status is JobStatus.CANCELLING
    assert cmds == (CancelGroup(group="job-1"),)


def test_cancel_job_requested_does_not_touch_an_unrelated_jobs_identity() -> None:
    other = Job(id=JobId(2), label="b.bin", group="job-2")
    model = AppModel(jobs={JobId(1): Job(id=JobId(1), label="a.bin", group="job-1"), JobId(2): other})
    new_model, _ = update(model, CancelJobRequested(job_id=JobId(1)))
    assert new_model.jobs[JobId(2)] is other
    assert new_model.recent is model.recent  # a field this message never touches


def test_cancel_job_requested_for_an_unknown_job_is_a_no_op() -> None:
    model = AppModel()
    new_model, cmds = update(model, CancelJobRequested(job_id=JobId(1)))
    assert new_model is model
    assert cmds == ()


# -- Queueing (bounding the total process count) --------------------------


def test_start_export_queues_instead_of_running_while_another_export_runs() -> None:
    model = AppModel(jobs={JobId(1): Job(id=JobId(1), label="a.bin", group="job-1")}, next_job_id=JobId(2))
    unit = _unit("b.bin")
    new_model, cmds = update(model, StartExport(unit=unit, dst_text="./b.bin", sparse=True))

    job = new_model.jobs[JobId(2)]
    assert job.status is JobStatus.QUEUED
    assert new_model.queued_requests[JobId(2)] == QueuedExport(unit=unit, dst=Path("./b.bin"), sparse=True)
    assert cmds == (Notify(message=EXPORT_QUEUED_MESSAGE, severity="information"),)


def test_start_export_queues_while_a_verify_full_check_runs() -> None:
    model = AppModel(verify_full_running=True)
    unit = _unit("a.bin")
    new_model, cmds = update(model, StartExport(unit=unit, dst_text="./a.bin", sparse=True))

    job = new_model.jobs[JobId(1)]
    assert job.status is JobStatus.QUEUED
    assert job.group == "job-1"  # a group is still minted now, for RunExport to use once promoted
    assert cmds == (Notify(message=EXPORT_QUEUED_MESSAGE, severity="information"),)


def test_export_finished_promotes_the_earliest_queued_job() -> None:
    unit_b = _unit("b.bin")
    model = AppModel(
        jobs={
            JobId(1): Job(id=JobId(1), label="export a.bin", group="job-1"),
            JobId(2): Job(id=JobId(2), label="export b.bin", group="job-2", status=JobStatus.QUEUED),
        },
        queued_requests={JobId(2): QueuedExport(unit=unit_b, dst=Path("./b.bin"), sparse=True)},
        next_job_id=JobId(3),
    )
    outcome = JobOutcome(notify_message="a.bin: exported", notify_severity="information", status_text="done")

    new_model, cmds = update(model, ExportFinished(job_id=JobId(1), outcome=outcome))

    assert JobId(1) not in new_model.jobs
    assert new_model.jobs[JobId(2)].status is JobStatus.RUNNING
    assert JobId(2) not in new_model.queued_requests
    assert len(cmds) == 2
    notify, run_export = cmds
    assert notify == Notify(message="a.bin: exported", severity="information", title=EXPORT_NOTIFY_TITLE)
    assert isinstance(run_export, RunExport)
    assert run_export.job_id == JobId(2)
    assert run_export.group == "job-2"
    assert run_export.unit is unit_b


def test_export_finished_with_nothing_queued_returns_only_the_notify() -> None:
    model = AppModel(jobs={JobId(1): Job(id=JobId(1), label="a.bin", group="job-1")})
    outcome = JobOutcome(notify_message="x", notify_severity="information", status_text="x")
    _, cmds = update(model, ExportFinished(job_id=JobId(1), outcome=outcome))
    assert len(cmds) == 1


def test_cancel_job_requested_for_a_queued_job_removes_it_without_cancel_group() -> None:
    """A QUEUED job never started a worker -- routing it through
    CANCELLING (as a RUNNING job would be) would leave it waiting for an
    ExportFinished that can never arrive."""
    unit = _unit("b.bin")
    model = AppModel(
        jobs={
            JobId(1): Job(id=JobId(1), label="export a.bin", group="job-1"),
            JobId(2): Job(id=JobId(2), label="export b.bin", group="job-2", status=JobStatus.QUEUED),
        },
        queued_requests={JobId(2): QueuedExport(unit=unit, dst=Path("./b.bin"), sparse=True)},
    )

    new_model, cmds = update(model, CancelJobRequested(job_id=JobId(2)))

    assert JobId(2) not in new_model.jobs
    assert JobId(2) not in new_model.queued_requests
    assert JobId(1) in new_model.jobs  # the running job is untouched
    assert len(new_model.recent) == 1
    assert new_model.recent[0].id == JobId(2)
    assert cmds == (
        Notify(message=new_model.recent[0].outcome.notify_message, severity="information", title=EXPORT_NOTIFY_TITLE),
    )


# -- Verify FULL mutual exclusion ------------------------------------------


def test_verify_full_started_sets_the_flag() -> None:
    model = AppModel()
    new_model, cmds = update(model, VerifyFullStarted())
    assert new_model.verify_full_running is True
    assert cmds == ()


def test_verify_full_finished_clears_the_flag_and_promotes_a_queued_export() -> None:
    unit = _unit("a.bin")
    model = AppModel(
        verify_full_running=True,
        jobs={JobId(1): Job(id=JobId(1), label="export a.bin", group="job-1", status=JobStatus.QUEUED)},
        queued_requests={JobId(1): QueuedExport(unit=unit, dst=Path("./a.bin"), sparse=True)},
    )

    new_model, cmds = update(model, VerifyFullFinished())

    assert new_model.verify_full_running is False
    assert new_model.jobs[JobId(1)].status is JobStatus.RUNNING
    assert JobId(1) not in new_model.queued_requests
    assert len(cmds) == 1
    (run_export,) = cmds
    assert isinstance(run_export, RunExport)
    assert run_export.job_id == JobId(1)


def test_verify_full_finished_with_nothing_queued_returns_no_commands() -> None:
    model = AppModel(verify_full_running=True)
    new_model, cmds = update(model, VerifyFullFinished())
    assert new_model.verify_full_running is False
    assert cmds == ()


def test_export_occupied_reflects_running_and_cancelling_but_not_queued() -> None:
    assert AppModel(jobs={JobId(1): Job(id=JobId(1), label="a", group="g")}).export_occupied is True
    assert (
        AppModel(jobs={JobId(1): Job(id=JobId(1), label="a", group="g", status=JobStatus.CANCELLING)}).export_occupied
        is True
    )
    assert (
        AppModel(jobs={JobId(1): Job(id=JobId(1), label="a", group="g", status=JobStatus.QUEUED)}).export_occupied
        is False
    )
    assert AppModel().export_occupied is False
