"""Unit tests for ``browser.runtime.app_effects.AppEffects``, driven
against a bare running ``App`` and a real ``Store`` running the real
``core.app.update``, so each test covers the whole round-trip: dispatch
-> update -> perform -> real worker -> dispatch back."""

from __future__ import annotations

import asyncio
import errno
import functools
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from textual.app import App
from textual.worker import Worker

from support.content_fakes import BlockingContentSource
from support.fakes import faithful_to
from support.pilot import SDK_TIMEOUT, wait_until
from synology_apm_repo.browser.core.app.cmd import AppCmd, CancelGroup, Notify
from synology_apm_repo.browser.core.app.model import AppModel
from synology_apm_repo.browser.core.app.msg import AppMsg, CancelJobRequested, StartExport
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.runtime import app_effects
from synology_apm_repo.browser.runtime.app_effects import AppEffects
from synology_apm_repo.browser.runtime.load_gate import LoadGate
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.sdk.export import ExportResult, ExportWriter, LocalFileSink
from synology_apm_repo.sdk.presentation import ProgressMeter
from synology_apm_repo.sdk.units.base import ContentSource, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _make_store(app: App[None]) -> Store[AppModel, AppMsg, AppCmd]:
    """A real ``Store`` wired to a real ``AppEffects``; ``_perform`` late-binds
    ``effects``, which needs the store at construction."""
    effects: AppEffects

    def _perform(cmd: AppCmd) -> None:
        effects.perform(cmd)

    store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, _perform)
    effects = AppEffects(app, store, LoadGate())
    return store


@faithful_to(ContentSource)
class _InstantContentSource:
    """Reports one progress tick, writes, and returns."""

    size = 100

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: Any = None,
        tuning: object = None,
    ) -> ExportResult:
        if progress is not None:
            await progress(50)
        await sink.write_at(0, b"x" * 50)
        return ExportResult(bytes_written=50, logical_size=100, holes=0, zeros=50)


@faithful_to(ContentSource)
class _FailingContentSource:
    """Raises ``ValueError`` from the export."""

    size = 10

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: Any = None,
        tuning: object = None,
    ) -> ExportResult:
        raise ValueError("boom")


@faithful_to(ContentSource)
class _OutOfSpaceContentSource:
    """Writes some bytes, then the destination runs out of room."""

    size = 100

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: Any = None,
        tuning: object = None,
    ) -> ExportResult:
        await sink.write_at(0, b"x" * 10)
        raise OSError(errno.ENOSPC, "No space left on device")


@faithful_to(ContentSource)
class _ProgressThenBlockContentSource:
    """Reports one progress tick, then blocks until cancelled."""

    size = 100

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: Any = None,
        tuning: object = None,
    ) -> ExportResult:
        if progress is not None:
            await progress(50)
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


@faithful_to(ContentSource)
class _TwoTickThenBlockContentSource:
    """Reports two progress ticks 0.25 s apart on ``clock`` (past
    ``ProgressMeter``'s default ``rate_sample_interval=0.2``), then blocks
    until cancelled; the test hands ``clock`` to the meter as its ``now``."""

    size = 100

    def __init__(self) -> None:
        self.clock = 0.0

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: Any = None,
        tuning: object = None,
    ) -> ExportResult:
        if progress is not None:
            await progress(10)
            self.clock += 0.25
            await progress(50)
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


@faithful_to(ContentSource)
class _WritesPartialThenBlockContentSource:
    """Writes some data to the sink, then blocks until cancelled, so
    cancellation has a real partial file to remove."""

    size = 100

    def __init__(self) -> None:
        self.written = asyncio.Event()

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: Any = None,
        tuning: object = None,
    ) -> ExportResult:
        await sink.write_at(0, b"partial")
        self.written.set()
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


def _unit(content: object, name: str = "item.bin") -> RestorableUnit:
    return RestorableUnit(ref=NodeRef("repo", (name,)), name=name, is_leaf=True, content=content)  # type: ignore[arg-type]


async def test_perform_notify_calls_app_notify(monkeypatch: pytest.MonkeyPatch) -> None:
    app = App[None]()
    async with app.run_test():
        calls: list[tuple[str, str, str]] = []
        monkeypatch.setattr(
            app,
            "notify",
            lambda message, *, title="", severity="information", **kw: calls.append((message, title, severity)),
        )
        # perform() is called directly, so a throwaway store suffices.
        store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, lambda cmd: None)
        effects = AppEffects(app, store, LoadGate())

        effects.perform(Notify(message="hello", severity="warning", title="Title"))

        assert calls == [("hello", "Title", "warning")]


async def test_perform_cancel_group_calls_workers_cancel_group(monkeypatch: pytest.MonkeyPatch) -> None:
    app = App[None]()
    async with app.run_test():
        calls: list[tuple[object, str]] = []

        def _fake_cancel_group(node: object, group: str) -> list[Worker[None]]:
            calls.append((node, group))
            return []

        monkeypatch.setattr(app.workers, "cancel_group", _fake_cancel_group)
        store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, lambda cmd: None)
        effects = AppEffects(app, store, LoadGate())

        effects.perform(CancelGroup(group="job-1"))

        assert calls == [(app, "job-1")]


async def test_run_export_success_updates_the_model_and_writes_the_notify(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_InstantContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        assert len(store.model.jobs) == 1

        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        assert dst.exists()
        assert len(store.model.recent) == 1
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "information"
        assert "exported" in outcome.notify_message
        assert "[green]done[/green]" in outcome.status_text


async def test_run_export_refuses_an_existing_destination_without_touching_it(
    tmp_path: Path,
) -> None:
    """An existing destination is refused unconditionally (this UI has no
    ``--force``) and left untouched."""
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_InstantContentSource())
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"original content")

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        assert len(store.model.recent) == 1
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "error"
        assert "already exists" in outcome.notify_message
        assert dst.read_bytes() == b"original content"
        assert not (tmp_path / "out.bin.part").exists()


async def test_run_export_creates_missing_parent_directories(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_InstantContentSource())
        dst = tmp_path / "does" / "not" / "exist" / "out.bin"
        assert not dst.parent.exists()

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        assert dst.exists()
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "information"


async def test_run_export_progress_ticks_reach_the_model(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_ProgressThenBlockContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        job_id = next(iter(store.model.jobs))

        await wait_until(pilot, lambda: store.model.jobs[job_id].done == 50, timeout=SDK_TIMEOUT, interval=0.02)
        job = store.model.jobs[job_id]
        assert job.total == 100
        assert job.percent == 50
        assert job.elapsed_text != ""  # formatted by the effect from its ProgressMeter

        app.workers.cancel_group(app, job.group)
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)
        assert store.model.recent[0].outcome.notify_severity == "warning"


async def test_run_export_failure_dispatches_an_error_outcome(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_FailingContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        assert len(store.model.recent) == 1
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "error"
        assert "boom" in outcome.notify_message
        assert "[red]error:[/red]" in outcome.status_text


async def test_run_export_out_of_space_reports_the_shortage_and_what_was_left(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_OutOfSpaceContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "error"
        assert "not enough free space to write" in outcome.notify_message
        assert "No space left on device" in outcome.notify_message
        assert "partial file removed" in outcome.notify_message
        assert "[red]error:[/red]" in outcome.status_text
        assert not (tmp_path / "out.bin.part").exists()


async def test_run_export_cancellation_before_any_write_reports_that_nothing_was_written(
    tmp_path: Path,
) -> None:
    """Cancelling through the store marks the job CANCELLING, cancels the
    worker, and reports that nothing was ever written."""
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        source = BlockingContentSource()
        unit = _unit(source)
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        job_id = next(iter(store.model.jobs))
        await wait_until(pilot, lambda: source.started, timeout=SDK_TIMEOUT, interval=0.02)

        store.dispatch(CancelJobRequested(job_id=job_id))
        assert store.model.jobs[job_id].status.value == "cancelling"

        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        assert "no output file was ever written" in outcome.notify_message
        assert not dst.exists()


async def test_a_second_cancel_while_the_cancelled_export_is_reporting_still_finishes_the_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pressing cancel again while the first cancel is still being reported must not leave the job
    CANCELLING forever: the worker's ``ExportFinished`` is sent whatever interrupts its wrap-up."""
    real_abort = LocalFileSink.abort
    calls = 0
    reporting = asyncio.Event()

    async def abort_that_stalls_on_the_second_call(self: LocalFileSink) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:  # the worker's own wrap-up, after run_export's abort
            reporting.set()
            await asyncio.Event().wait()
        return await real_abort(self)

    monkeypatch.setattr(LocalFileSink, "abort", abort_that_stalls_on_the_second_call)
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        source = BlockingContentSource()
        store.dispatch(StartExport(target=_unit(source), dst_text=str(tmp_path / "out.bin"), sparse=True))
        job_id = next(iter(store.model.jobs))
        await wait_until(pilot, lambda: source.started, timeout=SDK_TIMEOUT, interval=0.02)

        store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, reporting.is_set, timeout=SDK_TIMEOUT, interval=0.02)
        store.dispatch(CancelJobRequested(job_id=job_id))  # a second press, mid-report

        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        assert "cancelled" in outcome.notify_message


async def test_a_cancel_while_a_full_disk_failure_is_being_reported_still_finishes_the_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_abort = LocalFileSink.abort
    calls = 0
    reporting = asyncio.Event()

    async def abort_that_stalls_on_the_second_call(self: LocalFileSink) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:  # the worker's own out-of-space report, after run_export's abort
            reporting.set()
            await asyncio.Event().wait()
        return await real_abort(self)

    monkeypatch.setattr(LocalFileSink, "abort", abort_that_stalls_on_the_second_call)
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_OutOfSpaceContentSource())
        store.dispatch(StartExport(target=unit, dst_text=str(tmp_path / "out.bin"), sparse=True))
        job_id = next(iter(store.model.jobs))
        await wait_until(pilot, reporting.is_set, timeout=SDK_TIMEOUT, interval=0.02)

        store.dispatch(CancelJobRequested(job_id=job_id))

        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "error"
        assert "No space left on device" in outcome.notify_message


@pytest.mark.parametrize("cancel", [True, False], ids=["cancelled", "full-disk"])
async def test_a_failing_abort_while_reporting_an_export_does_not_crash_the_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancel: bool
) -> None:
    """The report needs ``abort()``'s outcome; if that itself raises, the job is still finished
    (without the leftover note) and the error does not escape the worker."""
    real_abort = LocalFileSink.abort
    calls = 0

    async def abort_that_fails_on_the_second_call(self: LocalFileSink) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:  # the worker's own wrap-up, after run_export's abort
            raise PermissionError(errno.EACCES, "cannot remove the partial file")
        return await real_abort(self)

    monkeypatch.setattr(LocalFileSink, "abort", abort_that_fails_on_the_second_call)
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        source = BlockingContentSource() if cancel else _OutOfSpaceContentSource()
        store.dispatch(StartExport(target=_unit(source), dst_text=str(tmp_path / "out.bin"), sparse=True))
        job_id = next(iter(store.model.jobs))
        if cancel:
            await wait_until(pilot, lambda: getattr(source, "started", False), timeout=SDK_TIMEOUT, interval=0.02)
            store.dispatch(CancelJobRequested(job_id=job_id))

        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == ("warning" if cancel else "error")
        assert app.return_code in (None, 0)  # nothing escaped the worker and ended the app


async def test_run_export_progress_includes_rate_and_eta_once_warmed_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = _TwoTickThenBlockContentSource()
    monkeypatch.setattr(app_effects, "ProgressMeter", functools.partial(ProgressMeter, now=lambda: content.clock))
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(content)
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        job_id = next(iter(store.model.jobs))

        await wait_until(pilot, lambda: store.model.jobs[job_id].done == 60, timeout=SDK_TIMEOUT, interval=0.02)
        job = store.model.jobs[job_id]
        assert "/s" in job.rate_text
        assert job.eta_text != ""

        # Cleanup: cancel the blocked worker.
        store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)


async def test_run_export_cancellation_after_a_partial_write_removes_the_part_file(
    tmp_path: Path,
) -> None:
    """Cancelling removes the partial file (this UI has no
    ``--keep-partial``)."""
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        source = _WritesPartialThenBlockContentSource()
        unit = _unit(source)
        dst = tmp_path / "out.bin"
        part_path = tmp_path / "out.bin.part"

        store.dispatch(StartExport(target=unit, dst_text=str(dst), sparse=True))
        job_id = next(iter(store.model.jobs))
        await wait_until(pilot, lambda: source.written.is_set(), timeout=SDK_TIMEOUT, interval=0.02)

        store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        assert "partial file removed" in outcome.notify_message
        assert not part_path.exists()


async def test_a_promoted_queued_export_actually_runs_to_completion(
    tmp_path: Path,
) -> None:
    """When the first job ends, the queued second job is promoted and its
    ``RunExport`` executes through to a written destination file."""
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        first_source = BlockingContentSource()
        first_unit = _unit(first_source, name="first.bin")
        second_unit = _unit(_InstantContentSource(), name="second.bin")
        first_dst = tmp_path / "first.bin"
        second_dst = tmp_path / "second.bin"

        store.dispatch(StartExport(target=first_unit, dst_text=str(first_dst), sparse=True))
        first_job_id = next(iter(store.model.jobs))
        await wait_until(pilot, lambda: first_source.started, timeout=SDK_TIMEOUT, interval=0.02)

        store.dispatch(StartExport(target=second_unit, dst_text=str(second_dst), sparse=True))
        assert len(store.model.jobs) == 2
        second_job_id = next(jid for jid in store.model.jobs if jid != first_job_id)
        assert store.model.jobs[second_job_id].status.value == "queued"

        store.dispatch(CancelJobRequested(job_id=first_job_id))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        assert second_dst.exists()
        finished = {f.id: f.outcome for f in store.model.recent}
        assert second_job_id in finished
        assert finished[second_job_id].notify_severity == "information"
        assert "exported" in finished[second_job_id].notify_message
