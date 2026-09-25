"""Unit tests for ``browser.runtime.app_effects.AppEffects`` — driven
against a bare ``App`` (``run_worker``/``workers.cancel_group``/
``notify`` all need a real, running App; nothing here needs
``ApmRepoBrowserApp`` specifically) and a real ``Store`` running the
real ``core.app.update``, so this proves the whole round-trip (dispatch
-> update -> perform -> real worker -> dispatch back) works, not just
that ``AppEffects`` calls *some* dispatch.

Fake ``ContentSource``s duplicate ``test_browser_export_screen.py``'s
own ``_BlockingContentSource`` shape rather than importing it."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.worker import Worker

from synology_apm_repo.browser.core.app.cmd import AppCmd, CancelGroup, Notify
from synology_apm_repo.browser.core.app.model import AppModel
from synology_apm_repo.browser.core.app.msg import AppMsg, CancelJobRequested, StartExport
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.runtime.app_effects import AppEffects
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.sdk.api import ExportResult
from synology_apm_repo.sdk.units.base import RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        return iter(())


def _make_store(app: App[None]) -> Store[AppModel, AppMsg, AppCmd]:
    """Ties a real ``Store`` to a real ``AppEffects`` -- the two need each
    other at construction time, resolved via ordinary closure
    late-binding (``_perform`` isn't called until after ``effects`` is
    assigned), the same trick ``AppEffects._run_export``'s own
    ``on_meter_update``/``meter`` pair already relies on."""
    effects: AppEffects

    def _perform(cmd: AppCmd) -> None:
        effects.perform(cmd)

    store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, _perform)
    effects = AppEffects(app, store)
    return store


class _InstantContentSource:
    """A fake ``ContentSource`` whose ``export_to`` reports one progress
    tick then returns immediately — no real I/O, deterministic."""

    size = 100
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: Any = None) -> ExportResult:
        if progress is not None:
            await progress(50, 100)
        dst.write_bytes(b"x" * 50)
        return ExportResult(bytes_written=50, logical_size=100, holes=0, zeros=50)


class _FailingContentSource:
    size = 10
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: Any = None) -> ExportResult:
        raise ValueError("boom")


class _BlockingContentSource:
    """Never completes on its own, only via cancellation, so a test can
    deterministically catch it mid-export."""

    size = 4096
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: Any = None) -> ExportResult:
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


class _ProgressThenBlockContentSource:
    """Reports one real progress tick (so a test can observe it actually
    reaching the model), then blocks forever like
    ``_BlockingContentSource`` above — needed because
    ``_InstantContentSource`` completes before a test could ever inspect
    its mid-export state."""

    size = 100
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: Any = None) -> ExportResult:
        if progress is not None:
            await progress(50, 100)
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


class _TwoTickThenBlockContentSource:
    """Reports two progress ticks a real ~0.25s apart (matching
    ``ProgressMeter``'s own default ``rate_sample_interval=0.2``) before
    blocking — the only way to genuinely exercise ``rate > 0``/``eta is
    not None`` in ``on_meter_update``: both are computed from *real*
    elapsed wall-clock time between samples, not from a single instant
    tick. Deliberate interval sampling -- a real ``asyncio.sleep``, not a
    poll-and-wait, because there is no state to poll for in between: the
    meter's own smoothing genuinely needs that time to pass."""

    size = 100
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: Any = None) -> ExportResult:
        if progress is not None:
            await progress(10, 100)
            await asyncio.sleep(0.25)
            await progress(60, 100)
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


class _WritesPartialThenBlockContentSource:
    """Writes its destination (``export_to``'s own first argument is
    ``part_path`` — see ``AppEffects._run_export``'s own call site) then
    blocks — simulates a real ``export_to()`` that had already written
    some data before being cancelled, so cancellation has a real,
    on-disk partial file to remove (as opposed to
    ``_BlockingContentSource``'s "nothing was ever written" case, where
    the same ``unlink(missing_ok=True)`` call is still a safe no-op)."""

    size = 100
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: Any = None) -> ExportResult:
        dst.write_bytes(b"partial")
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


def _unit(content: object, name: str = "item.bin") -> RestorableUnit:
    return RestorableUnit(ref=NodeRef("repo", (name,)), name=name, is_leaf=True, content=content)  # type: ignore[arg-type]


async def test_perform_notify_calls_app_notify() -> None:
    app = _FakeApp()
    async with app.run_test():
        calls: list[tuple[str, str, str]] = []
        app.notify = lambda message, *, title="", severity="information", **kw: calls.append(  # type: ignore[method-assign]
            (message, title, severity)
        )
        # Neither test in this pair dispatches through a real Store --
        # perform() is called directly, so a throwaway store (its own
        # `perform` never runs) is enough.
        store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, lambda cmd: None)
        effects = AppEffects(app, store)

        effects.perform(Notify(message="hello", severity="warning", title="Title"))

        assert calls == [("hello", "Title", "warning")]


async def test_perform_cancel_group_calls_workers_cancel_group() -> None:
    app = _FakeApp()
    async with app.run_test():
        calls: list[tuple[object, str]] = []

        def _fake_cancel_group(node: object, group: str) -> list[Worker[None]]:
            calls.append((node, group))
            return []

        app.workers.cancel_group = _fake_cancel_group  # type: ignore[method-assign]
        store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, lambda cmd: None)
        effects = AppEffects(app, store)

        effects.perform(CancelGroup(group="job-1"))

        assert calls == [(app, "job-1")]


async def test_run_export_success_updates_the_model_and_writes_the_notify(
    tmp_path: Path, wait_until: Any, sdk_timeout: float
) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_InstantContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(unit=unit, dst_text=str(dst), sparse=True))
        assert len(store.model.jobs) == 1

        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)

        assert dst.exists()
        assert len(store.model.recent) == 1
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "information"
        assert "exported" in outcome.notify_message
        assert "[green]done[/green]" in outcome.status_text


async def test_run_export_refuses_an_existing_destination_without_touching_it(
    tmp_path: Path, wait_until: Any, sdk_timeout: float
) -> None:
    """Mirrors the CLI's own ``test_existing_destination_is_refused_without_force``
    -- this UI has no ``--force`` equivalent, so the refusal is
    unconditional."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_InstantContentSource())
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"original content")

        store.dispatch(StartExport(unit=unit, dst_text=str(dst), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)

        assert len(store.model.recent) == 1
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "error"
        assert "already exists" in outcome.notify_message
        assert dst.read_bytes() == b"original content"
        assert not (tmp_path / "out.bin.part").exists()


async def test_run_export_creates_missing_parent_directories(
    tmp_path: Path, wait_until: Any, sdk_timeout: float
) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_InstantContentSource())
        dst = tmp_path / "does" / "not" / "exist" / "out.bin"
        assert not dst.parent.exists()

        store.dispatch(StartExport(unit=unit, dst_text=str(dst), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)

        assert dst.exists()
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "information"


async def test_run_export_progress_ticks_reach_the_model(tmp_path: Path, wait_until: Any, sdk_timeout: float) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_ProgressThenBlockContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(unit=unit, dst_text=str(dst), sparse=True))
        job_id = next(iter(store.model.jobs))

        await wait_until(pilot, lambda: store.model.jobs[job_id].done == 50, timeout=sdk_timeout, interval=0.02)
        job = store.model.jobs[job_id]
        assert job.total == 100
        assert job.percent == 50
        assert job.elapsed_text != ""  # rate/ETA/size/elapsed text, formatted by the effect

        app.workers.cancel_group(app, job.group)
        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)
        assert store.model.recent[0].outcome.notify_severity == "warning"


async def test_run_export_failure_dispatches_an_error_outcome(
    tmp_path: Path, wait_until: Any, sdk_timeout: float
) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_FailingContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(unit=unit, dst_text=str(dst), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)

        assert len(store.model.recent) == 1
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "error"
        assert "boom" in outcome.notify_message
        assert "[red]error:[/red]" in outcome.status_text


async def test_run_export_cancellation_before_any_write_still_reports_removed(
    tmp_path: Path, wait_until: Any, sdk_timeout: float
) -> None:
    """``CancelJobRequested`` dispatched through the store (not a direct
    ``app.workers.cancel_group`` call, unlike the progress-ticks test
    above) -- proves the full round-trip: dispatch -> update() marks
    CANCELLING and emits CancelGroup -> AppEffects.perform cancels the
    real worker -> the worker's own CancelledError handler dispatches
    ExportFinished back."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_BlockingContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(unit=unit, dst_text=str(dst), sparse=True))
        job_id = next(iter(store.model.jobs))
        # Give the event loop a turn so run_worker's task actually starts
        # (reaches _BlockingContentSource.export_to's own await point)
        # before cancelling it -- cancelling a task that hasn't been
        # scheduled to run at all yet is a real race, not something this
        # test should paper over with a longer timeout.
        await pilot.pause()

        store.dispatch(CancelJobRequested(job_id=job_id))
        assert store.model.jobs[job_id].status.value == "cancelling"

        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)
        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        # keep_partial=False (this worker's default, matching the CLI's
        # own export command -- sdk.presentation.export_target's shared
        # resolve_cancelled_partial()) always reports removal without
        # checking whether anything was actually written, the same
        # "nothing left behind either way" reasoning the CLI's own
        # test_default_is_safe_even_if_the_partial_file_never_existed
        # documents.
        assert "partial file removed" in outcome.notify_message
        assert not dst.exists()


async def test_run_export_progress_includes_rate_and_eta_once_warmed_up(
    tmp_path: Path, wait_until: Any, sdk_timeout: float
) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_TwoTickThenBlockContentSource())
        dst = tmp_path / "out.bin"

        store.dispatch(StartExport(unit=unit, dst_text=str(dst), sparse=True))
        job_id = next(iter(store.model.jobs))

        await wait_until(pilot, lambda: store.model.jobs[job_id].done == 60, timeout=sdk_timeout, interval=0.02)
        job = store.model.jobs[job_id]
        assert "/s" in job.rate_text
        assert job.eta_text != ""

        # Cleanup: cancel so the still-blocked worker doesn't outlive the
        # test (run_test()'s own teardown would otherwise wait on it).
        store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)


async def test_run_export_cancellation_after_a_partial_write_removes_the_part_file(
    tmp_path: Path, wait_until: Any, sdk_timeout: float
) -> None:
    """``keep_partial=False`` removes a cancelled export's partial file,
    matching the CLI's own ``export`` command's default; there is no
    ``--keep-partial`` equivalent in this UI to opt out of that."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = _unit(_WritesPartialThenBlockContentSource())
        dst = tmp_path / "out.bin"
        part_path = tmp_path / "out.bin.part"

        store.dispatch(StartExport(unit=unit, dst_text=str(dst), sparse=True))
        job_id = next(iter(store.model.jobs))
        await wait_until(pilot, lambda: part_path.exists(), timeout=sdk_timeout, interval=0.02)

        store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        assert "partial file removed" in outcome.notify_message
        assert not part_path.exists()


async def test_a_promoted_queued_export_actually_runs_to_completion(
    tmp_path: Path, wait_until: Any, sdk_timeout: float
) -> None:
    """The real "two jobs, first ends, second promoted" round-trip
    through ``update()``'s own ``_promote_next_queued`` and back into
    ``AppEffects.perform()``'s ``RunExport`` case -- every existing
    two-job test only ever cancels the *queued* job itself
    (``test_browser_export_screen.py``'s
    ``test_export_screen_cancelling_a_queued_export_shows_cancelled_not_cancelling``);
    none lets the promoted job's own ``RunExport`` actually execute
    through to a real, written destination file."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app)
        first_unit = _unit(_BlockingContentSource(), name="first.bin")
        second_unit = _unit(_InstantContentSource(), name="second.bin")
        first_dst = tmp_path / "first.bin"
        second_dst = tmp_path / "second.bin"

        store.dispatch(StartExport(unit=first_unit, dst_text=str(first_dst), sparse=True))
        first_job_id = next(iter(store.model.jobs))
        await pilot.pause()  # let the first worker's task actually start before queuing the second

        store.dispatch(StartExport(unit=second_unit, dst_text=str(second_dst), sparse=True))
        assert len(store.model.jobs) == 2
        second_job_id = next(jid for jid in store.model.jobs if jid != first_job_id)
        assert store.model.jobs[second_job_id].status.value == "queued"

        store.dispatch(CancelJobRequested(job_id=first_job_id))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=sdk_timeout, interval=0.02)

        assert second_dst.exists()
        finished = {f.id: f.outcome for f in store.model.recent}
        assert second_job_id in finished
        assert finished[second_job_id].notify_severity == "information"
        assert "exported" in finished[second_job_id].notify_message
