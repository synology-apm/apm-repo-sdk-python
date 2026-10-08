"""Unit tests for ``widgets/worker_progress.py``'s ``work``/
``run_worker_with_progress``/``run_worker_no_progress``, which show a
``DebouncedProgress`` while a worker runs unless opted out. The debounce is
shortened by this directory's autouse ``fast_browser_debounce``.
"""

from __future__ import annotations

import asyncio

import pytest
from textual.app import App, ComposeResult
from textual.widgets import Static

from support.fakes import faithful_to
from support.pilot import UI_TIMEOUT, wait_until
from synology_apm_repo.browser.widgets import progress_hint
from synology_apm_repo.browser.widgets.progress_hint import LoadingSink, StaticTextSink
from synology_apm_repo.browser.widgets.worker_progress import run_worker_no_progress, run_worker_with_progress, work


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        yield Static("", id="status")


@faithful_to(LoadingSink)
class _RecordingSink:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def show(self, frame: str) -> None:
        self.calls.append(frame)

    def hide(self) -> None:
        self.calls.append("hide")


async def test_work_shows_its_sink_after_the_debounce_delay() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        recorded = _RecordingSink()

        @work(sink=lambda self: recorded)
        async def _slow(self: App[None]) -> None:
            await asyncio.Event().wait()  # never resolves on its own -- cancelled below

        worker = _slow(app)
        await wait_until(pilot, lambda: recorded.calls, timeout=UI_TIMEOUT, message="sink was never shown")
        worker.cancel()


async def test_work_busy_false_never_shows_anything_even_past_the_delay() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        recorded = _RecordingSink()

        control = _RecordingSink()

        @work(busy=False, sink=lambda self: recorded)
        async def _fast(self: App[None]) -> None:
            await asyncio.Event().wait()

        @work(sink=lambda self: control)
        async def _control(self: App[None]) -> None:
            await asyncio.Event().wait()

        worker = _fast(app)
        control_worker = _control(app)
        # The control, started after it, has shown: past the delay a busy worker's sink would have.
        await wait_until(pilot, lambda: control.calls, timeout=UI_TIMEOUT, message="the control sink was never shown")
        assert recorded.calls == []
        worker.cancel()
        control_worker.cancel()


async def test_work_uses_the_given_sink_and_hides_it_once_the_worker_finishes() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        release = asyncio.Event()

        # Held open by `release`, so "Loading" stays up until the test has seen it.
        @work(sink=lambda self: StaticTextSink(self, "#status", base=lambda: "base"))
        async def _slow(self: App[None]) -> None:
            await release.wait()

        _slow(app)
        await wait_until(
            pilot,
            lambda: "Loading" in str(app.query_one("#status", Static).render()),
            timeout=UI_TIMEOUT,
            message="the sink never showed",
        )
        release.set()
        await wait_until(
            pilot,
            lambda: str(app.query_one("#status", Static).render()) == "base",
            timeout=UI_TIMEOUT,
            message="the sink never hid once the worker finished",
        )


def test_work_rejects_thread_true_at_decoration_time() -> None:
    with pytest.raises(ValueError, match="thread=True"):

        @work(thread=True)
        async def _threaded(self: App[None]) -> None:
            pass


async def test_work_forwards_kwargs_to_textual_work() -> None:
    app = _FakeApp()
    async with app.run_test():

        @work(group="my-group", name="my-worker", busy=False)
        async def _slow(self: App[None]) -> None:
            await asyncio.Event().wait()

        worker = _slow(app)
        assert worker.group == "my-group"
        assert worker.name == "my-worker"
        worker.cancel()


async def test_run_worker_with_progress_shows_the_given_sink() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        recorded = _RecordingSink()

        async def _slow() -> None:
            await asyncio.Event().wait()

        worker = run_worker_with_progress(app, _slow, sink=recorded)
        await wait_until(pilot, lambda: recorded.calls, timeout=UI_TIMEOUT, message="sink was never shown")
        worker.cancel()


async def test_run_worker_no_progress_never_shows_anything(monkeypatch: pytest.MonkeyPatch) -> None:
    ticked_sinks: list[LoadingSink] = []
    original_tick = progress_hint.DebouncedProgress._tick

    def recording_tick(self: progress_hint.DebouncedProgress) -> None:
        ticked_sinks.append(self._sink)
        original_tick(self)

    monkeypatch.setattr(progress_hint.DebouncedProgress, "_tick", recording_tick)
    app = _FakeApp()
    async with app.run_test() as pilot:
        control = _RecordingSink()
        started = asyncio.Event()

        async def _held() -> None:
            started.set()
            await asyncio.Event().wait()

        async def _slow() -> None:
            await asyncio.Event().wait()

        worker = run_worker_no_progress(app, _held)
        await wait_until(pilot, started.is_set, message="the no-progress worker never started")
        control_worker = run_worker_with_progress(app, _slow, sink=control)
        # The control, started after it, has shown: past the delay any
        # progress the no-progress worker armed would have ticked too.
        await wait_until(pilot, lambda: control.calls, timeout=UI_TIMEOUT, message="the control sink was never shown")
        assert ticked_sinks and all(sink is control for sink in ticked_sinks)
        worker.cancel()
        control_worker.cancel()
