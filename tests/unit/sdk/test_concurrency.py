"""Unit tests for ``synology_apm_repo.sdk.concurrency``.

``run_in_worker_loop()``/``close_worker_loop()`` are exercised through the
module-private ``_worker_runner`` global in this process, with no real worker.
A real pool's targets here are stdlib functions: a function defined in this
``tests/`` tree can't be pickled as a ``spawn``-context target under pytest's
import mode, so no test observes a worker's ``atexit`` hook firing.
"""

from __future__ import annotations

import asyncio
import errno
import math
import multiprocessing
import os
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import pytest

from synology_apm_repo.sdk import concurrency
from synology_apm_repo.sdk.errors import WorkerProcessError


@pytest.fixture(autouse=True)
def _reset_worker_loop() -> Iterator[None]:
    """Each test starts and ends without a ``_worker_runner``: a real worker
    keeps one for life, but every test here shares one process."""
    assert concurrency._worker_runner is None
    yield
    if concurrency._worker_runner is not None:
        concurrency.close_worker_loop()


def test_preload_resource_tracker_starts_the_multiprocessing_tracker(monkeypatch: pytest.MonkeyPatch) -> None:
    """POSIX-only by contract: the tracker's helper-process launch relies on
    fd inheritance, so this is deliberately a no-op on Windows."""
    calls: list[bool] = []
    monkeypatch.setattr("synology_apm_repo.sdk.concurrency.resource_tracker.ensure_running", lambda: calls.append(True))

    concurrency.preload_resource_tracker()

    assert calls == ([True] if os.name == "posix" else [])


def test_run_in_worker_loop_reuses_the_same_loop_across_calls() -> None:
    async def _capture() -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    first = concurrency.run_in_worker_loop(_capture())
    second = concurrency.run_in_worker_loop(_capture())

    assert first is second


def test_run_in_worker_loop_does_not_close_the_loop_after_returning() -> None:
    async def _noop() -> None:
        return None

    concurrency.run_in_worker_loop(_noop())

    assert concurrency._worker_runner is not None
    assert not concurrency._worker_runner.get_loop().is_closed()


def test_run_in_worker_loop_survives_a_raising_task() -> None:
    async def _boom() -> None:
        raise ValueError("synthetic failure")

    async def _noop() -> None:
        return None

    with pytest.raises(ValueError, match="synthetic failure"):
        concurrency.run_in_worker_loop(_boom())

    # One task raising must not poison the worker's persistent loop.
    concurrency.run_in_worker_loop(_noop())


def test_close_worker_loop_is_a_noop_without_a_loop() -> None:
    concurrency.close_worker_loop()

    assert concurrency._worker_runner is None


def test_close_worker_loop_closes_the_loop() -> None:
    async def _noop() -> None:
        return None

    concurrency.run_in_worker_loop(_noop())
    assert concurrency._worker_runner is not None
    loop = concurrency._worker_runner.get_loop()

    concurrency.close_worker_loop()

    assert loop.is_closed()
    assert concurrency._worker_runner is None


def test_run_in_worker_loop_after_close_starts_a_fresh_loop() -> None:
    async def _noop() -> None:
        return None

    concurrency.run_in_worker_loop(_noop())
    assert concurrency._worker_runner is not None
    first_loop = concurrency._worker_runner.get_loop()
    concurrency.close_worker_loop()

    concurrency.run_in_worker_loop(_noop())
    assert concurrency._worker_runner is not None
    second_loop = concurrency._worker_runner.get_loop()

    assert second_loop is not first_loop


async def test_bounded_gather_runs_worker_for_every_item() -> None:
    seen: list[int] = []

    async def worker(item: int) -> None:
        seen.append(item)

    await concurrency.bounded_gather(range(5), worker, max_concurrent=2)

    assert sorted(seen) == [0, 1, 2, 3, 4]


async def test_bounded_gather_never_exceeds_max_concurrent_in_flight() -> None:
    in_flight = 0
    max_seen = 0

    async def worker(item: int) -> None:
        nonlocal in_flight, max_seen
        in_flight += 1
        max_seen = max(max_seen, in_flight)
        await asyncio.sleep(0)
        in_flight -= 1

    await concurrency.bounded_gather(range(10), worker, max_concurrent=3)

    assert max_seen <= 3
    assert in_flight == 0


async def test_bounded_gather_on_done_runs_after_the_semaphore_is_released() -> None:
    """``on_done`` doesn't count against ``max_concurrent``: with a limit of 1,
    an ``on_done`` that blocks until the second ``worker`` starts must not
    deadlock."""
    worker_starts = 0
    second_worker_started = asyncio.Event()

    async def worker(item: int) -> None:
        nonlocal worker_starts
        worker_starts += 1
        if worker_starts == 2:
            second_worker_started.set()

    async def on_done(item: int) -> None:
        if item == 0:
            await asyncio.wait_for(second_worker_started.wait(), timeout=1.0)

    await concurrency.bounded_gather(range(2), worker, max_concurrent=1, on_done=on_done)

    assert worker_starts == 2


async def test_bounded_gather_propagates_a_worker_exception() -> None:
    async def worker(item: int) -> None:
        if item == 2:
            raise ValueError("boom")

    with pytest.raises(ExceptionGroup, match="unhandled errors in a TaskGroup") as exc_info:
        await concurrency.bounded_gather(range(5), worker, max_concurrent=2)
    assert isinstance(exc_info.value.exceptions[0], ValueError)


@pytest.mark.parametrize(("cpus", "expected"), [(None, 1), (1, 1), (2, 1), (3, 1), (8, 4), (16, 8), (64, 8)])
def test_default_worker_count_is_half_the_cpus_clamped_to_one_through_eight(
    monkeypatch: pytest.MonkeyPatch, cpus: int | None, expected: int
) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: cpus)

    assert concurrency.default_worker_count() == expected


async def test_dispatch_to_pool_returns_every_result_and_reports_each_to_on_result() -> None:
    seen: list[tuple[int, int]] = []

    async def on_result(item: int, result: int) -> None:
        seen.append((item, result))

    with ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context("spawn")) as pool:
        results = await concurrency.dispatch_to_pool(
            pool, math.factorial, [3, 4, 5], max_concurrent=2, on_result=on_result
        )

    assert sorted(results) == [6, 24, 120]  # completion order, so compared sorted
    assert sorted(seen) == [(3, 6), (4, 24), (5, 120)]


async def test_dispatch_to_pool_surfaces_a_dead_worker_as_a_group_holding_broken_process_pool() -> None:
    with (
        ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn")) as pool,
        pytest.raises(ExceptionGroup, match="unhandled errors in a TaskGroup") as excinfo,
    ):
        await concurrency.dispatch_to_pool(pool, os._exit, [1], max_concurrent=1)

    assert excinfo.group_contains(BrokenProcessPool)


async def test_cancelling_dispatch_to_pool_returns_promptly_and_the_pool_still_shuts_down() -> None:
    """Each item parks its worker in a two-party ``Barrier.wait`` that only
    this test's own ``wait`` releases; ``n_waiting`` shows when one is in flight."""
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager:
        barrier = manager.Barrier(2)
        pool = ProcessPoolExecutor(max_workers=1, mp_context=ctx)
        task = asyncio.create_task(concurrency.dispatch_to_pool(pool, barrier.wait, [30.0, 30.0], max_concurrent=1))
        async with asyncio.timeout(30):
            # A worker process takes a moment to start; a cross-process proxy has no event to await.
            while barrier.n_waiting != 1:  # noqa: ASYNC110
                await asyncio.sleep(0.01)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert barrier.n_waiting == 1  # the cancel didn't wait for the in-flight item

        await asyncio.to_thread(barrier.wait, 10)  # releases the in-flight item
        await asyncio.wait_for(asyncio.to_thread(pool.shutdown, wait=True, cancel_futures=True), 20)
        # The queued item was never started: it would be parked in the barrier now.
        assert barrier.n_waiting == 0


def test_first_failure_raises_the_first_real_failure_and_notes_the_others() -> None:
    first, second = OSError(errno.ENOSPC, "full"), ValueError("later")
    group = BaseExceptionGroup("g", [asyncio.CancelledError(), BaseExceptionGroup("inner", [first, second])])

    raised = concurrency.first_failure(group, operation="export")

    assert raised is first
    assert raised.__notes__ == ["1 other export worker failure: ValueError"]


def test_first_failure_leaves_a_group_of_only_cancellations_alone() -> None:
    group = BaseExceptionGroup("g", [asyncio.CancelledError()])

    assert concurrency.first_failure(group, operation="export") is group


def test_raise_worker_failure_maps_a_dead_worker_to_worker_process_error() -> None:
    died = BrokenProcessPool("gone")
    with pytest.raises(WorkerProcessError, match="verify worker process died") as excinfo:
        concurrency.raise_worker_failure(BaseExceptionGroup("g", [died]), operation="verify")
    assert excinfo.value.__cause__ is died


def test_raise_worker_failure_raises_any_other_failure_itself() -> None:
    failure = ValueError("bad bucket")
    with pytest.raises(ValueError, match="bad bucket"):
        concurrency.raise_worker_failure(BaseExceptionGroup("g", [failure]), operation="verify")


@pytest.mark.parametrize(
    ("descriptors", "expected"),
    [([], None), (["a", "a"], "a"), (["a", "b"], None), ([None, None], None), (["a", None], None)],
)
def test_common_descriptor_is_the_one_shared_value(descriptors: list[str | None], expected: str | None) -> None:
    assert concurrency.common_descriptor(descriptors) == expected


async def test_shutdown_pool_shuts_the_executor_down() -> None:
    executor = concurrency.new_process_pool()
    await concurrency.shutdown_pool(executor)
    with pytest.raises(RuntimeError, match="cannot schedule new futures after shutdown"):
        executor.submit(os.getpid)
