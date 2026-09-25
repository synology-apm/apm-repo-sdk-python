"""Tests for ``sdk/concurrency.py``.

``preload_resource_tracker()``'s own test is mock-based, no real process
spawn needed, since the whole contract is "delegates to
``multiprocessing.resource_tracker.ensure_running()``, now, with whatever
``sys.stderr`` currently is" -- capturing "now" matters since the TUI later
replaces ``sys.stderr`` with something whose ``fileno()`` can't be trusted.

``run_in_worker_loop()``/``close_worker_loop()``'s own tests exercise the
module-private ``_worker_runner`` global directly, in-process — no real
``ProcessPoolExecutor`` needed to prove the loop-lifetime contract itself.
They don't re-test ``asyncio.Runner.close()``'s own teardown behavior
(cancelling a leftover pending task, say) — that's stdlib, already tested
upstream; these tests only cover the thin lazy-create/reuse/reset wrapper
this module adds around it.

No test here spins up a real ``new_process_pool()`` worker to confirm an
``atexit`` hook fires on normal shutdown: this project's ``tests/`` tree
(``--import-mode=importlib``, no ``tests/__init__.py``) isn't a real,
``sys.path``-importable package, so a function defined in a test file
can't be pickled and re-imported as a ``spawn``-context initializer/target
— only real, installed ``synology_apm_repo.*`` code can serve that role
under pytest here. See ``dedup/export_scheduler.py``'s/``units/verify_reachable.py``'s
own test files' ``TestMultiprocessExecutorTeardown`` for the real-subprocess
coverage that *is* possible this way, and each's own shutdown-hook tests
for what's covered by direct calls instead.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator

import pytest

from synology_apm_repo.sdk import concurrency


@pytest.fixture(autouse=True)
def _reset_worker_loop() -> Iterator[None]:
    """``_worker_runner`` is the process-global under test here — real
    worker processes keep exactly one for their whole lifetime, but this
    suite runs every test in the same process, so each test must start and
    end with a clean slate rather than leaking a runner into the next
    test."""
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

    # The loop a failed task ran on must still be usable for the next one
    # — one task raising must never poison the worker's persistent loop.
    concurrency.run_in_worker_loop(_noop())


def test_close_worker_loop_is_a_noop_without_a_loop() -> None:
    concurrency.close_worker_loop()  # must not raise

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
    """Regression test: ``on_done`` must not itself count against
    ``max_concurrent`` -- a caller's own slow/throttled post-completion
    callback (a progress tick that awaits, say) must not hold up the
    next item's own dispatch, the same reasoning ``dispatch_to_pool``'s
    own ``on_result`` already documents. Verified by driving
    ``max_concurrent=1`` with an ``on_done`` that blocks until a second
    ``worker`` call has already started -- which can only happen if the
    semaphore slot ``on_done``'s own item held was already released
    before ``on_done`` ran."""
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

    with pytest.raises(ExceptionGroup) as exc_info:
        await concurrency.bounded_gather(range(5), worker, max_concurrent=2)
    assert isinstance(exc_info.value.exceptions[0], ValueError)
