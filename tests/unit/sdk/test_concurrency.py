"""Tests for ``sdk/concurrency.py``.

``preload_resource_tracker()``'s own test is mock-based, no real process
spawn needed, since the whole contract is "delegates to
``multiprocessing.resource_tracker.ensure_running()``, now, with whatever
``sys.stderr`` currently is" (see its own docstring for why that matters
before ``sys.stderr`` gets replaced by something whose ``fileno()``
returns a sentinel instead of raising).

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
under pytest here. See ``dedup/chunk_walk.py``'s/``units/verify_reachable.py``'s
own test files' ``TestMultiprocessExecutorTeardown`` for the real-subprocess
coverage that *is* possible this way, and each's own shutdown-hook tests
for what's covered by direct calls instead.
"""

from __future__ import annotations

import asyncio
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
    calls: list[bool] = []
    monkeypatch.setattr("synology_apm_repo.sdk.concurrency.resource_tracker.ensure_running", lambda: calls.append(True))

    concurrency.preload_resource_tracker()

    assert calls == [True]


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
