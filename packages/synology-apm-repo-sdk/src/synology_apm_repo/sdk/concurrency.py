"""Real multi-core parallelism for CPU-bound work (decompress/decrypt/hash),
via ``concurrent.futures.ProcessPoolExecutor`` rather than
``asyncio.to_thread()``, which never gets more than one CPU core working
under CPython's GIL. Owns worker-count sizing and dynamic-load-balanced
dispatch to a pool — every such call site uses these rather than
re-deriving its own.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Sequence
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import resource_tracker
from typing import Any, TypeVar

_T = TypeVar("_T")
_R = TypeVar("_R")

_MAX_WORKERS = 8
"""Ceiling on ``default_worker_count()``'s clamp."""


def default_worker_count() -> int:
    """``clamp(os.cpu_count() // 2, 1, 8)`` — every ``ProcessPoolExecutor``
    this SDK builds for CPU-bound decode work is sized by this, not a raw
    ``os.cpu_count()``. Halving biases toward a machine's faster cores on a
    hybrid (performance + efficiency) chip, since these call sites
    synchronize a whole batch at a shared boundary and one slow-core task
    drags the batch down regardless of added concurrency.
    """
    cpu = os.cpu_count() or 1
    return max(1, min(_MAX_WORKERS, cpu // 2))


def new_process_pool(
    *,
    initializer: Callable[..., None] | None = None,
    initargs: Sequence[object] = (),
) -> ProcessPoolExecutor:
    """A ``ProcessPoolExecutor`` sized by ``default_worker_count()``, using
    an explicit ``multiprocessing.get_context("spawn")`` — never the
    platform default, since Linux's default (``fork``) would duplicate a
    parent process that may already hold a live asyncio event loop and
    open ``aiosqlite`` background threads, neither of which survives a
    fork correctly.
    """
    # typeshed's own ProcessPoolExecutor overloads pair a variadic
    # initializer signature with a matching initargs tuple type, which a
    # generic wrapper like this one — deliberately agnostic about any one
    # caller's own initializer shape — can never satisfy exactly.
    return ProcessPoolExecutor(
        max_workers=default_worker_count(),
        mp_context=multiprocessing.get_context("spawn"),
        initializer=initializer,
        initargs=tuple(initargs),  # type: ignore[arg-type]
    )


_worker_runner: asyncio.Runner | None = None
"""A worker process's own persistent ``asyncio.Runner``, lazily created by
the first ``run_in_worker_loop()`` call that process ever makes."""


def run_in_worker_loop(coro: Coroutine[Any, Any, _R]) -> _R:
    """Runs ``coro`` on this worker process's own persistent event loop —
    created once, on first call, and reused for every later call in the
    same process — instead of ``asyncio.run()``'s throwaway-loop-per-call
    shape, which would close a worker-lifetime resource's bound loop out
    from under it. Call ``close_worker_loop()`` once, at worker shutdown,
    to release this loop gracefully.
    """
    global _worker_runner
    if _worker_runner is None:
        _worker_runner = asyncio.Runner()
    return _worker_runner.run(coro)


def close_worker_loop() -> None:
    """Gracefully closes this worker process's persistent event loop — call
    once, from an ``atexit`` hook a worker's own ``ProcessPoolExecutor``
    initializer registers, after the worker's own resources are already
    released through one final ``run_in_worker_loop()`` call. A no-op if
    ``run_in_worker_loop()`` was never called in this process.
    """
    global _worker_runner
    if _worker_runner is None:
        return
    _worker_runner.close()
    _worker_runner = None


def preload_resource_tracker() -> None:
    """Launches multiprocessing's resource-tracker helper process now, using
    whatever ``sys.stderr`` currently is. Call this before replacing
    ``sys.stderr`` with a stream whose ``fileno()`` returns a sentinel
    (Textual's own output capture does this) — the tracker's launch code
    appends ``sys.stderr.fileno()`` unvalidated and crashes the first
    ``ProcessPoolExecutor`` built afterward otherwise, with
    ``ValueError: bad value(s) in fds_to_keep``.

    No-op on non-POSIX platforms: the tracker's helper-process launch
    relies on POSIX fd inheritance.
    """
    if os.name == "posix":
        resource_tracker.ensure_running()


async def bounded_gather(
    items: Iterable[_T],
    worker: Callable[[_T], Awaitable[None]],
    *,
    max_concurrent: int,
    on_done: Callable[[_T], Awaitable[None]] | None = None,
) -> None:
    """Runs ``worker(item)`` for every item in ``items``, concurrently,
    bounded to at most ``max_concurrent`` in flight at once, inside one
    ``asyncio.TaskGroup``. For I/O-bound async work that never leaves the
    event loop; see ``dispatch_to_pool`` for CPU-bound work across a real
    process pool.

    ``on_done(item)``, when given, is awaited once per item after
    ``worker(item)`` completes and the semaphore slot is already
    released, so it doesn't hold up the next item's dispatch.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _bounded(item: _T) -> None:
        async with semaphore:
            await worker(item)
        if on_done is not None:
            await on_done(item)

    async with asyncio.TaskGroup() as tg:
        for item in items:
            tg.create_task(_bounded(item))


async def dispatch_to_pool(
    executor: ProcessPoolExecutor,
    worker_fn: Callable[[_T], _R],
    items: Iterable[_T],
    *,
    max_concurrent: int,
    on_result: Callable[[_T, _R], Awaitable[None]] | None = None,
) -> list[_R]:
    """Bounded, dynamically load-balanced dispatch of ``items`` to
    ``worker_fn`` across ``executor``: at most ``max_concurrent`` in flight
    at once, with a free worker always picking up the next not-yet-started
    item via the executor's own task queue — self-correcting for uneven
    per-item cost, unlike a static up-front partition.

    ``on_result(item, result)``, when given, is awaited once per item as
    its future resolves, for a caller's own parent-only bookkeeping.

    Results are returned in **completion order**, not ``items``' own order
    — a caller that needs input order preserved should zip its own index
    onto ``items`` and sort afterward.
    """
    sem = asyncio.Semaphore(max_concurrent)
    loop = asyncio.get_running_loop()
    results: list[_R] = []

    async def _run(item: _T) -> None:
        async with sem:
            result = await loop.run_in_executor(executor, worker_fn, item)
        if on_result is not None:
            await on_result(item, result)
        results.append(result)

    async with asyncio.TaskGroup() as tg:
        for item in items:
            tg.create_task(_run(item))
    return results
