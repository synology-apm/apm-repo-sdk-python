"""Real multi-core parallelism for CPU-bound work — decompress/decrypt/hash
never actually run concurrently under CPython's GIL no matter how many
``asyncio.to_thread()``-dispatched OS threads share the work; only a real
``concurrent.futures.ProcessPoolExecutor`` gets more than one CPU core
working on it at once. This module owns the two things every such call
site needs and must never re-derive independently: how many worker
processes to use, and how to dispatch a batch of independent work items to
them with real dynamic load balancing.

See ``ARCHITECTURE.md``'s "Async-native, by design" section for the
measurement this is based on, and its "Cross-cutting shared mechanisms"
section for this module's place alongside ``storage.store_descriptor``/
``dedup.pool_descriptor`` (the two things a worker process needs to
rebuild its own repository state — this module knows nothing about either).
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
"""Ceiling on ``default_worker_count()``'s own clamp — a starting value,
not a universally measured one; see that function's own docstring."""


def default_worker_count() -> int:
    """``clamp(os.cpu_count() // 2, 1, 8)`` — every ``ProcessPoolExecutor``
    this SDK builds for CPU-bound decode work is sized by this, not a raw
    ``os.cpu_count()``.

    The ``// 2`` (not the full logical core count) is deliberate: on a
    hybrid (performance + efficiency core) machine, some tasks land on
    the slower efficiency cores, and since these call sites synchronize a
    whole batch at a shared boundary (a verify run's bucket list, an
    export window), the batch's own completion time is dragged down to
    whichever task landed on a slow core, not sped up by the extra
    concurrency. Halving the logical core count is a simple, portable way
    to bias toward a machine's faster cores without needing
    platform-specific "how many performance cores does this chip have"
    detection, and degrades safely on a uniform (non-hybrid) machine too.
    The ``8`` ceiling keeps this from growing unbounded on a many-core
    server, where the same synchronization-drags-down-the-batch effect
    still applies once workers start competing for shared I/O/memory
    bandwidth.
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
    platform default, since Linux's default (``fork``) is unsafe here: it
    would duplicate a parent process that may already hold a live asyncio
    event loop and open ``aiosqlite`` background threads, neither of which
    survives a fork correctly. The one place both the worker-count formula
    and the ``spawn`` context are applied, so no two call sites can drift
    out of sync on either.

    Named ``new_process_pool``, not ``new_worker_pool``, specifically to
    avoid reading like it returns a ``dedup.pool.Pool`` — the two concepts
    ("OS process pool" vs. "dedup chunk pool") sit right next to each
    other in every call site that uses this.
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
the first ``run_in_worker_loop()`` call that process ever makes — process-
global exactly like ``_worker_store``/``_worker_pool`` already are in
``dedup/chunk_walk.py``/``units/verify_reachable.py``, for the same
reason: a ``ProcessPoolExecutor`` worker built by ``new_process_pool()``
handles many work items over its lifetime, not one. ``Runner`` (stdlib,
3.11+, this project's own floor) is exactly "an ``asyncio.run()`` whose
``run()`` can be called more than once against the same embedded loop" —
its own docstring names this precise use case ("everywhere ... the
preferred single ``asyncio.run()`` call doesn't work"), so this reuses it
rather than hand-rolling the same lazily-created-loop/cancel-pending-
tasks/``shutdown_asyncgens``/``shutdown_default_executor``/close sequence
``Runner.close()`` already implements (and already tests) upstream."""


def run_in_worker_loop(coro: Coroutine[Any, Any, _R]) -> _R:
    """Runs ``coro`` on this worker process's own persistent event loop —
    created once, on first call, and reused for every later call in the
    same process — instead of ``asyncio.run()``'s own throwaway-loop-
    per-call shape, which is unsafe here: a worker-lifetime resource built
    once via a ``ProcessPoolExecutor`` ``initializer=`` (an ``ObjectStore``
    whose real network client is lazily built and cached on first use,
    say) binds itself to whichever loop happens to be running the first
    time it's actually used, and ``asyncio.run()`` unconditionally closes
    that loop when its one call returns — a second call reusing that same
    cached client would then try to send a request through a now-dead
    loop. Call ``close_worker_loop()`` once, at worker shutdown, to
    release this loop gracefully instead of leaving it for the OS to
    reclaim at process exit.
    """
    global _worker_runner
    if _worker_runner is None:
        _worker_runner = asyncio.Runner()
    return _worker_runner.run(coro)


def close_worker_loop() -> None:
    """Gracefully closes this worker process's persistent event loop — call
    once, from an ``atexit`` hook a worker's own ``ProcessPoolExecutor``
    initializer registers, after any of the worker's own resources (an
    ``ObjectStore``'s ``aclose()``, say) have already been released through
    one final ``run_in_worker_loop()`` call. A no-op if
    ``run_in_worker_loop()`` was never called in this process (no loop was
    ever created).
    """
    global _worker_runner
    if _worker_runner is None:
        return
    _worker_runner.close()
    _worker_runner = None


def preload_resource_tracker() -> None:
    """Launches multiprocessing's resource-tracker helper process now, using
    whatever ``sys.stderr`` currently is.

    The tracker is a singleton for the whole parent process and launches
    its helper at most once. Call this before replacing ``sys.stderr``
    with a stream whose ``fileno()`` returns a sentinel instead of raising
    (Textual's own output capture does this for the whole time an ``App``
    is running) — the tracker's launch code appends
    ``sys.stderr.fileno()`` to the file descriptors it hands to that
    helper with no validation that the value is a real, open descriptor,
    and crashes the first ``ProcessPoolExecutor`` built afterward if it
    isn't. A caller that never replaces ``sys.stderr`` this way has no
    need for this — the tracker's ordinary lazy launch on first use
    already works fine there.

    No-op on non-POSIX platforms: the tracker's helper-process launch
    relies on POSIX fd inheritance, which doesn't exist on Windows.
    """
    if os.name == "posix":
        resource_tracker.ensure_running()


async def dispatch_to_pool(
    executor: ProcessPoolExecutor,
    worker_fn: Callable[[_T], _R],
    items: Iterable[_T],
    *,
    max_concurrent: int,
    on_result: Callable[[_T, _R], Awaitable[None]] | None = None,
) -> list[_R]:
    """Bounded, dynamically load-balanced dispatch of ``items`` to
    ``worker_fn`` across ``executor``: an ``asyncio.Semaphore(max_concurrent)``
    gates how many are in flight at once, and a free worker always picks up
    the next not-yet-started item via the executor's own internal task
    queue — this beats even an exactly-balanced *static* partition of
    ``items`` across workers ahead of time, since real per-item wall-clock
    cost isn't perfectly predictable from a cheap proxy metric (bucket
    chunk count, say); a dynamic queue self-corrects for whatever actually
    happens at runtime, a static split can't.

    ``on_result(item, result)``, when given, is awaited once per item as
    its future resolves — the one place a caller applies its own
    parent-only bookkeeping (tagging a result with caller state the worker
    itself has no access to, ticking a progress callback) that has no
    business living in this generic dispatch loop.

    Results are returned in **completion order**, not ``items``' own
    order — every current caller only aggregates them (sums bytes,
    extends a findings list), never depends on order; a caller that needs
    input order preserved should zip its own index onto ``items`` and sort
    afterward.
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
