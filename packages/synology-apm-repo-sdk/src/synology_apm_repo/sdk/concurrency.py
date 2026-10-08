"""Multi-core parallelism for CPU-bound work (decompress/decrypt/hash)
through a ``ProcessPoolExecutor`` (``asyncio.to_thread()`` stays on one
core under the GIL): pool sizing and construction, load-balanced dispatch,
worker-failure reporting and the worker-side event loop, plus
``bounded_gather`` for I/O-bound fan-out."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Sequence
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import resource_tracker
from typing import Any, NoReturn

from ._util.closing import leaf_exceptions
from .errors import WorkerProcessError

_MAX_WORKERS = 8
"""Ceiling on ``default_worker_count()``'s clamp."""


def default_worker_count() -> int:
    """``clamp(os.cpu_count() // 2, 1, 8)``, the size of every worker pool.
    Halved because on a hybrid (performance + efficiency) chip a batch
    waits for its slowest task, so efficiency cores add little.
    """
    cpu = os.cpu_count() or 1
    return max(1, min(_MAX_WORKERS, cpu // 2))


def new_process_pool(
    *,
    initializer: Callable[..., None] | None = None,
    initargs: Sequence[object] = (),
) -> ProcessPoolExecutor:
    """A ``ProcessPoolExecutor`` sized by ``default_worker_count()``, always
    with the ``spawn`` start method: ``fork`` (Linux's default) would copy
    a live event loop and ``aiosqlite`` threads, which don't survive it.
    """
    # typeshed pairs the initializer's signature with initargs' tuple type,
    # which a wrapper generic over the initializer can't satisfy.
    return ProcessPoolExecutor(
        max_workers=default_worker_count(),
        mp_context=multiprocessing.get_context("spawn"),
        initializer=initializer,
        initargs=tuple(initargs),  # type: ignore[arg-type]
    )


async def shutdown_pool(executor: ProcessPoolExecutor) -> None:
    """Shuts ``executor`` down — queued items cancelled, running ones waited
    for — off the event loop, so the wait doesn't freeze it."""
    await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)


def common_descriptor[T](descriptors: Sequence[T | None]) -> T | None:
    """The one descriptor every entry equals, or ``None`` when ``descriptors``
    is empty, holds a ``None``, or disagrees: whether several units of work
    can share one worker pool, whose processes are bound to one descriptor
    for their lifetime."""
    if not descriptors or descriptors[0] is None:
        return None
    first = descriptors[0]
    return first if all(descriptor == first for descriptor in descriptors) else None


def first_failure(group: BaseExceptionGroup[BaseException], *, operation: str) -> BaseException:
    """The failure a worker-dispatch exception group stands for: its first
    exception that is not a cancellation, with a note counting the others.
    A group of only cancellations is returned unchanged."""
    failures = [exc for exc in leaf_exceptions(group) if not isinstance(exc, asyncio.CancelledError)]
    if not failures:
        return group
    first, others = failures[0], failures[1:]
    if others:
        kinds = ", ".join(sorted({type(exc).__name__ for exc in others}))
        noun = "failure" if len(others) == 1 else "failures"
        first.add_note(f"{len(others)} other {operation} worker {noun}: {kinds}")
    return first


def raise_worker_failure(group: BaseExceptionGroup[BaseException], *, operation: str) -> NoReturn:
    """Raises what a worker dispatch's exception group stands for:
    ``first_failure``, or ``WorkerProcessError`` when a worker process died.

    Raises:
        WorkerProcessError: A worker process died.
    """
    failure = first_failure(group, operation=operation)
    if isinstance(failure, BrokenProcessPool):
        raise WorkerProcessError(
            f"{operation} worker process died unexpectedly (it was killed or ran out of memory); "
            f"the {operation} was aborted"
        ) from failure
    raise failure from None


_worker_runner: asyncio.Runner | None = None
"""This worker process's persistent ``asyncio.Runner``, created by the
first ``run_in_worker_loop()``."""


def run_in_worker_loop[R](coro: Coroutine[Any, Any, R]) -> R:
    """Runs ``coro`` on this worker process's persistent event loop, so
    worker-lifetime resources bound to it survive between calls (unlike
    ``asyncio.run()``'s loop per call). ``close_worker_loop()`` releases
    it at worker shutdown.
    """
    global _worker_runner
    if _worker_runner is None:
        _worker_runner = asyncio.Runner()
    return _worker_runner.run(coro)


def close_worker_loop() -> None:
    """Closes this worker process's persistent event loop, after its
    resources were released through a last ``run_in_worker_loop()``; for
    an ``atexit`` hook the pool initializer registers. A no-op if no loop
    was created.
    """
    global _worker_runner
    if _worker_runner is None:
        return
    _worker_runner.close()
    _worker_runner = None


def preload_resource_tracker() -> None:
    """Launches multiprocessing's resource-tracker process now. Call it
    before replacing ``sys.stderr`` with a stream whose ``fileno()``
    returns a sentinel (Textual's output capture does): launched later,
    the tracker passes that fd on and the first process pool fails with
    ``ValueError: bad value(s) in fds_to_keep``. A no-op off POSIX.
    """
    if os.name == "posix":
        resource_tracker.ensure_running()


async def bounded_gather[T](
    items: Iterable[T],
    worker: Callable[[T], Awaitable[None]],
    *,
    max_concurrent: int,
    on_done: Callable[[T], Awaitable[None]] | None = None,
) -> None:
    """Runs ``worker(item)`` for every item, at most ``max_concurrent`` at
    once, for I/O-bound work on the event loop (``dispatch_to_pool`` is
    for CPU-bound work). ``on_done(item)`` runs after its slot is freed.

    Raises:
        ExceptionGroup: One or more ``worker``/``on_done`` calls failed;
            the rest were cancelled.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _bounded(item: T) -> None:
        async with semaphore:
            await worker(item)
        if on_done is not None:
            await on_done(item)

    async with asyncio.TaskGroup() as tg:
        for item in items:
            tg.create_task(_bounded(item))


async def dispatch_to_pool[T, R](
    executor: ProcessPoolExecutor,
    worker_fn: Callable[[T], R],
    items: Iterable[T],
    *,
    max_concurrent: int,
    on_result: Callable[[T, R], Awaitable[None]] | None = None,
) -> list[R]:
    """Runs ``worker_fn(item)`` on ``executor`` for every item, at most
    ``max_concurrent`` at once, each free worker taking the next item, so
    uneven per-item cost balances itself. ``on_result(item, result)`` is
    awaited in this process as each one finishes. Results come back in
    completion order, not ``items`` order.

    Raises:
        ExceptionGroup: One or more items failed (see
            ``raise_worker_failure``); the rest were cancelled.
    """
    sem = asyncio.Semaphore(max_concurrent)
    loop = asyncio.get_running_loop()
    results: list[R] = []

    async def _run(item: T) -> None:
        async with sem:
            result = await loop.run_in_executor(executor, worker_fn, item)
        if on_result is not None:
            await on_result(item, result)
        results.append(result)

    async with asyncio.TaskGroup() as tg:
        for item in items:
            tg.create_task(_run(item))
    return results
