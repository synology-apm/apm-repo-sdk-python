"""Releasing resources on every exit path: close sweeps (every resource
gets a close attempt even when an earlier one fails, and the failures are
reported together), ``AsyncClosing``, and ``shield_or_undo`` for work a
cancellation can't stop."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Iterable
from types import TracebackType
from typing import Self

RESOURCE_CLOSE_TIMEOUT = 10.0
"""Seconds one resource's close may take in an owner-level close sweep,
so one hung close can't block the rest of the sweep."""

Closer = Callable[[], Awaitable[object]]


async def close_each(closers: Iterable[Closer], *, per_close_timeout: float | None = None) -> list[Exception]:
    """Await every closer in order, each bounded by ``per_close_timeout`` when given,
    and return the failures instead of raising. Cancellation propagates."""
    errors: list[Exception] = []
    for close in closers:
        try:
            if per_close_timeout is None:
                await close()
            else:
                await asyncio.wait_for(close(), timeout=per_close_timeout)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
    return errors


async def close_all(closers: Iterable[Closer], message: str, *, per_close_timeout: float | None = None) -> None:
    """``close_each``, then raise its failures together.

    Raises:
        ExceptionGroup: One or more closers failed; every one was still attempted.
    """
    errors = await close_each(closers, per_close_timeout=per_close_timeout)
    if errors:
        raise ExceptionGroup(message, errors)


async def close_preserving(
    primary: BaseException, closers: Iterable[Closer], *, per_close_timeout: float | None = None
) -> None:
    """Cleanup on a failure path: ``close_each``, with each close failure
    attached to ``primary`` as a note, so the error being propagated is
    never replaced by a secondary one."""
    for exc in await close_each(closers, per_close_timeout=per_close_timeout):
        primary.add_note(f"cleanup also failed: {exc!r}")


async def _settle[T](task: asyncio.Future[T]) -> None:
    """Waits until ``task`` is done, through any further cancellation of
    the caller."""
    while not task.done():
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait([task])


async def shield_or_undo[T](work: Awaitable[T], undo: Callable[[T | None], Awaitable[object]]) -> T:
    """Awaits ``work`` (typically a thread, which can't be stopped) and
    lets it finish even if the caller is cancelled. If the await fails or
    is cancelled, then once ``work`` ends, runs ``undo(result)`` (``None``
    if ``work`` failed); neither wait yields to a further cancellation,
    and an ``undo`` failure is noted on the exception being raised."""
    task = asyncio.ensure_future(work)
    try:
        return await asyncio.shield(task)
    except BaseException as exc:
        await _settle(task)
        result = None if task.cancelled() or task.exception() is not None else task.result()
        undo_task = asyncio.ensure_future(undo(result))
        await _settle(undo_task)
        if not undo_task.cancelled() and (undo_exc := undo_task.exception()) is not None:
            exc.add_note(f"cleanup also failed: {undo_exc!r}")
        raise


def leaf_exceptions[E: BaseException](group: BaseExceptionGroup[E]) -> list[E]:
    """Flatten nested exception groups into their leaf exceptions, in order."""
    leaves: list[E] = []
    for exc in group.exceptions:
        if isinstance(exc, BaseExceptionGroup):
            leaves.extend(leaf_exceptions(exc))
        else:
            leaves.append(exc)
    return leaves


class AsyncClosing:
    """Mixin for a class with an ``async close()``: ``async with`` yields the
    instance and closes it on every exit path."""

    async def close(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
