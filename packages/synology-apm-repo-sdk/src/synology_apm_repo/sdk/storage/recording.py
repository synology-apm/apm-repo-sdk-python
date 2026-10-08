"""``InstrumentedStore``: an ``ObjectStore`` wrapper that hands every call
to its subclass as a typed call event. ``TracingStore`` (the CLI's
``--trace`` flag) is one.
"""

from __future__ import annotations

import abc
import dataclasses
import time
from collections.abc import Callable
from typing import assert_never, override

from .base import Entry, ObjectStore


@dataclasses.dataclass(frozen=True, slots=True)
class ReadCall:
    """A completed ``read`` and the bytes it returned."""

    path: str
    offset: int
    length: int | None
    data: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class SizeCall:
    """A completed ``size``."""

    path: str
    size: int


@dataclasses.dataclass(frozen=True, slots=True)
class ExistsCall:
    """A completed ``exists``."""

    path: str
    found: bool


@dataclasses.dataclass(frozen=True, slots=True)
class ListCall:
    """A completed ``listdir``."""

    path: str
    entries: list[Entry]


Call = ReadCall | SizeCall | ExistsCall | ListCall


@dataclasses.dataclass(frozen=True, slots=True)
class FailedCall:
    """A call that raised ``error``; ``offset``/``length`` are ``0``/``None`` for every method but ``read``."""

    method: str
    path: str
    offset: int
    length: int | None
    error: Exception


class InstrumentedStore(abc.ABC):
    """Forwards each ``ObjectStore`` method to ``backing`` unchanged and hands
    every completed call to ``_observe`` and every failed one (an
    ``Exception``, not a cancellation) to ``_observe_failure``, with its
    wall-clock ``elapsed`` seconds and ``started`` epoch time; the failure
    is then re-raised unchanged."""

    def __init__(self, backing: ObjectStore) -> None:
        self._backing = backing

    @abc.abstractmethod
    def _observe(self, call: Call, *, elapsed: float, started: float) -> None: ...

    @abc.abstractmethod
    def _observe_failure(self, call: FailedCall, *, elapsed: float, started: float) -> None: ...

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        started, t0 = time.time(), time.monotonic()
        try:
            data = await self._backing.read(path, offset, length)
        except Exception as exc:
            self._observe_failure(
                FailedCall("read", path, offset, length, exc), elapsed=time.monotonic() - t0, started=started
            )
            raise
        self._observe(ReadCall(path, offset, length, data), elapsed=time.monotonic() - t0, started=started)
        return data

    async def size(self, path: str) -> int:
        started, t0 = time.time(), time.monotonic()
        try:
            n = await self._backing.size(path)
        except Exception as exc:
            self._observe_failure(
                FailedCall("size", path, 0, None, exc), elapsed=time.monotonic() - t0, started=started
            )
            raise
        self._observe(SizeCall(path, n), elapsed=time.monotonic() - t0, started=started)
        return n

    async def exists(self, path: str) -> bool:
        started, t0 = time.time(), time.monotonic()
        try:
            found = await self._backing.exists(path)
        except Exception as exc:
            self._observe_failure(
                FailedCall("exists", path, 0, None, exc), elapsed=time.monotonic() - t0, started=started
            )
            raise
        self._observe(ExistsCall(path, found), elapsed=time.monotonic() - t0, started=started)
        return found

    async def listdir(self, path: str) -> list[Entry]:
        started, t0 = time.time(), time.monotonic()
        try:
            entries = await self._backing.listdir(path)
        except Exception as exc:
            self._observe_failure(
                FailedCall("listdir", path, 0, None, exc), elapsed=time.monotonic() - t0, started=started
            )
            raise
        self._observe(ListCall(path, entries), elapsed=time.monotonic() - t0, started=started)
        return entries

    async def close(self) -> None:
        """Forward to ``backing``'s ``close()``, so ``Session.close()`` still
        releases a wrapped ``S3Store``/``AzureStore``."""
        await self._backing.close()


@dataclasses.dataclass(frozen=True, slots=True)
class TraceEvent:
    """One ``ObjectStore`` call, for the ``--trace`` CLI flag.

    Attributes:
        method: ``"read"``, ``"size"``, ``"exists"``, or ``"listdir"``.
        path: Store-relative path the call targeted.
        offset: ``read``'s byte offset; ``0`` for every other method.
        length: ``read``'s requested length; ``None`` for every other method.
        result_length: Bytes actually returned (``read``) or entries listed
            (``listdir``); ``None`` for ``size``/``exists``, which report a plain
            value rather than a count.
        elapsed: Wall-clock seconds the call took.
        started: Epoch seconds (``time.time()``) at which the call began.
        error: The exception class name when the call failed (it is
            re-raised unchanged); ``None`` when it succeeded.
    """

    method: str
    path: str
    offset: int = 0
    length: int | None = None
    result_length: int | None = None
    elapsed: float = 0.0
    started: float = 0.0
    error: str | None = None


class TracingStore(InstrumentedStore):
    """Wraps a real ``ObjectStore``, calling ``on_event`` with a ``TraceEvent``
    per call, failed ones included. Results and exceptions pass through
    unchanged."""

    def __init__(self, backing: ObjectStore, on_event: Callable[[TraceEvent], None]) -> None:
        super().__init__(backing)
        self._on_event = on_event

    @property
    def backing(self) -> ObjectStore:
        """The real store this instance forwards to, so ``Session`` can
        recognize two wraps of one connector as a single resource."""
        return self._backing

    @override
    def _observe(self, call: Call, *, elapsed: float, started: float) -> None:
        match call:
            case ReadCall(path=path, offset=offset, length=length, data=data):
                event = TraceEvent("read", path, offset, length, len(data), elapsed, started)
            case SizeCall(path=path):
                event = TraceEvent("size", path, elapsed=elapsed, started=started)
            case ExistsCall(path=path):
                event = TraceEvent("exists", path, elapsed=elapsed, started=started)
            case ListCall(path=path, entries=entries):
                event = TraceEvent("listdir", path, result_length=len(entries), elapsed=elapsed, started=started)
            case _:
                assert_never(call)
        self._on_event(event)

    @override
    def _observe_failure(self, call: FailedCall, *, elapsed: float, started: float) -> None:
        self._on_event(
            TraceEvent(
                call.method,
                call.path,
                call.offset,
                call.length,
                elapsed=elapsed,
                started=started,
                error=type(call.error).__name__,
            )
        )
