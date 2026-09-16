"""``RecordingStore``/``ReplayStore`` capture real ``ObjectStore`` traffic
into a small, committable fixture, then replay it later without touching
any real sample data.

The problem this solves: integration tests need real, production-shaped
bytes to be meaningful, but the real sample repositories are far too large
to live in CI. Wrapping ``ObjectStore``'s four narrow byte-oriented
methods to record every call/result pair is cheap, and the resulting
fixture for one realistic scenario is typically a few hundred KB to a
couple MB uncompressed — small enough to commit alongside the test it
supports: record once against a real sample with ``RecordingStore``, dump
it to JSON, then replay it forever after in CI via ``ReplayStore``, with
zero real I/O. Fixtures are committed gzip-compressed
(``tests/fixtures/*.json.gz``) via ``write_fixture_text``; ``load_fixture_text``/
``ReplayStore.from_path`` decompress transparently on read.

``TracingStore`` below reuses the same mechanism for the CLI's ``--trace``
flag — observing every ``ObjectStore.read()`` call is the same capability
whether the goal is a test fixture or showing a user why a command was
slow.
"""

from __future__ import annotations

import base64
import dataclasses
import gzip
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..errors import NotFoundError
from .base import ObjectStore, aclose_if_possible


def _read_key(path: str, offset: int, length: int | None) -> str:
    # NUL can't appear in a store-relative path, so it's a safe separator.
    return f"{path}\x00{offset}\x00{length if length is not None else ''}"


def load_fixture_text(path: Path) -> str:
    """Read a fixture, transparently gzip-decompressing.

    Args:
        path: Fixture file to read. Read as plain text unless it ends in
            ``.gz`` — this project's on-disk convention for a committed
            ``tests/fixtures/*.json.gz`` cassette.
    """
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f:
            return f.read().decode("utf-8")
    return path.read_text()


def write_fixture_text(path: Path, text: str) -> None:
    """Write fixture text, gzip-compressing when ``path`` ends in ``.gz``."""
    if path.suffix == ".gz":
        path.write_bytes(gzip.compress(text.encode("utf-8"), compresslevel=6))
    else:
        path.write_text(text)


class _InstrumentedStore:
    """Wraps a real ``ObjectStore``, forwarding each of its four narrow
    methods unchanged and reporting every call/result pair to a callback.

    Held privately by ``RecordingStore``/``TracingStore`` (composition, not a
    base class — neither is meant to expose this helper on its own public
    surface) so both can share this forwarding logic while differing only
    in what the callback does with each call.
    """

    def __init__(
        self,
        backing: ObjectStore,
        on_call: Callable[..., None],
    ) -> None:
        """
        Args:
            backing: Real store every call is forwarded to unchanged.
            on_call: Invoked after each forwarded call as
                ``on_call(method, path, *, offset=0, length=None, result, elapsed)``.
                ``result``/``offset``/``length`` are typed loosely (``Any``/defaulted)
                since their real shape depends on ``method``: ``bytes`` for
                ``read``, ``int`` for ``size``, ``bool`` for ``exists``, ``list[str]``
                for ``listdir``; only ``read`` has a real ``offset``/``length`` in
                the first place.
        """
        self._backing = backing
        self._on_call = on_call

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        t0 = time.monotonic()
        data = await self._backing.read(path, offset, length)
        self._on_call("read", path, offset=offset, length=length, result=data, elapsed=time.monotonic() - t0)
        return data

    async def size(self, path: str) -> int:
        t0 = time.monotonic()
        n = await self._backing.size(path)
        self._on_call("size", path, result=n, elapsed=time.monotonic() - t0)
        return n

    async def exists(self, path: str) -> bool:
        t0 = time.monotonic()
        found = await self._backing.exists(path)
        self._on_call("exists", path, result=found, elapsed=time.monotonic() - t0)
        return found

    async def listdir(self, path: str) -> list[str]:
        t0 = time.monotonic()
        entries = await self._backing.listdir(path)
        self._on_call("listdir", path, result=entries, elapsed=time.monotonic() - t0)
        return entries

    async def aclose(self) -> None:
        """Forwards to ``backing``'s own ``aclose()`` when it has one
        (``S3Store``/``AzureStore``); a no-op otherwise (``LocalFsStore``/
        ``SmbStore``, which have nothing to close). Shared by
        ``RecordingStore``/``TracingStore`` so wrapping a store this way
        never silently drops its own close contract — without this,
        ``Session.close()``'s own ``isinstance(store, AsyncCloseable)``
        check finds no ``aclose`` on the wrapper at all (``@runtime_
        checkable`` only looks at method *presence*), so a traced/recorded
        ``S3Store``/``AzureStore``'s real ``aiohttp`` connector is never
        closed."""
        await aclose_if_possible(self._backing)


class RecordingStore:
    """Wraps a real ``ObjectStore`` and records every call/result pair for
    later ``dump``."""

    def __init__(self, backing: ObjectStore) -> None:
        self._reads: dict[str, bytes] = {}
        self._sizes: dict[str, int] = {}
        self._exists: dict[str, bool] = {}
        self._listdirs: dict[str, list[str]] = {}
        self._instrumented = _InstrumentedStore(backing, self._on_call)

    def _on_call(
        self, method: str, path: str, *, offset: int = 0, length: int | None = None, result: Any, elapsed: float
    ) -> None:
        if method == "read":
            self._reads[_read_key(path, offset, length)] = result
        elif method == "size":
            self._sizes[path] = result
        elif method == "exists":
            self._exists[path] = result
        else:
            self._listdirs[path] = result

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return await self._instrumented.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._instrumented.size(path)

    async def exists(self, path: str) -> bool:
        return await self._instrumented.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._instrumented.listdir(path)

    async def aclose(self) -> None:
        """See ``_InstrumentedStore.aclose``'s own docstring — needed so a
        real ``S3Store``/``AzureStore`` recorded against (``--record-
        against=profile:<name>``) still gets its ``aiohttp`` connector
        closed once recording is done."""
        await self._instrumented.aclose()

    def dump(self) -> str:
        """Serialize everything recorded so far to a JSON string suitable
        for committing as a test fixture (binary read results are
        base64-encoded)."""
        payload = {
            "reads": {k: base64.b64encode(v).decode("ascii") for k, v in self._reads.items()},
            "sizes": self._sizes,
            "exists": self._exists,
            "listdirs": self._listdirs,
        }
        return json.dumps(payload, indent=1, sort_keys=True)


class ReplayStore:
    """Answers ``ObjectStore`` calls purely from a ``RecordingStore.dump``
    fixture — no real backing store, no I/O.

    Raises:
        NotFoundError: For any call that was not recorded, so a fixture's
            coverage gaps surface immediately as a test failure rather
            than silently returning wrong data (this is why ``exists()``
            also raises rather than defaulting to ``False`` for an
            unrecorded path — a "does not exist" result must have been
            recorded).
    """

    def __init__(self, fixture_json: str) -> None:
        payload = json.loads(fixture_json)
        self._reads: dict[str, bytes] = {k: base64.b64decode(v) for k, v in payload["reads"].items()}
        self._sizes: dict[str, int] = payload["sizes"]
        self._exists: dict[str, bool] = payload["exists"]
        self._listdirs: dict[str, list[str]] = payload["listdirs"]

    @classmethod
    def from_path(cls, path: Path) -> ReplayStore:
        """Construct from a fixture file, transparently gzip-decompressing
        a ``.json.gz`` path (see ``load_fixture_text``)."""
        return cls(load_fixture_text(path))

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        key = _read_key(path, offset, length)
        try:
            return self._reads[key]
        except KeyError as exc:
            raise NotFoundError(
                f"ReplayStore has no recorded read for path={path!r} offset={offset} length={length} "
                "— fixture does not cover this call",
                ref=path,
            ) from exc

    async def size(self, path: str) -> int:
        try:
            return self._sizes[path]
        except KeyError as exc:
            raise NotFoundError(f"ReplayStore has no recorded size for {path!r}", ref=path) from exc

    async def exists(self, path: str) -> bool:
        try:
            return self._exists[path]
        except KeyError as exc:
            raise NotFoundError(f"ReplayStore has no recorded exists() for {path!r}", ref=path) from exc

    async def listdir(self, path: str) -> list[str]:
        try:
            return list(self._listdirs[path])
        except KeyError as exc:
            raise NotFoundError(f"ReplayStore has no recorded listdir for {path!r}", ref=path) from exc


@dataclasses.dataclass(frozen=True)
class TraceEvent:
    """One ``ObjectStore`` call, for the ``--trace`` CLI flag.

    Unlike ``RecordingStore``, nothing is buffered — a real ``--trace`` run
    against a real repository can touch millions of chunks, so
    ``TracingStore`` streams one event per call straight to a callback
    instead.

    Attributes:
        method: ``"read"``, ``"size"``, ``"exists"``, or ``"listdir"``.
        path: Store-relative path the call targeted.
        offset: ``read``'s byte offset; ``0`` for every other method.
        length: ``read``'s requested length; ``None`` for every other method.
        result_length: Bytes actually returned (``read``) or entries listed
            (``listdir``); ``None`` for ``size``/``exists``, which report a plain
            value rather than a count.
        elapsed: Wall-clock seconds the call took.
    """

    method: str
    path: str
    offset: int = 0
    length: int | None = None
    result_length: int | None = None
    elapsed: float = 0.0


class TracingStore:
    """Wraps a real ``ObjectStore``, calling ``on_event`` once per call with a
    ``TraceEvent`` — the same "wrap the four narrow methods" mechanism
    ``RecordingStore`` above uses. Every call is forwarded to ``backing`` and
    its result returned/raised unchanged."""

    def __init__(self, backing: ObjectStore, on_event: Callable[[TraceEvent], None]) -> None:
        self._on_event = on_event
        self._instrumented = _InstrumentedStore(backing, self._on_call)

    def _on_call(
        self, method: str, path: str, *, offset: int = 0, length: int | None = None, result: Any, elapsed: float
    ) -> None:
        # Only read()/listdir() have a meaningful result_length; size()/
        # exists() report a plain value, not a byte/entry count.
        result_length = len(result) if method in ("read", "listdir") else None
        self._on_event(
            TraceEvent(
                method=method, path=path, offset=offset, length=length, result_length=result_length, elapsed=elapsed
            )
        )

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return await self._instrumented.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._instrumented.size(path)

    async def exists(self, path: str) -> bool:
        return await self._instrumented.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._instrumented.listdir(path)

    async def aclose(self) -> None:
        """See ``_InstrumentedStore.aclose``'s own docstring — needed so a
        real ``S3Store``/``AzureStore`` wrapped for ``--trace`` (or by this
        project's own smoke tooling, which always traces) still gets its
        ``aiohttp`` connector closed by ``Session.close()``."""
        await self._instrumented.aclose()
