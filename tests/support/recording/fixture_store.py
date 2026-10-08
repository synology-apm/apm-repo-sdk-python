"""Recorded fixtures: ``RecordingStore`` captures a real sample's
``ObjectStore`` traffic once (``dump``), and ``ReplayStore`` answers the same
calls from that fixture with no real sample. Fixtures are committed
gzip-compressed (``tests/fixtures/*.json.gz``) via ``write_fixture_text``;
``load_fixture_text``/``ReplayStore.from_path`` decompress on read.
``AliasedStore`` shows a local recording each sample's alias in place of
its real directory name.

A fixture is a JSON object with a ``"format"`` version, the ``reads``,
``sizes``, ``exists`` and ``listdirs`` a call answered, and ``missing``: per
method, the keys a call raised ``NotFoundError`` for.
"""

from __future__ import annotations

import base64
import gzip
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import override

from support.fakes import faithful_to
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore, join_path
from synology_apm_repo.sdk.storage.recording import (
    Call,
    ExistsCall,
    FailedCall,
    InstrumentedStore,
    ListCall,
    ReadCall,
    SizeCall,
)

#: The fixture format ``RecordingStore.dump`` writes, and the only one
#: ``ReplayStore`` reads.
FORMAT_VERSION = 2

#: ``missing``'s sections, one per ``ObjectStore`` method that can raise
#: ``NotFoundError``.
_MISSING_METHODS = ("read", "size", "exists", "listdir")


def _read_key(path: str, offset: int, length: int | None) -> str:
    # NUL can't appear in a store-relative path, so it's a safe separator.
    return f"{path}\x00{offset}\x00{length if length is not None else ''}"


def parse_read_key(key: str) -> tuple[str, int, int | None]:
    """Split a ``reads`` (or ``missing["read"]``) key back into its call.

    Args:
        key: A fixture's read key.

    Returns:
        The read's ``(path, offset, length)``; ``length`` is ``None`` for a
        read to the end of the file.
    """
    path, offset, length = key.split("\x00")
    return path, int(offset), int(length) if length else None


class FixtureFormatError(ValueError):
    """A fixture not in ``FORMAT_VERSION``: it was recorded by an older
    ``RecordingStore`` and needs re-recording."""


class UnrecordedCallError(AssertionError):
    """A ``ReplayStore`` call its fixture never recorded: the fixture is
    stale for the code path under test and needs re-recording. Not an SDK
    error, so no ``except NotFoundError`` in the code under test absorbs it."""


def load_fixture_text(path: Path) -> str:
    """Read a fixture, gzip-decompressing when ``path`` ends in ``.gz``.

    Args:
        path: Fixture file to read.

    Returns:
        The fixture text.
    """
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f:
            return f.read().decode("utf-8")
    return path.read_text(encoding="utf-8")


def write_fixture_text(path: Path, text: str) -> None:
    """Write fixture text, gzip-compressing when ``path`` ends in ``.gz``.

    Args:
        path: Destination file.
        text: Fixture text to write.
    """
    if path.suffix == ".gz":
        path.write_bytes(gzip.compress(text.encode("utf-8"), compresslevel=6))
    else:
        path.write_text(text, encoding="utf-8")


@faithful_to(ObjectStore)
class AliasedStore:
    """An ``ObjectStore`` presenting each local sample directory under its
    alias: a path segment naming an alias, or a file named after one
    (``<alias>.key``), reaches the real directory, and a listing reports the
    alias. A recording rooted above the samples (``manifest``'s
    ``all-local``) thereby records alias paths only.

    Args:
        inner: The real store.
        renames: Real directory name -> alias
            (``manifest.sample_directory_renames``).
    """

    def __init__(self, inner: ObjectStore, renames: dict[str, str]) -> None:
        self._inner = inner
        self._to_alias = dict(renames)
        self._to_real = {alias: real for real, alias in renames.items()}

    @staticmethod
    def _renamed(segment: str, mapping: dict[str, str]) -> str:
        for old, new in mapping.items():
            if segment == old or segment.startswith(f"{old}."):
                return new + segment[len(old) :]
        return segment

    def _real(self, path: str) -> str:
        return "/".join(self._renamed(segment, self._to_real) for segment in path.split("/"))

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return await self._inner.read(self._real(path), offset, length)

    async def size(self, path: str) -> int:
        return await self._inner.size(self._real(path))

    async def exists(self, path: str) -> bool:
        return await self._inner.exists(self._real(path))

    async def listdir(self, path: str) -> list[Entry]:
        entries = await self._inner.listdir(self._real(path))
        return [entry._replace(name=self._renamed(entry.name, self._to_alias)) for entry in entries]

    async def close(self) -> None:
        await self._inner.close()


class RecordingStore(InstrumentedStore):
    """Wraps a real ``ObjectStore`` and records every call/result pair for
    later ``dump``: a successful call's result, and a call that raised
    ``NotFoundError`` as a recorded miss. Any other failure is not recorded,
    so replaying it fails as an unrecorded call."""

    def __init__(self, backing: ObjectStore) -> None:
        super().__init__(backing)
        self._reads: dict[str, bytes] = {}
        self._sizes: dict[str, int] = {}
        self._exists: dict[str, bool] = {}
        self._listdirs: dict[str, list[str]] = {}
        self._missing: dict[str, set[str]] = {method: set() for method in _MISSING_METHODS}

    def rebind(self, backing: ObjectStore) -> None:
        """Record through ``backing`` from now on, keeping what was recorded.
        A network store's client belongs to the event loop that built it, and
        each test runs in its own loop."""
        self._backing = backing

    @override
    def _observe(self, call: Call, *, elapsed: float, started: float) -> None:
        match call:
            case ReadCall(path=path, offset=offset, length=length, data=data):
                self._reads[_read_key(path, offset, length)] = data
            case SizeCall(path=path, size=size):
                self._sizes[path] = size
            case ExistsCall(path=path, found=found):
                self._exists[path] = found
            case ListCall(path=path, entries=entries):
                # A listing is stored as its names plus each reported size as that
                # file's size, so the fixture format (and the anonymizer that
                # rewrites it) has no section of its own for it.
                self._listdirs[path] = [entry.name for entry in entries]
                for entry in entries:
                    if entry.size is not None:
                        self._sizes[join_path(path, entry.name)] = entry.size

    @override
    def _observe_failure(self, call: FailedCall, *, elapsed: float, started: float) -> None:
        if not isinstance(call.error, NotFoundError) or call.method not in self._missing:
            return
        key = _read_key(call.path, call.offset, call.length) if call.method == "read" else call.path
        self._missing[call.method].add(key)

    def dump(self) -> str:
        """Everything recorded so far as a JSON string for a test fixture
        (read results base64-encoded)."""
        payload = {
            "format": FORMAT_VERSION,
            "reads": {k: base64.b64encode(v).decode("ascii") for k, v in self._reads.items()},
            "sizes": self._sizes,
            "exists": self._exists,
            "listdirs": self._listdirs,
            "missing": {method: sorted(keys) for method, keys in self._missing.items()},
        }
        return json.dumps(payload, indent=1, sort_keys=True)


@dataclass(frozen=True, slots=True)
class _Fixture:
    """A decoded fixture; never mutated, so replays can share one."""

    reads: dict[str, bytes]
    sizes: dict[str, int]
    exists: dict[str, bool]
    listdirs: dict[str, list[str]]
    missing: dict[str, frozenset[str]]

    @classmethod
    def decode(cls, fixture_json: str, *, source: str = "this fixture") -> _Fixture:
        payload = json.loads(fixture_json)
        if payload.get("format") != FORMAT_VERSION:
            raise FixtureFormatError(
                f"{source} is not in fixture format {FORMAT_VERSION}; re-record it with the make record-fixture "
                "command `PYTHONPATH=tests uv run python -m support.recording.manifest` prints for it "
                '(tests/CLAUDE.md, "Recording a fixture")'
            )
        return cls(
            reads={k: base64.b64decode(v) for k, v in payload["reads"].items()},
            sizes=payload["sizes"],
            exists=payload["exists"],
            listdirs=payload["listdirs"],
            missing={method: frozenset(keys) for method, keys in payload["missing"].items()},
        )


@lru_cache(maxsize=8)
def _decoded_fixture(path: Path, mtime_ns: int) -> _Fixture:
    """``path`` decoded, keyed by its mtime so a re-recorded file is re-read."""
    return _Fixture.decode(load_fixture_text(path), source=path.name)


@faithful_to(ObjectStore)
class ReplayStore:
    """Answers ``ObjectStore`` calls purely from a ``RecordingStore.dump``
    fixture, with no real store and no I/O.

    A recorded miss replays as ``NotFoundError``. Any other call the fixture
    never recorded raises ``UnrecordedCallError`` and is appended to
    ``misses``, so a test whose code under test swallows the error still
    fails (``record_target`` checks ``misses`` at teardown).

    Args:
        fixture_json: A ``RecordingStore.dump`` string.
        strict: ``False`` answers every unrecorded call with
            ``NotFoundError`` and leaves ``misses`` empty, for a caller
            probing a fixture with calls its recording test never made (the
            anonymizer's vault-key probe).

    Raises:
        FixtureFormatError: The fixture is not in ``FORMAT_VERSION``.
    """

    def __init__(self, fixture_json: str, *, strict: bool = True) -> None:
        self._load(_Fixture.decode(fixture_json), strict=strict)

    def _load(self, fixture: _Fixture, *, strict: bool) -> None:
        self._fixture = fixture
        self._strict = strict
        #: Every unrecorded call so far, as ``"<method> <description>"``.
        self.misses: list[str] = []

    @classmethod
    def from_path(cls, path: Path) -> ReplayStore:
        """Construct from a fixture file (``.gz`` is decompressed; see
        ``load_fixture_text``). The decoded fixture is cached per process
        while the file is unchanged; each store keeps its own ``misses``.

        Raises:
            FixtureFormatError: The fixture is not in ``FORMAT_VERSION``.
        """
        replay = cls.__new__(cls)
        replay._load(_decoded_fixture(path, path.stat().st_mtime_ns), strict=True)
        return replay

    def _miss(self, method: str, key: str, description: str) -> Exception:
        """The error for a call with no recorded result: ``NotFoundError`` for
        a recorded miss (or any miss, replaying non-strictly), else
        ``UnrecordedCallError``, logged in ``misses``."""
        if not self._strict or key in self._fixture.missing.get(method, frozenset()):
            return NotFoundError(f"ReplayStore: recorded {method}() of {description} found nothing", ref=key)
        self.misses.append(f"{method} {description}")
        return UnrecordedCallError(
            f"ReplayStore has no recorded {method}() for {description} -- the fixture does not cover this call; "
            're-record it (tests/CLAUDE.md, "Recording a fixture")'
        )

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        key = _read_key(path, offset, length)
        try:
            return self._fixture.reads[key]
        except KeyError:
            raise self._miss("read", key, f"path={path!r} offset={offset} length={length}") from None

    async def size(self, path: str) -> int:
        try:
            return self._fixture.sizes[path]
        except KeyError:
            raise self._miss("size", path, repr(path)) from None

    async def exists(self, path: str) -> bool:
        try:
            return self._fixture.exists[path]
        except KeyError:
            raise self._miss("exists", path, repr(path)) from None

    async def close(self) -> None:
        """Nothing to release: a replay holds only the fixture."""

    async def listdir(self, path: str) -> list[Entry]:
        """The recorded listing, each file with the size recorded for it (by
        a listing or a ``size`` call), ``None`` where none was recorded."""
        try:
            names = self._fixture.listdirs[path]
        except KeyError:
            raise self._miss("listdir", path, repr(path)) from None
        return sorted(
            (Entry(name, self._fixture.sizes.get(join_path(path, name))) for name in names), key=lambda e: e.name
        )
