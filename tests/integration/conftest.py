"""Shared fixtures for ``tests/integration/`` -- the ``--record-against``
recording/replay machinery, used nowhere under ``tests/unit/``."""

from __future__ import annotations

import dataclasses
import importlib.util
import sys
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest

from synology_apm_repo.sdk.dedup.pool import BucketReader
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.recording import RecordingStore, ReplayStore, write_fixture_text
from synology_apm_repo.sdk.units.base import ContentSource, RestorableUnit

_FIXTURES = Path(__file__).parent.parent / "fixtures"

_PASSED = pytest.StashKey[bool]()
_CONTENT_GUARD_INSTALLED = pytest.StashKey[bool]()


class ContentRecordingBlocked(RuntimeError):
    """Raised during a ``--record-against`` session when a test reads real
    backed-up content -- a unit's own bytes via ``RestorableUnit.open()``,
    or a dedup chunk's own plaintext via ``BucketReader.read_chunk``/
    ``read_chunks`` -- without declaring it needs to. See ``record_target``'s
    ``allow_content`` parameter."""


_BLOCKED_MESSAGE = (
    "this test read real backed-up content during --record-against without record_target(..., "
    "allow_content=True) -- per tests/CLAUDE.md, a replay test proves structural/addressing "
    "correctness, not content meaning: narrow the test to a structural check (kind/size/is_leaf) "
    "instead, or pass allow_content=True if it genuinely needs real content bytes as a structural "
    "oracle (a hash/signature check that never asserts on the content's own meaning)"
)


class _GuardedContentSource:
    """Wraps a real ``ContentSource`` for the duration of a recording
    session, refusing to materialize its actual bytes unless the owning
    test opted in via ``record_target(..., allow_content=True)``.
    ``size``/``supports_concurrent_export`` pass through unguarded -- they
    carry no content, only metadata a provider already resolved without
    reading real bytes."""

    def __init__(self, inner: ContentSource) -> None:
        self._inner = inner

    @property
    def size(self) -> int | None:
        return self._inner.size

    @property
    def supports_concurrent_export(self) -> bool:
        return self._inner.supports_concurrent_export

    async def read(self, *args: object, **kwargs: object) -> bytes:
        raise ContentRecordingBlocked(_BLOCKED_MESSAGE)

    def stream(self, *args: object, **kwargs: object) -> object:
        raise ContentRecordingBlocked(_BLOCKED_MESSAGE)

    async def export_to(self, *args: object, **kwargs: object) -> object:
        raise ContentRecordingBlocked(_BLOCKED_MESSAGE)


#: Captured at import time, before any test can patch ``RestorableUnit.open``
#: -- always the real, unwrapped implementation, regardless of how many
#: guard layers get installed/torn down across the session.
_real_restorable_unit_open = RestorableUnit.open


def _guarded_restorable_unit_open(self: RestorableUnit) -> ContentSource:
    return _GuardedContentSource(_real_restorable_unit_open(self))  # type: ignore[return-value]


async def _guarded_bucket_read_chunk(self: object, *args: object, **kwargs: object) -> bytes:
    raise ContentRecordingBlocked(_BLOCKED_MESSAGE)


async def _guarded_bucket_read_chunks(self: object, *args: object, **kwargs: object) -> dict[int, bytes]:
    raise ContentRecordingBlocked(_BLOCKED_MESSAGE)


def _install_content_guard(request: pytest.FixtureRequest) -> None:
    """Installs the ``RestorableUnit.open()``/``BucketReader.read_chunk``/
    ``read_chunks`` guard for the rest of this one test -- idempotent per
    test node, since ``record_target`` may be called more than once (e.g.
    a test recording against two fixture names). Two separate choke
    points, not one: ``RestorableUnit.open()`` covers every ``units``-layer
    consumer (``FsProvider``, ``DriveProvider``, ``MailProvider``, ...)
    without touching each provider's own construction site;
    ``BucketReader.read_chunk``/``read_chunks`` covers the dedup layer's
    own real-content reads (a ``DedupFile`` built directly off
    ``DedupRepo.open_file()``, or ``Pool.read_chunk()`` called
    directly) -- neither choke point alone reaches the other layer. A raw
    ``ObjectStore.read()`` call straight on a real backend (this project's
    lowest-level crypto/format tests, e.g. ``test_crypto.py``'s real-bytes
    chunk-decrypt regression test) is deliberately left unguarded: it
    can't be distinguished from a catalog-metadata read without a path
    heuristic, and by the time
    a test is reaching for the raw store directly it has already made a
    conscious, reviewable choice -- unlike the incidental
    ``.unit(node).open().read()`` this guard exists to catch."""
    if request.node.stash.get(_CONTENT_GUARD_INSTALLED, False):
        return
    request.node.stash[_CONTENT_GUARD_INSTALLED] = True
    for patcher in (
        patch.object(RestorableUnit, "open", _guarded_restorable_unit_open),
        patch.object(BucketReader, "read_chunk", _guarded_bucket_read_chunk),
        patch.object(BucketReader, "read_chunks", _guarded_bucket_read_chunks),
    ):
        patcher.start()
        request.addfinalizer(patcher.stop)


#: Paths this session actually wrote via record_target() -- populated by
#: pytest_sessionfinish's own write pass below (see _RECORDING_SESSIONS),
#: consumed by that same hook's post-recording anonymize pass. A plain
#: module-level set (not a fixture) since it must survive across every test
#: in the session, not just one.
_WRITTEN_FIXTURES: set[Path] = set()


@dataclasses.dataclass
class _RecordingSession:
    """One fixture name's shared recording state for the whole
    ``--record-against`` invocation -- shared, not built fresh per test, so
    multiple tests recording into the same fixture accumulate into the one
    store instead of each overwriting the other's work."""

    store: RecordingStore
    #: AND-reduced across every test that calls record_target() with this
    #: fixture's name: stays True only if every one of them passes. A test
    #: that never even touches this fixture leaves it alone.
    all_passed: bool = True


#: Keyed by the fixture path each name resolves to, populated the first time
#: any test in this invocation calls record_target(name) -- shared, not
#: rebuilt per test, so multiple tests recording into the same fixture
#: accumulate into one RecordingStore instead of each overwriting the
#: other's work.
_RECORDING_SESSIONS: dict[Path, _RecordingSession] = {}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--record-against",
        default=None,
        metavar="local:<path>|profile:<profile-name>",
        help="Record every tests/fixtures/*.json.gz a test's own record_target() call "
        "asks for against a real backend, instead of replaying the committed one. "
        "'local:' uses the given path directly as the store root (absolute, or "
        "relative to cwd); 'profile:' goes through profiles.build_store(). Run "
        "pytest once per real root you want to record from, scoped to just the "
        "test(s) that root backs (e.g. -k or a node id) -- one invocation, one "
        "backend.",
    )
    parser.addoption(
        "--no-anonymize",
        action="store_true",
        default=False,
        help="With --record-against: skip the automatic scripts/anonymize_catalog_metadata.py "
        "pass this session would otherwise run (once, at session end) over exactly the "
        "fixtures this run actually wrote. Has no effect without --record-against -- a normal "
        "run never writes a fixture at all.",
    )


def _load_anonymize_module() -> ModuleType:
    """``scripts/`` isn't an installed package, so this loads it by path --
    the same technique ``tests/unit/scripts/test_anonymize_catalog_metadata.py``
    already uses. Registered in ``sys.modules`` before executing because
    the module's ``@dataclass`` resolves its ``from __future__ import
    annotations`` string annotations via ``sys.modules[cls.__module__]``
    at class-definition time."""
    script_path = Path(__file__).parent.parent.parent / "scripts" / "anonymize_catalog_metadata.py"
    spec = importlib.util.spec_from_file_location("anonymize_catalog_metadata", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Writes every ``record_target()`` recording accumulated this session
    (see ``_RECORDING_SESSIONS``/``record_target`` below) whose sharing
    tests *all* passed, then runs ``scripts/anonymize_catalog_metadata.py``
    once, in one batch, over exactly the fixtures this session actually
    wrote -- so recording a fixture is anonymized by default rather than
    relying on a separate manual step someone can forget (see
    ``CONTRIBUTING.md``'s "Sample data" section). Both passes are a no-op
    when nothing was recorded (every normal `make test`/CI run); the
    anonymize pass alone is skipped if ``--no-anonymize`` was passed.

    One anonymize batch across every fixture this run wrote, not one call
    per fixture right after each is written: some sensitive values only
    survive as a path segment in *one* fixture while their own owning
    catalog row lives in a *different* fixture recorded in the same run
    -- anonymizing each in isolation as soon as it's written would miss
    that."""
    for path, recording in _RECORDING_SESSIONS.items():
        if recording.all_passed:
            write_fixture_text(path, recording.store.dump())
            _WRITTEN_FIXTURES.add(path)
        else:
            print(f"skipped writing {path} -- at least one test sharing it failed this run")

    if not _WRITTEN_FIXTURES or session.config.getoption("--no-anonymize"):
        return
    anonymize_module = _load_anonymize_module()
    changed = anonymize_module.anonymize_fixtures(sorted(_WRITTEN_FIXTURES))
    for path in changed:
        print(f"anonymized {path}")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Iterator[None]:
    """Stashes whether a test's own call phase passed, so ``record_target``'s
    own finalizer (below) can mark a shared recording as not safe to write
    when any test sharing it fails against the real backend it was pointed
    at -- "if the sample fails, the recording fails," not silently kept or
    patched around."""
    outcome = yield
    report = outcome.get_result()  # type: ignore[attr-defined]
    if report.when == "call":
        item.stash[_PASSED] = report.passed


async def _resolve_record_backend(spec: str) -> ObjectStore:
    kind, _, rest = spec.partition(":")
    if kind == "local":
        if not rest:
            raise SystemExit(
                "--record-against=local:<path> requires a non-empty path -- no default sample directory is assumed."
            )
        root = Path(rest)
        if not root.is_dir():
            raise SystemExit(f"--record-against=local:{rest} -- {root} is not a directory")
        return LocalFsStore(root)
    if kind == "profile":
        from synology_apm_repo.sdk.profiles import build_store

        return await build_store(rest)
    raise SystemExit(f"--record-against must be 'local:<name>' or 'profile:<name>', got {spec!r}")


@pytest.fixture
def record_target(request: pytest.FixtureRequest) -> Callable[..., Awaitable[ObjectStore]]:
    """``store = await record_target("name.json.gz")`` -- the one call every
    ``tests/integration/**/test_*.py`` file makes to get its store,
    replacing the old ``ReplayStore.from_path(_FIXTURES / name)`` one-liner.

    ``allow_content=True`` opts one call out of the real-content recording
    guard installed below (default: guarded) -- pass it only for a test
    that genuinely needs to read real backed-up content as a structural
    oracle (per ``tests/CLAUDE.md``'s own phrase); everything else gets
    ``ContentRecordingBlocked`` at the moment it would have captured real
    content into the fixture, instead of that mistake surfacing only in a
    later manual audit.

    No ``--record-against``: identical behavior to that one-liner (this is
    every normal ``make test``/CI run). With it: wraps the real backend
    ``--record-against`` resolves to in a ``RecordingStore`` -- shared
    across every test in this invocation that requests the same ``name``
    (see ``_RECORDING_SESSIONS``), not a fresh one per test. Multiple tests
    whose own calls aren't a subset of each other (a common shape when one
    fixture backs several scenarios) simply accumulate into the one shared
    store instead of each overwriting the other's recording -- point
    ``TEST=`` at every test sharing a fixture (or the whole file) in one
    ``pytest --record-against=...`` invocation and it merges automatically;
    no throwaway script needed.

    The shared recording is written to ``tests/fixtures/name`` once, at
    session end (see ``pytest_sessionfinish`` above), only if *every* test
    that touched this name during the run passed — one shared fixture is
    one shared contract among every test backed by it, so one of them
    failing withholds the whole write rather than committing a recording
    known to be wrong for at least one sharing test. A test whose
    assertions no longer hold against the real backend (moved, renamed,
    restructured data) simply fails, like any other test failure, and
    writes nothing — there is deliberately no fallback path that tries to
    keep a stale recipe "working" against changed real data.

    Every fixture actually written this way is anonymized automatically at
    session end (see ``pytest_sessionfinish`` above) unless ``--no-
    anonymize`` is passed — recording a real fixture and forgetting the
    separate anonymize step is exactly the kind of mistake that puts real
    data in a commit, so this isn't left to a person remembering it."""
    spec = request.config.getoption("--record-against")

    async def _target(name: str, *, allow_content: bool = False) -> ObjectStore:
        if spec is None:
            return ReplayStore.from_path(_FIXTURES / name)
        if not allow_content:
            _install_content_guard(request)
        path = _FIXTURES / name
        recording = _RECORDING_SESSIONS.get(path)
        if recording is None:
            backend = await _resolve_record_backend(spec)
            recording = _RecordingSession(store=RecordingStore(backend))
            _RECORDING_SESSIONS[path] = recording

        def _mark_result() -> None:
            if not request.node.stash.get(_PASSED, False):
                recording.all_passed = False

        request.addfinalizer(_mark_result)
        return recording.store

    return _target
