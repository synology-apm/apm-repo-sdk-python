"""Shared fixtures for ``tests/integration/`` -- the ``--record-against``
recording/replay machinery, used nowhere under ``tests/unit/``."""

from __future__ import annotations

import copy
import dataclasses
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from support.recording import anonymize_catalog_metadata
from support.recording.fixture_store import AliasedStore, RecordingStore, ReplayStore, write_fixture_text
from support.recording.manifest import load_targets, sample_directory_renames
from synology_apm_repo.sdk.dedup.pool import BucketReader
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import ContentSource, Node, RestorableUnit

_FIXTURES = Path(__file__).parent.parent / "fixtures"

_PASSED = pytest.StashKey[bool]()
_CONTENT_GUARD_INSTALLED = pytest.StashKey[bool]()


class ContentRecordingBlocked(RuntimeError):
    """Raised when a test reads real backed-up content without
    ``record_target(..., allow_content=True)``."""


_BLOCKED_MESSAGE = (
    "this test read real backed-up content without record_target(..., "
    "allow_content=True) -- per tests/CLAUDE.md, a replay test proves structural/addressing "
    "correctness, not content meaning: narrow the test to a structural check (kind/size/is_leaf) "
    "instead, or pass allow_content=True if it genuinely needs real content bytes as a structural "
    "oracle (a hash/signature check that never asserts on the content's own meaning)"
)


async def _blocked_read(*args: object, **kwargs: object) -> object:
    raise ContentRecordingBlocked(_BLOCKED_MESSAGE)


def _blocked_stream(*args: object, **kwargs: object) -> object:
    raise ContentRecordingBlocked(_BLOCKED_MESSAGE)


def _guarded_content(inner: ContentSource) -> ContentSource:
    """A shallow copy of ``inner`` whose byte-reading methods refuse to run.
    A copy rather than a wrapper, so ``isinstance`` checks and metadata
    (``size``, a disk's fragments) still see the real type."""
    guarded = copy.copy(inner)
    for name in ("read", "planned_bytes", "export_range"):
        setattr(guarded, name, _blocked_read)
    setattr(guarded, "stream", _blocked_stream)  # noqa: B010 -- an instance-level override mypy can't type
    return guarded


#: The real ``RestorableUnit.of``, captured before any test can patch it.
_real_restorable_unit_of = RestorableUnit.of.__func__  # type: ignore[attr-defined]


def _guarded_restorable_unit_of(
    cls: type[RestorableUnit], /, node: Node, content: ContentSource, **changes: Any
) -> RestorableUnit:
    return _real_restorable_unit_of(cls, node, _guarded_content(content), **changes)  # type: ignore[no-any-return]


async def _guarded_bucket_read_chunk(self: object, *args: object, **kwargs: object) -> bytes:
    raise ContentRecordingBlocked(_BLOCKED_MESSAGE)


async def _guarded_bucket_read_chunks(self: object, *args: object, **kwargs: object) -> dict[int, bytes]:
    raise ContentRecordingBlocked(_BLOCKED_MESSAGE)


def _install_content_guard(request: pytest.FixtureRequest) -> None:
    """Installs the content guard for the rest of this one test (idempotent
    per test node), on a replay and a recording alike, so a read the
    recording never made (a background preview) fails the same way in both.
    Two choke points cover the two layers that read real content:
    ``RestorableUnit.of()`` (every unit a provider builds) and
    ``BucketReader.read_chunk``/``read_chunks`` (the dedup layer, e.g. a
    ``DedupFile`` from ``DedupRepo.open_file()``). A raw ``ObjectStore.read()``
    stays unguarded: it can't be told apart from a catalog-metadata read."""
    if request.node.stash.get(_CONTENT_GUARD_INSTALLED, False):
        return
    request.node.stash[_CONTENT_GUARD_INSTALLED] = True
    for patcher in (
        patch.object(RestorableUnit, "of", classmethod(_guarded_restorable_unit_of)),
        patch.object(BucketReader, "read_chunk", _guarded_bucket_read_chunk),
        patch.object(BucketReader, "read_chunks", _guarded_bucket_read_chunks),
    ):
        patcher.start()
        request.addfinalizer(patcher.stop)


#: Paths ``pytest_sessionfinish`` wrote this session, then anonymizes.
_WRITTEN_FIXTURES: set[Path] = set()


@dataclasses.dataclass
class _RecordingSession:
    """One fixture name's recording state, shared by every test in the
    ``--record-against`` invocation that requests it."""

    store: RecordingStore
    #: True only if every test that requested this fixture passed.
    all_passed: bool = True


#: Keyed by fixture path; populated the first time a test requests that name.
_RECORDING_SESSIONS: dict[Path, _RecordingSession] = {}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--record-against",
        default=None,
        metavar="local:<path>|profile:<profile-name>",
        help="Record every tests/fixtures/*.json.gz a test's own record_target() call "
        "asks for against a real backend, instead of replaying the committed one. "
        "'local:' uses the given path directly as the store root (absolute, or "
        "relative to cwd); 'profile:' goes through profiles.store_from_profile(). Run "
        "pytest once per real root you want to record from, scoped to just the "
        "test(s) that root backs (e.g. -k or a node id) -- one invocation, one "
        "backend.",
    )
    parser.addoption(
        "--no-anonymize",
        action="store_true",
        default=False,
        help="With --record-against: skip the automatic tests/support/recording/anonymize_catalog_metadata.py "
        "pass this session would otherwise run (once, at session end) over exactly the "
        "fixtures this run actually wrote. Has no effect without --record-against -- a normal "
        "run never writes a fixture at all.",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Refuses ``--record-against`` under xdist: each worker would keep its own
    ``_RECORDING_SESSIONS`` and write, then anonymize, only its share of a
    fixture's calls."""
    # -n and --tx are xdist's two ways to start workers; --dist alone starts none.
    distributed = config.getoption("numprocesses", None) or config.getoption("tx", None)
    if config.getoption("--record-against") and distributed:
        raise pytest.UsageError("--record-against records in one process; drop -n/--tx")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Writes every recording whose sharing tests all passed, then runs
    ``tests/support/recording/anonymize_catalog_metadata.py`` once over exactly the fixtures
    written (skipped by ``--no-anonymize``); a no-op on a normal run.

    Anonymization is one batch, not per fixture: a sensitive value can
    survive as a path segment in one fixture while its owning catalog row
    lives in another fixture from the same run."""
    for path, recording in _RECORDING_SESSIONS.items():
        if recording.all_passed:
            write_fixture_text(path, recording.store.dump())
            _WRITTEN_FIXTURES.add(path)
        else:
            print(f"skipped writing {path} -- at least one test sharing it failed this run")  # noqa: T201

    if not _WRITTEN_FIXTURES or session.config.getoption("--no-anonymize"):
        return
    changed = anonymize_catalog_metadata.anonymize_fixtures(sorted(_WRITTEN_FIXTURES))
    for path in changed:
        print(f"anonymized {path}")  # noqa: T201


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Iterator[None]:
    """Stashes whether a test's call phase passed, so ``record_target``'s
    finalizer can withhold a shared recording when any sharing test fails."""
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
        return AliasedStore(LocalFsStore(root), sample_directory_renames(load_targets()))
    if kind == "profile":
        from synology_apm_repo.sdk.profiles import store_from_profile

        return await store_from_profile(rest)
    raise SystemExit(f"--record-against must be 'local:<path>' or 'profile:<name>', got {spec!r}")


@pytest.fixture
def record_target(request: pytest.FixtureRequest) -> Callable[..., Awaitable[ObjectStore]]:
    """``store = await record_target("name.json.gz")`` -- the one call every
    ``tests/integration/**/test_*.py`` file makes to get its store.

    Without ``--record-against``: returns ``ReplayStore.from_path(_FIXTURES /
    name)``, and fails the test at teardown if it made any call the fixture
    never recorded, even one the code under test caught. With it: a
    ``RecordingStore`` around the real backend, shared by every test in the
    invocation requesting the same ``name`` (see
    ``_RECORDING_SESSIONS``), so tests whose calls don't subset each other
    merge into one recording.

    The recording is written to ``tests/fixtures/name`` at session end
    (``pytest_sessionfinish``) only if every test that touched the name
    passed, and is then anonymized unless ``--no-anonymize`` is passed.

    ``allow_content=True`` opts one call out of the real-content guard
    (``ContentRecordingBlocked``, installed on a replay too); pass it only
    for a test that needs real content bytes as a structural oracle (see
    ``tests/CLAUDE.md``)."""
    spec = request.config.getoption("--record-against")

    async def _target(name: str, *, allow_content: bool = False) -> ObjectStore:
        if not allow_content:
            _install_content_guard(request)
        if spec is None:
            replay = ReplayStore.from_path(_FIXTURES / name)

            def _check_misses() -> None:
                # Code under test may have caught the UnrecordedCallError.
                if replay.misses:
                    pytest.fail(
                        f"{name} has no recording for {len(replay.misses)} call(s) this test made, "
                        f"first: {replay.misses[0]} -- re-record it",
                        pytrace=False,
                    )

            request.addfinalizer(_check_misses)
            return replay
        path = _FIXTURES / name
        # A fresh backend per call: a network store's client is bound to the
        # event loop that built it, and each test runs in its own loop.
        backend = await _resolve_record_backend(spec)
        recording = _RECORDING_SESSIONS.get(path)
        if recording is None:
            recording = _RecordingSession(store=RecordingStore(backend))
            _RECORDING_SESSIONS[path] = recording
        else:
            recording.store.rebind(backend)

        def _mark_result() -> None:
            if not request.node.stash.get(_PASSED, False):
                recording.all_passed = False

        request.addfinalizer(_mark_result)
        return recording.store

    return _target
