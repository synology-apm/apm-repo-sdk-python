"""Shared fixtures for ``tests/integration/`` -- ``record_target``, the
replay of the committed recorded fixtures, used nowhere under
``tests/unit/``."""

from __future__ import annotations

import copy
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from support.recording.fixture_store import STORE_SOURCE, ReplayStore
from synology_apm_repo.sdk.dedup.pool import BucketReader
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import ContentSource, Node, RestorableUnit

_FIXTURES = Path(__file__).parent.parent / "fixtures"

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


async def _guarded_bucket_read_chunk_blocks(self: object, *args: object, **kwargs: object) -> None:
    raise ContentRecordingBlocked(_BLOCKED_MESSAGE)


def _install_content_guard(request: pytest.FixtureRequest) -> None:
    """Installs the content guard for the rest of this one test (idempotent
    per test node), whatever serves the store, so a content read fails the
    same way whether or not the fixture happens to cover it (a background
    preview).
    Two choke points cover the two layers that read real content:
    ``RestorableUnit.of()`` (every unit a provider builds) and
    ``BucketReader.read_chunk``/``read_chunks``/``read_chunk_blocks`` (the
    dedup layer, e.g. a ``DedupFile`` from ``DedupRepo.open_file()``). A raw
    ``ObjectStore.read()`` stays unguarded: it can't be told apart from a
    catalog-metadata read."""
    if request.node.stash.get(_CONTENT_GUARD_INSTALLED, False):
        return
    request.node.stash[_CONTENT_GUARD_INSTALLED] = True
    for patcher in (
        patch.object(RestorableUnit, "of", classmethod(_guarded_restorable_unit_of)),
        patch.object(BucketReader, "read_chunk", _guarded_bucket_read_chunk),
        patch.object(BucketReader, "read_chunks", _guarded_bucket_read_chunks),
        patch.object(BucketReader, "read_chunk_blocks", _guarded_bucket_read_chunk_blocks),
    ):
        patcher.start()
        request.addfinalizer(patcher.stop)


@pytest.fixture
def record_target(request: pytest.FixtureRequest) -> Callable[..., Awaitable[ObjectStore]]:
    """``store = await record_target("name.json.gz")`` -- the one call every
    ``tests/integration/**/test_*.py`` file makes to get its store.

    Returns ``ReplayStore.from_path(_FIXTURES / name)``, and fails the test
    at teardown if it made any call the fixture never recorded, even one the
    code under test caught. A ``StoreSource`` set in ``STORE_SOURCE`` serves
    the store instead.

    ``allow_content=True`` opts one call out of the content guard
    (``ContentRecordingBlocked``); pass it only for a test that needs content
    bytes as a structural oracle (see ``tests/CLAUDE.md``)."""
    source = request.config.stash.get(STORE_SOURCE, None)

    async def _target(name: str, *, allow_content: bool = False) -> ObjectStore:
        if not allow_content:
            _install_content_guard(request)
        if source is not None:
            return await source(request, _FIXTURES / name)
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

    return _target
