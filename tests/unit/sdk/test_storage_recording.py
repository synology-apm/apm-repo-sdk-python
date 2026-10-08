"""Unit tests for ``synology_apm_repo.sdk.storage.recording``: ``TracingStore``
and the ``InstrumentedStore`` call forwarding it is built on. The other
``InstrumentedStore``, ``tests/support/recording/fixture_store.py``'s
``RecordingStore``, is tested in ``tests/unit/support/test_recording_fixture_store.py``."""

from __future__ import annotations

from pathlib import Path

import pytest

from support.store_fakes import CloseCountingStore, FailingStore
from synology_apm_repo.sdk.errors import NotFoundError, StorageBackendError
from synology_apm_repo.sdk.storage import recording as recording_module
from synology_apm_repo.sdk.storage.base import Entry
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.recording import TraceEvent, TracingStore


@pytest.fixture
def backing(tmp_path: Path) -> LocalFsStore:
    (tmp_path / "a.txt").write_bytes(b"0123456789")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"hello world")
    return LocalFsStore(tmp_path)


async def test_tracing_passes_through_to_backing(backing: LocalFsStore) -> None:
    events: list[TraceEvent] = []
    tracer = TracingStore(backing, events.append)
    assert await tracer.read("a.txt") == b"0123456789"
    assert await tracer.size("a.txt") == 10
    assert await tracer.exists("a.txt") is True
    assert await tracer.listdir("") == [Entry("a.txt", 10), Entry("sub", None)]


async def test_tracing_close_forwards_to_the_backing_store() -> None:
    fake = CloseCountingStore()
    await TracingStore(fake, lambda _event: None).close()
    assert fake.close_count == 1


async def test_tracing_emits_one_event_per_call_with_correct_shape(
    backing: LocalFsStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Each wrapped call reads time.time() once and time.monotonic() twice.
    # Rebinding recording.py's own ``time`` name (not the global module
    # asyncio also uses) makes every elapsed/started exact, so an elapsed
    # left at 0.0 fails where an "elapsed >= 0.0" check would pass.
    ticks = iter(n / 1000 for n in range(1, 9))
    starts = iter(1_000.0 + n for n in range(4))

    class _FakeTime:
        monotonic = staticmethod(lambda: next(ticks))
        time = staticmethod(lambda: next(starts))

    monkeypatch.setattr(recording_module, "time", _FakeTime())

    events: list[TraceEvent] = []
    tracer = TracingStore(backing, events.append)
    await tracer.read("a.txt", offset=3, length=4)
    await tracer.size("a.txt")
    await tracer.exists("nope")
    await tracer.listdir("sub")

    assert [e.method for e in events] == ["read", "size", "exists", "listdir"]
    assert [round(e.elapsed, 3) for e in events] == [0.001, 0.001, 0.001, 0.001]
    assert [e.started for e in events] == [1_000.0, 1_001.0, 1_002.0, 1_003.0]
    read_event = events[0]
    assert read_event.path == "a.txt"
    assert read_event.offset == 3
    assert read_event.length == 4
    assert read_event.result_length == 4  # len(b"3456")

    listdir_event = events[3]
    assert listdir_event.result_length == 1  # len(["b.txt"])


async def test_tracing_reports_a_failed_call_and_still_raises_its_error(backing: LocalFsStore) -> None:
    events: list[TraceEvent] = []
    tracer = TracingStore(backing, events.append)
    with pytest.raises(NotFoundError, match="no such file"):
        await tracer.read("does-not-exist.txt", 4, 8)
    [event] = events
    assert (event.method, event.path, event.offset, event.length) == ("read", "does-not-exist.txt", 4, 8)
    assert (event.error, event.result_length) == ("NotFoundError", None)


@pytest.mark.parametrize("method", ["size", "exists", "listdir"])
async def test_tracing_reports_a_failure_of_every_method(method: str) -> None:
    events: list[TraceEvent] = []
    tracer = TracingStore(FailingStore(), events.append)
    with pytest.raises(StorageBackendError, match="down"):
        await getattr(tracer, method)("p")
    assert [(e.method, e.error) for e in events] == [(method, "StorageBackendError")]


async def test_tracing_a_listing_is_one_listdir_event_counting_its_entries(backing: LocalFsStore) -> None:
    events: list[TraceEvent] = []
    tracer = TracingStore(backing, events.append)

    entries = await tracer.listdir("")

    assert entries == [Entry("a.txt", 10), Entry("sub", None)]
    assert [(e.method, e.path, e.result_length) for e in events] == [("listdir", "", 2)]
    assert events[0].started > 0
