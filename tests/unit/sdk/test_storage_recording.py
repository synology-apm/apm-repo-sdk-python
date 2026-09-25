"""Unit tests for ``synology_apm_repo.sdk.storage.recording``."""

from __future__ import annotations

from pathlib import Path

import pytest

from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage import recording as recording_module
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.recording import (
    RecordingStore,
    ReplayStore,
    TraceEvent,
    TracingStore,
    load_fixture_text,
    write_fixture_text,
)


@pytest.fixture
def backing(tmp_path: Path) -> LocalFsStore:
    (tmp_path / "a.txt").write_bytes(b"0123456789")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"hello world")
    return LocalFsStore(tmp_path)


class _FakeCloseableStore:
    """A minimal ``ObjectStore`` that also satisfies ``AsyncCloseable`` --
    stands in for ``S3Store``/``AzureStore`` (real backends, not exercised
    here) just enough to prove ``RecordingStore``/``TracingStore``'s own
    ``aclose()`` actually forwards to a backing store that has one."""

    def __init__(self) -> None:
        self.closed = False

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        raise NotImplementedError

    async def size(self, path: str) -> int:
        raise NotImplementedError

    async def exists(self, path: str) -> bool:
        raise NotImplementedError

    async def listdir(self, path: str) -> list[str]:
        raise NotImplementedError

    async def aclose(self) -> None:
        self.closed = True


async def test_recording_passes_through_to_backing(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    assert await rec.read("a.txt") == b"0123456789"
    assert await rec.read("a.txt", offset=3, length=4) == b"3456"
    assert await rec.size("a.txt") == 10
    assert await rec.exists("a.txt") is True
    assert await rec.exists("nope") is False
    assert await rec.listdir("") == ["a.txt", "sub"]


async def test_recording_aclose_is_a_no_op_over_a_backing_store_with_nothing_to_close(backing: LocalFsStore) -> None:
    # LocalFsStore has no aclose() of its own -- must not raise (regression
    # guard for Session.close()'s own isinstance(store, AsyncCloseable)
    # check: RecordingStore/TracingStore always satisfy AsyncCloseable now,
    # regardless of what they wrap, so this path must stay a safe no-op).
    await RecordingStore(backing).aclose()


async def test_recording_aclose_forwards_to_an_asynccloseable_backing_store() -> None:
    # The real regression this guards: Session.close()'s own isinstance(
    # store, AsyncCloseable) check used to find no aclose() at all on a
    # RecordingStore wrapping a real S3Store/AzureStore, silently skipping
    # its real aiohttp connector's own close.
    fake = _FakeCloseableStore()
    await RecordingStore(fake).aclose()
    assert fake.closed is True


async def test_dump_and_replay_round_trip(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    await rec.read("a.txt")
    await rec.read("a.txt", offset=3, length=4)
    await rec.read("sub/b.txt")
    await rec.size("a.txt")
    await rec.exists("a.txt")
    await rec.exists("nope")
    await rec.listdir("")

    fixture = rec.dump()
    replay = ReplayStore(fixture)

    assert await replay.read("a.txt") == b"0123456789"
    assert await replay.read("a.txt", offset=3, length=4) == b"3456"
    assert await replay.read("sub/b.txt") == b"hello world"
    assert await replay.size("a.txt") == 10
    assert await replay.exists("a.txt") is True
    assert await replay.exists("nope") is False
    assert await replay.listdir("") == ["a.txt", "sub"]


async def test_replay_raises_for_unrecorded_read(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    await rec.read("a.txt")
    replay = ReplayStore(rec.dump())

    with pytest.raises(NotFoundError):
        await replay.read("sub/b.txt")  # never recorded


async def test_replay_raises_for_unrecorded_read_with_different_offset(backing: LocalFsStore) -> None:
    # a fixture covering read("a.txt", 0, None) does not automatically
    # cover read("a.txt", 3, 4) — every distinct call must be recorded.
    rec = RecordingStore(backing)
    await rec.read("a.txt")
    replay = ReplayStore(rec.dump())

    assert await replay.read("a.txt") == b"0123456789"
    with pytest.raises(NotFoundError):
        await replay.read("a.txt", offset=3, length=4)


async def test_replay_raises_for_unrecorded_exists_rather_than_defaulting_false(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    await rec.exists("a.txt")  # only this one path was ever queried
    replay = ReplayStore(rec.dump())

    assert await replay.exists("a.txt") is True
    with pytest.raises(NotFoundError):
        await replay.exists("sub")


async def test_replay_raises_for_unrecorded_size(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    replay = ReplayStore(rec.dump())
    with pytest.raises(NotFoundError):
        await replay.size("a.txt")


async def test_replay_raises_for_unrecorded_listdir(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    replay = ReplayStore(rec.dump())
    with pytest.raises(NotFoundError):
        await replay.listdir("")


async def test_fixture_is_stable_json_text(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    await rec.read("a.txt")
    fixture = rec.dump()
    assert isinstance(fixture, str)
    # re-dumping identical recorded state produces byte-identical text
    # (sort_keys=True) — fixtures should diff cleanly in version control.
    rec2 = RecordingStore(backing)
    await rec2.read("a.txt")
    assert rec2.dump() == fixture


# -- gzip-compressed fixture I/O (load_fixture_text/write_fixture_text/ReplayStore.from_path) --


async def test_write_and_load_fixture_text_round_trips_through_gzip(backing: LocalFsStore, tmp_path: Path) -> None:
    rec = RecordingStore(backing)
    await rec.read("a.txt")
    fixture = rec.dump()

    gz_path = tmp_path / "fixture.json.gz"
    write_fixture_text(gz_path, fixture)
    assert load_fixture_text(gz_path) == fixture


def test_write_fixture_text_without_gz_suffix_writes_plain_text(tmp_path: Path) -> None:
    path = tmp_path / "fixture.json"
    write_fixture_text(path, "hello")
    assert path.read_text() == "hello"
    assert load_fixture_text(path) == "hello"


async def test_replay_store_from_path_reads_gzip_compressed_fixture(backing: LocalFsStore, tmp_path: Path) -> None:
    rec = RecordingStore(backing)
    await rec.read("a.txt")
    gz_path = tmp_path / "fixture.json.gz"
    write_fixture_text(gz_path, rec.dump())

    replay = ReplayStore.from_path(gz_path)
    assert await replay.read("a.txt") == b"0123456789"


# -- TracingStore (the --trace mount point) ---------------------------------


async def test_tracing_passes_through_to_backing(backing: LocalFsStore) -> None:
    events: list[TraceEvent] = []
    tracer = TracingStore(backing, events.append)
    assert await tracer.read("a.txt") == b"0123456789"
    assert await tracer.size("a.txt") == 10
    assert await tracer.exists("a.txt") is True
    assert await tracer.listdir("") == ["a.txt", "sub"]


async def test_tracing_aclose_is_a_no_op_over_a_backing_store_with_nothing_to_close(backing: LocalFsStore) -> None:
    # TracingStore always satisfies AsyncCloseable now, regardless of
    # what it wraps, so this must stay a safe no-op even over a backing
    # store (LocalFsStore) with no aclose() of its own.
    await TracingStore(backing, lambda _event: None).aclose()


async def test_tracing_aclose_forwards_to_an_asynccloseable_backing_store() -> None:
    # TracingStore's aclose() must forward to its backing store's own,
    # not just swallow it -- the exact path a `--trace`d (or this
    # project's own smoke tooling, which always traces) real
    # S3Store/AzureStore goes through.
    fake = _FakeCloseableStore()
    await TracingStore(fake, lambda _event: None).aclose()
    assert fake.closed is True


async def test_tracing_emits_one_event_per_call_with_correct_shape(
    backing: LocalFsStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Each of the 4 wrapped calls reads time.monotonic() exactly twice (t0,
    # then again at event-construction time) — rebind recording.py's own
    # ``time`` name (not the real global ``time`` module, which asyncio's own
    # scheduling also depends on) to a fake with 8 controlled, strictly
    # increasing values, so each event's elapsed is an exact, predictable,
    # non-zero number. This is the real regression this test guards
    # against: ``elapsed`` silently staying at its dataclass default of 0.0
    # (e.g. a call site that forgot to pass elapsed=...) would still
    # satisfy a mere "elapsed >= 0.0" check, but not an exact-value one.
    ticks = iter(n / 1000 for n in range(1, 9))

    class _FakeTime:
        monotonic = staticmethod(lambda: next(ticks))

    monkeypatch.setattr(recording_module, "time", _FakeTime())

    events: list[TraceEvent] = []
    tracer = TracingStore(backing, events.append)
    await tracer.read("a.txt", offset=3, length=4)
    await tracer.size("a.txt")
    await tracer.exists("nope")
    await tracer.listdir("sub")

    assert [e.method for e in events] == ["read", "size", "exists", "listdir"]
    assert [round(e.elapsed, 3) for e in events] == [0.001, 0.001, 0.001, 0.001]
    read_event = events[0]
    assert read_event.path == "a.txt"
    assert read_event.offset == 3
    assert read_event.length == 4
    assert read_event.result_length == 4  # len(b"3456")

    listdir_event = events[3]
    assert listdir_event.result_length == 1  # len(["b.txt"])


async def test_tracing_still_raises_the_backing_stores_own_errors(backing: LocalFsStore) -> None:
    events: list[TraceEvent] = []
    tracer = TracingStore(backing, events.append)
    with pytest.raises(NotFoundError):
        await tracer.read("does-not-exist.txt")
    assert events == []  # the event fires only after a successful backing call
