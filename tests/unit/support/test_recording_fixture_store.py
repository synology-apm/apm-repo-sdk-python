"""Unit tests for ``tests/support/recording/fixture_store.py``:
``RecordingStore`` (an ``InstrumentedStore``) and ``ReplayStore``, the
gzip-aware fixture I/O, and ``AliasedStore``."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path

import pytest

from support.fakes import faithful_to
from support.recording.fixture_store import (
    AliasedStore,
    FixtureFormatError,
    RecordingStore,
    ReplayStore,
    UnrecordedCallError,
    load_fixture_text,
    parse_read_key,
    write_fixture_text,
)
from support.store_fakes import CloseCountingStore, FailingStore
from synology_apm_repo.sdk.errors import NotFoundError, StorageBackendError
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore
from synology_apm_repo.sdk.storage.local import LocalFsStore


@pytest.fixture
def backing(tmp_path: Path) -> LocalFsStore:
    (tmp_path / "a.txt").write_bytes(b"0123456789")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"hello world")
    return LocalFsStore(tmp_path)


async def test_recording_passes_through_to_backing(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    assert await rec.read("a.txt") == b"0123456789"
    assert await rec.read("a.txt", offset=3, length=4) == b"3456"
    assert await rec.size("a.txt") == 10
    assert await rec.exists("a.txt") is True
    assert await rec.exists("nope") is False
    assert await rec.listdir("") == [Entry("a.txt", 10), Entry("sub", None)]


async def test_recording_close_forwards_to_the_backing_store() -> None:
    fake = CloseCountingStore()
    await RecordingStore(fake).close()
    assert fake.close_count == 1


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
    assert await replay.listdir("") == [Entry("a.txt", 10), Entry("sub", None)]


async def test_replay_raises_for_unrecorded_read(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    await rec.read("a.txt")
    replay = ReplayStore(rec.dump())

    with pytest.raises(UnrecordedCallError, match="no recorded"):
        await replay.read("sub/b.txt")  # never recorded


async def test_replay_raises_for_unrecorded_read_with_different_offset(backing: LocalFsStore) -> None:
    # a fixture covering read("a.txt", 0, None) does not automatically
    # cover read("a.txt", 3, 4) — every distinct call must be recorded.
    rec = RecordingStore(backing)
    await rec.read("a.txt")
    replay = ReplayStore(rec.dump())

    assert await replay.read("a.txt") == b"0123456789"
    with pytest.raises(UnrecordedCallError, match="no recorded"):
        await replay.read("a.txt", offset=3, length=4)


async def test_replay_raises_for_unrecorded_exists_rather_than_defaulting_false(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    await rec.exists("a.txt")  # only this one path was ever queried
    replay = ReplayStore(rec.dump())

    assert await replay.exists("a.txt") is True
    with pytest.raises(UnrecordedCallError, match="no recorded"):
        await replay.exists("sub")


async def test_replay_raises_for_unrecorded_size(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    replay = ReplayStore(rec.dump())
    with pytest.raises(UnrecordedCallError, match="no recorded"):
        await replay.size("a.txt")


async def test_replay_raises_for_unrecorded_listdir(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    replay = ReplayStore(rec.dump())
    with pytest.raises(UnrecordedCallError, match="no recorded"):
        await replay.listdir("")


async def test_replay_logs_every_unrecorded_call_even_when_the_caller_swallows_it(backing: LocalFsStore) -> None:
    replay = ReplayStore(RecordingStore(backing).dump())

    with contextlib.suppress(Exception):  # what code under test catching broadly does
        await replay.size("a.txt")

    assert replay.misses == ["size 'a.txt'"]


async def test_recording_a_not_found_replays_as_not_found_and_is_not_a_miss(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    with pytest.raises(NotFoundError, match="no such file"):
        await rec.read("nope.txt", offset=4, length=2)
    with pytest.raises(NotFoundError, match="no such path"):
        await rec.size("nope.txt")
    with pytest.raises(NotFoundError, match="no such directory"):
        await rec.listdir("no-dir")
    assert json.loads(rec.dump())["missing"] == {
        "exists": [],
        "listdir": ["no-dir"],
        "read": ["nope.txt\x004\x002"],
        "size": ["nope.txt"],
    }

    replay = ReplayStore(rec.dump())

    with pytest.raises(NotFoundError, match="found nothing"):
        await replay.read("nope.txt", offset=4, length=2)
    with pytest.raises(NotFoundError, match="found nothing"):
        await replay.size("nope.txt")
    with pytest.raises(NotFoundError, match="found nothing"):
        await replay.listdir("no-dir")
    assert replay.misses == []


def test_a_fixture_without_the_current_format_is_rejected_with_the_re_record_command(
    backing: LocalFsStore, tmp_path: Path
) -> None:
    payload = json.loads(RecordingStore(backing).dump())
    del payload["format"], payload["missing"]
    path = tmp_path / "old.json.gz"
    write_fixture_text(path, json.dumps(payload))

    with pytest.raises(
        FixtureFormatError, match=r"old\.json\.gz is not in fixture format 2.*support\.recording\.manifest"
    ):
        ReplayStore.from_path(path)
    with pytest.raises(FixtureFormatError, match="not in fixture format 2"):
        ReplayStore(json.dumps(payload), strict=False)


async def test_a_non_strict_replay_answers_an_unrecorded_call_as_not_found(backing: LocalFsStore) -> None:
    replay = ReplayStore(RecordingStore(backing).dump(), strict=False)

    with pytest.raises(NotFoundError, match="found nothing"):
        await replay.exists("a.txt")
    assert replay.misses == []


async def test_recording_skips_a_failure_other_than_not_found() -> None:
    recorder = RecordingStore(FailingStore())
    with pytest.raises(StorageBackendError, match="down"):
        await recorder.size("a.txt")
    assert json.loads(recorder.dump())["missing"]["size"] == []


def test_parse_read_key_inverts_the_recorded_read_key() -> None:
    assert parse_read_key("dir/f\x003\x004") == ("dir/f", 3, 4)
    assert parse_read_key("dir/f\x000\x00") == ("dir/f", 0, None)


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


async def test_recording_records_a_not_found_read_as_a_miss_not_a_read(backing: LocalFsStore) -> None:
    recorder = RecordingStore(backing)
    with pytest.raises(NotFoundError, match="no such file"):
        await recorder.read("does-not-exist.txt")
    payload = json.loads(recorder.dump())
    assert payload["reads"] == {}
    assert payload["missing"]["read"] == ["does-not-exist.txt\x000\x00"]


@faithful_to(ObjectStore)
class _UnsizedListingStore:
    """An ``ObjectStore`` whose listing reports no size for any entry."""

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        raise NotImplementedError

    async def size(self, path: str) -> int:
        raise NotImplementedError

    async def exists(self, path: str) -> bool:
        raise NotImplementedError

    async def close(self) -> None:
        pass

    async def listdir(self, path: str) -> list[Entry]:
        return [Entry("x", None), Entry("y", None)]


async def test_recording_a_listing_round_trips_through_the_existing_fixture_sections(
    backing: LocalFsStore,
) -> None:
    rec = RecordingStore(backing)
    assert await rec.listdir("sub") == [Entry("b.txt", 11)]
    assert await rec.listdir("") == [Entry("a.txt", 10), Entry("sub", None)]

    payload = json.loads(rec.dump())
    assert payload["listdirs"] == {"sub": ["b.txt"], "": ["a.txt", "sub"]}
    assert payload["sizes"] == {"sub/b.txt": 11, "a.txt": 10}  # listing sizes share size()'s "sizes" section

    replay = ReplayStore(rec.dump())
    assert await replay.listdir("sub") == [Entry("b.txt", 11)]
    assert await replay.listdir("") == [Entry("a.txt", 10), Entry("sub", None)]
    assert await replay.size("sub/b.txt") == 11  # a size recorded from a listing also answers size()


async def test_recording_a_listing_without_sizes_records_names_and_no_sizes() -> None:
    rec = RecordingStore(_UnsizedListingStore())
    assert await rec.listdir("d") == [Entry("x", None), Entry("y", None)]

    payload = json.loads(rec.dump())
    assert payload["listdirs"] == {"d": ["x", "y"]}
    assert payload["sizes"] == {}
    assert await ReplayStore(rec.dump()).listdir("d") == [Entry("x", None), Entry("y", None)]


async def test_replay_reports_a_size_only_where_one_was_recorded_and_sorts_by_name(backing: LocalFsStore) -> None:
    rec = RecordingStore(backing)
    await rec.listdir("sub")
    payload = json.loads(rec.dump())
    payload["listdirs"]["sub"] = ["nested", "b.txt", "c.txt"]  # a hand-edited, unsorted fixture listing
    payload["sizes"]["c.txt"] = 3  # a size recorded for a path outside "sub"

    replay = ReplayStore(json.dumps(payload))

    assert await replay.listdir("sub") == [Entry("b.txt", 11), Entry("c.txt", None), Entry("nested", None)]


async def test_replays_from_one_fixture_file_keep_separate_misses(backing: LocalFsStore, tmp_path: Path) -> None:
    path = tmp_path / "f.json.gz"
    write_fixture_text(path, RecordingStore(backing).dump())
    first, second = ReplayStore.from_path(path), ReplayStore.from_path(path)

    with pytest.raises(UnrecordedCallError, match="no recorded size"):
        await first.size("a.txt")

    assert (first.misses, second.misses) == (["size 'a.txt'"], [])


async def test_from_path_rereads_a_fixture_file_rewritten_since(backing: LocalFsStore, tmp_path: Path) -> None:
    path = tmp_path / "f.json.gz"
    write_fixture_text(path, RecordingStore(backing).dump())
    ReplayStore.from_path(path)
    rec = RecordingStore(backing)
    await rec.size("a.txt")
    write_fixture_text(path, rec.dump())
    os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns + 1_000_000))

    assert await ReplayStore.from_path(path).size("a.txt") == 10


async def test_aliased_store_shows_each_sample_directory_and_its_key_file_by_alias(tmp_path: Path) -> None:
    (tmp_path / "alice-backup").mkdir()
    (tmp_path / "alice-backup" / "repo_info").write_bytes(b"info")
    (tmp_path / "alice-backup.key").write_bytes(b"key")
    (tmp_path / "bob").mkdir()
    store = AliasedStore(LocalFsStore(tmp_path), {"alice-backup": "vault-plain"})

    assert sorted(entry.name for entry in await store.listdir("")) == ["bob", "vault-plain", "vault-plain.key"]
    assert await store.read("vault-plain/repo_info", 1, 2) == b"nf"
    assert await store.read("vault-plain.key") == b"key"
    assert await store.size("vault-plain/repo_info") == 4
    assert await store.exists("vault-plain") is True
    assert await store.exists("alice-backup-old") is False
    await store.close()
