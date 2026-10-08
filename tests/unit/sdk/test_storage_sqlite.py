"""Unit tests for ``synology_apm_repo.sdk.storage.sqlite``."""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import threading
from pathlib import Path
from typing import Any, BinaryIO

import aiosqlite
import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk.errors import NotFoundError, ResourceLimitExceededError
from synology_apm_repo.sdk.storage import sqlite_source as sqlite_source_mod
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite import apply_index_hint, open_sqlite
from unit.sdk.storage_fakes import fake_disk_usage


def _make_plain_db(root: Path, name: str = "test.db") -> None:
    # Plain, synchronous sqlite3 on purpose: this is test *setup* writing a
    # fixture file, not the SDK's own read path.
    conn = sqlite3.connect(str(root / name))
    conn.execute("CREATE TABLE t (v INTEGER)")
    conn.execute("INSERT INTO t VALUES (1)")
    conn.commit()
    conn.close()


async def test_fast_path_opens_plain_db(tmp_path: Path) -> None:
    _make_plain_db(tmp_path)
    store = LocalFsStore(tmp_path)

    conn, tmp_dir = await open_sqlite(store, "test.db")
    try:
        assert tmp_dir is None  # fast path: nothing materialized
        cursor = await conn.execute("SELECT v FROM t")
        # aiosqlite types fetchall() as Iterable[Row]; with the default
        # (None) row_factory these really are plain tuples at runtime.
        rows: list[Any] = list(await cursor.fetchall())
        assert rows == [(1,)]
    finally:
        await conn.close()


async def test_fast_path_refuses_a_path_that_escapes_the_store_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _make_plain_db(tmp_path, "outside.db")
    with pytest.raises(NotFoundError, match="escapes store root"):
        await open_sqlite(LocalFsStore(root), "../outside.db")


async def test_zero_length_wal_sidecar_still_takes_fast_path(tmp_path: Path) -> None:
    _make_plain_db(tmp_path)
    (tmp_path / "test.db-wal").write_bytes(b"")  # present, but empty
    store = LocalFsStore(tmp_path)

    conn, tmp_dir = await open_sqlite(store, "test.db")
    try:
        assert tmp_dir is None
    finally:
        await conn.close()


async def test_nonzero_shm_alone_does_not_trigger_slow_path(tmp_path: Path) -> None:
    # Only -wal's size gates the slow path.
    _make_plain_db(tmp_path)
    (tmp_path / "test.db-shm").write_bytes(b"\x00" * 32768)
    store = LocalFsStore(tmp_path)

    conn, tmp_dir = await open_sqlite(store, "test.db")
    try:
        assert tmp_dir is None
    finally:
        await conn.close()


async def test_nonzero_wal_takes_slow_path_and_sees_wal_committed_data(tmp_path: Path) -> None:
    # A non-checkpointed WAL: a read transaction on a second connection blocks
    # SQLite's automatic checkpoint, so the newer row exists only in -wal.
    db_path = tmp_path / "test.db"
    writer = sqlite3.connect(str(db_path))
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("CREATE TABLE t (v INTEGER)")
    writer.execute("INSERT INTO t VALUES (1)")
    writer.commit()

    blocker = sqlite3.connect(str(db_path))
    blocker.execute("BEGIN")
    blocker.execute("SELECT * FROM t")  # opens a read snapshot, blocks full checkpoint

    writer.execute("INSERT INTO t VALUES (2)")
    writer.commit()
    writer.close()

    wal_path = tmp_path / "test.db-wal"
    assert wal_path.exists() and wal_path.stat().st_size > 0, "test setup failed to produce a non-empty WAL"

    store = LocalFsStore(tmp_path)
    conn, materialized = await open_sqlite(store, "test.db")
    try:
        assert materialized is not None  # slow path: something was materialized
        cursor = await conn.execute("SELECT v FROM t")
        rows = sorted(v for (v,) in await cursor.fetchall())
        assert rows == [1, 2]  # the WAL-only row is visible
    finally:
        await conn.close()
        if materialized is not None:
            materialized.cleanup()
        blocker.close()


async def test_the_slow_path_copy_is_refused_when_it_would_eat_into_the_reserve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _make_plain_db(repo)
    (repo / "test.db-wal").write_bytes(b"w" * 100)  # forces the slow path
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    db_size = (repo / "test.db").stat().st_size
    # 16 GiB keeps the 1 GiB floor reserve; one byte short for test.db.
    fake_disk_usage(monkeypatch, total=16 << 30, free=(1 << 30) + db_size - 1)
    with pytest.raises(ResourceLimitExceededError, match="not enough free space under"):
        await open_sqlite(LocalFsStore(repo), "test.db", tmp_dir=scratch)
    assert list(scratch.iterdir()) == []


async def test_transform_forces_slow_path_even_with_no_wal(tmp_path: Path) -> None:
    _make_plain_db(tmp_path, name="wrapped.db")
    wrapped_path = tmp_path / "wrapped.db"

    def _xor(data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data)

    def _xor_to_file(data: bytes, dest: BinaryIO) -> None:
        dest.write(_xor(data))

    wrapped_path.write_bytes(_xor(wrapped_path.read_bytes()))
    store = LocalFsStore(tmp_path)

    conn, materialized = await open_sqlite(store, "wrapped.db", transform=_xor_to_file)
    try:
        assert materialized is not None  # transform alone forces the slow path, no -wal needed
        cursor = await conn.execute("SELECT v FROM t")
        rows: list[Any] = list(await cursor.fetchall())
        assert rows == [(1,)]
    finally:
        await conn.close()
        if materialized is not None:
            materialized.cleanup()


async def test_transform_applied_independently_to_main_file_and_wal(tmp_path: Path) -> None:
    # The main file and its sidecars are wrapped independently: each file is
    # detected on its own (FORMAT-SPEC.md §6.1, §5.5).
    db_path = tmp_path / "test.db"
    writer = sqlite3.connect(str(db_path))
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("CREATE TABLE t (v INTEGER)")
    writer.execute("INSERT INTO t VALUES (1)")
    writer.commit()

    blocker = sqlite3.connect(str(db_path))
    blocker.execute("BEGIN")
    blocker.execute("SELECT * FROM t")

    writer.execute("INSERT INTO t VALUES (2)")
    writer.commit()
    writer.close()

    wal_path = tmp_path / "test.db-wal"
    assert wal_path.exists() and wal_path.stat().st_size > 0, "test setup failed to produce a non-empty WAL"

    def _xor(data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data)

    def _xor_to_file(data: bytes, dest: BinaryIO) -> None:
        dest.write(_xor(data))

    # Snapshot the three files into a directory no connection has open,
    # and wrap them there: `blocker` has to stay open to keep the WAL from
    # being checkpointed away, but on Windows it also holds `-shm` memory
    # mapped, which makes rewriting that file in place fail with EINVAL.
    wrapped_dir = tmp_path / "wrapped"
    wrapped_dir.mkdir()
    for candidate in (db_path, wal_path, tmp_path / "test.db-shm"):
        if candidate.exists():
            (wrapped_dir / candidate.name).write_bytes(_xor(candidate.read_bytes()))

    store = LocalFsStore(wrapped_dir)
    conn, materialized = await open_sqlite(store, "test.db", transform=_xor_to_file)
    try:
        assert materialized is not None
        cursor = await conn.execute("SELECT v FROM t")
        rows = sorted(v for (v,) in await cursor.fetchall())
        assert rows == [1, 2]  # both the main file's and the WAL-only row decoded correctly
    finally:
        await conn.close()
        if materialized is not None:
            materialized.cleanup()
        blocker.close()


async def test_a_fast_failing_main_read_does_not_race_a_slower_sidecars_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing main-file read must not let ``tmp.cleanup()`` run while a
    slower sidecar task is still writing into the same directory; the main
    read's own ``RuntimeError`` propagates unwrapped."""
    (tmp_path / "wrapped.db-wal").write_bytes(b"fake-wal-bytes")
    events: list[str] = []
    sidecar_reading, main_failed = asyncio.Event(), asyncio.Event()

    class _RecordingTemporaryDirectory(tempfile.TemporaryDirectory[str]):
        def cleanup(self) -> None:
            events.append("cleanup")
            super().cleanup()

    monkeypatch.setattr(tempfile, "TemporaryDirectory", _RecordingTemporaryDirectory)

    @faithful_to(ObjectStore)
    class _RacyStore:
        async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
            if path.endswith("-wal"):
                sidecar_reading.set()
                await main_failed.wait()  # in flight when the main read below raises
                for _ in range(20):  # loop turns in which a non-waiting caller would reach its cleanup
                    await asyncio.sleep(0)
                data = (tmp_path / path).read_bytes()
                events.append("sidecar done")
                return data
            await asyncio.wait_for(sidecar_reading.wait(), 5)
            main_failed.set()
            raise RuntimeError("main file read failed")

        async def size(self, path: str) -> int:
            return (tmp_path / path).stat().st_size

        async def exists(self, path: str) -> bool:
            return (tmp_path / path).exists()

        async def close(self) -> None:
            pass

        async def listdir(self, path: str) -> list[Entry]:
            raise NotImplementedError

    with pytest.raises(RuntimeError, match="main file read failed"):
        await open_sqlite(_RacyStore(), "wrapped.db")
    assert events == ["sidecar done", "cleanup"]


async def test_a_cancelled_slow_path_open_removes_its_temp_directory(tmp_path: Path) -> None:
    (tmp_path / "wrapped.db-wal").write_bytes(b"fake-wal-bytes")
    tmp_root = tmp_path / "tmp"
    tmp_root.mkdir()

    class _CancellingStore(_NonLocalStub):
        async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await open_sqlite(_CancellingStore(tmp_path), "wrapped.db", tmp_dir=tmp_root)
    assert list(tmp_root.iterdir()) == []


@faithful_to(ObjectStore)
class _NonLocalStub:
    """An ``ObjectStore`` that is not a ``LocalFsStore``, so the fast path is
    unavailable whatever the WAL state."""

    def __init__(self, root: Path) -> None:
        self._root = root

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        data = (self._root / path).read_bytes()
        return data[offset:] if length is None else data[offset : offset + length]

    async def size(self, path: str) -> int:
        return (self._root / path).stat().st_size

    async def exists(self, path: str) -> bool:
        return (self._root / path).exists()

    async def close(self) -> None:
        pass

    async def listdir(self, path: str) -> list[Entry]:
        return sorted(Entry(p.name, None if p.is_dir() else p.stat().st_size) for p in (self._root / path).iterdir())


async def test_connection_opened_in_worker_thread_can_be_closed_from_main_thread(tmp_path: Path) -> None:
    # The TUI opens a connection in a worker thread's loop and closes it from
    # the main thread's at shutdown; aiosqlite routes every statement, close()
    # included, through the thread that opened it, so no ProgrammingError.
    _make_plain_db(tmp_path)
    store = LocalFsStore(tmp_path)

    opened: list[aiosqlite.Connection] = []
    errors: list[BaseException] = []

    async def open_on_worker_loop() -> None:
        conn, _tmp_dir = await open_sqlite(store, "test.db")
        cursor = await conn.execute("SELECT v FROM t")
        await cursor.fetchall()  # touch it on the opening thread too
        opened.append(conn)

    def worker_thread() -> None:
        try:
            asyncio.run(open_on_worker_loop())
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    worker = threading.Thread(target=worker_thread)
    worker.start()
    await asyncio.to_thread(worker.join)

    assert not errors, errors
    assert len(opened) == 1
    cursor = await opened[0].execute("SELECT v FROM t")
    await cursor.fetchall()  # use, then close, from *this* (main) thread's loop
    await opened[0].close()


async def test_non_local_store_always_materializes_even_without_wal(tmp_path: Path) -> None:
    _make_plain_db(tmp_path)
    store = _NonLocalStub(tmp_path)

    conn, materialized = await open_sqlite(store, "test.db")
    try:
        assert materialized is not None
        cursor = await conn.execute("SELECT v FROM t")
        rows: list[Any] = list(await cursor.fetchall())
        assert rows == [(1,)]
    finally:
        await conn.close()
        if materialized is not None:
            materialized.cleanup()


# -- apply_index_hint() --------------------------------------------------


async def _index_names(conn: aiosqlite.Connection, table: str) -> set[str]:
    cursor = await conn.execute(f"PRAGMA index_list({table})")
    return {row[1] for row in await cursor.fetchall()}


class TestApplyIndexHint:
    """``apply_index_hint()`` creates the index on a writable connection and is
    a silent no-op on a read-only one (where ``CREATE INDEX`` raises
    "attempt to write a readonly database"), the shape the fast path opens."""

    async def test_creates_an_index_when_none_covers_the_columns(self, tmp_path: Path) -> None:
        conn = await aiosqlite.connect(tmp_path / "t.db")
        await conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")
        await conn.commit()
        try:
            before = await _index_names(conn, "t")
            await apply_index_hint(conn, "t", ["a"])
            after = await _index_names(conn, "t")
            assert after - before  # a genuinely new index was created
        finally:
            await conn.close()

    async def test_skips_when_an_existing_index_already_covers_the_columns(self, tmp_path: Path) -> None:
        conn = await aiosqlite.connect(tmp_path / "t.db")
        await conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")
        await conn.execute("CREATE INDEX existing_idx ON t (a)")
        await conn.commit()
        try:
            before = await _index_names(conn, "t")
            await apply_index_hint(conn, "t", ["a"])
            after = await _index_names(conn, "t")
            assert after == before  # no new index — "existing_idx" already covers it
        finally:
            await conn.close()

    async def test_recognizes_a_composite_index_by_its_leading_columns(self, tmp_path: Path) -> None:
        conn = await aiosqlite.connect(tmp_path / "t.db")
        await conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)")
        await conn.execute("CREATE INDEX existing_idx ON t (a, b)")
        await conn.commit()
        try:
            before = await _index_names(conn, "t")
            await apply_index_hint(conn, "t", ["a", "b"])  # exact leading-column match
            after = await _index_names(conn, "t")
            assert after == before
        finally:
            await conn.close()

    async def test_repeated_calls_are_idempotent(self, tmp_path: Path) -> None:
        conn = await aiosqlite.connect(tmp_path / "t.db")
        await conn.execute("CREATE TABLE t (a INTEGER)")
        await conn.commit()
        try:
            await apply_index_hint(conn, "t", ["a"])
            first = await _index_names(conn, "t")
            await apply_index_hint(conn, "t", ["a"])  # must not raise, must not duplicate
            second = await _index_names(conn, "t")
            assert first == second
        finally:
            await conn.close()

    async def test_readonly_connection_silently_does_nothing(self, tmp_path: Path) -> None:
        db_path = tmp_path / "t.db"
        writer = await aiosqlite.connect(db_path)
        await writer.execute("CREATE TABLE t (a INTEGER)")
        await writer.commit()
        await writer.close()

        ro = await aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            await apply_index_hint(ro, "t", ["a"])  # must not raise
        finally:
            await ro.close()

        # Confirm nothing was actually written: re-open writable and check.
        writer2 = await aiosqlite.connect(db_path)
        try:
            assert await _index_names(writer2, "t") == set()
        finally:
            await writer2.close()

    async def test_a_genuinely_different_error_still_propagates(self, tmp_path: Path) -> None:
        conn = await aiosqlite.connect(tmp_path / "t.db")
        await conn.execute("CREATE TABLE t (a INTEGER)")
        await conn.commit()
        try:
            with pytest.raises(aiosqlite.OperationalError, match="no such column"):
                await apply_index_hint(conn, "t", ["no_such_column"])
        finally:
            await conn.close()


async def _plan(conn: aiosqlite.Connection, sql: str, *params: object) -> str:
    cursor = await conn.execute(f"EXPLAIN QUERY PLAN {sql}", params)
    return " ".join(str(row[-1]) for row in await cursor.fetchall())


async def test_index_hint_builds_a_real_index_on_a_materialized_copy(tmp_path: Path) -> None:
    """The slow path's copy is writable, so the hint takes effect there instead
    of leaving every hinted query a full table scan."""
    _make_plain_db(tmp_path, name="wrapped.db")
    wrapped = tmp_path / "wrapped.db"

    def _xor(data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data)

    def _xor_to_file(data: bytes, dest: BinaryIO) -> None:
        dest.write(_xor(data))

    wrapped.write_bytes(_xor(wrapped.read_bytes()))
    store = LocalFsStore(tmp_path)

    conn, materialized = await open_sqlite(store, "wrapped.db", transform=_xor_to_file)
    try:
        assert materialized is not None  # transform forces the slow path
        await apply_index_hint(conn, "t", ["v"])
        assert "_synology_apm_repo_idx_t_v" in await _index_names(conn, "t")
        # "USING INDEX" or "USING COVERING INDEX" depending on the columns
        # SQLite can answer straight from the index itself.
        assert "_synology_apm_repo_idx_t_v" in await _plan(conn, "SELECT v FROM t WHERE v = ?", 1)
    finally:
        await conn.close()
        if materialized is not None:
            materialized.cleanup()


async def test_index_hint_is_a_silent_no_op_on_the_fast_path(tmp_path: Path) -> None:
    """The fast path opens the store's real file read-only and immutable: the
    hint is a no-op and the query still answers (by scanning)."""
    _make_plain_db(tmp_path)
    store = LocalFsStore(tmp_path)

    conn, tmp_dir = await open_sqlite(store, "test.db")
    try:
        assert tmp_dir is None  # fast path: the real file
        await apply_index_hint(conn, "t", ["v"])  # must not raise
        assert await _index_names(conn, "t") == set()
        cursor = await conn.execute("SELECT v FROM t WHERE v = ?", (1,))
        assert list(await cursor.fetchall()) == [(1,)]
    finally:
        await conn.close()


def _copy(raw: bytes, dest: BinaryIO) -> None:
    dest.write(raw)


class TestConnectCancelled:
    @pytest.mark.parametrize("path", ["fast", "slow", "temp file"])
    async def test_a_connection_that_opens_after_the_caller_was_cancelled_is_closed(
        self, path: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancelled while ``aiosqlite.connect`` is still opening: the connection
        that opens anyway is closed, not leaked with its non-daemon thread, and a
        temp copy is removed."""
        db = tmp_path / "x.db"
        sqlite3.connect(db).close()
        started, gate, open_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
        opened: list[Any] = []
        real_connect = aiosqlite.connect

        async def gated_connect(database: str, **kwargs: Any) -> aiosqlite.Connection:
            async def open_after_gate() -> aiosqlite.Connection:
                started.set()
                await gate.wait()
                connection = await real_connect(database, **kwargs)
                opened.append(connection)
                open_done.set()
                return connection

            # Like aiosqlite's own thread, the open goes on even if this
            # await is cancelled.
            return await asyncio.shield(asyncio.ensure_future(open_after_gate()))

        monkeypatch.setattr(aiosqlite, "connect", gated_connect)
        store = LocalFsStore(tmp_path)
        task: asyncio.Task[object]
        match path:
            case "fast":
                task = asyncio.create_task(open_sqlite(store, "x.db"))
            case "slow":
                task = asyncio.create_task(open_sqlite(store, "x.db", tmp_dir=tmp_path, transform=_copy))
            case _:
                copy = tmp_path / "copy.db"
                copy.write_bytes(db.read_bytes())
                task = asyncio.create_task(sqlite_source_mod._connect_temp_file(str(copy)))
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        gate.set()

        with pytest.raises(asyncio.CancelledError):
            await task
        # A caller that didn't wait for the open would return before it ends; wait for it here.
        await asyncio.wait_for(open_done.wait(), 5)
        (connection,) = opened
        with pytest.raises(ValueError, match="no active connection"):
            await connection.execute("SELECT 1")
        assert sorted(entry.name for entry in tmp_path.iterdir()) == ["x.db"]
