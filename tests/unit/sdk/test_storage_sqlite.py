"""Unit tests for ``synology_apm_repo.sdk.storage.sqlite``."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from typing import Any

import aiosqlite
import pytest

from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite import apply_index_hint, open_sqlite


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
    # per spec, only -wal's size gates the decision — a non-zero -shm alone
    # (the actual, universal shape observed across every real sample) must
    # not force materialization.
    _make_plain_db(tmp_path)
    (tmp_path / "test.db-shm").write_bytes(b"\x00" * 32768)
    store = LocalFsStore(tmp_path)

    conn, tmp_dir = await open_sqlite(store, "test.db")
    try:
        assert tmp_dir is None
    finally:
        await conn.close()


async def test_nonzero_wal_takes_slow_path_and_sees_wal_committed_data(tmp_path: Path) -> None:
    # Construct a genuine, non-checkpointed WAL: hold a read transaction
    # open on a second connection to block SQLite's automatic checkpoint,
    # then commit more data on the first connection and close it — the
    # main .db file alone is now stale; the newer row only exists in -wal.
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


async def test_transform_forces_slow_path_even_with_no_wal(tmp_path: Path) -> None:
    _make_plain_db(tmp_path, name="wrapped.db")
    wrapped_path = tmp_path / "wrapped.db"

    def _xor(data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data)

    wrapped_path.write_bytes(_xor(wrapped_path.read_bytes()))
    store = LocalFsStore(tmp_path)

    conn, materialized = await open_sqlite(store, "wrapped.db", transform=_xor)
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
    # Same WAL-building recipe as test_nonzero_wal_takes_slow_path_and_sees_wal_committed_data,
    # plus wrapping the main file and its sidecars independently -- each
    # file decides its own wrapping on its own, per FORMAT-SPEC.md §6.1's
    # per-file detection rule.
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
    conn, materialized = await open_sqlite(store, "test.db", transform=_xor)
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


async def test_a_fast_failing_main_read_does_not_race_a_slower_sidecars_write(tmp_path: Path) -> None:
    """Regression: a failing main-file read must not let ``tmp.cleanup()``
    run while a still-in-flight sidecar task is mid-write into the same
    directory. ``sidecar_finished`` being set proves the sidecar's own
    ``store.read()`` -- deliberately slower than the main file's immediate
    failure -- ran to completion before the exception (still its own
    ``RuntimeError``, not wrapped) ever propagated."""
    (tmp_path / "wrapped.db-wal").write_bytes(b"fake-wal-bytes")
    sidecar_finished = asyncio.Event()

    class _RacyStore:
        async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
            if path.endswith("-wal"):
                await asyncio.sleep(0.05)  # still in flight when the main read below raises
                data = (tmp_path / path).read_bytes()
                sidecar_finished.set()
                return data
            raise RuntimeError("main file read failed")

        async def size(self, path: str) -> int:
            return (tmp_path / path).stat().st_size

        async def exists(self, path: str) -> bool:
            return (tmp_path / path).exists()

        async def listdir(self, path: str) -> list[str]:
            raise NotImplementedError

    with pytest.raises(RuntimeError, match="main file read failed"):
        await open_sqlite(_RacyStore(), "wrapped.db")
    assert sidecar_finished.is_set()


class _NonLocalStub:
    """Minimal ObjectStore stand-in that is deliberately *not* a
    LocalFsStore, to prove the fast path is unavailable to any other
    backend regardless of WAL state.

    Its four methods are ``async def`` like the real
    ``ObjectStore`` Protocol's —
    a synchronous stub would hand ``open_sqlite`` coroutine objects where
    it expects ``bytes``/``int``/``bool``.
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        data = (self._root / path).read_bytes()
        return data[offset:] if length is None else data[offset : offset + length]

    async def size(self, path: str) -> int:
        return (self._root / path).stat().st_size

    async def exists(self, path: str) -> bool:
        return (self._root / path).exists()

    async def listdir(self, path: str) -> list[str]:
        return sorted(p.name for p in (self._root / path).iterdir())


async def test_connection_opened_in_worker_thread_can_be_closed_from_main_thread(tmp_path: Path) -> None:
    # A connection lazily created inside one @work(thread=True) worker
    # and later closed from the main UI thread at app shutdown must not
    # raise sqlite3.ProgrammingError. Reproduce the shape directly (open
    # on a background thread running its own event loop, close on this
    # — the main — thread's loop) without needing Textual at all.
    #
    # See open_sqlite()'s own docstring for why this is safe (aiosqlite's
    # per-connection thread pinning). Asserts that the open-here /
    # close-there sequence completes without ProgrammingError.
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
        except BaseException as exc:
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
        # aiosqlite types fetchall() as Iterable[Row]; with the default
        # (None) row_factory these really are plain tuples at runtime.
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
    """``apply_index_hint()`` is a pure declaration of intent, not a
    command guaranteed to succeed — see its own docstring. These tests
    exercise both halves of that contract: real index creation against a
    writable connection, and a silent no-op against a genuinely
    read-only one (the shape ``storage/sqlite.py``'s own fast path
    opens directly against a real repository file — never to be
    mutated)."""

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
    """The slow path's copy is ours to write to, so the hint must actually
    take effect there — otherwise every hinted query silently degrades to the
    full table scan a ``mode=ro`` connection would have left it with."""
    _make_plain_db(tmp_path, name="wrapped.db")
    wrapped = tmp_path / "wrapped.db"

    def _xor(data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data)

    wrapped.write_bytes(_xor(wrapped.read_bytes()))
    store = LocalFsStore(tmp_path)

    conn, materialized = await open_sqlite(store, "wrapped.db", transform=_xor)
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
    """The fast path opens the store's *real* file, read-only and immutable —
    the hint stays a no-op there rather than raising, and the query still
    answers correctly (by scanning)."""
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
