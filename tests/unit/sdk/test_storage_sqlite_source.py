"""Unit tests for ``synology_apm_repo.sdk.storage.sqlite_source`` —
synthetic envelopes built the same way as
``tests/unit/sdk/test_format_crypto.py``'s ``_build_ahlt`` (see
``tests/integration/sdk/test_storage_sqlite_source_envelopes.py``
for the byte-for-byte cross-check against all four envelope combinations
found in real samples)."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import threading
import zlib
from pathlib import Path
from typing import Any

import aiosqlite
import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite_source import Envelope, SqliteSource, peel


def _build_ahlt(vault_key: bytes, plaintext: bytes) -> bytes:
    iv = os.urandom(16)
    encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(iv)).encryptor()
    ciphertext = encryptor.update(plaintext) + encryptor.finalize()
    header = bytearray(64)
    header[0:4] = b"aHlT"
    header[8:24] = iv
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header) + ciphertext


def _real_sqlite_bytes() -> bytes:
    # Plain, synchronous sqlite3: this only *builds* the fixture bytes the
    # SDK is then asked to read; it is not the SDK's own read path.
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t(x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        return conn.serialize()
    finally:
        conn.close()


async def _fetchone(conn: aiosqlite.Connection, sql: str) -> Any:
    """One row, as a plain tuple. Typed ``Any`` because aiosqlite declares
    ``fetchone() -> Row | None`` while the default (None) row_factory
    really produces ordinary tuples at runtime."""
    cursor = await conn.execute(sql)
    return await cursor.fetchone()


class TestPeel:
    """``peel()`` stays synchronous — it is pure bytes work, no I/O."""

    def test_raw_passes_through_unchanged(self) -> None:
        data = _real_sqlite_bytes()
        payload, envelopes = peel(data)
        assert payload == data
        assert envelopes == []

    def test_zstd_only(self) -> None:
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        payload, envelopes = peel(compressed)
        assert payload == raw
        assert envelopes == [Envelope.ZSTD]

    def test_ahlt_only(self) -> None:
        vault_key = os.urandom(32)
        raw = _real_sqlite_bytes()
        enveloped = _build_ahlt(vault_key, raw)
        payload, envelopes = peel(enveloped, vault_key=vault_key)
        assert payload == raw
        assert envelopes == [Envelope.AHLT]

    def test_ahlt_then_zstd(self) -> None:
        vault_key = os.urandom(32)
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        enveloped = _build_ahlt(vault_key, compressed)
        payload, envelopes = peel(enveloped, vault_key=vault_key)
        assert payload == raw
        assert envelopes == [Envelope.AHLT, Envelope.ZSTD]

    def test_ahlt_without_vault_key_raises_key_required(self) -> None:
        vault_key = os.urandom(32)
        enveloped = _build_ahlt(vault_key, _real_sqlite_bytes())
        with pytest.raises(KeyRequiredError):
            peel(enveloped)


class TestSqliteSource:
    """Opening the connection is itself an ``await``, so ``SqliteSource``
    takes no constructor arguments — every case below goes through the
    ``await SqliteSource.from_bytes(...)`` factory instead."""

    async def test_opens_a_working_connection(self) -> None:
        data = _real_sqlite_bytes()
        async with await SqliteSource.from_bytes(data) as src:
            row = await _fetchone(src.connection, "SELECT x FROM t")
            assert row == (1,)

    async def test_connection_is_writable_so_index_hints_can_take_effect(self) -> None:
        """Deliberately *not* read-only. The connection is onto this
        instance's own temp copy of already-peeled bytes, which ``close()``
        unlinks, so nothing written here can reach the store — and writability
        is the whole reason ``apply_index_hint`` can build a real index rather
        than silently leaving its caller a full table scan."""
        data = _real_sqlite_bytes()
        async with await SqliteSource.from_bytes(data) as src:
            await src.connection.execute("INSERT INTO t VALUES (2)")
            await src.connection.commit()
            assert await _fetchone(src.connection, "SELECT COUNT(*) FROM t") == (2,)

    async def test_close_removes_the_sidecars_a_writable_connection_can_leave(self) -> None:
        """A read-write connection can create ``-wal``/``-shm``/``-journal``
        next to the temp file; none of them may outlive ``close()``."""
        data = _real_sqlite_bytes()
        src = await SqliteSource.from_bytes(data)
        path = src._path
        assert path is not None
        await src.connection.execute("PRAGMA journal_mode=WAL")
        await src.connection.execute("INSERT INTO t VALUES (3)")
        await src.connection.commit()
        await src.close()
        leftovers = [p for p in (path, *(path + s for s in ("-wal", "-shm", "-journal"))) if os.path.exists(p)]
        assert leftovers == []

    async def test_close_removes_the_temp_file(self) -> None:
        data = _real_sqlite_bytes()
        src = await SqliteSource.from_bytes(data)
        path = src._path
        assert path is not None  # built from bytes, always set
        assert os.path.exists(path)
        await src.close()
        assert not os.path.exists(path)

    async def test_close_is_idempotent(self) -> None:
        # A caller that closes a provider itself, ahead of the
        # session-wide cleanup that would otherwise close the same
        # underlying SqliteSource again at session end (Repository's own
        # provider list and DedupRepo's db_sources cache can both
        # hold a reference to it), must not crash on the second close.
        data = _real_sqlite_bytes()
        src = await SqliteSource.from_bytes(data)
        await src.close()
        await src.close()  # must not raise FileNotFoundError

    async def test_failure_during_open_still_cleans_up_the_temp_file(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # build the fixture data *before* patching the connect function —
        # it calls sqlite3.connect(":memory:") itself to build it.
        data = _real_sqlite_bytes()

        created_paths: list[str] = []
        real_mkstemp = tempfile.mkstemp

        def spying_mkstemp(suffix: str | None = None) -> tuple[int, str]:
            fd, path = real_mkstemp(suffix=suffix)
            created_paths.append(path)
            return fd, path

        def failing_connect(database: str, uri: bool = False) -> aiosqlite.Connection:
            raise sqlite3.OperationalError("simulated failure")

        monkeypatch.setattr(tempfile, "mkstemp", spying_mkstemp)
        # from_bytes() now opens via ``aiosqlite.connect``, not ``sqlite3.connect``.
        monkeypatch.setattr(aiosqlite, "connect", failing_connect)

        with pytest.raises(sqlite3.OperationalError):
            await SqliteSource.from_bytes(data)

        assert len(created_paths) == 1
        assert not os.path.exists(created_paths[0])

    async def test_close_from_a_different_thread_than_the_one_that_opened_it(self) -> None:
        # Same thread-pinning guarantee as test_storage_sqlite.py's
        # test_connection_opened_in_worker_thread_can_be_closed_from_main_thread,
        # exercised through SqliteSource instead of open_sqlite directly.
        # SqliteSource instances materialized inside one
        # @work(thread=True) worker (device.py's target.db, fs.py's
        # version.db.zst, ...) are routinely closed from a different
        # thread later (the TUI's main thread at shutdown). The worker
        # runs its own event loop, so this really is two different OS
        # threads, not two Tasks.
        data = _real_sqlite_bytes()
        errors: list[BaseException] = []
        sources: list[SqliteSource] = []

        async def open_on_worker_loop() -> None:
            sources.append(await SqliteSource.from_bytes(data))

        def worker_thread() -> None:
            try:
                asyncio.run(open_on_worker_loop())
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=worker_thread)
        worker.start()
        await asyncio.to_thread(worker.join)

        assert not errors, errors
        assert len(sources) == 1
        await sources[0].close()  # closed from *this* (main) thread — must not raise

    async def test_end_to_end_with_peel(self) -> None:
        vault_key = os.urandom(32)
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        enveloped = _build_ahlt(vault_key, compressed)

        payload, envelopes = peel(enveloped, vault_key=vault_key)
        assert envelopes == [Envelope.AHLT, Envelope.ZSTD]
        async with await SqliteSource.from_bytes(payload) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_from_bytes_is_the_only_bytes_facing_factory(self) -> None:
        assert not hasattr(SqliteSource(), "connection")
        data = _real_sqlite_bytes()
        async with await SqliteSource.from_bytes(data) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)


class _CountingMemoryStore:
    """A minimal, non-``LocalFsStore`` ``ObjectStore`` fake — deliberately
    *not* ``LocalFsStore``, so ``open_sqlite``'s ``isinstance(store,
    LocalFsStore)`` fast-path check fails and every open goes through the
    materialize-via-``store.read()`` path, making ``read()`` call counts a
    direct proxy for network round trips on a real remote backend."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files
        self.read_calls = 0

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self.read_calls += 1
        data = self._files[path]
        return data[offset:] if length is None else data[offset : offset + length]

    async def size(self, path: str) -> int:
        return len(self._files[path])

    async def exists(self, path: str) -> bool:
        return path in self._files

    async def listdir(self, path: str) -> list[str]:
        raise NotImplementedError


class TestFromRawStore:
    """``from_raw_store`` — the entry point for a caller that already
    knows ``path`` is never enveloped, reading it directly via
    ``open_sqlite`` (real ``db/<name>``,
    ``saas/*/db/saas_{version,snapshot}``). A caller whose source may be
    enveloped instead uses ``from_enveloped_store`` (``target.db``, which
    may also have a real ``-wal``/``-shm`` sidecar; see ``TestFromEnvelopedStore``
    below) or its own ``store.read()`` + ``peel`` + ``SqliteSource.from_bytes()``
    (``version.db.zst``, ``units/fs.py``'s ``_entry_table_connection()``,
    which never has WAL sidecars to merge) — see
    ``TestPeel``/``TestSqliteSource`` above for that half's own
    coverage."""

    async def test_reads_a_raw_file_correctly(self, tmp_path: Path) -> None:
        (tmp_path / "db").write_bytes(_real_sqlite_bytes())
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_raw_store(store, "db") as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_reads_the_path_only_once(self) -> None:
        # Goes straight to open_sqlite() with no throwaway peek read
        # first — one store.read() per open, not two.
        raw = _real_sqlite_bytes()
        raw_store = _CountingMemoryStore({"db": raw})
        async with await SqliteSource.from_raw_store(raw_store, "db") as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)
        assert raw_store.read_calls == 1

    async def test_no_wal_uses_the_open_sqlite_fast_path(self, tmp_path: Path) -> None:
        (tmp_path / "db").write_bytes(_real_sqlite_bytes())
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_raw_store(store, "db") as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)
            # the fast path opens the real file directly — no temp file/dir.
            assert src._path is None
            assert src._tmp_dir is None

    async def test_a_real_nonempty_wal_sees_the_wals_content(self, tmp_path: Path) -> None:
        # A real -wal sidecar with actual uncommitted pages — proves this
        # doesn't just read the main file's bytes and ignore WAL, the way
        # a naive store.read() would. The writer connection is
        # deliberately kept *open* throughout: sqlite checkpoints
        # (folding the -wal back into the main file and deleting it) on
        # connection close, which would defeat the setup.
        db_path = tmp_path / "db"
        writer = sqlite3.connect(db_path)
        try:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("CREATE TABLE t(x INTEGER)")
            writer.execute("INSERT INTO t VALUES (1)")
            writer.commit()
            writer.execute("INSERT INTO t VALUES (2)")
            writer.commit()
            assert (tmp_path / "db-wal").exists()
            assert (tmp_path / "db-wal").stat().st_size > 0

            store = LocalFsStore(tmp_path)
            async with await SqliteSource.from_raw_store(store, "db") as src:
                cursor = await src.connection.execute("SELECT x FROM t ORDER BY x")
                rows: list[Any] = list(await cursor.fetchall())
                assert rows == [(1,), (2,)]
        finally:
            writer.close()


class TestFromEnvelopedStore:
    """``from_enveloped_store`` — the entry point for a caller whose
    source may be ``aHlT``-enveloped *and* may have a real ``-wal``/``-shm``
    sidecar (``copy_meta_file/*/target.db``, FORMAT-SPEC.md: copy_meta_file-layout;
    ``catalog/version.py``'s ``open_target_db()`` is the one real call
    site)."""

    async def test_reads_a_plaintext_file_with_no_vault_key(self, tmp_path: Path) -> None:
        (tmp_path / "target.db").write_bytes(_real_sqlite_bytes())
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(store, "target.db", vault_key=None) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_ahlt_without_vault_key_raises_key_required(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        (tmp_path / "target.db").write_bytes(_build_ahlt(vault_key, _real_sqlite_bytes()))
        store = LocalFsStore(tmp_path)
        with pytest.raises(KeyRequiredError):
            await SqliteSource.from_enveloped_store(store, "target.db", vault_key=None)

    async def test_ahlt_with_no_wal_decrypts_correctly(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        (tmp_path / "target.db").write_bytes(_build_ahlt(vault_key, _real_sqlite_bytes()))
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(store, "target.db", vault_key=vault_key) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_zero_length_wal_is_still_ignored_under_envelope(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        (tmp_path / "target.db").write_bytes(_build_ahlt(vault_key, _real_sqlite_bytes()))
        (tmp_path / "target.db-wal").write_bytes(b"")  # present, but empty
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(store, "target.db", vault_key=vault_key) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_ahlt_with_a_real_wal_decrypts_and_merges_both_independently(self, tmp_path: Path) -> None:
        # Main file and its -wal are wrapped with two *different* random
        # IVs (_build_ahlt draws a fresh one each call) -- proving each is
        # peeled independently, not as one shared ciphertext.
        vault_key = os.urandom(32)
        db_path = tmp_path / "target.db"
        writer = sqlite3.connect(db_path)
        try:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("CREATE TABLE t(x INTEGER)")
            writer.execute("INSERT INTO t VALUES (1)")
            writer.commit()
            writer.execute("INSERT INTO t VALUES (2)")
            writer.commit()
            wal_path = tmp_path / "target.db-wal"
            assert wal_path.exists() and wal_path.stat().st_size > 0, "test setup failed to produce a non-empty WAL"
            real_db_bytes = db_path.read_bytes()
            real_wal_bytes = wal_path.read_bytes()
        finally:
            writer.close()

        db_path.write_bytes(_build_ahlt(vault_key, real_db_bytes))
        wal_path.write_bytes(_build_ahlt(vault_key, real_wal_bytes))

        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(store, "target.db", vault_key=vault_key) as src:
            cursor = await src.connection.execute("SELECT x FROM t ORDER BY x")
            rows: list[Any] = list(await cursor.fetchall())
            assert rows == [(1,), (2,)]  # the WAL-only row decoded and merged correctly
