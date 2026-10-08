"""Unit tests for ``storage.sqlite_source`` using synthetic ``aHlT``/zstd
envelopes; ``tests/integration/sdk/test_storage_sqlite_source_envelopes.py``
covers the real-sample envelope combinations."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import tempfile
import threading
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import aiosqlite
import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

import synology_apm_repo.sdk.storage.disk_space as disk_space_module
import synology_apm_repo.sdk.storage.sqlite_source as sqlite_source_module
from support.fakes import faithful_to
from support.format_builders import zstd_frame_without_content_size
from synology_apm_repo.sdk.errors import DataCorruptError, KeyRequiredError, ResourceLimitExceededError
from synology_apm_repo.sdk.format.compression import iter_decompressed_zstd
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore
from synology_apm_repo.sdk.storage.disk_space import DiskReservation
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite_source import (
    Envelope,
    SqliteSource,
    peel,
)
from unit.sdk.storage_fakes import fake_disk_usage


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
    # Plain sqlite3 only builds the fixture bytes; it isn't the SDK's read path.
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t(x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        return conn.serialize()
    finally:
        conn.close()


async def _fetchone(conn: aiosqlite.Connection, sql: str) -> Any:
    """One row as a plain tuple; ``Any`` because aiosqlite types ``fetchone()``
    as ``Row | None`` but the default row_factory yields tuples."""
    cursor = await conn.execute(sql)
    return await cursor.fetchone()


class TestPeel:
    """``peel()`` is synchronous — pure bytes work, no I/O."""

    def test_raw_passes_through_unchanged(self) -> None:
        data = _real_sqlite_bytes()
        payload, envelopes = peel(data, max_zstd_output_size=None)
        assert payload == data
        assert envelopes == []

    def test_zstd_only(self) -> None:
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        payload, envelopes = peel(compressed, max_zstd_output_size=None)
        assert payload == raw
        assert envelopes == [Envelope.ZSTD]

    def test_ahlt_only(self) -> None:
        vault_key = os.urandom(32)
        raw = _real_sqlite_bytes()
        enveloped = _build_ahlt(vault_key, raw)
        payload, envelopes = peel(enveloped, vault_key=vault_key, max_zstd_output_size=None)
        assert payload == raw
        assert envelopes == [Envelope.AHLT]

    def test_ahlt_then_zstd(self) -> None:
        vault_key = os.urandom(32)
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        enveloped = _build_ahlt(vault_key, compressed)
        payload, envelopes = peel(enveloped, vault_key=vault_key, max_zstd_output_size=None)
        assert payload == raw
        assert envelopes == [Envelope.AHLT, Envelope.ZSTD]

    def test_ahlt_without_vault_key_raises_key_required(self) -> None:
        vault_key = os.urandom(32)
        enveloped = _build_ahlt(vault_key, _real_sqlite_bytes())
        with pytest.raises(KeyRequiredError, match="data is aHlT-enveloped but no vault_key was given"):
            peel(enveloped, max_zstd_output_size=None)


class TestEffectiveZstdOutputSize:
    """``_effective_zstd_output_size()`` sizes the disk-space reservation that
    ``from_enveloped_bytes``/``from_enveloped_store`` run before writing."""

    def test_not_zstd_framed_uses_the_exact_write_size_not_the_fallback(self) -> None:
        # A plain payload is written as-is, so its own length sizes the check.
        plain = b"not a zstd frame, just some plain bytes"
        assert sqlite_source_module._effective_zstd_output_size(plain, 64 << 20) == len(plain)

    def test_zstd_framed_with_a_declared_size_uses_it(self) -> None:
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        assert sqlite_source_module._effective_zstd_output_size(compressed, 16) == len(raw)

    def test_zstd_framed_with_no_declared_size_uses_the_fallback(self) -> None:
        compressed = zstd_frame_without_content_size(_real_sqlite_bytes())
        assert sqlite_source_module._effective_zstd_output_size(compressed, 12345) == 12345


class TestPeelInto:
    """``_peel_into()``: ``peel()``'s contract, streaming the plain bytes into
    an open file instead of returning them (behind every enveloped
    ``SqliteSource`` factory)."""

    def test_raw_passes_through_unchanged(self, tmp_path: Path) -> None:
        data = _real_sqlite_bytes()
        dest = tmp_path / "out.db"
        with open(dest, "wb") as f:
            envelopes = sqlite_source_module._peel_into(
                data, f, vault_key=None, max_output_size=None, dir_path=tmp_path
            )
        assert dest.read_bytes() == data
        assert envelopes == []

    def test_zstd_only(self, tmp_path: Path) -> None:
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        dest = tmp_path / "out.db"
        with open(dest, "wb") as f:
            envelopes = sqlite_source_module._peel_into(
                compressed, f, vault_key=None, max_output_size=None, dir_path=tmp_path
            )
        assert dest.read_bytes() == raw
        assert envelopes == [Envelope.ZSTD]

    def test_ahlt_then_zstd(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        enveloped = _build_ahlt(vault_key, compressed)
        dest = tmp_path / "out.db"
        with open(dest, "wb") as f:
            envelopes = sqlite_source_module._peel_into(
                enveloped, f, vault_key=vault_key, max_output_size=None, dir_path=tmp_path
            )
        assert dest.read_bytes() == raw
        assert envelopes == [Envelope.AHLT, Envelope.ZSTD]

    def test_ahlt_without_vault_key_raises_key_required(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        enveloped = _build_ahlt(vault_key, _real_sqlite_bytes())
        dest = tmp_path / "out.db"
        with (
            open(dest, "wb") as f,
            pytest.raises(KeyRequiredError, match="data is aHlT-enveloped but no vault_key was given"),
        ):
            sqlite_source_module._peel_into(enveloped, f, vault_key=None, max_output_size=None, dir_path=tmp_path)

    def test_max_zstd_output_size_rejects_an_oversized_frame(self, tmp_path: Path) -> None:
        # No declared content size, so the fallback cap is enforced.
        raw = _real_sqlite_bytes() * 1000
        compressed = zstd_frame_without_content_size(raw)
        dest = tmp_path / "out.db"
        with (
            open(dest, "wb") as f,
            pytest.raises(zstandard.ZstdError, match="decompressed output exceeds max_output_size"),
        ):
            sqlite_source_module._peel_into(compressed, f, vault_key=None, max_output_size=16, dir_path=tmp_path)


class TestSqliteSource:
    """``SqliteSource``'s connection, temp-file cleanup and ``close()``,
    built through its factories (mostly ``from_bytes``)."""

    async def test_opens_a_working_connection(self) -> None:
        data = _real_sqlite_bytes()
        async with await SqliteSource.from_bytes(data) as src:
            row = await _fetchone(src.connection, "SELECT x FROM t")
            assert row == (1,)

    async def test_connection_is_writable_so_index_hints_can_take_effect(self) -> None:
        """The connection is writable (a private temp copy), which ``apply_index_hint`` needs."""
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
        assert path is not None
        assert os.path.exists(path)
        await src.close()
        assert not os.path.exists(path)

    async def test_close_removes_scratch_files_off_the_event_loop_thread(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Deleting the scratch copy is filesystem work that must not run on
        the thread driving the event loop."""
        src = await SqliteSource.from_bytes(_real_sqlite_bytes())
        path = src._path
        assert path is not None
        loop_thread = threading.get_ident()
        unlinks: list[tuple[str, int]] = []
        real_unlink = os.unlink

        def _recording_unlink(path: str) -> None:
            unlinks.append((path, threading.get_ident()))
            real_unlink(path)

        monkeypatch.setattr(os, "unlink", _recording_unlink)

        await src.close()

        assert [p for p, _ in unlinks] == [path, path + "-wal", path + "-shm", path + "-journal"]
        unlink_threads = {t for _, t in unlinks}
        assert len(unlink_threads) == 1
        assert loop_thread not in unlink_threads

    async def test_close_is_idempotent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Several owners can close the same source, so a second close must be safe.
        data = _real_sqlite_bytes()
        src = await SqliteSource.from_bytes(data)
        path = src._path
        assert path is not None
        await src.close()
        assert not os.path.exists(path)
        unlinked: list[str] = []
        monkeypatch.setattr(os, "unlink", unlinked.append)
        await src.close()  # must not raise FileNotFoundError
        assert unlinked == []  # the second close does no cleanup work at all

    async def test_failure_during_open_still_cleans_up_the_temp_file(self, monkeypatch: pytest.MonkeyPatch) -> None:
        data = _real_sqlite_bytes()

        created_paths: list[str] = []
        real_mkstemp = tempfile.mkstemp

        def spying_mkstemp(suffix: str | None = None, dir: str | Path | None = None) -> tuple[int, str]:  # noqa: A002
            fd, path = real_mkstemp(suffix=suffix, dir=dir)
            created_paths.append(path)
            return fd, path

        def failing_connect(database: str, uri: bool = False) -> aiosqlite.Connection:
            raise sqlite3.OperationalError("simulated failure")

        monkeypatch.setattr(tempfile, "mkstemp", spying_mkstemp)
        monkeypatch.setattr(aiosqlite, "connect", failing_connect)

        with pytest.raises(sqlite3.OperationalError, match="simulated failure"):
            await SqliteSource.from_bytes(data)

        assert len(created_paths) == 1
        assert not os.path.exists(created_paths[0])

    async def test_cancelling_a_temp_file_write_removes_the_file_once_the_write_ends(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        entered, release = threading.Event(), threading.Event()

        def blocking_peel_into(data: bytes, dest: Any, **_kwargs: Any) -> list[Envelope]:
            entered.set()
            release.wait(timeout=10)
            dest.write(data)
            return []

        monkeypatch.setattr(sqlite_source_module, "_peel_into", blocking_peel_into)
        task = asyncio.ensure_future(
            SqliteSource.from_enveloped_bytes(_real_sqlite_bytes(), max_output_size=None, tmp_dir=tmp_path, what="test")
        )
        await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        await asyncio.sleep(0)
        assert [p.suffix for p in tmp_path.iterdir()] == [".db"]  # still being written: not yet unlinked
        task.cancel()  # a second cancellation, during that cleanup wait
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert list(tmp_path.iterdir()) == []

    async def test_a_connect_cancelled_mid_flight_closes_what_it_opened_and_removes_the_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        connecting, release = asyncio.Event(), asyncio.Event()
        opened: list[_ClosingConnection] = []

        class _ClosingConnection:
            closed = False

            async def close(self) -> None:
                self.closed = True

        async def slow_connect(database: str, uri: bool = False) -> _ClosingConnection:
            connecting.set()
            await release.wait()  # the thread opening it can't be interrupted
            opened.append(_ClosingConnection())
            return opened[0]

        monkeypatch.setattr(aiosqlite, "connect", slow_connect)
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        task = asyncio.ensure_future(SqliteSource.from_bytes(_real_sqlite_bytes()))
        await connecting.wait()  # the temp file is written; the connect is in flight
        task.cancel()
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert opened and opened[0].closed
        assert list(tmp_path.iterdir()) == []

    async def test_close_from_a_different_thread_than_the_one_that_opened_it(self) -> None:
        # A source opened on a worker thread's own event loop closes from the main thread.
        data = _real_sqlite_bytes()
        errors: list[BaseException] = []
        sources: list[SqliteSource] = []

        async def open_on_worker_loop() -> None:
            sources.append(await SqliteSource.from_bytes(data))

        def worker_thread() -> None:
            try:
                asyncio.run(open_on_worker_loop())
            except BaseException as exc:  # noqa: BLE001
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

        payload, envelopes = peel(enveloped, vault_key=vault_key, max_zstd_output_size=None)
        assert envelopes == [Envelope.AHLT, Envelope.ZSTD]
        async with await SqliteSource.from_bytes(payload) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_from_bytes_opens_the_content(self) -> None:
        data = _real_sqlite_bytes()
        async with await SqliteSource.from_bytes(data) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)


class TestFromEnvelopedBytes:
    """``from_enveloped_bytes()`` streams the decrypt+decompress into its own
    temp file rather than materializing the decompressed payload as
    ``bytes`` first."""

    async def test_zstd_only_with_a_real_cap(self) -> None:
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        source, envelopes = await SqliteSource.from_enveloped_bytes(
            compressed, max_output_size=1 << 20, what="test blob"
        )
        try:
            assert envelopes == [Envelope.ZSTD]
            assert await _fetchone(source.connection, "SELECT x FROM t") == (1,)
        finally:
            await source.close()

    async def test_disk_space_check_runs_off_the_event_loop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # reserve_disk_space blocks, so it must run via asyncio.to_thread().
        seen: list[threading.Thread] = []
        real_reserve_disk_space = disk_space_module.reserve_disk_space

        @contextlib.contextmanager
        def _spy(dir_path: Path, needed_bytes: int | None) -> Iterator[DiskReservation]:
            seen.append(threading.current_thread())
            with real_reserve_disk_space(dir_path, needed_bytes) as reservation:
                yield reservation

        monkeypatch.setattr(sqlite_source_module, "reserve_disk_space", _spy)
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        source, _envelopes = await SqliteSource.from_enveloped_bytes(
            compressed, max_output_size=1 << 20, tmp_dir=str(tmp_path), what="test blob"
        )
        await source.close()
        assert seen and seen[0] is not threading.main_thread()

    async def test_ahlt_then_zstd_with_max_output_size_none(self) -> None:
        vault_key = os.urandom(32)
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        enveloped = _build_ahlt(vault_key, compressed)
        source, envelopes = await SqliteSource.from_enveloped_bytes(
            enveloped, vault_key=vault_key, max_output_size=None, what="test blob"
        )
        try:
            assert envelopes == [Envelope.AHLT, Envelope.ZSTD]
            assert await _fetchone(source.connection, "SELECT x FROM t") == (1,)
        finally:
            await source.close()

    async def test_close_removes_the_temp_file(self) -> None:
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        source, _envelopes = await SqliteSource.from_enveloped_bytes(
            compressed, max_output_size=1 << 20, what="test blob"
        )
        path = source.path
        assert path is not None
        assert os.path.exists(path)
        await source.close()
        assert not os.path.exists(path)

    async def test_ahlt_without_vault_key_raises_data_corrupt_and_creates_no_leaked_file(self, tmp_path: Path) -> None:
        # KeyRequiredError is translated to DataCorruptError, like a malformed
        # zstd frame, so a candidate that merely fakes the aHlT magic
        # degrades like any other corrupt one.
        vault_key = os.urandom(32)
        enveloped = _build_ahlt(vault_key, _real_sqlite_bytes())
        with pytest.raises(DataCorruptError, match="failed to decompress"):
            await SqliteSource.from_enveloped_bytes(
                enveloped, max_output_size=1 << 20, tmp_dir=str(tmp_path), what="test blob"
            )
        assert list(tmp_path.iterdir()) == []

    async def test_a_decompression_failure_mid_stream_cleans_up_the_partial_temp_file(self, tmp_path: Path) -> None:
        # An invalid frame descriptor byte (right after the magic) raises after the temp file exists.
        raw = _real_sqlite_bytes() * 1000
        compressed = bytearray(zstandard.ZstdCompressor().compress(raw))
        compressed[4] = 0xFF
        with pytest.raises(DataCorruptError, match="failed to decompress"):
            await SqliteSource.from_enveloped_bytes(
                bytes(compressed), max_output_size=1 << 30, tmp_dir=str(tmp_path), what="test blob"
            )
        assert list(tmp_path.iterdir()) == []

    async def test_max_output_size_exceeded_cleans_up_the_partial_temp_file(self, tmp_path: Path) -> None:
        # No declared content size, so the fallback cap is enforced.
        raw = _real_sqlite_bytes() * 1000
        compressed = zstd_frame_without_content_size(raw)
        with pytest.raises(DataCorruptError, match="failed to decompress"):
            await SqliteSource.from_enveloped_bytes(
                compressed, max_output_size=16, tmp_dir=str(tmp_path), what="test blob"
            )
        assert list(tmp_path.iterdir()) == []

    async def test_insufficient_disk_space_raises_and_leaves_no_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Nothing free above the 1 GiB floor of a 16 GiB filesystem.
        fake_disk_usage(monkeypatch, total=16 << 30, free=1 << 30)
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        with pytest.raises(ResourceLimitExceededError, match="not enough free space under"):
            await SqliteSource.from_enveloped_bytes(
                compressed, max_output_size=1 << 30, tmp_dir=str(tmp_path), what="test blob"
            )
        assert list(tmp_path.iterdir()) == []

    async def test_max_output_size_none_with_no_declared_size_stops_at_the_space_above_the_reserve(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The resource error must not be translated to DataCorruptError.
        raw = _real_sqlite_bytes() * 10
        fake_disk_usage(monkeypatch, total=16 << 30, free=(1 << 30) + len(raw) - 1)
        compressed = zstd_frame_without_content_size(raw)
        with pytest.raises(ResourceLimitExceededError, match=r"needs more than 80\.0 KiB plus 1\.0 GiB kept free"):
            await SqliteSource.from_enveloped_bytes(
                compressed, max_output_size=None, tmp_dir=str(tmp_path), what="test blob"
            )
        assert list(tmp_path.iterdir()) == []
        assert disk_space_module._in_flight == {}

    async def test_max_output_size_none_with_no_declared_size_fits_when_there_is_room(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        raw = _real_sqlite_bytes()
        fake_disk_usage(monkeypatch, total=16 << 30, free=(1 << 30) + len(raw))
        compressed = zstd_frame_without_content_size(raw)
        source, envelopes = await SqliteSource.from_enveloped_bytes(
            compressed, max_output_size=None, tmp_dir=str(tmp_path), what="test blob"
        )
        async with source:
            assert envelopes == [Envelope.ZSTD]
            assert await _fetchone(source.connection, "SELECT x FROM t") == (1,)

    async def test_a_cancelled_write_releases_its_disk_space_hold(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        started = threading.Event()
        proceed = threading.Event()
        real_iter = iter_decompressed_zstd

        def _blocking_iter(data: bytes | bytearray, *, max_output_size: int | None) -> Iterator[bytes]:
            started.set()
            proceed.wait()
            yield from real_iter(data, max_output_size=max_output_size)

        monkeypatch.setattr(sqlite_source_module, "iter_decompressed_zstd", _blocking_iter)
        task = asyncio.ensure_future(
            SqliteSource.from_enveloped_bytes(compressed, max_output_size=None, tmp_dir=str(tmp_path), what="test blob")
        )
        await asyncio.to_thread(started.wait)
        assert sum(disk_space_module._in_flight.values()) == len(raw)
        task.cancel()
        proceed.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert disk_space_module._in_flight == {}
        assert list(tmp_path.iterdir()) == []

    async def test_max_output_size_none_with_a_declared_size_frame_still_checks_disk_space(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A declared size is the effective ceiling even with max_output_size=None.
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        declared = zstandard.get_frame_parameters(compressed).content_size
        assert declared == len(raw)
        fake_disk_usage(monkeypatch, total=16 << 30, free=(1 << 30) + declared - 1)
        with pytest.raises(ResourceLimitExceededError, match="not enough free space under"):
            await SqliteSource.from_enveloped_bytes(
                compressed, max_output_size=None, tmp_dir=str(tmp_path), what="test blob"
            )
        assert list(tmp_path.iterdir()) == []

        fake_disk_usage(monkeypatch, total=16 << 30, free=(1 << 30) + declared)
        source, envelopes = await SqliteSource.from_enveloped_bytes(
            compressed, max_output_size=None, tmp_dir=str(tmp_path), what="test blob"
        )
        try:
            assert envelopes == [Envelope.ZSTD]
            assert await _fetchone(source.connection, "SELECT x FROM t") == (1,)
        finally:
            await source.close()


@faithful_to(ObjectStore)
class _CountingMemoryStore:
    """A non-``LocalFsStore`` ``ObjectStore`` fake: ``open_sqlite``'s
    ``LocalFsStore`` fast path is skipped, so ``read()`` call counts proxy
    network round trips."""

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

    async def close(self) -> None:
        pass

    async def listdir(self, path: str) -> list[Entry]:
        raise NotImplementedError


class TestFromRawStore:
    """``from_raw_store``: the entry point for a path that is never enveloped,
    read via ``open_sqlite`` (``db/<name>``, ``saas/*/db/saas_{version,
    snapshot}``). Possibly-enveloped sources use ``from_enveloped_store`` or
    ``from_enveloped_bytes`` (covered in their own classes)."""

    async def test_reads_a_raw_file_correctly(self, tmp_path: Path) -> None:
        (tmp_path / "db").write_bytes(_real_sqlite_bytes())
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_raw_store(store, "db") as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_reads_the_path_only_once(self) -> None:
        # No throwaway peek read: one store.read() per open.
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
            # The fast path opens the real file directly: no temp file/dir.
            assert src._path is None
            assert src._tmp_dir is None

    async def test_a_real_nonempty_wal_sees_the_wals_content(self, tmp_path: Path) -> None:
        # A real -wal with un-checkpointed pages must be honored. The writer
        # stays open because closing it would checkpoint the -wal away.
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
    """``from_enveloped_store``: the entry point for a source that may be
    ``aHlT``-enveloped and have a ``-wal``/``-shm`` sidecar
    (``copy_meta_file/*/target.db``, opened by ``catalog/version.py``'s
    ``open_target_db()``; see FORMAT-SPEC.md: Landing directory layout; File-level encryption)."""

    async def test_reads_a_plaintext_file_with_no_vault_key(self, tmp_path: Path) -> None:
        (tmp_path / "target.db").write_bytes(_real_sqlite_bytes())
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(store, "target.db", vault_key=None, what="test blob") as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_ahlt_without_vault_key_raises_data_corrupt(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        (tmp_path / "target.db").write_bytes(_build_ahlt(vault_key, _real_sqlite_bytes()))
        store = LocalFsStore(tmp_path)
        with pytest.raises(DataCorruptError, match="failed to decompress"):
            await SqliteSource.from_enveloped_store(store, "target.db", vault_key=None, what="test blob")

    async def test_ahlt_with_no_wal_decrypts_correctly(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        (tmp_path / "target.db").write_bytes(_build_ahlt(vault_key, _real_sqlite_bytes()))
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(
            store, "target.db", vault_key=vault_key, what="test blob"
        ) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_zero_length_wal_is_still_ignored_under_envelope(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        (tmp_path / "target.db").write_bytes(_build_ahlt(vault_key, _real_sqlite_bytes()))
        (tmp_path / "target.db-wal").write_bytes(b"")  # present, but empty
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(
            store, "target.db", vault_key=vault_key, what="test blob"
        ) as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_ahlt_with_a_real_wal_decrypts_and_merges_both_independently(self, tmp_path: Path) -> None:
        # The main file and its -wal get different random IVs, so each must
        # be peeled independently.
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
        async with await SqliteSource.from_enveloped_store(
            store, "target.db", vault_key=vault_key, what="test blob"
        ) as src:
            cursor = await src.connection.execute("SELECT x FROM t ORDER BY x")
            rows: list[Any] = list(await cursor.fetchall())
            assert rows == [(1,), (2,)]  # the WAL-only row decoded and merged correctly

    async def test_a_declared_size_over_the_fallback_is_still_honored_in_full(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A declared size takes precedence over the fallback cap (patched small here).
        monkeypatch.setattr(sqlite_source_module, "_MAX_TARGET_DB_DECOMPRESS_SIZE_FALLBACK", 16)
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        assert zstandard.get_frame_parameters(compressed).content_size == len(raw) > 16
        (tmp_path / "target.db").write_bytes(compressed)
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(store, "target.db", vault_key=None, what="test blob") as src:
            assert await _fetchone(src.connection, "SELECT x FROM t") == (1,)

    async def test_disk_space_check_is_sized_off_a_declared_size_when_present(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        raw = _real_sqlite_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        declared = zstandard.get_frame_parameters(compressed).content_size
        assert declared == len(raw)
        seen: list[int | None] = []
        real_reserve_disk_space = disk_space_module.reserve_disk_space

        @contextlib.contextmanager
        def _spy(dir_path: Path, needed_bytes: int | None) -> Iterator[DiskReservation]:
            seen.append(needed_bytes)
            with real_reserve_disk_space(dir_path, needed_bytes) as reservation:
                yield reservation

        monkeypatch.setattr(sqlite_source_module, "reserve_disk_space", _spy)
        (tmp_path / "target.db").write_bytes(compressed)
        store = LocalFsStore(tmp_path)
        async with await SqliteSource.from_enveloped_store(store, "target.db", vault_key=None, what="test blob"):
            pass
        assert declared in seen  # sized off the declared value, not the fallback

    async def test_no_declared_size_falls_back_to_the_fallback_and_rejects_an_oversized_frame(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A cap-exceeded zstandard.ZstdError is translated to DataCorruptError.
        monkeypatch.setattr(sqlite_source_module, "_MAX_TARGET_DB_DECOMPRESS_SIZE_FALLBACK", 16)
        raw = _real_sqlite_bytes() * 1000
        compressed = zstd_frame_without_content_size(raw)
        assert zstandard.get_frame_parameters(compressed).content_size == zstandard.CONTENTSIZE_UNKNOWN
        (tmp_path / "target.db").write_bytes(compressed)
        store = LocalFsStore(tmp_path)
        with pytest.raises(DataCorruptError, match="failed to decompress"):
            await SqliteSource.from_enveloped_store(store, "target.db", vault_key=None, what="test blob")
