"""Unit tests for ``synology_apm_repo.sdk.units.saas.objectdb`` —
a lightweight fake ``DedupFile`` backed by an in-memory buffer for
``ObjectDb`` itself, plus ``parse_object_db_id``'s manual-
override string parsing.

Every real caller (every application-layer provider, and
``RawObjectProvider`` too) resolves its content through the
connector's own object-name index instead, never a scan (see
``units/saas/raw_object.py``'s own module docstring)."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from typing import cast

import pytest

from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb, parse_object_db_id


def _build_object_db(rows: list[tuple[str, int, int]]) -> bytes:
    """A real, valid SQLite file (page-aligned, correct header) with an
    ``object_table`` — the same schema real ObjectDBs have."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE object_table(object_id TEXT PRIMARY KEY, offset INTEGER, length INTEGER)")
        conn.executemany("INSERT INTO object_table VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        return path.read_bytes()


class _FakeDedupFile:
    """Wraps an in-memory buffer — ``ObjectDb``'s entire contract
    with ``DedupFile`` is ``.read()`` (via ``peel``), so this is
    enough to test it without a real repository."""

    def __init__(self, buf: bytes) -> None:
        self._buf = buf
        self.size = len(buf)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if length is None:
            length = self.size - offset
        return self._buf[offset : offset + length]


class TestObjectDbOpenAndLookup:
    async def test_load_and_lookup(self) -> None:
        db_bytes = _build_object_db([("v1_object_1", 100, 200), ("v1_object_2", 300, 400)])
        fake = _FakeDedupFile(db_bytes)
        async with await ObjectDb.load(cast(DedupFile, fake), 0, len(db_bytes)) as db:
            assert await db.get("v1_object_1") == (100, 200)
            assert await db.get("v1_object_2") == (300, 400)
            assert await db.object_map() == {"v1_object_1": (100, 200), "v1_object_2": (300, 400)}

    async def test_get_unknown_object_id_raises_not_found(self) -> None:
        db_bytes = _build_object_db([("v1_object_1", 0, 10)])
        async with await ObjectDb.from_bytes(db_bytes) as db:
            with pytest.raises(NotFoundError):
                await db.get("no-such-object")

    async def test_object_db_is_an_async_context_manager(self) -> None:
        db_bytes = _build_object_db([("v1_object_1", 0, 10)])
        async with await ObjectDb.from_bytes(db_bytes) as db:
            assert await db.object_map() == {"v1_object_1": (0, 10)}

    async def test_missing_object_table_closes_the_source_and_reraises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A bad (offset, length) pair -- a stale/corrupt object-name index
        # entry landing on real SQLite bytes that simply have no
        # object_table -- must not leak the already-opened SqliteSource
        # (an unclosed aiosqlite connection hangs interpreter shutdown).
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "no_object_table.db"
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE unrelated(x INTEGER)")
            conn.commit()
            conn.close()
            db_bytes = path.read_bytes()

        original_close = SqliteSource.close
        closed = False

        async def spy_close(self: SqliteSource) -> None:
            nonlocal closed
            closed = True
            await original_close(self)

        monkeypatch.setattr(SqliteSource, "close", spy_close)

        with pytest.raises(DataCorruptError):
            await ObjectDb.from_bytes(db_bytes)

        assert closed


class TestParseObjectDbId:
    """The manual-override string, ``<streamUuid>_<offset>_<length>``
    (this module's own docstring)."""

    def test_parses_the_documented_shape(self) -> None:
        assert parse_object_db_id("DRMdjvEJPzoxQiUC_27467776_12288") == ("DRMdjvEJPzoxQiUC", 27467776, 12288)

    def test_offset_and_length_are_always_the_last_two_fields_even_with_underscores_in_the_id(self) -> None:
        # No real stream_uuid seen in samples has an underscore, but the
        # split must still be robust from the right, not the left.
        assert parse_object_db_id("weird_stream_uuid_5_10") == ("weird_stream_uuid", 5, 10)

    def test_missing_underscore_raises_not_found(self) -> None:
        with pytest.raises(NotFoundError, match="malformed"):
            parse_object_db_id("no-underscores-at-all")

    def test_non_numeric_offset_raises_not_found(self) -> None:
        with pytest.raises(NotFoundError, match="malformed"):
            parse_object_db_id("stream_notanumber_10")

    def test_non_numeric_length_raises_not_found(self) -> None:
        with pytest.raises(NotFoundError, match="malformed"):
            parse_object_db_id("stream_10_notanumber")
