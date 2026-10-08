"""Unit tests for ``synology_apm_repo.sdk.units.saas.objectdb``: ``ObjectDb``
over hand-built ObjectDB bytes (``from_bytes``, or ``load`` through an
in-memory ``FakeDedupFile``), and ``parse_object_db_id``."""

from __future__ import annotations

import contextlib
import io
import sqlite3
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
import zstandard

from support.format_builders import (
    build_object_db,
)
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.storage.disk_space import DiskReservation, reserve_disk_space
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.units.saas import objectdb as objectdb_module
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb, parse_object_db_id
from unit.sdk.saas_fakes import FakeDedupFile


class TestObjectDbOpenAndLookup:
    async def test_load_and_lookup(self) -> None:
        db_bytes = build_object_db([("v1_object_1", 100, 200), ("v1_object_2", 300, 400)])
        fake = FakeDedupFile(db_bytes)
        async with await ObjectDb.load(cast(DedupFile, fake), 0, len(db_bytes)) as db:
            assert await db.get("v1_object_1") == (100, 200)
            assert await db.get("v1_object_2") == (300, 400)
            assert await db.object_map() == {"v1_object_1": (100, 200), "v1_object_2": (300, 400)}

    async def test_get_unknown_object_id_raises_not_found(self) -> None:
        db_bytes = build_object_db([("v1_object_1", 0, 10)])
        async with await ObjectDb.from_bytes(db_bytes) as db:
            with pytest.raises(NotFoundError, match="no object_table row for object_id"):
                await db.get("no-such-object")

    async def test_get_many_returns_only_the_ids_that_have_a_row(self) -> None:
        db_bytes = build_object_db([("a", 0, 10), ("b", 10, 20), ("c", 30, 5)])
        async with await ObjectDb.from_bytes(db_bytes) as db:
            assert await db.get_many(["c", "missing", "a"]) == {"c": (30, 5), "a": (0, 10)}
            assert await db.get_many([]) == {}

    async def test_object_map_is_ordered_by_object_id_not_by_row_order(self) -> None:
        db_bytes = build_object_db([("zeta", 0, 1), ("alpha", 1, 1), ("mid", 2, 1)])
        async with await ObjectDb.from_bytes(db_bytes) as db:
            assert list(await db.object_map()) == ["alpha", "mid", "zeta"]

    async def test_object_db_is_an_async_context_manager(self) -> None:
        db_bytes = build_object_db([("v1_object_1", 0, 10)])
        async with await ObjectDb.from_bytes(db_bytes) as db:
            assert await db.object_map() == {"v1_object_1": (0, 10)}

    async def test_disk_space_check_is_sized_off_the_real_payload_not_the_fallback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Sized off the payload, not _MAX_OBJECT_DB_DECOMPRESS_SIZE_FALLBACK
        # (64 MiB), which would reject a tiny write on a nearly full host.
        import synology_apm_repo.sdk.storage.sqlite_source as sqlite_source_module

        db_bytes = build_object_db([("v1_object_1", 0, 10)])
        seen: list[int | None] = []
        real_reserve_disk_space = reserve_disk_space

        @contextlib.contextmanager
        def _spy(dir_path: Path, needed_bytes: int | None) -> Iterator[DiskReservation]:
            seen.append(needed_bytes)
            with real_reserve_disk_space(dir_path, needed_bytes) as reservation:
                yield reservation

        monkeypatch.setattr(sqlite_source_module, "reserve_disk_space", _spy)
        async with await ObjectDb.from_bytes(db_bytes):
            pass
        assert seen == [len(db_bytes)]

    async def test_missing_object_table_closes_the_source_and_reraises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A stale index entry can land on SQLite with no object_table; the
        # opened SqliteSource must not leak (an unclosed aiosqlite
        # connection hangs interpreter shutdown).
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

        with pytest.raises(DataCorruptError, match="table 'object_table' does not exist"):
            await ObjectDb.from_bytes(db_bytes)

        assert closed

    async def test_a_declared_size_over_the_fallback_is_still_honored_in_full(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A declared content size replaces the fallback as the ceiling; the
        # fallback is patched below the payload's declared size.
        monkeypatch.setattr(objectdb_module, "_MAX_OBJECT_DB_DECOMPRESS_SIZE_FALLBACK", 16)
        db_bytes = build_object_db([("v1_object_1", 0, 10)])
        compressed = zstandard.ZstdCompressor().compress(db_bytes)
        assert zstandard.get_frame_parameters(compressed).content_size == len(db_bytes) > 16
        async with await ObjectDb.from_bytes(compressed) as db:
            assert await db.object_map() == {"v1_object_1": (0, 10)}

    async def test_no_declared_size_falls_back_to_the_fallback_and_rejects_an_oversized_frame(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The cap-exceeded ZstdError surfaces as DataCorruptError, which
        # from_bytes()'s callers catch.
        monkeypatch.setattr(objectdb_module, "_MAX_OBJECT_DB_DECOMPRESS_SIZE_FALLBACK", 16)
        db_bytes = build_object_db([("v1_object_1", 0, 10)]) * 1000
        buf = io.BytesIO()
        writer = zstandard.ZstdCompressor(write_content_size=False).stream_writer(buf, closefd=False)
        writer.write(db_bytes)
        writer.flush(zstandard.FLUSH_FRAME)
        compressed = buf.getvalue()
        assert zstandard.get_frame_parameters(compressed).content_size == zstandard.CONTENTSIZE_UNKNOWN
        with pytest.raises(DataCorruptError, match="failed to decompress"):
            await ObjectDb.from_bytes(compressed)

    async def test_a_malformed_zstd_magic_prefixed_header_raises_data_corrupt_not_a_raw_zstderror(self) -> None:
        # The zstd magic with too few bytes left for a frame header makes
        # zstandard.get_frame_parameters() raise ZstdError.
        malformed = b"\x28\xb5\x2f\xfd\x00"
        with pytest.raises(DataCorruptError, match="failed to decompress"):
            await ObjectDb.from_bytes(malformed)


class TestParseObjectDbId:
    """The manual-override string, ``<streamUuid>_<offset>_<length>``."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(
                "AbCdEfGhIjKlMnOp_27467776_12288", ("AbCdEfGhIjKlMnOp", 27467776, 12288), id="the_documented_shape"
            ),
            pytest.param(
                "weird_stream_uuid_5_10",
                ("weird_stream_uuid", 5, 10),
                id="offset_and_length_are_always_the_last_two_fields_even_with_underscores_in_the_id",
            ),
        ],
    )
    def test_parses(self, value: str, expected: tuple[str, int, int]) -> None:
        assert parse_object_db_id(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("no-underscores-at-all", id="missing_underscore"),
            pytest.param("stream_notanumber_10", id="non_numeric_offset"),
            pytest.param("stream_10_notanumber", id="non_numeric_length"),
            pytest.param("stream_\u00b2_10", id="superscript_digit_offset"),
            pytest.param("stream_10_\u0661", id="non_ascii_decimal_digit_length"),
        ],
    )
    def test_malformed_raises_not_found(self, value: str) -> None:
        with pytest.raises(NotFoundError, match="malformed"):
            parse_object_db_id(value)
