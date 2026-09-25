"""Unit tests for ``synology_apm_repo.sdk.units.saas.services`` — pure
synthetic bytes for ``sniff()``, a lightweight fake ``DedupFile`` (same
pattern as ``test_units_saas_objectdb.py``) for ``inspect_object()``
(see ``tests/integration/sdk/test_saas_stream_and_objectdb_discovery.py``
for the cross-check against real apv-sample-1 service DBs). An
object's location always comes from the connector's own object-name
index, never a schema scan; ``sniff()`` only guesses which service owns
an already-located ``SERVICE_DB`` blob from its table names."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
from pathlib import Path
from typing import cast

import pytest
import zstandard

from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.errors import DataCorruptError
from synology_apm_repo.sdk.storage.sqlite_source import peel as _real_peel
from synology_apm_repo.sdk.units.saas.services import (
    IndexEntry,
    ServiceKind,
    decompress_service_db,
    inspect_object,
    open_service_db,
    sniff,
)

_SQLITE_MAGIC = b"SQLite format 3\x00"


def _build_service_db(table_name: str, *, padding_blob: bytes = b"") -> bytes:
    """A real, valid SQLite file with a table name that
    ``_SERVICE_TABLE_HINTS`` recognizes, ZSTD-compressed the way a real
    service-level DB snapshot is stored inside a ``saas_obj`` object.
    ``padding_blob``, when given, is stored as an extra ``config_table``
    row's own BLOB value — inflating both the raw and (being close to
    incompressible for random bytes) the compressed size without any
    bytes existing outside the one real zstd frame, unlike padding the
    *compressed* output with trailing garbage (a shape
    ``inspect_object()`` never actually sees: its own ``length`` always
    comes from the connector's object-name index, exactly sized to the real
    object)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "svc.db"
        conn = sqlite3.connect(path)
        conn.execute(f"CREATE TABLE {table_name}(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE config_table(k TEXT, v TEXT)")
        if padding_blob:
            conn.execute("INSERT INTO config_table VALUES ('padding', ?)", (padding_blob,))
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


class _FakeDedupFile:
    """``inspect_object()``'s entire contract with
    ``DedupFile`` is ``.read()`` (same pattern as
    ``test_units_saas_objectdb.py``'s fake + ``cast``)."""

    def __init__(self, buf: bytes) -> None:
        self._buf = buf
        self.size = len(buf)
        self.read_calls: list[tuple[int, int]] = []

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if length is None:
            length = self.size - offset
        self.read_calls.append((offset, length))
        return self._buf[offset : offset + length]


class TestSniff:
    async def test_sniffs_a_service_db(self) -> None:
        blob = _build_service_db("item_table")
        result = await sniff(blob)
        assert result.kind is ServiceKind.SERVICE_DB
        assert result.tables == frozenset({"item_table", "config_table"})
        assert result.service_name == "drive"

    async def test_sniffs_a_service_db_with_no_recognized_table(self) -> None:
        blob = _build_service_db("some_unknown_table")
        result = await sniff(blob)
        assert result.kind is ServiceKind.SERVICE_DB
        assert result.service_name is None

    async def test_sniffs_the_index_shape_with_db_objects_list(self) -> None:
        data = b'{"db_objects":[{"name":"mail_db","object_id":"v1_object_1"}],"version":1}'
        result = await sniff(data)
        assert result.kind is ServiceKind.INDEX
        assert result.index_entries == (IndexEntry(name="mail_db", object_id="v1_object_1"),)

    async def test_sniffs_the_indirect_index_shape(self) -> None:
        data = b'{"name":"db_infos_in_snapshot","object_id":"v1_object_42"}'
        result = await sniff(data)
        assert result.kind is ServiceKind.INDEX
        assert len(result.index_entries) == 1
        assert result.index_entries[0].name == "db_infos_in_snapshot"
        assert result.index_entries[0].object_id == "v1_object_42"

    async def test_speculative_peel_runs_on_a_worker_thread_not_the_event_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``sniff()``'s own speculative ``peel()`` call (up to
        ``_MAX_SNIFF_DECOMPRESS`` = 128 MiB, on data not yet confirmed to
        even be real SQLite) is the other independent thread-hop this
        module needed — a different call site than
        ``TestDecompressServiceDb``'s, so verified separately rather than
        assumed to follow from that one passing."""
        import synology_apm_repo.sdk.units.saas.services as services_module

        blob = _build_service_db("item_table")
        main_thread = threading.current_thread()
        seen_threads: list[threading.Thread] = []

        def _spying_peel(data: bytes, **kwargs: object) -> tuple[bytes, list[object]]:
            seen_threads.append(threading.current_thread())
            return _real_peel(data, **kwargs)  # type: ignore[arg-type,return-value]

        monkeypatch.setattr(services_module, "peel", _spying_peel)

        result = await sniff(blob)
        assert result.kind is ServiceKind.SERVICE_DB
        assert len(seen_threads) == 1
        assert seen_threads[0] is not main_thread

    async def test_sniffs_generic_meta_json(self) -> None:
        data = b'{"content_list": [], "values": {"Attachments": false}}'
        result = await sniff(data)
        assert result.kind is ServiceKind.META_JSON

    async def test_meta_json_tolerates_a_leading_newline(self) -> None:
        # real apv-sample-1 META objects are prefixed with "\n" before "{"
        data = b'\n{"fields": []}'
        result = await sniff(data)
        assert result.kind is ServiceKind.META_JSON

    async def test_invalid_json_starting_with_brace_falls_through_to_binary(self) -> None:
        data = b"{not valid json at all"
        result = await sniff(data)
        assert result.kind is ServiceKind.BINARY

    async def test_sniffs_an_rfc822_skeleton(self) -> None:
        data = b"Received: from mail.example.com\r\nFrom: a@example.com\r\nMIME-Version: 1.0\r\n\r\nbody"
        result = await sniff(data)
        assert result.kind is ServiceKind.MAIL_SKELETON

    async def test_sniffs_binary_content(self) -> None:
        data = b"\xff\xd8\xff\xe1\x00\x18Exif\x00\x00"  # JPEG/EXIF magic
        result = await sniff(data)
        assert result.kind is ServiceKind.BINARY

    async def test_empty_bytes_is_binary(self) -> None:
        assert (await sniff(b"")).kind is ServiceKind.BINARY

    async def test_zstd_payload_that_is_not_sqlite_is_binary(self) -> None:
        compressed = zstandard.ZstdCompressor().compress(b"just some text, not a database")
        result = await sniff(compressed)
        assert result.kind is ServiceKind.BINARY

    async def test_truncated_zstd_frame_is_binary_not_an_exception(self) -> None:
        compressed = zstandard.ZstdCompressor().compress(b"x" * 10_000)
        result = await sniff(compressed[:8])  # truncated frame
        assert result.kind is ServiceKind.BINARY

    async def test_content_over_the_sniff_decompress_cap_is_treated_as_not_zstd_framed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A candidate whose zstd magic matches but whose decompressed
        size exceeds ``_MAX_SNIFF_DECOMPRESS`` must fall back to
        ``BINARY``, the same as a genuinely non-zstd candidate — never
        raise out of ``sniff()`` itself, which never raises.
        ``_MAX_SNIFF_DECOMPRESS`` monkeypatched down so this
        test doesn't need an actual 128 MiB payload to trip it.

        Regression coverage for the cap's own reason to exist:
        ``zstandard``'s one-shot ``decompress(data,
        max_output_size=N)`` silently ignores the cap for a frame that
        declares its own content size — the shape ``_build_service_db``
        always produces, and the common real-world shape — so the
        bounded path must use the streaming decompressor instead, or
        this candidate would wrongly land on ``SERVICE_DB``."""
        import synology_apm_repo.sdk.units.saas.services as services_module

        monkeypatch.setattr(services_module, "_MAX_SNIFF_DECOMPRESS", 1024)
        blob = _build_service_db("item_table", padding_blob=os.urandom(4096))
        result = await sniff(blob)
        assert result.kind is ServiceKind.BINARY


class TestInspectObject:
    async def test_reads_the_full_object_when_under_the_cap(self) -> None:
        blob = b'{"content_list": []}'
        fake = _FakeDedupFile(blob)
        result = await inspect_object(cast(DedupFile, fake), 0, len(blob))
        assert result.kind is ServiceKind.META_JSON
        assert fake.read_calls == [(0, len(blob))]

    async def test_reads_only_a_head_when_over_the_cap_and_not_zstd_magic(self) -> None:
        big = b"\x00" * (10 << 20)  # 10 MiB, over the 8 MiB cap, no zstd magic
        fake = _FakeDedupFile(big)
        result = await inspect_object(cast(DedupFile, fake), 0, len(big))
        assert result.kind is ServiceKind.BINARY
        assert fake.read_calls == [(0, 4096)]  # head only, not the full 10 MiB

    async def test_reads_the_full_object_when_over_the_cap_but_zstd_magic_matches(self) -> None:
        """A small head is read first to check the zstd magic; only a
        genuine match costs the second, full read a real Teams
        channel's message DB (well over this cap, compressed) needs — a
        real service DB carrying a large, incompressible
        BLOB row stands in for "a real object big enough to need the
        full read" (not trailing garbage after the frame, which
        ``inspect_object()`` never actually sees -- see
        ``_build_service_db``'s ``padding_blob`` parameter above)."""
        big = _build_service_db("item_table", padding_blob=os.urandom(9 << 20))
        assert len(big) > (8 << 20), "test invariant: must actually be large enough to trip the 8 MiB cap"
        fake = _FakeDedupFile(big)
        result = await inspect_object(cast(DedupFile, fake), 0, len(big))
        assert result.kind is ServiceKind.SERVICE_DB
        assert result.tables == {"item_table", "config_table"}
        assert fake.read_calls == [(0, 4096), (0, len(big))]  # head check, then the real full read


class TestDecompressServiceDb:
    """``decompress_service_db`` is ``async`` so its own
    ``peel()`` call can hop to ``asyncio.to_thread`` — a real Teams
    channel's message DB decompresses to tens of MiB, well within
    real-world scale for a service DB, the same class of "multi-MB
    decrypt+decompress must not block the event loop" work
    ``dedup/pool/_bucket_reader.py`` already has a stated policy for."""

    async def test_decompresses_a_real_service_db(self) -> None:
        blob = _build_service_db("item_table")
        payload = await decompress_service_db(blob)
        assert payload.startswith(_SQLITE_MAGIC)

    async def test_peel_runs_on_a_worker_thread_not_the_event_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import synology_apm_repo.sdk.units.saas.services as services_module

        blob = _build_service_db("item_table")
        main_thread = threading.current_thread()
        seen_threads: list[threading.Thread] = []

        def _spying_peel(data: bytes, **kwargs: object) -> tuple[bytes, list[object]]:
            seen_threads.append(threading.current_thread())
            return _real_peel(data, **kwargs)  # type: ignore[arg-type,return-value]

        monkeypatch.setattr(services_module, "peel", _spying_peel)

        payload = await decompress_service_db(blob)
        assert payload.startswith(_SQLITE_MAGIC)
        assert len(seen_threads) == 1
        assert seen_threads[0] is not main_thread

    async def test_a_zstd_error_from_peel_surfaces_as_data_corrupt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import synology_apm_repo.sdk.units.saas.services as services_module

        def _failing_peel(data: bytes, **kwargs: object) -> tuple[bytes, list[object]]:
            raise zstandard.ZstdError("corrupted zstd frame")

        monkeypatch.setattr(services_module, "peel", _failing_peel)

        with pytest.raises(DataCorruptError, match="failed to decompress"):
            await decompress_service_db(b"whatever")


class TestOpenServiceDb:
    async def test_opens_a_real_service_db_for_querying(self) -> None:
        blob = _build_service_db("item_table")
        async with await open_service_db(blob) as source:
            cursor = await source.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            rows = await cursor.fetchall()
            assert {r[0] for r in rows} == {"item_table", "config_table"}

    async def test_raises_data_corrupt_when_not_zstd_framed(self) -> None:
        with pytest.raises(DataCorruptError, match="not ZSTD-framed"):
            await open_service_db(b'{"content_list": []}')

    async def test_raises_data_corrupt_on_severely_truncated_zstd_frame(self) -> None:
        """Truncated right after the frame header, with none of the
        actual compressed block surviving. ``decompress_service_db``'s
        own "unbounded" path is deliberately unbounded, not the one-shot
        API with a size cap, since a real cap couldn't fit a real
        service DB's legitimate size — for input this short, the
        streaming reader itself doesn't raise, it simply produces zero
        bytes. Still
        correctly surfaces as ``DataCorruptError`` one layer down: zero bytes
        is obviously not a SQLite file either."""
        compressed = zstandard.ZstdCompressor().compress(b"x" * 10_000)
        with pytest.raises(DataCorruptError, match="not a SQLite file"):
            await open_service_db(compressed[:8])

    async def test_raises_on_mid_frame_truncation_of_a_real_multi_block_db(self) -> None:
        """Unlike the severely-truncated case above, truncating a
        *larger*, multi-block real payload partway through produces
        real, SQLite-magic-matching output: the early pages survive,
        later ones don't. ``open_service_db`` itself doesn't raise for
        this (it only materializes the bytes and opens a connection,
        the same as it would for a real, intact file — SQLite itself is
        lazy about page validation), exactly like every real caller in
        this package that goes on to actually query the result. Caught
        one layer down instead, the moment a real query touches the
        malformed pages: real SQLite's own internal consistency check
        raises."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "big.db"
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE item_table(id INTEGER PRIMARY KEY, payload TEXT)")
            conn.executemany(
                "INSERT INTO item_table (payload) VALUES (?)",
                ((f"row-{i}-" + "x" * 200,) for i in range(20_000)),
            )
            conn.commit()
            conn.close()
            raw = path.read_bytes()
        compressed = zstandard.ZstdCompressor().compress(raw)
        truncated = compressed[: len(compressed) // 2]
        async with await open_service_db(truncated) as source:
            with pytest.raises(sqlite3.DatabaseError):
                await source.connection.execute("SELECT COUNT(*) FROM item_table")

    async def test_raises_data_corrupt_when_decompressed_payload_is_not_sqlite(self) -> None:
        compressed = zstandard.ZstdCompressor().compress(b"just some text, not a database")
        with pytest.raises(DataCorruptError, match="not a SQLite file"):
            await open_service_db(compressed)
