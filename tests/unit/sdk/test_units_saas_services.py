"""Unit tests for ``synology_apm_repo.sdk.units.saas.services``: synthetic
bytes for ``sniff()`` and ``open_service_db()``, ``saas_fakes.FakeDedupFile``
for ``inspect_object()``'s read pattern
(``tests/integration/sdk/test_units_saas_stream_objectdb_discovery.py`` is
the real-data counterpart). ``sniff()`` only guesses which service owns an
already-located ``SERVICE_DB`` blob from its table names."""

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
    inspect_object,
    open_service_db,
    sniff,
)
from unit.sdk.saas_fakes import FakeDedupFile


def _build_service_db(table_name: str, *, padding_blob: bytes = b"") -> bytes:
    """A real, valid SQLite file with a table name ``_SERVICE_TABLE_HINTS``
    recognizes, ZSTD-compressed the way a real service-level DB snapshot is
    stored. ``padding_blob``, when given, is stored as an extra
    ``config_table`` row's BLOB value, inflating size without adding bytes
    outside the one real zstd frame."""
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
        """``sniff()``'s speculative ``peel()`` (up to
        ``_MAX_SNIFF_DECOMPRESS`` = 128 MiB) runs off the event loop."""
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

    @pytest.mark.parametrize(
        ("data", "expected"),
        [
            pytest.param(
                b'{"content_list": [], "values": {"Attachments": false}}', ServiceKind.META_JSON, id="generic_meta_json"
            ),
            # Real META objects are prefixed with "\n" before "{".
            pytest.param(b'\n{"fields": []}', ServiceKind.META_JSON, id="meta_json_tolerates_a_leading_newline"),
            pytest.param(
                b"{not valid json at all",
                ServiceKind.BINARY,
                id="invalid_json_starting_with_brace_falls_through_to_binary",
            ),
            pytest.param(
                b'{"a": ' + b"[" * 100_000 + b"]" * 100_000 + b"}",
                ServiceKind.BINARY,
                id="json_nested_past_the_recursion_limit_falls_through_to_binary",
            ),
            pytest.param(
                b"Received: from mail.example.com\r\nFrom: a@example.com\r\nMIME-Version: 1.0\r\n\r\nbody",
                ServiceKind.MAIL_SKELETON,
                id="an_rfc822_skeleton",
            ),
            pytest.param(b"\xff\xd8\xff\xe1\x00\x18Exif\x00\x00", ServiceKind.BINARY, id="binary_content"),
            pytest.param(b"", ServiceKind.BINARY, id="empty_bytes_is_binary"),
        ],
    )
    async def test_sniff_classifies(self, data: bytes, expected: ServiceKind) -> None:
        result = await sniff(data)
        assert result.kind is expected

    async def test_zstd_payload_that_is_not_sqlite_is_binary(self) -> None:
        compressed = zstandard.ZstdCompressor().compress(b"just some text, not a database")
        result = await sniff(compressed)
        assert result.kind is ServiceKind.BINARY

    @pytest.mark.parametrize(
        "keep",
        [
            pytest.param(8, id="truncated_zstd_frame"),
            # Below the 7-byte minimum zstandard.get_frame_parameters()
            # needs to parse a frame header at all, so it raises ZstdError
            # (propagated by zstd_content_size()); sniff() still says BINARY.
            pytest.param(5, id="truncated_before_the_frame_header_itself_parses"),
        ],
    )
    async def test_truncated_zstd_is_binary_not_an_exception(self, keep: int) -> None:
        compressed = zstandard.ZstdCompressor().compress(b"x" * 10_000)
        result = await sniff(compressed[:keep])
        assert result.kind is ServiceKind.BINARY

    async def test_content_over_the_sniff_decompress_cap_is_treated_as_not_zstd_framed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A zstd frame declaring a decompressed size over
        ``_MAX_SNIFF_DECOMPRESS`` is ``BINARY`` — not decompressed, never
        raised on. The cap is patched down to avoid a 128 MiB payload."""
        import synology_apm_repo.sdk.units.saas.services as services_module

        monkeypatch.setattr(services_module, "_MAX_SNIFF_DECOMPRESS", 1024)
        blob = _build_service_db("item_table", padding_blob=os.urandom(4096))
        result = await sniff(blob)
        assert result.kind is ServiceKind.BINARY


class TestInspectObject:
    async def test_reads_the_full_object_when_under_the_cap(self) -> None:
        blob = b'{"content_list": []}'
        fake = FakeDedupFile(blob)
        result = await inspect_object(cast(DedupFile, fake), 0, len(blob))
        assert result.kind is ServiceKind.META_JSON
        assert fake.read_calls == [(0, len(blob))]

    async def test_reads_only_a_head_when_over_the_cap_and_not_zstd_magic(self) -> None:
        big = b"\x00" * (10 << 20)  # 10 MiB, over the 8 MiB cap, no zstd magic
        fake = FakeDedupFile(big)
        result = await inspect_object(cast(DedupFile, fake), 0, len(big))
        assert result.kind is ServiceKind.BINARY
        assert fake.read_calls == [(0, 4096)]  # head only, not the full 10 MiB

    async def test_reads_the_full_object_when_over_the_cap_but_zstd_magic_matches(self) -> None:
        """Over the cap, a head read checks the zstd magic first; only a
        match costs the second, full read."""
        big = _build_service_db("item_table", padding_blob=os.urandom(9 << 20))
        assert len(big) > (8 << 20), "test invariant: large enough to trip the 8 MiB cap"
        fake = FakeDedupFile(big)
        result = await inspect_object(cast(DedupFile, fake), 0, len(big))
        assert result.kind is ServiceKind.SERVICE_DB
        assert result.tables == {"item_table", "config_table"}
        assert fake.read_calls == [(0, 4096), (0, len(big))]  # head check, then the real full read


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
        """Truncated right after the frame header: decompresses to zero
        bytes instead of the declared 10,000, caught by the declared-size
        check before ``open_service_db``'s SQLite-magic check."""
        compressed = zstandard.ZstdCompressor().compress(b"x" * 10_000)
        with pytest.raises(DataCorruptError, match=r"declared a decompressed size of 10000.*actually produced 0"):
            await open_service_db(compressed[:8])

    async def test_raises_on_mid_frame_truncation_of_a_real_multi_block_db(self) -> None:
        """Truncated partway through, this decompresses to SQLite-magic
        output whose later pages are missing; the declared-size check
        raises at open, not at the first query touching those pages."""
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
        with pytest.raises(DataCorruptError, match=r"declared a decompressed size of .*actually produced"):
            await open_service_db(truncated)

    async def test_raises_data_corrupt_when_decompressed_payload_is_not_sqlite(self) -> None:
        compressed = zstandard.ZstdCompressor().compress(b"just some text, not a database")
        with pytest.raises(DataCorruptError, match="not a SQLite file"):
            await open_service_db(compressed)


def test_a_db_matching_two_service_hints_is_named_by_its_first_table_in_sorted_order() -> None:
    import synology_apm_repo.sdk.units.saas.services as services_module

    # A Teams DB can hold both a chat and a channel table.
    tables = frozenset({"msg_info_table", "channel_info_table"})
    assert services_module._service_name_for(tables) == "teams_channel"
