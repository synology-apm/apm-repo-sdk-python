"""Unit tests for ``synology_apm_repo.sdk.diagnostics`` at the SDK layer:
``_verify_map_crc``/``_walk_composition_records`` directly, plus one or two
dataclass-shape assertions per public function. The remaining branches are
covered through the CLI's rendering in ``tests/unit/cli/test_cli_commands_dump.py``.
"""

from __future__ import annotations

from pathlib import Path

from support.format_builders import (
    chunk_addr_int,
    composition_header_bytes,
)
from support.repo_builders import (
    composition_record_bytes,
    filler_bucket_bytes,
    uncompressed_bucket_bytes,
)
from support.store_fakes import CountingStore
from synology_apm_repo.sdk.diagnostics import (
    BucketInspection,
    ChunkMapInspection,
    CompositionWalk,
    _verify_map_crc,
    _walk_composition_records,
    inspect_bucket,
    inspect_chunk_map,
    walk_composition,
)
from synology_apm_repo.sdk.format.composition import parse_record_head
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import RECORD_HEAD_LENGTH
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _mapping_entry(*, file_chunk_idx: int, addr_int: int, map_num: int, repeat: int = 0) -> bytes:
    tail = (map_num << 16) | repeat
    return bytes([0x00]) + file_chunk_idx.to_bytes(7, "big") + addr_int.to_bytes(8, "big") + tail.to_bytes(4, "big")


def _zero_entry(*, file_chunk_idx: int, zero_num: int) -> bytes:
    return bytes([0x01]) + file_chunk_idx.to_bytes(7, "big") + (0).to_bytes(8, "big") + zero_num.to_bytes(4, "big")


def _build_map_array() -> bytes:
    return _mapping_entry(file_chunk_idx=0, addr_int=chunk_addr_int(5, 3, 0), map_num=1) + _zero_entry(
        file_chunk_idx=1, zero_num=1
    )


class TestVerifyMapCrc:
    async def test_matching_crc_returns_true(self, tmp_path: Path) -> None:
        record_bytes = composition_record_bytes(map_array=_build_map_array())
        (tmp_path / "c0").write_bytes(record_bytes)
        store = LocalFsStore(tmp_path)
        record = parse_record_head(record_bytes[:RECORD_HEAD_LENGTH])
        assert await _verify_map_crc(store, "c0", RECORD_HEAD_LENGTH, record) is True

    async def test_corrupted_map_bytes_returns_false(self, tmp_path: Path) -> None:
        record_bytes = bytearray(composition_record_bytes(map_array=_build_map_array()))
        # +5 lands in the first entry's file_chunk_idx field, past its kind
        # tag, so the entry still parses as a MAPPING.
        record_bytes[RECORD_HEAD_LENGTH + 5] ^= 0xFF
        (tmp_path / "c0").write_bytes(bytes(record_bytes))
        store = LocalFsStore(tmp_path)
        record = parse_record_head(bytes(record_bytes[:RECORD_HEAD_LENGTH]))
        assert await _verify_map_crc(store, "c0", RECORD_HEAD_LENGTH, record) is False


class TestWalkCompositionRecords:
    async def test_stops_at_limit_with_more_data_remaining(self, tmp_path: Path) -> None:
        record = composition_record_bytes(map_array=_build_map_array())
        path = tmp_path / "c0"
        path.write_bytes(record + record)
        store = LocalFsStore(tmp_path)
        records, next_offset, stop_exc = await _walk_composition_records(
            store, "c0", start=0, file_size=len(record) * 2, limit=1, verify_map=False
        )
        assert len(records) == 1
        assert next_offset == len(record)  # positioned right after the one record walked
        assert stop_exc is None

    async def test_stops_cleanly_at_file_size(self, tmp_path: Path) -> None:
        record = composition_record_bytes(map_array=_build_map_array())
        path = tmp_path / "c0"
        path.write_bytes(record)
        store = LocalFsStore(tmp_path)
        records, next_offset, stop_exc = await _walk_composition_records(
            store, "c0", start=0, file_size=len(record), limit=10, verify_map=False
        )
        assert len(records) == 1
        assert next_offset == len(record)
        assert stop_exc is None

    async def test_a_failed_record_after_a_good_one_stops_but_keeps_it(self, tmp_path: Path) -> None:
        record = composition_record_bytes(map_array=_build_map_array())
        garbage = b"\x00" * RECORD_HEAD_LENGTH
        path = tmp_path / "c0"
        path.write_bytes(record + garbage)
        store = LocalFsStore(tmp_path)
        records, next_offset, stop_exc = await _walk_composition_records(
            store, "c0", start=0, file_size=len(record) + len(garbage), limit=10, verify_map=False
        )
        assert len(records) == 1  # the one good record before the garbage is kept
        assert next_offset == len(record)  # positioned at the record that failed to parse
        assert stop_exc is not None


class TestInspectBucket:
    async def test_returns_the_expected_shape(self, tmp_path: Path) -> None:
        entries = [(CompressType.NONE, 0), (CompressType.ZSTD, 100), (CompressType.LZ4, 50)]
        (tmp_path / "0.buk").write_bytes(filler_bucket_bytes(entries))
        store = LocalFsStore(tmp_path)
        result = await inspect_bucket(store, "0.buk")
        assert isinstance(result, BucketInspection)
        assert result.chunk_num == 3
        assert result.mode_flags == ["compress", "chunk_crc"]
        assert result.size_check_ok is True
        assert result.chunk is None  # no ``chunk`` argument

    async def test_an_uncompressed_bucket_has_no_size_self_check(self, tmp_path: Path) -> None:
        (tmp_path / "0.buk").write_bytes(uncompressed_bucket_bytes(4))
        result = await inspect_bucket(LocalFsStore(tmp_path), "0.buk")
        assert (result.chunk_num, result.mode_flags) == (4, [])
        assert (result.expected_size, result.size_check_ok) == (None, None)


class TestWalkComposition:
    async def test_returns_the_expected_shape(self, tmp_path: Path) -> None:
        record = composition_record_bytes(map_array=_build_map_array())
        (tmp_path / "c0").write_bytes(composition_header_bytes() + record)
        store = LocalFsStore(tmp_path)
        result = await walk_composition(store, "c0")
        assert isinstance(result, CompositionWalk)
        assert result.header == (1, 1)
        assert len(result.records) == 1
        assert result.records[0].map_num == 2
        assert result.stopped_with_error is None


class TestInspectChunkMap:
    async def test_returns_the_expected_shape(self, tmp_path: Path) -> None:
        record = composition_record_bytes(map_array=_build_map_array())
        header = composition_header_bytes()
        (tmp_path / "c0").write_bytes(header + record)
        store = LocalFsStore(tmp_path)
        result = await inspect_chunk_map(store, "c0", len(header), verify=True)
        assert isinstance(result, ChunkMapInspection)
        assert result.map_num == 2
        assert result.map_crc_ok is True
        assert result.entries[0].kind == "MAPPING"
        assert result.entries[0].addr is not None
        assert (
            result.entries[0].addr.stream_id,
            result.entries[0].addr.bucket_id,
            result.entries[0].addr.chunk_idx,
        ) == (
            5,
            3,
            0,
        )
        assert result.entries[1].kind == "ZERO"
        assert result.entries[1].addr is None

    async def test_limit_zero_skips_the_batched_read_entirely(self, tmp_path: Path) -> None:
        # shown == 0 must not reach store.read() with a zero-length request.
        record = composition_record_bytes(map_array=_build_map_array())
        header = composition_header_bytes()
        (tmp_path / "c0").write_bytes(header + record)
        store = CountingStore(LocalFsStore(tmp_path))
        result = await inspect_chunk_map(store, "c0", len(header), limit=0)
        assert result.map_num == 2
        assert result.entries == []
        assert store.read_count == 1  # the record head only
