"""Unit tests for ``synology_apm_repo.sdk.diagnostics`` — its own contract
at the SDK layer, not just exercised indirectly via
``tests/unit/cli/test_cli_dump.py``'s JSON/text rendering. Stays narrow:
``_verify_map_crc``/``_walk_composition_records`` directly, plus one or two
dataclass-shape assertions per public function, not a second full pass
over every branch the CLI-level test already covers. Same synthetic
byte-construction approach (and duplicated builder helpers, per
``tests/CLAUDE.md``) as that file.
"""

from __future__ import annotations

import os
import struct
import zlib
from pathlib import Path

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
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.composition import parse_record_head
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import RECORD_HEAD_LENGTH, SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _encode_size_store(entries: list[tuple[int, int]]) -> bytes:
    n = len(entries)
    tight_len = (n * 15 + 7) >> 3
    buf = bytearray(tight_len + 4)
    for idx, (type_value, size) in enumerate(entries):
        bit_off = idx * 15
        byte_off = bit_off >> 3
        bit_shift = 17 - (bit_off & 7)
        blob = (type_value << 12) | size
        window = int.from_bytes(buf[byte_off : byte_off + 4], "big")
        window |= (blob << bit_shift) & 0xFFFFFFFF
        buf[byte_off : byte_off + 4] = window.to_bytes(4, "big")
    return bytes(buf[:tight_len])


def _build_bucket(entries: list[tuple[CompressType, int]]) -> bytes:
    chunk_num = len(entries)
    tight = _encode_size_store([(e[0].value, e[1]) for e in entries])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", chunk_num)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    chunk_data = b""
    for ctype, stored_len in entries:
        eff = 4096 if ctype is CompressType.NONE else stored_len
        chunk_data += os.urandom(eff)
    trailer_len = 4 * chunk_num + redundancy_size((chunk_num * 15 + 7) >> 3, 256)
    trailer = os.urandom(trailer_len)
    return bytes(header) + sizestore_region + chunk_data + trailer


def _build_composition_header() -> bytes:
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", SUB_FILE_SIZE)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _addr_int(stream_id: int, bucket_id: int, chunk_idx: int) -> int:
    return ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx)).to_int()


def _mapping_entry(*, file_chunk_idx: int, addr_int: int, map_num: int, repeat: int = 0) -> bytes:
    tail = (map_num << 16) | repeat
    return bytes([0x00]) + file_chunk_idx.to_bytes(7, "big") + addr_int.to_bytes(8, "big") + tail.to_bytes(4, "big")


def _zero_entry(*, file_chunk_idx: int, zero_num: int) -> bytes:
    return bytes([0x01]) + file_chunk_idx.to_bytes(7, "big") + (0).to_bytes(8, "big") + zero_num.to_bytes(4, "big")


def _build_map_array() -> bytes:
    return _mapping_entry(file_chunk_idx=0, addr_int=_addr_int(5, 3, 0), map_num=1) + _zero_entry(
        file_chunk_idx=1, zero_num=1
    )


def _build_record(*, map_array: bytes, attr: bytes = b"") -> bytes:
    map_num = len(map_array) // 20
    map_crc = zlib.crc32(map_array) & 0xFFFFFFFF
    attr_crc = zlib.crc32(attr) & 0xFFFFFFFF
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = map_num.to_bytes(8, "big")
    head[14:18] = map_crc.to_bytes(4, "big")
    head[18:20] = (0x0001).to_bytes(2, "big")  # Redundancy bit set
    head[20:24] = len(attr).to_bytes(4, "big")
    head[24:28] = attr_crc.to_bytes(4, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    trailer_len = redundancy_size(len(map_array), 8192)
    trailer = os.urandom(trailer_len)
    return bytes(head) + map_array + attr + trailer


class TestVerifyMapCrc:
    async def test_matching_crc_returns_true(self, tmp_path: Path) -> None:
        record_bytes = _build_record(map_array=_build_map_array())
        (tmp_path / "c0").write_bytes(record_bytes)
        store = LocalFsStore(tmp_path)
        record = parse_record_head(record_bytes[:RECORD_HEAD_LENGTH])
        assert await _verify_map_crc(store, "c0", RECORD_HEAD_LENGTH, record) is True

    async def test_corrupted_map_bytes_returns_false(self, tmp_path: Path) -> None:
        record_bytes = bytearray(_build_record(map_array=_build_map_array()))
        # +5 lands inside the first entry's file_chunk_idx field, not its
        # leading kind-tag byte -- same corruption offset
        # test_cli_dump.py's own composition_bad_map_crc_path fixture
        # uses, for the same "still parses as a MAPPING entry, just wrong
        # bytes" reason.
        record_bytes[RECORD_HEAD_LENGTH + 5] ^= 0xFF
        (tmp_path / "c0").write_bytes(bytes(record_bytes))
        store = LocalFsStore(tmp_path)
        record = parse_record_head(bytes(record_bytes[:RECORD_HEAD_LENGTH]))
        assert await _verify_map_crc(store, "c0", RECORD_HEAD_LENGTH, record) is False


class TestWalkCompositionRecords:
    async def test_stops_at_limit_with_more_data_remaining(self, tmp_path: Path) -> None:
        record = _build_record(map_array=_build_map_array())
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
        record = _build_record(map_array=_build_map_array())
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
        record = _build_record(map_array=_build_map_array())
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
        (tmp_path / "0.buk").write_bytes(_build_bucket(entries))
        store = LocalFsStore(tmp_path)
        result = await inspect_bucket(store, "0.buk")
        assert isinstance(result, BucketInspection)
        assert result.chunk_num == 3
        assert result.mode_flags == ["compress", "chunk_crc"]
        assert result.size_check_ok is True
        assert result.chunk is None  # no --chunk given


class TestWalkComposition:
    async def test_returns_the_expected_shape(self, tmp_path: Path) -> None:
        record = _build_record(map_array=_build_map_array())
        (tmp_path / "c0").write_bytes(_build_composition_header() + record)
        store = LocalFsStore(tmp_path)
        result = await walk_composition(store, "c0")
        assert isinstance(result, CompositionWalk)
        assert result.header == (1, 1)
        assert len(result.records) == 1
        assert result.records[0].map_num == 2
        assert result.stopped_with_error is None


class TestInspectChunkMap:
    async def test_returns_the_expected_shape(self, tmp_path: Path) -> None:
        record = _build_record(map_array=_build_map_array())
        header = _build_composition_header()
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
        record = _build_record(map_array=_build_map_array())
        header = _build_composition_header()
        (tmp_path / "c0").write_bytes(header + record)
        store = LocalFsStore(tmp_path)
        result = await inspect_chunk_map(store, "c0", len(header), limit=0)
        assert result.map_num == 2
        assert result.entries == []
