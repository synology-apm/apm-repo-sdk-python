"""Round-trips every ``tests/support/format_builders.py`` encoder through the SDK's
own parser, so the shared write-side layout and the reader agree. One
hand-checked example each; ``tests/unit/sdk/test_format_*_properties.py``
round-trip them over generated fields."""

from __future__ import annotations

import sqlite3
import zlib
from pathlib import Path

import zstandard

from support.format_builders import (
    COMPOSITION_SUB_FILE_SIZE,
    SIZE_STORE_REGION_LEN,
    bucket_header_bytes,
    chunk_addr_int,
    chunk_crc_store_bytes,
    chunk_map_record_bytes,
    composition_header_bytes,
    encode_size_store,
    inf_header,
    mapping_record,
    pack_object_db,
    record_head_bytes,
    redundancy_blob_bytes,
    repo_info_bytes,
    repo_transaction_bytes,
    zero_record,
    zstd_frame_without_content_size,
)
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import parse_bucket_header, parse_chunk_crc_store, parse_size_store
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind, parse_chunk_map_record
from synology_apm_repo.sdk.format.composition import parse_composition_header, parse_record_head
from synology_apm_repo.sdk.format.const import COMPRESS_RESERVED_LENG, SUB_FILE_SIZE
from synology_apm_repo.sdk.format.headers import MAGIC, parse_index_header
from synology_apm_repo.sdk.format.redundancy import attempt_repair, parse_redundancy_blob
from synology_apm_repo.sdk.format.repo_info import parse_repo_info
from synology_apm_repo.sdk.format.repo_transaction import parse_repo_transaction


def test_composition_sub_file_size_matches_the_sdk() -> None:
    assert COMPOSITION_SUB_FILE_SIZE == SUB_FILE_SIZE


def test_size_store_region_len_matches_the_sdk() -> None:
    # The SizeStore region fills the compressed bucket's reserved prefix after the 64-byte header.
    assert SIZE_STORE_REGION_LEN == COMPRESS_RESERVED_LENG - 64


def test_chunk_addr_int_round_trips() -> None:
    addr = ChunkAddress.from_int(chunk_addr_int(3, 0x12_3456_789A, 0xBEEF))
    assert (addr.stream_id, addr.bucket_id, addr.chunk_idx) == (3, 0x12_3456_789A, 0xBEEF)


def test_size_store_round_trips() -> None:
    entries = [(1, 4000), (0, 1234), (2, 0), (1, 4095)]  # sizes are 12-bit
    parsed = parse_size_store(encode_size_store(entries), len(entries))
    assert [(entry.compress_type.value, entry.stored_len) for entry in parsed] == entries


def test_bucket_header_round_trips() -> None:
    data = bucket_header_bytes(mode=0x83, chunk_num=8192, chunk_size_crc=0x01020304, crc_of_chunk_crc=0xA0B0C0D0)
    # The one field off a 4-byte boundary: crcOfChunkCrc at [29, 33).
    assert data[29:33] == bytes.fromhex("a0b0c0d0")
    header = parse_bucket_header(data)
    assert (header.major, header.minor, header.mode, header.chunk_num) == (3, 0, 0x83, 8192)
    assert (header.chunk_size_crc, header.crc_of_chunk_crc) == (0x01020304, 0xA0B0C0D0)


def test_chunk_crc_store_round_trips() -> None:
    trailer = chunk_crc_store_bytes([1, 0xFFFFFFFF])
    assert trailer == bytes.fromhex("00000001ffffffff")
    assert parse_chunk_crc_store(trailer, 2, verify_crc=zlib.crc32(trailer)) == (1, 0xFFFFFFFF)


def test_redundancy_blob_round_trips() -> None:
    data = bytes(range(1, 6))  # coverage 2: windows [1, 2] [3, 4] [5]
    raw = redundancy_blob_bytes(data, coverage=2)
    assert raw[:16] == b"RD" + bytes.fromhex("0000000000020000000000000005")
    assert raw[-4:] == bytes([1 ^ 5, 2, 3, 4])  # even windows XOR into [0, 2), odd into [2, 4)
    blob = parse_redundancy_blob(raw, data_size=5, coverage=2)
    assert blob.step_crc == (zlib.crc32(data[:2]), zlib.crc32(data[:4]), zlib.crc32(data))
    assert attempt_repair(b"\x01\x02\x03\x04\x00", raw, coverage=2, expected_crc=zlib.crc32(data)) == data


def test_chunk_map_record_round_trips() -> None:
    record = chunk_map_record_bytes(
        kind_value=ChunkMapKind.ZERO.value, file_chunk_idx=7, addr_int=0, tail_u32=3, inherit=True
    )
    assert record[0] == 0x11
    entry = parse_chunk_map_record(record)
    assert (entry.kind, entry.is_inherit) == (ChunkMapKind.ZERO, True)
    assert entry.file_offset == 7 << 12


def test_mapping_record_round_trips() -> None:
    entry = parse_chunk_map_record(mapping_record(8192, 4, 5, map_num=3, repeat=2))
    assert (entry.kind, entry.file_offset) == (ChunkMapKind.MAPPING, 8192)
    assert entry.addr == ChunkAddress.from_int(chunk_addr_int(0, 4, 5))
    assert (entry.map_num, entry.repeat) == (3, 2)


def test_zero_record_round_trips() -> None:
    entry = parse_chunk_map_record(zero_record(4096, zero_num=6))
    assert (entry.kind, entry.file_offset, entry.map_num) == (ChunkMapKind.ZERO, 4096, 6)


def test_inf_header_round_trips() -> None:
    header = parse_index_header(inf_header(), expect_magic=MAGIC["bucket_meta"])
    assert (header.major, header.minor) == (0, 0)


def test_pack_object_db_rows_point_at_their_payloads(tmp_path: Path) -> None:
    content, object_db_len = pack_object_db([("a", b"alpha"), ("b", b"be")])
    (tmp_path / "object.db").write_bytes(content[:object_db_len])
    conn = sqlite3.connect(tmp_path / "object.db")
    rows = conn.execute("SELECT object_id, offset, length FROM object_table ORDER BY object_id").fetchall()
    conn.close()
    assert [content[off : off + ln] for _, off, ln in rows] == [b"alpha", b"be"]


def test_record_head_round_trips() -> None:
    head = parse_record_head(
        record_head_bytes(map_num=5, status=1, map_crc=0xABCD, mode=0x0003, attr_leng=9, attr_crc=0x1234)
    )
    assert (head.status.value, head.map_num, head.map_crc, head.mode, head.attr_leng, head.attr_crc) == (
        1,
        5,
        0xABCD,
        0x0003,
        9,
        0x1234,
    )


def test_composition_header_round_trips() -> None:
    header = parse_composition_header(composition_header_bytes(major=1, minor=2))
    assert (header.major, header.minor) == (1, 2)


def test_repo_info_round_trips() -> None:
    info = parse_repo_info(repo_info_bytes({"repo_type": 2, "repo_flag": 0}, uuid=b"abcdefghijklmnop", minor=3))
    assert info.uuid == "abcdefghijklmnop"
    assert (info.major, info.minor) == (2, 3)
    assert info.repo_type == 2
    assert info.repo_flag == 0


def test_repo_transaction_round_trips() -> None:
    data = repo_transaction_bytes({"transaction_id": 100, "session_id": "6"})
    assert (data[:4], data[20:36]) == (b"rPTs", bytes(16))
    txn = parse_repo_transaction(data)
    assert (txn.transaction_id, txn.session_id, txn.compact_id) == (100, 6, None)


def test_zstd_frame_without_content_size_declares_none() -> None:
    frame = zstd_frame_without_content_size(b"abc" * 100)
    assert zstandard.get_frame_parameters(frame).content_size == zstandard.CONTENTSIZE_UNKNOWN
    assert zstandard.ZstdDecompressor().decompressobj().decompress(frame) == b"abc" * 100
