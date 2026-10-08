"""Write-side encoders for on-disk structures FORMAT-SPEC.md describes, for
tests that build a synthetic repository byte by byte. The SDK only reads, so
these are the one write-side statement of each layout; they import nothing
from the SDK, so a misread layout cannot hide behind a shared constant.
``tests/unit/support/test_format_builders.py`` round-trips each one through the
SDK's parser."""

from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import zlib
from collections.abc import Mapping, Sequence
from pathlib import Path

import zstandard

#: FORMAT-SPEC.md: Composition sub-file size (16 MiB).
COMPOSITION_SUB_FILE_SIZE = 16 << 20

#: FORMAT-SPEC.md §4: the bucket SizeStore region's fixed on-disk length.
SIZE_STORE_REGION_LEN = 16320

#: FORMAT-SPEC.md: ChunkMapRecord ``Type`` values.
CHUNK_MAP_KIND_MAPPING = 0
CHUNK_MAP_KIND_ZERO = 1


def _crc32(data: bytes | bytearray) -> bytes:
    return (zlib.crc32(bytes(data)) & 0xFFFFFFFF).to_bytes(4, "big")


def _seal_header(header: bytearray) -> bytes:
    """A 64-byte header shell with its header CRC32 written at offset 60."""
    header[60:64] = _crc32(header[:60])
    return bytes(header)


def encode_size_store(entries: list[tuple[int, int]]) -> bytes:
    """A bucket's SizeStore: one 15-bit ``(compress type << 12) | size``
    record per ``(type_value, size)`` entry, packed big-endian."""
    tight_len = (len(entries) * 15 + 7) >> 3
    buf = bytearray(tight_len + 4)  # slack for the last record's 4-byte OR window
    for idx, (type_value, size) in enumerate(entries):
        bit_off = idx * 15
        byte_off = bit_off >> 3
        bit_shift = 17 - (bit_off & 7)
        blob = (type_value << 12) | size
        window = int.from_bytes(buf[byte_off : byte_off + 4], "big")
        window |= (blob << bit_shift) & 0xFFFFFFFF
        buf[byte_off : byte_off + 4] = window.to_bytes(4, "big")
    return bytes(buf[:tight_len])


def sizestore_region_pad(tight: bytes) -> bytes:
    """``tight`` zero-padded to the bucket's fixed SizeStore region length."""
    return tight + b"\x00" * (SIZE_STORE_REGION_LEN - len(tight))


def chunk_addr_int(stream_id: int, bucket_id: int, chunk_idx: int) -> int:
    """The raw ``uint64`` chunk address: stream id, then a 40-bit bucket id,
    then a 16-bit chunk index."""
    return (((stream_id << 40) | bucket_id) << 16) | chunk_idx


def bucket_header_bytes(
    *,
    mode: int,
    chunk_num: int,
    chunk_size_crc: int = 0,
    crc_of_chunk_crc: int = 0,
    major: int = 3,
    minor: int = 0,
) -> bytes:
    """A 64-byte ``.buk`` header: ``mode``, ``chunk_num`` and
    ``chunk_size_crc`` at offsets 8/12/16, ``crcOfChunkCrc`` at 29, with its
    trailing header CRC."""
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8:12] = mode.to_bytes(4, "big")
    header[12:16] = chunk_num.to_bytes(4, "big")
    header[16:20] = chunk_size_crc.to_bytes(4, "big")
    header[29:33] = crc_of_chunk_crc.to_bytes(4, "big")
    return _seal_header(header)


def chunk_crc_store_bytes(crcs: Sequence[int]) -> bytes:
    """A bucket's ChunkCrcStore trailer: one big-endian u32 per non-empty
    chunk."""
    return b"".join(crc.to_bytes(4, "big") for crc in crcs)


def redundancy_blob_bytes(data: bytes, *, coverage: int, version: int = 0) -> bytes:
    """A Redundancy blob protecting ``data``: ``"RD"``, u16 version, u32
    coverage, u64 data size, then the big-endian rolling CRC32 after each
    ``coverage``-byte window, then the parity: the XOR of the even windows
    in ``[0, coverage)`` and of the odd ones after it, each window aligned
    to its half's first byte, ``min(len(data), 2 * coverage)`` bytes in
    all."""
    step_crcs: list[int] = []
    parity = bytearray(min(len(data), 2 * coverage))
    running = 0
    for idx, start in enumerate(range(0, len(data), coverage)):
        window = data[start : start + coverage]
        running = zlib.crc32(window, running) & 0xFFFFFFFF
        step_crcs.append(running)
        base = (idx % 2) * coverage
        for offset, value in enumerate(window):
            parity[base + offset] ^= value
    head = b"RD" + version.to_bytes(2, "big") + coverage.to_bytes(4, "big") + len(data).to_bytes(8, "big")
    return head + b"".join(crc.to_bytes(4, "big") for crc in step_crcs) + bytes(parity)


def chunk_map_record_bytes(
    *, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int, inherit: bool = False
) -> bytes:
    """One 20-byte ChunkMapRecord: kind nibble and INHERIT bit (``0x10``),
    7-byte file chunk index, 8-byte chunk address, 4-byte map-num/repeat
    tail."""
    return (
        bytes([(kind_value & 0x0F) | (0x10 if inherit else 0)])
        + file_chunk_idx.to_bytes(7, "big")
        + addr_int.to_bytes(8, "big")
        + tail_u32.to_bytes(4, "big")
    )


def mapping_record(file_offset: int, bucket_id: int, chunk_idx: int, map_num: int, repeat: int = 0) -> bytes:
    """A MAPPING ChunkMapRecord at ``file_offset`` pointing at stream 0's
    ``(bucket_id, chunk_idx)`` for ``map_num`` chunks."""
    return chunk_map_record_bytes(
        kind_value=CHUNK_MAP_KIND_MAPPING,
        file_chunk_idx=file_offset >> 12,
        addr_int=chunk_addr_int(0, bucket_id, chunk_idx),
        tail_u32=(map_num << 16) | repeat,
    )


def zero_record(file_offset: int, zero_num: int) -> bytes:
    """A ZERO ChunkMapRecord at ``file_offset`` covering ``zero_num`` chunks."""
    return chunk_map_record_bytes(
        kind_value=CHUNK_MAP_KIND_ZERO, file_chunk_idx=file_offset >> 12, addr_int=0, tail_u32=zero_num
    )


def record_head_bytes(
    *, map_num: int, status: int = 0, map_crc: int = 0, mode: int = 0x0001, attr_leng: int = 0, attr_crc: int = 0
) -> bytes:
    """A 32-byte composition RecordHead, with its trailing CRC."""
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[2:4] = status.to_bytes(2, "big")
    head[6:14] = map_num.to_bytes(8, "big")
    head[14:18] = map_crc.to_bytes(4, "big")
    head[18:20] = mode.to_bytes(2, "big")
    head[20:24] = attr_leng.to_bytes(4, "big")
    head[24:28] = attr_crc.to_bytes(4, "big")
    head[28:32] = _crc32(head[:28])
    return bytes(head)


def composition_header_bytes(
    *, major: int = 1, minor: int = 1, sub_file_size: int = COMPOSITION_SUB_FILE_SIZE
) -> bytes:
    """A 64-byte composition sub-file header, with its trailing CRC."""
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8:12] = sub_file_size.to_bytes(4, "big")
    return _seal_header(header)


def inf_header() -> bytes:
    """A fingerprint-group ``.inf``'s 64-byte ``GMet`` header, version 0.0,
    with its trailing CRC."""
    header = bytearray(64)
    header[0:4] = b"GMet"
    return _seal_header(header)


def build_object_db(rows: list[tuple[str, int, int]]) -> bytes:
    """An ObjectDB sqlite file's bytes: one ``object_table`` row per
    ``(object_id, offset, length)``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE object_table(object_id TEXT PRIMARY KEY, offset INTEGER, length INTEGER)")
        conn.executemany("INSERT INTO object_table VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        return path.read_bytes()


def pack_object_db(payloads: Sequence[tuple[str, bytes]]) -> tuple[bytes, int]:
    """``payloads`` (``(object_id, data)``) back-to-back behind one ObjectDB
    whose offsets point past itself, and that ObjectDB's length."""
    relative_rows = []
    cursor = 0
    for object_id, data in payloads:
        relative_rows.append((object_id, cursor, len(data)))
        cursor += len(data)
    object_db_len = len(build_object_db(relative_rows))
    object_db = build_object_db([(oid, off + object_db_len, ln) for oid, off, ln in relative_rows])
    return object_db + b"".join(data for _, data in payloads), object_db_len


def _json_payload_file(
    magic: bytes,
    payload: Mapping[str, object] | bytes,
    *,
    major: int,
    minor: int,
    uuid: bytes,
    data_size: int | None,
    json_crc: int | None,
) -> bytes:
    """The 64-byte header (payload CRC at 8, payload length at 12, ``uuid``
    at 20) followed by the payload: JSON-encoded, or ``bytes`` verbatim.
    ``data_size``/``json_crc`` default to the payload's own."""
    body = payload if isinstance(payload, bytes) else json.dumps(dict(payload)).encode("utf-8")
    header = bytearray(64)
    header[0:4] = magic
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8:12] = _crc32(body) if json_crc is None else json_crc.to_bytes(4, "big")
    header[12:20] = (len(body) if data_size is None else data_size).to_bytes(8, "big")
    header[20:36] = uuid
    return _seal_header(header) + body


def repo_info_bytes(
    payload: Mapping[str, object] | bytes | None = None,
    *,
    uuid: bytes = b"a" * 16,
    version: int = 2,
    minor: int = 0,
    data_size: int | None = None,
    json_crc: int | None = None,
) -> bytes:
    """A ``repo_info`` file: the 64-byte header (payload CRC and length, the
    16-byte uuid, header CRC) followed by the payload, by default
    ``{"repo_type": 2}``; ``version`` is the major version."""
    return _json_payload_file(
        b"RpiF",
        {"repo_type": 2} if payload is None else payload,
        major=version,
        minor=minor,
        uuid=uuid,
        data_size=data_size,
        json_crc=json_crc,
    )


def repo_transaction_bytes(
    payload: Mapping[str, object] | bytes,
    *,
    major: int = 1,
    minor: int = 0,
    data_size: int | None = None,
    json_crc: int | None = None,
) -> bytes:
    """A ``repo_transaction.<N>`` file: ``repo_info``'s header shell with
    magic ``rPTs`` and bytes ``[20, 36)`` zero, followed by the payload."""
    return _json_payload_file(
        b"rPTs", payload, major=major, minor=minor, uuid=bytes(16), data_size=data_size, json_crc=json_crc
    )


def zstd_frame_without_content_size(data: bytes) -> bytes:
    """``data`` as one zstd frame whose header declares no content size."""
    buf = io.BytesIO()
    writer = zstandard.ZstdCompressor(write_content_size=False).stream_writer(buf, closefd=False)
    writer.write(data)
    writer.flush(zstandard.FLUSH_FRAME)
    return buf.getvalue()
