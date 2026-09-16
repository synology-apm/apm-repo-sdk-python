"""Unit tests for ``synology_apm_repo.sdk.units.verify_reachable`` —
synthetic repository roots written to real files, no sample repositories
required. Most fixtures here are FS workloads (the simplest end-to-end
``composition_extents_for_version`` path — one ``dedup.img`` per version,
no per-fragment/per-item complexity); ``TestVmExtents``/``TestPcpsExtents``/
``TestSaasExtents`` cover the other three workload types'
``composition_extents_for_version`` branches specifically."""

from __future__ import annotations

import asyncio
import atexit
import hashlib
import json
import os
import sqlite3
import struct
import zlib
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from synology_apm_repo.sdk import concurrency
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version, versions
from synology_apm_repo.sdk.catalog.workload import Workload, workload_by_id
from synology_apm_repo.sdk.dedup.chunk_walk import ChunkPlan
from synology_apm_repo.sdk.dedup.pool import BucketReader, BucketReaderCache, Pool
from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import Finding, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError, PermissionDeniedError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS, MODE_VAULT_ENCRYPT
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import REDUNDANCY_COVERAGE_COMPOSITION, SUB_FILE_SIZE
from synology_apm_repo.sdk.format.crypto import chunk_iv
from synology_apm_repo.sdk.format.redundancy import REDUNDANCY_MAGIC, redundancy_size
from synology_apm_repo.sdk.format.repo_info import MAGIC as REPO_INFO_MAGIC
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId, WorkloadId
from synology_apm_repo.sdk.presentation.progress import Progress
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units import verify_reachable as verify_reachable_module
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.device import DeviceProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.verify_reachable import (
    CompositionExtent,
    _all_children,
    _annotate_with_saas_resolution,
    _verify_bucket_worker,
    _verify_worker_init,
    _verify_worker_shutdown,
    verify_reachable,
)

_SIZE_STORE_REGION_LEN = 16320  # COMPRESS_RESERVED_LENG(16384) - HEADER_LEN(64)
_ALLOC_TABLE_OFFSET = 12288

# The composition/session's own addressing -- distinct from the Pool's own
# bucket addressing below, which every test in this file leaves at stream 0
# (matching every other synthetic fixture in this suite's own
# "@data/Pool/0/..." buckets).
_STREAM_ID = 8
_SESSION_ID = 3
_COMP_OFFSET = 64  # right after the composition sub-file's own 64-byte cMpS header


# -- byte-level builders (mirrors test_catalog_version.py's own) --


def _write_repo_info(path: Path) -> None:
    payload = json.dumps({"repo_type": 2}).encode("utf-8")
    header = bytearray(64)
    header[0:4] = REPO_INFO_MAGIC
    header[4:6] = (2).to_bytes(2, "big")
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[20:36] = b"abcdefghijklmnop"
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + payload)


def _db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(path)


def _write_connection_config(path: Path, rows: list[tuple[int, str, int]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, "
        "connection_id TEXT, version_type INTEGER)"
    )
    conn.executemany("INSERT INTO connection_config VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_workload_config(path: Path, rows: list[tuple[int, str, str, dict[str, object]]]) -> None:
    """Appends (``CREATE TABLE IF NOT EXISTS``) rather than always
    creating fresh — ``_write_fs_workload`` calls this once per workload
    it registers, and a test building more than one workload in the same
    repository needs every call after the first to add to the same table."""
    conn = _db(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS workload_config(workload_id INTEGER PRIMARY KEY, workload_uid TEXT, "
        "workload_type TEXT, workload_spec TEXT)"
    )
    conn.executemany(
        "INSERT INTO workload_config VALUES (?, ?, ?, ?)",
        [(wid, uid, wtype, json.dumps(spec)) for wid, uid, wtype, spec in rows],
    )
    conn.commit()
    conn.close()


def _version_spec_json(start_time: int) -> str:
    return json.dumps({"status": {"start_time": str(start_time), "status": "COMPLETED"}})


def _write_copy_target_version(path: Path, rows: list[tuple[object, ...]]) -> None:
    """Appends — see ``_write_workload_config``'s own docstring."""
    conn = _db(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS copy_target_version(version_id INTEGER PRIMARY KEY, workload_id INTEGER, "
        "connection_config_id INTEGER, version_uid TEXT, target_type TEXT, target_id TEXT, "
        "saas_stream_uuid TEXT, saas_snapshot_uuid TEXT, saas_version_id INTEGER, deleted INTEGER, "
        "version_spec TEXT)"
    )
    conn.executemany("INSERT INTO copy_target_version VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_copy_target_version_meta(path: Path, rows: list[tuple[str, str, list[str], int]]) -> None:
    """Appends — see ``_write_workload_config``'s own docstring."""
    conn = _db(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS copy_target_version_meta(version_uid TEXT PRIMARY KEY, target_meta_path TEXT, "
        "meta_filenames TEXT, status INTEGER)"
    )
    conn.executemany(
        "INSERT INTO copy_target_version_meta VALUES (?, ?, ?, ?)",
        [(uid, path_, json.dumps(names), status) for uid, path_, names, status in rows],
    )
    conn.commit()
    conn.close()


def _write_file_meta_sizes(path: Path, rows: list[tuple[str, int]]) -> None:
    """``db/file_meta``'s ``file_size`` column — without a matching row
    here, ``DedupRepo.locate_file`` resolves an unknown ``file_size``
    (``None``), which every ``composition_extents_for_version`` branch
    that checks ``dedup_file.size is None`` treats as "nothing to check
    at all" rather than raising, silently emptying every extent this
    file's own tests build."""
    conn = _db(path)
    conn.execute("CREATE TABLE IF NOT EXISTS file_meta(path TEXT PRIMARY KEY, file_size INTEGER)")
    conn.executemany("INSERT INTO file_meta VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _write_file_map(path: Path, rows: list[tuple[str, int, int, int, int, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE file_map(path TEXT PRIMARY KEY, crtime DATETIME, mtime DATETIME, "
        "stream_id INTEGER, session_id INTEGER, comp_offset INTEGER, block INTEGER, status INTEGER)"
    )
    conn.executemany(
        "INSERT INTO file_map(path, stream_id, session_id, comp_offset, block, status) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _write_target_db_with_version_id(path: Path, version_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_table(id INTEGER PRIMARY KEY, version_id INTEGER, data_format INTEGER, "
        "status INTEGER, folder_name TEXT)"
    )
    conn.execute("INSERT INTO version_table VALUES (1, ?, 1, 1, 'folder')", (version_id,))
    conn.commit()
    conn.close()


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    return (
        bytes([type_byte])
        + file_chunk_idx.to_bytes(7, "big")
        + addr_int.to_bytes(8, "big")
        + tail_u32.to_bytes(4, "big")
    )


def _mapping_record(file_offset: int, bucket_id: int, chunk_idx: int, map_num: int) -> bytes:
    addr_int = (bucket_id << 16) | chunk_idx  # Pool stream_id=0 implicitly, matching this file's own buckets
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value,
        file_chunk_idx=file_offset >> 12,
        addr_int=addr_int,
        tail_u32=map_num << 16,
    )


def _composition_header_bytes() -> bytes:
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = SUB_FILE_SIZE.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _record_head_bytes(*, map_num: int, map_crc: int) -> bytes:
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = map_num.to_bytes(8, "big")
    head[14:18] = map_crc.to_bytes(4, "big")
    head[18:20] = (1).to_bytes(2, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(head)


def _write_composition(
    root: Path,
    *,
    stream_id: int,
    session_id: int,
    entries: bytes,
    corrupt_header: bool = False,
    corrupt_record_head: bool = False,
    on_disk_entries: bytes | None = None,
    trailer: bytes = b"",
) -> None:
    """``on_disk_entries`` (default: ``entries`` itself) lets the bytes
    actually written to disk differ from the bytes ``map_crc`` is computed
    over, and ``trailer`` is appended right after them -- together express a
    record whose on-disk chunk-map array is corrupted but parity-recoverable
    via a real Redundancy blob appended after it. Every existing caller
    leaves both at their defaults and gets today's exact behavior
    unchanged."""
    map_num = len(entries) // 20
    head = bytearray(_record_head_bytes(map_num=map_num, map_crc=zlib.crc32(entries) & 0xFFFFFFFF))
    if corrupt_record_head:
        head[0] ^= 0xFF  # bad "Mu" magic
    comp_header = bytearray(_composition_header_bytes())
    if corrupt_header:
        comp_header[0] ^= 0xFF  # bad "cMpS" magic
    path = root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    on_disk = on_disk_entries if on_disk_entries is not None else entries
    path.write_bytes(bytes(comp_header) + bytes(head) + on_disk + trailer)


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


def _write_bucket(path: Path, plaintexts: list[bytes], *, corrupt_chunk_crc_idx: int | None = None) -> None:
    """A real, self-consistent, unencrypted ZSTD-compressed ``.buk`` file
    (Pool stream 0, bucket 0) with a real ChunkCrcStore trailer, so
    ``check_bucket_structure``'s self-consistency check and
    ``check_chunk_ciphertext_crc``'s per-chunk check both pass against a
    genuinely intact bucket.

    ``corrupt_chunk_crc_idx``, when given, flips that one chunk's own
    recorded ChunkCrcStore entry *before* the trailer's own
    ``crcOfChunkCrc`` is computed — a wrong-but-internally-self-consistent
    trailer, so the mismatch is only caught by an actual per-chunk
    ciphertext check, never by the bucket's own structural check, and the
    chunk's real stored bytes (and so its decode/fingerprint) stay intact.
    """
    payloads = [zstandard.ZstdCompressor().compress(p) for p in plaintexts]
    entries = [(CompressType.ZSTD.value, len(p)) for p in payloads]
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF

    chunk_crcs = [zlib.crc32(p) & 0xFFFFFFFF for p in payloads]
    if corrupt_chunk_crc_idx is not None:
        chunk_crcs[corrupt_chunk_crc_idx] ^= 0xFFFFFFFF
    chunk_crc_store = b"".join(crc.to_bytes(4, "big") for crc in chunk_crcs)
    crc_of_chunk_crc = zlib.crc32(chunk_crc_store) & 0xFFFFFFFF

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[29:33] = struct.pack(">I", crc_of_chunk_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    sizestore_region = tight + b"\x00" * (_SIZE_STORE_REGION_LEN - len(tight))
    trailer = chunk_crc_store + os.urandom(redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(payloads) + trailer)


def _write_bucket_with_compacted_slot(path: Path, plaintexts: list[bytes], *, compacted_idx: int) -> None:
    """Like ``_write_bucket``, but ``compacted_idx`` is stored as
    ``CompressType.COMPACTED`` (contributing no chunk data, no
    ChunkCrcStore entry of its own) instead of a real ZSTD chunk -- for a
    test proving FULL's own per-bucket check never reaches a compacted
    slot, only every other, genuinely live one."""
    payloads: list[bytes | None] = []
    entries: list[tuple[int, int]] = []
    for idx, plain in enumerate(plaintexts):
        if idx == compacted_idx:
            payloads.append(None)
            entries.append((CompressType.COMPACTED.value, 0))
        else:
            compressed = zstandard.ZstdCompressor().compress(plain)
            payloads.append(compressed)
            entries.append((CompressType.ZSTD.value, len(compressed)))
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF

    chunk_crcs = [zlib.crc32(p) & 0xFFFFFFFF for p in payloads if p is not None]
    chunk_crc_store = b"".join(crc.to_bytes(4, "big") for crc in chunk_crcs)
    crc_of_chunk_crc = zlib.crc32(chunk_crc_store) & 0xFFFFFFFF

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[29:33] = struct.pack(">I", crc_of_chunk_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    sizestore_region = tight + b"\x00" * (_SIZE_STORE_REGION_LEN - len(tight))
    chunk_data = b"".join(p for p in payloads if p is not None)
    trailer = chunk_crc_store + os.urandom(redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + chunk_data + trailer)


def _write_compacted_bucket(path: Path) -> None:
    """A minimal, valid, fully-``COMPACTED`` single-chunk bucket: opens
    and passes both the header/SizeStore CRC and
    ``check_bucket_structure``'s own size/trailer self-consistency check,
    but ``non_compacted_chunk_indices()`` returns ``[]`` so no per-chunk
    content check ever runs against it -- for a test that only cares
    about ``check_bucket_structure``'s own outcome per bucket."""
    tight = _encode_size_store([(CompressType.COMPACTED.value, 0)])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", 1)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (_SIZE_STORE_REGION_LEN - len(tight))
    trailer = os.urandom(redundancy_size((1 * 15 + 7) >> 3, 256))  # 0 non-empty chunks -> no ChunkCrcStore bytes
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + trailer)


def _inf_header() -> bytes:
    header = bytearray(64)
    header[0:4] = b"GMet"
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _write_inf_and_fgp(pool_root: Path, plaintexts: list[bytes], *, wrong_digest_idx: int | None = None) -> None:
    """One ``.inf``/``.fgp`` pair recording every chunk's real SHA-256
    fingerprint for bucket 0 — so FULL's own fingerprint check
    (``Pool.verify_fingerprints``) succeeds for every chunk these tests
    touch, leaving only a deliberately corrupted
    ChunkCrcStore entry (``_write_bucket``'s own ``corrupt_chunk_crc_idx``)
    to surface as a finding, rather than an incidental "no fingerprint
    data at all" finding drowning it out.

    ``wrong_digest_idx``, when given, stores a deliberately wrong digest
    for that one chunk instead — for a test of the fingerprint check
    itself rather than the ciphertext CRC one."""
    buf = bytearray(_ALLOC_TABLE_OFFSET + 1024 * 8)
    buf[0:64] = _inf_header()
    raw_pos = len(plaintexts)  # byte_off=0, rec_num=len(plaintexts)
    buf[_ALLOC_TABLE_OFFSET : _ALLOC_TABLE_OFFSET + 4] = raw_pos.to_bytes(4, "big")
    inf_path = pool_root / "0" / "0.inf"
    inf_path.parent.mkdir(parents=True, exist_ok=True)
    inf_path.write_bytes(bytes(buf))
    digests = [hashlib.sha256(p).digest() for p in plaintexts]
    if wrong_digest_idx is not None:
        digests[wrong_digest_idx] = hashlib.sha256(b"wrong").digest()
    (pool_root / "0" / "0_0.fgp").write_bytes(b"".join(digests))


def _write_legacy_bucket(path: Path, plaintexts: list[bytes]) -> None:
    """The pre-ABP layout (no ``MODE_COMPRESS``, no SizeStore at all) —
    every chunk implicitly ``CompressType.NONE`` at a fixed 4096-byte
    stride starting at ``RESERVED_LENG`` (4096). Exercises
    ``_check_bucket_group``'s early-return for a bucket
    ``check_bucket_structure``/the per-chunk checks below both skip
    entirely, since neither formula applies to this layout."""
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", 0)  # mode: no MODE_COMPRESS
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + b"\x00" * (4096 - 64) + b"".join(plaintexts))


def _write_encrypted_bucket(
    path: Path, plaintexts: list[bytes], vault_key: bytes, *, corrupt_chunk_crc_idx: int | None = None
) -> None:
    """A real vault-encrypted bucket (Pool stream 0, bucket 0) — for a
    repository opened *without* a vault key, so ``_check_one_bucket``'s
    own ``key_missing`` branch is the only thing that ever inspects it.

    ``corrupt_chunk_crc_idx``, when given, flips that one chunk's own
    recorded ChunkCrcStore entry *before* the trailer's own
    ``crcOfChunkCrc`` is computed — same shape as ``_write_bucket``'s own
    parameter, for a test proving ciphertext CRC stays exhaustive (not a
    QUICK-sized sample) even with no key to decrypt with."""
    ciphertexts: list[bytes] = []
    for chunk_idx, plain in enumerate(plaintexts):
        compressed = zstandard.ZstdCompressor().compress(plain)
        addr = ChunkAddress(StreamId(0), BucketId(0), ChunkIdx(chunk_idx))
        encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(chunk_iv(addr))).encryptor()
        ciphertexts.append(encryptor.update(compressed) + encryptor.finalize())
    entries = [(CompressType.ZSTD.value, len(ct)) for ct in ciphertexts]
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    chunk_crcs = [zlib.crc32(ct) & 0xFFFFFFFF for ct in ciphertexts]
    if corrupt_chunk_crc_idx is not None:
        chunk_crcs[corrupt_chunk_crc_idx] ^= 0xFFFFFFFF
    chunk_crc_store = b"".join(crc.to_bytes(4, "big") for crc in chunk_crcs)
    crc_of_chunk_crc = zlib.crc32(chunk_crc_store) & 0xFFFFFFFF

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC | MODE_VAULT_ENCRYPT)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[29:33] = struct.pack(">I", crc_of_chunk_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    sizestore_region = tight + b"\x00" * (_SIZE_STORE_REGION_LEN - len(tight))
    trailer = chunk_crc_store + os.urandom(redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(ciphertexts) + trailer)


def _write_fs_workload(
    tmp_path: Path,
    *,
    workload_id: int,
    version_uid: str,
    target_id: str,
    meta_dirname: str,
    dedup_version_id: int,
    dedup_img_size: int | None = None,
    write_target_db: bool = True,
) -> str:
    """Register one resolvable FS workload+version in
    ``workload_config``/``copy_target_version``/``copy_target_version_meta``
    plus its own ``copy_meta_file/<meta_dirname>/target.db`` — everything
    ``composition_extents_for_version``'s FS branch needs short of the
    ``file_map`` row itself (a caller adds that separately, so two
    versions built this way can share one row's target composition/bucket).
    Returns the ``dedup.img`` path this version's ``file_map`` row must use.

    ``dedup_img_size``, when given, also registers ``db/file_meta``'s own
    ``file_size`` for that path — required for
    ``composition_extents_for_version``'s FS branch to return a non-empty
    extent at all (see ``_write_file_meta_sizes``'s own docstring).

    ``write_target_db=False`` leaves this version's own catalog rows
    claiming a resolvable target.db that's never actually written — for a
    version that should resolve per its own metadata but doesn't.
    """
    fs_spec: dict[str, object] = {"namespace": "ns-a", "spec": {"workload_type": "FS", "workload_name": target_id}}
    _write_workload_config(tmp_path / "db" / "workload_config", [(workload_id, f"{target_id}-uid", "FS", fs_spec)])
    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                workload_id * 100,
                workload_id,
                1,
                version_uid,
                "FS",
                target_id,
                "",
                "",
                0,
                0,
                _version_spec_json(1786000000),
            )
        ],
    )
    _write_copy_target_version_meta(
        tmp_path / "db" / "copy_target_version_meta",
        [(version_uid, f"/pv/copy_meta_file/{meta_dirname}", ["target.db", "version.db.zst"], 1)],
    )
    if write_target_db:
        _write_target_db_with_version_id(tmp_path / "copy_meta_file" / meta_dirname / "target.db", dedup_version_id)
        # fs_meta_available()/copy_meta_dir_exists() (verify_reachable's own
        # resolvability pre-filter, matching Repository.versions()'s) need
        # the copy_meta_file/<meta_dirname> directory to actually show up
        # in a listing -- target.db above already guarantees that in
        # practice, but a dedicated marker file makes the dependency
        # explicit rather than incidental.
        (tmp_path / "copy_meta_file" / meta_dirname / "version.db.zst").touch()
    dedup_img_path = f"{target_id}/{dedup_version_id}/dedup.img"
    if dedup_img_size is not None:
        _write_file_meta_sizes(tmp_path / "db" / "file_meta", [(dedup_img_path, dedup_img_size)])
    return dedup_img_path


def _write_vm_target_db(
    path: Path,
    *,
    config_device_id: int,
    disk_name: str,
    src_file_path: str,
    extra_objects: list[tuple[int, int, str, str, int, int | None]] | None = None,
) -> None:
    """One VM device with one dedup-object disk, mirroring
    ``test_units_device.py``'s own ``_write_target_db`` -- the minimum
    ``DeviceProvider._object_nodes()`` needs to resolve one real disk
    (``data_format=1`` matches ``_DATA_FORMAT_DEDUP``, so this object is
    never flagged ``unsupported``).

    ``extra_objects``: ``(object_id, data_format, file_path, src_file_path,
    dedup_object, file_size)`` rows appended alongside the main disk --
    for a sidecar/unresolvable-size object ``_vm_extents`` must skip
    rather than raise on.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_table(id INTEGER PRIMARY KEY, version_id INTEGER, data_format INTEGER, "
        "status INTEGER, folder_name TEXT)"
    )
    conn.execute("INSERT INTO version_table VALUES (1, 1, 1, 1, 'folder')")
    conn.execute(
        "CREATE TABLE device_table(device_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "device_uuid TEXT, host_name TEXT, os_name TEXT)"
    )
    conn.execute("INSERT INTO device_table VALUES (1, 1, ?, 'device-uuid', 'my-vm', 'Windows')", (config_device_id,))
    conn.execute(
        "CREATE TABLE object_table(object_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "data_format INTEGER, file_path TEXT, src_file_path TEXT, temp_postfix TEXT, dedup_object INTEGER, "
        "file_size INTEGER)"
    )
    conn.execute(
        "INSERT INTO object_table VALUES (1, 1, ?, 1, ?, ?, '', 1, 4096)",
        (config_device_id, disk_name, src_file_path),
    )
    for object_id, data_format, file_path, extra_src_path, dedup_object, file_size in extra_objects or []:
        conn.execute(
            "INSERT INTO object_table VALUES (?, 1, ?, ?, ?, ?, '', ?, ?)",
            (object_id, config_device_id, data_format, file_path, extra_src_path, dedup_object, file_size),
        )
    conn.commit()
    conn.close()


def _write_vm_workload(
    tmp_path: Path,
    *,
    workload_id: int,
    version_uid: str,
    target_id: str,
    meta_dirname: str,
    disk_name: str,
    src_file_path: str,
    extra_objects: list[tuple[int, int, str, str, int, int | None]] | None = None,
) -> None:
    """Register one resolvable VM workload+version -- the ``target_type``
    counterpart to ``_write_fs_workload``, but resolved via
    ``DeviceProvider`` (``device_table``/``object_table``) rather than a
    single shared ``dedup.img``. Returns nothing: unlike FS's single
    ``dedup.img`` path (needed by its own ``db/file_map`` row), a VM
    disk's ``file_map`` key is ``src_file_path``, already a caller-chosen
    parameter here."""
    vm_spec: dict[str, object] = {"namespace": "ns-a", "spec": {"workload_type": "VM", "workload_name": target_id}}
    _write_workload_config(tmp_path / "db" / "workload_config", [(workload_id, f"{target_id}-uid", "VM", vm_spec)])
    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                workload_id * 100,
                workload_id,
                1,
                version_uid,
                "VM",
                target_id,
                "",
                "",
                0,
                0,
                _version_spec_json(1786000000),
            )
        ],
    )
    _write_copy_target_version_meta(
        tmp_path / "db" / "copy_target_version_meta",
        [(version_uid, f"/pv/copy_meta_file/{meta_dirname}", ["target.db"], 1)],
    )
    _write_vm_target_db(
        tmp_path / "copy_meta_file" / meta_dirname / "target.db",
        config_device_id=1,
        disk_name=disk_name,
        src_file_path=src_file_path,
        extra_objects=extra_objects,
    )


def _write_copy_target_file(path: Path, rows: list[tuple[int, int]]) -> None:
    """``(version_id, fid)`` rows appended to the *same* physical file as
    ``copy_target_version`` -- the real on-disk shape (see
    ``test_units_device.py``'s own ``_write_copy_target_version_and_file``
    docstring for why these two tables share one file)."""
    conn = _db(path)
    conn.execute("CREATE TABLE IF NOT EXISTS copy_target_file(version_id INTEGER, fid INTEGER)")
    conn.executemany("INSERT INTO copy_target_file VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _write_pcps_file_meta(path: Path, rows: list[tuple[int, str, int]]) -> None:
    """``rows``: ``(fid, path, file_size)`` -- PC/PS's own ``file_meta``
    schema (keyed by ``fid``, not ``path``), distinct from FS's
    ``_write_file_meta_sizes`` above; never used in the same ``tmp_path``
    as that one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE file_meta(fid INTEGER PRIMARY KEY, path TEXT, file_size INTEGER)")
    conn.executemany("INSERT INTO file_meta VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_pcps_workload(
    tmp_path: Path,
    *,
    workload_id: int,
    version_uid: str,
    target_id: str,
    fid: int,
    src_file_path: str,
    disk_size: int,
) -> None:
    """Register one resolvable PC/PS workload+version with one disk
    fragment -- no ``target.db``/meta directory at all, matching
    ``test_units_device.py``'s own
    ``test_pcps_lists_disks_via_copy_target_file_chain``: PC/PS resolves
    entirely off ``copy_target_version``/``copy_target_file`` (one
    physical file) joined with ``db/file_meta``."""
    pcps_spec: dict[str, object] = {"namespace": "ns-a", "spec": {"workload_type": "PC", "workload_name": target_id}}
    _write_workload_config(tmp_path / "db" / "workload_config", [(workload_id, f"{target_id}-uid", "PC", pcps_spec)])
    version_id = workload_id * 100
    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                version_id,
                workload_id,
                1,
                version_uid,
                "PC",
                target_id,
                "",
                "",
                0,
                0,
                _version_spec_json(1786000000),
            )
        ],
    )
    _write_copy_target_file(tmp_path / "db" / "copy_target_version", [(version_id, fid)])
    _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(fid, src_file_path, disk_size)])


def _write_saas_snapshot_db(
    path: Path, snapshots: list[tuple[int, str, int, int]], distribution: list[tuple[int, int, int, int]]
) -> None:
    """``snapshots``: (snapshot_id, snapshot_uuid, first_version_id, stream_version).
    ``distribution``: (offset, length, snapshot_id, version_id)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE snapshot_info(snapshot_id INTEGER PRIMARY KEY, snapshot_uuid TEXT, "
        "first_version_id INTEGER, stream_version INTEGER)"
    )
    conn.executemany("INSERT INTO snapshot_info VALUES (?, ?, ?, ?)", snapshots)
    conn.execute(
        "CREATE TABLE snapshot_distribution(offset INTEGER, length INTEGER, snapshot_id INTEGER, version_id INTEGER)"
    )
    conn.executemany("INSERT INTO snapshot_distribution VALUES (?, ?, ?, ?)", distribution)
    conn.commit()
    conn.close()


def _write_saas_version_db(
    path: Path,
    versions: list[tuple[int, int, int, int]],
    target_type: str,
    *,
    latest_complete_version: int | None = None,
) -> None:
    """``versions``: (snapshot_id, version_id, stream_version, deleted).
    ``latest_complete_version`` defaults to ``max(stream_version)`` across
    ``versions`` when not given explicitly."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_info(snapshot_id INTEGER, version_id INTEGER, stream_version INTEGER, deleted INTEGER)"
    )
    conn.executemany(
        "INSERT INTO version_info(snapshot_id, version_id, stream_version, deleted) VALUES (?, ?, ?, ?)", versions
    )
    conn.execute("CREATE TABLE stream_info(id INTEGER PRIMARY KEY, target_type TEXT, latest_complete_version INTEGER)")
    resolved_latest = (
        latest_complete_version if latest_complete_version is not None else max((v[2] for v in versions), default=None)
    )
    conn.execute(
        "INSERT INTO stream_info(id, target_type, latest_complete_version) VALUES (1, ?, ?)",
        (target_type, resolved_latest),
    )
    conn.commit()
    conn.close()


def _write_saas_workload(
    tmp_path: Path,
    *,
    workload_id: int,
    version_uid: str,
    stream_uuid: str,
    connection_config_id: int,
    saas_obj_size: int,
) -> str:
    """Register one resolvable M365 SaaS workload+version -- mirrors
    ``test_units_saas_stream.py``'s own ``_build_saas_repo``/``_version``,
    plus the catalog-level ``workload_config``/``copy_target_version`` row
    ``composition_extents_for_version``'s dispatch (and
    ``verify_reachable``'s own ``catalog.versions()`` walk) needs on top
    of ``SaasStream``'s own db layout.

    ``connection_config_id`` must already have a real
    ``db/connection_config`` row -- ``_open()``'s own vault-wide one
    (``connection_config_id=1``, ``connection_id="conn-a"``) already
    provides this, so no caller of this function needs to write a second,
    conflicting copy of that table. Returns the ``saas_obj``'s own
    ``file_map``/``file_meta`` path -- ``open_saas_obj``'s "Copy" lookup
    tries ``connection_id`` (``"conn-a"``) as the path's middle segment
    first, so that's the one real callers/this fixture's own ``file_map``
    row must agree on, not the numeric ``connection_config_id``.
    """
    saas_spec: dict[str, object] = {
        "namespace": "ns-a",
        "spec": {"workload_type": "M365", "workload_name": stream_uuid},
    }
    _write_workload_config(
        tmp_path / "db" / "workload_config", [(workload_id, f"{stream_uuid}-uid", "M365", saas_spec)]
    )
    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                workload_id * 100,
                workload_id,
                connection_config_id,
                version_uid,
                "M365",
                stream_uuid,
                stream_uuid,
                "snap-uuid-1",
                3,
                0,
                _version_spec_json(1786000000),
            )
        ],
    )
    stream_db_dir = tmp_path / "saas" / str(connection_config_id) / stream_uuid / "db"
    _write_saas_snapshot_db(
        stream_db_dir / "saas_snapshot",
        snapshots=[(1, "snap-uuid-1", 3, 1)],
        distribution=[(0, saas_obj_size, 1, 3)],
    )
    _write_saas_version_db(stream_db_dir / "saas_version", versions=[(1, 3, 1, 0)], target_type="M365")
    return f"{stream_uuid}/conn-a/1/saas_obj"


def _write_saas_workload_multi_generation(
    tmp_path: Path,
    *,
    workload_id: int,
    stream_uuid: str,
    connection_config_id: int,
    live_stream_version: int,
    total_versions: int,
    latest_complete_version: int,
) -> str:
    """Like ``_write_saas_workload`` but registers ``total_versions``
    distinct catalog versions (``version_id``/``saas_version_id`` 1..
    ``total_versions``) in one snapshot, each mapped via ``version_info``
    to its own equal-numbered ``stream_version`` -- the real shape
    ``cleanupSaasFile()`` leaves behind once older generations are GC'd
    (only ``live_stream_version`` still has a resolvable ``saas_obj``;
    workload-format/saas-obj.md §11.4.6/§3, §7.1's progressive-superset
    property). Returns the live generation's own ``saas_obj`` path --
    caller writes ``file_map``/``file_meta``/composition/bucket for just
    that one, same contract as ``_write_saas_workload``."""
    saas_spec: dict[str, object] = {
        "namespace": "ns-a",
        "spec": {"workload_type": "M365", "workload_name": stream_uuid},
    }
    _write_workload_config(
        tmp_path / "db" / "workload_config", [(workload_id, f"{stream_uuid}-uid", "M365", saas_spec)]
    )
    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                workload_id * 100 + n,
                workload_id,
                connection_config_id,
                f"vuid-saas-gen{n}",
                "M365",
                stream_uuid,
                stream_uuid,
                "snap-uuid-1",
                n,
                0,
                _version_spec_json(1786000000 + n),
            )
            for n in range(1, total_versions + 1)
        ],
    )
    stream_db_dir = tmp_path / "saas" / str(connection_config_id) / stream_uuid / "db"
    _write_saas_snapshot_db(
        stream_db_dir / "saas_snapshot",
        snapshots=[(1, "snap-uuid-1", 1, 1)],
        distribution=[(0, 4096, 1, n) for n in range(1, total_versions + 1)],
    )
    _write_saas_version_db(
        stream_db_dir / "saas_version",
        versions=[(1, n, n, 0) for n in range(1, total_versions + 1)],
        target_type="M365",
        latest_complete_version=latest_complete_version,
    )
    return f"{stream_uuid}/conn-a/{live_stream_version}/saas_obj"


async def _open(tmp_path: Path) -> DedupRepo:
    _write_repo_info(tmp_path / "repo_info")
    _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


# -- tests ----------------------------------------------------------------


class TestOrphanedFileMapRow:
    """The actual regression this whole redesign exists for: a
    ``file_map`` row nothing in the catalog resolves to must never be
    visited/flagged by the top-down walk, unlike the old bottom-up scan's
    blind row-by-row sweep."""

    async def test_orphaned_row_is_never_visited(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-healthy",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                # An orphaned row: no version_table/target.db anywhere
                # points at this path, no matter what stream/session/
                # comp_offset it names. If the old bottom-up scan ran, its
                # missing composition subfile would surface as a
                # Stage.FILE_MAP DATA_MISSING finding; the top-down walk
                # must never even look at it.
                ("orphan/999/dedup.img", 99, 99, 0, 1, 2),
            ],
        )
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []
        assert not any("orphan" in f.path or "99" in f.path for f in findings)

    async def test_the_legitimate_row_is_still_actually_checked(self, tmp_path: Path) -> None:
        """Positive control for the test above: an empty ``findings``
        list there must mean "checked and clean," not "silently never
        checked at all" — corrupting the *legitimate* row's own chunk
        must still surface a finding despite the orphaned row alongside
        it."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-healthy",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                ("orphan/999/dedup.img", 99, 99, 0, 1, 2),
            ],
        )
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)


class TestUnresolvableVersion:
    """A version whose own catalog rows claim it should resolve to real
    content but doesn't. Every catalog-listed, non-deleted version is now
    attempted directly — no shallow pre-filter decides in advance whether
    to bother — and the failure is classified by exception type: a
    missing-file failure (``NotFoundError``) becomes a ``Symptom.DATA_MISSING``
    Finding worded to name the likely cause (a stale/rotated version
    reference), while genuine corruption
    (``DataCorruptError``/``FormatError``) surfaces distinctly as
    ``Symptom.CORRUPTION``, with no such wording."""

    async def test_missing_target_db_is_a_version_stage_finding(self, tmp_path: Path) -> None:
        """A version whose ``target.db`` was never written is invisible in
        every browsing path already, but verify now reports it directly
        rather than silently converging with that."""
        _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-broken",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            write_target_db=False,  # claims a target.db that's never actually written
        )
        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.VERSION
        assert findings[0].symptom is Symptom.DATA_MISSING
        assert "stale/rotated" in findings[0].detail

    async def test_missing_file_map_row_is_also_data_missing(self, tmp_path: Path) -> None:
        """A different ``NotFoundError``-shaped failure deeper in resolution
        (``target.db`` itself opens and resolves fine, but the
        ``dedup.img`` it points to has no ``db/file_map`` row at all)
        reaches the same classification — it's the exception type, not
        which specific check happened to fail, that decides the symptom
        now that there's no separate shallow-vs-deep distinction."""
        _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-broken",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            # write_target_db defaults to True -- target.db itself resolves fine.
        )
        # No db/file_map row for fsA/1/dedup.img -- FsProvider._dedup_img()
        # only discovers this once it actually tries to locate the file.
        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.VERSION
        assert findings[0].symptom is Symptom.DATA_MISSING

    async def test_corruption_during_resolution_is_symptom_corruption_not_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A resolution failure that *isn't* ``NotFoundError`` (a corrupt
        on-disk structure, not merely an absent one) classifies distinctly
        — no "stale/rotated" wording, since this isn't the known, benign
        retention-gap case ``NotFoundError`` covers."""
        _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )

        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        async def fake_fs_extents(repo: DedupRepo, version: Version) -> list[CompositionExtent]:
            raise DataCorruptError("simulated corrupt target.db", spec="test")

        monkeypatch.setattr(verify_reachable_module, "_fs_extents", fake_fs_extents)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.VERSION
        assert findings[0].symptom is Symptom.CORRUPTION
        assert "stale/rotated" not in findings[0].detail


class TestCatalogEnumerationFailure:
    """A ``connections()``/``workloads()``/``versions()`` call itself
    failing (a corrupt or unreadable ``connection_config``/
    ``workload_config``/``copy_target_version`` row or table) must be
    folded into a ``Finding`` the same as every other resolution failure
    in this module, not left to propagate out of ``verify_reachable()``
    entirely -- see ``Stage.VERSION``'s own docstring ("covers failure ...
    at the workload/connection-enumeration level")."""

    async def test_connections_failure_is_a_version_stage_finding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        async def fake_connections(repo: DedupRepo) -> list[Connection]:
            raise DataCorruptError("simulated corrupt connection_config", spec="test")

        monkeypatch.setattr(verify_reachable_module, "connections", fake_connections)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.VERSION and f.symptom is Symptom.CORRUPTION for f in findings)

    async def test_one_connections_workloads_failure_does_not_abort_its_siblings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """connection-a's own ``workloads()`` call raises; connection-b, a
        sibling in the same listing, must still be walked -- proving the
        loop continues past the failure rather than the whole function
        raising and abandoning every connection after it."""
        conn_a = Connection(
            connection_config_id=1,  # type: ignore[arg-type]
            connection_id="conn-a",  # type: ignore[arg-type]
            display_name="a",
            namespaces=(),
            workload_count=0,
            version_count=0,
        )
        conn_b = Connection(
            connection_config_id=2,  # type: ignore[arg-type]
            connection_id="conn-b",  # type: ignore[arg-type]
            display_name="b",
            namespaces=(),
            workload_count=0,
            version_count=0,
        )
        seen_connections: list[Connection] = []

        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        async def fake_connections(repo: DedupRepo) -> list[Connection]:
            return [conn_a, conn_b]

        async def fake_workloads(repo: DedupRepo, connection: Connection) -> list[Workload]:
            seen_connections.append(connection)
            if connection is conn_a:
                raise NotFoundError("simulated missing workload_config", ref="workload_config")
            return []

        monkeypatch.setattr(verify_reachable_module, "connections", fake_connections)
        monkeypatch.setattr(verify_reachable_module, "workloads", fake_workloads)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert seen_connections == [conn_a, conn_b]  # conn_b was still reached after conn_a's failure
        assert any(f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING and f.path == "a" for f in findings)

    async def test_one_workloads_versions_failure_does_not_abort_its_siblings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same shape one level deeper: workload-1's own ``versions()``
        call raises; workload-2, a sibling under the same connection, must
        still be walked."""
        conn_a = Connection(
            connection_config_id=1,  # type: ignore[arg-type]
            connection_id="conn-a",  # type: ignore[arg-type]
            display_name="a",
            namespaces=(),
            workload_count=0,
            version_count=0,
        )
        wl_1 = Workload(
            workload_id=1,  # type: ignore[arg-type]
            workload_uid="wl-1",  # type: ignore[arg-type]
            workload_type="VM",
            sub_type=None,
            display_name="workload-1",
            subtitle=None,
            spec={},
        )
        wl_2 = Workload(
            workload_id=2,  # type: ignore[arg-type]
            workload_uid="wl-2",  # type: ignore[arg-type]
            workload_type="VM",
            sub_type=None,
            display_name="workload-2",
            subtitle=None,
            spec={},
        )
        seen_workloads: list[Workload] = []

        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        async def fake_connections(repo: DedupRepo) -> list[Connection]:
            return [conn_a]

        async def fake_workloads(repo: DedupRepo, connection: Connection) -> list[Workload]:
            return [wl_1, wl_2]

        async def fake_versions(repo: DedupRepo, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
            seen_workloads.append(workload)
            if workload is wl_1:
                raise NotFoundError("simulated missing copy_target_version", ref="copy_target_version")
            return []

        monkeypatch.setattr(verify_reachable_module, "connections", fake_connections)
        monkeypatch.setattr(verify_reachable_module, "workloads", fake_workloads)
        monkeypatch.setattr(verify_reachable_module, "versions", fake_versions)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert seen_workloads == [wl_1, wl_2]  # wl_2 was still reached after wl_1's failure
        assert any(
            f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING and f.path == "workload-1" for f in findings
        )


class TestFullVsQuickChunkCoverage:
    """FULL checks every live chunk in a touched bucket exhaustively; QUICK
    never reads chunk content at all, so it never finds a chunk-level
    corruption regardless of which chunk it's in."""

    def _build(self, tmp_path: Path, *, chunk_num: int, corrupt_chunk_crc_idx: int) -> None:
        plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(chunk_num)]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=chunk_num),
        )
        _write_bucket(
            tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=corrupt_chunk_crc_idx
        )
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

    async def test_quick_never_finds_bucket_content_corruption(self, tmp_path: Path) -> None:
        chunk_num = 3
        self._build(tmp_path, chunk_num=chunk_num, corrupt_chunk_crc_idx=0)
        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert findings == []

    async def test_full_still_finds_the_same_corruption(self, tmp_path: Path) -> None:
        chunk_num = 3
        self._build(tmp_path, chunk_num=chunk_num, corrupt_chunk_crc_idx=chunk_num - 1)
        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)


class TestMemoization:
    """A bucket/chunk two versions both reference (internal dedup) is
    checked only once, not once per referencing version."""

    async def test_shared_bucket_corruption_is_reported_only_once(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_a = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        dedup_img_b = _write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-b",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        # Two distinct file_map rows (two distinct backup versions), both
        # resolving to the exact same composition record -- the real
        # shape internal dedup produces when two versions share content.
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
            ],
        )
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        bucket_findings = [f for f in findings if f.stage is Stage.BUCKET]
        assert len(bucket_findings) == 1

    async def test_plan_chunks_windowed_is_not_rewalked_for_a_shared_record(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Distinct from the bucket-level dedup above: two versions sharing
        one composition record (and so the exact same ``(record_key, start,
        end)`` window) must call ``plan_chunks_windowed`` only once between
        them, not once per version -- proving ``_extent_bucket_keys`` skips
        the whole chunk-map walk on a hit, not just reusing the record's own
        page cache underneath it."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_a = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        dedup_img_b = _write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-b",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
            ],
        )
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        from synology_apm_repo.sdk.dedup.chunk_walk import DEFAULT_WINDOW_ENTRIES
        from synology_apm_repo.sdk.dedup.chunk_walk import plan_chunks_windowed as real_plan_chunks_windowed
        from synology_apm_repo.sdk.dedup.dedup_file import DedupFile

        call_count = 0

        async def counting_plan_chunks_windowed(
            base: DedupFile,
            start: int,
            end: int,
            window_start: int,
            *,
            write_zero_fill: Callable[[int, int], Awaitable[None]] | None,
            max_entries: int = DEFAULT_WINDOW_ENTRIES,
        ) -> AsyncIterator[ChunkPlan]:
            nonlocal call_count
            call_count += 1
            async for plan in real_plan_chunks_windowed(
                base, start, end, window_start, write_zero_fill=write_zero_fill, max_entries=max_entries
            ):
                yield plan

        monkeypatch.setattr(verify_reachable_module, "plan_chunks_windowed", counting_plan_chunks_windowed)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        assert findings == []
        assert call_count == 1


class TestFullFingerprintMismatch:
    """FULL's exhaustive per-chunk check includes the plaintext fingerprint,
    not just the ciphertext CRC — a wrong stored digest surfaces even
    though the chunk's own ciphertext is perfectly intact."""

    async def test_wrong_fingerprint_is_a_finding(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts, wrong_digest_idx=0)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.symptom is Symptom.MISMATCH and "fingerprint" in f.detail for f in findings)


class TestCompositionStageFindings:
    """A corrupt composition sub-file header or ``RecordHead`` surfaces as
    its own ``Finding``, exactly like the bottom-up scanner's identical
    checks — proving ``_check_extent`` actually appends what
    ``check_composition_header``/``check_record_head`` return, not just
    calls them for effect."""

    async def test_corrupt_composition_header_is_a_finding(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
            corrupt_header=True,
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.COMPOSITION and f.symptom is Symptom.CORRUPTION for f in findings)

    async def test_corrupt_record_head_is_caught_as_a_finding_not_a_crash(self, tmp_path: Path) -> None:
        """``check_record_head`` (called first, inside ``_check_extent``)
        turns this corruption into a ``Finding`` — ``_check_extent`` must
        then skip ``plan_chunks_windowed(extent.dedup_file, ...)``
        entirely for this extent rather than letting it independently
        re-read/re-parse the very same corrupt ``RecordHead`` through
        ``DedupFile``'s own reader and raise uncaught, which would crash
        this whole ``verify_reachable()`` run instead of continuing on to
        the next extent/version — contradicting this module's own stated
        goal (see its docstring's "Considered and rejected" section) of
        surviving one bad record and still reporting everything else."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
            corrupt_record_head=True,
        )

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.FILE_MAP and f.symptom is Symptom.CORRUPTION for f in findings)

    async def test_corrupt_record_head_is_only_reported_once_across_a_shared_record(self, tmp_path: Path) -> None:
        """The same broken ``RecordHead`` reached by two versions sharing
        one composition record must skip ``plan_chunks_windowed`` on
        *both* visits, not just the first — proving the "already broken"
        outcome is itself memoized across ``_checked_records`` cache hits,
        not just the original ``Finding``."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path_1 = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        dedup_img_path_2 = _write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-2",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_path_1, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_path_2, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
            ],
        )
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
            corrupt_record_head=True,
        )

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        record_head_findings = [f for f in findings if f.stage is Stage.FILE_MAP and f.symptom is Symptom.CORRUPTION]
        assert len(record_head_findings) == 1

    async def test_corrupt_individual_chunk_map_entry_is_a_finding_not_a_crash(self, tmp_path: Path) -> None:
        """Distinct from the ``RecordHead``-level gap above: here
        ``check_record_head``/``check_map_and_attr_crc`` both succeed (the
        array's own whole-array ``mapCrc`` covers the bytes as written,
        corrupt entry included), but ``plan_chunks_windowed`` — which
        re-parses the same array a second time through ``DedupFile``'s own
        reader — chokes on the one entry with an invalid type nibble and
        raises ``DataCorruptError``. ``_check_extent``'s ``try``/``except``
        around that call must turn this into a ``Finding`` too, not just
        the ``RecordHead``-parse-failure case."""
        bad_entry = bytes([0x0F]) + bytes(19)  # invalid kind nibble -- neither MAPPING(0) nor ZERO(1)
        good_entry = _mapping_record(0, 0, 0, map_num=1)
        entries = bad_entry + good_entry
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=2 * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=_SESSION_ID, entries=entries
        )

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(
            f.stage is Stage.COMPOSITION and f.symptom is Symptom.CORRUPTION and "chunk-map walk failed" in f.detail
            for f in findings
        )

    async def test_cache_miss_record_fetch_failure_is_a_finding_not_a_crash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_discover_extent``'s own cache-miss path (``self._composition_records``
        has never seen this ``record_key`` yet) calls ``extent.dedup_file._get_record()``
        directly, ahead of ``plan_chunks_windowed`` -- this must stay inside the
        same ``try``/``except`` as every other failure in this method rather
        than propagate and abort the whole ``verify_reachable()`` run, the same
        catch-and-continue contract ``test_corrupt_individual_chunk_map_entry_is_a_finding_not_a_crash``
        above already covers for a *later* failure in this same block."""
        from synology_apm_repo.sdk.dedup.dedup_file import DedupFile

        async def failing_get_record(self: DedupFile) -> None:
            raise NotFoundError("synthetic record-fetch failure", ref="synthetic")

        monkeypatch.setattr(DedupFile, "_get_record", failing_get_record)

        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [bytes([1]) * 4096])
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", [bytes([1]) * 4096])

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(
            f.stage is Stage.COMPOSITION
            and f.symptom is Symptom.CORRUPTION
            and "synthetic record-fetch failure" in f.detail
            for f in findings
        )

    async def test_earlier_window_bucket_claim_survives_a_later_window_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``plan_chunks_windowed`` can yield more than one ``ChunkPlan`` per
        extent (a window boundary mid-walk) -- an already-yielded window's
        own buckets must stay claimed even though a *later* window's own
        corrupt entry aborts the rest of the walk, proving
        ``_extent_bucket_keys``'s cache-miss path in ``_discover_extent``
        claims incrementally as each window is yielded, not only once the
        whole walk finishes without error (which would silently drop an
        otherwise-reachable bucket's corruption from this run entirely).
        ``plan_chunks_windowed`` itself is faked, the same way
        ``test_unrelated_valueerror_from_chunk_walk_is_not_absorbed`` below
        fakes it -- forcing a real multi-window split whose second window's
        own failure is independent of composition *page* boundaries (2048
        entries each, parsed as one atomic unit) would need implausibly
        large fixture data for what's really a unit test of
        ``_discover_extent``'s own claiming order, not of real chunk-map
        parsing."""
        from synology_apm_repo.sdk.dedup.chunk_walk import ChunkRun

        claimed_key = (StreamId(_STREAM_ID), BucketId(0))

        async def two_windows_then_raise(*args: object, **kwargs: object) -> AsyncIterator[ChunkPlan]:
            yield ChunkPlan(groups={claimed_key: [ChunkRun(0, 1, 0)]}, holes=0, zeros=0)
            raise DataCorruptError("synthetic later-window corruption", ref="synthetic")
            yield  # pragma: no cover - unreachable; keeps this an async generator

        monkeypatch.setattr(verify_reachable_module, "plan_chunks_windowed", two_windows_then_raise)

        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        # Wrong-but-self-consistent CRC on bucket 0's only chunk --
        # undetectable unless the faked first window's own claim above
        # actually reached _bucket_claim/_pending_buckets despite the
        # second window's raise.
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(
            f.stage is Stage.COMPOSITION
            and f.symptom is Symptom.CORRUPTION
            and "synthetic later-window corruption" in f.detail
            for f in findings
        )
        assert any(f.stage is Stage.BUCKET for f in findings)


class TestMapCrcRepairPropagation:
    """A repaired chunk-map array must reach *every* extent that walks it —
    not just get reported and then discarded. ``check_map_and_attr_crc``'s
    own repair is in-memory only; without propagating it into
    ``plan_chunks_windowed``'s own independent ``CompositionRecord``, that
    walk would silently re-derive entries from the still-corrupted on-disk
    bytes right after this run already reported them fixed."""

    async def test_both_versions_sharing_the_repaired_record_use_the_fixed_bytes(self, tmp_path: Path) -> None:
        """Two versions share one composition record whose sole
        ``ChunkMapRecord`` entry is corrupted on disk but parity-recoverable.
        Byte 13 (squarely inside ``ChunkAddress``'s ``bucket_id`` bits --
        stream_id is byte 8, chunk_idx is bytes 14-15) is flipped, which for
        an all-zero good address deterministically decodes to
        ``bucket_id=255`` instead of raising -- the "silently wrong, no
        exception" failure mode this fix targets, distinct from
        ``test_corrupt_individual_chunk_map_entry_is_a_finding_not_a_crash``
        above, which covers a corruption that *does* raise.

        Without seeding the repaired bytes into each version's own fresh
        ``CompositionRecord``, at least one version's walk would claim
        ``bucket_id=255`` (nonexistent) and report a ``Symptom.DATA_MISSING``
        ``Stage.BUCKET`` finding for it."""
        plaintexts = [bytes([1]) * 4096]
        good_entry = _mapping_record(0, 0, 0, map_num=1)
        corrupted_entry = bytearray(good_entry)
        corrupted_entry[13] ^= 0xFF

        coverage = REDUNDANCY_COVERAGE_COMPOSITION
        # A single window spanning the whole (20-byte) array: parity is just
        # that window XORed against nothing, i.e. the window's own bytes.
        redundancy_blob = (
            REDUNDANCY_MAGIC
            + struct.pack(">H", 0)
            + struct.pack(">IQ", coverage, len(good_entry))
            + struct.pack(">I", zlib.crc32(good_entry) & 0xFFFFFFFF)
            + good_entry
        )

        dedup_img_a = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        dedup_img_b = _write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-b",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
            ],
        )
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=good_entry,
            on_disk_entries=bytes(corrupted_entry),
            trailer=redundancy_blob,
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        repaired_findings = [f for f in findings if f.symptom is Symptom.REPAIRED_VIA_PARITY]
        assert len(repaired_findings) == 1  # memoized once per record, not once per sharing version
        bucket_findings = [f for f in findings if f.stage is Stage.BUCKET]
        assert bucket_findings == []  # neither version's walk wandered into the phantom bucket_id=255

    async def test_seeding_overrides_a_page_pcps_open_disk_already_prewarmed(self, tmp_path: Path) -> None:
        """PC/PS's own ``PcpsDiskTree.open_disk()`` (``device_pcps.py``'s
        ``_open_one()``) calls ``CompositionRecord.extent()`` — which
        cold-fetches page 0 (and the last page) straight off disk — while
        assembling a disk's fragments, *before* ``_discover_extent`` ever
        runs its own ``check_map_and_attr_crc`` on that same, memoized
        ``CompositionRecord``. For a single-page record (this test's
        ``map_num=1``), that pre-warmed page *is* the one page
        ``seed_pages_from_array`` needs to overwrite —
        ``AsyncKeyedCache.resolve`` is a no-op for an already-settled key,
        so without force-invalidating it first, this exact page would keep
        its stale, pre-repair entry even after a successful repair."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        good_entry = _mapping_record(0, 0, 0, map_num=1)
        corrupted_entry = bytearray(good_entry)
        corrupted_entry[13] ^= 0xFF  # see the sibling test above for why byte 13

        coverage = REDUNDANCY_COVERAGE_COMPOSITION
        redundancy_blob = (
            REDUNDANCY_MAGIC
            + struct.pack(">H", 0)
            + struct.pack(">IQ", coverage, len(good_entry))
            + struct.pack(">I", zlib.crc32(good_entry) & 0xFFFFFFFF)
            + good_entry
        )

        _write_pcps_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-pcps",
            target_id="PC-uid",
            fid=100,
            src_file_path=src_file_path,
            disk_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=good_entry,
            on_disk_entries=bytes(corrupted_entry),
            trailer=redundancy_blob,
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        assert any(f.symptom is Symptom.REPAIRED_VIA_PARITY for f in findings)
        assert [f for f in findings if f.stage is Stage.BUCKET] == []  # not the phantom bucket_id=255

    async def test_seed_pages_from_array_failure_is_its_own_finding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``seed_pages_from_array``'s own defensive length-mismatch
        ``ValueError`` (raised when a concurrent writer changes a record
        between two independent reads of it) is caught at its own call
        site in ``_discover_extent`` and turned into a
        ``Symptom.CORRUPTION`` finding — the same outcome the old, broader
        shared ``except`` used to produce, now scoped to just this call."""
        from synology_apm_repo.sdk.dedup.composition_reader import CompositionRecord

        async def failing_seed(self: CompositionRecord, array_raw: bytes) -> None:
            raise ValueError("synthetic reseed failure")

        monkeypatch.setattr(CompositionRecord, "seed_pages_from_array", failing_seed)

        plaintexts = [bytes([1]) * 4096]
        good_entry = _mapping_record(0, 0, 0, map_num=1)
        corrupted_entry = bytearray(good_entry)
        corrupted_entry[13] ^= 0xFF

        coverage = REDUNDANCY_COVERAGE_COMPOSITION
        redundancy_blob = (
            REDUNDANCY_MAGIC
            + struct.pack(">H", 0)
            + struct.pack(">IQ", coverage, len(good_entry))
            + struct.pack(">I", zlib.crc32(good_entry) & 0xFFFFFFFF)
            + good_entry
        )

        dedup_img = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=good_entry,
            on_disk_entries=bytes(corrupted_entry),
            trailer=redundancy_blob,
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        reseed_findings = [f for f in findings if "synthetic reseed failure" in f.detail]
        assert len(reseed_findings) == 1
        assert reseed_findings[0].symptom is Symptom.CORRUPTION
        assert reseed_findings[0].stage is Stage.COMPOSITION

    async def test_unrelated_valueerror_from_chunk_walk_is_not_absorbed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``ValueError`` raised deeper in ``plan_chunks_windowed``'s own
        call chain (e.g. ``chunk_walk.py``'s ``_validate_window_start``
        guarding a caller invariant, not corruption) must propagate rather
        than being caught by ``_discover_extent``'s ``except`` and
        misreported as a ``Symptom.CORRUPTION`` finding — that ``except``
        is scoped to ``plan_chunks_windowed``'s own corruption/not-found/
        format failures, not to an arbitrary ``ValueError`` from anywhere
        in its call chain."""

        async def failing_plan_chunks_windowed(*args: object, **kwargs: object) -> AsyncIterator[ChunkPlan]:
            raise ValueError("synthetic caller-invariant violation")
            yield  # pragma: no cover - never reached; makes this an async generator

        monkeypatch.setattr(verify_reachable_module, "plan_chunks_windowed", failing_plan_chunks_windowed)

        dedup_img = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [bytes([1]) * 4096])
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", [bytes([1]) * 4096])

        repo = await _open(tmp_path)
        try:
            with pytest.raises(ValueError, match="synthetic caller-invariant violation"):
                await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()


class TestBucketStageFindings:
    """A missing/corrupt bucket, and a legacy uncompressed bucket the
    per-chunk checks don't apply to at all, each take their own distinct
    path through ``_check_one_bucket``."""

    def _write_reachable_fs_version(self, tmp_path: Path, dedup_img_size: int) -> None:
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=dedup_img_size,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )

    async def test_missing_bucket_is_data_missing(self, tmp_path: Path) -> None:
        self._write_reachable_fs_version(tmp_path, dedup_img_size=4096)
        # no bucket file at all
        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.DATA_MISSING for f in findings)

    async def test_corrupt_bucket_header_is_corruption(self, tmp_path: Path) -> None:
        self._write_reachable_fs_version(tmp_path, dedup_img_size=4096)
        bucket_path = tmp_path / "@data" / "Pool" / "0" / "0.buk"
        _write_bucket(bucket_path, [bytes([1]) * 4096])
        raw = bytearray(bucket_path.read_bytes())
        raw[0] ^= 0xFF  # corrupt the "bFiL" magic
        bucket_path.write_bytes(bytes(raw))

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.CORRUPTION for f in findings)

    async def test_legacy_uncompressed_bucket_has_nothing_to_check(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        self._write_reachable_fs_version(tmp_path, dedup_img_size=len(plaintexts) * 4096)
        _write_legacy_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []

    async def test_unexpected_error_opening_a_bucket_is_a_finding_not_a_crash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An exception that isn't ``NotFoundError``/``DataCorruptError``/``FormatError``
        (a real ``ObjectStore`` can raise ``PermissionDeniedError``, for one)
        escaping the bucket-*open* call itself must still become a
        ``Finding`` rather than propagating into ``flush_pending_buckets``'s
        ``asyncio.TaskGroup`` and cancelling every other bucket in the same
        batch -- distinct from ``TestConcurrentBucketCheckIsolation``'s own
        test, which covers a failure *after* a successful open instead."""
        self._write_reachable_fs_version(tmp_path, dedup_img_size=4096)
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [bytes([1]) * 4096])

        import synology_apm_repo.sdk.dedup.pool as pool_module

        async def failing_open(self: pool_module.Pool, key: tuple[StreamId, BucketId]) -> pool_module.BucketReader:
            raise RuntimeError("simulated unexpected open failure")

        monkeypatch.setattr(pool_module.Pool, "open_bucket_uncached_by_key", failing_open)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.CORRUPTION
        assert "unexpected error" in findings[0].detail


class TestKeyMissing:
    """A vault-encrypted bucket in a repository opened without a vault
    key surfaces exactly one ``KEY_MISSING`` finding — header-derived, so
    it fires at both levels even though QUICK never attempts any chunk
    check (key or no key) and FULL skips only its own decode+fingerprint
    half."""

    async def test_encrypted_bucket_without_key_is_key_missing(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_encrypted_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, os.urandom(32))

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.ENCRYPT_KEY and f.symptom is Symptom.KEY_MISSING for f in findings)

    async def test_full_level_still_checks_ciphertext_crc_exhaustively_without_a_key(self, tmp_path: Path) -> None:
        """FULL's own promise (see ``units/verify_reachable.py``'s module
        docstring) is that ciphertext CRC stays exhaustive regardless of
        key availability — only decode+fingerprint needs one. Proven by
        corrupting the last of several chunks and confirming FULL still
        catches it even with no vault key."""
        chunk_num = 6
        plaintexts = [bytes([1]) * 4096 for _ in range(chunk_num)]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=chunk_num),
        )
        _write_encrypted_bucket(
            tmp_path / "@data" / "Pool" / "0" / "0.buk",
            plaintexts,
            os.urandom(32),
            corrupt_chunk_crc_idx=chunk_num - 1,
        )

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.ENCRYPT_KEY and f.symptom is Symptom.KEY_MISSING for f in findings)
        assert any(
            f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH and "ciphertext" in f.detail for f in findings
        )


class TestFsExtentSizeUnavailable:
    """``composition_extents_for_version``'s FS branch returns no extent
    at all (nothing to check, no crash) when ``dedup_file.size`` can't be
    resolved — no ``db/file_meta`` row for this ``dedup.img`` path."""

    async def test_no_file_meta_row_yields_no_extents_and_no_crash(self, tmp_path: Path) -> None:
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,  # dedup_img_size omitted -- no db/file_meta row at all
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        # deliberately no bucket at all -- if this version's extent were
        # checked at all, the missing bucket would surface as a finding.

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert findings == []


class TestVmExtents:
    """``_vm_extents`` -- resolves a VM version's disk object(s) via
    ``DeviceProvider``, the exact machinery real VM browsing uses."""

    async def test_vm_disk_extent_is_checked(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        _write_vm_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-vm",
            target_id="VM-uid",
            meta_dirname="VM_meta",
            disk_name="disk.img",
            src_file_path=src_file_path,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)

    async def test_non_dedup_and_unresolved_size_objects_are_skipped(self, tmp_path: Path) -> None:
        """A plain sidecar file (``dedup_object=0``, no Pool addressing at
        all) and a dedup object whose own ``file_size`` never resolved
        (``NULL`` in ``object_table``) are both skipped, not raised on --
        only the one real, resolvable disk contributes an extent."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        _write_vm_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-vm",
            target_id="VM-uid",
            meta_dirname="VM_meta",
            disk_name="disk.img",
            src_file_path=src_file_path,
            extra_objects=[
                (2, 1, "sidecar.txt", "VM-uid/ActiveBackup_2026-01-01/my-vm/sidecar.txt", 0, 10),
                (3, 1, "disk1.img", "VM-uid/ActiveBackup_2026-01-01/my-vm/disk1.img", 1, None),
            ],
        )
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                # The null-file_size disk's own row -- its composition
                # opens fine (locate_file() succeeds), the object simply
                # never got a real file_size in object_table, which is
                # what content.size is None actually exercises (a real
                # file_map miss would raise NotFoundError earlier, inside
                # provider.unit() itself, aborting the whole version --
                # a different, already-covered case).
                ("VM-uid/ActiveBackup_2026-01-01/my-vm/disk1.img", _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
            ],
        )
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []

    async def test_one_unresolvable_disk_does_not_discard_another_resolvable_disks_findings(
        self, tmp_path: Path
    ) -> None:
        """A real ``file_map`` miss on one disk (``provider.unit()`` raises
        ``NotFoundError``) used to abort ``_vm_extents`` entirely -- the case
        the test above's own comment called "a different, already-covered
        case" -- discarding every other, fully-resolvable disk's checks
        along with it. This confirms the fix: the missing disk gets its
        own ``Stage.VERSION``/``DATA_MISSING`` finding, and the other
        disk's own bucket-corruption finding still surfaces alongside
        it."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        missing_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk1.img"
        _write_vm_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-vm",
            target_id="VM-uid",
            meta_dirname="VM_meta",
            disk_name="disk.img",
            src_file_path=src_file_path,
            extra_objects=[
                # dedup_object=1 (not a sidecar) with a real file_size, but
                # no matching db/file_map row below -- provider.unit()
                # raises NotFoundError for this one disk specifically.
                (3, 1, "disk1.img", missing_file_path, 1, 4096),
            ],
        )
        _write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)
        assert any(
            f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING and "disk1.img" in f.path for f in findings
        )

    async def test_unresolvable_disk_with_a_non_notfound_error_is_a_corruption_finding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The other branch of ``_unresolvable_disk_finding`` -- a disk
        object failing with something other than ``NotFoundError`` (e.g. a
        genuinely corrupt composition record) surfaces as
        ``Symptom.CORRUPTION``, not ``DATA_MISSING``, same distinction
        ``discover_version``'s own identical whole-version branch already
        makes."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        _write_vm_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-vm",
            target_id="VM-uid",
            meta_dirname="VM_meta",
            disk_name="disk.img",
            src_file_path=src_file_path,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        import synology_apm_repo.sdk.units.device as device_module

        async def fake_unit(self: device_module.DeviceProvider, node: Node) -> object:
            raise DataCorruptError("synthetic corruption")

        monkeypatch.setattr(device_module.DeviceProvider, "unit", fake_unit)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.VERSION and f.symptom is Symptom.CORRUPTION for f in findings)


class TestPcpsExtents:
    """``_pcps_extents`` -- resolves a PC/PS version's disk fragment(s) via
    ``DeviceProvider``/``VirtualDiskContentSource``, one
    ``CompositionExtent`` per fragment."""

    async def test_pcps_fragment_extent_is_checked(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_pcps_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-pcps",
            target_id="PC-uid",
            fid=100,
            src_file_path=src_file_path,
            disk_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)

    async def test_one_unresolvable_disk_does_not_discard_another_resolvable_disks_findings(
        self, tmp_path: Path
    ) -> None:
        """Same partial-failure fix as ``TestVmExtents``'s own identical
        test, exercised through ``_pcps_extents`` instead: two disks
        (neither's filename matches the ``D(...)S(...)`` fragment-grouping
        convention, so each lands as its own single-fragment disk, per
        ``_pcps_disk_key``'s own docstring), one fully resolvable, one
        whose only fragment has a ``file_meta`` row but no matching
        ``file_map`` row -- ``PcpsDiskTree.open_disk()`` raises ``NotFoundError``
        for that disk alone (every fragment failed to resolve), which must
        not discard the other, fully-resolvable disk's own checks."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        missing_file_path = "PC-uid/ActiveBackup_2026-01-01/disk1.img"
        version_id = 1000
        _write_workload_config(
            tmp_path / "db" / "workload_config",
            [
                (
                    10,
                    "PC-uid-uid",
                    "PC",
                    {"namespace": "ns-a", "spec": {"workload_type": "PC", "workload_name": "PC-uid"}},
                )
            ],
        )
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(version_id, 10, 1, "vuid-pcps", "PC", "PC-uid", "", "", 0, 0, _version_spec_json(1786000000))],
        )
        _write_copy_target_file(tmp_path / "db" / "copy_target_version", [(version_id, 100), (version_id, 200)])
        _write_pcps_file_meta(
            tmp_path / "db" / "file_meta",
            [(100, src_file_path, len(plaintexts) * 4096), (200, missing_file_path, 4096)],
        )
        _write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)
        assert any(f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING for f in findings)

    async def test_unit_with_unexpected_content_type_is_skipped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Defensive branch, never observed in practice --
        ``DeviceProvider.unit()`` only ever returns a
        ``VirtualDiskContentSource`` for a ``PCPS_DISK`` node -- but
        ``_pcps_extents`` must skip rather than crash if that ever
        changes/a future content type doesn't match."""
        src_file_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_pcps_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-pcps",
            target_id="PC-uid",
            fid=100,
            src_file_path=src_file_path,
            disk_size=4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        # deliberately no bucket at all -- if the fragment were checked at
        # all despite the fake unit below, its missing bucket would
        # surface as a finding.

        import synology_apm_repo.sdk.units.device as device_module

        async def fake_unit(self: device_module.DeviceProvider, node: Node) -> SimpleNamespace:
            return SimpleNamespace(content=object())

        monkeypatch.setattr(device_module.DeviceProvider, "unit", fake_unit)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert findings == []


class TestAllChildrenPagination:
    """``_all_children``'s own pagination loop -- a full first page (equal
    to its internal 1024-item page size) must fetch a second page rather
    than stopping short, exercised directly against a fake provider
    rather than engineering 1024+ real catalog rows."""

    async def test_a_full_first_page_fetches_a_second(self) -> None:
        page_size = 1024
        first_page = [Node(ref=NodeRef("repo", (f"n{i}",)), name=f"n{i}", is_leaf=True) for i in range(page_size)]
        second_page = [Node(ref=NodeRef("repo", ("last",)), name="last", is_leaf=True)]
        calls: list[tuple[int, int | None]] = []

        class _FakeProvider:
            async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
                calls.append((offset, limit))
                return first_page if offset == 0 else second_page

        root = Node(ref=NodeRef("repo", ()), name="root", is_leaf=False)
        result = await _all_children(cast(DeviceProvider, _FakeProvider()), root)
        assert result == [*first_page, *second_page]
        assert calls == [(0, page_size), (page_size, page_size)]


class TestSaasExtents:
    """``_saas_extents`` -- resolves an M365/GW version's whole
    ``saas_obj`` via ``SaasStream.open_saas_obj``."""

    async def test_saas_obj_extent_is_checked(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        saas_obj_path = _write_saas_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-saas",
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            saas_obj_size=len(plaintexts) * 4096,
        )
        _write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, len(plaintexts) * 4096)])
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)

    async def test_saas_obj_size_unavailable_yields_no_extent_and_no_crash(self, tmp_path: Path) -> None:
        """Mirrors ``TestFsExtentSizeUnavailable``: no ``db/file_meta``
        row for the resolved ``saas_obj`` path means ``dedup_file.size``
        is ``None``, so ``_saas_extents`` returns no extent at all rather
        than crashing on an unresolvable size."""
        saas_obj_path = _write_saas_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-saas",
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            saas_obj_size=4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        # deliberately no bucket at all -- if this version's extent were
        # checked at all, the missing bucket would surface as a finding.

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert findings == []


class TestSaasResolutionLabel:
    """``_annotate_with_saas_resolution`` — reaches a real ``Finding.path``
    only via ``CompositionExtent.unit_label`` (a ``Stage.COMPOSITION``/
    ``Symptom.CORRUPTION`` finding), not the common ``Stage.BUCKET``/
    ``Symptom.MISMATCH`` case (its own docstring explains why). Tested
    directly here (calling ``open_saas_obj``/the annotator on a real,
    fixture-backed ``SaasStreamCache``) rather than only through a full
    ``verify_reachable()`` run: ``_checked_sessions`` dedups
    ``check_composition_header`` to whichever catalog version is walked
    *first* for a shared composition sub-file, so an end-to-end fixture
    can't reliably exercise a specific version's own annotation without
    depending on unrelated sort-order details ``TestSaasSupersededGeneration``
    already covers for the broader "content still gets checked" claim."""

    async def test_annotates_when_a_substitution_happened(self, tmp_path: Path) -> None:
        saas_obj_path = _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,
            total_versions=3,
            latest_complete_version=3,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        repo = await _open(tmp_path)
        try:
            workload = await workload_by_id(repo, WorkloadId(10))
            assert workload is not None
            all_versions = await versions(repo, workload)
            version = next(v for v in all_versions if v.saas_version_id == 1)
            async with SaasStreamCache(repo) as cache:
                await cache.open_saas_obj(version)
                label = _annotate_with_saas_resolution("SaaS saas_obj", cache, version)
        finally:
            await repo.close()
        assert label == "SaaS saas_obj (stream_version 3, requested 1)"

    async def test_leaves_label_unannotated_when_no_substitution_happened(self, tmp_path: Path) -> None:
        saas_obj_path = _write_saas_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-saas",
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            saas_obj_size=4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        repo = await _open(tmp_path)
        try:
            workload = await workload_by_id(repo, WorkloadId(10))
            assert workload is not None
            [version] = await versions(repo, workload)
            async with SaasStreamCache(repo) as cache:
                await cache.open_saas_obj(version)
                label = _annotate_with_saas_resolution("SaaS saas_obj", cache, version)
        finally:
            await repo.close()
        assert label == "SaaS saas_obj"

    async def test_end_to_end_composition_corruption_finding_carries_the_label(self, tmp_path: Path) -> None:
        """The plumbing (``_saas_extents`` → ``CompositionExtent.unit_label``
        → ``check_composition_header``) actually reaches a real
        ``Finding.path`` — proven through the full ``verify_reachable()``
        pipeline, not just this class's own direct calls to
        ``_annotate_with_saas_resolution`` above. A single-version fixture
        (no substitution) is used deliberately — see this class's own
        docstring for why a multi-generation one can't reliably exercise a
        *specific* version's own annotation end-to-end."""
        plaintexts = [bytes([1]) * 4096]
        saas_obj_path = _write_saas_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-saas",
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            saas_obj_size=len(plaintexts) * 4096,
        )
        _write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, len(plaintexts) * 4096)])
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
            corrupt_header=True,
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        corruptions = [f for f in findings if f.stage is Stage.COMPOSITION and f.symptom is Symptom.CORRUPTION]
        assert corruptions
        # No substitution happened here (the requested stream_version is
        # itself the live one) -- _annotate_with_saas_resolution leaves
        # the label unchanged.
        assert all(f.path == "SaaS saas_obj" for f in corruptions)


class TestSaasSupersededGeneration:
    """The actual MiaDemo regression: several catalog versions in one
    snapshot map (via ``version_info``) to different, increasing
    ``stream_version``s; only the latest has a resolvable ``saas_obj`` --
    older generations were server-side GC'd (``cleanupSaasFile()``,
    workload-format/saas-obj.md §11.4.6). Forward-resolution must read
    every one of them via the substituted, still-live generation, not
    report ``DATA_MISSING``."""

    async def test_older_generations_resolve_via_the_substituted_live_one(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        saas_obj_path = _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,
            total_versions=3,
            latest_complete_version=3,
        )
        _write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, len(plaintexts) * 4096)])
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert not any(f.symptom is Symptom.DATA_MISSING for f in findings)
        # Proves the substituted generation's real content is actually
        # read/checked, not just silently skipped: the deliberately
        # corrupted chunk still surfaces. (Bucket-stage findings don't
        # carry the resolved-generation label -- see TestSaasResolutionLabel
        # for where that annotation actually surfaces.)
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)

    async def test_genuine_gap_still_reports_data_missing(self, tmp_path: Path) -> None:
        """No live generation exists anywhere for any of the 3 catalog
        versions -- must not be masked by the forward-resolution fix."""
        _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,  # never actually written to file_map below
            total_versions=3,
            latest_complete_version=3,
        )
        # Deliberately no db/file_map row at all -- nothing resolves.
        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        missing = [f for f in findings if f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING]
        assert len(missing) == 3

    async def test_generation_past_latest_complete_version_is_not_used_as_substitute(self, tmp_path: Path) -> None:
        """A ``file_map`` row exists at ``stream_version=3``, but
        ``latest_complete_version=1`` (simulated crash garbage, per
        ``SaasDbWrapper::Rollback()``'s own semantics) -- must not be
        used as a substitute; the real gap is still reported."""
        _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,
            total_versions=3,
            latest_complete_version=1,
        )
        saas_obj_path = "stream-uuid-1/conn-a/3/saas_obj"
        _write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, 4096)])
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [bytes([1]) * 4096])
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", [bytes([1]) * 4096])

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        missing = [f for f in findings if f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING]
        assert len(missing) == 3


class TestSaasCachingAcrossWalkerRun:
    """The correctness/performance defect this whole redesign guards
    against: ``_saas_extents`` must resolve through the walker's own
    run-scoped ``SaasStreamCache``, not a fresh ``SaasStream`` per
    ``discover_version()`` call — otherwise every per-stream cache
    forward-resolution builds gets rebuilt from scratch on every catalog
    version."""

    async def test_file_map_prefix_scan_runs_once_per_middle_not_once_per_catalog_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        plaintexts = [bytes([1]) * 4096]
        saas_obj_path = _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,
            total_versions=3,
            latest_complete_version=3,
        )
        _write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, len(plaintexts) * 4096)])
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        calls = 0
        real_fn = repo.file_map_paths_with_prefix

        async def _counted(prefix: str, *, status: int | None = None) -> list[str]:
            nonlocal calls
            calls += 1
            return await real_fn(prefix, status=status)

        monkeypatch.setattr(repo, "file_map_paths_with_prefix", _counted)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert not any(f.symptom is Symptom.DATA_MISSING for f in findings)
        # 3 catalog versions all sharing one stream, 2 candidate middles
        # (connection_id, numeric ccid) -- scanned once each for the
        # whole run, not once per catalog version (which would be 6).
        assert calls == 2


class TestBucketSizing:
    """``finalize_pending_buckets()``'s own FULL-level sizing pass --
    ``_size_one_bucket``'s ``NotFoundError`` handling specifically, since a
    claimed bucket can be missing on disk (a stale/rotated reference) the
    same way ``_check_one_bucket`` itself already handles."""

    async def test_a_missing_bucket_sizes_as_zero_bytes_without_raising(self, tmp_path: Path) -> None:
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        # Deliberately no bucket file at all -- exercises _size_one_bucket's
        # own NotFoundError branch, not just _check_one_bucket's.

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.DATA_MISSING for f in findings)

    async def test_a_permission_denied_bucket_sizes_as_zero_bytes_without_raising(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_size_one_bucket`` is called from inside
        ``finalize_pending_buckets``'s own ``asyncio.TaskGroup`` — letting
        ``PermissionDeniedError`` (a real, documented ``ObjectStore.size()``
        outcome) escape there would cancel every other in-flight sizing
        task and abort the whole ``verify_reachable()`` call. The bucket
        itself is real and otherwise intact, so the run still completes
        clean once checking (not just sizing) actually opens it."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        bucket_path = tmp_path / "@data" / "Pool" / "0" / "0.buk"
        _write_bucket(bucket_path, plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        real_size = LocalFsStore.size

        calls = 0

        async def denying_size(self: LocalFsStore, path: str) -> int:
            nonlocal calls
            calls += 1
            # Only the sizing pass's own call (the first) should fail --
            # check_bucket_structure's later, independent size() call
            # (part of the real check, not sizing) must still succeed so
            # this test isolates _size_one_bucket's own failure handling.
            if path.endswith("0.buk") and calls == 1:
                raise PermissionDeniedError("simulated: permission denied", ref=path)
            return await real_size(self, path)

        monkeypatch.setattr(LocalFsStore, "size", denying_size)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []

    async def test_an_unexpected_sizing_error_sizes_as_zero_bytes_without_raising(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The broad ``except Exception`` net -- some other, unanticipated
        backend error, not one of ``ObjectStore.size()``'s own documented
        ``NotFoundError``/``PermissionDeniedError`` outcomes."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        bucket_path = tmp_path / "@data" / "Pool" / "0" / "0.buk"
        _write_bucket(bucket_path, plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        real_size = LocalFsStore.size

        calls = 0

        async def broken_size(self: LocalFsStore, path: str) -> int:
            nonlocal calls
            calls += 1
            if path.endswith("0.buk") and calls == 1:
                raise RuntimeError("simulated: unexpected backend error")
            return await real_size(self, path)

        monkeypatch.setattr(LocalFsStore, "size", broken_size)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []


class TestProgressCallback:
    """``progress`` is reported in two stages: ``phase="discovering"``
    (``unit="items"``, one tick per ``(workload, version)`` pair, a
    stable denominator known up front) while walking versions and
    claiming their buckets, then ``phase="verifying"`` (one tick per
    bucket actually checked) once discovery is entirely done and a real,
    stable total is known. See ``verify_reachable()``'s own docstring for
    why checking never starts until every version has been discovered: a
    done/total pair against a still-growing "claimed so far" total would
    make the percentage/rate/ETA actively misleading, not just
    approximate.

    The ``verifying`` unit itself depends on ``level``: ``"bytes"`` at
    FULL (each claimed bucket's own real on-disk size, summed), or
    ``"buckets"`` at QUICK (a plain count, no sizing at all) -- see
    ``finalize_pending_buckets``'s own docstring for why bytes would
    overstate QUICK's real throughput rather than just being
    unavailable."""

    async def test_full_level_reports_discovering_then_verifying_in_bytes(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        bucket_path = tmp_path / "@data" / "Pool" / "0" / "0.buk"
        _write_bucket(bucket_path, plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)
        bucket_size = bucket_path.stat().st_size

        calls: list[Progress] = []

        async def on_progress(progress: Progress) -> None:
            calls.append(progress)

        repo = await _open(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.FULL, progress=on_progress)
        finally:
            await repo.close()
        assert len(calls) == 2

        discovering = calls[0]
        assert discovering.phase == "discovering"
        assert discovering.determinate is True
        assert discovering.unit == "items"
        assert discovering.done == 0
        assert discovering.total == 1
        assert "fsA" in discovering.detail

        verifying = calls[1]
        assert verifying.phase == "verifying"
        assert verifying.determinate is True
        assert verifying.unit == "bytes"
        assert verifying.done == bucket_size
        assert verifying.total == bucket_size
        assert "fsA" in verifying.detail

    async def test_quick_level_reports_verifying_as_a_plain_bucket_count(self, tmp_path: Path) -> None:
        """The exact same fixture as the FULL-level test above, at QUICK
        instead -- no sizing pass runs at all, and the ``verifying`` tick
        is a plain ``1/1`` bucket count rather than a byte total."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        calls: list[Progress] = []

        async def on_progress(progress: Progress) -> None:
            calls.append(progress)

        repo = await _open(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.QUICK, progress=on_progress)
        finally:
            await repo.close()
        verifying = next(c for c in calls if c.phase == "verifying")
        assert verifying.unit == "buckets"
        assert verifying.done == 1
        assert verifying.total == 1

    async def test_reports_once_per_bucket_across_a_single_version_claiming_several(self, tmp_path: Path) -> None:
        """One version claiming several distinct buckets in one
        ``discover_version`` call still gets a separate ``verifying``
        progress call per bucket, not one lumped call for the whole
        version -- checking never starts until discovery is entirely
        done, so each of the 5 buckets claimed here gets its own turn
        through ``check_all_buckets()``'s own ``TaskGroup`` rather than
        being folded into a single report for the version that claimed
        them all."""
        bucket_ids = [0, 1, 2, 3, 4]
        entries = b"".join(
            _mapping_record(i * 4096, bucket_id=bid, chunk_idx=0, map_num=1) for i, bid in enumerate(bucket_ids)
        )
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(bucket_ids) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=_SESSION_ID, entries=entries
        )
        # No real bucket files -- each one simply reports NotFoundError/
        # DATA_MISSING, which still counts as "checked" for progress
        # purposes (this test only cares that every claimed bucket gets
        # its own progress call, not what each one finds). QUICK level,
        # so no sizing pass to worry about either -- a plain bucket count.

        calls: list[Progress] = []

        async def on_progress(progress: Progress) -> None:
            calls.append(progress)

        repo = await _open(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.QUICK, progress=on_progress)
        finally:
            await repo.close()
        verifying_calls = [c for c in calls if c.phase == "verifying"]
        assert len(verifying_calls) == len(bucket_ids)
        assert all(c.total == len(bucket_ids) for c in verifying_calls)
        assert sorted(c.done for c in verifying_calls) == list(range(1, len(bucket_ids) + 1))


class TestFindingType:
    def test_finding_is_frozen(self) -> None:
        finding = Finding(Stage.VERSION, Symptom.DATA_MISSING, "path", "detail")
        with pytest.raises(AttributeError):
            finding.path = "other"  # type: ignore[misc]

    def test_ref_defaults_to_none(self) -> None:
        assert Finding(Stage.VERSION, Symptom.DATA_MISSING, "path", "detail").ref is None


class TestFindingRef:
    """Every ``Finding`` a version's own check produces is tagged with a
    canonical ``cat:/wl:/ver:`` ref naming that exact version — see
    ``Finding.ref``'s own docstring for the shared-dedup caveat."""

    async def test_unresolvable_version_finding_carries_its_own_ref(self, tmp_path: Path) -> None:
        _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-broken",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            # write_target_db defaults to True -- target.db itself resolves
            # fine; the missing db/file_map row below is what actually
            # makes this version fail, once its content is really opened.
        )
        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].ref == "#cat:1/wl:10/ver:vuid-broken"

    async def test_a_deeper_stage_finding_carries_the_same_ref(self, tmp_path: Path) -> None:
        """Not just the ``Stage.VERSION`` resolution-failure case above --
        a bucket-stage finding is tagged too, though by a different
        mechanism: this version's own ``discover_version`` call claims the
        missing bucket (recording its ``ref`` in ``_bucket_ref_claim``),
        and the claim's ref is what actually gets applied to the finding,
        at whichever later ``flush_pending_buckets`` call opens and fails
        to find it — not a post-pass over what ``discover_version`` itself
        returned, the way the composition-stage case above works."""
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        # no bucket file at all -- a Stage.BUCKET/DATA_MISSING finding

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].ref == "#cat:1/wl:10/ver:vuid-1"


class TestBucketBatchFlushing:
    """``verify_reachable()``'s discovery loop sizes claimed buckets once
    ``_BUCKET_BATCH_SIZE`` is reached, not only at the very end -- but the
    check only ever runs *between* versions, so crossing the boundary
    needs more than one version's own discovery to actually observe two
    sizing flushes (a single version claiming everything at once still
    only ever triggers one). Checking itself (``check_all_buckets()``)
    always runs exactly once, after discovery finishes entirely -- there
    is nothing left to prove about batching there any more."""

    def _build_version_touching_buckets(
        self,
        tmp_path: Path,
        *,
        workload_id: int,
        version_uid: str,
        target_id: str,
        meta_dirname: str,
        session_id: int,
        bucket_ids: list[int],
    ) -> str:
        entries = b"".join(
            _mapping_record(i * 4096, bucket_id=bid, chunk_idx=0, map_num=1) for i, bid in enumerate(bucket_ids)
        )
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=workload_id,
            version_uid=version_uid,
            target_id=target_id,
            meta_dirname=meta_dirname,
            dedup_version_id=1,
            dedup_img_size=len(bucket_ids) * 4096,
        )
        _write_composition(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, entries=entries
        )
        return dedup_img_path

    async def _count_meaningful_flushes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> int:
        """Counts only calls with something actually pending -- the main
        loop's own unconditional post-loop flush is always called once
        more regardless, and would otherwise inflate every count by one
        even when it has nothing left to do."""
        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        flush_calls = 0
        original = verify_reachable_module._ReachabilityWalker.finalize_pending_buckets

        async def counting_flush(self: verify_reachable_module._ReachabilityWalker) -> None:
            nonlocal flush_calls
            if self.pending_bucket_count > 0:
                flush_calls += 1
            await original(self)

        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "finalize_pending_buckets", counting_flush)
        repo = await _open(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        return flush_calls

    async def test_one_version_at_exactly_the_batch_size_flushes_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        batch_size = verify_reachable_module._BUCKET_BATCH_SIZE
        dedup_img_a = self._build_version_touching_buckets(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            session_id=_SESSION_ID,
            bucket_ids=list(range(batch_size)),
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        # Deliberately no bucket files at all -- this test only cares how
        # many times finalize_pending_buckets() itself runs, not what each
        # one finds (a "missing bucket" Finding per claimed key, from the
        # later check phase, is fine).
        assert await self._count_meaningful_flushes(tmp_path, monkeypatch) == 1

    async def test_crossing_the_batch_boundary_across_two_versions_flushes_twice(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        batch_size = verify_reachable_module._BUCKET_BATCH_SIZE
        dedup_img_a = self._build_version_touching_buckets(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            session_id=_SESSION_ID,
            bucket_ids=list(range(batch_size)),
        )
        # A second, distinct version claiming one more, previously
        # unclaimed bucket -- the mid-loop check only fires again once
        # *this* version's own discovery pushes pending back over the
        # threshold, which one new bucket alone can't do; the run's final,
        # unconditional flush is what actually picks this one up.
        dedup_img_b = self._build_version_touching_buckets(
            tmp_path,
            workload_id=11,
            version_uid="vuid-2",
            target_id="fsB",
            meta_dirname="FSB_meta",
            session_id=_SESSION_ID + 1,
            bucket_ids=[batch_size],
        )
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID + 1, _COMP_OFFSET, 1, 2),
            ],
        )
        assert await self._count_meaningful_flushes(tmp_path, monkeypatch) == 2


class TestConcurrentBucketCheckIsolation:
    """One bucket's own check failing unexpectedly must not stop the
    other buckets in its concurrent flush batch from being checked and
    reported -- proves ``_check_one_bucket``'s own full-body safety net
    (see its docstring) actually delivers the outcome it exists for: no
    batch sibling of a failing bucket is ever silently dropped."""

    async def test_one_broken_bucket_does_not_prevent_its_batch_siblings_from_reporting(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        bucket_ids = [0, 1, 2]
        entries = b"".join(
            _mapping_record(i * 4096, bucket_id=bid, chunk_idx=0, map_num=1) for i, bid in enumerate(bucket_ids)
        )
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(bucket_ids) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=_SESSION_ID, entries=entries
        )
        # Every bucket fully COMPACTED -- non_compacted_chunk_indices()
        # returns [] for each, so check_bucket_structure (monkeypatched
        # below) is the *only* thing this test's own outcome depends on.
        for bucket_id in bucket_ids:
            _write_compacted_bucket(tmp_path / "@data" / "Pool" / "0" / f"{bucket_id}.buk")

        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        async def flaky_check_bucket_structure(repo: DedupRepo, reader: BucketReader) -> list[Finding]:
            if reader.path.endswith("/1.buk"):
                raise RuntimeError("simulated unexpected failure")
            return []

        monkeypatch.setattr(verify_reachable_module, "check_bucket_structure", flaky_check_bucket_structure)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        bucket_findings = [f for f in findings if f.stage is Stage.BUCKET]
        assert len(bucket_findings) == 1
        assert bucket_findings[0].symptom is Symptom.CORRUPTION
        assert "unexpected error" in bucket_findings[0].detail
        assert bucket_findings[0].path.endswith("/1.buk")

    async def test_a_format_error_past_the_open_call_is_corruption_not_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Distinct from a ``NotFoundError`` *opening* the bucket (its own
        ``Symptom.DATA_MISSING`` branch, a step earlier) -- a
        ``NotFoundError``/``DataCorruptError``/``FormatError`` raised *after* the
        bucket already opened successfully (here, from
        ``check_bucket_structure`` itself) is a different, more
        surprising failure and classifies as ``Symptom.CORRUPTION``
        instead, via this method's own specific-exception branch rather
        than its final broad safety net."""
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        _write_compacted_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk")

        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        async def failing_check_bucket_structure(repo: DedupRepo, reader: BucketReader) -> list[Finding]:
            raise NotFoundError("simulated: bucket vanished after its own open", ref=reader.path)

        monkeypatch.setattr(verify_reachable_module, "check_bucket_structure", failing_check_bucket_structure)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.CORRUPTION
        assert "unexpected error" not in findings[0].detail


class TestFullChecksEveryPhysicalChunk:
    """FULL's per-bucket check is drawn from every *physical*, live chunk
    in a touched bucket, not just the one chunk-map-referenced index that
    caused the bucket to be claimed -- and always skips ``COMPACTED``
    slots, never mistaking one for corruption."""

    async def test_full_skips_a_compacted_slot_and_checks_the_rest(self, tmp_path: Path) -> None:
        plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(6)]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,  # this extent's own logical size -- one chunk
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        # The chunk-map references only chunk_idx=1 of this bucket -- but
        # whole-bucket checking claims the *whole* bucket regardless, so
        # FULL's own check below covers every physical live chunk (1..5,
        # six total minus the one compacted at index 0), not just this one
        # referenced index.
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 1, map_num=1),
        )
        _write_bucket_with_compacted_slot(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, compacted_idx=0)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []


class TestMultiVersionBucketRefClaim:
    """Two versions whose extents touch one shared bucket, plus one of
    them also touching a second, unshared bucket -- each bucket's own
    finding carries whichever version's discovery claimed it first
    (``_bucket_ref_claim``'s "first claimer wins"), regardless of how
    later batching actually schedules the checks."""

    async def test_shared_and_unshared_buckets_carry_their_claiming_versions_ref(self, tmp_path: Path) -> None:
        dedup_img_a = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        dedup_img_b = _write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-2",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=2 * 4096,
        )
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID + 1, _COMP_OFFSET, 1, 2),
            ],
        )
        # Version 1's own composition: references bucket 0 only.
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=1),
        )
        # Version 2's own composition: references bucket 0 (already
        # claimed by version 1's own discovery, which runs first) *and*
        # bucket 1 (new, unclaimed until now).
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID + 1,
            entries=_mapping_record(0, 0, 0, map_num=1) + _mapping_record(4096, 1, 0, map_num=1),
        )
        # Neither bucket file exists on disk -- each claim surfaces as its
        # own DATA_MISSING finding, so this test can assert purely on
        # which ref each one carries.

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        # Pool bucket addressing (embedded in _mapping_record's own
        # addr_int) is stream 0 implicitly -- independent of _STREAM_ID,
        # which addresses the *composition* record instead.
        bucket_findings = {f.path: f for f in findings if f.stage is Stage.BUCKET}
        assert len(bucket_findings) == 2
        assert bucket_findings["bucket 0/0"].ref == "#cat:1/wl:10/ver:vuid-1"
        assert bucket_findings["bucket 0/1"].ref == "#cat:1/wl:11/ver:vuid-2"


class TestMultiprocessExecutorTeardown:
    """FULL level's own ``ProcessPoolExecutor.shutdown()`` must never be
    called directly — it's a plain, synchronous, potentially slow blocking
    call (as slow as whatever a still-running worker takes to finish its
    current bucket), which would otherwise freeze this whole process's
    event loop — every other ``Task``, not just this one verify run — for
    that whole stretch. Measured directly (a scratch script, not this
    test): ~1.4s of total event-loop freeze for one deliberately slow
    worker with no ``to_thread()`` hop. This fixture's real ``LocalFsStore``
    is describable, so this exercises the real multiprocess path, not the
    fallback."""

    async def test_executor_shutdown_is_routed_through_to_thread(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from concurrent.futures import ProcessPoolExecutor

        plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(3)]
        dedup_img_path = _write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        _write_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=_mapping_record(0, 0, 0, map_num=len(plaintexts)),
        )
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        _write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        real_to_thread = asyncio.to_thread
        recorded: list[object] = []

        async def _recording_to_thread(func: object, *args: object, **kwargs: object) -> object:
            recorded.append(func)
            return await real_to_thread(func, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(asyncio, "to_thread", _recording_to_thread)

        repo = await _open(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()

        shutdown_calls = [
            f for f in recorded if getattr(f, "__name__", None) == "shutdown" and getattr(f, "__self__", None)
        ]
        assert shutdown_calls, f"executor.shutdown() was never routed through asyncio.to_thread; saw: {recorded}"
        assert all(isinstance(f.__self__, ProcessPoolExecutor) for f in shutdown_calls)  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def _reset_verify_worker_globals() -> Iterator[None]:
    """``_worker_pool``/``_worker_bucket_cache``/``_worker_store``/
    ``_worker_vault_key`` (this module's own process-global worker state)
    and ``concurrency``'s own persistent ``_worker_runner`` must not leak
    between tests — a real worker process only ever sets these once, per
    its own whole lifetime, but this suite runs every test in the same
    process."""
    yield
    verify_reachable_module._worker_pool = None
    verify_reachable_module._worker_bucket_cache = None
    verify_reachable_module._worker_store = None
    verify_reachable_module._worker_vault_key = None
    if concurrency._worker_runner is not None:
        concurrency.close_worker_loop()


class _LoopCheckingStore:
    """Reproduces ``S3Store._get_client``'s exact hazard
    (``storage/s3.py``) — a network client lazily built and cached on
    first use, bound to whichever event loop happens to be running then —
    without touching ``aioboto3``/``aiohttp``: caches the running loop on
    this store's first call and raises if a later call runs on a
    *different* one, the same observable failure a per-task
    ``asyncio.run()`` worker produces against a real lazily-cached client
    once a second task reuses it."""

    def __init__(self, backing: LocalFsStore) -> None:
        self._backing = backing
        self._bound_loop: asyncio.AbstractEventLoop | None = None

    def _check_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._bound_loop is None:
            self._bound_loop = loop
        elif self._bound_loop is not loop:
            raise RuntimeError("Event loop is closed")

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self._check_loop()
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        self._check_loop()
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        self._check_loop()
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        self._check_loop()
        return await self._backing.listdir(path)


class TestVerifyWorkerLoopReuse:
    """Regression test for the bug ``concurrency.run_in_worker_loop`` fixes:
    a ``ProcessPoolExecutor`` worker handles many buckets over its
    lifetime, and its ``ObjectStore`` (built once per worker process — see
    ``_verify_worker_init``'s own docstring) is meant to survive every one
    of them, including a lazily-cached, loop-bound client
    (``S3Store._get_client``, say).

    Every layer ``_verify_bucket_worker`` calls through
    (``_check_one_bucket_core``/``_open_bucket_or_finding``) converts an
    unexpected exception into a ``Finding`` rather than raising (see each
    one's own docstring) — so pre-fix, this bug never crashed a real
    ``verify --level full`` run the way it crashed ``export``; it silently
    produced a false-positive "unexpected error: Event loop is closed"
    corruption finding for the second (and every later) bucket a worker
    checked instead, arguably worse than a crash. This test asserts on the
    returned findings for that reason, not ``pytest.raises``.

    Exercised in-process — no real subprocess needed, since the bug is
    about ``asyncio.run()``'s own per-call loop, not about multiprocessing
    itself — via ``_LoopCheckingStore``, which reproduces that hazard
    without a real network backend. Two *different* bucket keys are used
    deliberately: the same key twice would hit ``_worker_bucket_cache``'s
    own cache on the second call and never call the store again at all,
    proving nothing about loop reuse."""

    def test_second_bucket_in_the_same_worker_reuses_the_first_ones_loop(self, tmp_path: Path) -> None:
        plaintexts = [b"x" * 4096]
        _write_legacy_bucket(tmp_path / "Pool" / "0" / "0.buk", plaintexts)
        _write_legacy_bucket(tmp_path / "Pool" / "0" / "1.buk", plaintexts)
        store = _LoopCheckingStore(LocalFsStore(tmp_path))
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        verify_reachable_module._worker_pool = pool
        verify_reachable_module._worker_bucket_cache = BucketReaderCache()
        verify_reachable_module._worker_store = store
        verify_reachable_module._worker_vault_key = None

        # First bucket in this "worker": builds the loop-checking store's
        # cached loop.
        findings_a, _ = _verify_bucket_worker((StreamId(0), BucketId(0)))
        # Second bucket, same worker (same process-global state, exactly
        # like a real ProcessPoolExecutor worker handling a second item)
        # — must reuse the same loop, not open a fresh one that orphans
        # the first bucket's cached loop reference. Before this fix, the
        # store's own read raised "Event loop is closed", swallowed into
        # exactly the finding asserted absent below.
        findings_b, _ = _verify_bucket_worker((StreamId(0), BucketId(1)))

        for findings in (findings_a, findings_b):
            assert not any("Event loop is closed" in f.detail for f in findings), findings


class TestVerifyWorkerShutdown:
    """``_verify_worker_init`` registers ``_verify_worker_shutdown`` via
    ``atexit`` as its very last statement; ``_verify_worker_shutdown``
    itself releases an ``AsyncCloseable`` store, tolerating the store's own
    ``aclose()`` raising. See ``test_concurrency.py``'s own module
    docstring for why a real spawned-worker proof that ``atexit`` itself
    fires isn't possible in this test suite — this class covers this
    shutdown function's own logic instead, via direct calls."""

    def test_verify_worker_init_registers_the_shutdown_hook(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_legacy_bucket(tmp_path / "Pool" / "0" / "0.buk", [b"x" * 4096])
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        descriptor = PoolDescriptor.from_pool(pool)
        assert descriptor is not None
        registered: list[object] = []
        monkeypatch.setattr(atexit, "register", registered.append)

        _verify_worker_init(descriptor)

        assert registered == [_verify_worker_shutdown]

    def test_shutdown_acloses_an_asynccloseable_store(self) -> None:
        closed: list[bool] = []

        class _FakeAsyncCloseableStore:
            async def aclose(self) -> None:
                closed.append(True)

        verify_reachable_module._worker_store = _FakeAsyncCloseableStore()  # type: ignore[assignment]

        _verify_worker_shutdown()

        assert closed == [True]

    def test_shutdown_tolerates_the_store_s_aclose_raising(self) -> None:
        class _FailingAsyncCloseableStore:
            async def aclose(self) -> None:
                raise RuntimeError("synthetic aclose failure")

        verify_reachable_module._worker_store = _FailingAsyncCloseableStore()  # type: ignore[assignment]

        _verify_worker_shutdown()  # must not raise despite aclose() failing


class TestCleanupCloseExceptionPriority:
    """``verify_reachable()``'s own ``finally`` block: a ``walker.close()``
    failure must never replace a genuine failure already propagating from
    the walk itself, but must not vanish silently either when nothing else
    went wrong. ``_open()``'s repo has a connection but no
    ``workload_config`` table, so ``workloads()`` raises and is folded into
    a ``Finding`` -- ``pairs`` stays empty and the walk itself does no real
    work, keeping these tests focused on the ``finally`` block alone."""

    async def test_close_failure_propagates_when_the_walk_otherwise_succeeded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def failing_close(self: verify_reachable_module._ReachabilityWalker) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "close", failing_close)

        repo = await _open(tmp_path)
        try:
            with pytest.raises(RuntimeError, match="close failed"):
                await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

    async def test_close_failure_is_suppressed_when_the_walk_already_raised(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def failing_check_all_buckets(
            self: verify_reachable_module._ReachabilityWalker,
        ) -> list[Finding]:
            raise RuntimeError("walk failed")

        async def failing_close(self: verify_reachable_module._ReachabilityWalker) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "check_all_buckets", failing_check_all_buckets)
        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "close", failing_close)

        repo = await _open(tmp_path)
        try:
            with pytest.raises(RuntimeError, match="walk failed"):
                await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

    async def test_close_failure_propagates_even_when_called_from_within_an_unrelated_except_block(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A caller invoking ``verify_reachable()`` from inside its own
        ``except:`` block (handling a completely unrelated error) must not
        make this ``finally`` block think its *own* walk failed --
        ``sys.exc_info()`` would leak that outer, unrelated exception in
        here, which is exactly what ``walk_failed`` (set only by this
        function's own ``except`` clause) avoids."""

        async def failing_close(self: verify_reachable_module._ReachabilityWalker) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "close", failing_close)

        repo = await _open(tmp_path)
        try:
            try:
                raise ValueError("unrelated outer error")
            except ValueError:
                with pytest.raises(RuntimeError, match="close failed"):
                    await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
