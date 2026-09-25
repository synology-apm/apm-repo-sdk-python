"""Unit tests for `synology_apm_repo.sdk.units.verify_reachable` —
bucket sizing/progress/finding-ref plumbing, and the verify worker's
own process-pool/executor lifecycle (loop reuse, shutdown, cleanup-vs-
walk-exception ordering).

Split from `test_units_verify_reachable.py` for navigability; builder
helpers are duplicated per `tests/CLAUDE.md`'s no-cross-file-import
policy, not because this slice needs a different fixture shape."""

from __future__ import annotations

import asyncio
import atexit
import hashlib
import json
import os
import sqlite3
import struct
import zlib
from collections.abc import Iterator
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk import concurrency
from synology_apm_repo.sdk.dedup.pool import BucketReader, BucketReaderCache, Pool
from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import Finding, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.format.repo_info import MAGIC as REPO_INFO_MAGIC
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.presentation.progress import Progress
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units import verify_bucket_check as verify_bucket_check_module
from synology_apm_repo.sdk.units import verify_reachable as verify_reachable_module
from synology_apm_repo.sdk.units.verify_bucket_check import (
    _verify_bucket_worker,
    _verify_worker_init,
    _verify_worker_shutdown,
)
from synology_apm_repo.sdk.units.verify_reachable import verify_reachable

_SIZE_STORE_REGION_LEN = 16320  # COMPRESS_RESERVED_LENG(16384) - HEADER_LEN(64)
_ALLOC_TABLE_OFFSET = 12288
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
    """Appends (``CREATE TABLE IF NOT EXISTS``) rather than always
    creating fresh -- a test building more than one row in the same
    repository needs every call after the first to add to the same table."""
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
    """Same append-not-recreate shape as ``_write_copy_target_version`` above,
    for ``copy_target_version_meta``'s own table."""
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
    ``file_size`` for that path — without it, ``dedup_file.size`` stays
    unresolved (``None``), which every ``composition_extents_for_version``
    branch treats as nothing to check rather than an error, so this
    version's extent would come back empty.

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


async def _open(tmp_path: Path) -> DedupRepo:
    _write_repo_info(tmp_path / "repo_info")
    _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


# -- tests ----------------------------------------------------------------


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
    stable total is known. Checking never starts until every version has
    been discovered: a done/total pair against a still-growing "claimed
    so far" total would make the percentage/rate/ETA actively
    misleading, not just approximate.

    The ``verifying`` unit itself depends on ``level``: ``"bytes"`` at
    FULL (each claimed bucket's own real on-disk size, summed), or
    ``"buckets"`` at QUICK (a plain count, no sizing at all) -- QUICK
    never reads chunk content, so a bucket's byte size isn't a
    meaningful throughput signal there, only its count is."""

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
    canonical ``cat:/wl:/ver:`` ref naming that exact version -- for
    dedup'd data shared across versions, that's whichever version's
    check claimed it first, not necessarily the one under test."""

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
    reported -- proves ``_check_one_bucket_core``'s final broad
    ``except Exception`` actually delivers the outcome it exists for: no
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

        import synology_apm_repo.sdk.units.verify_bucket_check as verify_bucket_check_module

        async def flaky_check_bucket_structure(repo: DedupRepo, reader: BucketReader) -> list[Finding]:
            if reader.path.endswith("/1.buk"):
                raise RuntimeError("simulated unexpected failure")
            return []

        monkeypatch.setattr(verify_bucket_check_module, "check_bucket_structure", flaky_check_bucket_structure)

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

        import synology_apm_repo.sdk.units.verify_bucket_check as verify_bucket_check_module

        async def failing_check_bucket_structure(repo: DedupRepo, reader: BucketReader) -> list[Finding]:
            raise NotFoundError("simulated: bucket vanished after its own open", ref=reader.path)

        monkeypatch.setattr(verify_bucket_check_module, "check_bucket_structure", failing_check_bucket_structure)

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
    that whole stretch. This fixture's real ``LocalFsStore`` is
    describable, so this exercises the real multiprocess path, not the
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
    ``_worker_vault_key`` (``verify_bucket_check``'s own process-global
    worker state) and ``concurrency``'s own persistent ``_worker_runner``
    must not leak
    between tests — a real worker process only ever sets these once, per
    its own whole lifetime, but this suite runs every test in the same
    process."""
    yield
    verify_bucket_check_module._worker_pool = None
    verify_bucket_check_module._worker_bucket_cache = None
    verify_bucket_check_module._worker_store = None
    verify_bucket_check_module._worker_vault_key = None
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
    lifetime, and its ``ObjectStore`` (built once by ``_verify_worker_init``,
    which runs once per worker process's whole lifetime, not once per task)
    is meant to survive every one of them, including a lazily-cached,
    loop-bound client (``S3Store._get_client``, say).

    Every layer ``_verify_bucket_worker`` calls through
    (``_check_one_bucket_core``/``_open_bucket_or_finding``) converts an
    unexpected exception into a ``Finding`` rather than raising -- an
    escaped exception would cancel every other in-flight bucket check via
    the caller's own concurrent dispatch — so pre-fix, this bug never
    crashed a real ``verify --level full`` run the way it crashed ``export``; it silently
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
        verify_bucket_check_module._worker_pool = pool
        verify_bucket_check_module._worker_bucket_cache = BucketReaderCache()
        verify_bucket_check_module._worker_store = store
        verify_bucket_check_module._worker_vault_key = None

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
    ``aclose()`` raising. A real spawned-worker proof that ``atexit`` itself
    fires isn't possible in this test suite (a function defined in a test
    file can't be pickled for a ``spawn``-context worker) -- this class
    covers this shutdown function's own logic instead, via direct calls."""

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

        verify_bucket_check_module._worker_store = _FakeAsyncCloseableStore()  # type: ignore[assignment]

        _verify_worker_shutdown()

        assert closed == [True]

    def test_shutdown_tolerates_the_store_s_aclose_raising(self) -> None:
        class _FailingAsyncCloseableStore:
            async def aclose(self) -> None:
                raise RuntimeError("synthetic aclose failure")

        verify_bucket_check_module._worker_store = _FailingAsyncCloseableStore()  # type: ignore[assignment]

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
