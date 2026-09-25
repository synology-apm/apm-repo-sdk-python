"""Unit tests for `synology_apm_repo.sdk.units.verify_reachable` —
bucket-stage findings and each workload type's own
`composition_extents_for_version` branch (FS/VM/PCPS/SaaS), including
SaaS resolution-label/superseded-generation handling and caching across
a walker run.

Split from `test_units_verify_reachable.py` for navigability; builder
helpers are duplicated per `tests/CLAUDE.md`'s no-cross-file-import
policy, not because this slice needs a different fixture shape."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import struct
import zlib
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workload_by_id
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import DataCorruptError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS, MODE_VAULT_ENCRYPT
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.crypto import chunk_iv
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.format.repo_info import MAGIC as REPO_INFO_MAGIC
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId, WorkloadId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.device import DeviceProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.verify_extents import _all_children, _annotate_with_saas_resolution
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
    ``copy_target_version`` -- that's the real on-disk shape, not two
    separate files."""
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
    to its own equal-numbered ``stream_version`` -- the real shape left
    behind once older generations are server-side GC'd (only
    ``live_stream_version`` still has a resolvable ``saas_obj``;
    FORMAT-SPEC.md: saas-addressing's progressive-superset property). Returns the
    live generation's own ``saas_obj`` path --
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
        """FULL checks every live chunk's ciphertext CRC32 regardless of
        key availability -- only the decrypt+decompress+SHA-256
        fingerprint half needs a vault key. Proven by
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
        ``NotFoundError``) must not abort ``_vm_extents`` entirely -- distinct
        from the test above, where the composition opens fine and only
        ``content.size`` is ``None`` -- discarding every other, fully-resolvable
        disk's checks along with it. The missing disk gets its
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
        convention, so each becomes its own singleton disk group instead
        of being merged with the other), one fully
        resolvable, one
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
    ``Symptom.MISMATCH`` case: that one is tagged from ``_bucket_claim``'s
    own ``ref`` only, and its ``label`` half feeds ``Progress.detail``
    during the checking phase, never a ``Finding`` field. Tested
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
        (no substitution) is used deliberately -- a multi-generation one
        can't reliably exercise a *specific* version's own annotation
        end-to-end, for the same reason described in this class's
        docstring above."""
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
    """A real-world regression this class guards against: several catalog
    versions in one snapshot map (via ``version_info``) to different,
    increasing ``stream_version``s; only the latest has a resolvable
    ``saas_obj`` -- older generations were server-side GC'd
    (FORMAT-SPEC.md: saas-addressing). Forward-resolution must read
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
        ``latest_complete_version=1`` (simulating crash garbage left
        behind by an incomplete write that was later rolled back) -- must
        not be used as a substitute; the real gap is still reported."""
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
