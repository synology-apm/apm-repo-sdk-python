"""Unit tests for `synology_apm_repo.sdk.units.verify_reachable` —
composition-stage findings: map/CRC repair propagation and the
composition-record cache's own wiring into the walk.

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
from collections.abc import AsyncIterator
from pathlib import Path
from typing import cast

import pytest
import zstandard

from synology_apm_repo.sdk.asynccache import AsyncKeyedCache
from synology_apm_repo.sdk.dedup.chunk_walk import ChunkPlan
from synology_apm_repo.sdk.dedup.composition_reader import CompositionRecord
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import REDUNDANCY_COVERAGE_COMPOSITION, SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import REDUNDANCY_MAGIC, redundancy_size
from synology_apm_repo.sdk.format.repo_info import MAGIC as REPO_INFO_MAGIC
from synology_apm_repo.sdk.identifiers import BucketId, SessionId, StreamId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units import verify_reachable as verify_reachable_module
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


async def _open(tmp_path: Path) -> DedupRepo:
    _write_repo_info(tmp_path / "repo_info")
    _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


# -- tests ----------------------------------------------------------------


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
        goal of surviving one bad record and still reporting everything
        else."""
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
        has never seen this ``record_key`` yet) calls ``extent.dedup_file.cached_record()``
        directly, ahead of ``plan_chunks_windowed`` -- this must stay inside the
        same ``try``/``except`` as every other failure in this method rather
        than propagate and abort the whole ``verify_reachable()`` run, the same
        catch-and-continue contract ``test_corrupt_individual_chunk_map_entry_is_a_finding_not_a_crash``
        above already covers for a *later* failure in this same block."""
        from synology_apm_repo.sdk.dedup.dedup_file import DedupFile

        async def failing_get_record(self: DedupFile) -> None:
            raise NotFoundError("synthetic record-fetch failure", ref="synthetic")

        monkeypatch.setattr(DedupFile, "cached_record", failing_get_record)

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

    async def test_repair_propagation_survives_a_fully_evicted_composition_record_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same scenario as the sibling test above, but with
        ``_composition_records`` forced to ``maxsize=0`` -- every
        ``resolve()`` immediately evicts what it just cached (see
        ``AsyncKeyedCache``'s own eviction semantics), so version B's
        visit to the record version A already resolved is guaranteed a
        cold miss rather than possibly a hit. The outcome must be
        identical to the unforced case: a cold-refetched
        ``CompositionRecord`` still gets correctly reseeded from
        ``_record_checks``' own (unbounded) ``repaired_map_array``, and
        ``_record_checks`` itself (also unbounded) still stops
        ``_ensure_record_checked`` from re-running the check and emitting
        a second ``Symptom.REPAIRED_VIA_PARITY`` finding."""
        monkeypatch.setattr(verify_reachable_module, "_COMPOSITION_RECORD_CACHE_MAXSIZE", 0)

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
        assert len(repaired_findings) == 1  # still memoized once, even though every cache access was a miss
        bucket_findings = [f for f in findings if f.stage is Stage.BUCKET]
        assert bucket_findings == []  # version B's cold-refetched record was still correctly reseeded

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


class TestCompositionRecordCacheWiring:
    """``_ReachabilityWalker._composition_records`` is constructed as an
    ``AsyncKeyedCache`` bounded to ``_COMPOSITION_RECORD_CACHE_MAXSIZE`` --
    a plain wiring check independent of ``TestMapCrcRepairPropagation``'s
    own functional (reseed-still-correct-under-eviction) coverage above,
    mirroring ``test_dedup_pool.py``'s own ``TestLruEviction`` pattern of
    asserting eviction itself, separately from any one scenario that
    happens to trigger it."""

    async def test_composition_records_is_bounded_to_the_module_constant(self, tmp_path: Path) -> None:
        repo = await _open(tmp_path)
        walker = verify_reachable_module._ReachabilityWalker(repo, VerifyLevel.QUICK)
        try:
            assert isinstance(walker._composition_records, AsyncKeyedCache)
            assert walker._composition_records.maxsize == verify_reachable_module._COMPOSITION_RECORD_CACHE_MAXSIZE
        finally:
            await walker.close()
            await repo.close()

    async def test_composition_records_evicts_least_recently_used(self, tmp_path: Path) -> None:
        repo = await _open(tmp_path)
        walker = verify_reachable_module._ReachabilityWalker(repo, VerifyLevel.QUICK)
        try:
            walker._composition_records = AsyncKeyedCache(maxsize=2)
            key1, key2, key3 = (
                (StreamId(1), SessionId(1), 0),
                (StreamId(2), SessionId(2), 0),
                (StreamId(3), SessionId(3), 0),
            )

            async def fake_get_record(key: tuple[StreamId, SessionId, int]) -> CompositionRecord:
                return cast(CompositionRecord, object())

            first = await walker._composition_records.resolve(key1, fake_get_record)
            await walker._composition_records.resolve(key2, fake_get_record)
            await walker._composition_records.resolve(key3, fake_get_record)  # evicts key1 (oldest)

            assert key1 not in walker._composition_records
            refetched = await walker._composition_records.resolve(key1, fake_get_record)
            assert refetched is not first  # a genuinely fresh fetch, not the stale cached instance
        finally:
            await walker.close()
            await repo.close()
