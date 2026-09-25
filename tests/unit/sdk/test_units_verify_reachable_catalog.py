"""Unit tests for `synology_apm_repo.sdk.units.verify_reachable` —
catalog/version enumeration: orphaned file_map rows, unresolvable
versions, and catalog-enumeration failure, all at the walk's own entry
point into a version before any composition/bucket work happens; plus
full-vs-quick chunk-level coverage, cross-version bucket/chunk-walk
memoization, and FULL's own per-chunk fingerprint check, which do reach
that deeper work.

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
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.chunk_walk import ChunkPlan
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.format.repo_info import MAGIC as REPO_INFO_MAGIC
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units import verify_reachable as verify_reachable_module
from synology_apm_repo.sdk.units.verify_extents import CompositionExtent
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

        import synology_apm_repo.sdk.units.verify_extents as verify_extents_module

        async def fake_fs_extents(repo: DedupRepo, version: Version) -> list[CompositionExtent]:
            raise DataCorruptError("simulated corrupt target.db", spec="test")

        monkeypatch.setattr(verify_extents_module, "_fs_extents", fake_fs_extents)

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
    entirely -- ``Stage.VERSION`` covers failure at the
    workload/connection-enumeration level too, not just a specific
    version's own resolution."""

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
