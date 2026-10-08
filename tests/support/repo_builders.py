"""On-disk writers for synthetic repository pieces -- buckets, compositions,
fingerprint groups, ``db/*`` tables, SaaS version/snapshot dbs -- for tests
that build a repository under ``tmp_path``. The byte layouts come from
``format_builders``; this module fills them with self-consistent content
(compression, encryption, CRCs, fingerprints) and writes them."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import zlib
from collections.abc import Mapping, Sequence
from pathlib import Path

import lz4.block
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from support.format_builders import (
    CHUNK_MAP_KIND_MAPPING,
    bucket_header_bytes,
    chunk_addr_int,
    chunk_crc_store_bytes,
    chunk_map_record_bytes,
    composition_header_bytes,
    encode_size_store,
    inf_header,
    record_head_bytes,
    repo_info_bytes,
    sizestore_region_pad,
)
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import (
    MODE_CHUNK_CRC,
    MODE_COMPRESS,
    MODE_VAULT_ENCRYPT,
    chunk_size_store_tight_length,
)
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.crypto import chunk_iv
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId

_ALLOC_TABLE_OFFSET = 12288


def filler_bucket_bytes(
    entries: list[tuple[CompressType, int]], *, mode: int = MODE_COMPRESS | MODE_CHUNK_CRC
) -> bytes:
    """A structurally valid ``.buk`` whose payloads and trailer are random
    filler: only the SizeStore ``entries`` (``(CompressType, stored_len)``
    per chunk) and the resulting lengths are meaningful."""
    chunk_num = len(entries)
    tight = encode_size_store([(ctype.value, stored_len) for ctype, stored_len in entries])
    header = bucket_header_bytes(mode=mode, chunk_num=chunk_num, chunk_size_crc=zlib.crc32(tight))

    chunk_data = b"".join(
        os.urandom(4096 if ctype is CompressType.NONE else 0 if ctype is CompressType.COMPACTED else stored_len)
        for ctype, stored_len in entries
    )
    non_empty = sum(1 for ctype, _ in entries if ctype is not CompressType.COMPACTED)
    trailer = os.urandom(4 * non_empty + redundancy_size(chunk_size_store_tight_length(chunk_num), 256))
    return header + sizestore_region_pad(tight) + chunk_data + trailer


def uncompressed_bucket_bytes(chunk_num: int) -> bytes:
    """A legacy uncompressed-layout bucket: header (mode 0, no SizeStore)
    followed by ``chunk_num`` random full chunks."""
    return bucket_header_bytes(mode=0, chunk_num=chunk_num) + os.urandom(4096 * chunk_num)


def chunk_it(buf: bytes) -> list[bytes]:
    padded = buf + b"\x00" * (-len(buf) % 4096)
    return [padded[i : i + 4096] for i in range(0, len(padded), 4096)]


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(path)


def version_spec_json(
    start_time: object = None,
    end_time: object = None,
    status: object = None,
    *,
    additional_meta: dict[str, object] | None = None,
) -> str:
    """A minimal real-shaped ``version_spec`` blob: just the
    ``status.start_time``/``end_time``/``status``/``additional_meta`` fields
    the catalog reads (real times are protobuf-JSON int64-as-string). An
    omitted argument leaves its key absent, not zero/empty."""
    status_obj: dict[str, object] = {}
    if start_time is not None:
        status_obj["start_time"] = str(start_time)
    if end_time is not None:
        status_obj["end_time"] = str(end_time)
    if additional_meta is not None:
        status_obj["additional_meta"] = json.dumps(additional_meta)
    if status is not None:
        status_obj["status"] = status
    return json.dumps({"status": status_obj})


def write_bare_connection_config(path: Path, rows: list[tuple[int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, connection_id TEXT)")
    conn.executemany("INSERT INTO connection_config VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def write_bucket(
    path: Path,
    plaintexts: list[bytes],
    *,
    stream_id: int = 0,
    bucket_id: int = 0,
    vault_key: bytes | None = None,
    compress_types: list[CompressType] | None = None,
    chunk_crc_store: bool = True,
    corrupt_chunk_crc_idx: int | None = None,
    mode_extra: int = 0,
) -> None:
    """A real, self-consistent ``.buk`` file: one chunk per plaintext,
    ZSTD-compressed unless ``compress_types`` says otherwise (``NONE``/
    ``LZ4``/``ZSTD`` per chunk; ``COMPACTED`` carries no payload and isn't
    supported), AES-256-CTR encrypted at ``(stream_id, bucket_id, idx)``
    when ``vault_key`` is given.

    ``chunk_crc_store`` writes a real ChunkCrcStore trailer and the header's
    ``crcOfChunkCrc``; ``False`` leaves random filler there instead.
    ``corrupt_chunk_crc_idx`` (requires ``chunk_crc_store``) flips one
    chunk's recorded CRC *before* ``crcOfChunkCrc`` is computed, so only a
    per-chunk ciphertext check catches it. ``mode_extra`` is OR-ed into the
    header mode word.
    """
    if compress_types is None:
        compress_types = [CompressType.ZSTD] * len(plaintexts)
    assert len(compress_types) == len(plaintexts)
    assert chunk_crc_store or corrupt_chunk_crc_idx is None, "corrupt_chunk_crc_idx requires chunk_crc_store"

    compressor = zstandard.ZstdCompressor()
    payloads: list[bytes] = []
    entries: list[tuple[int, int]] = []
    for chunk_idx, (plain, ctype) in enumerate(zip(plaintexts, compress_types, strict=True)):
        if ctype is CompressType.ZSTD:
            compressed = compressor.compress(plain)
        elif ctype is CompressType.LZ4:
            compressed = lz4.block.compress(plain, store_size=False)
        elif ctype is CompressType.NONE:
            compressed = plain
        else:
            raise ValueError(f"write_bucket() doesn't support {ctype}")
        if vault_key is not None:
            addr = ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx))
            encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(chunk_iv(addr))).encryptor()
            payload = encryptor.update(compressed) + encryptor.finalize()
        else:
            payload = compressed
        payloads.append(payload)
        entries.append((ctype.value, 0 if ctype is CompressType.NONE else len(payload)))

    chunk_num = len(plaintexts)
    tight = encode_size_store(entries)
    mode = MODE_COMPRESS | MODE_CHUNK_CRC | (MODE_VAULT_ENCRYPT if vault_key is not None else 0) | mode_extra

    if chunk_crc_store:
        chunk_crcs = [zlib.crc32(p) for p in payloads]
        if corrupt_chunk_crc_idx is not None:
            chunk_crcs[corrupt_chunk_crc_idx] ^= 0xFFFFFFFF
        chunk_crc_bytes = chunk_crc_store_bytes(chunk_crcs)
        crc_of_chunk_crc = zlib.crc32(chunk_crc_bytes)
    else:
        chunk_crc_bytes = os.urandom(4 * chunk_num)
        crc_of_chunk_crc = 0
    header = bucket_header_bytes(
        mode=mode, chunk_num=chunk_num, chunk_size_crc=zlib.crc32(tight), crc_of_chunk_crc=crc_of_chunk_crc
    )

    trailer = chunk_crc_bytes + os.urandom(redundancy_size((chunk_num * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(header + sizestore_region_pad(tight) + b"".join(payloads) + trailer)


def write_composition(
    root: Path, *, stream_id: int, session_id: int, num_chunks: int = 1, file_offset: int = 0
) -> None:
    """One composition chunk file (``c0``) holding a single record: one
    MAPPING entry at ``file_offset`` for ``num_chunks`` chunks from
    ``(stream_id, bucket 0, chunk 0)``."""
    entry = chunk_map_record_bytes(
        kind_value=CHUNK_MAP_KIND_MAPPING,
        file_chunk_idx=file_offset >> 12,
        addr_int=chunk_addr_int(stream_id, 0, 0),
        tail_u32=num_chunks << 16,
    )
    path = root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(composition_header_bytes() + record_head_bytes(map_num=1) + entry)


def write_composition_entries(comp_root: Path, entries: bytes, *, stream_id: int, session_id: int) -> None:
    """One composition chunk file (``c0``) holding a single record of
    ``entries`` (concatenated 20-byte ChunkMapRecords), with a matching
    RecordHead ``map_num``/``map_crc``."""
    record_bytes = record_head_bytes(map_num=len(entries) // 20, map_crc=zlib.crc32(entries) & 0xFFFFFFFF) + entries
    path = comp_root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(composition_header_bytes() + record_bytes)


def write_connection_config(path: Path, rows: list[tuple[int, str, int]]) -> None:
    conn = open_db(path)
    conn.execute(
        "CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, "
        "connection_id TEXT, version_type INTEGER)"
    )
    conn.executemany("INSERT INTO connection_config VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def write_copy_target_version(path: Path, rows: list[tuple[object, ...]]) -> None:
    """Creates the table if absent, then appends ``rows``, so repeated calls
    build up one table."""
    conn = open_db(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS copy_target_version(version_id INTEGER PRIMARY KEY, workload_id INTEGER, "
        "connection_config_id INTEGER, version_uid TEXT, target_type TEXT, target_id TEXT, "
        "saas_stream_uuid TEXT, saas_snapshot_uuid TEXT, saas_version_id INTEGER, deleted INTEGER, "
        "version_spec TEXT)"
    )
    conn.executemany("INSERT INTO copy_target_version VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def write_copy_target_version_db(
    path: Path, *, version_uid: str, object_db_id: str, db_objects: list[tuple[str, str]]
) -> None:
    """A SaaS version's object-name index (``units/saas/object_name_index.py``),
    unencrypted. ``SaasWorkloadProvider``/``TeamsChatProvider`` find their
    service DBs only through it, so a DB a test wants found is registered
    here, not merely written into ``saas_obj``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE copy_target_version(version_uid TEXT PRIMARY KEY, version_spec TEXT)")
    additional_meta = json.dumps(
        {
            "object_db_id": object_db_id,
            "db_object_ids": {"db_objects": [{"name": name, "object_id": object_id} for name, object_id in db_objects]},
        }
    )
    version_spec = json.dumps({"status": {"additional_meta": additional_meta}})
    conn.execute("INSERT INTO copy_target_version VALUES (?, ?)", (version_uid, version_spec))
    conn.commit()
    conn.close()


def write_copy_target_version_meta(path: Path, rows: list[tuple[str, str, list[str], int]]) -> None:
    """Appends like ``write_copy_target_version``; each ``meta_filenames``
    list is JSON-encoded."""
    conn = open_db(path)
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


def write_file_map(path: Path, rows: list[tuple[str, int, int, int, int, int]]) -> None:
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


def write_inf(path: Path, entries: dict[int, tuple[int, int]]) -> None:
    """A fingerprint-group ``.inf`` whose allocation table holds ``entries``:
    bucket index within the group -> (byte offset, record number)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = bytearray(_ALLOC_TABLE_OFFSET + 1024 * 8)
    buf[0:64] = inf_header()
    for idx, (byte_off, rec_num) in entries.items():
        off = _ALLOC_TABLE_OFFSET + idx * 8
        buf[off : off + 4] = (((byte_off // 4096) << 15) | rec_num).to_bytes(4, "big")
    path.write_bytes(bytes(buf))


def write_inf_and_fgp(pool_root: Path, plaintexts: list[bytes], *, wrong_digest_idx: int | None = None) -> None:
    """One ``.inf``/``.fgp`` pair holding every chunk's real SHA-256
    fingerprint for bucket 0, so ``Pool.verify_fingerprints`` passes and a
    FULL verify reports no "no fingerprint data" finding. ``wrong_digest_idx``
    stores a wrong digest for that one chunk."""
    buf = bytearray(_ALLOC_TABLE_OFFSET + 1024 * 8)
    buf[0:64] = inf_header()
    raw_pos = len(plaintexts)  # byte_off=0, rec_num=len(plaintexts)
    buf[_ALLOC_TABLE_OFFSET : _ALLOC_TABLE_OFFSET + 4] = raw_pos.to_bytes(4, "big")
    inf_path = pool_root / "0" / "0.inf"
    inf_path.parent.mkdir(parents=True, exist_ok=True)
    inf_path.write_bytes(bytes(buf))
    digests = [hashlib.sha256(p).digest() for p in plaintexts]
    if wrong_digest_idx is not None:
        digests[wrong_digest_idx] = hashlib.sha256(b"wrong").digest()
    (pool_root / "0" / "0_0.fgp").write_bytes(b"".join(digests))


def write_legacy_bucket(path: Path, plaintexts: list[bytes]) -> None:
    """A legacy uncompressed-layout bucket (no ``MODE_COMPRESS``, no
    SizeStore): every chunk ``CompressType.NONE`` at a fixed 4096-byte
    stride from ``RESERVED_LENG`` (4096)."""
    header = bucket_header_bytes(mode=0, chunk_num=len(plaintexts))  # mode: no MODE_COMPRESS
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(header + b"\x00" * (4096 - 64) + b"".join(plaintexts))


def write_repo_info(path: Path, *, uuid: bytes = b"a" * 16) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(repo_info_bytes(uuid=uuid))


def write_saas_snapshot_db(
    path: Path,
    snapshots: Sequence[tuple[int, str, int, int]] = ((1, "snap-uuid", 3, 1),),
    distribution: Sequence[tuple[int, int, int, int]] = (),
) -> None:
    """A SaaS stream's ``saas_snapshot`` db. ``snapshots``: (snapshot_id,
    snapshot_uuid, first_version_id, stream_version); ``distribution``:
    (offset, length, snapshot_id, version_id)."""
    conn = open_db(path)
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


def write_saas_version_db(
    path: Path,
    versions: Sequence[tuple[int, int, int, int]] = ((1, 3, 1, 0),),
    target_type: str | None = "M365",
    *,
    latest_complete_version: int | None = None,
) -> None:
    """A SaaS stream's ``saas_version`` db. ``versions``: (snapshot_id,
    version_id, stream_version, deleted). ``target_type=None`` leaves
    ``stream_info`` empty. ``latest_complete_version`` defaults to the
    highest ``stream_version`` (the real writer never leaves it behind a row
    it committed); pass it to model a stale value or crash-garbage rows."""
    conn = open_db(path)
    conn.execute(
        "CREATE TABLE version_info(snapshot_id INTEGER, version_id INTEGER, stream_version INTEGER, deleted INTEGER)"
    )
    conn.executemany(
        "INSERT INTO version_info(snapshot_id, version_id, stream_version, deleted) VALUES (?, ?, ?, ?)", versions
    )
    conn.execute("CREATE TABLE stream_info(id INTEGER PRIMARY KEY, target_type TEXT, latest_complete_version INTEGER)")
    if target_type is not None:
        if latest_complete_version is None:
            latest_complete_version = max((v[2] for v in versions), default=None)
        conn.execute(
            "INSERT INTO stream_info(id, target_type, latest_complete_version) VALUES (1, ?, ?)",
            (target_type, latest_complete_version),
        )
    conn.commit()
    conn.close()


def write_target_db_with_version_id(path: Path, version_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_table(id INTEGER PRIMARY KEY, version_id INTEGER, data_format INTEGER, "
        "status INTEGER, folder_name TEXT)"
    )
    conn.execute("INSERT INTO version_table VALUES (1, ?, 1, 1, 'folder')", (version_id,))
    conn.commit()
    conn.close()


def write_vault_encryption_key_db(path: Path, rows: Sequence[tuple[str, str]] = (("NoEncryption", ""),)) -> None:
    """``db/vault_encryption_key``; ``rows``: (user_key_uuid, encrypted_data_key)."""
    conn = open_db(path)
    conn.execute(
        "CREATE TABLE vault_encryption_key(user_key_uuid TEXT UNIQUE NOT NULL, "
        "encrypted_data_key TEXT, crtime DATETIME DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.executemany("INSERT INTO vault_encryption_key(user_key_uuid, encrypted_data_key) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def write_vault_link_key(path: Path, keys: list[str]) -> None:
    conn = open_db(path)
    conn.execute("CREATE TABLE vault_link_key(key TEXT)")
    conn.executemany("INSERT INTO vault_link_key VALUES (?)", [(k,) for k in keys])
    conn.commit()
    conn.close()


def write_workload_config(path: Path, rows: Sequence[tuple[int, str, str, Mapping[str, object]]]) -> None:
    """Appends like ``write_copy_target_version``; each ``workload_spec``
    dict is JSON-encoded."""
    conn = open_db(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS workload_config(workload_id INTEGER PRIMARY KEY, workload_uid TEXT, "
        "workload_type TEXT, workload_spec TEXT)"
    )
    conn.executemany(
        "INSERT INTO workload_config VALUES (?, ?, ?, ?)",
        [(wid, uid, wtype, json.dumps(dict(spec))) for wid, uid, wtype, spec in rows],
    )
    conn.commit()
    conn.close()


def write_copy_target_file(path: Path, rows: list[tuple[int, int]]) -> None:
    """``(version_id, fid)`` rows, appended into the same physical file as
    ``copy_target_version`` (the real on-disk shape)."""
    conn = open_db(path)
    conn.execute("CREATE TABLE IF NOT EXISTS copy_target_file(version_id INTEGER, fid INTEGER)")
    conn.executemany("INSERT INTO copy_target_file VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def write_pcps_file_meta(path: Path, rows: list[tuple[int, str, int | None]]) -> None:
    """PC/PS's ``db/file_meta``, keyed by ``fid``; ``rows``: ``(fid, path,
    file_size)``. The real table has more columns, none read."""
    conn = open_db(path)
    conn.execute("CREATE TABLE file_meta(fid INTEGER PRIMARY KEY, path TEXT, file_size INTEGER)")
    conn.executemany("INSERT INTO file_meta VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def composition_record_bytes(*, map_array: bytes, attr: bytes = b"") -> bytes:
    """One composition record: a RecordHead (Redundancy mode bit set) whose
    ``map_num``/``map_crc``/``attr_leng``/``attr_crc`` match ``map_array``
    and ``attr``, then both, then a random-filler Redundancy trailer of the
    right length."""
    head = record_head_bytes(
        map_num=len(map_array) // 20,
        map_crc=zlib.crc32(map_array),
        mode=0x0001,
        attr_leng=len(attr),
        attr_crc=zlib.crc32(attr),
    )
    return head + map_array + attr + os.urandom(redundancy_size(len(map_array), 8192))
