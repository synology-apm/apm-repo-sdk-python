"""Unit tests for ``synology_apm_repo.sdk.units.dispatch``'s SaaS
routing (``saas_provider_for``, ``SUPPORTED_SAAS_SUB_TYPES``) —
a full synthetic repository root (same building blocks as
``test_units_saas_calendar.py``), since ``saas_provider_for`` actually
constructs real providers rather than just inspecting types."""

from __future__ import annotations

import dataclasses
import json
import os
import sqlite3
import struct
import tempfile
import zlib
from pathlib import Path
from typing import cast

import pytest
import zstandard

import synology_apm_repo.sdk.units.dispatch as dispatch_module
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import (
    BucketId,
    ChunkIdx,
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamId,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
    WorkloadUid,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.dispatch import SUPPORTED_SAAS_SUB_TYPES, is_supported, saas_provider_for
from synology_apm_repo.sdk.units.saas.provider import CompositeSaasProvider, SaasWorkloadProvider, SharedSaasContext
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider
from synology_apm_repo.sdk.units.saas.teams_chat import TeamsChatProvider

_STREAM_ID = 17
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "dispatch-stream-uuid"


def _write_repo_info(path: Path) -> None:
    payload = json.dumps({"repo_type": 2}).encode("utf-8")
    header = bytearray(64)
    header[0:4] = b"RpiF"
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[20:36] = b"a" * 16
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + payload)


def _write_vault_encryption_key_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE vault_encryption_key(user_key_uuid TEXT UNIQUE, encrypted_data_key TEXT)")
    conn.execute("INSERT INTO vault_encryption_key VALUES ('NoEncryption', '')")
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


def _write_connection_config(path: Path, rows: list[tuple[int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, connection_id TEXT)")
    conn.executemany("INSERT INTO connection_config VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _write_saas_snapshot_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE snapshot_info(snapshot_id INTEGER PRIMARY KEY, snapshot_uuid TEXT, "
        "first_version_id INTEGER, stream_version INTEGER)"
    )
    conn.execute("INSERT INTO snapshot_info VALUES (1, 'snap-uuid', 3, 1)")
    conn.execute(
        "CREATE TABLE snapshot_distribution(offset INTEGER, length INTEGER, snapshot_id INTEGER, version_id INTEGER)"
    )
    conn.commit()
    conn.close()


def _write_saas_version_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_info(snapshot_id INTEGER, version_id INTEGER, stream_version INTEGER, deleted INTEGER)"
    )
    conn.execute("INSERT INTO version_info VALUES (1, 3, 1, 0)")
    conn.execute("CREATE TABLE stream_info(target_type TEXT)")
    conn.execute("INSERT INTO stream_info VALUES ('GW')")
    conn.commit()
    conn.close()


def _write_copy_target_version_db(
    path: Path, *, version_uid: str, object_db_id: str, db_objects: list[tuple[str, str]]
) -> None:
    """The connector's own index bookkeeping
    (``synology_apm_repo.sdk.units.saas.object_name_index``) — every
    ``SaasWorkloadProvider``/``TeamsChatProvider`` construction now
    resolves its service DB(s) *only* through this table, with no
    scan-based fallback, so a fixture repository that wants a table found
    must record it here rather than merely embedding the bytes
    somewhere in ``saas_obj``. Plain, unencrypted JSON — these fixture
    repositories never configure a vault_key, matching every other db this
    file writes."""
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


def _build_object_db(rows: list[tuple[str, int, int]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE object_table(object_id TEXT PRIMARY KEY, offset INTEGER, length INTEGER)")
        conn.executemany("INSERT INTO object_table VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        return path.read_bytes()


def _build_calendar_list_db(calendars: list[tuple[str, str]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "cal.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute("CREATE TABLE calendar_table(calendar_id TEXT PRIMARY KEY, calendar_name TEXT, timezone TEXT)")
        conn.executemany("INSERT INTO calendar_table VALUES (?, ?, 'UTC')", calendars)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_event_db(events: list[tuple[str, str, str, str]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "event.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE calendar_event_table(event_id TEXT PRIMARY KEY, calendar_id TEXT, summary TEXT, "
            "meta_object_id TEXT)"
        )
        conn.executemany("INSERT INTO calendar_event_table VALUES (?, ?, ?, ?)", events)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


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


def _write_bucket(path: Path, plaintexts: list[bytes]) -> None:
    compressor = zstandard.ZstdCompressor()
    payloads = [compressor.compress(p) for p in plaintexts]
    entries = [(CompressType.ZSTD.value, len(p)) for p in payloads]
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    trailer = os.urandom(4 * len(plaintexts) + redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(payloads) + trailer)


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    return (
        bytes([type_byte])
        + file_chunk_idx.to_bytes(7, "big")
        + addr_int.to_bytes(8, "big")
        + tail_u32.to_bytes(4, "big")
    )


def _write_composition(root: Path, *, stream_id: int, session_id: int, num_chunks: int) -> None:
    addr_int = ChunkAddress(StreamId(stream_id), BucketId(0), ChunkIdx(0)).to_int()
    entry = _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value, file_chunk_idx=0, addr_int=addr_int, tail_u32=num_chunks << 16
    )
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = (1).to_bytes(8, "big")
    head[18:20] = (1).to_bytes(2, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    record_bytes = bytes(head) + entry

    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = SUB_FILE_SIZE.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    path = root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + record_bytes)


def _chunk_it(buf: bytes) -> list[bytes]:
    padded = buf + b"\x00" * (-len(buf) % 4096)
    return [padded[i : i + 4096] for i in range(0, len(padded), 4096)]


def _build_empty_saas_repo(tmp_path: Path, *, session_id: int = 20) -> None:
    """A saas_obj with no embedded ObjectDB and no ``copy_target_version``
    index bookkeeping at all (see ``_write_copy_target_version_db``
    for how these fixtures normally record that bookkeeping) — every
    provider's location resolution must find nothing for this version,
    the "never had one" shape
    ``resolve_object_name_index``'s
    own docstring lists as expected-and-safe."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    content = b"\x00" * 4096
    saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, 1, 2)])
    _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=1)
    _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", [content])


def _build_mail_db(rows: list[tuple[str, str, str]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail.db"
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE mail_table(mail_id TEXT PRIMARY KEY, subject TEXT, meta_object_id TEXT, "
            "parent_folder_id TEXT)"
        )
        conn.executemany("INSERT INTO mail_table(mail_id, subject, meta_object_id) VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_mail_and_calendar_repo(tmp_path: Path, *, session_id: int = 22) -> None:
    """Both a (schema-only, no rows — dispatch only cares whether the
    object-name index can find the table at all, see
    ``SaasWorkloadProvider.create()``) ``mail_table`` and a real
    populated calendar service-DB pair exist in the same version — the
    shape ``units/dispatch.py``'s own docstring says is real for M365's
    ``USER_EXCHANGE``/``GROUP_EXCHANGE``: more than one application-layer
    candidate recognizes the same version at once, not as alternatives.

    Also writes real ``copy_target_version`` index bookkeeping naming
    every object below (see ``_write_copy_target_version_db``)."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    mail_db_bytes = _build_mail_db([])
    list_db_bytes = _build_calendar_list_db([("cal-1", "Primary")])
    event_db_bytes = _build_event_db([("event-1", "cal-1", "Meeting", "meta_1")])
    meta_1 = json.dumps(
        {
            "attachment_list": [],
            "client_metadata": {"summary": "Meeting", "start": {"date": "2026-01-01"}, "end": {"date": "2026-01-02"}},
        }
    ).encode()

    payloads = [
        ("mail_svc", mail_db_bytes),
        ("cal_svc", list_db_bytes),
        ("event_svc", event_db_bytes),
        ("meta_1", meta_1),
    ]
    relative_rows = []
    cursor = 0
    content = b""
    for object_id, payload in payloads:
        relative_rows.append((object_id, cursor, len(payload)))
        content += payload
        cursor += len(payload)
    object_db_len = len(_build_object_db(relative_rows))
    absolute_rows = [(oid, off + object_db_len, ln) for oid, off, ln in relative_rows]
    object_db_bytes = _build_object_db(absolute_rows)
    saas_obj_content = object_db_bytes + content
    plaintexts = _chunk_it(saas_obj_content)

    saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, len(plaintexts), 2)])
    _write_composition(
        tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", plaintexts)
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=[("mail_db", "mail_svc"), ("calendar_db", "cal_svc"), ("calendar_event_db", "event_svc")],
    )


def _build_calendar_only_repo(tmp_path: Path, *, session_id: int = 21) -> None:
    """Only a calendar_table/calendar_event_table service DB pair exists
    (named in real ``copy_target_version`` index bookkeeping — see
    ``_write_copy_target_version_db``) — used to prove
    ``saas_provider_for`` tries Mail and Contact first for
    ``"USER_EXCHANGE"`` (both fail, since the object-name index has no
    ``mail_db``/``contact_db`` entry at all for this version) before
    landing on Calendar."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    list_db_bytes = _build_calendar_list_db([("cal-1", "Primary")])
    event_db_bytes = _build_event_db([("event-1", "cal-1", "Meeting", "meta_1")])
    meta_1 = json.dumps(
        {
            "attachment_list": [],
            "client_metadata": {"summary": "Meeting", "start": {"date": "2026-01-01"}, "end": {"date": "2026-01-02"}},
        }
    ).encode()

    payloads = [("cal_svc", list_db_bytes), ("event_svc", event_db_bytes), ("meta_1", meta_1)]
    relative_rows = []
    cursor = 0
    content = b""
    for object_id, payload in payloads:
        relative_rows.append((object_id, cursor, len(payload)))
        content += payload
        cursor += len(payload)
    object_db_len = len(_build_object_db(relative_rows))
    absolute_rows = [(oid, off + object_db_len, ln) for oid, off, ln in relative_rows]
    object_db_bytes = _build_object_db(absolute_rows)
    saas_obj_content = object_db_bytes + content
    plaintexts = _chunk_it(saas_obj_content)

    saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, len(plaintexts), 2)])
    _write_composition(
        tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", plaintexts)
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=[("calendar_db", "cal_svc"), ("calendar_event_db", "event_svc")],
    )


def _build_contact_db(contacts: list[tuple[str, str, str, str]]) -> bytes:
    """GWS-shaped ``contact_table`` — no folder column (see
    ``units/saas/contact.py``'s own module docstring); each entry is
    ``(contact_id, first_name, last_name, meta_object_id)``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "contact.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE contact_table(contact_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, "
            "meta_object_id TEXT)"
        )
        conn.executemany("INSERT INTO contact_table VALUES (?, ?, ?, ?)", contacts)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_item_service_db(*, root_folder_id: str, items: list[tuple[str, str, str, int, int, str, str]]) -> bytes:
    """Drive's own ``config_table``/``item_table`` pair — each item is
    ``(item_id, name, parent_folder_id, type, size, content_object_id,
    hash)``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "item.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute("INSERT INTO config_table VALUES ('root_folder_id', ?)", (root_folder_id,))
        conn.execute(
            "CREATE TABLE item_table(item_id TEXT PRIMARY KEY, name TEXT, parent_folder_id TEXT, "
            "type INTEGER, size INTEGER, mtime INTEGER, meta_object_id TEXT, content_object_id TEXT, hash TEXT)"
        )
        for item_id, name, parent_folder_id, item_type, size, content_object_id, item_hash in items:
            conn.execute(
                "INSERT INTO item_table(item_id, name, parent_folder_id, type, size, mtime, "
                "meta_object_id, content_object_id, hash) VALUES (?, ?, ?, ?, ?, 0, '', ?, ?)",
                (item_id, name, parent_folder_id, item_type, size, content_object_id, item_hash),
            )
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_list_version_db(lists: list[tuple[str, str, str, int, str]]) -> bytes:
    """Site's own ``list_version_table`` — each entry is ``(list_id,
    list_title, meta_object_id, list_type, root_folder_id)``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "list.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE list_version_table(list_id TEXT PRIMARY KEY, list_title TEXT, "
            "meta_object_id TEXT, list_type INTEGER, root_folder_id TEXT)"
        )
        conn.executemany("INSERT INTO list_version_table VALUES (?, ?, ?, ?, ?)", lists)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_item_version_db(items: list[tuple[str, str, str, str, str, str, str, str, str]]) -> bytes:
    """Site's own ``item_version_table`` — each entry is ``(item_id,
    list_id, file_id, parent_folder_id, title, item_type, meta_object_id,
    url_path, value1)``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "item.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE item_version_table(item_id TEXT, list_id TEXT, file_id TEXT, parent_folder_id TEXT, "
            "title TEXT, item_type TEXT, meta_object_id TEXT, url_path TEXT, value1 TEXT)"
        )
        conn.executemany("INSERT INTO item_version_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", items)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_index_json(entries: list[tuple[str, str]]) -> bytes:
    """``entries``: (name, object_id) — the shape
    ``units/saas/services.py``'s own index sniffing recognizes."""
    return json.dumps({"version": 1, "db_objects": [{"name": n, "object_id": o} for n, o in entries]}).encode()


def _build_channel_list_db(channels: list[tuple[str, str]]) -> bytes:
    """Teams' own ``channel_info_table`` — each entry is ``(channel_id,
    name)``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "chan.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE channel_info_table(row_id INTEGER, channel_id TEXT PRIMARY KEY, name TEXT, "
            "description TEXT, metadata TEXT, channel_type TEXT, create_time INTEGER)"
        )
        conn.executemany(
            "INSERT INTO channel_info_table(row_id, channel_id, name) VALUES (?, ?, ?)",
            [(i + 1, cid, name) for i, (cid, name) in enumerate(channels)],
        )
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_chat_list_db(chats: list[tuple[str, str]]) -> bytes:
    """Chat's own ``chat_info_table`` — each entry is ``(chat_id,
    topic)``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "chat.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT)")
        conn.executemany("INSERT INTO chat_info_table(chat_id, topic) VALUES (?, ?)", chats)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _write_single_object_saas_repo(
    tmp_path: Path, *, session_id: int, payloads: list[tuple[str, bytes]], db_objects: list[tuple[str, str]]
) -> None:
    """Shared boilerplate behind every single-``saas_obj`` fixture repository
    below: repo_info/vault key/connection config/snapshot+version dbs,
    one compression bucket holding every ``payloads`` entry back-to-back
    behind one embedded ObjectDB, and ``copy_target_version`` naming
    ``db_objects`` (see ``_write_copy_target_version_db``)."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    relative_rows = []
    cursor = 0
    content = b""
    for object_id, payload in payloads:
        relative_rows.append((object_id, cursor, len(payload)))
        content += payload
        cursor += len(payload)
    object_db_len = len(_build_object_db(relative_rows))
    absolute_rows = [(oid, off + object_db_len, ln) for oid, off, ln in relative_rows]
    object_db_bytes = _build_object_db(absolute_rows)
    saas_obj_content = object_db_bytes + content
    plaintexts = _chunk_it(saas_obj_content)

    saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, len(plaintexts), 2)])
    _write_composition(
        tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", plaintexts)
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=db_objects,
    )


def _build_contact_repo(tmp_path: Path, *, session_id: int = 23) -> None:
    contact_db_bytes = _build_contact_db([("contact-1", "Grace", "Hopper", "meta_1")])
    meta_1 = json.dumps(
        {"version": "2.0", "client_metadata": {"names": [{"givenName": "Grace", "familyName": "Hopper"}]}}
    ).encode()
    _write_single_object_saas_repo(
        tmp_path,
        session_id=session_id,
        payloads=[("contact_svc", contact_db_bytes), ("meta_1", meta_1)],
        db_objects=[("contact_db", "contact_svc")],
    )


def _build_drive_repo(tmp_path: Path, *, session_id: int = 24) -> None:
    item_db_bytes = _build_item_service_db(
        root_folder_id="root-id", items=[("item-a", "file-a.txt", "root-id", 1, 4, "content_a", "hash-a")]
    )
    _write_single_object_saas_repo(
        tmp_path,
        session_id=session_id,
        payloads=[("drive_svc", item_db_bytes), ("content_a", b"data")],
        db_objects=[("drive_db", "drive_svc")],
    )


def _build_site_repo(tmp_path: Path, *, session_id: int = 25) -> None:
    list_db_bytes = _build_list_version_db([("list-1", "Tasks", "meta_list_1", 0, "")])
    item_db_bytes = _build_item_version_db([("1", "list-1", "", "", "Task A", "0", "meta_item_1", "", "")])
    meta_list_1 = b'{"version": "1.0", "metadata": {}, "fields": {}, "views": {}}'
    meta_item_1 = json.dumps({"version": "1.0", "values": {"Title": "Task A"}, "content_list": []}).encode()
    _write_single_object_saas_repo(
        tmp_path,
        session_id=session_id,
        payloads=[
            ("list_svc", list_db_bytes),
            ("item_svc", item_db_bytes),
            ("meta_list_1", meta_list_1),
            ("meta_item_1", meta_item_1),
        ],
        db_objects=[("site_list_db", "list_svc"), ("site_item_db", "item_svc")],
    )


def _build_teams_channel_repo(tmp_path: Path, *, session_id: int = 26) -> None:
    list_db_bytes = _build_channel_list_db([("chan-a", "General")])
    index_bytes = _build_index_json([("teams_channel_db", "list_db")])
    _write_single_object_saas_repo(
        tmp_path,
        session_id=session_id,
        payloads=[("list_db", list_db_bytes), ("index", index_bytes)],
        db_objects=[("db_infos_in_snapshot", "index")],
    )


def _build_teams_chat_repo(tmp_path: Path, *, session_id: int = 27) -> None:
    list_db_bytes = _build_chat_list_db([("chat-a", "Project Sync")])
    index_bytes = _build_index_json([("chat_db", "list_db")])
    _write_single_object_saas_repo(
        tmp_path,
        session_id=session_id,
        payloads=[("list_db", list_db_bytes), ("index", index_bytes)],
        db_objects=[("db_infos_in_snapshot", "index")],
    )


def _build_mail_only_repo(tmp_path: Path, *, session_id: int = 28) -> None:
    mail_db_bytes = _build_mail_db([("mail-1", "Hello", "meta_1")])
    meta_1 = b'{"content_list": []}'
    _write_single_object_saas_repo(
        tmp_path,
        session_id=session_id,
        payloads=[("mail_svc", mail_db_bytes), ("meta_1", meta_1)],
        db_objects=[("mail_db", "mail_svc")],
    )


def _version() -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-dispatch"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(_CCID),
        target_type="GW",
        target_id=TargetId(_STREAM_UUID),
        saas_stream_uuid=StreamUuid(_STREAM_UUID),
        saas_snapshot_uuid=SnapshotUuid("snap-uuid"),
        saas_version_id=SaasVersionId(3),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


def _workload(sub_type: str | None) -> Workload:
    return Workload(
        workload_id=WorkloadId(1),
        workload_uid=WorkloadUid("wuid"),
        workload_type="GW",
        sub_type=sub_type,
        display_name="test workload",
        subtitle=None,
        spec={},
    )


async def _open_repo(tmp_path: Path) -> DedupRepo:
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


class TestSupportedSaasSubTypes:
    def test_contains_the_expected_sub_types(self) -> None:
        assert {
            "MAIL",
            "CONTACT",
            "CALENDAR",
            "DRIVE",
            "USER_DRIVE",
            "SITE",
            "USER_EXCHANGE",
            "TEAMS",
            "USER_CHAT",
            "TEAM_DRIVE",
            "GROUP_EXCHANGE",
        } == SUPPORTED_SAAS_SUB_TYPES

    def test_genuinely_unrecognized_sub_types_are_excluded(self) -> None:
        # A made-up sub_type this connector has never heard of — not to
        # be confused with TEAM_DRIVE/GROUP_EXCHANGE, which are real,
        # supported sub_types (DriveProvider / Mail+Contact+CalendarProvider;
        # see units/dispatch.py's own comment).
        assert "SOME_FUTURE_CONNECTOR_TYPE" not in SUPPORTED_SAAS_SUB_TYPES


class TestIsSupported:
    """``is_supported()`` is the one plain, no-I/O check
    ``api.repository.Repository.workload_is_supported()`` wraps for the CLI's
    ``doctor`` command — see that method's own docstring for why nothing
    outside ``sdk.api``/``sdk.units`` should import
    ``SUPPORTED_TARGET_TYPES``/``SUPPORTED_SAAS_SUB_TYPES`` directly."""

    @pytest.mark.parametrize("workload_type", ["VM", "PC", "PS", "FS"])
    def test_device_and_fs_workload_types_are_supported_regardless_of_sub_type(self, workload_type: str) -> None:
        wl = Workload(
            workload_id=WorkloadId(1),
            workload_uid=WorkloadUid("wuid"),
            workload_type=workload_type,
            sub_type=None,
            display_name="test workload",
            subtitle=None,
            spec={},
        )
        assert is_supported(wl) is True

    def test_recognized_saas_sub_type_is_supported(self) -> None:
        assert is_supported(_workload("MAIL")) is True

    def test_unrecognized_saas_sub_type_is_not_supported(self) -> None:
        assert is_supported(_workload("SOME_FUTURE_CONNECTOR_TYPE")) is False

    def test_saas_workload_with_no_sub_type_at_all_is_not_supported(self) -> None:
        assert is_supported(_workload(None)) is False


class TestSaasProviderForDegradation:
    @pytest.mark.parametrize(
        "sub_type",
        [
            "CALENDAR",
            "TEAMS",
            "SOME_FUTURE_CONNECTOR_TYPE",  # unrecognized — degrades without attempting anything
            "TEAM_DRIVE",  # a real, now-supported sub_type (see TestSupportedSaasSubTypes) —
            # this is the ordinary per-version degradation every other recognized
            # sub_type already gets when its own service DB isn't found, not a
            # TEAM_DRIVE-specific gap.
            "GROUP_EXCHANGE",
            None,
            "CONTACT",
            "DRIVE",
            "USER_DRIVE",
            "SITE",
            "USER_CHAT",
            "MAIL",
        ],
    )
    async def test_sub_type_with_no_matching_service_db_degrades_to_raw(
        self, tmp_path: Path, sub_type: str | None
    ) -> None:
        _build_empty_saas_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload(sub_type), _version()) as provider,
        ):
            assert isinstance(provider, RawObjectProvider)


class TestSaasProviderForUserExchangeCandidates:
    """``MailProvider``/``ContactProvider``/``CalendarProvider`` are
    constructor-style factory *functions* over one shared
    ``SaasWorkloadProvider`` class — ``isinstance(provider,
    CalendarProvider)`` isn't meaningful (``CalendarProvider`` isn't a
    type). ``root().name`` is the black-box signal these tests
    actually care about: did dispatch land on the Calendar *config*
    specifically, not just "some SaaS provider" (which
    ``isinstance(provider, SaasWorkloadProvider)`` alone couldn't
    distinguish either)."""

    async def test_user_exchange_tries_mail_and_contact_before_landing_on_calendar(self, tmp_path: Path) -> None:
        _build_calendar_only_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version()) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Calendars"

    async def test_direct_calendar_sub_type_also_resolves_to_calendar_provider(self, tmp_path: Path) -> None:
        _build_calendar_only_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("CALENDAR"), _version()) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Calendars"

    async def test_group_exchange_tries_mail_and_contact_before_landing_on_calendar(self, tmp_path: Path) -> None:
        # GROUP_EXCHANGE's candidate tuple is the same (MailProvider,
        # ContactProvider, CalendarProvider) order as USER_EXCHANGE's
        # above — same shared M365 service-DB schema, so the same
        # trial-order behavior applies unchanged.
        _build_calendar_only_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("GROUP_EXCHANGE"), _version()) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Calendars"


class TestSaasProviderForUserExchangeMultipleMatches:
    """When more than one candidate recognizes the same version — the
    real shape a real M365 ``USER_EXCHANGE`` account has (Mail *and*
    Contact *and* Calendar all present at once) — dispatch must not
    silently keep only the first and discard the rest."""

    async def test_mail_and_calendar_both_present_wraps_in_composite(self, tmp_path: Path) -> None:
        _build_mail_and_calendar_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version()) as provider,
        ):
            assert isinstance(provider, CompositeSaasProvider)
            groups = await provider.children(provider.root())
            assert {g.name for g in groups} == {"Mail", "Calendars"}

    async def test_exactly_one_match_is_returned_unwrapped(self, tmp_path: Path) -> None:
        # Single-match behavior (see TestSaasProviderForUserExchangeCandidates
        # above) stays unwrapped — composite wrapping is for
        # genuinely-plural matches only.
        _build_calendar_only_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version()) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert not isinstance(provider, CompositeSaasProvider)

    async def test_unit_on_the_composite_root_raises(self, tmp_path: Path) -> None:
        _build_mail_and_calendar_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version()) as provider,
        ):
            assert isinstance(provider, CompositeSaasProvider)
            with pytest.raises(ValueError, match="not a restorable unit"):
                await provider.unit(provider.root())

    async def test_unit_on_a_real_leaf_delegates_to_its_own_sub_provider(self, tmp_path: Path) -> None:
        """``unit()`` on an actual (non-root) leaf -- the composite's own
        delegation path, distinct from ``test_unit_on_the_composite_root_
        raises`` above (which only exercises the "no key at all" early
        raise, never reaching the tagged-sub-provider dispatch)."""
        _build_mail_and_calendar_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version()) as provider,
        ):
            assert isinstance(provider, CompositeSaasProvider)
            [calendar_group] = [g for g in await provider.children(provider.root()) if g.name == "Calendars"]
            [calendar] = await provider.children(calendar_group)
            [event] = await provider.children(calendar)
            unit = await provider.unit(event)
            data = await unit.open().read()
            assert b"Meeting" in data

    async def test_unexpected_failure_after_a_prior_candidate_succeeded_closes_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test: Mail (the first USER_EXCHANGE candidate) builds
        successfully before Calendar raises something other than the
        routine ``UnsupportedDataFormatError`` degrade signal — the already-built
        Mail provider must not leak; nothing else tracks it until
        saas_provider_for actually returns. Spies on the one Mail instance
        specifically (an instance-attribute override, found first by
        ``getattr(provider, "close", None)``) rather than the whole
        ``SaasWorkloadProvider`` class, since Contact — the middle
        candidate — legitimately self-closes on its own routine
        ``UnsupportedDataFormatError`` miss (``SaasWorkloadProvider.create()``'s
        own, pre-existing cleanup), which would otherwise be
        indistinguishable from the leak this test targets."""
        _build_mail_and_calendar_repo(tmp_path)

        async def raising_candidate(
            repo: DedupRepo, version: Version, *, shared: SharedSaasContext | None = None
        ) -> SaasWorkloadProvider:
            raise RuntimeError("synthetic unexpected failure")

        candidates = dispatch_module._SAAS_SUB_TYPE_CANDIDATES["USER_EXCHANGE"]
        mail_factory = next(factory for tag, factory in candidates if tag == "mail")
        mail_close_calls: list[SaasWorkloadProvider] = []

        async def spying_mail(
            repo: DedupRepo, version: Version, *, shared: SharedSaasContext | None = None
        ) -> SaasWorkloadProvider:
            instance = cast(SaasWorkloadProvider, await mail_factory(repo, version, shared=shared))
            original_close = instance.close

            async def spy_close() -> None:
                mail_close_calls.append(instance)
                await original_close()

            instance.close = spy_close  # type: ignore[method-assign]
            return instance

        patched = tuple(
            (tag, spying_mail if tag == "mail" else raising_candidate if tag == "calendar" else factory)
            for tag, factory in candidates
        )
        monkeypatch.setitem(dispatch_module._SAAS_SUB_TYPE_CANDIDATES, "USER_EXCHANGE", patched)

        async with await _open_repo(tmp_path) as repo:
            with pytest.raises(RuntimeError, match="synthetic unexpected failure"):
                await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version())
            assert len(mail_close_calls) == 1

    async def test_children_of_a_node_with_an_unrecognized_tag_is_empty(self, tmp_path: Path) -> None:
        # A pasted canonical ref naming a sub-provider tag this composite
        # was never actually built with (or one from a stale/differently
        # shaped snapshot) -- not this provider's job to raise, just
        # report nothing there.
        _build_mail_and_calendar_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version()) as provider,
        ):
            assert isinstance(provider, CompositeSaasProvider)
            [mail_group] = [g for g in await provider.children(provider.root()) if g.name == "Mail"]
            phantom = dataclasses.replace(mail_group, attrs={**mail_group.attrs, "key": ("no-such-tag", "x")})
            assert await provider.children(phantom) == []


class TestSaasProviderForSingleCandidateSubTypes:
    """Every remaining single-candidate ``sub_type`` in
    ``_SAAS_SUB_TYPE_CANDIDATES`` gets its own resolution proof here,
    mirroring ``TestSaasProviderForUserExchangeCandidates``'s own
    ``root().name`` pattern for CALENDAR — a real match must land on the
    *right* provider, not just "some SaaS provider."."""

    async def test_contact_sub_type_resolves_to_contact_provider(self, tmp_path: Path) -> None:
        _build_contact_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("CONTACT"), _version()) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Contacts"

    @pytest.mark.parametrize("sub_type", ["DRIVE", "USER_DRIVE", "TEAM_DRIVE"])
    async def test_drive_family_sub_type_resolves_to_drive_provider(self, tmp_path: Path, sub_type: str) -> None:
        # TEAM_DRIVE complements TestSaasProviderForDegradation's existing
        # degradation-only coverage for it — a real "drive_db" match here
        # proves this sub_type also resolves to DriveProvider, not just
        # that it degrades when unmatched.
        _build_drive_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload(sub_type), _version()) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "/"

    async def test_site_sub_type_resolves_to_site_provider(self, tmp_path: Path) -> None:
        _build_site_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("SITE"), _version()) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Lists"

    async def test_mail_sub_type_alone_resolves_to_mail_provider(self, tmp_path: Path) -> None:
        # MAIL is otherwise only exercised as half of the USER_EXCHANGE
        # composite (TestSaasProviderForUserExchangeMultipleMatches) —
        # this proves it resolves correctly on its own too.
        _build_mail_only_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("MAIL"), _version()) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Mail"

    async def test_teams_sub_type_resolves_to_teams_chat_provider(self, tmp_path: Path) -> None:
        # Complements TestSaasProviderForDegradation's existing
        # degradation-only coverage for TEAMS — a real channel index
        # match proves this sub_type also resolves to TeamsChatProvider.
        _build_teams_channel_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("TEAMS"), _version()) as provider,
        ):
            assert isinstance(provider, TeamsChatProvider)
            assert provider.root().name == "Channels"

    async def test_user_chat_sub_type_resolves_to_teams_chat_provider(self, tmp_path: Path) -> None:
        _build_teams_chat_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            await saas_provider_for(repo, _workload("USER_CHAT"), _version()) as provider,
        ):
            assert isinstance(provider, TeamsChatProvider)
            assert provider.root().name == "Chats"
