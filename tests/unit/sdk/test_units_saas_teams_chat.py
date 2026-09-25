"""Unit tests for ``synology_apm_repo.sdk.units.saas.teams_chat`` — a
full synthetic repository root (same building blocks as
``test_units_saas_calendar.py``/``test_units_dispatch_saas.py``), with a
hand-built index object plus one channel-list DB and per-channel message
DBs embedded in its ``saas_obj`` content (see
``tests/integration/sdk/test_units_saas_teams_chat.py`` for the
real-apv-sample-1 cross-check)."""

from __future__ import annotations

import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import pytest
import zstandard

from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError, UnsupportedDataFormatError
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
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.table import Table
from synology_apm_repo.sdk.units.base import Node, UnitKind, mtime_from_epoch
from synology_apm_repo.sdk.units.content.saas_teams_chat import (
    _parse_json_object,
    _RenderedMessage,
    _reply_note_html,
    render_channel_html,
)
from synology_apm_repo.sdk.units.saas import teams_chat as teams_chat_module
from synology_apm_repo.sdk.units.saas.object_name_index import ObjectNameIndex
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.services import IndexEntry, ServiceKind, SniffResult
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.saas.teams_chat import (
    TeamsChatProvider,
    _channel_info,
    _chat_display_name_from_members,
    _chat_labels,
    _is_container,
    _mtime_attr,
    _owning_account_email,
    _resolve_message_index,
    _TeamsEntityFlatTree,
)

_STREAM_ID = 29
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "teams-stream-uuid"


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


def _write_workload_config(path: Path, rows: list[tuple[int, str]]) -> None:
    """``rows``: ``(workload_id, workload_spec)``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE workload_config(workload_id INTEGER PRIMARY KEY, workload_spec TEXT)")
    conn.executemany("INSERT INTO workload_config VALUES (?, ?)", rows)
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
    conn.execute("INSERT INTO stream_info VALUES ('M365')")
    conn.commit()
    conn.close()


def _write_copy_target_version_db(path: Path, *, version_uid: str, object_db_id: str, index_object_id: str) -> None:
    """The connector's own index bookkeeping
    (``synology_apm_repo.sdk.units.saas.object_name_index``) — Teams/Chat's
    one ``db_objects`` entry is always named ``"db_infos_in_snapshot"``
    and points *at the INDEX object itself* (module docstring), unlike
    every other SaaS provider's per-table naming. No scan-based
    fallback exists, so a fixture repository that wants its index found must
    record this."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE copy_target_version(version_uid TEXT PRIMARY KEY, version_spec TEXT)")
    additional_meta = json.dumps(
        {
            "object_db_id": object_db_id,
            "db_object_ids": {"db_objects": [{"name": "db_infos_in_snapshot", "object_id": index_object_id}]},
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


def _build_channel_list_db(channels: list[tuple[str, str]]) -> bytes:
    """``channels``: (channel_id, name) — the real ``channel_info_table``
    shape (module docstring)."""
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


def _build_message_db(messages: list[tuple[str, str]], *, stickers: dict[str, dict[str, str]] | None = None) -> bytes:
    """``messages``: (sender display name, content_preview). Builds
    ``author``/``metadata`` as real ``msg_info_table`` rows actually are:
    both are themselves JSON strings, not plain strings, inside a real
    row — so tests exercise the real parsing path, not just its
    fallback. ``stickers``, when given, also creates a real
    ``sticker_info_table`` (``msg_id -> {url: base64_content}``) so a test
    can exercise ``_read_stickers``' own table-reading path end to end,
    not just ``render_channel_html``'s formatting of an already-built
    dict."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "msg.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE msg_info_table(row_id INTEGER PRIMARY KEY, msg_id TEXT, author TEXT, "
            "create_time INTEGER, content_preview TEXT, metadata TEXT, is_sys_message INTEGER, reply_to_id TEXT)"
        )
        base_time = 1700000000
        rows = []
        for i, (sender, preview) in enumerate(messages):
            author = json.dumps({"email": "", "id": "", "name": sender, "tenant_id": ""})
            metadata = json.dumps(
                {
                    "attachments": [],
                    "body": {"content": preview, "contentType": "text"},
                    "createdDateTime": f"2023-11-14T22:{13 + i:02d}:20.{i:03d}Z",
                    "from": {"user": {"displayName": sender}},
                }
            )
            rows.append((str(i), author, base_time + i, preview, metadata))
        conn.executemany(
            "INSERT INTO msg_info_table(msg_id, author, create_time, content_preview, metadata, "
            "is_sys_message) VALUES (?, ?, ?, ?, ?, 0)",
            rows,
        )
        if stickers:
            conn.execute("CREATE TABLE sticker_info_table(msg_id TEXT, url TEXT, base64_content TEXT)")
            conn.executemany(
                "INSERT INTO sticker_info_table VALUES (?, ?, ?)",
                [(msg_id, url, content) for msg_id, by_url in stickers.items() for url, content in by_url.items()],
            )
        conn.commit()
        conn.close()
        return path.read_bytes()


def _build_message_db_compressed(messages: list[tuple[str, str]]) -> bytes:
    return zstandard.ZstdCompressor().compress(_build_message_db(messages))


def _build_chat_list_db(chats: list[tuple[str, str]], id_col: str, label_col: str) -> bytes:
    """A ``chat_info_table`` with caller-chosen column names — used to
    exercise ``teams_chat._chat_labels``'s best-effort column
    matching (unconfirmed schema, module docstring)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "chat.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(f"CREATE TABLE chat_info_table({id_col} TEXT PRIMARY KEY, {label_col} TEXT)")
        conn.executemany(f"INSERT INTO chat_info_table({id_col}, {label_col}) VALUES (?, ?)", chats)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_index_json(entries: list[tuple[str, str]]) -> bytes:
    """``entries``: (name, object_id) — the exact shape ``services.py``
    recognizes (its own ``_index_entries`` helper)."""
    return json.dumps({"version": 1, "db_objects": [{"name": n, "object_id": o} for n, o in entries]}).encode()


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


def _build_empty_saas_repo(tmp_path: Path, *, session_id: int = 30) -> None:
    """A saas_obj with no embedded ObjectDB at all."""
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


def _build_teams_repo(
    tmp_path: Path,
    *,
    list_db_bytes: bytes,
    list_db_name: str,
    entries: list[tuple[str, bytes]],
    session_id: int = 31,
) -> None:
    """``entries``: (channel_or_chat_id, message_db_compressed_bytes).
    Assembles ``index -> list_db -> message_db*`` in exactly that byte
    order (order doesn't matter to the provider — it locates everything
    by object_id, not position — but matches the real sample's own
    ordering, index last, for realism)."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    index_bytes = _build_index_json([(list_db_name, "list_db"), *[(cid, f"msg_{cid}") for cid, _ in entries]])
    payloads = [("list_db", list_db_bytes), *[(f"msg_{cid}", data) for cid, data in entries], ("index", index_bytes)]

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
        index_object_id="index",
    )


def _version() -> Version:
    return Version(
        version_id=VersionId(71),
        version_uid=VersionUid("vuid-teams"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(_CCID),
        target_type="M365",
        target_id=TargetId(_STREAM_UUID),
        saas_stream_uuid=StreamUuid(_STREAM_UUID),
        saas_snapshot_uuid=SnapshotUuid("snap-uuid"),
        saas_version_id=SaasVersionId(3),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


@pytest.fixture
async def channel_provider(tmp_path: Path) -> AsyncIterator[TeamsChatProvider]:
    _build_teams_repo(
        tmp_path,
        list_db_bytes=_build_channel_list_db([("chan-a", "Alpha"), ("chan-b", "Beta")]),
        list_db_name="teams_channel_db",
        entries=[
            ("chan-a", _build_message_db_compressed([("Alice", "hi from alpha")])),
            ("chan-b", _build_message_db_compressed([("Bob", "hi from beta")])),
        ],
    )
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
        try:
            yield provider
        finally:
            await provider.close()


async def _channels_of(provider: TeamsChatProvider) -> list[Node]:
    """Every real channel across the Standard/Private/Shared synthetic
    level ``children(root())`` inserts — every channel built by this
    file's own fixtures defaults to Standard (none sets
    ``channel_type``), so this is normally just that one category's own
    children, but written generically over however many categories are
    actually present."""
    channels: list[Node] = []
    for category in await provider.children(provider.root()):
        channels.extend(await provider.children(category))
    return channels


def _row(**kwargs: object) -> dict[str, object]:
    base: dict[str, object] = {
        "author": None,
        "create_time": None,
        "content_preview": None,
        "metadata": None,
        "is_sys_message": 0,
        "is_deleted": 0,
        "reply_to_id": None,
        "msg_id": None,
    }
    base.update(kwargs)
    return base


class TestRenderChannelHtml:
    """Direct tests of the pure function (no I/O — same testability as
    this project's Codec Layer modules), independent of the provider/DB
    plumbing ``TestChannelUnit`` exercises end-to-end."""

    def test_basic_message_shows_sender_and_content(self) -> None:
        row = _row(
            metadata=json.dumps(
                {"from": {"user": {"displayName": "Alice"}}, "body": {"content": "hello", "contentType": "text"}}
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "Alice" in page
        assert "hello" in page

    def test_author_column_is_the_fallback_sender_when_metadata_from_is_absent(self) -> None:
        row = _row(author=json.dumps({"name": "Bob"}), content_preview="hi")
        page = render_channel_html([row], channel_name="General")
        assert "Bob" in page
        assert "hi" in page

    def test_unknown_sender_when_neither_source_has_a_name(self) -> None:
        row = _row(content_preview="hi")
        page = render_channel_html([row], channel_name="General")
        assert "(unknown sender)" in page

    def test_system_message_shows_generic_label_not_the_literal_tag(self) -> None:
        row = _row(
            is_sys_message=1,
            metadata=json.dumps({"body": {"content": "<systemEventMessage/>", "contentType": "html"}}),
        )
        page = render_channel_html([row], channel_name="General")
        assert "(system event)" in page
        assert "systemEventMessage" not in page

    def test_content_preview_used_when_metadata_body_is_absent(self) -> None:
        row = _row(content_preview="fallback text")
        page = render_channel_html([row], channel_name="General")
        assert "fallback text" in page

    def test_attachment_with_inline_content_is_shown_in_full(self) -> None:
        row = _row(
            metadata=json.dumps(
                {
                    "body": {"content": "see attached", "contentType": "text"},
                    "attachments": [{"name": "poll.json", "contentType": "application/json", "content": '{"a":1}'}],
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "poll.json" in page
        # the content is shown escaped, same as every other body text —
        # not verbatim (it's still untrusted source data going into HTML).
        assert "&quot;a&quot;:1" in page

    def test_attachment_without_inline_content_shows_the_honest_placeholder(self) -> None:
        row = _row(
            metadata=json.dumps(
                {
                    "body": {"content": "see attached", "contentType": "text"},
                    "attachments": [{"name": "photo.jpg", "contentType": "image/jpeg", "contentUrl": "https://x/y"}],
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "photo.jpg" in page
        assert "not included in this offline export" in page

    def test_reply_to_id_with_no_resolvable_parent_shows_a_generic_note(self) -> None:
        # "12345" isn't any real msg_id on this page — reply_to_id is a
        # real link (module docstring), but a raw, meaningless
        # numeric id is never shown to a human reader; only a resolved
        # sender/preview is (see the sibling test below).
        row = _row(reply_to_id="12345", content_preview="a reply")
        page = render_channel_html([row], channel_name="General")
        assert "12345" not in page
        assert "not included in this export" in page

    def test_reply_to_id_with_a_resolvable_parent_shows_sender_and_preview(self) -> None:
        parent = _row(msg_id="1", content_preview="the original message", author=json.dumps({"name": "Alice"}))
        reply = _row(msg_id="2", reply_to_id="1", content_preview="a reply", author=json.dumps({"name": "Bob"}))
        page = render_channel_html([parent, reply], channel_name="General")
        assert "replying to" in page
        assert "Alice" in page
        assert "the original message" in page

    def test_messages_are_sorted_chronologically_not_by_input_order(self) -> None:
        early = _row(metadata=json.dumps({"createdDateTime": "2023-01-01T00:00:00Z", "body": {"content": "first"}}))
        late = _row(metadata=json.dumps({"createdDateTime": "2023-06-01T00:00:00Z", "body": {"content": "second"}}))
        page = render_channel_html([late, early], channel_name="General")  # deliberately out of order
        assert page.index("first") < page.index("second")

    def test_sender_is_html_escaped_even_though_body_tags_are_now_allowlisted(self) -> None:
        # The sender name is never HTML — always escaped outright,
        # regardless of the body-rendering allowlist below.
        row = _row(
            metadata=json.dumps(
                {
                    "from": {"user": {"displayName": "<script>alert(1)</script>"}},
                    "body": {"content": "hi", "contentType": "text"},
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "<script>alert(1)</script>" not in page
        assert "&lt;script&gt;" in page

    def test_confirmed_real_safe_tags_render_structurally_not_as_escaped_text(self) -> None:
        # <b> (and the rest of _ALLOWED_STRUCTURAL_TAGS) are real markup
        # in real "html" contentType bodies — rendered as real HTML, not
        # flattened to visible escaped text, because they're part of
        # _MessageBodyRenderer's safe allowlist. A tag genuinely capable
        # of running script is never in that allowlist, so escaping
        # still applies to it.
        row = _row(
            metadata=json.dumps({"body": {"content": "<div><b>bold</b> <em>emph</em></div>", "contentType": "html"}})
        )
        page = render_channel_html([row], channel_name="General")
        assert "<b>bold</b>" in page
        assert "<em>emph</em>" in page

    def test_a_tag_not_in_the_allowlist_is_dropped_but_its_text_content_survives_escaped(self) -> None:
        # <script> (and any other non-allowlisted tag) is never emitted
        # as a real tag — HTMLParser reports its own inner text as CDATA
        # (real per-spec <script>/<style> handling), which still comes
        # through handle_data and is still escaped, same as any other
        # text — it just never runs, and never appears wrapped in a real
        # <script> element.
        row = _row(metadata=json.dumps({"body": {"content": "<script>alert(1)</script>", "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "<script>" not in page
        assert "alert(1)" in page  # inert text now, not executable markup

    def test_channel_name_is_html_escaped_in_title_and_heading(self) -> None:
        page = render_channel_html([], channel_name="<script>x</script>")
        assert "<script>x</script>" not in page
        assert "&lt;script&gt;x&lt;/script&gt;" in page

    def test_empty_channel_produces_a_valid_page_with_no_messages(self) -> None:
        page = render_channel_html([], channel_name="Empty")
        assert page.startswith("<!doctype html>")
        assert "Empty" in page

    def test_real_sticker_img_is_rendered_as_a_real_embedded_image(self) -> None:
        """Real shape: a sticker is an ordinary ``<img src="...">`` tag
        inside a real ``"html"`` contentType
        body, matched against this message's own row in a separate
        ``sticker_info_table`` — never embedded in
        ``metadata.attachments[]`` at all."""
        row = _row(
            msg_id="123",
            metadata=json.dumps(
                {
                    "body": {
                        "content": '<div><img src="https://graph.microsoft.com/sticker1" width="100">caption</div>',
                        "contentType": "html",
                    }
                }
            ),
        )
        stickers_by_msg_id = {"123": {"https://graph.microsoft.com/sticker1": "BASE64DATA"}}
        page = render_channel_html([row], channel_name="General", stickers_by_msg_id=stickers_by_msg_id)
        assert '<img class="sticker" alt="[sticker]" src="data:image/jpeg;base64,BASE64DATA">' in page
        assert "caption" in page
        # the real hostedContents URL itself is never leaked into the output —
        # only the matched, offline-embedded image data is.
        assert "graph.microsoft.com" not in page

    def test_unmatched_img_src_is_dropped_not_shown_as_broken_markup(self) -> None:
        row = _row(
            msg_id="123",
            metadata=json.dumps(
                {"body": {"content": '<div><img src="https://not-a-real-sticker">text</div>', "contentType": "html"}}
            ),
        )
        stickers_by_msg_id = {"123": {"https://graph.microsoft.com/sticker1": "BASE64DATA"}}
        page = render_channel_html([row], channel_name="General", stickers_by_msg_id=stickers_by_msg_id)
        assert "not-a-real-sticker" not in page
        assert "text" in page

    def test_html_body_renders_structurally_even_with_no_stickers_for_this_message(self) -> None:
        """No entry for this message's own ``msg_id`` in
        ``stickers_by_msg_id`` (a different message's stickers are
        present, just not this one's) — doesn't affect anything else in
        the body: real structural tags (``<div>``/``<b>``) still render,
        only the sticker-``<img>`` mechanism specifically depends on a
        per-message match."""
        row = _row(
            msg_id="999",
            metadata=json.dumps({"body": {"content": "<div>plain <b>html</b></div>", "contentType": "html"}}),
        )
        stickers_by_msg_id = {"123": {"https://x/y": "b64"}}
        page = render_channel_html([row], channel_name="General", stickers_by_msg_id=stickers_by_msg_id)
        assert "<b>html</b>" in page
        assert "plain" in page

    def test_deleted_message_shows_a_placeholder_not_a_blank_body(self) -> None:
        # Real shape: is_deleted=1 rows have their real
        # metadata.body.content emptied to "" by the connector — reading
        # that literally would render an empty-looking message with no
        # indication anything was ever there.
        row = _row(
            is_deleted=1,
            content_preview="<The message is deleted>",
            metadata=json.dumps({"body": {"content": "", "contentType": "text"}}),
        )
        page = render_channel_html([row], channel_name="General")
        assert "(this message has been deleted)" in page
        assert "<The message is deleted>" not in page  # our own label, not the raw connector placeholder verbatim

    def test_deleted_message_omits_attachments(self) -> None:
        row = _row(
            is_deleted=1,
            metadata=json.dumps(
                {
                    "body": {"content": "", "contentType": "text"},
                    "attachments": [{"name": "should-not-appear.txt", "contentType": "text/plain", "content": "x"}],
                }
            ),
        )
        page = render_channel_html([row], channel_name="General")
        assert "should-not-appear.txt" not in page

    def test_emoji_tag_renders_its_own_alt_character(self) -> None:
        # Teams' own emoji picker inserts
        # <emoji id="..." alt="🙂" title=""></emoji>, never a bare
        # Unicode character directly in the raw body.
        row = _row(
            metadata=json.dumps(
                {"body": {"content": '<emoji id="smile" alt="🙂" title=""></emoji>hi', "contentType": "html"}}
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "🙂" in page
        assert "hi" in page

    def test_self_closed_img_tag_is_handled_the_same_way_as_an_unclosed_one(self) -> None:
        # HTMLParser only calls handle_startendtag() (rather than
        # handle_starttag()) for a tag literally closed with "/>" in the
        # raw markup -- every other <img> test in this class uses the
        # unclosed shape real Teams data has, leaving this branch
        # untested until now.
        row = _row(
            metadata=json.dumps(
                {
                    "body": {
                        "content": '<img alt="Self-closed" src="https://graph.microsoft.com/v1.0/x" />',
                        "contentType": "html",
                    }
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "Self-closed" in page

    def test_self_closed_emoji_tag_renders_its_own_alt_character(self) -> None:
        row = _row(metadata=json.dumps({"body": {"content": '<emoji id="smile" alt="🙂" />hi', "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "🙂" in page
        assert "hi" in page

    def test_self_closed_void_structural_tag_is_preserved(self) -> None:
        row = _row(metadata=json.dumps({"body": {"content": "line one<br/>line two", "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "<br>" in page

    def test_at_tag_renders_as_a_styled_mention_not_a_raw_id(self) -> None:
        # id="0" is dropped, never rendered -- see saas_teams_chat.py's
        # `at`-tag handling for why.
        row = _row(
            metadata=json.dumps({"body": {"content": 'hi <at id="0">Alice Example</at>!', "contentType": "html"}})
        )
        page = render_channel_html([row], channel_name="General")
        assert '<span class="mention">@Alice Example</span>' in page
        assert 'id="0"' not in page

    def test_unresolved_inline_image_shows_its_alt_text_instead_of_vanishing(self) -> None:
        # Real shape: an inline <img> whose src is a live,
        # offline-unfetchable Microsoft Graph/giphy URL — the alt text is
        # still shown, rather than the whole reference silently
        # disappearing with no trace.
        row = _row(
            metadata=json.dumps(
                {
                    "body": {
                        "content": '<img alt="Team outing photo" src="https://graph.microsoft.com/v1.0/x">',
                        "contentType": "html",
                    }
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "Team outing photo" in page
        assert "graph.microsoft.com" not in page

    def test_link_with_safe_scheme_keeps_a_real_href(self) -> None:
        row = _row(
            metadata=json.dumps(
                {"body": {"content": '<a href="https://example.com/page">click</a>', "contentType": "html"}}
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert '<a href="https://example.com/page"' in page
        assert "click" in page

    def test_link_with_unsafe_scheme_drops_the_href_but_keeps_the_text(self) -> None:
        row = _row(
            metadata=json.dumps({"body": {"content": '<a href="javascript:alert(1)">click</a>', "contentType": "html"}})
        )
        page = render_channel_html([row], channel_name="General")
        assert "javascript:" not in page
        assert "click" in page

    def test_void_structural_tags_render_with_no_matching_close_tag(self) -> None:
        content = "line one<br>line two<hr>"
        row = _row(metadata=json.dumps({"body": {"content": content, "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "<br>" in page
        assert "<hr>" in page
        assert "</br>" not in page and "</hr>" not in page

    def test_confirmed_real_structural_tags_render_headings_lists_and_tables(self) -> None:
        # Real shape (apv-sample-1's "playground111" channel's own DSM
        # 7.1 announcement post): headings, lists and a table all in one
        # real message.
        content = "<h1>Title</h1><ul><li>one</li><li>two</li></ul><table><tr><td>a</td><td>b</td></tr></table>"
        row = _row(metadata=json.dumps({"body": {"content": content, "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "<h1>Title</h1>" in page
        assert "<li>one</li>" in page
        assert "<td>a</td>" in page

    def test_date_separator_appears_once_per_day_not_once_per_message(self) -> None:
        same_day_1 = _row(metadata=json.dumps({"createdDateTime": "2023-01-01T01:00:00Z", "body": {"content": "a"}}))
        same_day_2 = _row(metadata=json.dumps({"createdDateTime": "2023-01-01T02:00:00Z", "body": {"content": "b"}}))
        next_day = _row(metadata=json.dumps({"createdDateTime": "2023-01-02T01:00:00Z", "body": {"content": "c"}}))
        page = render_channel_html([same_day_1, same_day_2, next_day], channel_name="General")
        assert page.count('class="date-sep"') == 2
        assert "2023-01-01" in page
        assert "2023-01-02" in page


class TestChannelTree:
    def test_root_is_named_channels(self, channel_provider: TeamsChatProvider) -> None:
        assert channel_provider.root().name == "Channels"

    async def test_root_children_are_a_single_standard_channels_category(
        self, channel_provider: TeamsChatProvider
    ) -> None:
        # Every real channel this file's fixtures build defaults to
        # Standard (none sets channel_type) -- so only that one category
        # ever appears, never an empty Private/Shared alongside it.
        categories = await channel_provider.children(channel_provider.root())
        assert [c.name for c in categories] == ["Standard Channels"]
        assert all(not c.is_leaf for c in categories)

    async def test_children_are_named_from_channel_info_table(self, channel_provider: TeamsChatProvider) -> None:
        names = {n.name for n in await _channels_of(channel_provider)}
        assert names == {"Alpha", "Beta"}

    async def test_children_are_leaves_with_teams_chat_message_kind(self, channel_provider: TeamsChatProvider) -> None:
        # Lets the browser route these leaves through its own dedicated
        # chat-transcript preview/columns purely off Node.kind, with no
        # provider-specific attrs marker needed.
        for node in await _channels_of(channel_provider):
            assert node.is_leaf
            assert node.kind is UnitKind.TEAMS_CHAT_MESSAGE
            assert node.attrs.get("degraded") is None

    async def test_pagination(self, channel_provider: TeamsChatProvider) -> None:
        [category] = await channel_provider.children(channel_provider.root())
        one = await channel_provider.children(category, offset=0, limit=1)
        assert len(one) == 1

    async def test_channels_are_listed_alphabetically_not_index_order(self, tmp_path: Path) -> None:
        # Inserted in the *opposite* of alphabetical order -- the
        # channel/chat index's own on-disk order isn't otherwise
        # meaningful (within one category, this provider's own tree is
        # one flat level, no dir-first concept applies).
        _build_teams_repo(
            tmp_path,
            list_db_bytes=_build_channel_list_db([("chan-z", "Zeta"), ("chan-a", "Alpha")]),
            list_db_name="teams_channel_db",
            entries=[
                ("chan-z", _build_message_db_compressed([("Zoe", "hi from zeta")])),
                ("chan-a", _build_message_db_compressed([("Alice", "hi from alpha")])),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
            try:
                names = [n.name for n in await _channels_of(provider)]
                assert names == ["Alpha", "Zeta"]
            finally:
                await provider.close()

    async def test_a_channel_missing_from_channel_info_table_still_lists_under_standard(self, tmp_path: Path) -> None:
        """A channel the index itself names (``entries``) but
        ``channel_info_table`` has no row for at all must still be
        listed, defaulting to Standard -- the same fallback
        ``_channel_info``'s own "no channel_type column" case gets,
        just for a channel missing from the table entirely rather than
        missing one column of an existing row."""
        _build_teams_repo(
            tmp_path,
            list_db_bytes=_build_channel_list_db([("chan-a", "Alpha")]),  # "chan-b" has no row here
            list_db_name="teams_channel_db",
            entries=[
                ("chan-a", _build_message_db_compressed([("Alice", "hi from alpha")])),
                ("chan-b", _build_message_db_compressed([("Bob", "hi from beta")])),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await TeamsChatProvider.create(repo, _version(), saas_streams) as provider,
        ):
            categories = await provider.children(provider.root())
            assert [c.name for c in categories] == ["Standard Channels"]
            names = {n.name for n in await provider.children(categories[0])}
            assert names == {"Alpha", "chan-b"}  # chan-b falls back to its raw id -- no name row either


class TestChannelUnit:
    async def test_unit_content_is_an_html_page_with_the_real_message(
        self, channel_provider: TeamsChatProvider
    ) -> None:
        node = next(n for n in await _channels_of(channel_provider) if n.name == "Alpha")
        unit = await channel_provider.unit(node)
        page = (await unit.open().read()).decode("utf-8")
        assert page.startswith("<!doctype html>")
        assert "Alice" in page
        assert "hi from alpha" in page
        assert "Alpha" in page  # the channel name, in the page title/heading

    async def test_unit_name_has_html_suffix(self, channel_provider: TeamsChatProvider) -> None:
        node = next(n for n in await _channels_of(channel_provider) if n.name == "Alpha")
        assert (await channel_provider.unit(node)).name == "Alpha.html"

    async def test_exported_content_is_a_well_formed_self_contained_html_file(
        self, channel_provider: TeamsChatProvider, tmp_path: Path
    ) -> None:
        node = next(n for n in await _channels_of(channel_provider) if n.name == "Beta")
        unit = await channel_provider.unit(node)
        dst = tmp_path / "export" / "beta.html"
        dst.parent.mkdir(parents=True, exist_ok=True)
        await unit.open().export_to(dst)
        page = dst.read_text(encoding="utf-8")
        assert "Bob" in page
        assert "hi from beta" in page
        # self-contained: no tag that would make a browser fetch an
        # external resource (a bare "http://" substring inside escaped
        # text content wouldn't make the page any less self-contained —
        # see the real-data integration test for exactly that case).
        assert "<script" not in page
        assert "<img" not in page
        assert "<link" not in page
        assert "<iframe" not in page
        assert 'src="http' not in page
        assert 'href="http' not in page

    async def test_a_real_sticker_table_gets_read_and_embedded_in_the_exported_page(self, tmp_path: Path) -> None:
        """``_read_stickers`` itself (reading a real ``sticker_info_table``
        via a live connection), not just ``render_channel_html``'s own
        formatting of an already-built ``stickers_by_msg_id`` dict --
        every other sticker-rendering test in this file passes that dict
        in directly (``TestRenderChannelHtml``)."""
        _build_teams_repo(
            tmp_path,
            list_db_bytes=_build_channel_list_db([("chan-a", "Alpha")]),
            list_db_name="teams_channel_db",
            entries=[
                (
                    "chan-a",
                    zstandard.ZstdCompressor().compress(
                        _build_message_db(
                            [("Alice", '<img src="https://graph.microsoft.com/sticker1">caption')],
                            stickers={"0": {"https://graph.microsoft.com/sticker1": "BASE64DATA"}},
                        )
                    ),
                ),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
            try:
                [node] = await _channels_of(provider)
                page = (await (await provider.unit(node)).open().read()).decode("utf-8")
                assert '<img class="sticker" alt="[sticker]" src="data:image/jpeg;base64,BASE64DATA">' in page
            finally:
                await provider.close()

    async def test_unit_on_a_node_with_no_object_id_raises(self, channel_provider: TeamsChatProvider) -> None:
        real_node = next(n for n in await _channels_of(channel_provider) if n.name == "Alpha")
        phantom = Node(ref=real_node.ref, name="phantom", is_leaf=True, attrs={})
        with pytest.raises(ValueError, match="not a restorable unit"):
            await channel_provider.unit(phantom)


class TestDegradedReason:
    async def test_chat_schema_not_found_at_all_reports_that_specifically(self, tmp_path: Path) -> None:
        # Mathematically unreachable through _is_container()'s own
        # contract on a real chat container (chat_info_table must be
        # present whenever channel_info_table isn't, since _is_container
        # requires at least one of the two) -- this diagnostic branch
        # exists for the case that contract stops holding (a future
        # schema change), so it's exercised here by setting the private
        # flag directly on a real chat provider rather than through a
        # real, currently-impossible container shape.
        list_db = _build_chat_list_db([("chat-1", "Carol & Dave")], id_col="chat_id", label_col="topic")
        _build_teams_repo(
            tmp_path,
            list_db_bytes=list_db,
            list_db_name="chat_db",
            entries=[("chat-1", _build_message_db_compressed([("Carol", "hi from chat")]))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
            try:
                provider._chat_schema_found = False
                provider._labels = {}  # ensure this entity_id isn't already labeled
                entity_id = next(iter(provider._entity_object_ids))
                assert provider._degraded_reason(entity_id) == (
                    "chat_info_table not found for this version — showing raw chat ids"
                )
            finally:
                await provider.close()


class TestChatFallback:
    """Chat's ``chat_info_table`` schema is unconfirmed against any real
    sample (module docstring) — these exercise the best-effort generic
    column matching and its graceful failure mode."""

    @asynccontextmanager
    async def _open(self, tmp_path: Path, list_db_bytes: bytes) -> AsyncIterator[TeamsChatProvider]:
        _build_teams_repo(
            tmp_path,
            list_db_bytes=list_db_bytes,
            list_db_name="chat_db",  # the real index name (_CONTAINER_DB_NAMES) — not just any string
            entries=[("chat-1", _build_message_db_compressed([("Carol", "hi from chat")]))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await TeamsChatProvider.create(repo, _version(), saas_streams)
            try:
                yield provider
            finally:
                await provider.close()

    async def test_recognizable_columns_resolve_a_label(self, tmp_path: Path) -> None:
        list_db = _build_chat_list_db([("chat-1", "Carol & Dave")], id_col="chat_id", label_col="topic")
        async with self._open(tmp_path, list_db) as provider:
            assert provider.root().name == "Chats"
            node = (await provider.children(provider.root()))[0]
            assert node.name == "Carol & Dave"
            assert node.attrs.get("degraded") is None

    async def test_unrecognizable_columns_fall_back_to_raw_id_and_flag_degraded(self, tmp_path: Path) -> None:
        list_db = _build_chat_list_db([("chat-1", "Carol & Dave")], id_col="weird_key", label_col="weird_value")
        async with self._open(tmp_path, list_db) as provider:
            node = (await provider.children(provider.root()))[0]
            assert node.name == "chat-1"  # falls back to the raw chat id
            assert node.attrs.get("degraded") is not None

    async def test_unit_on_a_chat_node_renders_the_real_message_html(self, tmp_path: Path) -> None:
        """Chat's ``.unit()`` renders through the same ``render_channel_html()``
        Channel's own does (``TestTree.test_unit_content_is_an_html_page_
        with_the_real_message``) — Chat's rendering path is implemented
        generically, unconfirmed against a real instance, so this is the
        synthetic coverage for that path."""
        list_db = _build_chat_list_db([("chat-1", "Carol & Dave")], id_col="chat_id", label_col="topic")
        async with self._open(tmp_path, list_db) as provider:
            node = (await provider.children(provider.root()))[0]
            unit = await provider.unit(node)
            page = (await unit.open().read()).decode("utf-8")
            assert page.startswith("<!doctype html>")
            assert "Carol" in page
            assert "hi from chat" in page


class _FakeObjectDb:
    """Stands in for ``synology_apm_repo.sdk.units.saas.objectdb.ObjectDb``
    -- just enough of its surface (``get``/``close``) for
    ``_is_container``/``_resolve_message_index`` to run against, without
    needing real embedded SQLite bytes for every failure branch."""

    def __init__(
        self, locations: dict[str, tuple[int, int]], *, get_raises: dict[str, Exception] | None = None
    ) -> None:
        self._locations = locations
        self._get_raises = get_raises or {}
        self.closed = False

    async def get(self, object_id: str) -> tuple[int, int]:
        if object_id in self._get_raises:
            raise self._get_raises[object_id]
        return self._locations[object_id]

    async def close(self) -> None:
        self.closed = True


class TestIsContainer:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.saas.teams_chat._is_container`` --
    every real caller only ever sees this succeed (a real sample's own
    index always names a real, valid container), so its own defensive
    branches never ran anywhere else in this suite."""

    async def test_zero_length_returns_none(self) -> None:
        db = _FakeObjectDb({"obj-a": (0, 0)})
        result = await _is_container(cast(DedupFile, object()), cast(ObjectDb, db), "obj-a")
        assert result is None

    async def test_non_service_db_kind_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        db = _FakeObjectDb({"obj-a": (0, 10)})

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            return SniffResult(kind=ServiceKind.META_JSON)

        monkeypatch.setattr(teams_chat_module, "inspect_object", fake_inspect)
        result = await _is_container(cast(DedupFile, object()), cast(ObjectDb, db), "obj-a")
        assert result is None

    async def test_service_db_without_a_container_table_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        db = _FakeObjectDb({"obj-a": (0, 10)})

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            return SniffResult(kind=ServiceKind.SERVICE_DB, tables=frozenset({"some_unrelated_table"}))

        monkeypatch.setattr(teams_chat_module, "inspect_object", fake_inspect)
        result = await _is_container(cast(DedupFile, object()), cast(ObjectDb, db), "obj-a")
        assert result is None


class TestResolveMessageIndex:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.saas.teams_chat._resolve_message_index``
    -- every real caller (``TeamsChatProvider.create``) only ever sees
    this resolve successfully against real, well-formed connector data."""

    async def test_none_catalog_index_returns_none(self) -> None:
        assert await _resolve_message_index(cast(DedupFile, object()), None) is None

    async def test_missing_db_infos_in_snapshot_entry_returns_none(self) -> None:
        object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={})
        assert await _resolve_message_index(cast(DedupFile, object()), object_name_index) is None

    async def test_object_db_load_failure_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise DataCorruptError("synthetic corruption for this test")

        monkeypatch.setattr(ObjectDb, "load", failing_load)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert await _resolve_message_index(cast(DedupFile, object()), object_name_index) is None

    async def test_index_object_id_not_in_the_object_db_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_db = _FakeObjectDb({}, get_raises={"idx-obj": NotFoundError("no such object", ref="idx-obj")})

        async def fake_load(dedup_file: object, offset: int, length: int) -> _FakeObjectDb:
            return fake_db

        monkeypatch.setattr(ObjectDb, "load", fake_load)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert await _resolve_message_index(cast(DedupFile, object()), object_name_index) is None
        assert fake_db.closed is True

    async def test_inspect_object_failure_on_the_index_itself_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_db = _FakeObjectDb({"idx-obj": (0, 10)})

        async def fake_load(dedup_file: object, offset: int, length: int) -> _FakeObjectDb:
            return fake_db

        monkeypatch.setattr(ObjectDb, "load", fake_load)

        async def raising_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            raise DataCorruptError("bad index bytes")

        monkeypatch.setattr(teams_chat_module, "inspect_object", raising_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert await _resolve_message_index(cast(DedupFile, object()), object_name_index) is None
        assert fake_db.closed is True

    async def test_index_object_that_does_not_actually_look_like_an_index_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_db = _FakeObjectDb({"idx-obj": (0, 10)})

        async def fake_load(dedup_file: object, offset: int, length: int) -> _FakeObjectDb:
            return fake_db

        monkeypatch.setattr(ObjectDb, "load", fake_load)

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            return SniffResult(kind=ServiceKind.META_JSON)

        monkeypatch.setattr(teams_chat_module, "inspect_object", fake_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert await _resolve_message_index(cast(DedupFile, object()), object_name_index) is None
        assert fake_db.closed is True

    async def test_index_with_no_recognized_container_entry_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_db = _FakeObjectDb({"idx-obj": (0, 10)})

        async def fake_load(dedup_file: object, offset: int, length: int) -> _FakeObjectDb:
            return fake_db

        monkeypatch.setattr(ObjectDb, "load", fake_load)

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            return SniffResult(
                kind=ServiceKind.INDEX, index_entries=(IndexEntry(name="some_other_db", object_id="other-obj"),)
            )

        monkeypatch.setattr(teams_chat_module, "inspect_object", fake_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert await _resolve_message_index(cast(DedupFile, object()), object_name_index) is None
        assert fake_db.closed is True

    async def test_a_recognized_container_entry_that_does_not_validate_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The index names a real teams_channel_db/chat_db entry, but that
        # entry's own object doesn't actually validate as a container
        # (_is_container's own further inspect_object call on it) --
        # distinct from the "index object isn't an INDEX at all" case
        # above.
        fake_db = _FakeObjectDb({"idx-obj": (0, 10), "container-obj": (100, 10)})

        async def fake_load(dedup_file: object, offset: int, length: int) -> _FakeObjectDb:
            return fake_db

        monkeypatch.setattr(ObjectDb, "load", fake_load)

        async def fake_inspect(dedup_file: object, offset: int, length: int) -> SniffResult:
            if offset == 0:
                return SniffResult(
                    kind=ServiceKind.INDEX,
                    index_entries=(IndexEntry(name="teams_channel_db", object_id="container-obj"),),
                )
            return SniffResult(kind=ServiceKind.META_JSON)  # the container object itself doesn't validate

        monkeypatch.setattr(teams_chat_module, "inspect_object", fake_inspect)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=1, object_ids={"db_infos_in_snapshot": "idx-obj"}
        )
        assert await _resolve_message_index(cast(DedupFile, object()), object_name_index) is None
        assert fake_db.closed is True


class TestOwningAccountEmail:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.saas.teams_chat._owning_account_email``
    -- every real caller's fixture repository already has a matching workload
    row with a real email, so neither "no such row" nor "row present but
    no email" ever ran."""

    async def test_no_matching_workload_row_returns_none(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_workload_config(tmp_path / "db" / "workload_config", [(999, json.dumps({}))])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await _owning_account_email(repo, _version()) is None

    async def test_matching_row_with_no_email_returns_none(self, tmp_path: Path) -> None:
        spec = json.dumps({"status": {"entity_meta": {"spec": {"user_info": {}}}}})
        _write_repo_info(tmp_path / "repo_info")
        _write_workload_config(tmp_path / "db" / "workload_config", [(1, spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await _owning_account_email(repo, _version()) is None

    async def test_matching_row_with_a_real_email_returns_it(self, tmp_path: Path) -> None:
        # The actual success path -- every other test either has no
        # matching row or a row missing user_info.email entirely.
        spec = json.dumps({"status": {"entity_meta": {"spec": {"user_info": {"email": "alice@example.com"}}}}})
        _write_repo_info(tmp_path / "repo_info")
        _write_workload_config(tmp_path / "db" / "workload_config", [(1, spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await _owning_account_email(repo, _version()) == "alice@example.com"

    async def test_schema_drifted_workload_config_returns_none_instead_of_raising(self, tmp_path: Path) -> None:
        """A ``workload_config`` missing its own ``workload_spec``
        column entirely (``Table.create`` raising ``DataCorruptError``) is
        the same "nothing to read" shape as no matching row at all --
        this is an enrichment-only lookup (a nicer chat/channel label),
        never a reason to crash the caller."""
        _write_repo_info(tmp_path / "repo_info")
        (tmp_path / "db").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp_path / "db" / "workload_config")
        conn.execute("CREATE TABLE workload_config(workload_id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO workload_config VALUES (1)")
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await _owning_account_email(repo, _version()) is None


def _build_plain_sqlite_bytes(build: Callable[[sqlite3.Connection], object]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "container.db"
        conn = sqlite3.connect(path)
        build(conn)
        conn.commit()
        conn.close()
        return path.read_bytes()


class TestChatLabels:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.saas.teams_chat._chat_labels`` —
    ``container_bytes`` here is already-decompressed plain SQLite, the
    same shape its one real caller (``TeamsChatProvider.create``) hands
    it after its own ``decompress_service_db()`` call."""

    async def test_no_chat_info_table_at_all_returns_empty(self) -> None:
        container_bytes = _build_plain_sqlite_bytes(lambda conn: conn.execute("CREATE TABLE unrelated(x INTEGER)"))
        assert await _chat_labels(container_bytes, None) == ({}, {})

    async def test_a_sqlite_error_while_reading_labels_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT)")
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', 'Some Topic')")

        container_bytes = _build_plain_sqlite_bytes(build)

        async def raising_select(
            self: Table, where: str = "", params: object = (), **kwargs: object
        ) -> AsyncIterator[dict[str, object]]:
            raise sqlite3.OperationalError("synthetic corruption for this test")
            yield {}  # pragma: no cover - unreachable, makes this a real async generator

        monkeypatch.setattr(Table, "select", raising_select)
        assert await _chat_labels(container_bytes, None) == ({}, {})

    async def test_bot_label_for_an_unnamed_one_on_one_chat_with_no_derivable_member_name(self) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT, chat_type INTEGER)")
            # No topic (falsy) and chat_type=0 (ONE_ON_ONE, not MEETING)
            # -- the first pass leaves this chat unlabeled.
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', '', 0)")
            conn.execute("CREATE TABLE chat_members_table(chat_id TEXT, members TEXT)")
            # An empty member list -- _chat_display_name_from_members()
            # has no real member to derive a name from, matching a real
            # Teams bot/app chat's own shape.
            conn.execute("INSERT INTO chat_members_table VALUES ('chat-1', '[]')")

        container_bytes = _build_plain_sqlite_bytes(build)
        labels, _create_times = await _chat_labels(container_bytes, "me@example.com")
        assert labels == {"chat-1": "Bot"}

    async def test_no_title_meeting_gets_the_literal_meeting_label(self) -> None:
        # _CHAT_TYPE_MEETING (2) with no topic set -- the chat_type == 2
        # branch has no coverage anywhere else in this file (the bot-chat
        # test above uses chat_type=0/ONE_ON_ONE).
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT, chat_type INTEGER)")
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', '', 2)")

        container_bytes = _build_plain_sqlite_bytes(build)
        labels, _create_times = await _chat_labels(container_bytes, None)
        assert labels == {"chat-1": "(no title)"}

    async def test_create_time_is_read_when_the_column_is_present(self) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT, create_time INTEGER)")
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', 'Some Topic', 1700000000)")

        container_bytes = _build_plain_sqlite_bytes(build)
        _labels, create_times = await _chat_labels(container_bytes, None)
        assert create_times == {"chat-1": 1700000000}

    async def test_create_time_is_absent_when_the_column_is_missing(self) -> None:
        # chat_info_table's real schema is unconfirmed (module docstring)
        # -- a connector version without this column must not crash.
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE chat_info_table(chat_id TEXT PRIMARY KEY, topic TEXT)")
            conn.execute("INSERT INTO chat_info_table VALUES ('chat-1', 'Some Topic')")

        container_bytes = _build_plain_sqlite_bytes(build)
        _labels, create_times = await _chat_labels(container_bytes, None)
        assert create_times == {}


class TestChannelInfo:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.saas.teams_chat._channel_info``."""

    async def test_channels_with_no_name_are_omitted(self) -> None:
        # ``if row.get("name")`` -- a falsy (empty/null) name must not
        # produce an empty-string label. No channel_type column at all --
        # Table's own schema-drift tolerance backfills None for it
        # (required=False), treated as Standard by _channel_info.
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE channel_info_table(channel_id TEXT PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-1', 'General')")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-2', '')")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-3', NULL)")

        container_bytes = _build_plain_sqlite_bytes(build)
        labels, categories, create_times = await _channel_info(container_bytes)
        assert labels == {"ch-1": "General"}
        assert categories == {"ch-1": "standard", "ch-2": "standard", "ch-3": "standard"}
        # No create_time column at all -- required=False backfills None,
        # so no channel gets an entry rather than a bogus one.
        assert create_times == {}

    async def test_channel_type_drives_category(self) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE channel_info_table(channel_id TEXT PRIMARY KEY, name TEXT, channel_type TEXT)")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-1', 'General', 'standard')")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-2', 'private 2', 'private')")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-3', 'shared 1', 'shared')")
            # An unrecognized/malformed channel_type value degrades to
            # Standard rather than raising or being dropped.
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-4', 'weird', 'unknown-type')")

        container_bytes = _build_plain_sqlite_bytes(build)
        _labels, categories, _create_times = await _channel_info(container_bytes)
        assert categories == {"ch-1": "standard", "ch-2": "private", "ch-3": "shared", "ch-4": "standard"}

    async def test_create_time_is_read_when_present(self) -> None:
        def build(conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE channel_info_table(channel_id TEXT PRIMARY KEY, name TEXT, create_time INTEGER)")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-1', 'General', 1700000000)")
            conn.execute("INSERT INTO channel_info_table VALUES ('ch-2', 'No Time', NULL)")

        container_bytes = _build_plain_sqlite_bytes(build)
        _labels, _categories, create_times = await _channel_info(container_bytes)
        assert create_times == {"ch-1": 1700000000}


class TestTeamsEntityFlatTree:
    """Direct unit tests for ``_TeamsEntityFlatTree`` -- the ``TreeStrategy``
    ``TeamsChatProvider`` builds its own channel/chat listing from (bare
    for Chat, wrapped in ``CategorizedGroupTree`` for Channel's own
    category split). Exercised here without a real repository, since
    none of this class's own logic does any I/O."""

    async def test_children_of_a_leafs_own_key_is_empty(self) -> None:
        tree = _TeamsEntityFlatTree({"chat-1": "obj-1"}, {})
        assert await tree.children_of(("chat-1",)) == []

    def test_row_for_a_wrong_length_key_is_none(self) -> None:
        tree = _TeamsEntityFlatTree({"chat-1": "obj-1"}, {})
        assert tree.row_for(()) is None
        assert tree.row_for(("chat-1", "extra")) is None

    def test_row_for_an_entity_not_in_the_index_is_none(self) -> None:
        tree = _TeamsEntityFlatTree({"chat-1": "obj-1"}, {})
        assert tree.row_for(("nope",)) is None

    def test_row_for_a_real_entity(self) -> None:
        tree = _TeamsEntityFlatTree({"chat-1": "obj-1"}, {})
        assert tree.row_for(("chat-1",)) == {"entity_id": "chat-1", "object_id": "obj-1"}


class TestMtimeAttr:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.saas.teams_chat._mtime_attr``."""

    def test_returns_mtime_when_present(self) -> None:
        assert _mtime_attr({"ch-1": 1700000000}, "ch-1") == {"mtime": mtime_from_epoch(1700000000)}

    def test_blank_when_entity_has_no_entry(self) -> None:
        assert _mtime_attr({}, "ch-1") == {}

    def test_blank_when_create_time_is_out_of_datetimes_representable_range(self) -> None:
        # A corrupt-catalog value degrades this one entity's Created cell
        # to blank rather than raising -- mtime_from_epoch returns None
        # for an epoch outside datetime's own representable range.
        assert _mtime_attr({"ch-1": 99999999999999999}, "ch-1") == {}


class TestDegradation:
    async def test_no_index_raises_unsupported_data_format(self, tmp_path: Path) -> None:
        _build_empty_saas_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError):
                await TeamsChatProvider.create(repo, _version(), saas_streams)

    async def test_no_index_found_closes_its_stream_instead_of_leaking_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test: this is the *routine* degrade-to-RawObjectProvider
        path (no channel/chat index for this version, not corrupt data), hit
        on every such version — create() must call close() on this
        path rather than only on success, or whatever it already opened
        (e.g. ``self._db``) leaks -- an unclosed aiosqlite connection's
        worker thread has no daemon flag, so it blocks interpreter
        shutdown forever."""
        _build_empty_saas_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        closed_instances = []
        original_close = TeamsChatProvider.close

        async def spy_close(self: TeamsChatProvider) -> None:
            closed_instances.append(self)
            await original_close(self)

        monkeypatch.setattr(TeamsChatProvider, "close", spy_close)
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError):
                await TeamsChatProvider.create(repo, _version(), saas_streams)
            assert len(closed_instances) == 1


class TestChatDisplayNameFromMembers:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.saas.teams_chat._chat_display_name_from_members``
    — pure formatting, no I/O."""

    def test_malformed_json_string_returns_none(self) -> None:
        assert _chat_display_name_from_members("not json", "me@example.com") is None

    def test_non_list_json_returns_none(self) -> None:
        assert _chat_display_name_from_members(json.dumps({"not": "a list"}), "me@example.com") is None

    def test_non_string_input_returns_none(self) -> None:
        assert _chat_display_name_from_members(None, "me@example.com") is None

    def test_derives_a_comma_joined_name_from_other_members_excluding_self(self) -> None:
        # The actual real mechanism this function exists for -- every
        # other test here only exercises the None-returning failure
        # modes (malformed/non-list/non-string input).
        members = json.dumps(
            [
                {"display_name": "Me", "userEmail": "me@example.com"},
                {"display_name": "Bob", "userEmail": "bob@example.com"},
                {"display_name": "Carol", "userEmail": "carol@example.com"},
            ]
        )
        assert _chat_display_name_from_members(members, "me@example.com") == "Bob, Carol"

    def test_every_member_being_self_or_nameless_returns_none(self) -> None:
        members = json.dumps(
            [
                {"display_name": "Me", "userEmail": "me@example.com"},
                {"userEmail": "no-name@example.com"},  # no display_name at all
            ]
        )
        assert _chat_display_name_from_members(members, "me@example.com") is None

    def test_self_stays_included_when_self_email_matches_no_member(self) -> None:
        # A member list built from independently-sourced identities can
        # legitimately carry no entry matching ``self_email`` at all (e.g.
        # a display_name/email pair that was resolved through a different
        # identity mapping than the chat's own member records) -- the
        # match simply finds nothing to exclude, and every member,
        # including self, stays in the joined name.
        members = json.dumps(
            [
                {"display_name": "Me", "userEmail": "me@example.com"},
                {"display_name": "Bob", "userEmail": "bob@example.com"},
            ]
        )
        assert _chat_display_name_from_members(members, "someone-else@example.com") == "Me, Bob"


def _rendered_message(
    *,
    msg_id: str | None = "m1",
    reply_to_id: str | None = None,
    is_deleted: bool = False,
    is_system: bool = False,
    sender: str = "Alice",
    content: str = "hello",
    preview: str = "hello",
) -> _RenderedMessage:
    return _RenderedMessage(
        created=None,
        sender=sender,
        is_system=is_system,
        is_deleted=is_deleted,
        msg_id=msg_id,
        reply_to_id=reply_to_id,
        content=content,
        preview=preview,
        attachments=(),
    )


class TestReplyNoteHtml:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.content.saas_teams_chat._reply_note_html``
    -- only ever exercised transitively through ``render_channel_html``
    elsewhere in this file, none of which happen to build a deleted or
    system parent, or a parent with no preview text."""

    def test_no_reply_to_id_is_empty(self) -> None:
        message = _rendered_message(reply_to_id=None)
        assert _reply_note_html(message, {}) == ""

    def test_parent_not_in_this_export_gets_the_unresolved_note(self) -> None:
        message = _rendered_message(reply_to_id="missing-parent")
        assert "not included in this export" in _reply_note_html(message, {})

    def test_deleted_parent_shows_the_deleted_placeholder(self) -> None:
        parent = _rendered_message(msg_id="p1", is_deleted=True, sender="Bob")
        message = _rendered_message(reply_to_id="p1")
        html = _reply_note_html(message, {"p1": parent})
        assert "Bob" in html
        assert "deleted" in html.lower()

    def test_system_parent_shows_its_own_content_not_the_preview(self) -> None:
        parent = _rendered_message(msg_id="p1", is_system=True, sender="System", content="Alice joined the chat")
        message = _rendered_message(reply_to_id="p1")
        html = _reply_note_html(message, {"p1": parent})
        assert "Alice joined the chat" in html

    def test_parent_with_no_preview_falls_back_to_ellipsis(self) -> None:
        parent = _rendered_message(msg_id="p1", sender="Bob", preview="")
        message = _rendered_message(reply_to_id="p1")
        html = _reply_note_html(message, {"p1": parent})
        assert "…" in html

    def test_parent_with_a_real_preview_gets_truncated_into_the_snippet(self) -> None:
        parent = _rendered_message(msg_id="p1", sender="Bob", preview="a real preview")
        message = _rendered_message(reply_to_id="p1")
        html = _reply_note_html(message, {"p1": parent})
        assert "a real preview" in html


class TestParseJsonObject:
    """Direct unit tests for
    ``synology_apm_repo.sdk.units.content.saas_teams_chat._parse_json_object``."""

    def test_malformed_json_string_returns_empty_dict(self) -> None:
        assert _parse_json_object("not json") == {}

    def test_non_object_json_returns_empty_dict(self) -> None:
        assert _parse_json_object(json.dumps([1, 2, 3])) == {}

    def test_non_string_input_returns_empty_dict(self) -> None:
        assert _parse_json_object(None) == {}


__all__: list[str] = []
