"""Unit tests for ``synology_apm_repo.sdk.units.saas.mail`` — a full
synthetic repository root (same building blocks as
``test_units_saas_calendar.py``) plus dedicated pure-function tests for
``build_eml()``'s ``X-ABL-ID`` reassembly. These synthetic tests cover
edge cases (nested ``message/rfc822`` fragments, missing ``X-ABL-ID``
matches) that aren't necessarily present in any one real sample;
``tests/integration/sdk/test_units_saas_mail.py`` covers the real-data
path."""

from __future__ import annotations

import dataclasses
import email
import email.policy
import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import AsyncIterator, Sequence
from email.message import EmailMessage, Message
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, UnsupportedDataFormatError
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
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.content.saas_mail import build_eml
from synology_apm_repo.sdk.units.saas.mail import MAIL_CONFIG, ArchiveMailProvider, MailProvider, _mail_display_name
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.saas.tree_strategy import TreeStrategy

_STREAM_ID = 16
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "mail-stream-uuid"


# -- build_eml() pure-function tests -------------------------------------


def _mark_part(msg: Message, content_type: str, abl_id: str) -> None:
    for part in msg.walk():
        if part.get_content_type() == content_type:
            part["X-ABL-ID"] = abl_id
            return
    raise AssertionError(f"no part with content type {content_type!r}")


class TestBuildEmlPureFunction:
    @pytest.mark.parametrize(
        ("subject", "maintype", "subtype", "filename", "encoding", "real_bytes"),
        [
            pytest.param(
                "Test", "application", "pdf", "doc.pdf", None, b"%PDF-1.4 fake pdf content 1234567890", id="base64"
            ),
            pytest.param(
                "QP",
                "text",
                "plain",
                "note.txt",
                "quoted-printable",
                b"line1\nline2 with =signs and \xe2\x82\xac euro",
                id="quoted_printable",
            ),
            pytest.param(
                "Plain",
                "text",
                "plain",
                "ascii.txt",
                "7bit",
                b"plain ascii content, no special encoding needed",
                id="7bit",
            ),
        ],
    )
    def test_attachment_round_trips_byte_for_byte(
        self, subject: str, maintype: str, subtype: str, filename: str, encoding: str | None, real_bytes: bytes
    ) -> None:
        # encoding=None leaves add_attachment's own default (base64 for a
        # binary maintype) in place, matching a real .eml skeleton's own
        # Content-Transfer-Encoding for that part.
        msg = EmailMessage()
        msg["Subject"] = subject
        msg.set_content("body")
        msg.add_attachment(b"", maintype=maintype, subtype=subtype, filename=filename)
        for part in msg.walk():
            if part.get_filename() == filename:
                if encoding is not None:
                    part.replace_header("Content-Transfer-Encoding", encoding)
                part["X-ABL-ID"] = "ID-file"
        skel = msg.as_bytes()

        result = build_eml(skel, {"ID-file": real_bytes})

        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        [found] = [p for p in reparsed.walk() if p.get_filename() == filename]
        assert found.get_payload(decode=True) == real_bytes
        assert found.get("X-ABL-ID") is None

    def test_parts_without_x_abl_id_are_left_untouched(self) -> None:
        msg = EmailMessage()
        msg["Subject"] = "Kept"
        msg.set_content("this stays exactly as-is")
        skel = msg.as_bytes()

        result = build_eml(skel, {})  # no fragments at all
        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        body = reparsed.get_payload(decode=True)
        assert isinstance(body, bytes)
        assert body.strip() == b"this stays exactly as-is"

    def test_matching_is_by_header_value_not_array_order(self) -> None:
        msg = EmailMessage()
        msg["Subject"] = "Order"
        msg.set_content("body")
        msg.add_attachment(b"", maintype="application", subtype="a", filename="a.bin")
        msg.add_attachment(b"", maintype="application", subtype="b", filename="b.bin")
        for part in msg.walk():
            if part.get_filename() == "a.bin":
                part["X-ABL-ID"] = "ID-file-2"  # deliberately "swapped" ids
            elif part.get_filename() == "b.bin":
                part["X-ABL-ID"] = "ID-file"

        skel = msg.as_bytes()
        # dict insertion order is the OPPOSITE of which part references which id
        fragments = {"ID-file": b"content-for-b", "ID-file-2": b"content-for-a"}
        result = build_eml(skel, fragments)

        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        for found in reparsed.walk():
            if found.get_filename() == "a.bin":
                assert found.get_payload(decode=True) == b"content-for-a"
            elif found.get_filename() == "b.bin":
                assert found.get_payload(decode=True) == b"content-for-b"

    def test_nested_message_rfc822_fragment_is_not_recursively_expanded(self) -> None:
        inner = EmailMessage()
        inner["Subject"] = "Inner"
        inner.set_content("inner body")
        inner_bytes = inner.as_bytes()

        outer = EmailMessage()
        outer["Subject"] = "Outer"
        outer.set_content("outer body")
        outer.add_attachment(b"", maintype="message", subtype="rfc822")
        for part in outer.walk():
            if part.get_content_type() == "message/rfc822":
                part.replace_header("Content-Transfer-Encoding", "7bit")
                part["X-ABL-ID"] = "ID-file"
        skel = outer.as_bytes()

        result = build_eml(skel, {"ID-file": inner_bytes})
        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        [nested] = [p for p in reparsed.walk() if p.get_content_type() == "message/rfc822"]
        # Python's own parser always re-nests a message/rfc822 body into a
        # sub-Message on parse (regardless of how it got there), which makes
        # is_multipart() true and get_payload(decode=True) unconditionally
        # None for this content type — not a bug in build_eml, just the
        # wrong way to read this particular part back. The sub-Message's own
        # serialization is the correct byte-for-byte comparison here.
        nested_payload = nested.get_payload()
        assert isinstance(nested_payload, list)
        [inner_message] = nested_payload
        assert isinstance(inner_message, email.message.Message)
        assert inner_message.as_bytes() == inner_bytes

    def test_unmatched_fragment_ids_in_the_dict_are_simply_unused(self) -> None:
        msg = EmailMessage()
        msg["Subject"] = "NoRefs"
        msg.set_content("body")
        skel = msg.as_bytes()
        # a fragment dict entry referencing an id no part in the skeleton uses
        result = build_eml(skel, {"ID-file-orphan": b"never used"})

        reparsed = email.message_from_bytes(result, policy=email.policy.compat32)
        assert reparsed["Subject"] == "NoRefs"
        body = reparsed.get_payload(decode=True)
        assert isinstance(body, bytes)
        assert body.strip() == b"body"
        assert b"never used" not in result


class TestDeclaredSize:
    def test_non_int_size_is_narrowed_to_none(self) -> None:
        from synology_apm_repo.sdk.units.saas.mail import _declared_size

        assert _declared_size({"size": "12"}) is None  # a string, not an int
        assert _declared_size({}) is None  # key altogether absent
        assert _declared_size({"size": 12}) == 12


# -- MailProvider integration tests (synthetic repo) ---------------------


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


def _write_saas_version_db(path: Path, target_type: str = "M365") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_info(snapshot_id INTEGER, version_id INTEGER, stream_version INTEGER, deleted INTEGER)"
    )
    conn.execute("INSERT INTO version_info VALUES (1, 3, 1, 0)")
    conn.execute("CREATE TABLE stream_info(target_type TEXT)")
    conn.execute("INSERT INTO stream_info VALUES (?)", (target_type,))
    conn.commit()
    conn.close()


def _write_copy_target_version_db(
    path: Path, *, version_uid: str, object_db_id: str, db_objects: list[tuple[str, str]]
) -> None:
    """The connector's own index bookkeeping
    (``synology_apm_repo.sdk.units.saas.object_name_index``) — every
    ``SaasWorkloadProvider``/``TeamsChatProvider`` construction resolves
    its service DB(s) *only* through this table, with no
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


def _build_mail_db(mails: list[tuple[str, str, str, str]]) -> bytes:
    """``mails``: (mail_id, subject, parent_folder_id, meta_object_id)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute("CREATE TABLE mail_table(mail_id TEXT, subject TEXT, parent_folder_id TEXT, meta_object_id TEXT)")
        conn.executemany("INSERT INTO mail_table VALUES (?, ?, ?, ?)", mails)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_mail_db_with_timestamps(mails: list[tuple[str, str, str, str, int]]) -> bytes:
    """Like ``_build_mail_db``, but with a real ``remote_timestamp``
    column -- ``mails``: (mail_id, subject, parent_folder_id,
    meta_object_id, remote_timestamp). A separate helper, not an
    optional-column extension of ``_build_mail_db`` itself, so no other
    test's schema shape shifts."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE mail_table("
            "mail_id TEXT, subject TEXT, parent_folder_id TEXT, meta_object_id TEXT, remote_timestamp INTEGER)"
        )
        conn.executemany("INSERT INTO mail_table VALUES (?, ?, ?, ?, ?)", mails)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_mail_folder_db(folders: list[tuple[str, str]]) -> bytes:
    """``mail_folder_table``: (folder_id, folder_name) -- see
    ``mail.py``'s own ``_m365_folder_names``/``_folder_names``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail_folder.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE mail_folder_table(folder_id TEXT, folder_name TEXT)")
        conn.executemany("INSERT INTO mail_folder_table VALUES (?, ?)", folders)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_mail_folder_hierarchy_db(folders: list[tuple[str, str, str, int]]) -> bytes:
    """The real M365 ``mail_folder_table`` schema
    (``folder_id``/``folder_name``/``parent_folder_id``/``is_root``) --
    distinct from ``_build_mail_folder_db``'s own 2-column schema, which
    stays untouched so no other test's schema shape shifts. ``folders``:
    (folder_id, folder_name, parent_folder_id, is_root)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail_folder.db"
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE mail_folder_table(folder_id TEXT, folder_name TEXT, parent_folder_id TEXT, is_root INTEGER)"
        )
        conn.executemany("INSERT INTO mail_folder_table VALUES (?, ?, ?, ?)", folders)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_gws_mail_db(mails: list[tuple[str, str, str]], memberships: list[tuple[int, str, str]]) -> bytes:
    """GWS's own ``mail_table`` — no ``parent_folder_id`` column at all,
    since GWS has no folder hierarchy, only many-to-many label membership
    — plus, inside this same object, the label *membership* half of
    ``mail_label_table`` (the join itself lives inside ``mail_db``, not a
    separate label db). ``mails``: (mail_id, subject, meta_object_id);
    ``memberships``: (row_id, mail_id, label_id)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute("CREATE TABLE mail_table(mail_id TEXT, subject TEXT, meta_object_id TEXT)")
        conn.executemany("INSERT INTO mail_table VALUES (?, ?, ?)", mails)
        conn.execute("CREATE TABLE mail_label_table(row_id INTEGER, mail_id TEXT, label_id TEXT)")
        conn.executemany("INSERT INTO mail_label_table VALUES (?, ?, ?)", memberships)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_gws_mail_label_db(labels: list[tuple[int, str, str, int]]) -> bytes:
    """GWS's label *definitions* half of ``mail_label_table``, living in
    its own, separate object — ``labels``: (row_id, label_id, label_name,
    label_type)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "mail_label.db"
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE mail_label_table(row_id INTEGER, label_id TEXT, label_name TEXT, label_type INTEGER)"
        )
        conn.executemany("INSERT INTO mail_label_table VALUES (?, ?, ?, ?)", labels)
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


def _write_saas_obj(tmp_path: Path, *, session_id: int, payloads: list[tuple[str, bytes]]) -> int:
    """Assembles ``payloads`` back-to-back behind one embedded ObjectDB,
    chunks and writes the whole thing as the version's one ``saas_obj``
    (file_map + Composition + Pool bucket) — the shared tail every
    ``_build_*_repo`` fixture builder below ends with. Returns the
    embedded ObjectDB's own length, which callers need to build the
    ``object_db_id`` they pass to ``_write_copy_target_version_db``."""
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
    return object_db_len


def _make_skel_and_attachment() -> tuple[bytes, bytes]:
    msg = EmailMessage()
    msg["Subject"] = "Hello"
    msg["From"] = "sender@example.com"
    msg.set_content("Mail body text")
    msg.add_attachment(b"", maintype="application", subtype="octet-stream", filename="file.bin")
    _mark_part(msg, "application/octet-stream", "ID-file")
    real_attachment = b"the real attachment bytes"
    return msg.as_bytes(), real_attachment


def _build_mail_repo(
    tmp_path: Path,
    *,
    session_id: int = 11,
    include_folder_names: bool = False,
    index_db_name: str = "mail_db",
    subject: str = "Hello",
) -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    mail_db_bytes = _build_mail_db([("mail-1", subject, "folder-1", "meta_1")])
    skel_bytes, attachment_bytes = _make_skel_and_attachment()
    meta_1 = json.dumps(
        {
            "version": "3.0",
            "content_list": [
                {
                    "fragment_id": "ID-skel",
                    "type": 0,
                    "file_name": "",
                    "content_id": "",
                    "object_id": "skel_obj",
                    "size": len(skel_bytes),
                },
                {
                    "fragment_id": "ID-file",
                    "type": 4,
                    "file_name": "file.bin",
                    "content_id": "",
                    "object_id": "att_obj",
                    "size": len(attachment_bytes),
                },
            ],
        }
    ).encode()

    payloads = [
        ("mail_svc", mail_db_bytes),
        ("meta_1", meta_1),
        ("skel_obj", skel_bytes),
        ("att_obj", attachment_bytes),
    ]
    if include_folder_names:
        payloads.append(("folder_svc", _build_mail_folder_db([("folder-1", "Inbox")])))
    object_db_len = _write_saas_obj(tmp_path, session_id=session_id, payloads=payloads)
    db_objects = [(index_db_name, "mail_svc")]
    if include_folder_names:
        db_objects.append(("mail_folder_db", "folder_svc"))
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=db_objects,
    )


def _build_mail_repo_with_timestamps(
    tmp_path: Path, mails: list[tuple[str, str, str, str, int]], *, session_id: int = 21
) -> None:
    """A listing-only fixture (like ``_build_gws_mail_repo``'s own -- no
    skeleton/attachment payloads at all, since ordering is proven purely
    through ``children()``, never ``unit()``) for multiple mails sharing
    one folder with distinct ``remote_timestamp`` values."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    mail_db_bytes = _build_mail_db_with_timestamps(mails)
    object_db_len = _write_saas_obj(tmp_path, session_id=session_id, payloads=[("mail_svc", mail_db_bytes)])
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=[("mail_db", "mail_svc")],
    )


def _build_mail_repo_with_folder_hierarchy(tmp_path: Path, *, session_id: int = 25) -> None:
    """A listing-only fixture (like ``_build_gws_mail_repo``'s own -- no
    skeleton/attachment payloads) for M365 Mail's real, nested folder
    hierarchy: "root" (the ``is_root`` anchor row) -> "inbox" (zero direct
    mail) -> "haha" (nested, has mail), and "sent" (top-level, has direct
    mail) alongside "inbox"."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    folders = [
        ("root", "RootLabel", "anchor", 1),
        ("inbox", "Inbox", "root", 0),
        ("sent", "Sent", "root", 0),
        ("haha", "haha", "inbox", 0),
    ]
    mails = [
        ("mail-1", "Under Sent", "sent", "meta_1"),
        ("mail-2", "Under Haha", "haha", "meta_2"),
    ]
    mail_db_bytes = _build_mail_db(mails)
    folder_db_bytes = _build_mail_folder_hierarchy_db(folders)
    object_db_len = _write_saas_obj(
        tmp_path, session_id=session_id, payloads=[("mail_svc", mail_db_bytes), ("folder_svc", folder_db_bytes)]
    )
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=[("mail_db", "mail_svc"), ("mail_folder_db", "folder_svc")],
    )


def _version() -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-mail"),
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
async def provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider]:
    _build_mail_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await MailProvider(repo, _version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


def _gws_version() -> Version:
    return Version(
        version_id=VersionId(62),
        version_uid=VersionUid("vuid-mail-gws"),
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


def _build_gws_mail_repo(tmp_path: Path, *, session_id: int = 15) -> None:
    """A GWS mailbox with two messages: ``mail-1`` carries one real
    label, ``mail-2`` carries none at all — the "no membership row for
    this mail_id" boundary ``extras_attr`` degrades on: a missing lookup
    returns ``{}``, so the result has no ``"labels"`` key at all, not an
    empty list."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version", "GW")

    mails = [("mail-1", "Hello", "meta_1"), ("mail-2", "No Labels", "meta_2")]
    memberships = [(1, "mail-1", "label-1")]  # mail-2 has none at all
    mail_db_bytes = _build_gws_mail_db(mails, memberships)
    mail_label_db_bytes = _build_gws_mail_label_db([(1, "label-1", "Important", 0)])

    payloads = [("mail_svc", mail_db_bytes), ("mail_label_svc", mail_label_db_bytes)]
    object_db_len = _write_saas_obj(tmp_path, session_id=session_id, payloads=payloads)
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_gws_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=[("mail_db", "mail_svc"), ("mail_label_db", "mail_label_svc")],
    )


@pytest.fixture
async def gws_provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider]:
    _build_gws_mail_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await MailProvider(repo, _gws_version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


class TestGwsTree:
    async def test_root_lists_a_single_synthetic_mail_group(self, gws_provider: SaasWorkloadProvider) -> None:
        # GWS has no folder hierarchy at all (group_column=None) — every
        # message sits in one synthetic root_name group instead of a
        # real, per-message folder the way M365's own tree resolves.
        groups = await gws_provider.children(gws_provider.root())
        assert len(groups) == 1
        assert groups[0].name == "Mail"
        assert groups[0].is_leaf is False

    async def test_group_lists_both_mails_with_real_label_names_and_the_empty_boundary(
        self, gws_provider: SaasWorkloadProvider
    ) -> None:
        [group] = await gws_provider.children(gws_provider.root())
        mails = await gws_provider.children(group)
        by_name = {mail.name: mail for mail in mails}
        assert set(by_name) == {"Hello", "No Labels"}
        # mail-1's one real label membership resolves through the object-name
        # index to its real definitions-table name, not the raw label_id.
        assert by_name["Hello"].attrs.get("labels") == ["Important"]
        # mail-2 has no membership row at all — extras_attr's own "found
        # nothing" degradation omits the key entirely, never an empty list.
        assert "labels" not in by_name["No Labels"].attrs


class TestMailDisplayName:
    """Direct unit tests for ``_mail_display_name`` -- isolated from
    ``MailProvider``'s own end-to-end wiring, which exercises the same
    empty-subject fallback via ``TestTree.test_folder_lists_an_empty_
    subject_mail_as_no_subject_not_the_raw_id`` below."""

    def test_empty_string_subject_gets_the_no_subject_label(self) -> None:
        assert _mail_display_name({"subject": ""}) == "(no subject)"

    def test_null_subject_gets_the_no_subject_label(self) -> None:
        assert _mail_display_name({"subject": None}) == "(no subject)"

    def test_real_subject_is_used_as_is(self) -> None:
        assert _mail_display_name({"subject": "Hello"}) == "Hello"


class TestTree:
    async def test_root_lists_the_folder(self, provider: SaasWorkloadProvider) -> None:
        folders = await provider.children(provider.root())
        assert len(folders) == 1
        assert folders[0].name == "folder-1"

    async def test_root_resolves_the_real_folder_name_when_mail_folder_db_is_present(self, tmp_path: Path) -> None:
        # Every other tree test uses the default fixture, which has no
        # mail_folder_db entry at all -- _m365_folder_names()'s real
        # id -> name resolution (as opposed to its "best-effort None"
        # fallback, which is what every other test actually exercises)
        # was never exercised.
        _build_mail_repo(tmp_path, include_folder_names=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                folders = await provider.children(provider.root())
                assert len(folders) == 1
                assert folders[0].name == "Inbox"
            finally:
                await provider.close()

    async def test_folder_lists_the_mail(self, provider: SaasWorkloadProvider) -> None:
        [folder] = await provider.children(provider.root())
        mails = await provider.children(folder)
        assert len(mails) == 1
        assert mails[0].name == "Hello"
        assert mails[0].kind is UnitKind.MAIL

    async def test_folder_lists_an_empty_subject_mail_as_no_subject_not_the_raw_id(self, tmp_path: Path) -> None:
        # An empty subject must show "(no subject)", never the raw mail_id,
        # since the exported .eml filename also falls back to it.
        _build_mail_repo(tmp_path, subject="")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                [folder] = await provider.children(provider.root())
                [mail] = await provider.children(folder)
                assert mail.name == "(no subject)"
                assert "mail-1" not in mail.name
            finally:
                await provider.close()

    async def test_unit_on_a_folder_raises(self, provider: SaasWorkloadProvider) -> None:
        [folder] = await provider.children(provider.root())
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(folder)

    async def test_folder_lists_mail_newest_first(self, tmp_path: Path) -> None:
        _build_mail_repo_with_timestamps(
            tmp_path,
            [
                ("mail-1", "Oldest", "folder-1", "meta_1", 100),
                ("mail-2", "Newest", "folder-1", "meta_2", 300),
                ("mail-3", "Middle", "folder-1", "meta_3", 200),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                [folder] = await provider.children(provider.root())
                mails = await provider.children(folder)
                assert [mail.name for mail in mails] == ["Newest", "Middle", "Oldest"]
            finally:
                await provider.close()

    async def test_folder_lists_the_mail_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``children_of()`` for one folder is exactly one ``WHERE
        parent_folder_id = ?`` query."""
        from synology_apm_repo.sdk.storage.table import Table

        calls: list[object] = []
        original_select = Table.select

        def counting_select(
            self: Table,
            where: str = "",
            params: Sequence[object] = (),
            *,
            order_by: str | None = None,
            limit: int | None = None,
            offset: int = 0,
        ) -> AsyncIterator[dict[str, object | None]]:
            calls.append(1)
            return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

        monkeypatch.setattr(Table, "select", counting_select)

        [folder] = await provider.children(provider.root())
        calls.clear()
        await provider.children(folder)
        assert len(calls) == 1


class TestM365RealFolderHierarchy:
    """The real, nested M365 mail_folder_table hierarchy
    (RecursiveGroupFlatTree) -- Inbox has zero direct mail but a real
    nested subfolder (haha) that does; Sent has direct mail of its own."""

    async def test_root_lists_real_folder_names_including_a_zero_mail_folder(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                folders = await provider.children(provider.root())
                # Inbox has zero direct mail -- it must still appear.
                assert {folder.name for folder in folders} == {"Inbox", "Sent"}
                assert all(folder.is_leaf is False for folder in folders)
            finally:
                await provider.close()

    async def test_the_zero_mail_folder_still_shows_its_real_nested_subfolder(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                [inbox] = [f for f in await provider.children(provider.root()) if f.name == "Inbox"]
                children = await provider.children(inbox)
                assert [child.name for child in children] == ["haha"]
                assert children[0].is_leaf is False
            finally:
                await provider.close()

    async def test_the_nested_subfolder_lists_its_own_mail(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                [inbox] = [f for f in await provider.children(provider.root()) if f.name == "Inbox"]
                [haha] = await provider.children(inbox)
                mails = await provider.children(haha)
                assert [mail.name for mail in mails] == ["Under Haha"]
                assert mails[0].kind is UnitKind.MAIL
            finally:
                await provider.close()

    async def test_a_folder_with_direct_mail_lists_it(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                [sent] = [f for f in await provider.children(provider.root()) if f.name == "Sent"]
                mails = await provider.children(sent)
                assert [mail.name for mail in mails] == ["Under Sent"]
            finally:
                await provider.close()

    async def test_unit_on_a_folder_raises(self, tmp_path: Path) -> None:
        _build_mail_repo_with_folder_hierarchy(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                [inbox] = [f for f in await provider.children(provider.root()) if f.name == "Inbox"]
                with pytest.raises(ValueError, match="not a restorable unit"):
                    await provider.unit(inbox)
            finally:
                await provider.close()


class TestArchiveMailFolderHierarchy:
    """Archive Mail shares mail.py's exact same hierarchy wiring (both
    configs go through the same _make_build_tree/_open_m365_folder_tree
    machinery) -- one representative test proves it's actually reached
    for Archive too, not just regular Mail."""

    async def test_root_lists_real_folder_names_including_a_zero_mail_folder(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
        _write_saas_version_db(stream_db_dir / "saas_version")

        folders = [
            ("root", "RootLabel", "anchor", 1),
            ("inbox", "Inbox", "root", 0),
            ("haha", "haha", "inbox", 0),
        ]
        mails = [("mail-1", "Under Haha", "haha", "meta_1")]
        mail_db_bytes = _build_mail_db(mails)
        folder_db_bytes = _build_mail_folder_hierarchy_db(folders)
        object_db_len = _write_saas_obj(
            tmp_path, session_id=27, payloads=[("mail_svc", mail_db_bytes), ("folder_svc", folder_db_bytes)]
        )
        _write_copy_target_version_db(
            tmp_path / "db" / "copy_target_version",
            version_uid=_version().version_uid,
            object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
            db_objects=[("archive_mail_db", "mail_svc"), ("archive_mail_folder_db", "folder_svc")],
        )

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await ArchiveMailProvider(repo, _version(), saas_streams)
            try:
                [inbox] = await provider.children(provider.root())
                assert inbox.name == "Inbox"
                [haha] = await provider.children(inbox)
                assert haha.name == "haha"
                mails_list = await provider.children(haha)
                assert [mail.name for mail in mails_list] == ["Under Haha"]
            finally:
                await provider.close()


class TestUnit:
    async def test_builds_a_valid_eml_with_the_real_attachment_bytes(self, provider: SaasWorkloadProvider) -> None:
        [folder] = await provider.children(provider.root())
        [mail] = await provider.children(folder)
        content = (await provider.unit(mail)).open()
        # LazyArtifact.size is None until assembled (see TestSize in
        # test_units_saas_artifact.py) — read the whole artifact instead.
        data = await content.read()

        reparsed = email.message_from_bytes(data, policy=email.policy.compat32)
        assert reparsed["Subject"] == "Hello"
        [attachment] = [p for p in reparsed.walk() if p.get_filename() == "file.bin"]
        assert attachment.get_payload(decode=True) == b"the real attachment bytes"
        assert attachment.get("X-ABL-ID") is None

    async def test_unit_name_has_eml_extension(self, provider: SaasWorkloadProvider) -> None:
        [folder] = await provider.children(provider.root())
        [mail] = await provider.children(folder)
        unit = await provider.unit(mail)
        assert unit.name == "Hello.eml"

    async def test_unit_name_for_an_empty_subject_mail_is_no_subject_not_the_raw_id(self, tmp_path: Path) -> None:
        _build_mail_repo(tmp_path, subject="")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await MailProvider(repo, _version(), saas_streams)
            try:
                [folder] = await provider.children(provider.root())
                [mail] = await provider.children(folder)
                unit = await provider.unit(mail)
                assert unit.name == "(no subject).eml"
            finally:
                await provider.close()


class TestAssembleEmlErrors:
    async def test_missing_skeleton_raises_data_corrupt_on_first_access(self, tmp_path: Path) -> None:
        _build_mail_repo_without_skeleton(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await MailProvider(repo, _version(), saas_streams)
            try:
                [folder] = await p.children(p.root())
                [mail] = await p.children(folder)
                unit = await p.unit(mail)  # must not raise here — lazy assembly
                with pytest.raises(DataCorruptError, match="no skeleton"):
                    await unit.open().read()
            finally:
                await p.close()

    async def test_meta_bytes_not_valid_json_raises_data_corrupt_on_first_access(self, tmp_path: Path) -> None:
        _build_mail_repo_with_malformed_meta(tmp_path)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await MailProvider(repo, _version(), saas_streams)
            try:
                [folder] = await p.children(p.root())
                [mail] = await p.children(folder)
                unit = await p.unit(mail)  # must not raise here — lazy assembly
                with pytest.raises(DataCorruptError, match="did not parse as JSON"):
                    await unit.open().read()
            finally:
                await p.close()


def _build_mail_repo_with_malformed_meta(tmp_path: Path, *, session_id: int = 13) -> None:
    """Same shape as ``_build_mail_repo`` but the META object's bytes
    aren't valid JSON at all (a genuinely corrupted repository, as
    opposed to ``_build_mail_repo_without_skeleton``'s well-formed-but-
    incomplete META)."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    mail_db_bytes = _build_mail_db([("mail-1", "Hello", "folder-1", "meta_1")])
    meta_1 = b"not json at all"

    payloads = [("mail_svc", mail_db_bytes), ("meta_1", meta_1)]
    object_db_len = _write_saas_obj(tmp_path, session_id=session_id, payloads=payloads)
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=[("mail_db", "mail_svc")],
    )


def _build_mail_repo_without_skeleton(tmp_path: Path, *, session_id: int = 12) -> None:
    """Same shape as ``_build_mail_repo`` but the META's
    ``content_list`` omits the type=0 skeleton entry entirely."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    mail_db_bytes = _build_mail_db([("mail-1", "Hello", "folder-1", "meta_1")])
    meta_1 = json.dumps({"version": "3.0", "content_list": []}).encode()

    payloads = [("mail_svc", mail_db_bytes), ("meta_1", meta_1)]
    object_db_len = _write_saas_obj(tmp_path, session_id=session_id, payloads=payloads)
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=[("mail_db", "mail_svc")],
    )


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_mail_table_exists(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
        _write_saas_version_db(stream_db_dir / "saas_version")

        content = b"\x00" * 4096
        saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, 11, 64, 1, 2)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=11, num_chunks=1)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", [content])

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError):
                await MailProvider(repo, _version(), saas_streams)

    async def test_tree_factory_failure_closes_resources_instead_of_leaking_them(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test: a ``tree_factory`` failure happens after every
        table is already open (``self._sources``/``self._object_db_cache``/
        ``self._stream``) -- ``create()`` must close them rather than only
        closing on the table-opening loop's own ``UnsupportedDataFormatError``."""
        _build_mail_repo(tmp_path)

        async def failing_tree_factory(provider: SaasWorkloadProvider) -> TreeStrategy:
            raise RuntimeError("synthetic tree_factory failure")

        failing_config = dataclasses.replace(MAIL_CONFIG, tree_factory=failing_tree_factory)

        closed_instances: list[SaasWorkloadProvider] = []
        original_close = SaasWorkloadProvider.close

        async def spy_close(self: SaasWorkloadProvider) -> None:
            closed_instances.append(self)
            await original_close(self)

        monkeypatch.setattr(SaasWorkloadProvider, "close", spy_close)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(RuntimeError, match="synthetic tree_factory failure"):
                await SaasWorkloadProvider.create(repo, _version(), failing_config, saas_streams)
        assert len(closed_instances) == 1

    async def test_tree_factory_schema_drift_degrades_to_unsupported_data_format(self, tmp_path: Path) -> None:
        """A ``tree_factory`` reading its own secondary table
        (``DataCorruptError``/``sqlite3.DatabaseError``, the same shape a
        connector-version schema mismatch produces) must convert to
        ``UnsupportedDataFormatError``, matching
        ``_open_table_via_index``'s own conversion, so one candidate's
        schema drift degrades like any other non-match instead of
        crashing ``saas_provider_for``'s whole dispatch loop."""
        _build_mail_repo(tmp_path)

        async def failing_tree_factory(provider: SaasWorkloadProvider) -> TreeStrategy:
            raise DataCorruptError("synthetic schema drift")

        failing_config = dataclasses.replace(MAIL_CONFIG, tree_factory=failing_tree_factory)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError):
                await SaasWorkloadProvider.create(repo, _version(), failing_config, saas_streams)


class TestArchiveMailProvider:
    """Direct tests for ``ArchiveMailProvider``/``ARCHIVE_MAIL_CONFIG`` --
    zero coverage anywhere else in this file despite being a real,
    separate M365-only mailbox with a schema byte-for-byte identical to
    regular Mail's, just resolved under the index's ``archive_mail_db``
    name instead of ``mail_db`` -- reuses ``_build_mail_repo`` with
    ``index_db_name="archive_mail_db"``."""

    async def test_root_group_is_named_archive_not_all_mail(self, tmp_path: Path) -> None:
        _build_mail_repo(tmp_path, index_db_name="archive_mail_db")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await ArchiveMailProvider(repo, _version(), saas_streams)
            try:
                assert provider.root().name == "Archive"
                folders = await provider.children(provider.root())
                assert len(folders) == 1
                folder = folders[0]
                mails = await provider.children(folder)
                assert len(mails) == 1
                assert mails[0].name == "Hello"
                assert mails[0].kind is UnitKind.MAIL
            finally:
                await provider.close()

    async def test_raises_unsupported_data_format_when_the_catalog_has_no_archive_mail_db(self, tmp_path: Path) -> None:
        # The default fixture registers "mail_db", never "archive_mail_db"
        # -- ArchiveMailProvider's own documented "no scan fallback"
        # contract.
        _build_mail_repo(tmp_path)  # index_db_name defaults to "mail_db"
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError):
                await ArchiveMailProvider(repo, _version(), saas_streams)
