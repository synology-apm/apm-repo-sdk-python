"""Regression tests for SaaS workload dispatch/listing — replayed from
committed fixtures recorded against real bytes, with **no external
dependency**: this always runs, on CI or anywhere else, because it goes
through ``ReplayStore`` instead of a real ``LocalFsStore``.

Deliberately narrow for Mail/Calendar: each test below only proves a real
workload dispatches to the right provider and lists its own folder/
calendar/event count correctly — none of them read an individual real
message's body or event's own fields. A message's body and an event's
summary/organizer/dates *are* their content (unlike a filename): reading
either would make ``RecordingStore`` capture that real content into the
committed fixture regardless of what the test then asserts, since
narrowing the assertion can't undo the capture. Mail's ``X-ABL-ID`` reassembly/attachment round-trip fidelity is covered
synthetically by this file's own ``test_eml_fully_parses_with_byte_
identical_attachment`` (below) and by
``tests/unit/sdk/test_units_saas_mail.py``; Calendar's ICS-building
correctness by ``tests/unit/sdk/test_units_saas_calendar.py``.

Fixtures, all recorded against the plaintext ``apv-sample-1`` vault (no
key material involved) via each test's own ``record_target()`` call —
see ``tests/conftest.py`` and ``tests/CLAUDE.md``'s "Recording a fixture"
section for the ``pytest --record-against=...`` workflow that
(re-)records these:

- ``saas_content_mail_family_apv1.json.gz`` — a real MAIL workload, a
  real GROUP_EXCHANGE workload's Mail+Calendar siblings, and a real
  USER_EXCHANGE workload's Archive Mail sibling: just each workload's
  own index resolution and top-level folder/group listing.
- ``saas_content_mail_m365_grace_apv1.json.gz`` — a real M365
  USER_EXCHANGE workload: same narrow scope.
- ``saas_content_calendar_alice_apv1.json.gz`` — the real CALENDAR
  workload on stream ``KxMWSUvtSZiaDTDy``: just its calendar/event
  counts.

Drive isn't covered here at all:
``tests/integration/sdk/test_units_saas_drive.py``'s own
real-content hash cross-check already covers everything a Drive
scenario in this file would (dispatch via the object-name index included —
see that file's own third test), so this file doesn't need its own copy
of that real data.

The synthetic Mail round-trip test at the bottom
(``test_eml_fully_parses_with_byte_identical_attachment``) builds a
hand-built repository under ``tmp_path``, with zero real-sample
dependency (no ``samples_dir``/``ReplayStore`` involved).
"""

from __future__ import annotations

import email
import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import AsyncIterator, Awaitable, Callable
from email.message import EmailMessage
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import Version, versions
from synology_apm_repo.sdk.catalog.workload import workloads
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
)
from synology_apm_repo.sdk.storage import LocalFsStore
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, detect_layout
from synology_apm_repo.sdk.units.base import Node, UnitProvider
from synology_apm_repo.sdk.units.dispatch import saas_provider_for
from synology_apm_repo.sdk.units.saas.calendar import CalendarProvider
from synology_apm_repo.sdk.units.saas.composite_provider import CompositeSaasProvider
from synology_apm_repo.sdk.units.saas.mail import MailProvider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

#: Internal catalog identifiers -- stable and non-identifying (never
#: touched by catalog-metadata anonymization, so the same values resolve
#: the right workload whether replaying the anonymized fixture or
#: recording fresh against the real backend).
_MAIL_WORKLOAD_ID = 5
_GROUP_EXCHANGE_WORKLOAD_ID = 21
_ARCHIVE_MAIL_WORKLOAD_ID = 24
_M365_EXCHANGE_MAIL_WORKLOAD_ID = 19


async def _open_repo(record_target: Callable[..., Awaitable[ObjectStore]], fixture_name: str) -> DedupRepo:
    store = await record_target(fixture_name, allow_content=True)
    layout = await detect_layout(store)
    return await DedupRepo.open(store, layout)


@pytest.fixture
async def mail_family_repo(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> AsyncIterator[DedupRepo]:
    """Shared by the three tests below that all read
    ``saas_content_mail_family_apv1.json.gz``. Sharing one opened
    ``ReplayStore``-backed repository across tests is safe because it
    answers purely from a static dict, with no call-order tracking. Each
    test still builds and closes its own provider on top of it.
    Function-scoped (not module-scoped) because it depends on
    ``record_target``, itself function-scoped (default pytest fixture
    scope) — pytest forbids a wider-scoped fixture depending on a
    narrower-scoped one."""
    async with await _open_repo(record_target, "saas_content_mail_family_apv1.json.gz") as r:
        yield r


async def test_alice_mail_workload_resolves_to_its_folder_and_lists_its_messages_replayed(
    mail_family_repo: DedupRepo,
) -> None:
    repo = mail_family_repo
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == _MAIL_WORKLOAD_ID)
    version = next(v for v in await versions(repo, workload) if v.version_id == 9)
    async with SaasStreamCache(repo) as saas_streams:
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, SaasWorkloadProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            [folder] = await provider.children(provider.root())
            mails = await provider.children(folder)
            assert len(mails) == 25
        finally:
            await provider.close()


async def test_group_exchange_mail_and_calendar_resolve_via_the_catalog_index_replayed(
    mail_family_repo: DedupRepo,
) -> None:
    repo = mail_family_repo
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == _GROUP_EXCHANGE_WORKLOAD_ID)
    version = next(v for v in await versions(repo, workload) if v.version_id == 93)
    async with SaasStreamCache(repo) as saas_streams:
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, CompositeSaasProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            groups = await provider.children(provider.root())
            group_names = {g.name for g in groups}
            assert group_names == {"Mail", "Calendars"}, group_names

            mail_group = next(g for g in groups if g.name == "Mail")
            [folder] = await provider.children(mail_group)
            mails = await provider.children(folder)
            assert len(mails) == 1

            calendar_group = next(g for g in groups if g.name == "Calendars")
            # One more synthetic level than before this group's own root:
            # Calendar's own My/Other Calendars split.
            [my_calendars] = await provider.children(calendar_group)
            [calendar_node] = await provider.children(my_calendars)
            events = await provider.children(calendar_node)
            assert events == []
        finally:
            await provider.close()


async def test_archive_mail_is_a_real_sibling_alongside_regular_mail_replayed(
    mail_family_repo: DedupRepo,
) -> None:
    repo = mail_family_repo
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == _ARCHIVE_MAIL_WORKLOAD_ID)
    version = next(v for v in await versions(repo, workload) if v.version_id == 96)
    async with SaasStreamCache(repo) as saas_streams:
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, CompositeSaasProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            groups = await provider.children(provider.root())
            group_names = {g.name for g in groups}
            assert group_names == {"Mail", "Contacts", "Calendars", "Archive"}, group_names

            archive_group = next(g for g in groups if g.name == "Archive")
            folders = await provider.children(archive_group)
            assert folders == []
        finally:
            await provider.close()


async def _count_leaves(provider: UnitProvider, node: Node) -> int:
    """Recursively counts real leaf units under ``node`` — M365 Mail's
    real folder hierarchy (``RecursiveGroupFlatTree``) can nest a message
    under any depth of subfolders now, not just directly under one flat
    top-level bucket."""
    total = 0
    for child in await provider.children(node):
        total += 1 if child.is_leaf else await _count_leaves(provider, child)
    return total


async def test_m365_exchange_mail_workload_resolves_to_its_folder_and_lists_its_messages_replayed(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with (
        await _open_repo(record_target, "saas_content_mail_m365_grace_apv1.json.gz") as repo,
        SaasStreamCache(repo) as saas_streams,
    ):
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        workload = next(w for w in all_workloads if w.workload_id == _M365_EXCHANGE_MAIL_WORKLOAD_ID)
        version = next(v for v in await versions(repo, workload) if v.version_id == 91)
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, CompositeSaasProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            groups = await provider.children(provider.root())
            mail_group = next(g for g in groups if g.name == "Mail")
            # The real mail_folder_table hierarchy resolves now (more than
            # the single flat bucket a purely item-driven listing showed)
            # — count messages across however many real folders/subfolders
            # they're actually spread across, rather than assuming exactly
            # one top-level folder holds all of them.
            assert await _count_leaves(provider, mail_group) == 122
        finally:
            await provider.close()


async def test_calendar_workload_resolves_and_lists_a_real_event_count_replayed(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with (
        await _open_repo(record_target, "saas_content_calendar_alice_apv1.json.gz") as repo,
        SaasStreamCache(repo) as saas_streams,
    ):
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        candidates = [
            v
            for w in all_workloads
            if w.sub_type == "CALENDAR"
            for v in await versions(repo, w)
            if v.saas_stream_uuid == "KxMWSUvtSZiaDTDy" and not v.deleted
        ]
        version = max(candidates, key=lambda v: v.version_id)
        provider = await CalendarProvider(repo, version, saas_streams)
        try:
            checked = 0
            for category in await provider.children(provider.root()):
                for calendar in await provider.children(category):
                    checked += len(await provider.children(calendar))
            assert checked >= 41
        finally:
            await provider.close()


# -- Mail half: synthetic (CBT1's real mail_table has zero real mail rows) --
# A message's body and an attachment's bytes are content a real fixture
# can't narrow around -- reading either would make RecordingStore capture
# that real content regardless of what the test then asserts -- so
# attachment round-trip fidelity is proven synthetically instead, kept
# here alongside its real-replay siblings above rather than split into a
# separate tests/unit/ file.

_STREAM_ID = 18
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "synthetic-mail-stream"
_ATTACHMENT_BYTES = b"the exact original attachment bytes, byte for byte"


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
    conn.execute("INSERT INTO stream_info VALUES ('M365')")
    conn.commit()
    conn.close()


def _write_copy_target_version_db(
    path: Path, *, version_uid: str, object_db_id: str, db_objects: list[tuple[str, str]]
) -> None:
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


def _build_mail_repo(tmp_path: Path, *, session_id: int = 30) -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    mail_db_bytes = _build_mail_db([("mail-1", "Synthetic Test Email", "folder-1", "meta_1")])

    skel = EmailMessage()
    skel["Subject"] = "Synthetic Test Email"
    skel["From"] = "sender@example.com"
    skel.set_content("body text")
    skel.add_attachment(b"", maintype="application", subtype="octet-stream", filename="proof.bin")
    for part in skel.walk():
        if part.get_filename() == "proof.bin":
            part["X-ABL-ID"] = "ID-file"
    skel_bytes = skel.as_bytes()

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
                    "file_name": "proof.bin",
                    "content_id": "",
                    "object_id": "att_obj",
                    "size": len(_ATTACHMENT_BYTES),
                },
            ],
        }
    ).encode()

    payloads = [
        ("mail_svc", mail_db_bytes),
        ("meta_1", meta_1),
        ("skel_obj", skel_bytes),
        ("att_obj", _ATTACHMENT_BYTES),
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
        db_objects=[("mail_db", "mail_svc")],
    )


def _version() -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-p5-mail"),
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


async def test_eml_fully_parses_with_byte_identical_attachment(tmp_path: Path) -> None:
    """Synthetic reproduction of Mail's ``X-ABL-ID`` reassembly/attachment
    round-trip fidelity — a fully-controlled fixture,
    independent of any one sample's real data shape."""
    _build_mail_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await MailProvider(repo, _version(), saas_streams)
        try:
            [folder] = await provider.children(provider.root())
            [mail] = await provider.children(folder)
            content = (await provider.unit(mail)).open()
            data = await content.read()

            parsed = email.message_from_bytes(data, policy=email.policy.compat32)
            assert parsed["Subject"] == "Synthetic Test Email"

            [attachment] = [p for p in parsed.walk() if p.get_filename() == "proof.bin"]
            assert attachment.get_payload(decode=True) == _ATTACHMENT_BYTES
            assert attachment.get("X-ABL-ID") is None
        finally:
            await provider.close()


__all__: list[str] = []
