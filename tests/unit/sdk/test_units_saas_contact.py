"""Unit tests for ``synology_apm_repo.sdk.units.saas.contact`` — a
full synthetic repository root (same building blocks as
``test_units_saas_calendar.py``). A real-sample regression test also
exists (``tests/integration/sdk/test_units_saas_contact.py``), but only
proves dispatch and the top-level bucket's existence against real
GWS/M365 Contact workloads — no SaaS stream in apv-sample-1 has real
per-contact content recorded, so listing, grouping, and CSV/JSON-content
correctness (same posture as ``units/device.py``'s unverified PC/PS path)
are proven only here."""

from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import AsyncIterator, Sequence
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
from synology_apm_repo.sdk.storage.table import Table
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.content.saas_contact import build_contact_csv
from synology_apm_repo.sdk.units.saas.contact import ContactProvider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_STREAM_ID = 14
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "contact-stream-uuid"


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


def _write_saas_version_db(path: Path, target_type: str) -> None:
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
    its service DB(s) *only* through this table, with no scan-based
    fallback, so a fixture repository that wants a table found must
    record it here rather than merely embedding the bytes somewhere in
    ``saas_obj``. Plain, unencrypted JSON — these fixture repositories
    never configure a vault_key, matching every other db this file writes."""
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


def _build_m365_contact_db(
    contacts: list[tuple[str, str, str, str, str]], *, emails: dict[str, str] | None = None
) -> bytes:
    """``contacts``: (contact_id, first_name, last_name, parent_folder_id, meta_object_id).
    ``emails`` (contact_id -> primary_email), when given, populates the
    real ``primary_email`` column — every existing call site omits it,
    leaving that column empty."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "contact.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE contact_table(contact_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, "
            "parent_folder_id TEXT, meta_object_id TEXT, primary_email TEXT)"
        )
        conn.executemany(
            "INSERT INTO contact_table VALUES (?, ?, ?, ?, ?, ?)",
            [(*row, (emails or {}).get(row[0], "")) for row in contacts],
        )
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_gws_contact_db(
    contacts: list[tuple[str, str, str, str]], group_memberships: tuple[tuple[str, str], ...] = ()
) -> bytes:
    """``contacts``: (contact_id, first_name, last_name, meta_object_id) — no folder column.
    ``group_memberships`` (contact_id, group_id), when given, also
    populates ``contact_group_table`` inside this same object — membership
    rows live alongside ``contact_table`` here, while group definitions
    live in the separate ``contact_group_db`` object (``_build_group_db``)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "contact.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE contact_table(contact_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, "
            "meta_object_id TEXT)"
        )
        conn.executemany("INSERT INTO contact_table VALUES (?, ?, ?, ?)", contacts)
        if group_memberships:
            conn.execute("CREATE TABLE contact_group_table(contact_id TEXT, group_id TEXT)")
            conn.executemany("INSERT INTO contact_group_table VALUES (?, ?)", group_memberships)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_group_db(groups: list[tuple[str, str]]) -> bytes:
    """``group_table``: (group_id, group_name) -- GWS's own group
    *definitions*, a separate object from the membership half above."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "group.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE group_table(group_id TEXT, group_name TEXT)")
        conn.executemany("INSERT INTO group_table VALUES (?, ?)", groups)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_contact_folder_db(folders: list[tuple[str, str]]) -> bytes:
    """``contact_folder_table``: (folder_id, folder_name) -- M365's own
    folder *definitions*, consumed by ``contact.py``'s
    ``_m365_contact_folder_names``."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "contact_folder.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE contact_folder_table(folder_id TEXT, folder_name TEXT)")
        conn.executemany("INSERT INTO contact_folder_table VALUES (?, ?)", folders)
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


_M365_META = json.dumps(
    {
        "version": "1.0",
        "client_metadata": {
            "givenName": "Ada",
            "middleName": "",
            "surname": "Lovelace",
            "emailAddresses": [{"address": "ada@example.com"}],
            "businessPhones": ["555-0100"],
            "jobTitle": "Mathematician",
            "companyName": "Analytical Engines Ltd",
            "businessAddress": {"street": "1 Engine St", "city": "London", "countryOrRegion": "UK"},
        },
        "contact_type": "Contact",
    }
).encode()

_GWS_META = json.dumps(
    {
        "version": "2.0",
        "client_metadata": {"names": [{"givenName": "Grace", "familyName": "Hopper"}]},
        "photo_size": 100,
        "photo_hash": "abc",
        "photo_object_id": "v1_object_photo",
    }
).encode()


def _build_contact_repo(
    tmp_path: Path,
    *,
    session_id: int = 9,
    is_m365: bool = True,
    include_folder_names: bool = False,
    include_groups: bool = False,
    emails: dict[str, str] | None = None,
) -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version", "M365" if is_m365 else "GW")

    if is_m365:
        contact_db_bytes = _build_m365_contact_db(
            [("contact-1", "Ada", "Lovelace", "folder-1", "meta_1")], emails=emails
        )
    else:
        group_memberships = (("contact-1", "group-1"),) if include_groups else ()
        contact_db_bytes = _build_gws_contact_db(
            [("contact-1", "Grace", "Hopper", "meta_1")], group_memberships=group_memberships
        )

    meta_bytes = _M365_META if is_m365 else _GWS_META
    payloads = [("contact_svc", contact_db_bytes), ("meta_1", meta_bytes)]
    if is_m365 and include_folder_names:
        payloads.append(("folder_svc", _build_contact_folder_db([("folder-1", "My Contacts")])))
    if not is_m365 and include_groups:
        payloads.append(("group_svc", _build_group_db([("group-1", "Friends")])))
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
    db_objects = [("contact_db", "contact_svc")]
    if is_m365 and include_folder_names:
        db_objects.append(("contact_folder_db", "folder_svc"))
    if not is_m365 and include_groups:
        db_objects.append(("contact_group_db", "group_svc"))
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid="vuid-contact",
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=db_objects,
    )


def _install_call_counting_select(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Sequence[object]]]:
    """Wraps ``Table.select`` to record every ``(where, params)`` call
    it receives (still delegating to the real implementation), returning
    the list those calls land in — the query-shape assertion this backs
    only makes sense once the tree has already been drilled once, so
    every caller clears the list after that setup and before the one
    real call under test."""
    calls: list[tuple[str, Sequence[object]]] = []
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
        calls.append((where, params))
        return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

    monkeypatch.setattr(Table, "select", counting_select)
    return calls


def _version(target_type: str) -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-contact"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(_CCID),
        target_type=target_type,
        target_id=TargetId(_STREAM_UUID),
        saas_stream_uuid=StreamUuid(_STREAM_UUID),
        saas_snapshot_uuid=SnapshotUuid("snap-uuid"),
        saas_version_id=SaasVersionId(3),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


class TestM365Tree:
    @pytest.fixture
    async def provider(self, tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider]:
        _build_contact_repo(tmp_path, is_m365=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await ContactProvider(repo, _version("M365"), saas_streams)
            try:
                yield p
            finally:
                await p.close()

    async def test_root_lists_the_folder(self, provider: SaasWorkloadProvider) -> None:
        folders = await provider.children(provider.root())
        assert len(folders) == 1
        assert folders[0].name == "folder-1"

    async def test_folder_lists_the_contact(self, provider: SaasWorkloadProvider) -> None:
        [folder] = await provider.children(provider.root())
        contacts = await provider.children(folder)
        assert len(contacts) == 1
        assert contacts[0].name == "Ada Lovelace"
        assert contacts[0].kind is UnitKind.CONTACT

    async def test_contact_with_a_real_email_exposes_it_as_an_attr(self, tmp_path: Path) -> None:
        _build_contact_repo(tmp_path, is_m365=True, emails={"contact-1": "ada@example.com"})
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await ContactProvider(repo, _version("M365"), saas_streams) as provider,
        ):
            [folder] = await provider.children(provider.root())
            [contact] = await provider.children(folder)
            assert contact.attrs.get("email") == "ada@example.com"

    async def test_unit_builds_a_csv_with_bom(self, provider: SaasWorkloadProvider) -> None:
        [folder] = await provider.children(provider.root())
        [contact] = await provider.children(folder)
        content = (await provider.unit(contact)).open()
        # LazyArtifact.size is None until assembled (see TestSize in
        # test_units_saas_artifact.py) — read the whole artifact instead.
        data = await content.read()
        assert data.startswith(b"\xef\xbb\xbf")
        text = data.decode("utf-8-sig")
        assert "Ada" in text
        assert "ada@example.com" in text
        assert "Analytical Engines Ltd" in text

    async def test_unit_name_has_csv_extension(self, provider: SaasWorkloadProvider) -> None:
        [folder] = await provider.children(provider.root())
        [contact] = await provider.children(folder)
        unit = await provider.unit(contact)
        assert unit.name == "Ada Lovelace.csv"

    async def test_unit_on_a_folder_raises(self, provider: SaasWorkloadProvider) -> None:
        [folder] = await provider.children(provider.root())
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(folder)

    async def test_folder_lists_the_contact_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = _install_call_counting_select(monkeypatch)

        [folder] = await provider.children(provider.root())
        calls.clear()
        await provider.children(folder)
        assert calls == [("parent_folder_id = ?", ("folder-1",))]

    async def test_root_resolves_the_real_folder_name_when_contact_folder_db_is_present(self, tmp_path: Path) -> None:
        # _m365_contact_folder_names()'s real id -> name resolution was
        # never exercised -- the shared ``provider`` fixture's default
        # repository has no contact_folder_db entry, so every other test here
        # only sees the raw-folder-id "best-effort None" fallback.
        _build_contact_repo(tmp_path, is_m365=True, include_folder_names=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await ContactProvider(repo, _version("M365"), saas_streams)
            try:
                folders = await provider.children(provider.root())
                assert len(folders) == 1
                assert folders[0].name == "My Contacts"
            finally:
                await provider.close()


class TestGwsTree:
    @pytest.fixture
    async def provider(self, tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider]:
        _build_contact_repo(tmp_path, is_m365=False)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await ContactProvider(repo, _version("GW"), saas_streams)
            try:
                yield p
            finally:
                await p.close()

    async def test_root_lists_a_single_synthetic_group(self, provider: SaasWorkloadProvider) -> None:
        groups = await provider.children(provider.root())
        assert len(groups) == 1
        assert groups[0].name == "Contacts"

    async def test_group_lists_the_contact(self, provider: SaasWorkloadProvider) -> None:
        [group] = await provider.children(provider.root())
        contacts = await provider.children(group)
        assert len(contacts) == 1
        assert contacts[0].name == "Grace Hopper"

    async def test_group_lists_the_contact_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GWS has no ``parent_folder_id`` column at all (``group_column
        =None``, since GWS Contact's groups are M:N, not a single column)
        — the single synthetic group's own members are still exactly one
        ``ORDER BY
        ... LIMIT ? OFFSET ?`` query with no ``WHERE`` at all, not a
        full-table scan building an in-memory index."""
        calls = _install_call_counting_select(monkeypatch)

        [group] = await provider.children(provider.root())
        calls.clear()
        await provider.children(group)
        assert calls == [("", ())]

    async def test_contact_gets_its_real_gws_group_names_as_an_extra_attr(self, tmp_path: Path) -> None:
        # _gws_contact_groups() has zero coverage anywhere in this file
        # -- the shared ``provider`` fixture's default repository has no
        # contact_group_db/contact_group_table membership at all.
        _build_contact_repo(tmp_path, is_m365=False, include_groups=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await ContactProvider(repo, _version("GW"), saas_streams)
            try:
                [group] = await provider.children(provider.root())
                [contact] = await provider.children(group)
                assert contact.attrs.get("groups") == ["Friends"]
            finally:
                await provider.close()

    async def test_unit_returns_raw_meta_json_not_csv(self, provider: SaasWorkloadProvider) -> None:
        [group] = await provider.children(provider.root())
        [contact] = await provider.children(group)
        content = (await provider.unit(contact)).open()
        # LazyArtifact.size is None until assembled (see TestSize in
        # test_units_saas_artifact.py) — read the whole artifact instead.
        data = await content.read()
        parsed = json.loads(data)
        assert parsed["version"] == "2.0"
        assert parsed["photo_object_id"] == "v1_object_photo"

    async def test_unit_name_has_json_extension(self, provider: SaasWorkloadProvider) -> None:
        [group] = await provider.children(provider.root())
        [contact] = await provider.children(group)
        unit = await provider.unit(contact)
        assert unit.name == "Grace Hopper.json"


def _parsed_csv_row(csv_bytes: bytes) -> dict[str, str]:
    text = csv_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    row = next(reader)
    return dict(zip(header, row, strict=True))


class TestBuildContactCsv:
    def test_meta_bytes_not_valid_json_raises_data_corrupt(self) -> None:
        with pytest.raises(DataCorruptError):
            build_contact_csv(b"not json at all")

    def test_missing_optional_fields_become_empty_columns(self) -> None:
        meta = json.dumps({"client_metadata": {"givenName": "Bare"}}).encode()
        csv_bytes = build_contact_csv(meta)
        parsed = _parsed_csv_row(csv_bytes)
        assert parsed["First Name"] == "Bare"
        for column, value in parsed.items():
            if column != "First Name":
                assert value == "", f"{column!r} expected to be empty, got {value!r}"

    @pytest.mark.parametrize(
        "client_metadata",
        [{}, {"jobTitle": None}, {"jobTitle": ""}],
        ids=["missing_key", "null_value", "empty_string"],
    )
    def test_job_title_or_default_catches_null_empty_and_missing(self, client_metadata: dict[str, object]) -> None:
        meta = json.dumps({"client_metadata": {**client_metadata, "givenName": "X"}}).encode()
        parsed = _parsed_csv_row(build_contact_csv(meta))
        assert parsed["Job Title"] == ""

    @pytest.mark.parametrize(
        "business_address",
        [{}, {"street": None}, {"street": ""}],
        ids=["missing_key", "null_value", "empty_string"],
    )
    def test_business_street_or_default_catches_null_empty_and_missing(
        self, business_address: dict[str, object]
    ) -> None:
        meta = json.dumps({"client_metadata": {"givenName": "X", "businessAddress": business_address}}).encode()
        parsed = _parsed_csv_row(build_contact_csv(meta))
        assert parsed["Business Street"] == ""

    @pytest.mark.parametrize(
        "client_metadata",
        [{"givenName": "X"}, {"givenName": "X", "businessAddress": None}, {"givenName": "X", "businessAddress": ""}],
        ids=["missing_key", "null_value", "empty_string"],
    )
    def test_business_address_itself_or_default_catches_null_empty_and_missing(
        self, client_metadata: dict[str, object]
    ) -> None:
        """``address = client_metadata.get("businessAddress") or {}`` —
        the nested-JSON ``.get(key) or default`` pattern CLAUDE.md calls
        out, exercised one level up from the per-field fallbacks above."""
        meta = json.dumps({"client_metadata": client_metadata}).encode()
        parsed = _parsed_csv_row(build_contact_csv(meta))
        assert parsed["Business Street"] == ""

    def test_malformed_non_dict_first_email_is_treated_as_absent(self) -> None:
        meta = json.dumps({"client_metadata": {"givenName": "X", "emailAddresses": ["not-a-dict"]}}).encode()
        parsed = _parsed_csv_row(build_contact_csv(meta))
        assert parsed["E-mail Address"] == ""


class TestDisplayName:
    def test_is_blank_when_both_names_are_blank(self) -> None:
        # Never falls back to the raw contact_id -- for GWS that's an
        # opaque People API resource name, not something a user would
        # want to see.
        from synology_apm_repo.sdk.units.saas.contact import _display_name

        row: dict[str, object | None] = {"first_name": "", "last_name": None, "contact_id": "contact-123"}
        assert _display_name(row) == ""


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_contact_table_exists(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
        _write_saas_version_db(stream_db_dir / "saas_version", "M365")

        content = b"\x00" * 4096
        saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, 9, 64, 1, 2)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9, num_chunks=1)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", [content])

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError):
                await ContactProvider(repo, _version("M365"), saas_streams)
