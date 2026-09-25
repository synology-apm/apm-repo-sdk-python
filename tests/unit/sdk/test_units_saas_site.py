"""Unit tests for ``synology_apm_repo.sdk.units.saas.site`` — a full
synthetic repository root (same building blocks as
``test_units_saas_drive.py``), with two real ZSTD-compressed service DBs
(``list_version_table`` + ``item_version_table``) embedded in its
``saas_obj`` content. This is the only coverage for Site's listing
mechanism, deliberately: unlike Mail/Drive, a Site workload's listing
always decodes this embedded service DB, so there is no real-data
integration test for it — a real recording would always carry real
content, with no metadata-only proxy to narrow down to (see
``CONTRIBUTING.md``'s "Sample data" section)."""

from __future__ import annotations

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
from synology_apm_repo.sdk.dedup.dedup_file import ByteRangeView
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
from synology_apm_repo.sdk.units.base import Node, UnitKind, mtime_from_epoch, node_leaf_kind
from synology_apm_repo.sdk.units.content.saas_site import build_values_json
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.site import (
    SiteProvider,
    _display_name,
    _is_document_library,
    _is_folder,
    _self_id_of,
)
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_STREAM_ID = 15
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "site-stream-uuid"

_FILE_CONTENT = b"the real document library file bytes"


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


def _build_list_db(lists: list[tuple[str, str, str, int, str, int]]) -> bytes:
    """``lists``: (list_id, list_title, meta_object_id, list_type,
    root_folder_id, create_time) — ``list_type`` 1 means document library,
    0 means a plain list; ``root_folder_id`` is that list's own top-level
    anchor, matched against a top-level item's own ``parent_folder_id``
    (a document library's top level is non-empty, unlike a plain list's
    ``""``, so without this match its items would appear empty);
    ``create_time`` is that list's own real creation epoch, read
    regardless of category."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "list.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE list_version_table(list_id TEXT PRIMARY KEY, list_title TEXT, "
            "meta_object_id TEXT, list_type INTEGER, root_folder_id TEXT, create_time INTEGER)"
        )
        conn.executemany("INSERT INTO list_version_table VALUES (?, ?, ?, ?, ?, ?)", lists)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_item_db(items: list[tuple[str, str, str, str, str, str, str | None, str, str]]) -> bytes:
    """``items``: (item_id, list_id, file_id, parent_folder_id, title,
    item_type, meta_object_id, url_path, value1) — ``url_path`` is what a
    document-library folder's own display name actually comes from (its
    ``title`` is empty); ``value1`` is a document-library file's own
    cached real byte size, gated on a non-empty ``file_id`` since it
    means something else entirely for a general list row."""
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


def _build_site_repo(
    tmp_path: Path,
    *,
    session_id: int = 10,
    extra_items: list[tuple[str, str, str, str, str, str, str | None, str, str]] | None = None,
    extra_payloads: list[tuple[str, bytes]] | None = None,
) -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    list_db_bytes = _build_list_db(
        [
            ("list-1", "Tasks", "meta_list_1", 0, "", 1_700_000_000),
            # A real document library's own top level is *not* the empty
            # string — "root-docs" here, matched by report.docx/Subfolder's
            # own parent_folder_id below, not "". Its own create_time (a real
            # column on both categories) is distinct from Tasks' own, so
            # a test can tell whether a given mtime actually came from
            # this list's own row rather than a copy-paste coincidence.
            ("list-2", "Docs", "meta_list_2", 1, "root-docs", 1_700_100_000),
        ]
    )
    item_db_bytes = _build_item_db(
        [
            # a plain list row (Tasks) — no file_id, no content in META
            ("1", "list-1", "", "", "Task A", "0", "meta_item_1", "", ""),
            # a document library file (Docs) — file content via content_list.
            # Title is deliberately a *content* title, not the real file
            # name — a real SharePoint page's title (e.g. "Home") commonly
            # differs from its underlying file name (e.g. "Home.aspx") —
            # url_path must win regardless.
            ("2", "list-2", "file-abc", "root-docs", "Quarterly Report", "FILE", "meta_item_2", "/report.docx", "36"),
            # a document library folder (Docs) with a nested file — empty
            # title (a real folder row's own title is empty; its display
            # name comes from url_path instead) and item_type "1" (the
            # numeric FileSystemObjectType encoding, not the string
            # "FOLDER" — both are real, interchangeable encodings
            # SharePoint uses.
            # value1 "null" (a real folder's own real value, never a
            # size) must not be misread as one.
            ("3", "list-2", "folder-xyz", "root-docs", "", "1", "meta_item_3", "/Subfolder", "null"),
            (
                "4",
                "list-2",
                "file-nested",
                "folder-xyz",
                "nested.txt",
                "FILE",
                "meta_item_4",
                "/Subfolder/nested.txt",
                "19",
            ),
            *(extra_items or []),
        ]
    )
    meta_item_1 = json.dumps({"version": "1.0", "values": {"Title": "Task A"}, "content_list": []}).encode()
    meta_item_2 = json.dumps(
        {"version": "1.0", "values": {}, "content_list": [{"type": 2, "object_id": "content_obj_2"}]}
    ).encode()
    meta_item_3 = json.dumps({"version": "1.0", "values": {}, "content_list": []}).encode()
    meta_item_4 = json.dumps(
        {"version": "1.0", "values": {}, "content_list": [{"type": 2, "object_id": "content_obj_4"}]}
    ).encode()

    payloads = [
        ("list_svc", list_db_bytes),
        ("item_svc", item_db_bytes),
        ("meta_item_1", meta_item_1),
        ("meta_item_2", meta_item_2),
        ("meta_item_3", meta_item_3),
        ("meta_item_4", meta_item_4),
        ("content_obj_2", _FILE_CONTENT),
        ("content_obj_4", b"nested file content"),
        ("meta_list_1", b'{"version": "1.0", "metadata": {}, "fields": {}, "views": {}}'),
        ("meta_list_2", b'{"version": "1.0", "metadata": {}, "fields": {}, "views": {}}'),
        *(extra_payloads or []),
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
        db_objects=[("site_list_db", "list_svc"), ("site_item_db", "item_svc")],
    )


def _version() -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-site"),
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
    _build_site_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await SiteProvider(repo, _version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


async def _all_lists(provider: SaasWorkloadProvider) -> list[Node]:
    """Flattens the "Document Library"/"List" category level
    (``TestCategorization`` covers that level itself) back into a flat list
    of List/Library nodes — what ``provider.children(provider.root())``
    itself returned before that category level existed. Keeps ``TestTree``/
    ``TestContent`` focused on per-list/per-item tree behavior."""
    categories = await provider.children(provider.root())
    lists: list[Node] = []
    for category in categories:
        lists.extend(await provider.children(category))
    return lists


class TestCategorization:
    async def test_root_lists_the_two_categories(self, provider: SaasWorkloadProvider) -> None:
        categories = await provider.children(provider.root())
        assert {n.name for n in categories} == {"Document Library", "List"}
        assert all(not n.is_leaf for n in categories)

    async def test_tasks_is_under_list_docs_is_under_document_library(self, provider: SaasWorkloadProvider) -> None:
        categories = {n.name: n for n in await provider.children(provider.root())}
        list_names = {n.name for n in await provider.children(categories["List"])}
        doc_library_names = {n.name for n in await provider.children(categories["Document Library"])}
        assert list_names == {"Tasks"}
        assert doc_library_names == {"Docs"}

    async def test_list_category_overrides_leaf_kind_to_category_group(self, provider: SaasWorkloadProvider) -> None:
        """The "List" category's own children are individual List group
        nodes, never leaves -- overriding its leaf_kind to CATEGORY_GROUP
        (rather than inheriting SITE_ITEM) is what lets the browser give
        it Name/Created columns instead of Document Library's Name/Size/
        Modified. Document Library keeps the provider's default SITE_ITEM,
        unaffected by this override."""
        categories = {n.name: n for n in await provider.children(provider.root())}
        assert node_leaf_kind(categories["List"]) is UnitKind.CATEGORY_GROUP
        assert node_leaf_kind(categories["Document Library"]) is UnitKind.SITE_ITEM

    async def test_a_list_group_node_is_flagged_for_overview_a_doc_library_is_not(
        self, provider: SaasWorkloadProvider
    ) -> None:
        """``site_list_overview`` is what ``unit_screen.py`` reads to (a)
        render a List's own group node as a non-expandable tree leaf and
        (b) build its spreadsheet-style overview instead — a document
        library keeps browsing like a plain folder tree, so it must not
        get this flag. Its items stay fetchable via ``provider.children()``
        regardless (see ``TestTree.test_tasks_list_has_one_plain_row``) —
        the browser's own overview reads them that way; only the browser
        decides not to make them tree-navigable."""
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        assert tasks.attrs.get("site_list_overview") is True
        assert "site_list_overview" not in docs.attrs

    async def test_both_a_list_and_a_doc_library_group_node_get_their_own_real_mtime(
        self, provider: SaasWorkloadProvider
    ) -> None:
        # A document library's own top-level group node has no other
        # real mtime source (its own children's individual mtimes don't
        # apply to the library itself) -- its own create_time fills the
        # same "Modified" column a List's "Created" column reuses.
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        assert tasks.attrs.get("mtime") == mtime_from_epoch(1_700_000_000)
        assert docs.attrs.get("mtime") == mtime_from_epoch(1_700_100_000)

    async def test_a_nested_document_library_folder_gets_no_list_level_mtime(
        self, provider: SaasWorkloadProvider
    ) -> None:
        # Only the library's own top-level group node (key length 2)
        # reuses the list's create_time -- a folder nested within it
        # (key length 3+) is a different node with no such column of
        # its own.
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        subfolder = next(n for n in await provider.children(docs) if n.name == "Subfolder")
        assert "mtime" not in subfolder.attrs


class TestTree:
    async def test_tasks_list_has_one_plain_row(self, provider: SaasWorkloadProvider) -> None:
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        items = await provider.children(tasks)
        assert len(items) == 1
        assert items[0].name == "Task A"
        assert items[0].is_leaf is True
        assert items[0].kind is UnitKind.SITE_ITEM
        # A plain list row's empty file_id gates _leaf_size off entirely --
        # value1 means something else per row for a general list, never a
        # byte size.
        assert items[0].size is None

    async def test_docs_list_has_a_file_and_a_folder_at_top_level(self, provider: SaasWorkloadProvider) -> None:
        """Also exercises three real fixes together: "Docs"' own top
        level is anchored at its non-empty ``root_folder_id``
        ("root-docs"), not the empty string (without ``root_folder_id_of``
        this whole list would appear empty); "report.docx"'s
        display name comes from its ``url_path``, not its own (deliberately
        different) content ``title`` "Quarterly Report" (a document-library
        item's ``title`` is a content title, not its real file name); and
        "Subfolder"'s display name comes from its ``url_path`` fallback
        too, since a real folder row's own ``title`` is empty."""
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        items = await provider.children(docs)
        names = {n.name for n in items}
        assert names == {"report.docx", "Subfolder"}
        report = next(n for n in items if n.name == "report.docx")
        subfolder = next(n for n in items if n.name == "Subfolder")
        assert report.is_leaf is True
        assert subfolder.is_leaf is False
        # report.docx's own real byte size (value1="36", matching
        # _FILE_CONTENT's real length) — a folder never reaches
        # _leaf_size at all (non-leaf), so it has no size regardless.
        assert report.size == len(_FILE_CONTENT)
        assert subfolder.size is None

    async def test_nested_folder_lists_its_own_file(self, provider: SaasWorkloadProvider) -> None:
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        subfolder = next(n for n in await provider.children(docs) if n.name == "Subfolder")
        nested = await provider.children(subfolder)
        assert [n.name for n in nested] == ["nested.txt"]
        assert nested[0].size == len(b"nested file content")

    async def test_docs_list_sorts_folders_first_then_files_by_display_name_not_title(self, tmp_path: Path) -> None:
        """Two more document-library files under "root-docs", each with a
        ``title`` that ranks in the *opposite* order from its real
        ``url_path``-derived display name — proves sorting follows the
        displayed name (``site.py``'s ``_NAME_ORDER_SQL``), not the raw
        ``title`` column, on top of the existing folder-before-files
        check ("Subfolder" still sorts before every file despite its own
        name starting with a capital "S")."""
        extra_items: list[tuple[str, str, str, str, str, str, str | None, str, str]] = [
            ("6", "list-2", "file-f6", "root-docs", "Zzz Title", "FILE", "meta_item_2", "/aaa-file.docx", "1"),
            ("7", "list-2", "file-f7", "root-docs", "Aaa Title", "FILE", "meta_item_2", "/zzz-file.docx", "1"),
        ]
        _build_site_repo(tmp_path, extra_items=extra_items)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await SiteProvider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                items = await provider.children(docs)
                assert [n.name for n in items] == ["Subfolder", "aaa-file.docx", "report.docx", "zzz-file.docx"]
            finally:
                await provider.close()

    async def test_docs_list_item_with_no_url_path_falls_back_to_title_for_sorting_too(self, tmp_path: Path) -> None:
        """Regression test: a document-library item with ``file_id`` set
        but an empty ``url_path`` -- ``_display_name`` falls back to
        ``title`` for it (the ``if url_path:`` guard). ``_NAME_ORDER_SQL``
        must fall back the same way, not key on the empty ``url_path``
        directly -- SQLite sorts ``''`` before every non-empty string, so
        a naive ``CASE`` (keying on ``file_id`` alone) would incorrectly
        rank this item before "report.docx" regardless of its own title."""
        extra_items: list[tuple[str, str, str, str, str, str, str | None, str, str]] = [
            ("6", "list-2", "file-f6", "root-docs", "zzzzz-fallback-title", "FILE", "meta_item_2", "", "1"),
        ]
        _build_site_repo(tmp_path, extra_items=extra_items)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await SiteProvider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                items = await provider.children(docs)
                assert [n.name for n in items] == ["Subfolder", "report.docx", "zzzzz-fallback-title"]
            finally:
                await provider.close()

    async def test_docs_list_items_with_no_title_or_url_path_sort_by_file_id_not_item_id(self, tmp_path: Path) -> None:
        """Regression test: a document-library item with ``file_id`` set
        but neither a usable ``url_path`` nor a ``title`` falls back to
        ``_self_id_of`` -- which prefers ``file_id`` over ``item_id`` --
        for both its display name and its sort key. ``_NAME_ORDER_SQL``'s
        own ``ELSE`` branch must match that preference, not sort by
        ``item_id`` unconditionally: these two items' ``item_id``s ("6",
        "9") sort in the *opposite* order from their real display
        names/sort keys (``file_id``), so a naive ``ELSE item_id`` would
        list them swapped."""
        extra_items: list[tuple[str, str, str, str, str, str, str | None, str, str]] = [
            ("6", "list-2", "zzz-file-id-for-item-6", "root-docs", "", "FILE", "meta_item_2", "", "1"),
            ("9", "list-2", "aaa-file-id-for-item-9", "root-docs", "", "FILE", "meta_item_2", "", "1"),
        ]
        _build_site_repo(tmp_path, extra_items=extra_items)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await SiteProvider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                items = await provider.children(docs)
                assert [n.name for n in items] == [
                    "Subfolder",
                    "report.docx",
                    "aaa-file-id-for-item-9",
                    "zzz-file-id-for-item-6",
                ]
            finally:
                await provider.close()

    async def test_docs_list_top_level_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``children_of()`` for one folder within one list issues
        exactly one ``WHERE list_id = ? AND parent_folder_id = ?`` query,
        backed by an index ``apply_index_hint()`` builds lazily into a
        private per-version temp copy (real ``item_version_table`` has no
        such index)."""
        from synology_apm_repo.sdk.storage.table import Table

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

        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        calls.clear()
        await provider.children(docs)
        # "Docs" is list_id "list-2", a document library whose own
        # top-level root_folder_id is "root-docs" (not the plain-list
        # empty-string default).
        assert calls == [("list_id = ? AND parent_folder_id = ?", ("list-2", "root-docs"))]

    async def test_a_leaf_item_with_an_unparseable_value1_has_no_size(self, tmp_path: Path) -> None:
        # Distinct from a folder's own value1="null" (test_docs_list_...
        # above): _leaf_size's file_id gate means a folder never reaches
        # int() at all, so that "null" case's own ValueError is
        # currently unreachable in practice -- this is a genuine leaf
        # (file_id set) whose cached value1 itself isn't a real number.
        extra_item = (
            "5",
            "list-2",
            "file-garbled",
            "root-docs",
            "Garbled",
            "FILE",
            "meta_item_3",
            "/garbled.docx",
            "not-a-number",
        )
        _build_site_repo(tmp_path, extra_items=[extra_item])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await SiteProvider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                garbled = next(n for n in await provider.children(docs) if n.name == "garbled.docx")
                assert garbled.size is None
            finally:
                await provider.close()


class TestContent:
    async def test_plain_row_content_is_its_values_json(self, provider: SaasWorkloadProvider) -> None:
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        [task] = await provider.children(tasks)
        content = (await provider.unit(task)).open()
        # LazyArtifact.size is None until assembled (see TestSize in
        # test_units_saas_artifact.py) — read the whole artifact instead.
        data = await content.read()
        assert json.loads(data) == {"Title": "Task A"}

    async def test_file_content_is_a_byte_range_view_not_an_artifact(self, provider: SaasWorkloadProvider) -> None:
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        report = next(n for n in await provider.children(docs) if n.name == "report.docx")
        content = (await provider.unit(report)).open()
        assert isinstance(content, ByteRangeView)
        assert await content.read(0, content.size or 0) == _FILE_CONTENT

    async def test_nested_file_content_reads_back_correctly(self, provider: SaasWorkloadProvider) -> None:
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        subfolder = next(n for n in await provider.children(docs) if n.name == "Subfolder")
        nested_file = next(n for n in await provider.children(subfolder) if n.name == "nested.txt")
        content = (await provider.unit(nested_file)).open()
        assert await content.read(0, content.size or 0) == b"nested file content"

    async def test_unit_on_an_item_with_no_meta_object_id_raises(self, tmp_path: Path) -> None:
        # A real, malformed-index shape: an item row whose meta_object_id
        # column is genuinely NULL (not merely an empty string) -- _assemble
        # can't even locate the META object to read, let alone open it.
        extra_item = ("5", "list-2", "file-no-meta", "root-docs", "No Meta", "FILE", None, "/no-meta.docx", "10")
        _build_site_repo(tmp_path, extra_items=[extra_item])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await SiteProvider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                no_meta = next(n for n in await provider.children(docs) if n.name == "no-meta.docx")
                with pytest.raises(ValueError, match="not a restorable unit"):
                    await provider.unit(no_meta)
            finally:
                await provider.close()

    async def test_unit_on_an_item_with_malformed_meta_json_raises_data_corrupt(self, tmp_path: Path) -> None:
        extra_item = ("5", "list-2", "file-bad-meta", "root-docs", "Bad Meta", "FILE", "meta_item_5", "/bad.docx", "1")
        _build_site_repo(tmp_path, extra_items=[extra_item], extra_payloads=[("meta_item_5", b"not json at all")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await SiteProvider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                bad = next(n for n in await provider.children(docs) if n.name == "bad.docx")
                with pytest.raises(DataCorruptError, match="did not parse as JSON"):
                    await provider.unit(bad)
            finally:
                await provider.close()

    async def test_unit_on_an_item_with_a_stale_content_object_id_raises_not_restorable(self, tmp_path: Path) -> None:
        # A real, malformed-index shape: META's own content_list names an
        # object_id the ObjectDB doesn't actually have (a stale/malformed
        # index) — the same "recorded but not actually present" shape
        # raw_object.py's own _named_nodes degrades on at listing time;
        # this one item just isn't restorable, not a reason to crash.
        extra_item = ("5", "list-2", "file-stale", "root-docs", "Stale", "FILE", "meta_item_5", "/stale.docx", "1")
        stale_meta = json.dumps(
            {"version": "1.0", "values": {}, "content_list": [{"type": 2, "object_id": "missing_content_obj"}]}
        ).encode()
        _build_site_repo(tmp_path, extra_items=[extra_item], extra_payloads=[("meta_item_5", stale_meta)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await SiteProvider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                stale = next(n for n in await provider.children(docs) if n.name == "stale.docx")
                with pytest.raises(ValueError, match="not a restorable unit"):
                    await provider.unit(stale)
            finally:
                await provider.close()

    async def test_unit_on_a_list_node_raises(self, provider: SaasWorkloadProvider) -> None:
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(tasks)

    async def test_unit_on_a_folder_node_raises(self, provider: SaasWorkloadProvider) -> None:
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        subfolder = next(n for n in await provider.children(docs) if n.name == "Subfolder")
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(subfolder)

    async def test_unit_on_the_root_node_raises(self, provider: SaasWorkloadProvider) -> None:
        # The root node's own key is () (length 0) -- shorter than
        # CategorizedGroupTree.row_for()'s own 2-segment minimum
        # (category, list_id, ...), distinct from the already-tested
        # list-node/folder-node cases above (both length >= 2).
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(provider.root())

    async def test_unit_on_a_bare_category_node_raises(self, provider: SaasWorkloadProvider) -> None:
        # A bare category node's own key is (category,) (length 1) --
        # also shorter than row_for()'s 2-segment minimum, but a
        # genuinely different case from the root node above (length 0).
        categories = await provider.children(provider.root())
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(categories[0])


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_site_tables_exist(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
        _write_saas_version_db(stream_db_dir / "saas_version")

        content = b"\x00" * 4096
        saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, 10, 64, 1, 2)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=10, num_chunks=1)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", [content])

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError):
                await SiteProvider(repo, _version(), saas_streams)

    async def test_children_of_a_list_with_no_indexed_items_returns_empty(self, provider: SaasWorkloadProvider) -> None:
        # a list_id with zero rows in item_version_table (index miss, not
        # a KeyError) — exercises the ``index is None`` branch directly.
        phantom_list = Node(ref=provider.root().ref, name="phantom", is_leaf=False, attrs={"list_id": "no-such-list"})
        assert await provider.children(phantom_list) == []


class TestIsDocumentLibrary:
    """Direct unit tests for ``_is_document_library`` -- ``_build_list_db``
    (this file's own shared DB builder) always supplies a real
    ``list_type`` value for every row, so the ``None`` case -- treated as
    "not a document library" rather than raising -- was never actually
    exercised anywhere in this file."""

    def test_list_type_1_is_a_document_library(self) -> None:
        assert _is_document_library({"list_type": 1}) is True

    def test_list_type_0_is_not_a_document_library(self) -> None:
        assert _is_document_library({"list_type": 0}) is False

    def test_missing_list_type_column_is_not_a_document_library(self) -> None:
        assert _is_document_library({}) is False

    def test_none_list_type_is_not_a_document_library(self) -> None:
        assert _is_document_library({"list_type": None}) is False


class TestIsFolder:
    """Direct unit tests for ``_is_folder`` -- every real fixture in this
    file only ever uses the numeric "1" encoding; ``item_type`` is
    documented (FORMAT-SPEC.md: sharepoint-site) as "FILE"/"FOLDER" or
    their numeric equivalents, but the string "FOLDER" encoding was
    never exercised."""

    def test_numeric_folder_encoding(self) -> None:
        assert _is_folder({"item_type": "1"}) is True

    def test_string_folder_encoding(self) -> None:
        assert _is_folder({"item_type": "FOLDER"}) is True

    def test_numeric_file_encoding_is_not_a_folder(self) -> None:
        assert _is_folder({"item_type": "0"}) is False

    def test_string_file_encoding_is_not_a_folder(self) -> None:
        assert _is_folder({"item_type": "FILE"}) is False


class TestDisplayName:
    """Direct unit tests for ``_display_name``."""

    def test_document_library_item_uses_the_url_path_basename(self) -> None:
        row: dict[str, object | None] = {
            "file_id": "f1",
            "item_id": "i1",
            "url_path": "/sites/x/Shared Documents/report.docx",
            "title": "",
        }
        assert _display_name(row) == "report.docx"

    def test_general_list_row_uses_its_own_title(self) -> None:
        row: dict[str, object | None] = {"file_id": "", "item_id": "i1", "url_path": "", "title": "My List Item"}
        assert _display_name(row) == "My List Item"

    def test_falls_back_to_self_id_when_both_title_and_url_path_are_empty(self) -> None:
        # Neither this file's real DB fixtures nor any existing test
        # ever builds a row missing both -- the final
        # ``return _self_id_of(row)`` fallback.
        row: dict[str, object | None] = {"file_id": "", "item_id": "i1", "url_path": "", "title": ""}
        assert _display_name(row) == _self_id_of(row) == "i1"

    def test_falls_back_to_file_id_not_item_id_when_file_id_is_set(self) -> None:
        # A document-library row (file_id set) with neither a usable
        # url_path nor a title falls back to _self_id_of, which prefers
        # file_id over item_id -- _NAME_ORDER_SQL's own ELSE branch must
        # match this, not key on item_id unconditionally.
        row: dict[str, object | None] = {"file_id": "f1", "item_id": "i1", "url_path": "", "title": ""}
        assert _display_name(row) == _self_id_of(row) == "f1"


class TestBuildValuesJson:
    """Direct unit tests for
    ``units.content.saas_site.build_values_json`` — every other exercise
    of it in this file goes through a full ``SiteProvider`` end-to-end
    (``TestUnit``'s own no-attachment-row test), which never covers an
    empty ``values`` dict or a non-string value in isolation."""

    def test_empty_values_serializes_to_an_empty_json_object(self) -> None:
        assert build_values_json({}) == b"{}"

    def test_non_string_values_round_trip_through_json(self) -> None:
        data = build_values_json({"Count": 3, "Active": True, "Notes": None})
        assert json.loads(data) == {"Count": 3, "Active": True, "Notes": None}

    def test_result_is_utf8_encoded_bytes(self) -> None:
        data = build_values_json({"Title": "café"})
        assert isinstance(data, bytes)
        assert json.loads(data.decode("utf-8")) == {"Title": "café"}
