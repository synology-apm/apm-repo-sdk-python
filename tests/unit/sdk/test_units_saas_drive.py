"""Unit tests for ``synology_apm_repo.sdk.units.saas.drive`` — a full
synthetic repository root (same building blocks as
``test_units_saas_raw_object.py``), with a real ZSTD-compressed
``item_table`` service DB embedded in its ``saas_obj`` content (see
``tests/integration/sdk/test_units_saas_drive.py`` for the cross-check
against real apv-sample-1 Drive data)."""

from __future__ import annotations

import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import cast

import pytest
import zstandard

from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import UnsupportedDataFormatError
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
from synology_apm_repo.sdk.units.base import Node, SupportsDirectRefLookup, UnitKind
from synology_apm_repo.sdk.units.resolve import find_node, find_path_with_children
from synology_apm_repo.sdk.units.saas.drive import DriveProvider, _root_folder_id
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.tree_strategy import RecursiveTree

_STREAM_ID = 12
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "drive-stream-uuid"

_CONTENT_A = b"file A content"
_CONTENT_B = b"file B content, a little longer"


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
    """See test_units_dispatch_saas.py's own
    ``_write_copy_target_version_db`` docstring."""
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


def _build_item_service_db(
    *,
    root_folder_id: str | None,
    items: list[tuple[str, str, str, int, int, str, str]],
    missing_hash_column: bool = False,
) -> bytes:
    """``items``: (item_id, name, parent_folder_id, type, size, content_object_id, hash).
    ``root_folder_id=None`` omits the config_table row entirely (see
    ``drive.py``'s own ``_root_folder_id``'s no-row -> "" fallback)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "svc.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        if root_folder_id is not None:
            conn.execute("INSERT INTO config_table VALUES ('root_folder_id', ?)", (root_folder_id,))
        hash_col = "" if missing_hash_column else ", hash TEXT"
        conn.execute(
            f"CREATE TABLE item_table(item_id TEXT PRIMARY KEY, name TEXT, parent_folder_id TEXT, "
            f"type INTEGER, size INTEGER, mtime INTEGER, meta_object_id TEXT, content_object_id TEXT{hash_col})"
        )
        for item_id, name, parent_folder_id, item_type, size, content_object_id, item_hash in items:
            if missing_hash_column:
                conn.execute(
                    "INSERT INTO item_table(item_id, name, parent_folder_id, type, size, mtime, "
                    "meta_object_id, content_object_id) VALUES (?, ?, ?, ?, ?, 0, '', ?)",
                    (item_id, name, parent_folder_id, item_type, size, content_object_id),
                )
            else:
                conn.execute(
                    "INSERT INTO item_table(item_id, name, parent_folder_id, type, size, mtime, "
                    "meta_object_id, content_object_id, hash) VALUES (?, ?, ?, ?, ?, 0, '', ?, ?)",
                    (item_id, name, parent_folder_id, item_type, size, content_object_id, item_hash),
                )
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


def _build_drive_repo(
    tmp_path: Path,
    *,
    session_id: int = 7,
    missing_hash_column: bool = False,
    extra_items: list[tuple[str, str, str, int, int, str, str]] | None = None,
    root_folder_id: str | None = "root-id",
) -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    items = [
        ("folder-1", "folder-1", "root-id", 0, 0, "", ""),
        ("item-a", "file-a.txt", "root-id", 1, len(_CONTENT_A), "content_a", "hash-a"),
        ("item-b", "file-b.txt", "folder-1", 1, len(_CONTENT_B), "content_b", "hash-b"),
        *(extra_items or []),
    ]
    service_db_bytes = _build_item_service_db(
        root_folder_id=root_folder_id, items=items, missing_hash_column=missing_hash_column
    )

    payloads = [("svc_obj", service_db_bytes), ("content_a", _CONTENT_A), ("content_b", _CONTENT_B)]
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
        db_objects=[("drive_db", "svc_obj")],
    )


def _version() -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-drive"),
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


@pytest.fixture
async def provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider]:
    _build_drive_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo:
        p = await DriveProvider(repo, _version())
        try:
            yield p
        finally:
            await p.close()


class TestTree:
    async def test_root_lists_top_level_entries(self, provider: SaasWorkloadProvider) -> None:
        top = await provider.children(provider.root())
        assert {n.name for n in top} == {"folder-1", "file-a.txt"}
        folder = next(n for n in top if n.name == "folder-1")
        assert folder.is_leaf is False
        file_a = next(n for n in top if n.name == "file-a.txt")
        assert file_a.is_leaf is True
        assert file_a.kind is UnitKind.DRIVE_ITEM
        assert file_a.size == len(_CONTENT_A)

    async def test_nested_folder_lists_its_own_child(self, provider: SaasWorkloadProvider) -> None:
        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        children = await provider.children(folder)
        assert [n.name for n in children] == ["file-b.txt"]

    async def test_children_of_a_leaf_returns_empty(self, provider: SaasWorkloadProvider) -> None:
        top = await provider.children(provider.root())
        file_a = next(n for n in top if n.name == "file-a.txt")
        assert await provider.children(file_a) == []

    async def test_children_of_a_node_with_no_item_id_returns_empty(self, provider: SaasWorkloadProvider) -> None:
        stray = Node(ref=provider.root().ref, name="stray", is_leaf=False)
        assert await provider.children(stray) == []

    async def test_attrs_carry_the_hash_column(self, provider: SaasWorkloadProvider) -> None:
        top = await provider.children(provider.root())
        file_a = next(n for n in top if n.name == "file-a.txt")
        assert file_a.attrs["hash"] == "hash-a"

    async def test_attrs_carry_content_object_id_and_mtime(self, provider: SaasWorkloadProvider) -> None:
        # _extra_attrs also returns content_object_id/mtime, never
        # asserted on anywhere else in this file (only "hash" is, above).
        top = await provider.children(provider.root())
        file_a = next(n for n in top if n.name == "file-a.txt")
        assert file_a.attrs["content_object_id"] == "content_a"
        assert file_a.attrs["mtime"] == 0

    async def test_pagination(self, provider: SaasWorkloadProvider) -> None:
        full = await provider.children(provider.root())
        page = await provider.children(provider.root(), offset=0, limit=1)
        assert len(page) == 1
        assert page[0].ref == full[0].ref

    async def test_pagination_with_a_nonzero_offset(self, provider: SaasWorkloadProvider) -> None:
        full = await provider.children(provider.root())
        assert len(full) >= 2
        page = await provider.children(provider.root(), offset=1, limit=1)
        assert len(page) == 1
        assert page[0].ref == full[1].ref

    async def test_children_of_one_folder_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each ``children_of()`` call is exactly one ``WHERE
        parent_folder_id = ?`` query. A regression to a full-table scan
        would show up here as more than one ``Table.select()`` call for
        what is still just one level's own children."""
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

        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        calls.clear()
        await provider.children(folder)
        assert len(calls) == 1


class TestContent:
    async def test_reads_back_real_file_content(self, provider: SaasWorkloadProvider) -> None:
        top = await provider.children(provider.root())
        file_a = next(n for n in top if n.name == "file-a.txt")
        content = (await provider.unit(file_a)).open()
        assert await content.read(0, content.size or 0) == _CONTENT_A

    async def test_reads_back_nested_file_content(self, provider: SaasWorkloadProvider) -> None:
        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        file_b = next(n for n in await provider.children(folder) if n.name == "file-b.txt")
        content = (await provider.unit(file_b)).open()
        assert await content.read(0, content.size or 0) == _CONTENT_B

    async def test_unit_on_a_folder_raises(self, provider: SaasWorkloadProvider) -> None:
        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(folder)

    async def test_unit_on_the_root_node_raises(self, provider: SaasWorkloadProvider) -> None:
        # The root node's own key is () (length 0) -- RecursiveTree's own
        # row_for() requires exactly one segment, distinct from the
        # already-tested folder case above (key length 1, but a folder).
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(provider.root())

    def test_repo_property_exposes_the_underlying_repository(self, provider: SaasWorkloadProvider) -> None:
        assert isinstance(provider.repo, DedupRepo)

    async def test_unit_on_a_leaf_item_with_no_content_object_id_raises(self, tmp_path: Path) -> None:
        # A file-type item (unlike a folder) with no content_object_id at
        # all -- real, observed shape (e.g. a Drive shortcut/link item
        # with nothing to actually restore), distinct from the
        # folder-dispatch case above.
        _build_drive_repo(tmp_path, extra_items=[("item-c", "empty.txt", "root-id", 1, 0, "", "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            provider = await DriveProvider(repo, _version())
            try:
                top = await provider.children(provider.root())
                empty_item = next(n for n in top if n.name == "empty.txt")
                with pytest.raises(ValueError, match="not a restorable unit"):
                    await provider.unit(empty_item)
            finally:
                await provider.close()

    async def test_unit_on_a_leaf_item_with_a_stale_content_object_id_raises_not_restorable(
        self, tmp_path: Path
    ) -> None:
        # A real, malformed-index shape: item_table names a
        # content_object_id the ObjectDB doesn't actually have (a
        # stale/malformed index) — the same "recorded but not actually
        # present" shape raw_object.py's own _named_nodes degrades on at
        # listing time; this one item just isn't restorable, not a
        # reason to crash the caller.
        _build_drive_repo(tmp_path, extra_items=[("item-d", "stale.txt", "root-id", 1, 1, "missing-content-id", "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            provider = await DriveProvider(repo, _version())
            try:
                top = await provider.children(provider.root())
                stale_item = next(n for n in top if n.name == "stale.txt")
                with pytest.raises(ValueError, match="not a restorable unit"):
                    await provider.unit(stale_item)
            finally:
                await provider.close()


class TestSchemaTolerance:
    async def test_missing_optional_hash_column_reads_back_as_none(self, tmp_path: Path) -> None:
        _build_drive_repo(tmp_path, missing_hash_column=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            provider = await DriveProvider(repo, _version())
            try:
                top = await provider.children(provider.root())
                file_a = next(n for n in top if n.name == "file-a.txt")
                assert file_a.attrs["hash"] is None
                content = (await provider.unit(file_a)).open()
                assert await content.read(0, content.size or 0) == _CONTENT_A
            finally:
                await provider.close()


class TestDirectRefLookup:
    """Drive is the one SaaS shape backed by
    ``RecursiveTreeSaasProvider`` — real, full-stack coverage of
    ``SupportsDirectRefLookup`` and of ``units.resolve``'s dispatch onto
    it, against the same nested ``folder-1``/``file-b.txt`` fixture the
    tree tests above already use."""

    async def test_provider_satisfies_the_protocol(self, provider: SaasWorkloadProvider) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)

    async def test_resolve_extra_finds_a_nested_item_without_walking_its_parent_folder(
        self, provider: SaasWorkloadProvider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The whole point of the capability: locating ``file-b.txt``
        (nested under ``folder-1``) never calls ``children()`` at all."""
        original_children = SaasWorkloadProvider.children

        calls: list[object] = []

        async def counting_children(
            self: SaasWorkloadProvider, node: Node, offset: int = 0, limit: int | None = None
        ) -> list[Node]:
            calls.append(1)
            return await original_children(self, node, offset, limit)

        monkeypatch.setattr(SaasWorkloadProvider, "children", counting_children)

        assert isinstance(provider, SupportsDirectRefLookup)
        node = await provider.resolve_extra(("item-b",))
        assert node is not None
        assert node.name == "file-b.txt"
        assert node.is_leaf is True
        assert calls == []

    async def test_resolve_extra_returns_none_for_an_unknown_id(self, provider: SaasWorkloadProvider) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        assert await provider.resolve_extra(("no-such-item",)) is None

    async def test_resolve_extra_rejects_a_multi_segment_key(self, provider: SaasWorkloadProvider) -> None:
        """Drive's own key is always a 1-tuple -- a caller passing
        anything else can't be asking about a real Drive item."""
        assert isinstance(provider, SupportsDirectRefLookup)
        assert await provider.resolve_extra(("item-b", "extra")) is None

    async def test_parent_of_a_top_level_item_is_the_version_root(self, provider: SaasWorkloadProvider) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        item_a = await provider.resolve_extra(("item-a",))
        assert item_a is not None
        parent = await provider.parent_of(item_a)
        assert parent == provider.root()

    async def test_parent_of_a_nested_item_is_its_own_folder(self, provider: SaasWorkloadProvider) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        item_b = await provider.resolve_extra(("item-b",))
        assert item_b is not None
        parent = await provider.parent_of(item_b)
        assert parent is not None
        assert parent.name == "folder-1"
        assert parent.is_leaf is False

    async def test_parent_of_the_version_root_is_none(self, provider: SaasWorkloadProvider) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        assert await provider.parent_of(provider.root()) is None

    async def test_parent_id_of_returns_none_for_a_key_never_resolved(self, provider: SaasWorkloadProvider) -> None:
        tree = cast("RecursiveTree", provider._tree)
        assert tree.parent_id_of(("never-seen-id",)) is None

    async def test_parent_of_returns_none_when_the_parent_id_does_not_match_a_real_row(self, tmp_path: Path) -> None:
        """Defensive case: a row whose own ``parent_folder_id`` doesn't
        match any real ``item_id`` (a data inconsistency, not a normal
        Drive tree) -- confirms this reports "no parent" rather than
        raising."""
        _build_drive_repo(
            tmp_path, extra_items=[("item-orphan", "orphan.txt", "missing-parent-id", 1, 1, "content_a", "")]
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            provider = await DriveProvider(repo, _version())
            try:
                assert isinstance(provider, SupportsDirectRefLookup)
                orphan = await provider.resolve_extra(("item-orphan",))
                assert orphan is not None
                assert await provider.parent_of(orphan) is None
            finally:
                await provider.close()

    async def test_find_node_resolves_a_nested_item_through_the_public_entry_point(
        self, provider: SaasWorkloadProvider
    ) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        item_b = await provider.resolve_extra(("item-b",))
        assert item_b is not None
        assert await find_node(provider, item_b.ref) == item_b

    async def test_find_path_with_children_rebuilds_the_real_ancestor_chain(
        self, provider: SaasWorkloadProvider
    ) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        item_b = await provider.resolve_extra(("item-b",))
        assert item_b is not None

        result = await find_path_with_children(provider, item_b.ref)

        assert result is not None
        chain, children_by_step = result
        assert [n.name for n in chain] == ["/", "folder-1", "file-b.txt"]
        assert [n.name for n in children_by_step[1]] == ["file-b.txt"]


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_item_table_exists(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
        _write_saas_version_db(stream_db_dir / "saas_version")

        # a saas_obj with no embedded ObjectDB at all
        content = b"\x00" * 4096
        saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, 7, 64, 1, 2)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=7, num_chunks=1)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", [content])

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            with pytest.raises(UnsupportedDataFormatError):
                await DriveProvider(repo, _version())

    async def test_a_databaseerror_from_objectdb_load_surfaces_as_unsupported_data_format(
        self, provider: SaasWorkloadProvider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Distinct from the "no object-name index at all" case above: here
        the object-name index resolves fine, but the object it points at
        doesn't validate as real SQLite (a stale/corrupt index
        entry) -- ``_open_table_via_index()``'s own
        ``ObjectDb.load()`` call must map that failure the same way,
        not let a raw ``sqlite3.DatabaseError``/``DataCorruptError`` escape."""
        from synology_apm_repo.sdk.errors import DataCorruptError
        from synology_apm_repo.sdk.units.saas.drive import _ITEM_TABLE
        from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb

        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise DataCorruptError("synthetic corruption for this test")

        monkeypatch.setattr(ObjectDb, "load", failing_load)

        object_name_index = provider._object_name_index
        assert object_name_index is not None
        # create() already resolved and cached this same (offset, length)
        # while building the tree -- close and clear it so
        # _open_table_via_index() actually calls the (now-failing)
        # ObjectDb.load() again, rather than short-circuiting on the cache
        # hit. Closing before clearing matters: provider.close()'s own
        # cleanup only walks _object_db_cache, so discarding this entry
        # without closing it first would leak its aiosqlite connection.
        for cached in provider._object_db_cache.values():
            await cached.close()
        provider._object_db_cache.clear()
        with pytest.raises(UnsupportedDataFormatError, match="did not validate"):
            await provider._open_table_via_index(_ITEM_TABLE, object_name_index, _version())


async def test_root_folder_id_falls_back_to_empty_string_when_config_table_has_no_row(tmp_path: Path) -> None:
    # config_table.root_folder_id names a synthetic anchor id with no row
    # of its own in item_table -- every other test in this file builds a
    # config_table that always has this row (_build_item_service_db's
    # own default), so the no-row "" fallback was never exercised.
    _build_drive_repo(tmp_path, root_folder_id=None)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo:
        provider = await DriveProvider(repo, _version())
        try:
            assert await _root_folder_id(provider) == ""
        finally:
            await provider.close()
