"""Unit tests for ``synology_apm_repo.sdk.units.saas.raw_object`` —
a full synthetic repository root (same building blocks as
``test_units_saas_stream.py``/``test_units_saas_mail.py``), with a
connector-recorded ``copy_target_version`` object-name index pointing at one
embedded ``ObjectDB`` blob, plus a second, index-unreferenced one for
the ``--object-db-id`` manual-override tests (see
``tests/integration/sdk/test_units_saas_raw_object.py`` for the cross-check
against a real apv-sample-1 stream)."""

from __future__ import annotations

import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
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
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_STREAM_ID = 11
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "raw-stream-uuid"


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
    (``synology_apm_repo.sdk.units.saas.object_name_index``) -- every
    ``SaasWorkloadProvider``/``TeamsChatProvider`` construction resolves
    its service DB(s) only through this table, with no scan-based
    fallback, so a fixture repository that wants a table found must
    record it here. Plain, unencrypted JSON -- these fixture repositories
    never configure a vault_key."""
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


def _build_raw_object_repo(tmp_path: Path, *, session_id: int = 6, extra_stale_catalog_entry: bool = False) -> str:
    """Lays down one index-referenced ``ObjectDB`` (two named entries,
    ``cat_a``/``cat_b``) at the front of ``saas_obj``, and a second,
    index-unreferenced one after it (one entry, ``b_object_1``) for the
    manual ``--object-db-id`` override tests. Returns the manual
    ObjectDB's own ``object_db_id`` string — its (offset, length) is
    otherwise opaque to a test, exactly as it would be for a real caller
    without an online ``SnapshotDB``/an earlier diagnostic browse to read
    it from."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    catalog_content = {"a_object_1": b'{"meta": "a1"}', "a_object_2": b'{"meta": "a2"}'}
    manual_content = {"b_object_1": b'{"meta": "b1"}'}

    def _build_blob(content: dict[str, bytes], base_offset: int) -> tuple[bytes, int, int]:
        """Returns ``(object_db_bytes + payload, object_db_offset,
        object_db_length)`` — offsets in ``content`` are absolute within
        the final ``saas_obj`` buffer, so the ``ObjectDB``'s own length
        (needed to place them) has to be computed once, then baked into
        a second, final build — same two-pass approach
        ``test_units_saas_mail.py``'s fixture uses."""
        relative_rows = []
        cursor = base_offset
        for object_id, data in content.items():
            relative_rows.append((object_id, cursor, len(data)))
            cursor += len(data)
        object_db_len = len(_build_object_db(relative_rows))
        real_base = base_offset + object_db_len
        absolute_rows = []
        cursor = real_base
        payload = b""
        for object_id, data in content.items():
            absolute_rows.append((object_id, cursor, len(data)))
            payload += data
            cursor += len(data)
        object_db_bytes = _build_object_db(absolute_rows)
        return object_db_bytes + payload, base_offset, len(object_db_bytes)

    catalog_blob, catalog_offset, catalog_db_len = _build_blob(catalog_content, 0)
    catalog_blob_len_padded = len(catalog_blob) + (-len(catalog_blob) % 4096)
    manual_blob, manual_offset, manual_db_len = _build_blob(manual_content, catalog_blob_len_padded)

    saas_obj_content = catalog_blob + b"\x00" * (catalog_blob_len_padded - len(catalog_blob)) + manual_blob
    plaintexts = _chunk_it(saas_obj_content)

    saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, len(plaintexts), 2)])
    _write_composition(
        tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", plaintexts)
    db_objects = [("cat_a", "a_object_1"), ("cat_b", "a_object_2")]
    if extra_stale_catalog_entry:
        # An index entry recording a name/object_id pair the real
        # ObjectDB was never actually given -- a stale/malformed index,
        # not a bug in this test's own fixture wiring.
        db_objects.append(("cat_c", "a_object_missing"))
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_{catalog_offset}_{catalog_db_len}",
        db_objects=db_objects,
    )
    return f"{_STREAM_UUID}_{manual_offset}_{manual_db_len}"


def _version() -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-raw"),
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
async def manual_object_db_id(tmp_path: Path) -> str:
    return _build_raw_object_repo(tmp_path)


@pytest.fixture
async def provider(tmp_path: Path, manual_object_db_id: str) -> AsyncIterator[RawObjectProvider]:
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with (
        await DedupRepo.open(store, layout) as repo,
        SaasStreamCache(repo) as saas_streams,
        await RawObjectProvider.create(repo, _version(), saas_streams) as p,
    ):
        yield p


@pytest.fixture
async def opened_repo(tmp_path: Path, manual_object_db_id: str) -> AsyncIterator[DedupRepo]:
    """Like ``provider`` above but yields the opened ``DedupRepo``
    itself — the ``--object-db-id`` override tests below build their own
    ``RawObjectProvider`` directly against it."""
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo:
        yield repo


class TestTree:
    def test_root_is_not_a_leaf_and_named_after_the_stream(self, provider: RawObjectProvider) -> None:
        root = provider.root()
        assert root.is_leaf is False
        assert root.name == _STREAM_UUID

    async def test_root_children_are_the_catalog_named_entries(self, provider: RawObjectProvider) -> None:
        nodes = await provider.children(provider.root())
        assert {n.name for n in nodes} == {"cat_a", "cat_b"}
        assert all(n.is_leaf for n in nodes)
        assert all(n.kind is UnitKind.RAW_OBJECT for n in nodes)

    async def test_pagination_on_root_children(self, provider: RawObjectProvider) -> None:
        all_nodes = await provider.children(provider.root())
        first_page = await provider.children(provider.root(), offset=0, limit=1)
        assert len(first_page) == 1
        assert first_page[0].ref == all_nodes[0].ref

    async def test_refs_are_stable_and_scoped_under_the_version(self, provider: RawObjectProvider) -> None:
        nodes = await provider.children(provider.root())
        [cat_a] = [n for n in nodes if n.name == "cat_a"]
        assert str(cat_a.ref).endswith("cat_a")
        assert "cat:1" in str(cat_a.ref)
        assert "wl:1" in str(cat_a.ref)
        assert "ver:vuid-raw" in str(cat_a.ref)

    async def test_a_catalog_entry_the_object_db_never_actually_has_is_silently_skipped(self, tmp_path: Path) -> None:
        # A named entry the index recorded but this ObjectDB doesn't
        # actually have -- not this provider's job to flag as
        # corruption, just absent from the listing.
        _build_raw_object_repo(tmp_path, extra_stale_catalog_entry=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await RawObjectProvider.create(repo, _version(), saas_streams) as provider,
        ):
            nodes = await provider.children(provider.root())
            assert {n.name for n in nodes} == {"cat_a", "cat_b"}


class TestUnit:
    async def test_unit_reads_back_the_real_content(self, provider: RawObjectProvider) -> None:
        nodes = await provider.children(provider.root())
        [node_a] = [n for n in nodes if n.name == "cat_a"]
        [node_b] = [n for n in nodes if n.name == "cat_b"]

        content_a = (await provider.unit(node_a)).open()
        content_b = (await provider.unit(node_b)).open()
        assert await content_a.read(0, content_a.size or 0) == b'{"meta": "a1"}'
        assert await content_b.read(0, content_b.size or 0) == b'{"meta": "a2"}'

    async def test_unit_on_the_root_node_raises(self, provider: RawObjectProvider) -> None:
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(provider.root())


class TestNoCatalogIndex:
    """A version with no object-name index recorded at all has nothing to
    show — never a scan of Pool/ObjectDB."""

    async def test_children_is_empty_without_an_object_name_index(self, tmp_path: Path) -> None:
        # Same fixture, minus _write_copy_target_version_db — reproduced
        # inline rather than reusing _build_raw_object_repo(), which
        # always writes one.
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
        _write_saas_version_db(stream_db_dir / "saas_version")
        saas_obj_content = b'{"meta": "orphan"}'
        plaintexts = _chunk_it(saas_obj_content)
        saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, 6, 64, len(plaintexts), 2)])
        _write_composition(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=6, num_chunks=len(plaintexts)
        )
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", plaintexts)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await RawObjectProvider.create(repo, _version(), saas_streams) as provider,
        ):
            assert await provider.children(provider.root()) == []


class TestObjectDbIdOverride:
    """The manual disambiguation escape hatch — pinned directly to the
    second, index-unreferenced ``ObjectDB`` this fixture also lays
    down, the same way a real user would from an online ``SnapshotDB``
    or a prior diagnostic browse."""

    async def test_manual_mode_shows_only_that_objectdbs_own_objects(
        self, opened_repo: DedupRepo, manual_object_db_id: str
    ) -> None:
        # The manual ObjectDB is deliberately never named by the index
        # at all -- the exact-set assertion below (not just
        # "cat_a" absent) proves reaching its one real object doesn't
        # merely re-derive the index's own choice.
        async with (
            SaasStreamCache(opened_repo) as saas_streams,
            await RawObjectProvider.create(
                opened_repo, _version(), saas_streams, object_db_id=manual_object_db_id
            ) as manual,
        ):
            children = await manual.children(manual.root())
            assert {n.name for n in children} == {"b_object_1"}
            assert all(n.is_leaf for n in children)

    async def test_manual_mode_unit_reads_back_the_real_content(
        self, opened_repo: DedupRepo, manual_object_db_id: str
    ) -> None:
        async with (
            SaasStreamCache(opened_repo) as saas_streams,
            await RawObjectProvider.create(
                opened_repo, _version(), saas_streams, object_db_id=manual_object_db_id
            ) as manual,
        ):
            [node] = await manual.children(manual.root())
            content = (await manual.unit(node)).open()
            assert await content.read(0, content.size or 0) == b'{"meta": "b1"}'

    async def test_mismatched_stream_uuid_raises_not_found(self, opened_repo: DedupRepo) -> None:
        async with SaasStreamCache(opened_repo) as saas_streams:
            with pytest.raises(NotFoundError, match="names stream"):
                await RawObjectProvider.create(opened_repo, _version(), saas_streams, object_db_id="wrong-stream_0_100")

    async def test_malformed_object_db_id_raises_not_found(self, opened_repo: DedupRepo) -> None:
        async with SaasStreamCache(opened_repo) as saas_streams:
            with pytest.raises(NotFoundError, match="malformed"):
                await RawObjectProvider.create(
                    opened_repo, _version(), saas_streams, object_db_id="not-shaped-like-one"
                )

    async def test_mismatched_stream_uuid_closes_its_stream_instead_of_leaking_it(
        self, opened_repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test: the mismatch check raises before ``_manual_db``/
        ``_indexed_db`` are ever assigned (this provider's own instance
        attrs), and this is the guaranteed-to-succeed fallback provider,
        reachable directly from CLI/TUI diagnostic tooling's
        ``--object-db-id`` — create() must call close() on this failure
        path rather than only on success."""
        closed_instances = []
        original_close = RawObjectProvider.close

        async def spy_close(self: RawObjectProvider) -> None:
            closed_instances.append(self)
            await original_close(self)

        monkeypatch.setattr(RawObjectProvider, "close", spy_close)
        async with SaasStreamCache(opened_repo) as saas_streams:
            with pytest.raises(NotFoundError, match="names stream"):
                await RawObjectProvider.create(opened_repo, _version(), saas_streams, object_db_id="wrong-stream_0_100")
        assert len(closed_instances) == 1


class TestCreateFailureCleanup:
    """``create()``'s own broad ``except Exception: await self.close();
    raise`` (unlike the narrower, already-tested mismatched-``--object-db-id``
    case above) — a real, unexpected failure loading the *indexed* ObjectDB
    (a stale/corrupt index entry, not a missing index) must still call
    close() rather than leak whatever it already opened, the same as every
    sibling application-layer provider's (Drive, Mail, Calendar, Site)
    equivalent test of a real corrupt/failing load during its own
    ``create()``."""

    async def test_a_corrupt_indexed_objectdb_closes_the_stream_instead_of_leaking_it(
        self, opened_repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed_instances = []
        original_close = RawObjectProvider.close

        async def spy_close(self: RawObjectProvider) -> None:
            closed_instances.append(self)
            await original_close(self)

        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise DataCorruptError("synthetic corruption for this test")

        monkeypatch.setattr(RawObjectProvider, "close", spy_close)
        monkeypatch.setattr(ObjectDb, "load", failing_load)

        async with SaasStreamCache(opened_repo) as saas_streams:
            with pytest.raises(DataCorruptError, match="synthetic corruption"):
                await RawObjectProvider.create(opened_repo, _version(), saas_streams)
        assert len(closed_instances) == 1
