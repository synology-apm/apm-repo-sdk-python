"""Unit tests for ``synology_apm_repo.sdk.units.fs`` — synthetic
repository roots written to real files, no sample repositories required."""

from __future__ import annotations

import json
import os
import sqlite3
import struct
import zlib
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.catalog.version import Version, VersionMeta
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import NotFoundError
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
from synology_apm_repo.sdk.units.base import node_modified_time
from synology_apm_repo.sdk.units.fs import FsProvider

_STREAM_ID = 8
_SNAPSHOT_UUID = "fs-uuid"
_VERSION_ID = 7
_DEDUP_IMG = (b"file-A-content--" * 256) + (b"file-B-content--" * 256)  # 2 x 4096 bytes
assert len(_DEDUP_IMG) == 8192


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


def _write_entry_table(path: Path, rows: list[tuple[str, str, int, int, int, str, str]]) -> bytes:
    """``rows``: (basename, dirname, file_size, file_mtime, file_type, content_dedup_id, xattr).
    Returns the standard-zstd-framed bytes ready to write as ``version.db.zst``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE entry_table(basename TEXT, dirname TEXT, file_size INTEGER, file_mtime INTEGER, "
        "file_type INTEGER, content_dedup_id TEXT, xattr TEXT)"
    )
    conn.executemany("INSERT INTO entry_table VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
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


def _build_fs_repo(
    tmp_path: Path,
    *,
    session_id: int = 3,
    extra_entry_rows: tuple[tuple[str, str, int, int, int, str, str], ...] = (),
) -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    dedup_img_path = f"{_SNAPSHOT_UUID}/{_VERSION_ID}/dedup.img"
    _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, session_id, 64, 2, 2)])
    _write_target_db_with_version_id(tmp_path / "copy_meta_file" / "FS_uid1" / "target.db", _VERSION_ID)

    entry_rows = [
        ("dir1", "/", 0, 0, 2, "", ""),
        ("fileA.txt", "/dir1", 4096, 1700000000, 1, "0", "[]"),
        ("fileB.txt", "/dir1", 4096, 1700000001, 1, "4096", "[]"),
        *extra_entry_rows,
    ]
    version_db_dir = tmp_path / "copy_meta_file" / "FS_uid1" / "ActiveBackup_2026-01-01_120000_vuuid"
    zst_bytes = _write_entry_table(version_db_dir / "_source.db", entry_rows)
    (version_db_dir / "version.db.zst").write_bytes(zst_bytes)

    _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=2)
    _write_bucket(
        tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk",
        [_DEDUP_IMG[0:4096], _DEDUP_IMG[4096:8192]],
    )


def _version(*, meta_filenames: tuple[str, ...] | None = None, no_meta: bool = False) -> Version:
    meta = (
        None
        if no_meta
        else VersionMeta(
            target_meta_path="/pv/20/copy_meta_file/FS_uid1",
            meta_filenames=meta_filenames or ("target.db", "ActiveBackup_2026-01-01_120000_vuuid/version.db.zst"),
            status=1,
        )
    )
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid-fs"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type="FS",
        target_id=TargetId(_SNAPSHOT_UUID),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=meta,
    )


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[DedupRepo]:
    _build_fs_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as r:
        yield r


class TestTree:
    async def test_root_lists_top_level_entries(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            children = await provider.children(provider.root())
            assert len(children) == 1
            assert children[0].name == "dir1"
            assert children[0].is_leaf is False

    async def test_leaf_children_are_files(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            files = await provider.children(dir1)
            assert {f.name for f in files} == {"fileA.txt", "fileB.txt"}
            assert all(f.is_leaf for f in files)
            assert all(f.size == 4096 for f in files)

    async def test_children_of_a_file_node_is_empty(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            file_a = next(f for f in await provider.children(dir1) if f.name == "fileA.txt")
            assert await provider.children(file_a) == []

    async def test_dirname_uses_absolute_path_no_double_slash(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            assert dir1.attrs["dirname"] == "/dir1"


class TestContent:
    async def test_reads_the_real_dedup_content(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            file_a = next(f for f in await provider.children(dir1) if f.name == "fileA.txt")
            file_b = next(f for f in await provider.children(dir1) if f.name == "fileB.txt")

            content_a = (await provider.unit(file_a)).open()
            content_b = (await provider.unit(file_b)).open()
            assert await content_a.read(0, 4096) == _DEDUP_IMG[0:4096]
            assert await content_b.read(0, 4096) == _DEDUP_IMG[4096:8192]

    async def test_content_dedup_id_is_a_dedup_img_relative_offset(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            file_b = next(f for f in await provider.children(dir1) if f.name == "fileB.txt")
            unit = await provider.unit(file_b)
            # a view, not the whole dedup.img
            assert unit.open().size == 4096

    async def test_unit_on_a_directory_node_raises(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            with pytest.raises(ValueError, match="not a restorable unit"):
                await provider.unit(dir1)

    async def test_dedup_img_is_cached_across_multiple_unit_calls(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            file_a = next(f for f in await provider.children(dir1) if f.name == "fileA.txt")
            first = await provider._dedup_img()
            await provider.unit(file_a)
            second = await provider._dedup_img()
            assert first is second

    async def test_entry_table_connection_is_cached_across_calls(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            first = await provider._entry_table_connection()
            await provider.children(provider.root())
            second = await provider._entry_table_connection()
            assert first is second

    async def test_close_closes_the_cached_entry_table_connection(self, repo: DedupRepo) -> None:
        provider = FsProvider(repo, _version())
        await provider._entry_table_connection()
        entry_db = provider._entry_db
        assert entry_db is not None
        path = entry_db._path
        assert path is not None
        assert os.path.exists(path)

        await provider.close()

        assert provider._entry_db is None
        assert not os.path.exists(path)  # SqliteSource.close() removes its temp file

    async def test_zero_byte_leaf_reads_as_empty_without_a_real_read(self, tmp_path: Path) -> None:
        # fs.py's own `dedup_img.view(int(content_dedup_id), node.size or
        # 0)` -- a real zero-byte file (file_size=0) was never built by
        # any fixture in this file.
        _build_fs_repo(tmp_path, extra_entry_rows=(("empty.txt", "/dir1", 0, 1700000002, 1, "8192", "[]"),))
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            empty = next(f for f in await provider.children(dir1) if f.name == "empty.txt")
            assert empty.size == 0

            content = (await provider.unit(empty)).open()
            assert content.size == 0
            assert await content.read(0, 0) == b""


class TestErrorHandling:
    async def test_missing_version_meta_raises_not_found(self, repo: DedupRepo) -> None:
        provider = FsProvider(repo, _version(no_meta=True))
        with pytest.raises(NotFoundError):
            await provider.children(provider.root())

    async def test_missing_version_db_zst_in_meta_filenames_raises_not_found(self, repo: DedupRepo) -> None:
        provider = FsProvider(repo, _version(meta_filenames=("target.db",)))
        with pytest.raises(NotFoundError):
            await provider.children(provider.root())

    async def test_empty_version_table_raises_not_found(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_file_map(tmp_path / "db" / "file_map", [])
        target_db_path = tmp_path / "copy_meta_file" / "FS_uid1" / "target.db"
        target_db_path.parent.mkdir(parents=True)
        conn = sqlite3.connect(target_db_path)
        conn.execute("CREATE TABLE version_table(id INTEGER PRIMARY KEY, version_id INTEGER)")
        conn.commit()
        conn.close()
        version_db_dir = tmp_path / "copy_meta_file" / "FS_uid1" / "ActiveBackup_2026-01-01_120000_vuuid"
        zst_bytes = _write_entry_table(version_db_dir / "_source.db", [])
        (version_db_dir / "version.db.zst").write_bytes(zst_bytes)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, FsProvider(repo, _version()) as provider:
            dir1 = await provider.children(provider.root())
            assert dir1 == []  # entry_table is empty, but that alone doesn't fail
            with pytest.raises(NotFoundError):
                await provider._dedup_img()


class TestPagination:
    """``children()``'s ``offset``/``limit`` push a real ``ORDER BY
    (CASE WHEN file_type = 2 THEN 0 ELSE 1 END), basename, rowid LIMIT ?
    OFFSET ?`` down to SQL — containers before leaves, then by basename."""

    async def test_pagination_matches_full_list_slice_sorted_by_basename(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        dedup_img_path = f"{_SNAPSHOT_UUID}/{_VERSION_ID}/dedup.img"
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, 3, 64, 2, 2)])
        _write_target_db_with_version_id(tmp_path / "copy_meta_file" / "FS_uid1" / "target.db", _VERSION_ID)

        # Deliberately inserted out of basename order — the returned
        # order must come from the real ORDER BY, not insertion order.
        entry_rows = [("dir1", "/", 0, 0, 2, "", "")]
        entry_rows += [(f"file{i}.txt", "/dir1", 10, 1700000000 + i, 1, str(i), "[]") for i in (4, 1, 5, 0, 3, 2)]
        version_db_dir = tmp_path / "copy_meta_file" / "FS_uid1" / "ActiveBackup_2026-01-01_120000_vuuid"
        zst_bytes = _write_entry_table(version_db_dir / "_source.db", entry_rows)
        (version_db_dir / "version.db.zst").write_bytes(zst_bytes)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            full = await provider.children(dir1)
            assert len(full) == 6
            assert [f.name for f in full] == sorted(f.name for f in full)

            page = await provider.children(dir1, offset=2, limit=2)
            assert [f.name for f in page] == [f.name for f in full[2:4]]

    async def test_directories_sort_before_files_regardless_of_name(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        dedup_img_path = f"{_SNAPSHOT_UUID}/{_VERSION_ID}/dedup.img"
        _write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, 3, 64, 2, 2)])
        _write_target_db_with_version_id(tmp_path / "copy_meta_file" / "FS_uid1" / "target.db", _VERSION_ID)

        # "zzz_dir" would sort after "aaa.txt" by name alone — asserting
        # it still comes first proves the container-vs-leaf rank, not
        # just the name tiebreaker, is being applied.
        entry_rows = [
            ("dir1", "/", 0, 0, 2, "", ""),
            ("aaa.txt", "/dir1", 10, 1700000000, 1, "0", "[]"),
            ("zzz_dir", "/dir1", 0, 0, 2, "", ""),
        ]
        version_db_dir = tmp_path / "copy_meta_file" / "FS_uid1" / "ActiveBackup_2026-01-01_120000_vuuid"
        zst_bytes = _write_entry_table(version_db_dir / "_source.db", entry_rows)
        (version_db_dir / "version.db.zst").write_bytes(zst_bytes)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            children = await provider.children(dir1)
            assert [c.name for c in children] == ["zzz_dir", "aaa.txt"]

    async def test_pagination_offset_past_end_returns_empty(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            assert await provider.children(dir1, offset=100, limit=10) == []

    async def test_pagination_limit_none_returns_everything(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            assert len(await provider.children(dir1, offset=0, limit=None)) == 2


class TestMtime:
    async def test_an_out_of_range_file_mtime_degrades_to_no_mtime_attr_without_failing_the_listing(
        self, tmp_path: Path
    ) -> None:
        """``file_mtime`` is a raw, unvalidated ``entry_table`` integer --
        an out-of-``datetime``-range value (corrupt data, not a real
        filesystem fact) must degrade only that one row's own Modified
        cell to blank, not raise out of ``children()`` and fail every
        other file in the same directory's listing too."""
        _build_fs_repo(tmp_path, extra_entry_rows=(("corrupt.txt", "/dir1", 10, 99999999999999, 1, "8192", "[]"),))
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            children = await provider.children(dir1)
            by_name = {c.name: c for c in children}
            assert node_modified_time(by_name["corrupt.txt"]) is None
            # The other, real rows are unaffected -- the whole listing
            # didn't fail just because one row's mtime was corrupt.
            assert node_modified_time(by_name["fileA.txt"]) is not None
            assert node_modified_time(by_name["fileB.txt"]) is not None
