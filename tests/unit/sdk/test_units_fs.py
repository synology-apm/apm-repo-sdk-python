"""Unit tests for ``units.fs``, using synthetic repository roots written to
real files."""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import zstandard

from support.model_factories import make_version
from support.repo_builders import (
    write_bucket,
    write_composition,
    write_file_map,
    write_repo_info,
    write_target_db_with_version_id,
    write_vault_encryption_key_db,
)
from support.store_fakes import CountingStore
from synology_apm_repo.sdk.catalog.version import Version, VersionMeta
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import NotFoundError, NotRestorableError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.units.fs import FsProvider, _Dir

_STREAM_ID = 8
_SNAPSHOT_UUID = "fs-uuid"
_VERSION_ID = 7
_DEDUP_IMG = (b"file-A-content--" * 256) + (b"file-B-content--" * 256)  # 2 x 4096 bytes
assert len(_DEDUP_IMG) == 8192


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


def _build_fs_repo(
    tmp_path: Path,
    *,
    session_id: int = 3,
    extra_entry_rows: tuple[tuple[str, str, int, int, int, str, str], ...] = (),
) -> None:
    write_repo_info(tmp_path / "repo_info")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    dedup_img_path = f"{_SNAPSHOT_UUID}/{_VERSION_ID}/dedup.img"
    write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, session_id, 64, 2, 2)])
    write_target_db_with_version_id(tmp_path / "copy_meta_file" / "FS_uid1" / "target.db", _VERSION_ID)

    entry_rows = [
        ("dir1", "/", 0, 0, 2, "", ""),
        ("fileA.txt", "/dir1", 4096, 1700000000, 1, "0", "[]"),
        ("fileB.txt", "/dir1", 4096, 1700000001, 1, "4096", "[]"),
        *extra_entry_rows,
    ]
    version_db_dir = tmp_path / "copy_meta_file" / "FS_uid1" / "ActiveBackup_2026-01-01_120000_vuuid"
    zst_bytes = _write_entry_table(version_db_dir / "_source.db", entry_rows)
    (version_db_dir / "version.db.zst").write_bytes(zst_bytes)

    write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=2)
    write_bucket(
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
    return make_version(version_uid="vuid-fs", target_type="FS", target_id=_SNAPSHOT_UUID, meta=meta)


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
            assert dir1.handle == _Dir("/dir1")


class TestContent:
    async def test_reads_the_real_dedup_content(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            file_a = next(f for f in await provider.children(dir1) if f.name == "fileA.txt")
            file_b = next(f for f in await provider.children(dir1) if f.name == "fileB.txt")

            content_a = (await provider.unit(file_a)).content
            content_b = (await provider.unit(file_b)).content
            assert await content_a.read(0, 4096) == _DEDUP_IMG[0:4096]
            assert await content_b.read(0, 4096) == _DEDUP_IMG[4096:8192]

    async def test_a_files_content_is_a_view_sized_to_the_file(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            file_b = next(f for f in await provider.children(dir1) if f.name == "fileB.txt")
            unit = await provider.unit(file_b)
            assert unit.content.size == 4096

    async def test_unit_on_a_directory_node_raises(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            with pytest.raises(NotRestorableError, match="not a restorable unit"):
                await provider.unit(dir1)

    async def test_dedup_img_is_cached_across_multiple_unit_calls(self, repo: DedupRepo) -> None:
        async with FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            file_a = next(f for f in await provider.children(dir1) if f.name == "fileA.txt")
            first = await provider.dedup_img()
            await provider.unit(file_a)
            second = await provider.dedup_img()
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
        entry_db = await provider._entry_db.get()
        path = entry_db._path
        assert path is not None
        assert os.path.exists(path)

        await provider.close()

        assert not provider._entry_db.opened
        assert not os.path.exists(path)  # SqliteSource.close() removes its temp file

    async def test_concurrent_first_uses_open_the_entry_table_once(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two ``children()`` calls racing the first open must share one
        ``version.db`` connection: a second one would be overwritten and
        never closed."""
        opened: list[SqliteSource] = []
        real = SqliteSource.from_enveloped_bytes

        async def counting(*args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(0)
            result = await real(*args, **kwargs)
            opened.append(result[0])
            return result

        monkeypatch.setattr(SqliteSource, "from_enveloped_bytes", counting)
        async with FsProvider(repo, _version()) as provider:
            root = provider.root()
            await asyncio.gather(provider.children(root), provider.children(root))
        assert len(opened) == 1

    async def test_zero_byte_leaf_reads_as_empty_without_a_real_read(self, tmp_path: Path) -> None:
        _build_fs_repo(tmp_path, extra_entry_rows=(("empty.txt", "/dir1", 0, 1700000002, 1, "8192", "[]"),))
        store = CountingStore(LocalFsStore(tmp_path))
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            empty = next(f for f in await provider.children(dir1) if f.name == "empty.txt")
            assert empty.size == 0

            content = (await provider.unit(empty)).content
            assert content.size == 0
            reads_before = store.read_count
            assert await content.read() == b""
            assert store.read_count == reads_before


class TestErrorHandling:
    async def test_missing_version_meta_raises_not_found(self, repo: DedupRepo) -> None:
        provider = FsProvider(repo, _version(no_meta=True))
        with pytest.raises(NotFoundError, match="has no copy_target_version_meta row"):
            await provider.children(provider.root())

    async def test_missing_version_db_zst_in_meta_filenames_raises_not_found(self, repo: DedupRepo) -> None:
        provider = FsProvider(repo, _version(meta_filenames=("target.db",)))
        with pytest.raises(NotFoundError, match=r"has no version\.db\.zst in meta_filenames"):
            await provider.children(provider.root())

    async def test_empty_version_table_raises_not_found(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        write_file_map(tmp_path / "db" / "file_map", [])
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
            with pytest.raises(NotFoundError, match=r"target\.db has no version_table row"):
                await provider.dedup_img()


class TestPagination:
    """``children()`` pages in SQL, ordering containers first, then by basename."""

    async def test_pagination_matches_full_list_slice_sorted_by_basename(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        dedup_img_path = f"{_SNAPSHOT_UUID}/{_VERSION_ID}/dedup.img"
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, 3, 64, 2, 2)])
        write_target_db_with_version_id(tmp_path / "copy_meta_file" / "FS_uid1" / "target.db", _VERSION_ID)

        # Inserted out of basename order: the ORDER BY must decide the order.
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
        write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        dedup_img_path = f"{_SNAPSHOT_UUID}/{_VERSION_ID}/dedup.img"
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, 3, 64, 2, 2)])
        write_target_db_with_version_id(tmp_path / "copy_meta_file" / "FS_uid1" / "target.db", _VERSION_ID)

        # "zzz_dir" sorts after "aaa.txt" by name alone.
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
    async def test_an_out_of_range_file_mtime_degrades_to_no_mtime_without_failing_the_listing(
        self, tmp_path: Path
    ) -> None:
        """An out-of-``datetime``-range ``file_mtime`` (corrupt data) blanks
        only that row's Modified cell instead of failing the whole listing."""
        _build_fs_repo(tmp_path, extra_entry_rows=(("corrupt.txt", "/dir1", 10, 99999999999999, 1, "8192", "[]"),))
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, FsProvider(repo, _version()) as provider:
            dir1 = (await provider.children(provider.root()))[0]
            children = await provider.children(dir1)
            by_name = {c.name: c for c in children}
            assert (by_name["corrupt.txt"]).mtime is None
            # The other rows are unaffected.
            assert (by_name["fileA.txt"]).mtime is not None
            assert (by_name["fileB.txt"]).mtime is not None
