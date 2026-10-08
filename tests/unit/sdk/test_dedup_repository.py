"""Unit tests for ``synology_apm_repo.sdk.dedup.repository`` — synthetic
repository roots written to real files, no sample repositories
required."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import dataclasses
import os
import sqlite3
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from support.format_builders import (
    mapping_record,
    repo_info_bytes,
)
from support.repo_builders import (
    write_bucket,
    write_composition_entries,
    write_file_map,
    write_vault_encryption_key_db,
)
from support.store_fakes import WrappingStore
from synology_apm_repo.sdk.asynccache import AsyncKeyedCache
from synology_apm_repo.sdk.cachemanager import DEFAULT_LIMITS, CacheLimits
from synology_apm_repo.sdk.dedup.composition_reader import CompositionRecord
from synology_apm_repo.sdk.dedup.fingerprint import FingerprintIndex
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.pool import FULL_VERIFY
from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor, build_worker_pool, close_worker_store
from synology_apm_repo.sdk.dedup.repository import DB_SOURCE_NAMES, DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, KeyMismatchError, KeyRequiredError, NotFoundError
from synology_apm_repo.sdk.identifiers import CompOffset, SessionId, StreamId
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource

_STREAM_ID = StreamId(7)
_SESSION_ID = SessionId(3)
_HEAD_OFF = CompOffset(64)
_PATH = "VM-abc/2026-08-06/disk.img"
_PLAINTEXT = bytes([1]) * 4096


def _write_repo_info(path: Path, *, uuid: str = "abcdefghijklmnop") -> None:
    payload_obj = {
        "repo_type": 2,
        "repo_flag": 0,
        "is_global_dedup_supported": True,
        "is_worm_supported": False,
        "storage_algorithm": {"compress_algorithm": 1, "encrypt_algorithm": 0},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(repo_info_bytes(payload_obj, uuid=uuid.encode("ascii")))


def _write_file_meta(path: Path, rows: list[tuple[str, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE file_meta(path TEXT, file_size INTEGER)")
    conn.executemany("INSERT INTO file_meta(path, file_size) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _write_composition(root: Path) -> None:
    write_composition_entries(root, mapping_record(0, 0, 0, map_num=1), stream_id=_STREAM_ID, session_id=_SESSION_ID)


def _build_full_repo(tmp_path: Path, *, encryption_user_key_uuid: str = "NoEncryption") -> None:
    _write_repo_info(tmp_path / "repo_info")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [(encryption_user_key_uuid, "")])
    write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
    _write_file_meta(tmp_path / "db" / "file_meta", [(_PATH, 4096)])
    _write_composition(tmp_path / "@data" / "Composition")
    write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [_PLAINTEXT])


@pytest.fixture
def vault_layout() -> RepoLayout:
    return RepoLayout(kind=RepoKind.VAULT, repo_root="")


class TestOpen:
    async def test_opens_and_reads_repo_info(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert repo.info.uuid == "abcdefghijklmnop"
            assert repo.info.repo_type == 2

    async def test_resolves_sequence_suffixed_repo_info(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        (tmp_path / "repo_info").rename(tmp_path / "repo_info.5")
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert repo.info.uuid == "abcdefghijklmnop"

    async def test_no_encryption_never_touches_the_key_db(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _write_repo_info(tmp_path / "repo_info")
        # No db/vault_encryption_key: open() must not need it when
        # keys.is_no_encryption.
        store = _SpyStore(LocalFsStore(tmp_path))
        keys = KeyMaterial(user_key_id="NoEncryption", user_key=b"\x00" * 32)
        async with await DedupRepo.open(store, vault_layout, keys) as repo:
            assert repo.info.uuid == "abcdefghijklmnop"
            assert not any("vault_encryption_key" in touched for touched in store.touched_paths)

    async def test_missing_keys_argument_defers_failure_to_first_read(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # open() with keys=None never raises for an encrypted repository;
        # KeyRequiredError surfaces from the first chunk read, before any
        # decrypt.
        _write_repo_info(tmp_path / "repo_info")
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        _write_file_meta(tmp_path / "db" / "file_meta", [(_PATH, 4096)])
        _write_composition(tmp_path / "@data" / "Composition")

        # Never used to decrypt, so any 32 bytes do.
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [_PLAINTEXT], vault_key=os.urandom(32))

        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout, keys=None) as repo:
            with pytest.raises(KeyRequiredError, match="is encrypted but no vault key was provided"):
                await (await repo.open_file(_PATH)).read(0, 4096)

    async def test_missing_wrapped_key_raises_key_required(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        store = LocalFsStore(tmp_path)
        keys = KeyMaterial(user_key_id="abcdefghijkl", user_key=os.urandom(32))
        with pytest.raises(KeyRequiredError, match="no wrapped VaultKey on record for user_key_id"):
            await DedupRepo.open(store, vault_layout, keys)

    async def test_wrong_key_propagates_key_mismatch(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _write_repo_info(tmp_path / "repo_info")
        user_key_id = "abcdefghijkl"
        correct_user_key = os.urandom(32)
        vault_key = os.urandom(32)
        nonce = user_key_id.encode("ascii")[:12]
        wrapped = AESGCM(correct_user_key).encrypt(nonce, vault_key, None)
        write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key", [(user_key_id, base64.b64encode(wrapped).decode())]
        )
        store = LocalFsStore(tmp_path)
        wrong_keys = KeyMaterial(user_key_id=user_key_id, user_key=os.urandom(32))
        with pytest.raises(KeyMismatchError, match="AES-256-GCM tag check failed"):
            await DedupRepo.open(store, vault_layout, wrong_keys)

    async def test_correct_key_enables_reading_encrypted_content(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        user_key_id = "abcdefghijkl"
        user_key = os.urandom(32)
        vault_key = os.urandom(32)
        nonce = user_key_id.encode("ascii")[:12]
        wrapped = AESGCM(user_key).encrypt(nonce, vault_key, None)

        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key", [(user_key_id, base64.b64encode(wrapped).decode())]
        )
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        _write_file_meta(tmp_path / "db" / "file_meta", [(_PATH, 4096)])
        _write_composition(tmp_path / "@data" / "Composition")
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [_PLAINTEXT], vault_key=vault_key)

        store = LocalFsStore(tmp_path)
        keys = KeyMaterial(user_key_id=user_key_id, user_key=user_key)
        async with await DedupRepo.open(store, vault_layout, keys) as repo:
            assert await (await repo.open_file(_PATH)).read(0, 4096) == _PLAINTEXT


class TestDb:
    async def test_returns_a_working_connection(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            conn = await repo.db("file_map")
            cursor = await conn.execute("SELECT path FROM file_map")
            row = await cursor.fetchone()
            assert row == (_PATH,)

    async def test_caches_the_connection(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            first = await repo.db("file_map")
            second = await repo.db("file_map")
            assert first is second

    async def test_resolves_sequence_suffixed_db_file(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        (tmp_path / "db" / "file_map").rename(tmp_path / "db" / "file_map.42")
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            conn = await repo.db("file_map")
            cursor = await conn.execute("SELECT path FROM file_map")
            row = await cursor.fetchone()
            assert row == (_PATH,)

    async def test_concurrent_opens_for_the_same_name_build_the_source_only_once(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Concurrent misses on the same name build exactly one
        ``SqliteSource``, and every caller shares its connection."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            builds = 0
            real_from_raw_store = SqliteSource.from_raw_store.__func__  # type: ignore[attr-defined]

            async def counting_from_raw_store(cls: type[SqliteSource], store: ObjectStore, path: str) -> SqliteSource:
                nonlocal builds
                builds += 1
                return await real_from_raw_store(cls, store, path)  # type: ignore[no-any-return]

            monkeypatch.setattr(SqliteSource, "from_raw_store", classmethod(counting_from_raw_store))

            connections = await asyncio.gather(*(repo.db("file_map") for _ in range(10)))

            assert builds == 1
            assert all(c is connections[0] for c in connections)
            assert len(repo._db_sources) == 1

    async def test_concurrent_opens_for_different_names_do_not_serialize_on_each_other(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only misses on the same name contend; different names never wait
        on each other."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            file_map_started = asyncio.Event()
            release_file_map = asyncio.Event()
            real_from_raw_store = SqliteSource.from_raw_store.__func__  # type: ignore[attr-defined]

            async def blocking_from_raw_store(cls: type[SqliteSource], store: ObjectStore, path: str) -> SqliteSource:
                if path.endswith("file_map"):
                    file_map_started.set()
                    await release_file_map.wait()
                return await real_from_raw_store(cls, store, path)  # type: ignore[no-any-return]

            monkeypatch.setattr(SqliteSource, "from_raw_store", classmethod(blocking_from_raw_store))

            file_map_task = asyncio.create_task(repo.db("file_map"))
            await asyncio.wait_for(file_map_started.wait(), timeout=1)
            # file_map's own fetch is still parked on release_file_map — a
            # different name must resolve without waiting behind it.
            file_meta = await asyncio.wait_for(repo.db("file_meta"), timeout=1)
            assert not file_map_task.done()
            release_file_map.set()
            file_map = await asyncio.wait_for(file_map_task, timeout=1)
            assert file_map is not file_meta
            assert await repo.db("file_map") is file_map
            assert await repo.db("file_meta") is file_meta


class TestLocateFile:
    async def test_returns_the_expected_triple_and_size(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.stream_id == _STREAM_ID
            assert loc.session_id == _SESSION_ID
            assert loc.comp_offset == _HEAD_OFF
            assert loc.file_size == 4096

    async def test_missing_path_raises_not_found(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            with pytest.raises(NotFoundError, match="no file_map entry for path"):
                await repo.locate_file("no/such/path")

    async def test_deleted_renamed_path_still_resolves_via_its_own_triple(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        deleted_path = f"{_PATH}_deleted_1699999999000"
        write_file_map(tmp_path / "db" / "file_map", [(deleted_path, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        _write_composition(tmp_path / "@data" / "Composition")
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [_PLAINTEXT])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(deleted_path)
            assert loc.stream_id == _STREAM_ID
            assert loc.session_id == _SESSION_ID
            assert loc.comp_offset == _HEAD_OFF
            assert await (await repo.open_file(deleted_path)).read(0, 4096) == _PLAINTEXT

    async def test_missing_file_meta_db_file_gives_none_size_not_a_crash(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        # No db/file_meta at all: _build_file_meta_table's NotFoundError branch.
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    async def test_file_meta_db_exists_but_lacks_the_file_meta_table_gives_none_size(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # db/file_meta exists without a file_meta table: Table.exists_in()'s
        # branch.
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        (tmp_path / "db").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp_path / "db" / "file_meta")
        conn.execute("CREATE TABLE some_other_table(x INTEGER)")
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    async def test_file_meta_table_has_no_row_for_this_path_gives_none_size(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        _write_file_meta(tmp_path / "db" / "file_meta", [("some/other/path", 4096)])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    async def test_file_meta_table_exists_but_lacks_file_size_column_gives_none_size(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # No file_size column: Table's missing-optional-column handling.
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        (tmp_path / "db").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp_path / "db" / "file_meta")
        conn.execute("CREATE TABLE file_meta(path TEXT)")
        conn.execute("INSERT INTO file_meta(path) VALUES (?)", (_PATH,))
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    async def test_file_meta_row_with_null_file_size_gives_none_size(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        (tmp_path / "db").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp_path / "db" / "file_meta")
        conn.execute("CREATE TABLE file_meta(path TEXT, file_size INTEGER)")
        conn.execute("INSERT INTO file_meta(path, file_size) VALUES (?, NULL)", (_PATH,))
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    @pytest.mark.parametrize("status", [0, 1, 3])
    async def test_not_yet_or_no_longer_complete_status_raises_not_found(
        self, tmp_path: Path, vault_layout: RepoLayout, status: int
    ) -> None:
        # Initialized (0) / Written (1) / Compacted (3): neither known-bad
        # nor Complete (2) (FORMAT-SPEC.md: db/file_map).
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, status)])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            with pytest.raises(NotFoundError, match=r"file_map row for path .* not Complete"):
                await repo.locate_file(_PATH)

    @pytest.mark.parametrize("status", [4, 5])
    async def test_known_bad_status_raises_data_corrupt(
        self, tmp_path: Path, vault_layout: RepoLayout, status: int
    ) -> None:
        # Corrupted (4) / Tainted (5): the known-bad values (FORMAT-SPEC.md: db/file_map).
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, status)])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            with pytest.raises(DataCorruptError, match=r"file_map row for path .* \(Corrupted/Tainted\)"):
                await repo.locate_file(_PATH)


class TestFileMapPathsWithPrefix:
    async def test_status_filter_excludes_non_matching_rows(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                ("prefix/a", _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2),
                ("prefix/b", _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 1),
            ],
        )
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert await repo.file_map_paths_with_prefix("prefix/") == ["prefix/a", "prefix/b"]
            assert await repo.file_map_paths_with_prefix("prefix/", status=2) == ["prefix/a"]
            assert await repo.file_map_paths_with_prefix("prefix/", status=1) == ["prefix/b"]
            assert await repo.file_map_paths_with_prefix("prefix/", status=4) == []


class TestOpenFileAndComposition:
    async def test_open_file_reads_real_content(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            f = await repo.open_file(_PATH)
            assert f.size == 4096
            assert await f.read(0, 4096) == _PLAINTEXT

    async def test_open_file_surfaces_the_same_status_gate_as_locate_file(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # open_file() has no status check of its own; it goes through locate_file().
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 4)])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            with pytest.raises(DataCorruptError, match=r"file_map row for path .* \(Corrupted/Tainted\)"):
                await repo.open_file(_PATH)

    async def test_open_composition_bypasses_file_map(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            f = repo.open_composition(_STREAM_ID, _SESSION_ID, _HEAD_OFF, size=4096)
            assert await f.read(0, 4096) == _PLAINTEXT

    async def test_open_composition_shares_its_repo_wide_composition_record_cache(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        """``open_composition()`` threads the repository's shared record
        cache into each ``CompositionReader`` it builds, so two calls for the
        same triple resolve the identical ``CompositionRecord``."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            first = repo.open_composition(_STREAM_ID, _SESSION_ID, _HEAD_OFF, size=4096)
            second = repo.open_composition(_STREAM_ID, _SESSION_ID, _HEAD_OFF, size=4096)
            assert await first.cached_record() is await second.cached_record()

    async def test_composition_records_limit_propagates_from_open(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(
            store, vault_layout, limits=dataclasses.replace(DEFAULT_LIMITS, composition_records=5)
        ) as repo:
            assert repo._composition_records.maxsize == 5


class TestCloseAndContextManager:
    async def test_close_clears_connection_cache(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        repo = await DedupRepo.open(store, vault_layout)
        await repo.db("file_map")
        await repo.close()
        assert repo._db_sources == {}

    async def test_used_as_a_context_manager(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert await (await repo.open_file(_PATH)).read(0, 4096) == _PLAINTEXT
        assert repo._db_sources == {}

    async def test_close_clears_composition_record_cache(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        repo = await DedupRepo.open(store, vault_layout)
        await repo.open_composition(_STREAM_ID, _SESSION_ID, _HEAD_OFF, size=4096).cached_record()
        assert len(repo._composition_records) > 0
        await repo.close()
        assert repo._composition_records == {}

    async def test_close_waits_for_a_composition_record_fetch_still_in_flight(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``close()`` does not return while a composition-record fetch is
        still in flight."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        repo = await DedupRepo.open(store, vault_layout)

        started = asyncio.Event()
        release = asyncio.Event()
        dedup_file = repo.open_composition(_STREAM_ID, _SESSION_ID, _HEAD_OFF, size=4096)
        real_build = dedup_file._comp_reader._build_record

        async def _slow_build(head_off: int) -> CompositionRecord:
            started.set()
            await release.wait()
            return await real_build(head_off)

        monkeypatch.setattr(dedup_file._comp_reader, "_build_record", _slow_build)

        cache = repo._composition_records
        real_quiesce = cache.quiesce
        quiescing = asyncio.Event()

        async def _signalling_quiesce() -> list[Exception]:
            quiescing.set()
            return await real_quiesce()

        monkeypatch.setattr(cache, "quiesce", _signalling_quiesce)

        resolve_task = asyncio.create_task(dedup_file.cached_record())
        await started.wait()  # the fetch is in flight (owner determined), not yet settled

        close_task = asyncio.create_task(repo.close())
        await quiescing.wait()  # close() reached the cache while the fetch is still in flight
        assert not close_task.done()  # blocked on the in-flight fetch, not returned early
        release.set()

        await resolve_task
        await close_task

        assert repo._composition_records == {}

    async def test_a_db_call_still_in_flight_when_close_runs_still_gets_closed(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``SqliteSource`` whose open is still in flight when ``close()``
        runs is closed once it lands: ``close()`` settles in-flight entries
        (``settle_all()``), not only settled ones."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        repo = await DedupRepo.open(store, vault_layout)

        started = asyncio.Event()
        release = asyncio.Event()
        created: list[SqliteSource] = []
        real_build = repo._build_db_source

        async def _slow_build(name: str) -> SqliteSource:
            started.set()
            await release.wait()
            source = await real_build(name)
            created.append(source)
            return source

        # Rebind the fetch rather than replace the cache, which the
        # repository's CacheManager owns.
        repo._db_sources._fetch = _slow_build
        real_settle_all = repo._db_sources.settle_all
        settling = asyncio.Event()

        async def _signalling_settle_all() -> tuple[dict[str, SqliteSource], list[Exception]]:
            settling.set()
            return await real_settle_all()

        monkeypatch.setattr(repo._db_sources, "settle_all", _signalling_settle_all)

        resolve_task = asyncio.create_task(repo.db("file_map"))
        await started.wait()  # the open is in flight (owner determined), not yet settled

        close_task = asyncio.create_task(repo.close())
        await settling.wait()  # close() reached the cache while the open is still in flight
        assert not close_task.done()
        release.set()

        await resolve_task
        await close_task

        assert len(created) == 1
        assert created[0]._closed is True
        assert repo._db_sources == {}

    async def test_close_reports_but_does_not_abort_when_one_source_fails_to_close(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One source failing to close doesn't stop the others' close
        attempts, and the failure surfaces in an ``ExceptionGroup``."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        repo = await DedupRepo.open(store, vault_layout)
        await repo.db("file_map")
        await repo.db("file_meta")

        failing_source = repo._db_sources["file_map"]
        other_source = repo._db_sources["file_meta"]

        async def _failing_close() -> None:
            raise RuntimeError("synthetic close failure")

        monkeypatch.setattr(failing_source, "close", _failing_close)

        try:
            with pytest.raises(
                ExceptionGroup, match=r"DedupRepo\.close\(\) failed to close every tracked resource"
            ) as exc_info:
                await repo.close()
            assert len(exc_info.value.exceptions) == 1
            assert isinstance(exc_info.value.exceptions[0], RuntimeError)
            assert other_source._closed is True
            assert repo._db_sources == {}
        finally:
            # The patched close() never releases the real connection.
            await failing_source.connection.close()


async def test_bad_repo_info_magic_raises_data_corrupt(tmp_path: Path, vault_layout: RepoLayout) -> None:
    path = tmp_path / "repo_info"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"X" * 64)
    store = LocalFsStore(tmp_path)
    with pytest.raises(DataCorruptError, match="bad magic"):
        await DedupRepo.open(store, vault_layout)


class _SpyStore(WrappingStore):
    """Records every path passed to ``read``/``size``/``exists``/``listdir``,
    to prove a path is never touched."""

    def __init__(self, backing: LocalFsStore) -> None:
        super().__init__(backing)
        self.touched_paths: list[str] = []

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self.touched_paths.append(path)
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        self.touched_paths.append(path)
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        self.touched_paths.append(path)
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[Entry]:
        self.touched_paths.append(path)
        return await self._backing.listdir(path)


class TestRestorePathNeverTouchesAuxiliaryFiles:
    async def test_ref_hot_and_sample_index_files_are_never_read(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        (tmp_path / "@data" / "Pool" / "0" / "0.ref").write_bytes(b"bRfC" + b"\x00" * 60)
        (tmp_path / "@data" / "sample.index").write_bytes(b"sMPl" + b"\x00" * 60)
        (tmp_path / "@data" / "Hot").mkdir(parents=True, exist_ok=True)
        (tmp_path / "@data" / "Hot" / "1.idx").write_bytes(b"HoOt" + b"\x00" * 60)
        store = LocalFsStore(tmp_path)
        spy = _SpyStore(store)
        async with await DedupRepo.open(spy, vault_layout) as repo:
            content = await (await repo.open_file(_PATH)).read(0, 4096)

            assert content == _PLAINTEXT
            forbidden = (".ref", "sample.index", "Hot/")
            assert not any(pat in touched for touched in spy.touched_paths for pat in forbidden)


class TestCacheRegistry:
    async def test_every_cache_this_repository_owns_is_registered(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        """Guard: a cache attribute added to ``DedupRepo``/``Pool`` without a
        registry decision fails here, so ``invalidate_all()`` cannot silently
        miss it."""
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            repo_caches = {name for name, value in vars(repo).items() if isinstance(value, AsyncKeyedCache | DirCache)}
            pool_caches = {
                name
                for name, value in vars(repo._pool).items()
                if isinstance(value, AsyncKeyedCache | FingerprintIndex)
            }
            assert repo_caches == {
                "_db_sources",
                "_file_meta_table_cache",
                "_composition_records",
                "_dir_cache",
                "_fixed_dir_cache",
            }
            assert pool_caches == {"_buckets", "_chunks", "_fingerprints"}
            assert repo.caches.names() == [
                "dir_scan",
                "dir_fixed",
                "pool",
                "db_sources",
                "file_meta_table",
                "composition_records",
            ]
            assert set(repo.caches.stats()) == {
                "dir_scan",
                "dir_fixed",
                "pool.buckets",
                "pool.chunks",
                "pool.allocation_tables",
                "db_sources",
                "file_meta_table",
                "composition_records",
            }

    async def test_every_registered_cache_is_bounded_or_says_what_bounds_it(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            unbounded = {name for name, stats in repo.caches.stats().items() if stats.maxsize is None}

            assert unbounded == {"db_sources"}
            assert repo.caches.bounds()["db_sources"] == "closed key set DB_SOURCE_NAMES"

    async def test_default_bounds_come_from_cache_limits(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            stats = repo.caches.stats()

            assert stats["dir_scan"].maxsize == DEFAULT_LIMITS.dir_scan
            assert stats["dir_fixed"].maxsize == DEFAULT_LIMITS.dir_fixed
            assert stats["pool.buckets"].maxsize == DEFAULT_LIMITS.bucket_readers
            assert stats["pool.chunks"].maxsize == DEFAULT_LIMITS.chunks
            assert stats["pool.allocation_tables"].maxsize == DEFAULT_LIMITS.allocation_tables
            assert stats["composition_records"].maxsize == DEFAULT_LIMITS.composition_records

    async def test_custom_limits_bound_every_cache_and_every_derived_pool(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        limits = CacheLimits(
            bucket_readers=3, chunks=5, composition_records=7, dir_scan=11, dir_fixed=13, allocation_tables=17
        )
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout, limits=limits) as repo:
            stats = repo.caches.stats()
            assert stats["dir_scan"].maxsize == 11
            assert stats["dir_fixed"].maxsize == 13
            assert stats["pool.buckets"].maxsize == 3
            assert stats["pool.chunks"].maxsize == 5
            assert stats["pool.allocation_tables"].maxsize == 17
            assert stats["composition_records"].maxsize == 7

            derived = repo.new_pool(verify=FULL_VERIFY)
            assert derived.limits == limits
            assert derived.cache_stats()["pool.buckets"].maxsize == 3

            descriptor = PoolDescriptor.from_pool(derived)
            assert descriptor is not None
            assert descriptor.limits == limits
            store, worker_pool = build_worker_pool(descriptor)
            try:
                assert worker_pool.cache_stats()["pool.chunks"].maxsize == 5
            finally:
                await close_worker_store(store)

    async def test_invalidate_all_closes_the_db_sources_and_the_repository_keeps_working(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            assert await (await repo.open_file(_PATH)).read(0, 4096) == _PLAINTEXT
            first = repo._db_sources["file_map"]

            await repo.caches.invalidate_all()

            assert first._closed is True
            assert len(repo._db_sources) == 0
            assert len(repo._composition_records) == 0
            assert await (await repo.open_file(_PATH)).read(0, 4096) == _PLAINTEXT  # reopened on demand
            assert repo._db_sources["file_map"] is not first

    async def test_invalidate_drops_the_file_meta_table_before_its_connection_is_closed(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            await repo.open_file(_PATH)  # builds the file_meta Table on its connection
            assert len(repo._file_meta_table_cache) == 1

            await repo.caches.invalidate("db_sources", "file_meta_table")

            assert len(repo._file_meta_table_cache) == 0
            assert len(repo._db_sources) == 0

    async def test_invalidating_only_db_sources_still_drops_the_table_bound_to_a_closed_connection(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            await repo.open_file(_PATH)  # builds the file_meta Table on its connection
            assert len(repo._file_meta_table_cache) == 1

            await repo.caches.invalidate("db_sources")  # the dependent cache is not named

            assert len(repo._file_meta_table_cache) == 0
            assert await (await repo.open_file(_PATH)).read(0, 4096) == _PLAINTEXT  # no closed-connection Table

    async def test_invalidating_the_pool_bumps_its_release_epoch(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            before = repo._pool.release_epoch

            await repo.caches.invalidate("pool")

            assert repo._pool.release_epoch == before + 1

    async def test_db_rejects_a_name_outside_the_closed_key_set(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            with pytest.raises(ValueError, match="DB_SOURCE_NAMES"):
                await repo.db("not_a_known_db")

            assert len(repo._db_sources) == 0

    async def test_the_alias_copy_target_file_resolves_to_a_registered_name(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        async with await DedupRepo.open(LocalFsStore(tmp_path), vault_layout) as repo:
            with contextlib.suppress(NotFoundError):  # this synthetic repo has no copy_target_version file
                await repo.db("copy_target_file")  # the alias itself must be accepted

            assert "copy_target_file" not in DB_SOURCE_NAMES

    async def test_close_reports_failures_flattened_under_its_own_message(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_full_repo(tmp_path)
        repo = await DedupRepo.open(LocalFsStore(tmp_path), vault_layout)
        await repo.db("file_map")
        source = repo._db_sources["file_map"]

        async def failing_close() -> None:
            raise OSError("cannot close")

        monkeypatch.setattr(source, "close", failing_close)

        with pytest.raises(ExceptionGroup, match=r"DedupRepo.close") as exc_info:
            await repo.close()

        assert [type(e) for e in exc_info.value.exceptions] == [OSError]
        monkeypatch.undo()
        await source.close()
