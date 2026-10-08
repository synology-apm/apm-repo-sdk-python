"""Unit tests for ``synology_apm_repo.sdk.units.saas.object_name_index``:
``resolve_object_name_index``'s "no index recorded" shapes, which return
``None`` rather than raise, and the shared lookup helpers
(``resolve_service_db``, ``read_indexed_table``, ``read_id_to_name_map``,
``read_grouped_names``) — mostly their degrade-to-``None`` branches, which
the providers' tests over well-formed data never reach."""

from __future__ import annotations

import base64
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import cast

import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

import synology_apm_repo.sdk.catalog.version as catalog_version_module
from support.format_builders import (
    build_object_db,
)
from support.repo_builders import (
    write_repo_info,
)
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, ResourceLimitExceededError
from synology_apm_repo.sdk.format.crypto import version_spec_iv
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.units.saas import object_name_index as object_name_index_module
from synology_apm_repo.sdk.units.saas.object_name_index import (
    ObjectNameIndex,
    read_grouped_names,
    read_id_to_name_map,
    read_indexed_table,
    resolve_object_name_index,
    resolve_service_db,
)
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.services import open_service_db
from unit.sdk.saas_fakes import FakeDedupFile

_VERSION_UID = "vuid-catalog"


def _write_copy_target_version(path: Path, version_spec: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE copy_target_version(version_uid TEXT PRIMARY KEY, version_spec TEXT)")
    conn.execute("INSERT INTO copy_target_version VALUES (?, ?)", (_VERSION_UID, version_spec))
    conn.commit()
    conn.close()


class _FakeVersion:
    version_uid = _VERSION_UID


async def _open_repo_with_version_spec(tmp_path: Path, version_spec: object) -> DedupRepo:
    write_repo_info(tmp_path / "repo_info")
    _write_copy_target_version(tmp_path / "db" / "copy_target_version", json.dumps(version_spec))
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


class TestResolveObjectNameIndexMissingLocation:
    async def test_no_copy_target_table_at_all_returns_none(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        repo = await DedupRepo.open(store, layout)
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_no_row_for_this_version_uid_returns_none(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", "{}")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        repo = await DedupRepo.open(store, layout)
        try:
            other = type("V", (), {"version_uid": "no-such-version"})()
            assert await resolve_object_name_index(repo, cast("Version", other)) is None
        finally:
            await repo.close()

    async def test_unparseable_version_spec_with_no_vault_key_returns_none(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", "not json at all")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        repo = await DedupRepo.open(store, layout)
        try:
            assert repo.vault_key is None
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_a_parse_version_spec_failure_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_repo_info(tmp_path / "repo_info")
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", "not json at all")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        repo = await DedupRepo.open(store, layout)
        monkeypatch.setattr(catalog_version_module, "parse_version_spec", lambda *args, **kwargs: None)
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    @pytest.mark.parametrize("additional_meta", ["not json", "[1, 2]", '"a string"'])
    async def test_unparseable_or_non_object_additional_meta_returns_none(
        self, tmp_path: Path, additional_meta: str
    ) -> None:
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    @pytest.mark.parametrize(
        "version_spec",
        [
            # A present key holding a JSON null or "", which
            # ``.get(key, default)`` alone wouldn't cover.
            pytest.param({"status": None}, id="null_status"),
            pytest.param({"status": {}}, id="missing_additional_meta"),
            pytest.param({"status": {"additional_meta": None}}, id="null_additional_meta"),
            pytest.param({"status": {"additional_meta": ""}}, id="empty_string_additional_meta"),
        ],
    )
    async def test_absent_or_falsy_status_or_additional_meta_returns_none(
        self, tmp_path: Path, version_spec: dict[str, object]
    ) -> None:
        repo = await _open_repo_with_version_spec(tmp_path, version_spec)
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    @pytest.mark.parametrize(
        "additional_meta",
        [
            pytest.param(
                {"db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}}, id="missing_object_db_id"
            ),
            pytest.param(
                {"object_db_id": None, "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}},
                id="null_object_db_id",
            ),
            pytest.param(
                {"object_db_id": "", "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}},
                id="empty_string_object_db_id",
            ),
            pytest.param(
                {"object_db_id": "stream_0_10", "db_object_ids": {"db_objects": "not-a-list"}},
                id="db_objects_not_a_list",
            ),
            pytest.param(
                {
                    "object_db_id": "not-shaped-right",
                    "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]},
                },
                id="malformed_object_db_id",
            ),
            pytest.param({"object_db_id": "stream_0_10", "db_object_ids": None}, id="null_db_object_ids"),
            pytest.param({"object_db_id": "stream_0_10", "db_object_ids": ["x"]}, id="db_object_ids_an_array"),
            pytest.param({"object_db_id": "stream_0_10", "db_object_ids": "x"}, id="db_object_ids_a_string"),
            pytest.param(
                {"object_db_id": 7, "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}},
                id="object_db_id_a_number",
            ),
            pytest.param(
                {"object_db_id": ["s_0_10"], "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}},
                id="object_db_id_an_array",
            ),
            pytest.param(
                {
                    "object_db_id": "stream_\u00b2_10",
                    "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]},
                },
                id="object_db_id_with_a_non_ascii_digit",
            ),
            # No entry has both "name" and "object_id", so
            # name_object_id_pairs() filters everything out.
            pytest.param(
                {"object_db_id": "stream_0_10", "db_object_ids": {"db_objects": [{"name": "x"}]}},
                id="no_valid_named_entries",
            ),
        ],
    )
    async def test_unusable_additional_meta_fields_return_none(
        self, tmp_path: Path, additional_meta: dict[str, object]
    ) -> None:
        repo = await _open_repo_with_version_spec(
            tmp_path, {"status": {"additional_meta": json.dumps(additional_meta)}}
        )
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()


class TestResolveObjectNameIndexHappyPath:
    async def test_well_formed_spec_resolves_a_real_object_name_index(self, tmp_path: Path) -> None:
        additional_meta = json.dumps(
            {
                "object_db_id": "stream-abc_100_200",
                "db_object_ids": {
                    "db_objects": [
                        {"name": "svc_a", "object_id": "obj-a"},
                        {"name": "svc_b", "object_id": "obj-b"},
                    ]
                },
            }
        )
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            result = await resolve_object_name_index(repo, cast("Version", _FakeVersion()))
            assert result == ObjectNameIndex(
                stream_uuid="stream-abc", offset=100, length=200, object_ids={"svc_a": "obj-a", "svc_b": "obj-b"}
            )
        finally:
            await repo.close()

    async def test_plaintext_version_spec_under_a_vault_key_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``parse_version_spec`` decrypts whenever a vault key is present
        (the column has no plaintext marker to probe), so a plaintext spec
        under a vault key fails to parse."""
        additional_meta = json.dumps(
            {
                "object_db_id": "stream-abc_100_200",
                "db_object_ids": {"db_objects": [{"name": "svc_a", "object_id": "obj-a"}]},
            }
        )
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        monkeypatch.setattr(type(repo), "vault_key", property(lambda self: b"k" * 32))
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_vault_key_decrypt_success_resolves_a_real_object_name_index(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The ciphertext is built with the AES-256-CTR encrypt side
        directly, not through the decrypt helper under test."""
        vault_key = b"k" * 32
        additional_meta = json.dumps(
            {
                "object_db_id": "stream-xyz_5_15",
                "db_object_ids": {"db_objects": [{"name": "svc_a", "object_id": "obj-a"}]},
            }
        )
        plaintext = json.dumps({"status": {"additional_meta": additional_meta}}).encode("utf-8")
        encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(version_spec_iv(_VERSION_UID))).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(plaintext) + encryptor.finalize()).decode("ascii")

        write_repo_info(tmp_path / "repo_info")
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", ciphertext_b64)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        repo = await DedupRepo.open(store, layout)
        monkeypatch.setattr(type(repo), "vault_key", property(lambda self: vault_key))
        try:
            result = await resolve_object_name_index(repo, cast("Version", _FakeVersion()))
            assert result == ObjectNameIndex(
                stream_uuid="stream-xyz", offset=5, length=15, object_ids={"svc_a": "obj-a"}
            )
        finally:
            await repo.close()


def _build_zstd_sqlite_blob(table_name: str) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "svc.db"
        conn = sqlite3.connect(path)
        conn.execute(f"CREATE TABLE {table_name}(id INTEGER)")
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_zstd_sqlite_blob_with_rows(create_sql: str, insert_sql: str, rows: list[tuple[object, ...]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "svc.db"
        conn = sqlite3.connect(path)
        conn.execute(create_sql)
        conn.executemany(insert_sql, rows)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


class TestResolveServiceDb:
    async def test_object_id_not_in_the_object_db_tries_the_next_alias(self) -> None:
        # The index names an object_id the ObjectDB doesn't have.
        object_db = await ObjectDb.from_bytes(build_object_db([]))
        try:
            object_name_index = ObjectNameIndex(
                stream_uuid="s", offset=0, length=1, object_ids={"alias_a": "missing-obj"}
            )
            result = await resolve_service_db(
                cast(DedupFile, FakeDedupFile(b"")), object_db, object_name_index, ("alias_a",), "item_table"
            )
            assert result is None
        finally:
            await object_db.close()

    async def test_corrupt_bytes_at_the_indexed_location_tries_the_next_alias(self) -> None:
        object_db = await ObjectDb.from_bytes(build_object_db([("obj-a", 0, 4)]))
        try:
            object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={"alias_a": "obj-a"})
            dedup_file = FakeDedupFile(b"\xff\xff\xff\xff")  # not ZSTD-framed, not a SQLite file either
            result = await resolve_service_db(
                cast(DedupFile, dedup_file), object_db, object_name_index, ("alias_a",), "item_table"
            )
            assert result is None
        finally:
            await object_db.close()

    async def test_insufficient_disk_space_tries_the_next_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A valid candidate too large for free disk space
        # (ResourceLimitExceededError) degrades like corrupt bytes.
        import synology_apm_repo.sdk.storage.sqlite_source as sqlite_source_module

        blob = _build_zstd_sqlite_blob("item_table")
        object_db = await ObjectDb.from_bytes(build_object_db([("obj-a", 0, len(blob))]))
        try:

            def _raise_resource_limit(dir_path: object, needed_bytes: object) -> None:
                raise ResourceLimitExceededError("synthetic: not enough disk space", ref=str(dir_path))

            monkeypatch.setattr(sqlite_source_module, "reserve_disk_space", _raise_resource_limit)
            object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={"alias_a": "obj-a"})
            result = await resolve_service_db(
                cast(DedupFile, FakeDedupFile(blob)), object_db, object_name_index, ("alias_a",), "item_table"
            )
            assert result is None
        finally:
            await object_db.close()

    async def test_a_db_whose_schema_cannot_be_read_is_closed_and_the_next_alias_tried(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The SQLite magic and a valid page size, then garbage: it opens,
        # but reading sqlite_master raises sqlite3.DatabaseError.
        corrupt = zstandard.ZstdCompressor().compress(b"SQLite format 3\x00\x10\x00\x01\x01" + b"\xff" * 4000)
        good = _build_zstd_sqlite_blob("item_table")
        object_db = await ObjectDb.from_bytes(
            build_object_db([("obj-a", 0, len(corrupt)), ("obj-b", len(corrupt), len(good))])
        )
        opened: list[SqliteSource] = []

        async def _recording_open(data: bytes | bytearray) -> SqliteSource:
            source = await open_service_db(data)
            opened.append(source)
            return source

        monkeypatch.setattr(object_name_index_module, "open_service_db", _recording_open)
        try:
            object_name_index = ObjectNameIndex(
                stream_uuid="s", offset=0, length=1, object_ids={"alias_a": "obj-a", "alias_b": "obj-b"}
            )
            result = await resolve_service_db(
                cast(DedupFile, FakeDedupFile(corrupt + good)),
                object_db,
                object_name_index,
                ("alias_a", "alias_b"),
                "item_table",
            )
            assert result is not None
            await result.source.close()
            assert result.object_id == "obj-b"
            assert opened[0].path is not None
            assert not Path(opened[0].path).exists()  # close() unlinks the temp copy
        finally:
            await object_db.close()

    async def test_right_db_wrong_table_tries_the_next_alias(self) -> None:
        blob = _build_zstd_sqlite_blob("other_table")
        object_db = await ObjectDb.from_bytes(build_object_db([("obj-a", 0, len(blob))]))
        try:
            object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={"alias_a": "obj-a"})
            result = await resolve_service_db(
                cast(DedupFile, FakeDedupFile(blob)), object_db, object_name_index, ("alias_a",), "item_table"
            )
            assert result is None
        finally:
            await object_db.close()

    async def test_a_later_alias_still_resolves_after_an_earlier_one_fails(self) -> None:
        blob = _build_zstd_sqlite_blob_with_rows(
            "CREATE TABLE item_table(id INTEGER)", "INSERT INTO item_table VALUES (?)", [(42,)]
        )
        object_db = await ObjectDb.from_bytes(build_object_db([("obj-b", 0, len(blob))]))
        try:
            object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={"alias_b": "obj-b"})
            result = await resolve_service_db(
                cast(DedupFile, FakeDedupFile(blob)),
                object_db,
                object_name_index,
                ("alias_a", "alias_b"),  # alias_a isn't in object_ids
                "item_table",
            )
            assert result is not None
            assert result.object_id == "obj-b"
            try:
                cursor = await result.source.connection.execute("SELECT id FROM item_table")
                row = await cursor.fetchone()
                assert row == (42,)
            finally:
                await result.source.close()
        finally:
            await object_db.close()


class TestReadIndexedTable:
    async def test_object_name_index_none_returns_none(self) -> None:
        async def _reader(connection: object) -> str:
            raise AssertionError("unreachable")  # pragma: no cover

        result = await read_indexed_table(
            cast(DedupFile, FakeDedupFile(b"")), None, ("alias_a",), "item_table", _reader
        )
        assert result is None

    async def test_object_db_load_failure_returns_none(self, tmp_path: Path) -> None:
        # SQLite bytes with no object_table, as a bad (offset, length) yields.
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "no_object_table.db"
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE unrelated(x INTEGER)")
            conn.commit()
            conn.close()
            db_bytes = path.read_bytes()

        async def _reader(connection: object) -> str:
            raise AssertionError("unreachable")  # pragma: no cover

        object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=len(db_bytes), object_ids={})
        result = await read_indexed_table(
            cast(DedupFile, FakeDedupFile(db_bytes)), object_name_index, ("alias_a",), "item_table", _reader
        )
        assert result is None

    async def test_insufficient_disk_space_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # ObjectDb.load() itself hitting the disk-space limit.
        import synology_apm_repo.sdk.storage.sqlite_source as sqlite_source_module

        def _raise_resource_limit(dir_path: object, needed_bytes: object) -> None:
            raise ResourceLimitExceededError("synthetic: not enough disk space", ref=str(dir_path))

        monkeypatch.setattr(sqlite_source_module, "reserve_disk_space", _raise_resource_limit)

        async def _reader(connection: object) -> str:
            raise AssertionError("unreachable")  # pragma: no cover

        db_bytes = build_object_db([])
        object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=len(db_bytes), object_ids={})
        result = await read_indexed_table(
            cast(DedupFile, FakeDedupFile(db_bytes)), object_name_index, ("alias_a",), "item_table", _reader
        )
        assert result is None

    async def test_reader_raising_data_corrupt_degrades_to_none_instead_of_crashing(self) -> None:
        """A schema-drifted table (``Table.create`` finding a required
        column missing, as a connector version mismatch produces)."""
        # [service_blob][object_db_bytes]: service_blob first, so the
        # object_table row can name its offset without depending on the
        # ObjectDB's own size.
        service_blob = _build_zstd_sqlite_blob_with_rows(
            "CREATE TABLE item_table(id INTEGER)", "INSERT INTO item_table VALUES (?)", [(42,)]
        )
        object_db_bytes = build_object_db([("obj-a", 0, len(service_blob))])
        dedup_file = FakeDedupFile(service_blob + object_db_bytes)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=len(service_blob), length=len(object_db_bytes), object_ids={"alias_a": "obj-a"}
        )

        async def _reader(connection: object) -> str:
            raise DataCorruptError("item_table is missing required column 'missing_column'")

        result = await read_indexed_table(
            cast(DedupFile, dedup_file), object_name_index, ("alias_a",), "item_table", _reader
        )
        assert result is None


class TestReadIdToNameMap:
    async def test_happy_path_resolves_a_real_id_to_name_map(self) -> None:
        blob = _build_zstd_sqlite_blob_with_rows(
            "CREATE TABLE def_table(id TEXT, name TEXT)",
            "INSERT INTO def_table VALUES (?, ?)",
            [("group-1", "Group One")],
        )
        object_db_len = len(build_object_db([("def-obj", 0, len(blob))]))
        object_db_bytes = build_object_db([("def-obj", object_db_len, len(blob))])
        dedup_file = FakeDedupFile(object_db_bytes + blob)
        object_name_index = ObjectNameIndex(
            stream_uuid="s", offset=0, length=len(object_db_bytes), object_ids={"def_alias": "def-obj"}
        )
        result = await read_id_to_name_map(
            cast(DedupFile, dedup_file),
            object_name_index,
            ("def_alias",),
            "def_table",
            id_column="id",
            name_column="name",
        )
        assert result == {"group-1": "Group One"}


class TestReadGroupedNames:
    async def test_happy_path_falls_back_to_the_raw_id_for_an_unresolved_group(self) -> None:
        """A membership row whose ``group_id`` has no definition keeps the
        raw id."""
        def_blob = _build_zstd_sqlite_blob_with_rows(
            "CREATE TABLE def_table(id TEXT, name TEXT)",
            "INSERT INTO def_table VALUES (?, ?)",
            [("group-1", "Group One")],
        )
        mem_blob = _build_zstd_sqlite_blob_with_rows(
            "CREATE TABLE mem_table(item_id TEXT, group_id TEXT)",
            "INSERT INTO mem_table VALUES (?, ?)",
            [("item-1", "group-1"), ("item-2", "group-unknown")],
        )
        relative_rows = [("def-obj", 0, len(def_blob)), ("mem-obj", len(def_blob), len(mem_blob))]
        object_db_len = len(build_object_db(relative_rows))
        absolute_rows = [(oid, off + object_db_len, ln) for oid, off, ln in relative_rows]
        object_db_bytes = build_object_db(absolute_rows)
        dedup_file = FakeDedupFile(object_db_bytes + def_blob + mem_blob)
        object_name_index = ObjectNameIndex(
            stream_uuid="s",
            offset=0,
            length=len(object_db_bytes),
            object_ids={"def_alias": "def-obj", "mem_alias": "mem-obj"},
        )
        result = await read_grouped_names(
            cast(DedupFile, dedup_file),
            object_name_index,
            definition_names=("def_alias",),
            definition_table="def_table",
            id_column="id",
            name_column="name",
            membership_names=("mem_alias",),
            membership_table="mem_table",
            item_column="item_id",
            group_column="group_id",
        )
        assert result == {"item-1": ["Group One"], "item-2": ["group-unknown"]}

    async def test_membership_unavailable_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _fake_read_id_to_name_map(*args: object, **kwargs: object) -> dict[str, str]:
            return {"group-1": "Group One"}

        async def _fake_read_indexed_table(*args: object, **kwargs: object) -> None:
            return None

        monkeypatch.setattr(object_name_index_module, "read_id_to_name_map", _fake_read_id_to_name_map)
        monkeypatch.setattr(object_name_index_module, "read_indexed_table", _fake_read_indexed_table)

        object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={})
        result = await read_grouped_names(
            cast(DedupFile, FakeDedupFile(b"")),
            object_name_index,
            definition_names=("def_a",),
            definition_table="def_table",
            id_column="id",
            name_column="name",
            membership_names=("mem_a",),
            membership_table="mem_table",
            item_column="item_id",
            group_column="group_id",
        )
        assert result is None
