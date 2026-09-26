"""Unit tests for ``synology_apm_repo.sdk.units.saas.object_name_index`` —
``resolve_object_name_index``'s own "no index recorded, never raises"
failure shapes, plus the shared lookup helpers (``resolve_service_db``,
``read_indexed_table``, ``read_grouped_names``) every SaaS
provider's secondary-table lookups build on. Every real caller
(``mail.py``/``contact.py``/``site.py``/``teams_chat.py``) only ever
exercises these against well-formed connector data, so most of this
module's own defensive branches never run via them — exercised directly
here instead, against minimal fakes/hand-built bytes."""

from __future__ import annotations

import base64
import json
import sqlite3
import tempfile
import zlib
from pathlib import Path
from typing import cast

import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError
from synology_apm_repo.sdk.format.crypto import version_spec_iv
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.saas import object_name_index as catalog_index_module
from synology_apm_repo.sdk.units.saas.object_name_index import (
    ObjectNameIndex,
    read_grouped_names,
    read_id_to_name_map,
    read_indexed_table,
    resolve_object_name_index,
    resolve_service_db,
)
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb

_VERSION_UID = "vuid-catalog"


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
    _write_repo_info(tmp_path / "repo_info")
    _write_copy_target_version(tmp_path / "db" / "copy_target_version", json.dumps(version_spec))
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


class TestResolveCatalogIndexMissingLocation:
    async def test_no_copy_target_table_at_all_returns_none(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        repo = await DedupRepo.open(store, layout)
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_no_row_for_this_version_uid_returns_none(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
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
        _write_repo_info(tmp_path / "repo_info")
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
        """``catalog.version.parse_version_spec`` (shared with
        ``catalog/version.py``'s status filter) has no decrypt/detect
        logic of its own — any failure there is exercised here by
        monkeypatching the shared function directly."""
        _write_repo_info(tmp_path / "repo_info")
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", "not json at all")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        repo = await DedupRepo.open(store, layout)
        monkeypatch.setattr(catalog_index_module, "parse_version_spec", lambda *args, **kwargs: None)
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_unparseable_additional_meta_returns_none(self, tmp_path: Path) -> None:
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": "not json"}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_null_status_returns_none(self, tmp_path: Path) -> None:
        """``status`` key present but JSON ``null`` — a present-but-null
        outer key, distinct from every ``additional_meta``/
        ``object_db_id`` case below (all of which have ``status`` itself
        present as an object). ``.get("status", {})`` would return
        ``None`` here, not ``{}``, and crash the chained ``.get(...)``
        with ``AttributeError`` — the bug this test guards against."""
        repo = await _open_repo_with_version_spec(tmp_path, {"status": None})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_missing_additional_meta_returns_none(self, tmp_path: Path) -> None:
        """``additional_meta`` key absent entirely — distinct from the
        null/empty-string cases below, which have the key present with a
        falsy value ``.get(key) or default`` would also catch, but a bare
        ``.get(key, default)`` would not."""
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_null_additional_meta_returns_none(self, tmp_path: Path) -> None:
        """``additional_meta`` key present but JSON ``null`` — distinct
        from the missing-key case above."""
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": None}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_empty_string_additional_meta_returns_none(self, tmp_path: Path) -> None:
        """``additional_meta`` key present but an empty string — distinct
        from both the missing-key and JSON-null cases above."""
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": ""}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_missing_object_db_id_returns_none(self, tmp_path: Path) -> None:
        """``object_db_id`` key absent entirely — distinct from the
        null/empty-string cases below."""
        additional_meta = json.dumps({"db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}})
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_null_object_db_id_returns_none(self, tmp_path: Path) -> None:
        """``object_db_id`` key present but JSON ``null`` — distinct from
        the missing-key case above."""
        additional_meta = json.dumps(
            {"object_db_id": None, "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}}
        )
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_empty_string_object_db_id_returns_none(self, tmp_path: Path) -> None:
        """``object_db_id`` key present but an empty string — distinct
        from both the missing-key and JSON-null cases above."""
        additional_meta = json.dumps(
            {"object_db_id": "", "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}}
        )
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_db_objects_not_a_list_returns_none(self, tmp_path: Path) -> None:
        additional_meta = json.dumps({"object_db_id": "stream_0_10", "db_object_ids": {"db_objects": "not-a-list"}})
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_malformed_object_db_id_returns_none(self, tmp_path: Path) -> None:
        additional_meta = json.dumps(
            {"object_db_id": "not-shaped-right", "db_object_ids": {"db_objects": [{"name": "x", "object_id": "y"}]}}
        )
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_null_db_object_ids_returns_none(self, tmp_path: Path) -> None:
        """``db_object_ids`` key present but JSON ``null`` — a
        present-but-null outer key, distinct from
        ``test_db_objects_not_a_list_returns_none`` below (where
        ``db_object_ids`` itself is present as an object).
        ``.get("db_object_ids", {})`` would return ``None`` here, not
        ``{}``, and crash the chained ``.get(...)`` with
        ``AttributeError`` — the bug this test guards against."""
        additional_meta = json.dumps({"object_db_id": "stream_0_10", "db_object_ids": None})
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()

    async def test_no_valid_named_entries_returns_none(self, tmp_path: Path) -> None:
        # object_db_id is well-shaped and db_objects is a list, but none
        # of its entries actually have both a "name" and "object_id" —
        # name_object_id_pairs() filters everything out.
        additional_meta = json.dumps({"object_db_id": "stream_0_10", "db_object_ids": {"db_objects": [{"name": "x"}]}})
        repo = await _open_repo_with_version_spec(tmp_path, {"status": {"additional_meta": additional_meta}})
        try:
            assert await resolve_object_name_index(repo, cast("Version", _FakeVersion())) is None
        finally:
            await repo.close()


class TestResolveCatalogIndexHappyPath:
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

    async def test_plaintext_version_spec_under_a_vault_key_now_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``catalog.version.parse_version_spec`` decrypts unconditionally
        whenever a vault_key is present, never probing whether a row is
        already plaintext first — production never lands a plaintext row
        under an encrypted connection anyway. So the identical,
        otherwise-well-formed plaintext ``version_spec`` from
        ``test_well_formed_spec_resolves_a_real_object_name_index`` above
        resolves to ``None`` here once a vault_key is present, instead of
        being parsed as plaintext."""
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
        """Distinct from ``test_a_parse_version_spec_failure_returns_none``
        above: here ``decrypt_version_spec`` is NOT monkeypatched — real AES-256-CTR
        ciphertext, built the same way ``test_format_crypto.py``'s own
        ``TestDecryptVersionSpec`` round-trip test does (encrypt side of the
        same primitives, never by calling back into the decrypt helper under
        test), is decrypted for real and its plaintext resolved into a real
        ``ObjectNameIndex``."""
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

        _write_repo_info(tmp_path / "repo_info")
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


class _FakeDedupFile:
    def __init__(self, buf: bytes) -> None:
        self._buf = buf
        self.size = len(buf)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if length is None:
            length = self.size - offset
        return self._buf[offset : offset + length]


def _build_object_db_bytes(rows: list[tuple[str, int, int]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE object_table(object_id TEXT PRIMARY KEY, offset INTEGER, length INTEGER)")
        conn.executemany("INSERT INTO object_table VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        return path.read_bytes()


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
        # object_name_index records this alias's object_id, but the real
        # ObjectDB at this location was never actually given it -- a
        # stale/malformed index, not this function's job to raise on.
        object_db = await ObjectDb.from_bytes(_build_object_db_bytes([]))
        try:
            object_name_index = ObjectNameIndex(
                stream_uuid="s", offset=0, length=1, object_ids={"alias_a": "missing-obj"}
            )
            result = await resolve_service_db(
                cast(DedupFile, _FakeDedupFile(b"")), object_db, object_name_index, ("alias_a",), "item_table"
            )
            assert result is None
        finally:
            await object_db.close()

    async def test_corrupt_bytes_at_a_catalog_authoritative_location_tries_the_next_alias(self) -> None:
        object_db = await ObjectDb.from_bytes(_build_object_db_bytes([("obj-a", 0, 4)]))
        try:
            object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={"alias_a": "obj-a"})
            dedup_file = _FakeDedupFile(b"\xff\xff\xff\xff")  # not ZSTD-framed, not a SQLite file either
            result = await resolve_service_db(
                cast(DedupFile, dedup_file), object_db, object_name_index, ("alias_a",), "item_table"
            )
            assert result is None
        finally:
            await object_db.close()

    async def test_right_db_wrong_table_tries_the_next_alias(self) -> None:
        blob = _build_zstd_sqlite_blob("other_table")
        object_db = await ObjectDb.from_bytes(_build_object_db_bytes([("obj-a", 0, len(blob))]))
        try:
            object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={"alias_a": "obj-a"})
            result = await resolve_service_db(
                cast(DedupFile, _FakeDedupFile(blob)), object_db, object_name_index, ("alias_a",), "item_table"
            )
            assert result is None
        finally:
            await object_db.close()

    async def test_a_later_alias_still_resolves_after_an_earlier_one_fails(self) -> None:
        blob = _build_zstd_sqlite_blob_with_rows(
            "CREATE TABLE item_table(id INTEGER)", "INSERT INTO item_table VALUES (?)", [(42,)]
        )
        object_db = await ObjectDb.from_bytes(_build_object_db_bytes([("obj-b", 0, len(blob))]))
        try:
            object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={"alias_b": "obj-b"})
            result = await resolve_service_db(
                cast(DedupFile, _FakeDedupFile(blob)),
                object_db,
                object_name_index,
                ("alias_a", "alias_b"),  # alias_a isn't in object_ids at all -> skipped, not this function's branches
                "item_table",
            )
            assert result is not None
            try:
                # Confirm the returned SqliteSource is actually queryable —
                # not just non-None — against the real row inserted above.
                cursor = await result.connection.execute("SELECT id FROM item_table")
                row = await cursor.fetchone()
                assert row == (42,)
            finally:
                await result.close()
        finally:
            await object_db.close()


class TestReadIndexedTable:
    async def test_catalog_index_none_returns_none(self) -> None:
        async def _reader(connection: object) -> str:
            raise AssertionError("unreachable")  # pragma: no cover

        result = await read_indexed_table(
            cast(DedupFile, _FakeDedupFile(b"")), None, ("alias_a",), "item_table", _reader
        )
        assert result is None

    async def test_object_db_load_failure_returns_none(self, tmp_path: Path) -> None:
        # Real SQLite bytes with no object_table at all -- a bad
        # (offset, length), same shape test_units_saas_objectdb.py's own
        # ObjectDb.load() test exercises directly.
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
            cast(DedupFile, _FakeDedupFile(db_bytes)), object_name_index, ("alias_a",), "item_table", _reader
        )
        assert result is None

    async def test_reader_raising_data_corrupt_degrades_to_none_instead_of_crashing(self) -> None:
        """A schema-drifted table (``reader``'s own ``Table.create``
        finding a required column missing, the real shape a connector
        version mismatch produces) must degrade the same as every other
        "no index recorded here" case above — not propagate and crash
        the caller (or a sibling SaaS provider sharing the same
        version, via ``units/dispatch.py``)."""
        # Layout: [service_blob][object_db_bytes] in one combined buffer,
        # so the object_table row's own (offset, length) columns can name
        # service_blob's position without a circular size dependency
        # between the two halves.
        service_blob = _build_zstd_sqlite_blob_with_rows(
            "CREATE TABLE item_table(id INTEGER)", "INSERT INTO item_table VALUES (?)", [(42,)]
        )
        object_db_bytes = _build_object_db_bytes([("obj-a", 0, len(service_blob))])
        dedup_file = _FakeDedupFile(service_blob + object_db_bytes)
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
        object_db_len = len(_build_object_db_bytes([("def-obj", 0, len(blob))]))
        object_db_bytes = _build_object_db_bytes([("def-obj", object_db_len, len(blob))])
        dedup_file = _FakeDedupFile(object_db_bytes + blob)
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
        """A real definitions table plus a real membership table, resolved
        end to end through the object-name index (no monkeypatching) — exercises
        object_name_index.py's own fallback (``names.get(group_id,
        group_id)``) for a membership row whose ``group_id`` has no matching
        definition."""
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
        object_db_len = len(_build_object_db_bytes(relative_rows))
        absolute_rows = [(oid, off + object_db_len, ln) for oid, off, ln in relative_rows]
        object_db_bytes = _build_object_db_bytes(absolute_rows)
        dedup_file = _FakeDedupFile(object_db_bytes + def_blob + mem_blob)
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

        monkeypatch.setattr(catalog_index_module, "read_id_to_name_map", _fake_read_id_to_name_map)
        monkeypatch.setattr(catalog_index_module, "read_indexed_table", _fake_read_indexed_table)

        object_name_index = ObjectNameIndex(stream_uuid="s", offset=0, length=1, object_ids={})
        result = await read_grouped_names(
            cast(DedupFile, _FakeDedupFile(b"")),
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


__all__: list[str] = []
