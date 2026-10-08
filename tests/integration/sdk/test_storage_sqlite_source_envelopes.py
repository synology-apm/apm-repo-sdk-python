"""Regression tests for ``synology_apm_repo.sdk.storage.sqlite_source``
and ``synology_apm_repo.sdk.storage.table``. Each fixture is shared by two
tests that read different files, so recording needs both run together.

- ``storage_sqlite_source_envelopes_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``: the raw ``db/repo_state`` and one
  FS workload's zstd-only ``version.db.zst``.
- ``storage_sqlite_source_envelopes_vault_encrypted.json.gz`` — recorded
  against ``vault-encrypted/@ActiveProtectVault``: an AHLT-then-zstd
  ``version.db.zst`` and an AHLT-only ``target.db``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from integration.sdk.vault_key_drivers import resolve_vault_encrypted_key, vault_encrypted_key
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.sqlite_source import Envelope, SqliteSource, peel
from synology_apm_repo.sdk.storage.table import Column, Table

_VERSION_DB_PATH = (
    "copy_meta_file/FS_7c43a536-398b-423f-b31f-ace4e358f397/"
    "ActiveBackup_2026-08-06_215715_0f57dd36-00c4-4ab9-8240-30454d48a84a/version.db.zst"
)
_VAULT_ENCRYPTED_VERSION_DB_PATH = (
    "copy_meta_file/FS_bdf14b81-1722-430a-a5bb-16853433189c/"
    "ActiveBackup_2026-08-07_090008_b7890dd7-7f2e-42df-baf9-7bc3d6143cfa/version.db.zst"
)
_VAULT_ENCRYPTED_TARGET_DB_PATH = "copy_meta_file/VM_05812ddb-2b5f-4ace-87e9-61f4fa0afdb8/target.db"


async def test_replayed_raw_repo_db(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("storage_sqlite_source_envelopes_vault_plain.json.gz")
    raw = await store.read("db/repo_state")
    payload, envelopes = peel(raw, max_zstd_output_size=None)
    assert envelopes == []
    async with await SqliteSource.from_bytes(payload) as src:
        cursor = await src.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in await cursor.fetchall()}
        assert "repo_state" in tables


async def test_replayed_zstd_only_version_db(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("storage_sqlite_source_envelopes_vault_plain.json.gz")
    raw = await store.read(_VERSION_DB_PATH)
    payload, envelopes = peel(raw, max_zstd_output_size=None)
    assert envelopes == [Envelope.ZSTD]
    async with await SqliteSource.from_bytes(payload) as src:
        table = await Table.create(
            src.connection, "entry_table", [Column("basename"), Column("dirname"), Column("file_size")]
        )
        rows = [row async for row in table.select()]
        assert len(rows) == 21

        matching = [row async for row in table.select("basename = ?", ["relink_config.sql.gz"])]
        assert len(matching) == 1
        assert matching[0]["file_size"] == 4972


async def test_replayed_ahlt_then_zstd_version_db(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    vault_key = vault_encrypted_key()
    store = await record_target("storage_sqlite_source_envelopes_vault_encrypted.json.gz")
    raw = await store.read(_VAULT_ENCRYPTED_VERSION_DB_PATH)
    payload, envelopes = peel(raw, vault_key=vault_key, max_zstd_output_size=None)
    assert envelopes == [Envelope.AHLT, Envelope.ZSTD]
    async with await SqliteSource.from_bytes(payload) as src:
        table = await Table.create(src.connection, "entry_table", [Column("basename"), Column("dirname")])
        rows = [row async for row in table.select()]
        assert len(rows) == 21

        matching = [row async for row in table.select("basename = ?", ["relink_config.sql.gz"])]
        assert len(matching) == 1
        assert matching[0]["dirname"] == "/test/ActiveBackup_2026-05-13_123726/test"


async def test_replayed_ahlt_only_target_db(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("storage_sqlite_source_envelopes_vault_encrypted.json.gz")
    vault_key = await resolve_vault_encrypted_key(store)
    raw = await store.read(_VAULT_ENCRYPTED_TARGET_DB_PATH)
    payload, envelopes = peel(raw, vault_key=vault_key, max_zstd_output_size=None)
    assert envelopes == [Envelope.AHLT]
    async with await SqliteSource.from_bytes(payload) as src:
        # object_table's optional columns vary across connector versions,
        # so declare one that may not exist.
        table = await Table.create(
            src.connection,
            "object_table",
            [
                Column("object_id"),
                Column("src_file_path"),
                Column("dedup_object"),
                Column("not_a_real_column", required=False),
            ],
        )
        rows = [row async for row in table.select("dedup_object = 1")]
        assert len(rows) == 1
        assert rows[0]["not_a_real_column"] is None
