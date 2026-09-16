"""Regression test for ``synology_apm_repo.sdk.storage.sqlite_source``
and ``synology_apm_repo.sdk.storage.table`` — replayed from committed
fixtures recorded against real bytes, with **no external dependency**:
this always runs, on CI or anywhere else, because it goes through
``ReplayStore`` instead of a real ``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded once by ``RecordingStore``):

- ``storage_sqlite_source_envelopes_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``, apv-sample-1's real, unencrypted
  envelope combinations: one full-file read each of raw ``db/repo_state``
  and one FS workload's zstd-only ``version.db.zst`` — neither of the two
  tests below sharing this fixture subsets the other, so recording needs
  both run together.
- ``storage_sqlite_source_envelopes_apv2_encrypted.json.gz`` —
  rooted at ``apv-sample-2-encrypted/@ActiveProtectVault``, the two
  AHLT-enveloped combinations: one full-file read each of an
  AHLT-then-zstd ``version.db.zst`` and an AHLT-only ``target.db`` —
  same "neither subsets the other" recording requirement as the apv1
  fixture above, plus one recording-only ``detect_layout()`` +
  ``resolve_vault_key()`` probe neither test below exercises directly —
  see ``tests/CLAUDE.md``'s "Recording a fixture" section for why an
  AHLT-enveloped fixture needs that probe. The real vault key is
  embedded below as a literal constant (this sample's own generated key,
  not customer data) rather than read from a real sample tree at test
  time.
"""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.format.crypto import parse_key_string, unwrap_vault_key
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.sqlite_source import Envelope, SqliteSource, peel
from synology_apm_repo.sdk.storage.table import Column, Table

_VERSION_DB_PATH = (
    "copy_meta_file/FS_7c43a536-398b-423f-b31f-ace4e358f397/"
    "ActiveBackup_2026-08-06_215715_0f57dd36-00c4-4ab9-8240-30454d48a84a/version.db.zst"
)
_APV2_VERSION_DB_PATH = (
    "copy_meta_file/FS_bdf14b81-1722-430a-a5bb-16853433189c/"
    "ActiveBackup_2026-08-07_090008_b7890dd7-7f2e-42df-baf9-7bc3d6143cfa/version.db.zst"
)
_APV2_TARGET_DB_PATH = "copy_meta_file/VM_05812ddb-2b5f-4ace-87e9-61f4fa0afdb8/target.db"

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="
_APV2_WRAPPED_B64 = "99GzNFTm0omh+fXS14OwVpP2TwgcUwaM7HrttM8bAoW5jGPr7uI3rBa2//SxLss2"


def _apv2_vault_key() -> bytes:
    user_key_id, user_key = parse_key_string(_APV2_ENCRYPTED_KEY_STRING)
    return unwrap_vault_key(user_key_id, user_key, base64.b64decode(_APV2_WRAPPED_B64))


async def test_replayed_raw_repo_db(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("storage_sqlite_source_envelopes_apv1.json.gz")
    raw = await store.read("db/repo_state")
    payload, envelopes = peel(raw)
    assert envelopes == []
    async with await SqliteSource.from_bytes(payload) as src:
        cursor = await src.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in await cursor.fetchall()}
        assert "repo_state" in tables


async def test_replayed_zstd_only_version_db(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("storage_sqlite_source_envelopes_apv1.json.gz")
    raw = await store.read(_VERSION_DB_PATH)
    payload, envelopes = peel(raw)
    assert envelopes == [Envelope.ZSTD]
    async with await SqliteSource.from_bytes(payload) as src:
        table = await Table.create(
            src.connection, "entry_table", [Column("basename"), Column("dirname"), Column("file_size")]
        )
        rows = [row async for row in table.select()]
        assert len(rows) == 21

        # relink_config.sql.gz is this real version.db's one such config dump.
        matching = [row async for row in table.select("basename = ?", ["relink_config.sql.gz"])]
        assert len(matching) == 1
        assert matching[0]["file_size"] == 4972


async def test_replayed_ahlt_then_zstd_version_db(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    vault_key = _apv2_vault_key()
    store = await record_target("storage_sqlite_source_envelopes_apv2_encrypted.json.gz")
    raw = await store.read(_APV2_VERSION_DB_PATH)
    payload, envelopes = peel(raw, vault_key=vault_key)
    assert envelopes == [Envelope.AHLT, Envelope.ZSTD]
    async with await SqliteSource.from_bytes(payload) as src:
        table = await Table.create(src.connection, "entry_table", [Column("basename"), Column("dirname")])
        rows = [row async for row in table.select()]
        assert len(rows) == 21

        # Same real relink_config.sql.gz entry as the plaintext apv-sample-1
        # version.db above (test_replayed_zstd_only_version_db), decrypted
        # through the AHLT layer here instead.
        matching = [row async for row in table.select("basename = ?", ["relink_config.sql.gz"])]
        assert len(matching) == 1
        assert matching[0]["dirname"] == "/test/ActiveBackup_2026-05-13_123726/test"


async def test_replayed_ahlt_only_target_db(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    vault_key = _apv2_vault_key()
    store = await record_target("storage_sqlite_source_envelopes_apv2_encrypted.json.gz")
    raw = await store.read(_APV2_TARGET_DB_PATH)
    payload, envelopes = peel(raw, vault_key=vault_key)
    assert envelopes == [Envelope.AHLT]
    async with await SqliteSource.from_bytes(payload) as src:
        # schema tolerance exercised for real: object_table's optional
        # columns vary across connector versions (traps #19/#20 in this
        # project's own traps list), so declare one that may not exist.
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


__all__: list[str] = []
