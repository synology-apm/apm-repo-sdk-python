"""Regression tests for ``format.crypto``'s three AES schemes against real
production-encrypted bytes. Each fixture's one test is its recording recipe:

- ``crypto_chunk0_vault_encrypted.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``: the key-unwrap +
  chunk-decrypt + decompress + fingerprint chain for one chunk, from narrow
  reads of its bucket, ``.inf`` and ``.fgp`` (never the whole bucket).
- ``crypto_vault_encrypted_target_db.json.gz`` — recorded against
  ``vault-encrypted``: ``ahlt_decrypt`` on a real ``aHlT``-enveloped
  ``target.db``.
- ``crypto_version_spec_vault_encrypted.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``: ``decrypt_version_spec``
  on one real ``db/copy_target_version`` row.
"""

from __future__ import annotations

import base64
import hashlib
import json
import struct
from collections.abc import Awaitable, Callable

from integration.sdk.vault_key_drivers import resolve_vault_encrypted_key, vault_encrypted_key
from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING, VAULT_ENCRYPTED_WRAPPED_B64
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import parse_bucket_header, parse_size_store
from synology_apm_repo.sdk.format.compression import CompressType, decompress
from synology_apm_repo.sdk.format.crypto import (
    ahlt_decrypt,
    decrypt_chunk,
    decrypt_version_spec,
    parse_key_string,
    unwrap_vault_key,
)
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource
from synology_apm_repo.sdk.storage.table import Column, Table

_EXPECTED_VAULT_KEY_HEX = "5241f772816e8e9b7cbb86a3314ce32880a70b22e66a037dee873287facc0ed1"
_EXPECTED_FINGERPRINT_HEX = "bce7cbcaaa49b7ff8852ab0a71c5076913df4338f10e53565765f06c5a395013"


async def test_replayed_chunk_decrypt_matches_real_fingerprint(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("crypto_chunk0_vault_encrypted.json.gz")

    user_key_id, user_key = parse_key_string(VAULT_ENCRYPTED_KEY_STRING)
    vault_key = unwrap_vault_key(user_key_id, user_key, base64.b64decode(VAULT_ENCRYPTED_WRAPPED_B64))
    assert vault_key.hex() == _EXPECTED_VAULT_KEY_HEX

    head = await store.read("@data/Pool/247/0.buk.8", 0, 16384)
    header = parse_bucket_header(head)
    entries = parse_size_store(head[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)
    assert entries[0].compress_type is CompressType.ZSTD
    assert entries[0].stored_len == 773

    ciphertext = await store.read("@data/Pool/247/0.buk.8", 16384, 773)
    addr = ChunkAddress(StreamId(247), BucketId(0), ChunkIdx(0))
    plain_compressed = decrypt_chunk(vault_key, addr, ciphertext)
    plaintext = decompress(CompressType.ZSTD, plain_compressed)
    computed_fingerprint = hashlib.sha256(plaintext).hexdigest()
    assert computed_fingerprint == _EXPECTED_FINGERPRINT_HEX

    fgp_table_entry = await store.read("@data/Pool/247/0.inf", 12288, 8)
    raw_pos = struct.unpack(">I", fgp_table_entry[0:4])[0]
    fgp_offset = (raw_pos >> 15) * 4096
    assert fgp_offset == 0  # chunk 0's fingerprints start at the segment's own offset 0

    stored_fingerprint = await store.read("@data/Pool/247/0_0.fgp", 0, 32)
    assert stored_fingerprint.hex() == computed_fingerprint


_VAULT_ENCRYPTED_TARGET_DB_PATH = "@ActiveProtectVault/copy_meta_file/VM_05812ddb-2b5f-4ace-87e9-61f4fa0afdb8/target.db"


async def test_replayed_ahlt_decrypt_matches_real_target_db(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("crypto_vault_encrypted_target_db.json.gz")
    vault_key = await resolve_vault_encrypted_key(store)

    raw = await store.read(_VAULT_ENCRYPTED_TARGET_DB_PATH)
    plaintext = ahlt_decrypt(raw, vault_key)

    # Structure only: anonymization re-encrypts a scrubbed plaintext, so no
    # byte-level digest survives re-recording.
    async with await SqliteSource.from_bytes(plaintext) as src:
        cursor = await src.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        assert "object_table" in {row[0] for row in await cursor.fetchall()}


_VERSION_SPEC_UID = "c6a1e6e3-13b1-4d19-8dd5-7929c3f5ff7b"


async def test_replayed_decrypt_version_spec_matches_real_status(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("crypto_version_spec_vault_encrypted.json.gz")
    vault_key = vault_encrypted_key()

    async with await SqliteSource.from_raw_store(store, "db/copy_target_version") as src:
        table = await Table.create(
            src.connection, "copy_target_version", [Column("version_uid"), Column("version_spec")]
        )
        rows = [row async for row in table.select("version_uid = ?", (_VERSION_SPEC_UID,))]
    assert len(rows) == 1
    row = rows[0]

    plaintext = decrypt_version_spec(str(row["version_spec"]), str(row["version_uid"]), vault_key)
    status = json.loads(plaintext)["status"]

    assert status["status"] == "COMPLETED"
    assert status["start_time"] == "1786024150"
    assert status["end_time"] == "1786024183"
