"""Regression tests for ``format.crypto``'s three AES schemes against real
production-encrypted bytes, replayed from committed fixtures — these
tests have **no external dependency** and always run, because they go
through ``ReplayStore`` instead of a real ``LocalFsStore`` (no need for
the actual sample tree).

``test_replayed_chunk_decrypt_matches_real_fingerprint`` replays the full
key-unwrap + chunk-decrypt + decompress + fingerprint chain. Its fixture
(``tests/fixtures/crypto_chunk0_apv2.json.gz``, recorded once by
``RecordingStore`` via each test's own ``record_target()`` call — see
``tests/conftest.py`` and ``tests/CLAUDE.md``'s "Recording a fixture"
section for the ``pytest --record-against=...`` workflow that
(re-)records these) was produced against a real store rooted at
``apv-sample-2-encrypted/@ActiveProtectVault``, recording exactly four
narrow reads — never the whole (multi-MB) bucket file — mirroring what
the Dedup Layer's bucket reader would actually fetch:

- ``@data/Pool/247/0.buk.8`` bytes ``[0, 16384)`` (header + SizeStore)
- ``@data/Pool/247/0.buk.8`` bytes ``[16384, 16384+773)`` (chunk 0's ciphertext)
- ``@data/Pool/247/0.inf`` bytes ``[12288, 12296)`` (the fgp allocation entry)
- ``@data/Pool/247/0_0.fgp`` bytes ``[0, 32)`` (chunk 0's stored fingerprint)

The expected key string, vault key, and fingerprint below are this
chunk's real, independently-derived values.

``test_replayed_ahlt_decrypt_matches_real_target_db`` has its own
dedicated ``tests/fixtures/crypto_apv2_encrypted_target_db.json.gz``
copy of the same real ``apv-sample-2-encrypted`` ``target.db`` bytes
``test_storage_sqlite_source_envelopes.py``'s own fixture backs
(that file's higher-level ``peel()`` coverage) to exercise
``ahlt_decrypt`` directly against a real ``aHlT``-enveloped ``target.db``.

``test_replayed_decrypt_version_spec_matches_real_status`` replays
``tests/fixtures/crypto_version_spec_apv2.json.gz`` (apv-sample-2-
encrypted's whole ``db/copy_target_version``, opened the same way
``DedupRepo.db()`` would) to exercise ``decrypt_version_spec``
against one real row's ciphertext.
"""

from __future__ import annotations

import base64
import hashlib
import json
import struct
from collections.abc import Awaitable, Callable

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

_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="
_WRAPPED_B64 = "99GzNFTm0omh+fXS14OwVpP2TwgcUwaM7HrttM8bAoW5jGPr7uI3rBa2//SxLss2"
_EXPECTED_VAULT_KEY_HEX = "5241f772816e8e9b7cbb86a3314ce32880a70b22e66a037dee873287facc0ed1"
_EXPECTED_FINGERPRINT_HEX = "bce7cbcaaa49b7ff8852ab0a71c5076913df4338f10e53565765f06c5a395013"


def _apv2_vault_key() -> bytes:
    user_key_id, user_key = parse_key_string(_KEY_STRING)
    return unwrap_vault_key(user_key_id, user_key, base64.b64decode(_WRAPPED_B64))


async def test_replayed_chunk_decrypt_matches_real_fingerprint(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("crypto_chunk0_apv2.json.gz")

    user_key_id, user_key = parse_key_string(_KEY_STRING)
    vault_key = unwrap_vault_key(user_key_id, user_key, base64.b64decode(_WRAPPED_B64))
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


_APV2_TARGET_DB_PATH = "@ActiveProtectVault/copy_meta_file/VM_05812ddb-2b5f-4ace-87e9-61f4fa0afdb8/target.db"
_EXPECTED_TARGET_DB_PLAINTEXT_SHA256 = "e709709724d7efbd5af0ffd726b951649f938dcccc88522d86a6dec0d932853c"


async def test_replayed_ahlt_decrypt_matches_real_target_db(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("crypto_apv2_encrypted_target_db.json.gz")
    vault_key = _apv2_vault_key()

    raw = await store.read(_APV2_TARGET_DB_PATH)
    plaintext = ahlt_decrypt(raw, vault_key)

    assert plaintext.startswith(b"SQLite format 3\x00")
    assert hashlib.sha256(plaintext).hexdigest() == _EXPECTED_TARGET_DB_PLAINTEXT_SHA256


_VERSION_SPEC_UID = "c6a1e6e3-13b1-4d19-8dd5-7929c3f5ff7b"


async def test_replayed_decrypt_version_spec_matches_real_status(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("crypto_version_spec_apv2.json.gz")
    vault_key = _apv2_vault_key()

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
