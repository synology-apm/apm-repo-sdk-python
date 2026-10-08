"""Regression tests for ``dedup.fingerprint`` against real ``.inf``/``.fgp``
files on plaintext and encrypted repositories:

- ``fingerprint_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``: one bucket's chunk 0 plus another
  bucket's three chunks straddling a ``.fgp`` 4 MiB segment boundary.
  Neither test's calls are a subset of the other's, so recording needs
  both run together.
- ``fingerprint_vault_encrypted.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``; its one test.
- ``fingerprint_objstore_encrypted.json.gz`` — recorded against one repo
  id's directory under ``objstore-encrypted/@ActiveProtectData/``; its
  one test.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Awaitable, Callable

from integration.sdk.vault_key_drivers import vault_encrypted_key
from support.recording.sample_constants import (
    OBJSTORE_ENCRYPTED_KEY_STRING,
    OBJSTORE_ENCRYPTED_WRAPPED_B64,
)
from synology_apm_repo.sdk.dedup.fingerprint import FingerprintIndex
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.crypto import parse_key_string, unwrap_vault_key
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage import DirCache
from synology_apm_repo.sdk.storage.base import ObjectStore


async def test_replayed_plaintext_apv_chunk_zero(record_target: Callable[..., Awaitable[ObjectStore]]) -> None:
    # allow_content=True: the chunk's plaintext is a structural oracle
    # (SHA-256 fingerprint match), never its meaning.
    store = await record_target("fingerprint_vault_plain.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    pool = Pool(store, "@data/Pool", dir_cache)
    plaintext = await pool.read_chunk(ChunkAddress(StreamId(132), BucketId(0), ChunkIdx(0)))
    expected = (await FingerprintIndex(store, dir_cache, "@data/Pool").digests(StreamId(132), BucketId(0), [0]))[0]
    assert hashlib.sha256(plaintext).digest() == expected


async def test_replayed_plaintext_apv_chunk_crossing_fgp_segment_boundary(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: same structural-oracle pattern as above.
    store = await record_target("fingerprint_vault_plain.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    pool = Pool(store, "@data/Pool", dir_cache)
    for chunk_idx in (1023, 1024, 1025):
        plaintext = await pool.read_chunk(ChunkAddress(StreamId(132), BucketId(16), ChunkIdx(chunk_idx)))
        expected = (
            await FingerprintIndex(store, dir_cache, "@data/Pool").digests(StreamId(132), BucketId(16), [chunk_idx])
        )[chunk_idx]
        assert hashlib.sha256(plaintext).digest() == expected


async def test_replayed_encrypted_apv_chunk_zero(record_target: Callable[..., Awaitable[ObjectStore]]) -> None:
    vault_key = vault_encrypted_key()

    # allow_content=True: same structural-oracle pattern as above.
    store = await record_target("fingerprint_vault_encrypted.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    pool = Pool(store, "@data/Pool", dir_cache, vault_key=vault_key)
    plaintext = await pool.read_chunk(ChunkAddress(StreamId(247), BucketId(0), ChunkIdx(0)))
    expected = (await FingerprintIndex(store, dir_cache, "@data/Pool").digests(StreamId(247), BucketId(0), [0]))[0]
    assert hashlib.sha256(plaintext).digest() == expected


async def test_replayed_encrypted_object_store_chunk_zero(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    user_key_id, user_key = parse_key_string(OBJSTORE_ENCRYPTED_KEY_STRING)
    vault_key = unwrap_vault_key(user_key_id, user_key, base64.b64decode(OBJSTORE_ENCRYPTED_WRAPPED_B64))

    # allow_content=True: same structural-oracle pattern as above.
    store = await record_target("fingerprint_objstore_encrypted.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    pool = Pool(store, "@data/Pool", dir_cache, vault_key=vault_key)
    plaintext = await pool.read_chunk(ChunkAddress(StreamId(246), BucketId(0), ChunkIdx(0)))
    expected = (await FingerprintIndex(store, dir_cache, "@data/Pool").digests(StreamId(246), BucketId(0), [0]))[0]
    assert hashlib.sha256(plaintext).digest() == expected
