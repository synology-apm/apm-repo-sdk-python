"""Regression tests for ``dedup.pool`` against real plaintext and
vault-encrypted bucket bytes. Each fixture's one test is its recording
recipe:

- ``pool_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``.
- ``pool_vault_encrypted.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable

import lz4.block

from integration.sdk.vault_key_drivers import vault_encrypted_key
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage import DirCache
from synology_apm_repo.sdk.storage.base import ObjectStore


async def test_replayed_plaintext_lz4_chunk_matches_known_fingerprint(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: the chunk's plaintext is a structural oracle
    # (SHA-256, and an independent decode path), never its meaning.
    store = await record_target("pool_vault_plain.json.gz", allow_content=True)
    pool = Pool(store, "@data/Pool", DirCache(store))

    addr = ChunkAddress(StreamId(132), BucketId(0), ChunkIdx(0))
    reader = await pool.bucket(StreamId(132), BucketId(0))
    assert reader.index[0].compress_type is CompressType.LZ4

    plaintext = await pool.read_chunk(addr)
    assert len(plaintext) == 4096
    assert hashlib.sha256(plaintext).hexdigest() == "9cac786004e88257a6c49ce252c3f8956fa8799d64181d311a72d233edb6a54c"

    # lz4 directly, bypassing format.compression.decompress.
    locator = reader.index.locator(0)
    compressed = await store.read("@data/Pool/132/0.buk.1", locator.offset, locator.length)
    independent_plaintext = lz4.block.decompress(compressed, uncompressed_size=4096)
    assert independent_plaintext == plaintext


async def test_replayed_encrypted_zstd_chunk_matches_known_fingerprint(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    vault_key = vault_encrypted_key()

    # allow_content=True: the chunk's plaintext is a structural oracle
    # (SHA-256), never its meaning.
    store = await record_target("pool_vault_encrypted.json.gz", allow_content=True)
    pool = Pool(store, "@data/Pool", DirCache(store), vault_key=vault_key)
    addr = ChunkAddress(StreamId(247), BucketId(0), ChunkIdx(0))
    plaintext = await pool.read_chunk(addr)

    assert len(plaintext) == 4096
    assert hashlib.sha256(plaintext).hexdigest() == "bce7cbcaaa49b7ff8852ab0a71c5076913df4338f10e53565765f06c5a395013"
