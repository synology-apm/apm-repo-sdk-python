"""Regression test for ``dedup.pool`` against real plaintext and
vault-encrypted bucket bytes — replayed from committed fixtures, with
**no external dependency**: this always runs, on CI or anywhere else,
because it goes through ``ReplayStore`` instead of a real
``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded once by ``RecordingStore``
via each test's own ``record_target()`` call — see ``tests/conftest.py``
and ``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...`` workflow that (re-)records these):

- ``pool_plaintext_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``.
- ``pool_encrypted_apv2.json.gz`` — rooted at
  ``apv-sample-2-encrypted/@ActiveProtectVault``. ``ReplayStore`` only
  replays *storage* calls, never the crypto layered on top, so the real
  vault key is embedded below as a literal constant (this sample's own
  generated vault key, not customer data) rather than read from a real
  sample tree at test time.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Awaitable, Callable

import lz4.block

from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.crypto import parse_key_string, unwrap_vault_key
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage import DirCache
from synology_apm_repo.sdk.storage.base import ObjectStore

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="
_WRAPPED_B64 = "99GzNFTm0omh+fXS14OwVpP2TwgcUwaM7HrttM8bAoW5jGPr7uI3rBa2//SxLss2"


async def test_replayed_plaintext_lz4_chunk_matches_known_fingerprint(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: reads a real chunk's plaintext directly via
    # Pool.read_chunk() -- a structural oracle (SHA-256 cross-check against
    # an independent decode path), never the chunk's own content meaning.
    store = await record_target("pool_plaintext_apv1.json.gz", allow_content=True)
    pool = Pool(store, "@data/Pool", DirCache(store))

    addr = ChunkAddress(StreamId(132), BucketId(0), ChunkIdx(0))
    reader = await pool.bucket(StreamId(132), BucketId(0))
    assert reader.entries[0].compress_type is CompressType.LZ4

    plaintext = await pool.read_chunk(addr)
    assert len(plaintext) == 4096
    assert hashlib.sha256(plaintext).hexdigest() == "9cac786004e88257a6c49ce252c3f8956fa8799d64181d311a72d233edb6a54c"

    # cross-check via an entirely independent decode path (lz4 directly,
    # not going through format.compression.decompress at all)
    locator = reader.locators[0]
    ciphertext = await store.read("@data/Pool/132/0.buk.1", locator.offset, locator.length)
    independent_plaintext = lz4.block.decompress(ciphertext, uncompressed_size=4096)
    assert independent_plaintext == plaintext


async def test_replayed_encrypted_zstd_chunk_matches_known_fingerprint(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    user_key_id, user_key = parse_key_string(_APV2_ENCRYPTED_KEY_STRING)
    vault_key = unwrap_vault_key(user_key_id, user_key, base64.b64decode(_WRAPPED_B64))

    # allow_content=True: reads a real chunk's plaintext directly via
    # Pool.read_chunk() -- a structural oracle (SHA-256 fingerprint match),
    # never the chunk's own content meaning.
    store = await record_target("pool_encrypted_apv2.json.gz", allow_content=True)
    pool = Pool(store, "@data/Pool", DirCache(store), vault_key=vault_key)
    addr = ChunkAddress(StreamId(247), BucketId(0), ChunkIdx(0))
    plaintext = await pool.read_chunk(addr)

    assert len(plaintext) == 4096
    assert hashlib.sha256(plaintext).hexdigest() == "bce7cbcaaa49b7ff8852ab0a71c5076913df4338f10e53565765f06c5a395013"


__all__: list[str] = []
