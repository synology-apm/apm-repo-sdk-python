"""Regression test for ``dedup.fingerprint`` against real
``.inf``/``.fgp`` files on plaintext and encrypted repositories — replayed from
committed fixtures, with **no external dependency**: this always runs,
on CI or anywhere else, because it goes through ``ReplayStore`` instead
of a real ``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded against a real store — see
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...``/``make record-fixture`` workflow that
(re-)records these):

- ``fingerprint_plaintext_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``, one bucket's chunk 0
  (``test_replayed_plaintext_apv_chunk_zero``) plus another bucket's
  three chunks straddling a real ``.fgp`` 4 MiB segment boundary
  (``test_replayed_plaintext_apv_chunk_crossing_fgp_segment_boundary``)
  — neither test's calls subset the other's, so recording needs both
  run together against one real backend.
- ``fingerprint_encrypted_apv2.json.gz`` — rooted at
  ``apv-sample-2-encrypted/@ActiveProtectVault``, an encrypted vault
  repository's chunk 0.
- ``fingerprint_encrypted_s3sample2.json.gz`` — rooted directly
  at one specific real repo id under
  ``s3-sample-2-encrypted/@ActiveProtectData/`` (the shape a real
  object-store ``--profile``/root always resolves to) — an encrypted
  object-store repository's chunk 0.

The two encrypted fixtures need a real vault key to decode a chunk at all
(``ReplayStore`` only replays *storage* calls, never the crypto layered
on top) — both keys are embedded below as literal constants (each
sample's own generated vault key, not customer data) rather than read
from a real sample tree at test time.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.dedup.fingerprint import fingerprint
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.crypto import parse_key_string, unwrap_vault_key
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage import DirCache
from synology_apm_repo.sdk.storage.base import ObjectStore

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="
_APV2_WRAPPED_B64 = "99GzNFTm0omh+fXS14OwVpP2TwgcUwaM7HrttM8bAoW5jGPr7uI3rBa2//SxLss2"

#: s3-sample-2-encrypted's real key and its own wrapped VaultKey record
#: (``@ActiveProtectKey/userKey/<user_key_id>``, read once from the real
#: sample tree — the same not-customer-data key material every other
#: encrypted-sample literal in this project embeds).
_S3SAMPLE2_ENCRYPTED_KEY_STRING = "IvcvldpbSRyd@Ys+mbaQyOElj6bHtL0+VFdp1e4swyEeApQLhdHgaTvg="
_S3SAMPLE2_WRAPPED_B64 = "fESeS8zV25718Uz8AxuEJd5fgP8L00Pi77Em6mWGU6EgME8isn1ltaLsGhc5yC74"


async def test_replayed_plaintext_apv_chunk_zero(record_target: Callable[..., Awaitable[ObjectStore]]) -> None:
    # allow_content=True: reads a real chunk's plaintext via
    # Pool.read_chunk() -- a structural oracle (SHA-256 fingerprint
    # match), never the chunk's own content meaning.
    store = await record_target("fingerprint_plaintext_apv1.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    pool = Pool(store, "@data/Pool", dir_cache)
    plaintext = await pool.read_chunk(ChunkAddress(StreamId(132), BucketId(0), ChunkIdx(0)))
    expected = await fingerprint(store, dir_cache, "@data/Pool", StreamId(132), BucketId(0), ChunkIdx(0))
    assert hashlib.sha256(plaintext).digest() == expected


async def test_replayed_plaintext_apv_chunk_crossing_fgp_segment_boundary(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: same structural-oracle pattern as above.
    store = await record_target("fingerprint_plaintext_apv1.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    pool = Pool(store, "@data/Pool", dir_cache)
    for chunk_idx in (1023, 1024, 1025):
        plaintext = await pool.read_chunk(ChunkAddress(StreamId(132), BucketId(16), ChunkIdx(chunk_idx)))
        expected = await fingerprint(store, dir_cache, "@data/Pool", StreamId(132), BucketId(16), ChunkIdx(chunk_idx))
        assert hashlib.sha256(plaintext).digest() == expected


async def test_replayed_encrypted_apv_chunk_zero(record_target: Callable[..., Awaitable[ObjectStore]]) -> None:
    user_key_id, user_key = parse_key_string(_APV2_ENCRYPTED_KEY_STRING)
    vault_key = unwrap_vault_key(user_key_id, user_key, base64.b64decode(_APV2_WRAPPED_B64))

    # allow_content=True: same structural-oracle pattern as above.
    store = await record_target("fingerprint_encrypted_apv2.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    pool = Pool(store, "@data/Pool", dir_cache, vault_key=vault_key)
    plaintext = await pool.read_chunk(ChunkAddress(StreamId(247), BucketId(0), ChunkIdx(0)))
    expected = await fingerprint(store, dir_cache, "@data/Pool", StreamId(247), BucketId(0), ChunkIdx(0))
    assert hashlib.sha256(plaintext).digest() == expected


async def test_replayed_encrypted_object_store_chunk_zero(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    user_key_id, user_key = parse_key_string(_S3SAMPLE2_ENCRYPTED_KEY_STRING)
    vault_key = unwrap_vault_key(user_key_id, user_key, base64.b64decode(_S3SAMPLE2_WRAPPED_B64))

    # allow_content=True: same structural-oracle pattern as above.
    store = await record_target("fingerprint_encrypted_s3sample2.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    pool = Pool(store, "@data/Pool", dir_cache, vault_key=vault_key)
    plaintext = await pool.read_chunk(ChunkAddress(StreamId(246), BucketId(0), ChunkIdx(0)))
    expected = await fingerprint(store, dir_cache, "@data/Pool", StreamId(246), BucketId(0), ChunkIdx(0))
    assert hashlib.sha256(plaintext).digest() == expected


__all__: list[str] = []
