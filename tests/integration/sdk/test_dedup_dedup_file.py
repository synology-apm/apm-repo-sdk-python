"""Regression test for ``dedup.dedup_file`` against real plaintext and
vault-encrypted VM disk images — replayed from committed fixtures, with
**no external dependency**: this always runs, on CI or anywhere else,
because it goes through ``ReplayStore`` instead of a real
``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded once by ``RecordingStore``
via each test's own ``record_target()`` call — see ``tests/conftest.py``
and ``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...`` workflow that (re-)records these):

- ``dedup_file_plaintext_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``, a real first-520-bytes read plus
  a full ``_extents()`` walk of the Fedora VM workload's (workload_id 3)
  20 GiB disk (stream 61, session 6, comp_offset 64).
- ``dedup_file_encrypted_apv2.json.gz`` — rooted at
  ``apv-sample-2-encrypted/@ActiveProtectVault``, the same shape of read
  against a real, vault-encrypted 30 GiB disk (stream 247, session 1).
  ``ReplayStore`` only replays *storage* calls, never the crypto layered
  on top, so the real vault key is embedded below as a literal constant
  (this sample's own generated vault key, not customer data) rather than
  read from a real sample tree at test time.
"""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile, ExtentKind
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.format.crypto import parse_key_string, unwrap_vault_key
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage import DirCache
from synology_apm_repo.sdk.storage.base import ObjectStore

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"

#: apv-sample-2-encrypted's real wrapped VaultKey record, and the real
#: key string that unwraps it — see ``tests/CLAUDE.md``'s "Recording a
#: fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="
_WRAPPED_B64 = "99GzNFTm0omh+fXS14OwVpP2TwgcUwaM7HrttM8bAoW5jGPr7uI3rBa2//SxLss2"


async def test_replayed_plaintext_vm_image_reads_as_valid_mbr_gpt(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("dedup_file_plaintext_apv1.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "@data/Composition", StreamId(61), SessionId(6))
    pool = Pool(store, "@data/Pool", dir_cache)

    size = 21_474_836_480
    dedup_file = DedupFile(comp_reader, pool, 64, size=size)

    header = await dedup_file.read(0, 520)
    assert header[510:512] == _MBR_BOOT_SIG
    assert header[512:520] == _GPT_SIG

    data_zero_chunks = sum(
        [e.length // 4096 async for e in dedup_file._extents() if e.kind in (ExtentKind.DATA, ExtentKind.ZERO)]
    )
    assert data_zero_chunks == 924_892  # == file_map.block for this exact row


async def test_replayed_encrypted_vm_image_reads_as_valid_mbr_gpt(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    user_key_id, user_key = parse_key_string(_APV2_ENCRYPTED_KEY_STRING)
    vault_key = unwrap_vault_key(user_key_id, user_key, base64.b64decode(_WRAPPED_B64))

    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("dedup_file_encrypted_apv2.json.gz", allow_content=True)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "@data/Composition", StreamId(247), SessionId(1))
    pool = Pool(store, "@data/Pool", dir_cache, vault_key=vault_key)

    size = 32_212_254_720
    dedup_file = DedupFile(comp_reader, pool, 64, size=size)

    header = await dedup_file.read(0, 520)
    assert header[510:512] == _MBR_BOOT_SIG
    assert header[512:520] == _GPT_SIG

    data_zero_chunks = sum(
        [e.length // 4096 async for e in dedup_file._extents() if e.kind in (ExtentKind.DATA, ExtentKind.ZERO)]
    )
    assert data_zero_chunks == 2_582_175  # == file_map.block for this exact row


__all__: list[str] = []
