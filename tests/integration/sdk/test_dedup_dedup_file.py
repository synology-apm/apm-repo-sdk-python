"""Regression tests for ``dedup.dedup_file`` against a real plaintext and a
real vault-encrypted VM disk image: a first-520-bytes read plus a full
``_extents()`` walk. Each fixture's one test is its recording recipe:

- ``dedup_file_vault_plain.json.gz`` -- recorded against
  ``vault-plain/@ActiveProtectVault``.
- ``dedup_file_vault_encrypted.json.gz`` -- recorded against
  ``vault-encrypted/@ActiveProtectVault``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from integration.sdk.vault_key_drivers import vault_encrypted_key
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.extent import ExtentKind
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage import DirCache
from synology_apm_repo.sdk.storage.base import ObjectStore

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"


async def test_replayed_plaintext_vm_image_reads_as_valid_mbr_gpt(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: only the MBR/GPT signature bytes are read.
    store = await record_target("dedup_file_vault_plain.json.gz", allow_content=True)
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
    vault_key = vault_encrypted_key()

    # allow_content=True: only the MBR/GPT signature bytes are read.
    store = await record_target("dedup_file_vault_encrypted.json.gz", allow_content=True)
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
