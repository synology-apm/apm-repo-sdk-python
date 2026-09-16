"""Regression test for ``dedup.repository``'s plaintext-vault,
encrypted-vault, and encrypted-object-store paths — replayed from
committed fixtures recorded against real bytes, with **no external
dependency**: this always runs, on CI or anywhere else, because it goes
through ``ReplayStore`` instead of a real ``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded against a real store — see
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...``/``make record-fixture`` workflow that
(re-)records these):

- ``dedup_repository_apv1_plaintext.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``, every call ``detect_layout()``/
  ``DedupRepo.open()``/``locate_file()``/``open_file().read(0, 520)``
  make resolving and reading the first 520 bytes of a real VM disk image.
- ``dedup_repository_apv2_encrypted.json.gz`` — an encrypted
  VAULT repository's ``locate_file()``/``open_file()`` against
  ``apv-sample-2-encrypted``'s real VM disk image.
- ``dedup_repository_s3sample2_generations.json.gz`` — both real
  repo ids of an encrypted OBJECT_STORE repository
  (``s3-sample-2-encrypted``), each opened and queried via
  ``repo.db("file_map")`` to lock in working multi-generation
  ``db/*.N`` selection.

The two encrypted scenarios need real key material to decrypt anything
(``ReplayStore`` only replays *storage* calls, never the crypto layered
on top) — both keys are embedded below as literal constants (each
sample's own generated vault key, not customer data) rather than read
from a real sample tree at test time.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout, iter_layouts

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="
#: s3-sample-2-encrypted's real key.
_S3SAMPLE2_ENCRYPTED_KEY_STRING = "IvcvldpbSRyd@Ys+mbaQyOElj6bHtL0+VFdp1e4swyEeApQLhdHgaTvg="

#: Internal catalog identifiers -- stable and non-identifying (never
#: touched by catalog-metadata anonymization). Each real sample assigns
#: its own ids independently.
_APV1_FEDORA_39_VM_WORKLOAD_ID = 3
_APV2_ENCRYPTED_WINDOWS_VM_WORKLOAD_ID = 3


async def test_replayed_open_file_on_plaintext_vault_repo(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("dedup_repository_apv1_plaintext.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo:
        assert repo.info.repo_type is not None
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm_name = next(w.display_name for w in all_workloads if w.workload_id == _APV1_FEDORA_39_VM_WORKLOAD_ID)
        vm_path = (
            "VM-ebd17568-24b5-4816-9e42-a9deb290ad74/ActiveBackup_2026-08-07_090008/"
            f"{vm_name}/796dfb65-8d9f-41b8-9862-f4e9221773f1.img"
        )
        loc = await repo.locate_file(vm_path)
        assert (loc.stream_id, loc.session_id, loc.comp_offset) == (61, 6, 64)
        assert loc.file_size == 21_474_836_480

        f = await repo.open_file(vm_path)
        header = await f.read(0, 520)
        assert header[510:512] == _MBR_BOOT_SIG
        assert header[512:520] == _GPT_SIG


async def test_replayed_open_file_on_encrypted_vault_repo(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    keys = KeyMaterial.from_key_string(_APV2_ENCRYPTED_KEY_STRING)
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("dedup_repository_apv2_encrypted.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout, keys) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm_name = next(w.display_name for w in all_workloads if w.workload_id == _APV2_ENCRYPTED_WINDOWS_VM_WORKLOAD_ID)
        vm_path = (
            "VM-19017d50-4f2b-4b99-b871-1e5479bba78c/ActiveBackup_2026-08-06_215706/"
            f"{vm_name}/0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img"
        )
        loc = await repo.locate_file(vm_path)
        assert (loc.stream_id, loc.session_id, loc.comp_offset) == (247, 1, 64)
        assert loc.file_size == 32_212_254_720

        f = await repo.open_file(vm_path)
        header = await f.read(0, 520)
        assert header[510:512] == _MBR_BOOT_SIG
        assert header[512:520] == _GPT_SIG


async def test_replayed_object_store_repos_open_and_expose_working_db_generation_selection(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    keys = KeyMaterial.from_key_string(_S3SAMPLE2_ENCRYPTED_KEY_STRING)
    store = await record_target("dedup_repository_s3sample2_generations.json.gz")

    layouts = [layout async for layout in iter_layouts(store)]
    assert len(layouts) == 2
    # The real, deterministic file_map row count each repository's own working
    # generation selects -- a regression that silently selected the wrong
    # ``db/*.N`` generation would very likely still satisfy ``row[0] > 0``
    # but not these exact counts.
    expected_counts = {
        "@ActiveProtectData/BikXpRbFNGI1": 11,
        "@ActiveProtectData/uoRtcQebTU5w": 6,
    }
    assert {layout.repo_root for layout in layouts} == set(expected_counts)
    for layout in layouts:
        async with await DedupRepo.open(store, layout, keys) as repo:
            conn = await repo.db("file_map")
            cursor = await conn.execute("SELECT COUNT(*) FROM file_map")
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == expected_counts[layout.repo_root]


__all__: list[str] = []
