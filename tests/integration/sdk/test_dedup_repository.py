"""Regression tests for ``dedup.repository``'s plaintext-vault,
encrypted-vault and encrypted-object-store paths. Each fixture's one test
is its recording recipe:

- ``dedup_repository_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``: ``locate_file()`` and
  ``open_file().read(0, 520)`` on a real VM disk image.
- ``dedup_repository_vault_encrypted.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``: the same on its VM disk
  image.
- ``dedup_repository_objstore_encrypted_generations.json.gz`` — recorded against
  ``objstore-encrypted``: both repo ids, each queried via
  ``repo.db("file_map")``, locking in multi-generation ``db/*.N``
  selection.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from support.recording.sample_constants import OBJSTORE_ENCRYPTED_KEY_STRING, VAULT_ENCRYPTED_KEY_STRING
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout, iter_repository_layouts

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"


#: Each sample assigns its own workload ids.
_VAULT_PLAIN_FEDORA_39_VM_WORKLOAD_ID = 3
_VAULT_ENCRYPTED_WINDOWS_VM_WORKLOAD_ID = 3


async def test_replayed_open_file_on_plaintext_vault_repo(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("dedup_repository_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo:
        assert repo.info.repo_type is not None
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm_name = next(w.display_name for w in all_workloads if w.workload_id == _VAULT_PLAIN_FEDORA_39_VM_WORKLOAD_ID)
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
    keys = KeyMaterial.from_key_string(VAULT_ENCRYPTED_KEY_STRING)
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("dedup_repository_vault_encrypted.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout, keys) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm_name = next(
            w.display_name for w in all_workloads if w.workload_id == _VAULT_ENCRYPTED_WINDOWS_VM_WORKLOAD_ID
        )
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
    keys = KeyMaterial.from_key_string(OBJSTORE_ENCRYPTED_KEY_STRING)
    store = await record_target("dedup_repository_objstore_encrypted_generations.json.gz")

    layouts = [layout async for repo in iter_repository_layouts(store) for layout in catalog_repo_layouts(repo)]
    assert len(layouts) == 2
    # Exact row counts: a wrong ``db/*.N`` generation would still be non-empty.
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
