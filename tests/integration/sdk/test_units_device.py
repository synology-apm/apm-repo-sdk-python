"""Regression tests for the Device (VM) resolution chain — catalog →
``DeviceProvider`` → a real disk image's MBR/GPT bytes and a real non-dedup
``.delta`` sidecar. Each fixture's one test is its recording recipe:

- ``device_vm_mbr_gpt_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``: the plaintext Windows VM's
  (``_WINDOWS_VM_WORKLOAD_ID``) disk image and ``.delta`` sidecar.
- ``device_vm_vault_encrypted.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``: that sample's Windows VM
  disk image through an ``aHlT``-encrypted ``target.db``.
- ``device_vm_repo_root_vault_plain.json.gz`` — recorded against ``vault-plain``
  itself (a non-empty ``repo_root`` from ``iter_repository_layouts``): the
  newest of the same VM's three versions.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING, VAULT_PLAIN_VM_VERSION_UID
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout, iter_repository_layouts
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.device import DeviceProvider

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"


#: Each sample assigns its own workload ids; vault-encrypted has a
#: second VM workload, so ``workload_type == "VM"`` alone is ambiguous.
_WINDOWS_VM_WORKLOAD_ID = 2
_VAULT_ENCRYPTED_WINDOWS_VM_WORKLOAD_ID = 3


async def test_replayed_plaintext_vm_disk_image_and_delta_sidecar(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: only the MBR/GPT signatures and the delta
    # sidecar's magic are read.
    store = await record_target("device_vm_mbr_gpt_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
        # Of three versions, the (not newest) one this fixture recorded.
        version = next(v for v in await versions(repo, vm) if v.version_uid == "887c024b-30c4-4c62-8df8-1fa0f8cebd1c")

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            assert len(devices) == 1

            objects = await provider.children(devices[0])
            disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
            delta = next(o for o in objects if o.kind is UnitKind.FILE)

            disk_content = (await provider.unit(disk)).content
            header = await disk_content.read(0, 520)
            assert header[510:512] == _MBR_BOOT_SIG
            assert header[512:520] == _GPT_SIG

            delta_content = (await provider.unit(delta)).content
            assert await delta_content.read(0, 4) == b"CbTT"


async def test_replayed_encrypted_vm_target_db_and_disk_image(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("device_vm_vault_encrypted.json.gz", allow_content=True)
    keys = KeyMaterial.from_key_string(VAULT_ENCRYPTED_KEY_STRING)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))

    async with await DedupRepo.open(store, layout, keys) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _VAULT_ENCRYPTED_WINDOWS_VM_WORKLOAD_ID)
        # Of three versions, the one this fixture recorded.
        version = next(v for v in await versions(repo, vm) if v.version_uid == "c9dfeb94-7037-41a2-acde-91dd456e259e")

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            objects = await provider.children(devices[0])
            disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)

            content = (await provider.unit(disk)).content
            header = await content.read(0, 520)
            assert header[510:512] == _MBR_BOOT_SIG
            assert header[512:520] == _GPT_SIG


async def test_replayed_vm_devices_listed_when_repo_root_is_non_empty(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """``DeviceProvider`` lists and reads a disk under a non-empty
    ``repo_root``, as ``iter_repository_layouts`` yields when the store is
    rooted above the vault."""
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("device_vm_repo_root_vault_plain.json.gz", allow_content=True)
    layout = await anext(
        layout
        async for repo in iter_repository_layouts(store)
        for layout in catalog_repo_layouts(repo)
        if layout.repo_root
    )
    assert layout.repo_root == "@ActiveProtectVault"

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
        # The newest of three versions: the one this fixture recorded.
        version = next(v for v in await versions(repo, vm) if v.version_uid == VAULT_PLAIN_VM_VERSION_UID)

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            assert len(devices) == 1

            objects = await provider.children(devices[0])
            disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
            content = (await provider.unit(disk)).content
            header = await content.read(0, 520)
            assert header[510:512] == _MBR_BOOT_SIG
            assert header[512:520] == _GPT_SIG
