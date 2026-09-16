"""Regression test for the full Device (VM) resolution chain — catalog
→ ``DeviceProvider`` → a real disk image's MBR/GPT bytes and a real
non-dedup ``.delta`` sidecar file — replayed from committed fixtures
recorded against real bytes, with **no external dependency**: these
always run, on CI or anywhere else, because they go through
``ReplayStore`` instead of a real ``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded against a real store — see
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...``/``make record-fixture`` workflow that
(re-)records these):

- ``device_vm_mbr_gpt_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``, the plaintext Windows VM's
  (``_WINDOWS_VM_WORKLOAD_ID``) disk image plus its ``.delta`` sidecar.
- ``device_vm_encrypted_apv2.json.gz`` — rooted at
  ``apv-sample-2-encrypted/@ActiveProtectVault``, that sample's own Windows
  VM disk image through an ``aHlT``-encrypted ``target.db``.
- ``device_vm_repo_root_apv1.json.gz`` — rooted at ``apv-sample-1``
  itself (a non-empty ``repo_root``, discovered via ``iter_layouts``) rather
  than directly at ``@ActiveProtectVault``, exercising the newest of the
  same VM's three versions.

Device's plaintext path needs no object-location scan, unlike a SaaS
provider's own first-open chain.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout, iter_layouts
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.device import DeviceProvider

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="

#: Internal catalog identifiers -- stable and non-identifying (never
#: touched by catalog-metadata anonymization). Each real sample assigns
#: its own ids independently, so apv-sample-1's Windows VM and
#: apv-sample-2-encrypted's own Windows VM need separate constants even
#: though both are informally "the Windows VM" -- apv-sample-2-encrypted
#: also has an unrelated Fedora VM workload (id 1), so filtering by
#: ``workload_type == "VM"`` alone would be ambiguous there.
_WINDOWS_VM_WORKLOAD_ID = 2
_APV2_ENCRYPTED_WINDOWS_VM_WORKLOAD_ID = 3


async def test_replayed_plaintext_vm_disk_image_and_delta_sidecar(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: reads only the disk's MBR/GPT signature bytes and
    # the delta sidecar's magic -- a structural oracle, never the disk's
    # own real content.
    store = await record_target("device_vm_mbr_gpt_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
        # This fixture's real apv-sample-1 data has three VM versions with
        # meta; versions() returns them newest-first, so picking by
        # version_uid pins down the exact one the fixture actually
        # recorded a full tree walk for (2026-08-06 21:57:06, not the
        # newest of the three).
        version = next(v for v in await versions(repo, vm) if v.version_uid == "887c024b-30c4-4c62-8df8-1fa0f8cebd1c")

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            assert len(devices) == 1

            objects = await provider.children(devices[0])
            disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
            delta = next(o for o in objects if o.kind is UnitKind.FILE)

            disk_content = (await provider.unit(disk)).open()
            header = await disk_content.read(0, 520)
            assert header[510:512] == _MBR_BOOT_SIG
            assert header[512:520] == _GPT_SIG

            delta_content = (await provider.unit(delta)).open()
            assert await delta_content.read(0, 4) == b"CbTT"


async def test_replayed_encrypted_vm_target_db_and_disk_image(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("device_vm_encrypted_apv2.json.gz", allow_content=True)
    keys = KeyMaterial.from_key_string(_ENCRYPTED_KEY_STRING)
    layout = await detect_layout(store)

    async with await DedupRepo.open(store, layout, keys) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _APV2_ENCRYPTED_WINDOWS_VM_WORKLOAD_ID)
        # This fixture's real apv-sample-2-encrypted data has three VM
        # versions with meta; versions() returns them newest-first, so
        # picking by version_uid pins down the exact one the fixture
        # actually recorded a full tree walk for.
        version = next(v for v in await versions(repo, vm) if v.version_uid == "c9dfeb94-7037-41a2-acde-91dd456e259e")

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            objects = await provider.children(devices[0])
            disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)

            content = (await provider.unit(disk)).open()
            header = await content.read(0, 520)
            assert header[510:512] == _MBR_BOOT_SIG
            assert header[512:520] == _GPT_SIG


async def test_replayed_vm_devices_listed_when_repo_root_is_non_empty(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """Exercises a non-empty ``repo_root`` (via ``iter_layouts``, matching
    ``Session.discover()``'s real usage), unlike the tests above which root
    ``ReplayStore`` directly at ``@ActiveProtectVault`` (``repo_root == ""``)."""
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("device_vm_repo_root_apv1.json.gz", allow_content=True)
    layout = await anext(layout async for layout in iter_layouts(store) if layout.repo_root)
    assert layout.repo_root == "@ActiveProtectVault"

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
        # This fixture recorded the newest of this VM's three versions
        # (versions() returns newest-first); version_uid pins it down
        # exactly.
        version = next(v for v in await versions(repo, vm) if v.version_uid == "06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7")

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            assert len(devices) == 1

            objects = await provider.children(devices[0])
            disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
            content = (await provider.unit(disk)).open()
            header = await content.read(0, 520)
            assert header[510:512] == _MBR_BOOT_SIG
            assert header[512:520] == _GPT_SIG


__all__: list[str] = []
