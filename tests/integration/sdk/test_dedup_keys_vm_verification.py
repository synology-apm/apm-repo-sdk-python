"""Regression test for two-layer key verification against a real
vault-encrypted VM disk image, replayed from a committed fixture.

Fixture: ``vm_key_verification_vault_encrypted.json.gz``, recorded against
``vault-encrypted/@ActiveProtectVault``; the one test is its
recording recipe.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.pool import VerifyPolicy
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"


#: vault-encrypted's Windows VM workload; its anonymized display name is
#: looked up through this id.
_VM_WORKLOAD_ID = 3


async def test_replayed_encrypted_repo_passes_two_layer_key_verification(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """``KeyMaterial.verify()`` checks the GCM layer; with
    ``VerifyPolicy(fingerprint=True)`` every chunk read is
    fingerprint-checked, so reading a real MBR back proves the fingerprint
    layer too."""
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle, never the disk's own real content.
    store = await record_target("vm_key_verification_vault_encrypted.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    keys = KeyMaterial.from_key_string(VAULT_ENCRYPTED_KEY_STRING)

    result = await keys.verify(store, layout)
    assert result.gcm_ok is True
    assert result.ok is True
    assert result.vault_key is not None

    async with await DedupRepo.open(store, layout, keys, verify=VerifyPolicy(fingerprint=True)) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm_name = next(w.display_name for w in all_workloads if w.workload_id == _VM_WORKLOAD_ID)
        encrypted_vm_path = (
            "VM-19017d50-4f2b-4b99-b871-1e5479bba78c/ActiveBackup_2026-08-06_215706/"
            f"{vm_name}/0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img"
        )
        f = await repo.open_file(encrypted_vm_path)
        header = await f.read(0, 520)
        assert header[510:512] == _MBR_BOOT_SIG
        assert header[512:520] == _GPT_SIG
