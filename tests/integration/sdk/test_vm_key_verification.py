"""Regression test for the two-layer key verification scenario against a
real, vault-encrypted VM disk image — replayed from a committed fixture,
with **no external dependency**: this always runs, on CI or anywhere
else, because it goes through ``ReplayStore`` instead of a real
``LocalFsStore``.

The fixture (``tests/fixtures/vm_key_verification_apv2_encrypted.json.gz``)
was recorded once by ``RecordingStore`` via this file's own
``record_target()`` call — see ``tests/conftest.py`` and
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...`` workflow that (re-)records it. It's a dedicated
copy of the same real bytes
``tests/integration/sdk/test_dedup_keys.py``'s own correct-key
scenario backs. The real vault key is embedded below as a literal
constant (this sample's own generated key, not customer data) rather
than read from a real sample tree at test time.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="

#: An internal catalog identifier -- stable and non-identifying (never
#: touched by catalog-metadata anonymization) -- for apv-sample-2-
#: encrypted's one VM workload. Used to look up its own display name
#: programmatically rather than writing that name into this file.
_VM_WORKLOAD_ID = 3


async def test_replayed_encrypted_repo_passes_two_layer_key_verification(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """Both layers matter: ``KeyMaterial.verify`` (the GCM layer, by itself
    already the whole answer to "is this key correct," see
    ``sdk.dedup.keys``'s own module docstring) plus a real chunk read with
    ``Pool.read_chunk``'s own ``verify_fingerprint=True`` option. Opening
    the repository with
    ``verify_fingerprint=True`` makes it the default for every read this
    test then does, so reading a real MBR back successfully is itself
    proof the fingerprint layer passed too, not just the GCM one."""
    # allow_content=True: reads only the disk's MBR/GPT signature bytes --
    # a structural oracle (also proving the fingerprint layer passed, per
    # this function's own docstring), never the disk's own real content.
    store = await record_target("vm_key_verification_apv2_encrypted.json.gz", allow_content=True)
    layout = await detect_layout(store)
    keys = KeyMaterial.from_key_string(_APV2_ENCRYPTED_KEY_STRING)

    result = await keys.verify(store, layout)
    assert result.gcm_ok is True
    assert result.ok is True
    assert result.vault_key is not None

    async with await DedupRepo.open(store, layout, keys, verify_fingerprint=True) as repo:
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


__all__: list[str] = []
