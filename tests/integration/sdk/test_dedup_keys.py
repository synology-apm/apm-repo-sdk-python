"""Regression tests for ``dedup.keys`` against unencrypted and encrypted
repositories. ``"NoEncryption"`` (``NO_ENCRYPTION_USER_KEY_ID``) is the
fixed sentinel of an unencrypted repository, not key material.

Fixtures:

- ``dedup_keys_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``; its one test.
- ``dedup_keys_vault_encrypted_verify.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``: ``KeyMaterial.verify()``
  plus a fingerprint-verified content read. The correct-key test is its
  recipe; the wrong-key test's calls are a subset (``verify()``'s reads
  depend only on ``user_key_id``, never ``user_key``).
- ``dedup_keys_objstore_encrypted.json.gz`` — recorded against
  ``objstore-encrypted``: both repo ids, each verified and opened with
  ``VerifyPolicy(fingerprint=True)``; its one test.
"""

from __future__ import annotations

import base64
import hashlib
import os
from collections.abc import Awaitable, Callable

from support.recording.sample_constants import OBJSTORE_ENCRYPTED_KEY_STRING, VAULT_ENCRYPTED_KEY_STRING
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.pool import VerifyPolicy
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout, iter_repository_layouts

#: vault-encrypted's Windows VM workload; its anonymized display name is
#: looked up through this id.
_VM_WORKLOAD_ID = 3


async def test_replayed_unencrypted_apv_repo_verifies_with_no_encryption(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("dedup_keys_vault_plain.json.gz")
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    km = KeyMaterial(user_key_id="NoEncryption", user_key=b"\x00" * 32)
    result = await km.verify(store, layout)
    assert result.ok is True
    assert result.vault_key is None  # nothing to unwrap — repository is unencrypted


async def test_replayed_encrypted_apv_repo_verifies_with_correct_key(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: only len(data) is asserted, as proof the
    # fingerprint check passed.
    store = await record_target("dedup_keys_vault_encrypted_verify.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    km = KeyMaterial.from_key_string(VAULT_ENCRYPTED_KEY_STRING)

    result = await km.verify(store, layout)
    assert result.ok is True
    assert result.gcm_ok is True
    assert result.vault_key is not None

    async with await DedupRepo.open(store, layout, km, verify=VerifyPolicy(fingerprint=True)) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm_name = next(w.display_name for w in all_workloads if w.workload_id == _VM_WORKLOAD_ID)
        encrypted_vm_path = (
            "VM-19017d50-4f2b-4b99-b871-1e5479bba78c/ActiveBackup_2026-08-06_215706/"
            f"{vm_name}/0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img"
        )
        f = await repo.open_file(encrypted_vm_path)
        data = await f.read(0, 4096)
        assert len(data) == 4096  # would have raised DataCorruptError on a fingerprint mismatch


async def test_replayed_encrypted_apv_repo_rejects_a_wrong_key(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("dedup_keys_vault_encrypted_verify.json.gz")
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    # correct userKeyID, deliberately wrong userKey secret
    user_key_id = VAULT_ENCRYPTED_KEY_STRING.split("@", 1)[0]
    wrong_key_string = f"{user_key_id}@{base64.b64encode(os.urandom(32)).decode()}"
    km = KeyMaterial.from_key_string(wrong_key_string)

    result = await km.verify(store, layout)
    assert result.ok is False
    assert result.gcm_ok is False


async def test_replayed_encrypted_object_store_repos_verify_with_correct_key(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: the size+hash pair below is a structural oracle,
    # never the file's own content meaning.
    store = await record_target("dedup_keys_objstore_encrypted.json.gz", allow_content=True)
    km = KeyMaterial.from_key_string(OBJSTORE_ENCRYPTED_KEY_STRING)

    # Each repository's recorded file_map row (size, content hash), keyed by
    # repo_root: the wrong vault key or generation could still return some
    # non-empty file, but not this one. The path is left out because it
    # embeds an anonymized device name.
    _EXPECTED_VAULT_KEY_HEX = "a63dc53de9e2ceb7990a557d21ef38fa0a0f65592a6c2d1c7c87d29f8b93c63f"
    expected_files = {
        "@ActiveProtectData/BikXpRbFNGI1": (
            21474836480,
            "bb519f2333e022de229408dc8f0a4e09e176a10838db63d6c9a9582ddd55a07a",
        ),
        "@ActiveProtectData/uoRtcQebTU5w": (
            594563072,
            "9cac786004e88257a6c49ce252c3f8956fa8799d64181d311a72d233edb6a54c",
        ),
    }

    layouts = [layout async for repo in iter_repository_layouts(store) for layout in catalog_repo_layouts(repo)]
    assert {layout.repo_root for layout in layouts} == set(expected_files)  # both repo ids under @ActiveProtectData
    for layout in layouts:
        result = await km.verify(store, layout)
        assert result.ok is True
        assert result.vault_key is not None
        assert result.vault_key.hex() == _EXPECTED_VAULT_KEY_HEX

        expected_size, expected_sha256 = expected_files[layout.repo_root]
        async with await DedupRepo.open(store, layout, km, verify=VerifyPolicy(fingerprint=True)) as repo:
            conn = await repo.db("file_map")
            cursor = await conn.execute("SELECT path FROM file_map LIMIT 1")
            row = await cursor.fetchone()
            assert row is not None
            f = await repo.open_file(str(row[0]))
            assert f.size == expected_size
            read_len = min(4096, f.size)
            data = await f.read(0, read_len)
            assert len(data) == read_len  # would have raised DataCorruptError on a fingerprint mismatch
            assert hashlib.sha256(data).hexdigest() == expected_sha256
