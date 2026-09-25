"""Regression test for ``dedup.keys`` against unencrypted and encrypted
repositories — replayed from committed fixtures, with **no external
dependency**: this always runs, on CI or anywhere else, because it goes
through ``ReplayStore`` instead of a real ``LocalFsStore``.
``"NoEncryption"`` (``NO_ENCRYPTION_USER_KEY_ID``) is a fixed sentinel
marking an unencrypted repository with no VaultKey to resolve, not
real key material. The
encrypted scenarios' real vault keys are embedded below as literal
constants (each sample's own generated key, not customer data) rather
than read from a real sample tree at test time.

The fixtures (``tests/fixtures/``, recorded once by ``RecordingStore``
via each test's own ``record_target()`` call — see ``tests/conftest.py``
and ``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...`` workflow that (re-)records these):

- ``dedup_keys_unencrypted_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``.
- ``dedup_keys_encrypted_apv2_verify.json.gz`` — an encrypted
  vault repository's ``KeyMaterial.verify()`` plus a ``verify_fingerprint=True``
  content read, shared with the wrong-key scenario below (whose own
  storage calls are an exact subset — ``KeyMaterial.verify()``'s reads
  depend only on ``user_key_id``, never ``user_key``).
  ``tests/integration/sdk/test_vm_key_verification.py``'s own
  two-layer verification test has its own dedicated copy of the same
  real bytes (``vm_key_verification_apv2_encrypted.json.gz``).
- ``dedup_keys_encrypted_s3sample2.json.gz`` — both real repo ids
  of an encrypted object-store repository (``s3-sample-2-encrypted``), each
  verified and opened with ``verify_fingerprint=True``.
"""

from __future__ import annotations

import base64
import hashlib
import os
from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout, iter_layouts

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="
#: s3-sample-2-encrypted's real key.
_S3SAMPLE2_ENCRYPTED_KEY_STRING = "IvcvldpbSRyd@Ys+mbaQyOElj6bHtL0+VFdp1e4swyEeApQLhdHgaTvg="

#: An internal catalog identifier -- stable and non-identifying (never
#: touched by catalog-metadata anonymization) -- for apv-sample-2-
#: encrypted's one VM workload. Used to look up its own display name
#: programmatically rather than writing that name into this file.
_VM_WORKLOAD_ID = 3


async def test_replayed_unencrypted_apv_repo_verifies_with_no_encryption(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("dedup_keys_unencrypted_apv1.json.gz")
    layout = await detect_layout(store)
    km = KeyMaterial(user_key_id="NoEncryption", user_key=b"\x00" * 32)
    result = await km.verify(store, layout)
    assert result.ok is True
    assert result.vault_key is None  # nothing to unwrap — repository is unencrypted


async def test_replayed_encrypted_apv_repo_verifies_with_correct_key(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: only len(data) is asserted (a fingerprint-
    # verification proof) -- a structural oracle, never content meaning.
    store = await record_target("dedup_keys_encrypted_apv2_verify.json.gz", allow_content=True)
    layout = await detect_layout(store)
    km = KeyMaterial.from_key_string(_APV2_ENCRYPTED_KEY_STRING)

    result = await km.verify(store, layout)
    assert result.ok is True
    assert result.gcm_ok is True
    assert result.vault_key is not None

    async with await DedupRepo.open(store, layout, km, verify_fingerprint=True) as repo:
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
    store = await record_target("dedup_keys_encrypted_apv2_verify.json.gz")
    layout = await detect_layout(store)
    # correct userKeyID, deliberately wrong userKey secret
    user_key_id = _APV2_ENCRYPTED_KEY_STRING.split("@", 1)[0]
    wrong_key_string = f"{user_key_id}@{base64.b64encode(os.urandom(32)).decode()}"
    km = KeyMaterial.from_key_string(wrong_key_string)

    result = await km.verify(store, layout)
    assert result.ok is False
    assert result.gcm_ok is False


async def test_replayed_encrypted_object_store_repos_verify_with_correct_key(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: the size+hash pair below is a structural oracle,
    # never the file's own content meaning -- see the comment above it.
    store = await record_target("dedup_keys_encrypted_s3sample2.json.gz", allow_content=True)
    km = KeyMaterial.from_key_string(_S3SAMPLE2_ENCRYPTED_KEY_STRING)

    # The real, deterministic per-repository file_map row this fixture recorded
    # (size/content-hash), keyed by repo_root -- a regression that
    # verified against the wrong vault key or opened the wrong generation
    # could still satisfy ``result.vault_key is not None``/``len(data) ==
    # read_len`` while returning the wrong (but still non-empty) file; the
    # size+hash pair together is already strong enough proof of "right
    # file, right content, no silent corruption" without also needing the
    # file's own real path (which would embed an anonymized device name).
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

    layouts = [layout async for layout in iter_layouts(store)]
    assert {layout.repo_root for layout in layouts} == set(expected_files)  # both repo ids under @ActiveProtectData
    for layout in layouts:
        result = await km.verify(store, layout)
        assert result.ok is True
        assert result.vault_key is not None
        assert result.vault_key.hex() == _EXPECTED_VAULT_KEY_HEX

        expected_size, expected_sha256 = expected_files[layout.repo_root]
        async with await DedupRepo.open(store, layout, km, verify_fingerprint=True) as repo:
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


__all__: list[str] = []
