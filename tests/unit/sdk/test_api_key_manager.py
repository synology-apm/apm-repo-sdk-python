"""Unit tests for ``synology_apm_repo.sdk.api.key_manager``'s ``KeyManager``,
driving ``adopt()``/``record()``/``verify()`` directly, outside
``set_key()``'s lockstep; ``test_api_repository.py`` covers ``KeyStatus``
resolution and ``set_key()`` through ``Repository``."""

from __future__ import annotations

from typing import Any, cast

import pytest

from synology_apm_repo.sdk.api.key_manager import KeyManager, KeyStatus
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification
from synology_apm_repo.sdk.errors import KeyMismatchError, KeyRequiredError
from synology_apm_repo.sdk.storage.layout import RepoLayout, key_probe_layout
from unit.sdk.api_fakes import VALID_B64_KEY, async_returning, some_key, vault_repository_layout


def test_require_verified_does_not_raise_when_encryption_status_unresolved() -> None:
    """``encrypted=None`` (no encryption-key record) is an unknown status,
    which doesn't block."""
    manager = KeyManager(None, None, encrypted=None)
    manager.require_verified()  # must not raise

    assert manager.status is KeyStatus.NO_KEY_PROVIDED
    assert manager.is_encrypted is None


def test_require_verified_raises_key_required_when_confirmed_encrypted_and_no_key() -> None:
    manager = KeyManager(None, None, encrypted=True)
    with pytest.raises(KeyRequiredError, match="call set_key"):
        manager.require_verified()


def test_require_verified_raises_key_mismatch_when_status_invalid() -> None:
    keys = some_key("some-id-0000")
    manager = KeyManager(keys, KeyVerification(gcm_ok=False, vault_key=None))
    with pytest.raises(KeyMismatchError, match="rejected"):
        manager.require_verified()


def test_adopt_sets_keys_but_leaves_status_and_verification_unchanged() -> None:
    """Only ``record()`` recomputes ``status``/``verification``."""
    manager = KeyManager(None, None, encrypted=True)
    status_before, verification_before = manager.status, manager.verification

    new_keys = some_key("some-id-0000")
    manager.adopt(new_keys)

    assert manager.keys is new_keys
    assert manager.status is status_before
    assert manager.verification is verification_before


@pytest.mark.parametrize(
    ("verification", "expected_status"),
    [
        (KeyVerification(gcm_ok=True, vault_key=b"x" * 32), KeyStatus.VERIFIED),
        (KeyVerification(gcm_ok=False, vault_key=None), KeyStatus.INVALID),
    ],
)
def test_record_computes_status_from_its_own_keys_param_not_self_keys(
    verification: KeyVerification, expected_status: KeyStatus
) -> None:
    """A rejected key never reaches ``adopt()``, so ``record()`` takes the
    tried key material as a parameter instead of reading ``self.keys``."""
    manager = KeyManager(None, None, encrypted=True)
    tried_keys = some_key("some-id-0000")

    manager.record(tried_keys, verification)

    assert manager.status is expected_status
    assert manager.verification is verification
    assert manager.keys is None  # record() never touches .keys -- only adopt() does


async def test_verify_is_pure_and_does_not_mutate_state(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(fake_verification))

    manager = KeyManager(None, None, encrypted=True)
    status_before, keys_before, verification_before = manager.status, manager.keys, manager.verification

    layout = vault_repository_layout()
    _, verification = await manager.verify(cast(Any, object()), layout, f"some-id-0000@{VALID_B64_KEY}")

    assert verification is fake_verification
    assert manager.status is status_before
    assert manager.keys is keys_before
    assert manager.verification is verification_before


async def test_verify_passes_key_probe_layout_to_key_material_verify(monkeypatch: pytest.MonkeyPatch) -> None:
    """``KeyMaterial.verify`` receives ``key_probe_layout()``'s ``RepoLayout``,
    not the whole ``RepositoryLayout``."""
    captured: list[RepoLayout] = []

    async def fake_verify(self: KeyMaterial, store: object, layout: RepoLayout) -> KeyVerification:
        captured.append(layout)
        return KeyVerification(gcm_ok=True, vault_key=b"x" * 32)

    monkeypatch.setattr(KeyMaterial, "verify", fake_verify)

    manager = KeyManager(None, None, encrypted=True)
    layout = vault_repository_layout("some-root")
    await manager.verify(cast(Any, object()), layout, f"some-id-0000@{VALID_B64_KEY}")

    assert captured == [key_probe_layout(layout)]


def test_every_key_status_has_a_user_facing_label() -> None:
    assert {status: status.label for status in KeyStatus} == {
        KeyStatus.NO_KEY_PROVIDED: "key needed",
        KeyStatus.NOT_ENCRYPTED: "not encrypted",
        KeyStatus.VERIFIED: "key verified",
        KeyStatus.INVALID: "invalid key",
    }
