"""Unit tests for ``synology_apm_repo.sdk.api.key_manager``'s ``KeyManager``
— the encryption-key state machine ``Repository`` delegates to. The four
``KeyStatus`` resolution branches and ``set_key()``'s own end-to-end
orchestration are already covered via ``Repository`` in
``test_api_repository.py``; these tests target ``KeyManager``'s own
contracts that only show up when ``adopt()``/``record()``/``verify()`` are
driven directly and out of ``set_key()``'s usual lockstep — nothing here
needs a real ``Store``."""

from __future__ import annotations

from typing import Any, cast

import pytest

from synology_apm_repo.sdk.api.key_manager import KeyManager, KeyStatus
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification
from synology_apm_repo.sdk.errors import KeyMismatchError, KeyRequiredError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout, key_probe_layout

# parse_key_string (format/crypto.py) requires a 12-char userKeyID and a
# userKey that decodes to exactly 32 raw bytes, regardless of the id.
_VALID_B64_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def _some_key(user_key_id: str) -> KeyMaterial:
    assert len(user_key_id) == 12
    return KeyMaterial.from_key_string(f"{user_key_id}@{_VALID_B64_KEY}")


def _async_returning(value: Any) -> Any:
    """A coroutine function that ignores its arguments and returns
    ``value`` — the async replacement for a ``lambda *a, **k: value`` stub."""

    async def _fn(*a: object, **k: object) -> Any:
        return value

    return _fn


def test_require_verified_does_not_raise_when_encryption_status_unresolved() -> None:
    """``encrypted=None`` — the repository's own encryption-key record was
    entirely absent, so ``is_encrypted`` itself couldn't resolve. Blocking on
    a genuinely unknown status would be presumptuous."""
    manager = KeyManager(None, None, encrypted=None)
    manager.require_verified()  # must not raise


def test_require_verified_raises_key_required_when_confirmed_encrypted_and_no_key() -> None:
    manager = KeyManager(None, None, encrypted=True)
    with pytest.raises(KeyRequiredError, match="call set_key"):
        manager.require_verified()


def test_require_verified_raises_key_mismatch_when_status_invalid() -> None:
    keys = _some_key("some-id-0000")
    manager = KeyManager(keys, KeyVerification(gcm_ok=False, vault_key=None))
    with pytest.raises(KeyMismatchError, match="rejected"):
        manager.require_verified()


def test_adopt_sets_keys_but_leaves_status_and_verification_unchanged() -> None:
    """``adopt()`` commits new key material immediately, but ``record()``
    is what recomputes ``status``/``verification`` once the reopen sweep
    finishes — the two are deliberately decoupled."""
    manager = KeyManager(None, None, encrypted=True)
    status_before, verification_before = manager.status, manager.verification

    new_keys = _some_key("some-id-0000")
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
    """``record()`` takes the just-tried key material as an explicit
    parameter rather than reading ``self.keys`` back — a rejected key must
    still land on ``INVALID`` even though a rejected key never reaches
    ``adopt()`` at all."""
    manager = KeyManager(None, None, encrypted=True)
    tried_keys = _some_key("some-id-0000")

    manager.record(tried_keys, verification)

    assert manager.status is expected_status
    assert manager.verification is verification
    assert manager.keys is None  # record() never touches .keys -- only adopt() does


async def test_verify_is_pure_and_does_not_mutate_state(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(fake_verification))

    manager = KeyManager(None, None, encrypted=True)
    status_before, keys_before, verification_before = manager.status, manager.keys, manager.verification

    layout = RepositoryLayout(kind=RepoKind.VAULT, repo_root="")
    _, verification = await manager.verify(cast(Any, object()), layout, f"some-id-0000@{_VALID_B64_KEY}")

    assert verification is fake_verification
    assert manager.status is status_before
    assert manager.keys is keys_before
    assert manager.verification is verification_before


async def test_verify_passes_key_probe_layout_to_key_material_verify(monkeypatch: pytest.MonkeyPatch) -> None:
    """``verify()`` narrows a full ``RepositoryLayout`` down to
    ``key_probe_layout()``'s throwaway ``RepoLayout`` before delegating —
    it must never hand ``KeyMaterial.verify`` the whole layout."""
    captured: list[RepoLayout] = []

    async def fake_verify(self: KeyMaterial, store: object, layout: RepoLayout) -> KeyVerification:
        captured.append(layout)
        return KeyVerification(gcm_ok=True, vault_key=b"x" * 32)

    monkeypatch.setattr(KeyMaterial, "verify", fake_verify)

    manager = KeyManager(None, None, encrypted=True)
    layout = RepositoryLayout(kind=RepoKind.VAULT, repo_root="some-root")
    await manager.verify(cast(Any, object()), layout, f"some-id-0000@{_VALID_B64_KEY}")

    assert captured == [key_probe_layout(layout)]
