"""Unit tests for ``synology_apm_repo.sdk.profiles.secrets`` — the
keyring-backed secret storage scheme (one item per profile, every secret
field merged into it) and the "no plaintext fallback, ever" guarantee.
Exercised against the in-memory ``fake_keyring`` fixture
(``tests/conftest.py``), never the real OS keyring."""

from __future__ import annotations

import sys

import pytest

from synology_apm_repo.sdk.errors import ProfileSecretBackendUnavailableError
from synology_apm_repo.sdk.profiles import secrets


def test_set_then_get_round_trips(fake_keyring: None) -> None:
    secrets.set_secrets("demo", {"access_key": "AKIA", "secret_key": "shh"})
    assert secrets.get_secrets("demo") == {"access_key": "AKIA", "secret_key": "shh"}


def test_set_then_get_round_trips_smb_password(fake_keyring: None) -> None:
    secrets.set_secrets("demo", {"password": "hunter2"})
    assert secrets.get_secrets("demo") == {"password": "hunter2"}


def test_get_secrets_never_set_is_empty_not_error(fake_keyring: None) -> None:
    assert secrets.get_secrets("never-saved") == {}


def test_secrets_are_scoped_per_profile(fake_keyring: None) -> None:
    secrets.set_secrets("one", {"access_key": "AAA"})
    secrets.set_secrets("two", {"access_key": "BBB"})
    assert secrets.get_secrets("one") == {"access_key": "AAA"}
    assert secrets.get_secrets("two") == {"access_key": "BBB"}


def test_set_secrets_merges_with_existing_fields(fake_keyring: None) -> None:
    """Setting one field after another doesn't clobber a sibling field
    already stored in the same keyring item — the read-modify-write this
    single-item scheme depends on."""
    secrets.set_secrets("demo", {"access_key": "AKIA"})
    secrets.set_secrets("demo", {"secret_key": "shh"})
    assert secrets.get_secrets("demo") == {"access_key": "AKIA", "secret_key": "shh"}


def test_single_keyring_item_regardless_of_field_count(fake_keyring: None) -> None:
    """The whole point of this scheme: every secret field for one profile
    lands in one keyring item, not one item per field."""
    import keyring

    secrets.set_secrets("demo", {"access_key": "AKIA", "secret_key": "shh"})
    backend = keyring.get_keyring()
    assert len(backend._values) == 1  # type: ignore[attr-defined]


def test_delete_secrets_removes_the_stored_item(fake_keyring: None) -> None:
    secrets.set_secrets("demo", {"access_key": "AKIA", "secret_key": "shh"})
    secrets.delete_secrets("demo")
    assert secrets.get_secrets("demo") == {}


def test_delete_secrets_never_set_is_not_an_error(fake_keyring: None) -> None:
    secrets.delete_secrets("never-saved")  # must not raise


def test_partial_secret_set_only_stores_given_fields(fake_keyring: None) -> None:
    """A blank/omitted secret means "use the ambient credential chain" —
    it must not be stored as an empty string."""
    secrets.set_secrets("demo", {"credential": "sas-token"})
    assert secrets.get_secrets("demo") == {"credential": "sas-token"}


def test_no_backend_raises_profile_secret_backend_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    import keyring
    import keyring.backends.fail

    monkeypatch.setattr(keyring, "get_keyring", lambda: keyring.backends.fail.Keyring())  # type: ignore[no-untyped-call]
    with pytest.raises(ProfileSecretBackendUnavailableError):
        secrets.get_secrets("demo")


def test_broken_keyring_install_raises_profile_secret_backend_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``sys.modules["keyring"] = None`` makes a subsequent ``import
    keyring`` raise ``ImportError`` (a real, documented CPython import
    system behavior), simulating a broken/partial install without actually
    uninstalling keyring from this dev environment. A caller should see
    this project's own exception, not a bare ``ImportError``."""
    monkeypatch.setitem(sys.modules, "keyring", None)
    with pytest.raises(ProfileSecretBackendUnavailableError):
        secrets.get_secrets("demo")
