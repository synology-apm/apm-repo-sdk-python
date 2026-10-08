"""Regression tests for ``synology-apm-repo-cli key``'s wrong-key and
correct-key verdicts.

Fixture: ``cli_key_vault_encrypted.json.gz``, recorded against
``vault-encrypted/@ActiveProtectVault``.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from support.cli import invoke
from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING

_WRONG_KEY = "wrongkeyid00@AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def test_wrong_key_reports_invalid_replayed(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_key_vault_encrypted.json.gz")
    result = invoke(["key", "--profile", "anything", "--key", _WRONG_KEY])
    assert "invalid" in result.output


def test_correct_key_reports_verified_replayed(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_key_vault_encrypted.json.gz")
    result = invoke(["key", "--profile", "anything", "--key", VAULT_ENCRYPTED_KEY_STRING])
    assert "verified" in result.output
    assert "gcm_ok=True" in result.output
    assert "fingerprint_ok" not in result.output


def test_json_output_replayed(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_key_vault_encrypted.json.gz")
    result = invoke(["--json", "key", "--profile", "anything", "--key", VAULT_ENCRYPTED_KEY_STRING])
    report = json.loads(result.stdout)
    assert report["status"] == "verified"
