"""Regression test for ``synology-apm-repo-cli key``'s wrong-key and correct-key
scenarios — replayed from a committed fixture, same
``monkeypatch.setattr(cli.browse, "resolve_profile_store", ...)`` seam as
this file's siblings.

The wrong key string here (``wrongkeyid00@AAA...``) is a
deliberately-invalid literal — not real secret material, just a
syntactically-valid-looking key that must fail GCM verification. The
correct-key scenarios' real key is embedded below as a literal constant
(this sample's own generated vault key, not customer data) rather than
read from a real sample tree at test time.

Fixture: ``cli_key_apv2_vault.json.gz``, this file's own dedicated
``apv-sample-2-encrypted`` vault recording via ``Session.open_remote()``
— key verification is pure in-memory crypto on already-fetched bytes, so
giving a key changes nothing about what gets read from storage, and any
one of this file's three tests below is this fixture's recording
recipe. ``test_cli_doctor.py``'s own no-key ``doctor`` scan draws
from a separately-recorded ``cli_doctor_apv2_vault.json.gz`` fixture
against the same real vault rather than this one.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from types import ModuleType

from typer.testing import CliRunner

import synology_apm_repo.cli.browse as browse_mod
from synology_apm_repo.cli.main import app

runner = CliRunner()

_WRONG_KEY = "wrongkeyid00@AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="


def test_wrong_key_reports_invalid_replayed(patch_profile_store: Callable[[str, ModuleType], None]) -> None:
    patch_profile_store("cli_key_apv2_vault.json.gz", browse_mod)
    result = runner.invoke(app, ["key", "--profile", "anything", "--key", _WRONG_KEY])
    assert result.exit_code == 0, result.output
    assert "invalid" in result.output


def test_correct_key_reports_verified_replayed(patch_profile_store: Callable[[str, ModuleType], None]) -> None:
    patch_profile_store("cli_key_apv2_vault.json.gz", browse_mod)
    result = runner.invoke(app, ["key", "--profile", "anything", "--key", _APV2_ENCRYPTED_KEY_STRING])
    assert result.exit_code == 0, result.output
    assert "verified" in result.output
    assert "gcm_ok=True" in result.output
    assert "fingerprint_ok" not in result.output


def test_json_output_replayed(patch_profile_store: Callable[[str, ModuleType], None]) -> None:
    patch_profile_store("cli_key_apv2_vault.json.gz", browse_mod)
    result = runner.invoke(app, ["--json", "key", "--profile", "anything", "--key", _APV2_ENCRYPTED_KEY_STRING])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["status"] == "verified"


__all__: list[str] = []
