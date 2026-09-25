"""Regression test for ``synology-apm-repo-cli doctor`` — replayed from committed
fixtures recorded against real bytes, with **no external dependency**:
same ``patch_profile_store`` fixture (``tests/integration/cli/conftest.py``)
every sibling in this directory uses.

``doctor.py``'s own ``_run()`` builds the exact same ``report`` dict
regardless of ``--verbose``/``--json`` (those flags only change how
it's *rendered*, never what's read) — so one fixture per repository/key
combination covers every rendering variant. Two dedicated fixtures:

- ``cli_doctor_apv1_vault.json.gz`` — ``apv-sample-1``'s vault, no key
  (covers human/``--verbose``/``--json``/sub_type-support/
  not-encrypted-without-a-key — 5 scenarios, all making identical
  storage calls per the previous paragraph, so any one of the 5 tests
  below is this fixture's recording recipe).
- ``cli_doctor_apv2_vault.json.gz`` — ``apv-sample-2-encrypted``'s
  vault. Opening with a real key additionally reads the wrapped-VaultKey
  record that opening with no key never touches, so
  ``test_doctor_verifies_a_correct_key_for_an_encrypted_repo_replayed``
  (real key) is a strict superset of
  ``test_doctor_fails_cleanly_for_an_encrypted_repo_given_no_key_replayed``
  (no key) and is this fixture's recording recipe — GCM key verification
  is real crypto run entirely on bytes already fetched from storage, so
  the *storage* calls are identical whichever key string is given (see
  ``test_cli_key.py``, which shares this same real vault via its
  own separately-recorded ``cli_key_apv2_vault.json.gz`` fixture rather
  than this one).

The scenario needing a real, correct key
(``test_doctor_verifies_a_correct_key_for_an_encrypted_repo``) embeds
that real key below as a literal constant (this sample's own generated
vault key, not customer data) rather than read from a real sample tree
at test time. ``test_doctor_on_nonexistent_path_fails_cleanly`` isn't
reproduced via ``ReplayStore`` at all: it already has zero real-sample
dependency (an empty ``tmp_path`` directory, no
``--profile``/``ReplayStore`` involved at all).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

from typer.testing import CliRunner

import synology_apm_repo.cli.repo_session as repo_session_mod
from synology_apm_repo.cli.main import app

runner = CliRunner()

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="

#: An internal catalog identifier -- stable and non-identifying (never
#: touched by catalog-metadata anonymization).
_WINDOWS_VM_WORKLOAD_ID = 2


def test_doctor_human_output_hides_internal_ids_replayed(
    patch_profile_store: Callable[[str, ModuleType], None],
) -> None:
    patch_profile_store("cli_doctor_apv1_vault.json.gz", repo_session_mod)
    result = runner.invoke(app, ["doctor", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert "Windows 10 (64-bit)" in result.output  # a real, never-anonymized OS-name subtitle
    assert "catalog_id" not in result.output
    assert "workload_id" not in result.output


def test_doctor_verbose_output_shows_internal_ids_replayed(
    patch_profile_store: Callable[[str, ModuleType], None],
) -> None:
    patch_profile_store("cli_doctor_apv1_vault.json.gz", repo_session_mod)
    result = runner.invoke(app, ["--verbose", "doctor", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert "repo_uuid" in result.output
    assert "m7e61v80stMAZgru" in result.output
    # repo_root is part of doctor's own --verbose output (see
    # commands/doctor.py's module docstring); repo_uuid is per-catalog
    # (each repo-id has its own repo_info for object storage; the same
    # value repeated per sibling for a vault).
    assert "repo_root" in result.output
    assert "repo_type=2" in result.output
    # --verbose's internal ids must reach the plain-text render too, not
    # just --json --verbose — _catalog_report()/_workload_report()
    # already compute these under verbose=True; _render_human() must
    # actually show them.
    assert "catalog_id=" in result.output
    assert "workload_id=" in result.output
    assert "workload_type=" in result.output


def test_doctor_json_output_hides_internal_ids_without_verbose_replayed(
    patch_profile_store: Callable[[str, ModuleType], None],
) -> None:
    """``--json`` must hide the same internal ids human mode does without
    ``--verbose`` — the two must never disagree about what's exposed by
    default (these report fields are ``NotRequired``, present only
    ``if state.verbose``, so both renderers read the exact same
    already-filtered dict)."""
    patch_profile_store("cli_doctor_apv1_vault.json.gz", repo_session_mod)
    result = runner.invoke(app, ["--json", "doctor", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["layout"] == "vault"
    assert "repo_uuid" not in report
    assert "repo_type" not in report
    assert "repo_root" not in report
    assert len(report["catalogs"]) == 2  # apv-sample-1's real 2 catalogs
    assert all(isinstance(c["display_name"], str) and c["display_name"] for c in report["catalogs"])
    total_workloads = sum(c["workload_count"] for c in report["catalogs"])
    total_versions = sum(c["version_count"] for c in report["catalogs"])
    assert total_workloads == 25
    assert total_versions == 109


def test_doctor_verbose_json_output_is_valid_and_keyed_by_stable_fields_replayed(
    patch_profile_store: Callable[[str, ModuleType], None],
) -> None:
    patch_profile_store("cli_doctor_apv1_vault.json.gz", repo_session_mod)
    result = runner.invoke(app, ["--verbose", "--json", "doctor", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["layout"] == "vault"
    # repo_uuid/repo_type are per-catalog now (a vault's own sibling
    # catalogs all share one repo_info, so every one of them reports the
    # same value).
    assert all(c["repo_uuid"] == "m7e61v80stMAZgru" for c in report["catalogs"])
    assert all(c["repo_type"] == 2 for c in report["catalogs"])
    assert len(report["catalogs"]) == 2  # apv-sample-1's real 2 catalogs
    total_workloads = sum(c["workload_count"] for c in report["catalogs"])
    total_versions = sum(c["version_count"] for c in report["catalogs"])
    assert total_workloads == 25
    assert total_versions == 109


def test_doctor_marks_saas_sub_types_supported_or_unsupported_correctly_replayed(
    patch_profile_store: Callable[[str, ModuleType], None],
) -> None:
    patch_profile_store("cli_doctor_apv1_vault.json.gz", repo_session_mod)
    result = runner.invoke(app, ["--verbose", "--json", "doctor", "--profile", "anything"])
    report = json.loads(result.stdout)
    all_workloads = [wl for cat in report["catalogs"] for wl in cat["workloads"]]

    mail_wl = next(wl for wl in all_workloads if wl["sub_type"] == "MAIL")
    assert mail_wl["supported"] is True
    teams_wl = next(wl for wl in all_workloads if wl["sub_type"] == "TEAMS")
    assert teams_wl["supported"] is True
    team_drive_wl = next(wl for wl in all_workloads if wl["sub_type"] == "TEAM_DRIVE")
    assert team_drive_wl["supported"] is True
    group_exchange_wl = next(wl for wl in all_workloads if wl["sub_type"] == "GROUP_EXCHANGE")
    assert group_exchange_wl["supported"] is True
    vm_wl = next(wl for wl in all_workloads if wl["workload_id"] == _WINDOWS_VM_WORKLOAD_ID)
    assert vm_wl["supported"] is True


def test_doctor_reports_not_encrypted_for_an_unencrypted_repo_even_without_a_key_replayed(
    patch_profile_store: Callable[[str, ModuleType], None],
) -> None:
    patch_profile_store("cli_doctor_apv1_vault.json.gz", repo_session_mod)
    result = runner.invoke(app, ["--json", "doctor", "--profile", "anything"])
    report = json.loads(result.stdout)
    assert report["key"]["status"] == "not_encrypted"
    assert report["key"]["is_encrypted"] is False


def test_doctor_fails_cleanly_for_an_encrypted_repo_given_no_key_replayed(
    patch_profile_store: Callable[[str, ModuleType], None],
) -> None:
    """``Repository.workloads()``/``.versions()`` raise ``KeyRequiredError``
    before any catalog I/O for a confirmed-encrypted, not-yet-keyed repository
    (see ``api/repository.py``'s ``_require_key_verified``) — `doctor`
    gets no special carve-out for this, the same as every other command;
    it fails cleanly via the shared ``opened_repo()`` handler rather than
    printing a report that includes a ``no_key_provided`` key status
    alongside real workload data. ``errors.py``'s ``friendly_message()`` rephrases
    the SDK's own ``KeyRequiredError`` wording (aimed at a caller who'd call
    ``set_key()`` directly) into CLI language before it reaches here —
    ``--key``/``synology-apm-repo-cli key``, never the internal
    ``set_key()`` method name."""
    patch_profile_store("cli_doctor_apv2_vault.json.gz", repo_session_mod)
    result = runner.invoke(app, ["--json", "doctor", "--profile", "anything"])
    assert result.exit_code == 1
    assert "this repository is encrypted" in result.output
    assert "--key" in result.output
    assert "set_key" not in result.output


def test_doctor_verifies_a_correct_key_for_an_encrypted_repo_replayed(
    patch_profile_store: Callable[[str, ModuleType], None],
) -> None:
    patch_profile_store("cli_doctor_apv2_vault.json.gz", repo_session_mod)
    result = runner.invoke(app, ["--json", "doctor", "--profile", "anything", "--key", _APV2_ENCRYPTED_KEY_STRING])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["key"]["status"] == "verified"
    assert report["key"]["is_encrypted"] is True
    assert report["key"]["gcm_ok"] is True
    assert "fingerprint_ok" not in report["key"]


def test_doctor_on_nonexistent_path_fails_cleanly(tmp_path: Path) -> None:
    # Zero real-sample dependency (an empty tmp_path directory, no
    # --profile/ReplayStore involved).
    empty = tmp_path / "not_a_repo"
    empty.mkdir()
    result = runner.invoke(app, ["doctor", str(empty)])
    assert result.exit_code == 1


__all__: list[str] = []
