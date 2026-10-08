"""Regression tests for ``synology-apm-repo-cli doctor``.

``--verbose``/``--json`` change only how the report renders, and key
verification runs on already-fetched bytes, so one fixture per repository
covers every variant:

- ``cli_doctor_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``.
- ``cli_doctor_vault_encrypted.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from support.cli import invoke
from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING

#: The Windows VM workload's catalog id.
_WINDOWS_VM_WORKLOAD_ID = 2


def test_doctor_human_output_hides_internal_ids_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_doctor_vault_plain.json.gz")
    result = invoke(["doctor", "--profile", "anything"])
    assert "Windows 10 (64-bit)" in result.output  # a real, never-anonymized OS-name subtitle
    assert "catalog_id" not in result.output
    assert "workload_id" not in result.output


def test_doctor_verbose_output_shows_internal_ids_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_doctor_vault_plain.json.gz")
    result = invoke(["--verbose", "doctor", "--profile", "anything"])
    assert "repo_uuid" in result.output
    assert "m7e61v80stMAZgru" in result.output
    assert "repo_root" in result.output
    assert "repo_type=2" in result.output
    # The per-catalog/workload ids reach the plain-text render too.
    assert "catalog_id=" in result.output
    assert "workload_id=" in result.output
    assert "workload_type=" in result.output


def test_doctor_json_output_hides_internal_ids_without_verbose_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    """``--json`` hides the same internal ids human mode does without
    ``--verbose``."""
    patch_profile_store("cli_doctor_vault_plain.json.gz")
    result = invoke(["--json", "doctor", "--profile", "anything"])
    report = json.loads(result.stdout)
    assert report["layout"] == "vault"
    assert "repo_uuid" not in report
    assert "repo_type" not in report
    assert "repo_root" not in report
    assert len(report["catalogs"]) == 2  # vault-plain's real 2 catalogs
    assert all(isinstance(c["display_name"], str) and c["display_name"] for c in report["catalogs"])
    total_workloads = sum(c["workload_count"] for c in report["catalogs"])
    total_versions = sum(c["version_count"] for c in report["catalogs"])
    assert total_workloads == 25
    assert total_versions == 109


def test_doctor_verbose_json_output_is_valid_and_keyed_by_stable_fields_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_doctor_vault_plain.json.gz")
    result = invoke(["--verbose", "--json", "doctor", "--profile", "anything"])
    report = json.loads(result.stdout)
    assert report["layout"] == "vault"
    # repo_uuid/repo_type are per-catalog; a vault's catalogs share one
    # repo_info, so all report the same value.
    assert all(c["repo_uuid"] == "m7e61v80stMAZgru" for c in report["catalogs"])
    assert all(c["repo_type"] == 2 for c in report["catalogs"])
    assert len(report["catalogs"]) == 2  # vault-plain's real 2 catalogs
    total_workloads = sum(c["workload_count"] for c in report["catalogs"])
    total_versions = sum(c["version_count"] for c in report["catalogs"])
    assert total_workloads == 25
    assert total_versions == 109


def test_doctor_marks_saas_sub_types_supported_or_unsupported_correctly_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_doctor_vault_plain.json.gz")
    result = invoke(["--verbose", "--json", "doctor", "--profile", "anything"])
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
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_doctor_vault_plain.json.gz")
    result = invoke(["--json", "doctor", "--profile", "anything"])
    report = json.loads(result.stdout)
    assert report["key"]["status"] == "not_encrypted"
    assert report["key"]["is_encrypted"] is False


def test_doctor_fails_cleanly_for_an_encrypted_repo_given_no_key_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    """An encrypted repository opened with no key is a clean error exit, not
    a partial report, worded in CLI terms (``--key``), never the SDK's
    ``set_key()``."""
    patch_profile_store("cli_doctor_vault_encrypted.json.gz")
    result = invoke(["--json", "doctor", "--profile", "anything"], exit_code=1)
    assert "this repository is encrypted" in result.output
    assert "--key" in result.output
    assert "set_key" not in result.output


def test_doctor_verifies_a_correct_key_for_an_encrypted_repo_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_doctor_vault_encrypted.json.gz")
    result = invoke(["--json", "doctor", "--profile", "anything", "--key", VAULT_ENCRYPTED_KEY_STRING])
    report = json.loads(result.stdout)
    assert report["key"]["status"] == "verified"
    assert report["key"]["is_encrypted"] is True
    assert report["key"]["gcm_ok"] is True
    assert "fingerprint_ok" not in report["key"]
