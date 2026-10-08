"""Regression tests for ``synology-apm-repo-cli verify``.

Fixture: ``cli_verify_objstore_m365_encrypted.json.gz``, recorded against
``objstore-m365-encrypted``, chosen for its size: ``VerifyLevel.FULL``
decodes every reachable chunk, so a FULL recording grows with the
repository's reachable data. Every test sharing it passes
``allow_content=True`` because FULL decodes chunks as a CRC/fingerprint
oracle.

objstore-m365-encrypted is not clean: 2 GWS versions on one stream have no
``version_info`` row in their ``saas_snapshot``, a genuine gap no later
generation can stand in for, so both levels report exactly these 2
findings and nothing else.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from support.cli import invoke
from support.recording.sample_constants import OBJSTORE_M365_ENCRYPTED_KEY_STRING, VAULT_PLAIN_FS_VERSION_UID
from synology_apm_repo.sdk.catalog.version import Version, versions
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.units import verify_reachable

#: objstore-m365-encrypted's genuine-gap findings, asserted by shape only: each
#: ``path``/``detail`` carries an anonymized display name.
_EXPECTED_FINDING_COUNT = 2

#: Those findings share one ``ref`` (one stream), so verify's human output
#: groups them under one header.
_EXPECTED_TEMPLATE_COUNT = 1

#: Part of ``verify_reachable.py``'s ``_SAAS_GENUINE_GAP_SUFFIX``.
_SAAS_GENUINE_GAP_TEXT = "genuine SaaS resolution gap"


def _assert_expected_genuine_gap_findings(findings: list[dict[str, str]]) -> None:
    assert len(findings) == _EXPECTED_FINDING_COUNT
    assert all(f["stage"] == "Version" for f in findings)
    assert all(f["symptom"] == "DataMissing" for f in findings)
    assert all(_SAAS_GENUINE_GAP_TEXT in f["detail"] for f in findings)


def test_quick_level_reports_known_genuine_gap_findings_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_verify_objstore_m365_encrypted.json.gz", allow_content=True)
    result = invoke(
        ["--json", "verify", "--profile", "anything", "--key", OBJSTORE_M365_ENCRYPTED_KEY_STRING], exit_code=3
    )
    report = json.loads(result.stdout)
    assert report["problem_count"] == _EXPECTED_FINDING_COUNT
    _assert_expected_genuine_gap_findings(report["findings"])


def test_full_level_flag_replayed(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_verify_objstore_m365_encrypted.json.gz", allow_content=True)
    result = invoke(
        ["--json", "verify", "--profile", "anything", "--key", OBJSTORE_M365_ENCRYPTED_KEY_STRING, "--level", "full"],
        exit_code=3,
    )
    report = json.loads(result.stdout)
    assert report["problem_count"] == _EXPECTED_FINDING_COUNT
    _assert_expected_genuine_gap_findings(report["findings"])


def test_human_output_reports_the_same_findings_replayed(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_verify_objstore_m365_encrypted.json.gz", allow_content=True)
    json_result = invoke(
        ["--json", "verify", "--profile", "anything", "--key", OBJSTORE_M365_ENCRYPTED_KEY_STRING], exit_code=3
    )
    findings = json.loads(json_result.stdout)["findings"]
    _assert_expected_genuine_gap_findings(findings)

    result = invoke(["verify", "--profile", "anything", "--key", OBJSTORE_M365_ENCRYPTED_KEY_STRING], exit_code=3)
    assert "level=quick" in result.output
    # Rich wraps at CliRunner's terminal width; unwrap before searching.
    unwrapped = result.output.replace("\n", "")
    assert unwrapped.count(_SAAS_GENUINE_GAP_TEXT) == _EXPECTED_TEMPLATE_COUNT
    assert unwrapped.count("versions)") == _EXPECTED_TEMPLATE_COUNT
    # Grouping collapses the shared sentence, never a finding's own path.
    for finding in findings:
        assert finding["path"] in unwrapped


def test_clean_repo_reports_no_findings_replayed(
    patch_profile_store: Callable[..., None], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean real FS version reports zero findings at QUICK, which reads
    no chunk content (so no ``allow_content=True``). Verify walks only that
    one version's compositions and buckets: every version's catalog row is
    still listed, then all but ``VAULT_PLAIN_FS_VERSION_UID`` are dropped.

    Fixture: ``cli_verify_vault_plain_clean.json.gz``, recorded against
    ``vault-plain``."""
    walked: list[Version] = []

    async def only_the_fs_version(
        repo: DedupRepo, workload: Workload, *, include_deleted: bool = False
    ) -> list[Version]:
        listed = await versions(repo, workload, include_deleted=include_deleted)
        kept = [version for version in listed if version.version_uid == VAULT_PLAIN_FS_VERSION_UID]
        walked.extend(kept)
        return kept

    monkeypatch.setattr(verify_reachable, "versions", only_the_fs_version)
    patch_profile_store("cli_verify_vault_plain_clean.json.gz")
    result = invoke(["--json", "verify", "--profile", "anything"])
    assert json.loads(result.stdout) == {"level": "quick", "problem_count": 0, "findings": []}
    assert [version.version_uid for version in walked] == [VAULT_PLAIN_FS_VERSION_UID]
