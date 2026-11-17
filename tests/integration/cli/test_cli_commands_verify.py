"""Regression tests for ``synology-apm-repo-cli verify``.

Fixture: ``cli_verify_vault_plain_clean.json.gz``, recorded against
``vault-plain``.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from support.cli import invoke
from support.recording.sample_constants import VAULT_PLAIN_FS_VERSION_UID
from synology_apm_repo.sdk.catalog.version import Version, versions
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.units import verify_reachable


def test_clean_repo_reports_no_findings_replayed(
    patch_profile_store: Callable[..., None], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean FS version reports zero findings at QUICK, which reads no
    chunk content (so no ``allow_content=True``). Verify walks only that
    one version's compositions and buckets: every version's catalog row is
    still listed, then all but ``VAULT_PLAIN_FS_VERSION_UID`` are dropped."""
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
