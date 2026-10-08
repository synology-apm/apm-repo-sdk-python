"""Unit tests for ``synology_apm_repo.cli.commands.key``'s handling of a
``SetKeyResult`` with ``reopen_errors`` (the key verified but reopening a
sibling catalog failed): shown as a warning alongside the accepted key."""

from __future__ import annotations

from pathlib import Path

import pytest

from support.cli import invoke
from support.fakes import faithful_to
from synology_apm_repo.sdk.api import KeyStatus, KeyVerification, Repository, SetKeyResult
from synology_apm_repo.sdk.errors import ApmRepoError
from unit.cli.session_fakes import install_fake_session

_GOOD_VERIFICATION = KeyVerification(gcm_ok=True, vault_key=b"vault-key-bytes")


@faithful_to(Repository)
class _FakeRepo:
    def __init__(self) -> None:
        self.key_verification = _GOOD_VERIFICATION
        self.key_status = KeyStatus.VERIFIED

    async def set_key(self, key_string: str) -> SetKeyResult:
        return SetKeyResult(_GOOD_VERIFICATION, (ApmRepoError("boom"),))


@pytest.fixture(autouse=True)
def _fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_session(monkeypatch, lambda: [_FakeRepo()])


def test_partial_reopen_failure_reports_the_verified_key_and_warns_instead_of_failing(tmp_path: Path) -> None:
    result = invoke(["key", str(tmp_path), "--key", "userKeyID@dGVzdA=="])
    # The warning goes to stderr, so a script reading stdout sees only the verified key.
    assert result.stdout == "verified\n  gcm_ok=True\n"
    assert " ".join(result.stderr.split()) == (
        "warning: the key verified, but 1 open catalog could not be switched to it: boom"
    )
