"""Unit tests for ``synology_apm_repo.cli.commands.key``'s ``ExceptionGroup``
handling -- ``Repository.set_key()`` raises this only when the key itself
verified fine but reopening/closing an already-opened sibling catalog
failed independently; this must be reported as a warning and still show
the accepted key, not routed to ``typer_async``'s ``fail_unexpected()`` as
an unhandled bug -- the same posture the browser's ``KeyDialog._verify``
already takes for the same exception shape."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.api import KeyStatus, KeyVerification
from synology_apm_repo.sdk.errors import ApmRepoError

runner = CliRunner()

_GOOD_VERIFICATION = KeyVerification(gcm_ok=True, vault_key=b"vault-key-bytes")


class _FakeRepo:
    def __init__(self) -> None:
        # set_key() records key_status/key_verification before raising the
        # ExceptionGroup, so a caller sees the true, already-decided key
        # status even when a partial catalog reopen/close failure is
        # reported alongside it.
        self.key_verification = _GOOD_VERIFICATION
        self.key_status = KeyStatus.VERIFIED

    async def set_key(self, key_string: str) -> KeyVerification:
        raise ExceptionGroup("Repository.set_key() failed to fully switch every open catalog", [ApmRepoError("boom")])


class _FakeSession:
    def __init__(self) -> None:
        pass

    async def open(self, *args: object, **kwargs: object) -> list[object]:
        return [_FakeRepo()]

    async def close(self) -> None:
        pass


@pytest.fixture(autouse=True)
def _fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    # key.py doesn't import Session itself -- Session() lives inside
    # cli.repo_session.opened_repo(), so that's the module this patches.
    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _FakeSession)


def test_partial_reopen_failure_still_reports_the_verified_key(tmp_path: Path) -> None:
    result = runner.invoke(app, ["key", str(tmp_path), "--key", "userKeyID@dGVzdA=="])
    assert result.exit_code == 0, result.output
    assert "verified" in result.output
    assert "gcm_ok=True" in result.output


def test_partial_reopen_failure_warns_instead_of_failing(tmp_path: Path) -> None:
    result = runner.invoke(app, ["key", str(tmp_path), "--key", "userKeyID@dGVzdA=="])
    assert result.exit_code == 0, result.output
    assert "warning" in result.output.lower()
    assert "internal error" not in result.output.lower()


__all__: list[str] = []
