"""``synology-apm-repo-cli verify``'s progress wiring: the meter it passes to
``Repository.verify(level, progress=...)`` renders on stderr like every
other command's progress. ``test_cli_commands_verify.py`` covers the report itself."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from support.cli import invoke
from support.fakes import faithful_to
from synology_apm_repo.sdk.api import Finding, Repository, VerifyLevel
from synology_apm_repo.sdk.presentation import Progress, ProgressCallback
from unit.cli.session_fakes import install_fake_session


@faithful_to(Repository)
class _ProgressReportingRepo:
    async def verify(
        self, level: VerifyLevel = VerifyLevel.QUICK, *, progress: ProgressCallback | None = None
    ) -> list[Finding]:
        assert progress is not None  # commands/verify.py must always pass one
        await progress(Progress(phase="verifying", determinate=True, done=0, total=1, unit="items"))
        return []


@pytest.fixture(autouse=True)
def _fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_session(monkeypatch, [_ProgressReportingRepo()])


def test_json_full_level_emits_a_verifying_phase_ndjson_line(tmp_path: Path) -> None:
    result = invoke(["--json", "verify", str(tmp_path), "--level", "full"])
    verifying_lines = [json.loads(line) for line in result.stderr.splitlines() if '"phase": "verifying"' in line]
    assert verifying_lines
    assert verifying_lines[0]["total"] == 1


def test_progress_always_renders_a_live_verifying_line_on_stderr(tmp_path: Path) -> None:
    result = invoke(["--progress", "always", "verify", str(tmp_path), "--level", "full"])
    assert "verifying" in result.stderr


def test_zero_findings_renders_clean_in_human_output(tmp_path: Path) -> None:
    result = invoke(["verify", str(tmp_path), "--level", "quick"])
    assert "clean" in result.output
    assert "level=quick" in result.output
