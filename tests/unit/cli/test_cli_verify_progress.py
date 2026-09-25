"""Unit test for ``synology-apm-repo-cli verify``'s own progress wiring —
that it passes ``build_progress_meter(state).update`` into
``Repository.verify(level, progress=...)`` and that meter's rendering
reaches stderr the same way every other command's discovery progress
already does. Distinct from ``test_cli_verify.py``, which covers
rendering, not progress wiring.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.api import Finding
from synology_apm_repo.sdk.presentation.progress import Progress

runner = CliRunner()


@pytest.fixture(autouse=True)
def _fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeRepo:
        async def verify(
            self, level: object, *, progress: Callable[[Progress], Awaitable[None]] | None = None, **kwargs: object
        ) -> list[Finding]:
            assert progress is not None  # commands/verify.py must always pass one
            await progress(Progress(phase="verifying", determinate=True, done=0, total=1, unit="items"))
            return []

    class _FakeSession:
        def __init__(self) -> None:
            pass

        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _FakeSession)


def test_json_full_level_emits_a_verifying_phase_ndjson_line(tmp_path: Path) -> None:
    result = runner.invoke(app, ["--json", "verify", str(tmp_path), "--level", "full"])
    assert result.exit_code == 0, result.output
    verifying_lines = [json.loads(line) for line in result.stderr.splitlines() if '"phase": "verifying"' in line]
    assert verifying_lines
    assert verifying_lines[0]["total"] == 1


def test_progress_always_renders_a_live_verifying_line_on_stderr(tmp_path: Path) -> None:
    result = runner.invoke(app, ["--progress", "always", "verify", str(tmp_path), "--level", "full"])
    assert result.exit_code == 0, result.output
    assert "verifying" in result.stderr


def test_zero_findings_renders_clean_in_human_output(tmp_path: Path) -> None:
    """The only place this asserts on ``_render_human``'s "clean" branch
    at all: ``test_cli_verify.py``'s own fixtures always use a
    fabricated non-empty Finding, and the real fixture-backed
    integration coverage (``tests/integration/cli/test_cli_verify.py``)
    is deliberately non-clean now (``sample-1``'s own known
    retention-gap findings) -- this fake repository's ``verify()``
    is the only scenario left that actually returns ``[]``."""
    result = runner.invoke(app, ["verify", str(tmp_path), "--level", "quick"])
    assert result.exit_code == 0, result.output
    assert "clean" in result.output
    assert "level=quick" in result.output


__all__: list[str] = []
