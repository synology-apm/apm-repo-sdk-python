"""Run the ``synology-apm-repo-cli`` app in-process for a test."""

from __future__ import annotations

from collections.abc import Sequence

from typer.testing import CliRunner, Result

from synology_apm_repo.cli.main import app

_RUNNER = CliRunner()


def invoke(args: Sequence[str], *, input: str | None = None, exit_code: int | None = 0) -> Result:  # noqa: A002 - CliRunner's own name
    """``synology-apm-repo-cli <args>``; fails the test unless it exits with ``exit_code``
    (``None`` accepts any), showing its output."""
    result = _RUNNER.invoke(app, list(args), input=input)
    if exit_code is not None:
        assert result.exit_code == exit_code, f"exit {result.exit_code} != {exit_code}\n{result.output}"
    return result
