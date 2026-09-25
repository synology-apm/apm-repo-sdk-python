"""Regression test for ``synology-apm-repo-cli --trace`` — replayed from a committed
fixture recorded against real bytes, with **no external dependency**:
same ``patch_profile_store`` fixture (``tests/integration/cli/conftest.py``)
every sibling in this directory uses.

Fixture: ``cli_trace_apv1_vault.json.gz``, this file's own dedicated
recording against ``apv-sample-1/@ActiveProtectVault`` — every test here
only ever does a bare ``ls #`` (with ``--profile`` supplying the store),
so any one of them is this fixture's recording recipe.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from types import ModuleType

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.repo_session as repo_session_mod
from synology_apm_repo.cli.main import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[[str, ModuleType], None]) -> None:
    patch_profile_store("cli_trace_apv1_vault.json.gz", repo_session_mod)


def test_trace_json_lines_on_stderr_describe_real_object_store_calls_replayed() -> None:
    result = runner.invoke(app, ["--trace", "--json", "ls", "#", "--profile", "anything"])
    assert result.exit_code == 0, result.output

    rows = json.loads(result.stdout)
    assert len(rows) == 2  # apv-sample-1's real 2 connections -- this test is about trace shape, not names

    all_lines = [line for line in result.stderr.splitlines() if line]
    events = [json.loads(line) for line in all_lines]
    trace_events = [e for e in events if "method" in e]
    assert trace_events, "expected at least one trace event on stderr"
    methods = {e["method"] for e in trace_events}
    assert methods <= {"read", "size", "exists", "listdir"}
    assert "read" in methods

    read_events = [e for e in trace_events if e["method"] == "read"]
    assert any("repo_info" in e["path"] or "db" in e["path"] for e in read_events)
    assert all(e["elapsed"] >= 0 for e in trace_events)


def test_trace_human_lines_appear_on_stderr_only_replayed() -> None:
    result = runner.invoke(app, ["--trace", "ls", "#", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert result.stdout.strip()  # ls's own normal output still reaches stdout
    assert "[trace]" not in result.stdout
    assert "[trace]" in result.stderr


def test_without_trace_no_trace_output_at_all_replayed() -> None:
    result = runner.invoke(app, ["ls", "#", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert "trace" not in result.stderr.lower()


__all__: list[str] = []
