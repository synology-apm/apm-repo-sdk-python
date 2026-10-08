"""Regression tests for ``synology-apm-repo-cli --trace``.

Fixture: ``cli_trace_vault_plain.json.gz``, recorded against
``vault-plain/@ActiveProtectVault``.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from support.cli import invoke


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_trace_vault_plain.json.gz")


def test_trace_json_lines_on_stderr_describe_real_object_store_calls_replayed() -> None:
    result = invoke(["--trace", "--json", "ls", "#", "--profile", "anything"])

    rows = json.loads(result.stdout)
    assert len(rows) == 2  # vault-plain's real 2 connections -- this test is about trace shape, not names

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
    result = invoke(["--trace", "ls", "#", "--profile", "anything"])
    assert result.stdout.strip()  # ls's own normal output still reaches stdout
    assert "[trace]" not in result.stdout
    assert "[trace]" in result.stderr


def test_without_trace_no_trace_output_at_all_replayed() -> None:
    result = invoke(["ls", "#", "--profile", "anything"])
    assert "trace" not in result.stderr.lower()
