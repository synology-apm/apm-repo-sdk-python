"""Unit tests for ``synology_apm_repo.cli.trace_render``: the "off"
short-circuit and the NDJSON payload shape are the parts that don't depend
on a real terminal, so those are what's tested here.
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stderr

from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.trace_render import build_trace_callback
from synology_apm_repo.sdk.api import TraceEvent


def test_trace_off_by_default_returns_none() -> None:
    assert build_trace_callback(CliState()) is None


def test_json_mode_emits_ndjson_to_stderr() -> None:
    state = CliState(trace=True, json=True)
    callback = build_trace_callback(state)
    assert callback is not None
    buf = io.StringIO()
    with redirect_stderr(buf):
        callback(
            TraceEvent(method="read", path="db/file_map", offset=64, length=256, result_length=256, elapsed=0.0005)
        )
    lines = [line for line in buf.getvalue().splitlines() if line]
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["method"] == "read"
    assert payload["path"] == "db/file_map"
    assert payload["offset"] == 64
    assert payload["length"] == 256
    assert payload["result_length"] == 256
    assert payload["elapsed"] == 0.0005


def test_human_mode_does_not_touch_stdout() -> None:
    import contextlib

    state = CliState(trace=True, json=False)
    callback = build_trace_callback(state)
    assert callback is not None
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), redirect_stderr(err):
        callback(TraceEvent(method="listdir", path="", result_length=3, elapsed=0.0002))
    assert out.getvalue() == ""
    assert "listdir" in err.getvalue()


__all__: list[str] = []
