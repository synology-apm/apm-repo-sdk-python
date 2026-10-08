"""Tests for scripts/analyze_store_trace.py on a small synthetic trace."""

from __future__ import annotations

import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from support.modules import load_module

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "analyze_store_trace.py"


@pytest.fixture
def analyze() -> ModuleType:
    return load_module("analyze_store_trace", _SCRIPT_PATH)


def _event(sample: str, method: str, path: str, *, step: str | None = None, elapsed: float = 0.5) -> dict[str, Any]:
    event: dict[str, Any] = {
        "sample": sample,
        "method": method,
        "path": path,
        "offset": 0,
        "length": None,
        "result_length": None,
        "elapsed": elapsed,
    }
    if step is not None:
        event["step"] = step
    return event


_DB = "@ActiveProtectData/RepoA/db"


def test_normalize_collapses_ids_and_suffixes(analyze: ModuleType) -> None:
    assert analyze.normalize_path("@ActiveProtectData/RepoA/db/file_map.12-wal") == (
        "@ActiveProtectData/<repo>/db/file_map.N-wal"
    )
    assert analyze.normalize_path("@ActiveProtectKey/userKey/abc") == "@ActiveProtectKey/userKey/<id>"
    assert analyze.normalize_path("x/@data/Pool/106/11/11952.buk.248") == "x/@data/Pool/*/*/*.buk"


def test_repeats_are_counted_once_per_extra_call(analyze: ModuleType) -> None:
    events = [
        _event("s1", "listdir", _DB, step="catalog.a"),
        _event("s1", "listdir", _DB, step="catalog.b"),
        _event("s1", "listdir", _DB, step="saas.c"),
        _event("s1", "exists", _DB),
        _event("s2", "listdir", _DB),
    ]
    rows = analyze.repeats_by_kind(events)
    assert rows == [("listdir", "@ActiveProtectData/<repo>/db", 2, 4, 1.0)]
    assert analyze.per_sample(events)["s1"] == {"calls": 4, "store_time_s": 2.0, "extra_calls": 2}


def test_repeats_are_attributed_to_the_issuing_step_with_an_untagged_fallback(analyze: ModuleType) -> None:
    events = [
        _event("s1", "listdir", _DB, step="catalog.a"),
        _event("s1", "listdir", _DB, step="catalog.b"),
        _event("s1", "listdir", _DB),
    ]
    assert analyze.repeats_by_step(events) == [("catalog.b", 1), ("(untagged)", 1)]


def test_render_and_main_read_a_jsonl_file(
    analyze: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    trace = tmp_path / "store_trace.jsonl"
    trace.write_text(
        "\n".join(json.dumps(e) for e in [_event("s1", "listdir", _DB), _event("s1", "listdir", _DB)]) + "\n"
    )
    assert analyze.main([str(trace)]) == 0
    out = capsys.readouterr().out
    assert "calls: 2" in out and "repeated calls: 1 (50.0%)" in out and "s1" in out
