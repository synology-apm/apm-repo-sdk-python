"""Unit tests for ``synology_apm_repo.cli.progress_render``.
``build_progress_meter()``-level tests exercise the "never" short-circuit
and the NDJSON payload shape; the live/plain line renderers and
``_format_line()`` are tested directly against a ``rich.Console(...,
force_terminal=...)`` (real terminal detection overridden, no actual tty
needed) and a duck-typed ``_FakeMeter`` (plain, controllable ``rate``/
``eta``/``elapsed`` attributes standing in for a real ``ProgressMeter``'s,
which derives them from real elapsed wall-clock time). The size/duration
formatting helpers this module uses now live in
``synology_apm_repo.sdk.presentation.format`` (shared with the TUI's
``ExportScreen``) and are tested there, not here.
"""

from __future__ import annotations

import datetime
import io
import json
from contextlib import redirect_stderr
from typing import Any, cast

import pytest
from rich.console import Console

from synology_apm_repo.cli.progress_render import (
    _format_line,
    _render,
    _render_live_line,
    _render_plain_line,
    build_progress_meter,
    finish_live_progress,
)
from synology_apm_repo.cli.state import CliState, ProgressMode
from synology_apm_repo.sdk.presentation.progress import Progress


class _FakeMeter:
    """``_format_line``/``_render_live_line``/``_render_plain_line`` only
    ever read ``rate``/``eta``/``elapsed`` off whatever they're handed —
    plain, controllable attributes here stand in for a real
    ``ProgressMeter``'s (which derives them from real elapsed wall-clock
    time, awkward to control deterministically in a test)."""

    def __init__(self, *, rate: float = 0.0, eta: datetime.timedelta | None = None) -> None:
        self.rate = rate
        self.eta = eta
        self.elapsed = datetime.timedelta(seconds=1)


async def test_progress_never_returns_a_meter_with_no_callback() -> None:
    meter = build_progress_meter(CliState(progress=ProgressMode.NEVER))
    # Calling update() must be safe and simply do nothing observable —
    # there is no callback to invoke. ``build_progress_meter`` itself stays
    # synchronous; only ``ProgressMeter.update()`` is a coroutine.
    await meter.update(Progress(phase="reading", determinate=True, done=1, total=10))
    assert meter.latest is not None  # the meter itself still tracks state


async def test_json_mode_emits_ndjson_to_stderr(monkeypatch: pytest.MonkeyPatch) -> None:
    state = CliState(json=True, progress=ProgressMode.AUTO)
    meter = build_progress_meter(state)
    buf = io.StringIO()
    with redirect_stderr(buf):
        await meter.update(
            Progress(phase="reading", determinate=True, done=50, total=100, unit="bytes", detail="x.bin")
        )
    lines = [line for line in buf.getvalue().splitlines() if line]
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["phase"] == "reading"
    assert payload["done"] == 50
    assert payload["total"] == 100
    assert payload["unit"] == "bytes"
    assert payload["detail"] == "x.bin"
    assert "rate" in payload
    assert "eta" in payload


async def test_json_mode_indeterminate_progress_includes_found() -> None:
    state = CliState(json=True)
    meter = build_progress_meter(state)
    buf = io.StringIO()
    with redirect_stderr(buf):
        await meter.update(Progress(phase="discovering", determinate=False, unit="items", found=3))
    payload = json.loads(buf.getvalue().splitlines()[0])
    assert payload["found"] == 3
    assert payload["total"] is None


# -- _format_line -----------------------------------------------------------


def test_format_line_non_bytes_unit_shows_a_plain_count() -> None:
    p = Progress(phase="scanning", determinate=True, done=3, total=10, unit="items")
    line = _format_line(cast(Any, _FakeMeter()), p)
    assert "3/10 items" in line


def test_format_line_includes_rate_when_positive() -> None:
    p = Progress(phase="reading", determinate=True, done=50, total=100, unit="bytes")
    without_rate = _format_line(cast(Any, _FakeMeter(rate=0.0)), p)
    with_rate = _format_line(cast(Any, _FakeMeter(rate=12.5)), p)
    assert with_rate.count("│") == without_rate.count("│") + 1  # exactly one extra "│ ..." segment


def test_format_line_includes_rate_for_indeterminate_progress_too() -> None:
    """``verify_reachable()``'s own per-bucket tick is exactly this shape
    — ``determinate=False`` (the true total isn't known yet) but with a
    real, meaningful ``done``-based rate (``meter.rate`` is computed from
    ``done`` regardless of ``determinate``) — so the rate still belongs on
    the line even though there's no total to compute an ETA against."""
    p = Progress(phase="verifying", determinate=False, done=42, unit="buckets", found=42)
    without_rate = _format_line(cast(Any, _FakeMeter(rate=0.0)), p)
    with_rate = _format_line(cast(Any, _FakeMeter(rate=3.5)), p)
    assert "3.5 buckets/s" in with_rate
    assert "buckets/s" not in without_rate
    assert "ETA" not in with_rate  # no total, so still no ETA even with a real rate


def test_format_line_includes_eta_when_known() -> None:
    p = Progress(phase="reading", determinate=True, done=50, total=100, unit="bytes")
    line = _format_line(cast(Any, _FakeMeter(eta=datetime.timedelta(seconds=30))), p)
    assert "ETA" in line


def test_format_line_includes_detail_when_present() -> None:
    p = Progress(phase="reading", determinate=False, unit="items", found=1, detail="some/path.bin")
    line = _format_line(cast(Any, _FakeMeter()), p)
    assert "some/path.bin" in line


# -- _render_live_line / _render_plain_line / _render -----------------------


def test_render_live_line_writes_a_carriage_return_terminated_line() -> None:
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=True, width=200)
    p = Progress(phase="reading", determinate=True, done=1, total=10, unit="items")
    _render_live_line(console, cast(Any, _FakeMeter()), p)
    assert "\r" in buf.getvalue()


def test_render_plain_line_throttles_rapid_successive_calls() -> None:
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, width=200)
    meter = _FakeMeter()
    p = Progress(phase="reading", determinate=True, done=1, total=10, unit="items")
    last_plain_emit = [0.0]
    _render_plain_line(console, cast(Any, meter), p, last_plain_emit)
    first_output = buf.getvalue()
    assert first_output  # the first call always renders
    _render_plain_line(console, cast(Any, meter), p, last_plain_emit)  # same throttle clock, well within the window
    assert buf.getvalue() == first_output  # nothing more was written


def test_render_picks_live_rendering_on_a_terminal() -> None:
    buf = io.StringIO()
    console = Console(file=buf, stderr=True, force_terminal=True, width=200)
    state = CliState(progress=ProgressMode.AUTO)
    p = Progress(phase="reading", determinate=True, done=1, total=10, unit="items")
    _render(state, console, cast(Any, _FakeMeter()), p, [0.0])
    assert "\r" in buf.getvalue()  # the live-line renderer's own signature (carriage return, no trailing newline)


# -- finish_live_progress ----------------------------------------------------


def test_finish_live_progress_is_a_noop_under_json() -> None:
    buf = io.StringIO()
    with redirect_stderr(buf):
        finish_live_progress(CliState(json=True, progress=ProgressMode.ALWAYS))
    assert buf.getvalue() == ""


def test_finish_live_progress_is_a_noop_when_progress_is_never() -> None:
    buf = io.StringIO()
    with redirect_stderr(buf):
        finish_live_progress(CliState(progress=ProgressMode.NEVER))
    assert buf.getvalue() == ""


def test_finish_live_progress_is_a_noop_on_a_non_terminal_stderr_under_auto() -> None:
    # redirect_stderr's StringIO is never a real terminal -- AUTO only
    # forces the clear when ALWAYS says so or the real stderr is a tty.
    buf = io.StringIO()
    with redirect_stderr(buf):
        finish_live_progress(CliState(progress=ProgressMode.AUTO))
    assert buf.getvalue() == ""


def test_finish_live_progress_clears_the_line_when_always_forces_it() -> None:
    """``--progress always`` forces the clear regardless of whether
    stderr is a real terminal -- the same "always forces live" rule
    ``_render``'s own ``live`` check uses."""
    buf = io.StringIO()
    with redirect_stderr(buf):
        finish_live_progress(CliState(progress=ProgressMode.ALWAYS))
    output = buf.getvalue()
    assert "\x1b[2K" in output  # clear-to-end-of-line
    assert output.endswith("\r")  # no trailing newline, matching _render_live_line's own convention


__all__: list[str] = []
