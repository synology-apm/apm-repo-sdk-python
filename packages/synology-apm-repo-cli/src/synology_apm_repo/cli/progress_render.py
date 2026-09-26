"""``--progress`` rendering:

- Progress always writes to **stderr**, never stdout — ``synology-apm-repo-cli
  cat <ref> > out.bin`` must never see a progress line mixed into the byte
  stream.
- ``auto`` (default): a live, single-line updating bar when stderr is a
  real terminal; a plain text line printed at most every 5 seconds
  otherwise.
- ``always``: force the live bar even when stderr isn't a tty.
- ``never``: fully silent — no progress output of any kind.
- ``--json``: NDJSON progress events on stderr instead of either of the
  above (machine-consumable; the command's actual result still goes to
  stdout, untouched by any of this).

``build_progress_meter`` is the one entry point every CLI command should
call, never ``ProgressMeter()`` directly — CLI and TUI both build on it, so
ETA/rate math is never reimplemented twice.
"""

from __future__ import annotations

import json
import sys
import time

from rich.console import Console

from synology_apm_repo.cli.state import CliState, ProgressMode
from synology_apm_repo.sdk.presentation.format import format_bytes as _format_bytes
from synology_apm_repo.sdk.presentation.format import format_duration as _format_duration
from synology_apm_repo.sdk.presentation.format import format_rate as _format_rate
from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter

_PLAIN_TEXT_INTERVAL = 5.0
_BAR_WIDTH = 24


def build_progress_meter(state: CliState) -> ProgressMeter:
    """The one entry point every CLI command uses to get a ``ProgressMeter``
    that renders according to ``state.progress``/``state.json``. Callers
    pass ``meter.update`` as the SDK-level ``progress`` callback
    (``Session.open``, ``ContentSource.export_to()``, ...); this function
    never needs to know which SDK call it's attached to.

    A plain ``def``: building the meter is pure construction — only the
    *callback* it wraps is ``async def``, since
    ``ProgressMeter.update()`` awaits it."""
    if state.progress is ProgressMode.NEVER:
        return ProgressMeter(callback=None)

    # Built once, not per-render — a fresh Console() re-probes terminal
    # capabilities each time, and on_update() can run up to 10x/second.
    # None when --json is on: NDJSON rendering never touches a Console.
    err_console = None if state.json else Console(stderr=True)

    # Plain-text throttle clock, scoped to this call — never keyed by
    # id(meter) in a module-level dict, since CPython can reuse a
    # garbage-collected meter's id() for an unrelated later one.
    last_plain_emit = [0.0]

    # async def only to satisfy ProgressMeter's callback type; nothing
    # here actually awaits.
    async def on_update(p: Progress) -> None:
        _render(state, err_console, meter, p, last_plain_emit)

    meter = ProgressMeter(callback=on_update)
    return meter


def finish_live_progress(state: CliState) -> None:
    """Clear any live progress line still sitting on stderr, once a
    command's progress-bearing work is done and it's about to print its
    own final report — ``_render_live_line``'s bare ``\\r`` otherwise
    leaves the old line's tail dangling behind a shorter report line
    sharing the same row. A no-op in every mode that never renders a live
    line, so every caller can call this unconditionally."""
    if state.json or state.progress is ProgressMode.NEVER:
        return
    console = Console(stderr=True)
    if state.progress is ProgressMode.ALWAYS or console.is_terminal:
        console.print("\x1b[2K", end="\r", style="dim", highlight=False)


def _render(
    state: CliState, err_console: Console | None, meter: ProgressMeter, p: Progress, last_plain_emit: list[float]
) -> None:
    if state.json:
        _render_ndjson(meter, p)
        return
    assert err_console is not None  # only None when state.json is True, handled above
    live = state.progress is ProgressMode.ALWAYS or err_console.is_terminal
    if live:
        _render_live_line(err_console, meter, p)
    else:
        _render_plain_line(err_console, meter, p, last_plain_emit)


def _render_ndjson(meter: ProgressMeter, p: Progress) -> None:
    payload: dict[str, object] = {"phase": p.phase, "done": p.done, "total": p.total, "unit": p.unit}
    if p.detail:
        payload["detail"] = p.detail
    if p.found is not None:
        payload["found"] = p.found
    payload["rate"] = round(meter.rate, 2)
    eta = meter.eta
    payload["eta"] = eta.total_seconds() if eta is not None else None
    print(json.dumps(payload), file=sys.stderr, flush=True)


def _render_live_line(console: Console, meter: ProgressMeter, p: Progress) -> None:
    # Clear-to-end-of-line (\x1b[2K) so a shorter line doesn't leave stale
    # trailing characters behind. No trailing newline (end="\r") so the
    # next update overwrites this same terminal row.
    console.print("\x1b[2K" + _format_line(meter, p), end="\r", style="dim", highlight=False)


def _render_plain_line(console: Console, meter: ProgressMeter, p: Progress, last_plain_emit: list[float]) -> None:
    now = time.monotonic()
    if now - last_plain_emit[0] < _PLAIN_TEXT_INTERVAL:
        return
    last_plain_emit[0] = now
    console.print(_format_line(meter, p), style="dim", highlight=False)


def _format_line(meter: ProgressMeter, p: Progress) -> str:
    parts = [p.phase]
    if p.determinate and p.total:
        pct = min(100, int(100 * p.done / p.total)) if p.total else 0
        filled = int(_BAR_WIDTH * pct / 100)
        bar = "█" * filled + "░" * (_BAR_WIDTH - filled)
        parts.append(f"│ {bar} {pct:3d}%")
        if p.unit == "bytes":
            parts.append(f"│ {_format_bytes(p.done)}/{_format_bytes(p.total)}")
        else:
            parts.append(f"│ {p.done}/{p.total} {p.unit}")
        rate = meter.rate
        if rate > 0:
            parts.append(f"│ {_format_rate(rate, p.unit)}")
        eta = meter.eta
        if eta is not None:
            parts.append(f"│ ETA {_format_duration(eta.total_seconds())}")
    else:
        found = p.found if p.found is not None else p.done
        parts.append(f"│ found {found} {p.unit}")
        # No ETA here — there's no total yet to divide the remainder by.
        rate = meter.rate
        if rate > 0:
            parts.append(f"│ {_format_rate(rate, p.unit)}")
    parts.append(f"│ elapsed {_format_duration(meter.elapsed.total_seconds())}")
    if p.detail:
        parts.append(f"│ {p.detail}")
    return " ".join(parts)
