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

Commands get a meter from ``build_progress_meter``, never ``ProgressMeter()``
directly.
"""

from __future__ import annotations

import json
import sys
import time

from rich.console import Console

from synology_apm_repo.cli.consoles import err_console
from synology_apm_repo.cli.state import CliState, ProgressMode
from synology_apm_repo.sdk.presentation import Progress, ProgressMeter
from synology_apm_repo.sdk.presentation import format_bytes as _format_bytes

_PLAIN_TEXT_INTERVAL = 5.0
_BAR_WIDTH = 24


def build_progress_meter(state: CliState) -> ProgressMeter:
    """A ``ProgressMeter`` that renders according to
    ``state.progress``/``state.json``; callers pass ``meter.update`` as an
    SDK ``progress`` callback."""
    if state.progress is ProgressMode.NEVER:
        return ProgressMeter(callback=None)

    # None under --json, which never touches a Console.
    progress_console = None if state.json else err_console

    # Plain-text throttle clock, scoped to this meter.
    last_plain_emit = [0.0]

    # async only to satisfy ProgressMeter's callback type.
    async def on_update(p: Progress) -> None:
        _render(state, progress_console, meter, p, last_plain_emit)

    meter = ProgressMeter(callback=on_update)
    return meter


def finish_live_progress(state: CliState) -> None:
    """Clear any live progress line left on stderr before a command prints
    its final report, which would otherwise share that row. A no-op in
    every mode that renders no live line."""
    if state.json or state.progress is ProgressMode.NEVER:
        return
    if state.progress is ProgressMode.ALWAYS or err_console.is_terminal:
        err_console.print("\x1b[2K", end="\r", style="dim", highlight=False)


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
    print(json.dumps(payload), file=sys.stderr, flush=True)  # noqa: T201 - one raw NDJSON line, past Rich


def _render_live_line(console: Console, meter: ProgressMeter, p: Progress) -> None:
    # \x1b[2K clears the previous, possibly longer line; end="\r" lets the
    # next update overwrite this row.
    console.print("\x1b[2K" + _format_line(meter, p), end="\r", style="dim", highlight=False)


def _render_plain_line(console: Console, meter: ProgressMeter, p: Progress, last_plain_emit: list[float]) -> None:
    now = time.monotonic()
    if now - last_plain_emit[0] < _PLAIN_TEXT_INTERVAL:
        return
    last_plain_emit[0] = now
    console.print(_format_line(meter, p), style="dim", highlight=False)


def _format_line(meter: ProgressMeter, p: Progress) -> str:
    text = meter.formatted(p.unit)
    parts: list[str] = [p.phase]
    if p.determinate and p.total:
        pct = min(100, int(100 * p.done / p.total))
        filled = int(_BAR_WIDTH * pct / 100)
        bar = "█" * filled + "░" * (_BAR_WIDTH - filled)
        parts.append(f"│ {bar} {pct:3d}%")
        if p.unit == "bytes":
            parts.append(f"│ {_format_bytes(p.done)}/{_format_bytes(p.total)}")
        else:
            parts.append(f"│ {p.done}/{p.total} {p.unit}")
        if text.rate:
            parts.append(f"│ {text.rate}")
        if text.eta:
            parts.append(f"│ ETA {text.eta}")
    else:
        found = p.found if p.found is not None else p.done
        parts.append(f"│ found {found} {p.unit}")
        if text.rate:
            parts.append(f"│ {text.rate}")
    parts.append(f"│ elapsed {text.elapsed}")
    if p.detail:
        parts.append(f"│ {p.detail}")
    return " ".join(parts)
