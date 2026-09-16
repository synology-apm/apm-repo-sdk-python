"""``--trace`` rendering: one line per ``ObjectStore`` call,
to stderr — same "never touch stdout" rule ``progress_render.py`` follows,
for the same reason.

``--json``: NDJSON events on stderr (one object per call), matching
``progress_render.py``'s own ``--json`` shape so a script consuming one
already knows the pattern for the other. Otherwise: a single
human-readable line per call.

``build_trace_callback`` is the one entry point every CLI command
should call — mirrors ``progress_render.py``'s ``build_progress_meter``
so both diagnostic axes are wired into every command the same way.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable

from rich.console import Console

from synology_apm_repo.cli.state import CliState
from synology_apm_repo.sdk.api import TraceEvent


def build_trace_callback(state: CliState) -> Callable[[TraceEvent], None] | None:
    """Returns ``None`` when ``--trace`` wasn't given — callers pass this
    straight through as ``Session.open()``/``discover()``'s ``trace=``
    kwarg either way, so no command needs its own "is tracing on" branch."""
    if not state.trace:
        return None

    err_console = Console(stderr=True)

    def on_event(event: TraceEvent) -> None:
        if state.json:
            _render_ndjson(event)
        else:
            _render_line(err_console, event)

    return on_event


def _render_ndjson(event: TraceEvent) -> None:
    payload = {
        "method": event.method,
        "path": event.path,
        "offset": event.offset,
        "length": event.length,
        "result_length": event.result_length,
        "elapsed": round(event.elapsed, 6),
    }
    print(json.dumps(payload), file=sys.stderr, flush=True)


def _render_line(console: Console, event: TraceEvent) -> None:
    parts = [f"[dim]\\[trace][/dim] {event.method:<8} {event.path}"]
    if event.method == "read":
        parts.append(f"offset={event.offset} length={event.length}")
    if event.result_length is not None:
        parts.append(f"-> {event.result_length}")
    parts.append(f"({event.elapsed * 1000:.2f}ms)")
    console.print(" ".join(parts), style="dim", highlight=False)
