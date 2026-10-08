"""``--trace`` rendering: one stderr line per ``ObjectStore`` call, or
one NDJSON event per call under ``--json`` (``progress_render.py``'s
stdout/stderr rule). ``repo_session.cli_session`` wires
``build_trace_callback`` into every session the CLI opens, and ``dump``
into the store it reads directly.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable

from rich.console import Console

from synology_apm_repo.cli.consoles import err_console
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.sdk import TraceEvent


def build_trace_callback(state: CliState) -> Callable[[TraceEvent], None] | None:
    """The ``trace=`` callback for ``Session.open()``, or
    ``None`` when ``--trace`` wasn't given."""
    if not state.trace:
        return None

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
        "error": event.error,
    }
    print(json.dumps(payload), file=sys.stderr, flush=True)  # noqa: T201 - one raw NDJSON line, past Rich


def _render_line(console: Console, event: TraceEvent) -> None:
    parts = [f"[dim]\\[trace][/dim] {event.method:<8} {event.path}"]
    if event.method == "read":
        parts.append(f"offset={event.offset} length={event.length}")
    if event.error is not None:
        parts.append(f"!! {event.error}")
    elif event.result_length is not None:
        parts.append(f"-> {event.result_length}")
    parts.append(f"({event.elapsed * 1000:.2f}ms)")
    console.print(" ".join(parts), style="dim", highlight=False)
