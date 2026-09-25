"""Size/duration/count formatting shared by the CLI and the TUI. Both
surfaces must render these identically, so they live in the SDK rather
than in ``cli/progress_render.py`` or ``browser/format.py``: a per-surface
copy would let them silently drift apart."""

from __future__ import annotations

from datetime import datetime


def pluralize(count: int, singular: str, plural: str | None = None) -> str:
    """``singular`` for ``count == 1``, else ``plural`` (default:
    ``singular`` with an "s" appended) — the English-pluralization shape
    every status line reporting a count already needed on its own."""
    if count == 1:
        return singular
    return plural if plural is not None else f"{singular}s"


def format_bytes(n: int) -> str:
    value = float(n)
    unit = "B"
    for candidate in ("KiB", "MiB", "GiB", "TiB"):
        if value < 1024:
            break
        value /= 1024
        unit = candidate
    return f"{int(value)} {unit}" if unit == "B" else f"{value:.1f} {unit}"


def format_duration(seconds: float) -> str:
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def format_timestamp(dt: datetime) -> str:
    """Renders an already timezone-aware ``datetime`` (a ``Node``'s own
    ``node_modified_time()``, a version's own backup epoch, ...) in the
    machine's local timezone — shared by ``catalog/version.py``'s own
    ``_version_display_name()``, so a Modified column and a version's own
    display name read consistently."""
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def format_rate(rate: float, unit: str) -> str:
    """Format a ``ProgressMeter.rate`` value for display: ``"12.3 MiB/s"``
    for the byte-denominated ``"bytes"`` unit, ``"4.0 <unit>/s"`` for any
    other progress unit. Callers should only invoke this once ``rate > 0``
    — a zero/negative rate isn't a meaningful display value and both call
    sites already gate on it."""
    return f"{format_bytes(int(rate))}/s" if unit == "bytes" else f"{rate:.1f} {unit}/s"
