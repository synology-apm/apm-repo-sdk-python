"""Size, duration, rate, timestamp and count formatting shared by every
frontend."""

from __future__ import annotations

from datetime import datetime


def pluralize(count: int, singular: str, plural: str | None = None) -> str:
    """``singular`` for ``count == 1``, else ``plural`` (default:
    ``singular`` with an "s" appended)."""
    if count == 1:
        return singular
    return plural if plural is not None else f"{singular}s"


def format_bytes(n: int) -> str:
    """``n`` bytes in binary units: ``"512 B"``, ``"1.5 MiB"``, up to TiB."""
    value = float(n)
    unit = "B"
    for candidate in ("KiB", "MiB", "GiB", "TiB"):
        if value < 1024:
            break
        value /= 1024
        unit = candidate
    return f"{int(value)} {unit}" if unit == "B" else f"{value:.1f} {unit}"


def format_duration(seconds: float) -> str:
    """``seconds`` as ``MM:SS``, or ``HH:MM:SS`` from one hour up."""
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def format_timestamp(dt: datetime) -> str:
    """A timezone-aware ``datetime`` as ``"YYYY-MM-DD HH:MM:SS"`` in the
    machine's local timezone; ``Version.display_name`` uses the same
    format."""
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def format_rate(rate: float, unit: str) -> str:
    """A ``ProgressMeter.rate`` value for display: ``"12.3 MiB/s"`` for the
    ``"bytes"`` unit, ``"4.0 <unit>/s"`` for any other. Meaningful only
    for ``rate > 0``."""
    return f"{format_bytes(int(rate))}/s" if unit == "bytes" else f"{rate:.1f} {unit}/s"
