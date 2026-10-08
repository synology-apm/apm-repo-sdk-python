"""``Progress``/``ProgressMeter``: the progress contract every frontend
builds on, so rate/ETA math is computed once.

1. ``total`` is the planned work the caller passes in (a sparse image's
   planned chunks, not its logical size).
2. Rate is a windowed average over the last ``rate_window_seconds``. ETA
   stays unknown through a warm-up window and is quantized so it doesn't
   jitter.
3. Rate samples are taken at most every ``rate_sample_interval``, however
   often ``ProgressMeter.update`` is called, which bounds the sample deque.
4. ``ProgressMeter.update`` is cheap to call per chunk; it forwards to the
   wrapped callback only every ``min_interval`` seconds or ``min_delta``
   units, whichever comes first.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from .format import format_duration, format_rate

ProgressPhase = Literal["discovering", "reading", "verifying"]
ProgressUnit = Literal["bytes", "items", "buckets"]


@dataclass(frozen=True, slots=True)
class Progress:
    """One snapshot of a long-running operation's state.

    Attributes:
        phase: What the operation is doing.
        determinate: Whether ``total`` is meaningful.
        done: How many ``unit``\\ s are finished.
        total: ``None`` when the total is unknown.
        unit: What ``done``/``total`` count.
        detail: The file/object currently being processed, for UI display.
        found: Incremental result count when ``total`` is unknown.
    """

    phase: ProgressPhase
    determinate: bool
    done: int = 0
    total: int | None = None
    unit: ProgressUnit = "bytes"
    detail: str = ""
    found: int | None = None


type ProgressCallback = Callable[[Progress], Awaitable[None]]
"""Receives each ``Progress`` snapshot of a long-running operation."""


@dataclass(frozen=True, slots=True)
class FormattedProgress:
    """A ``ProgressMeter``'s rate, ETA and elapsed time as display text.

    Attributes:
        rate: ``""`` until a rate has been measured.
        eta: ``""`` while unknown (no total, or still warming up).
        elapsed: Always set.
    """

    rate: str
    eta: str
    elapsed: str


class ProgressMeter:
    """Smooths raw ``Progress`` snapshots into a stable rate/ETA (rules
    2-3) and rate-limits how often the wrapped callback actually fires
    (rule 4). Construct one per operation; call ``update`` as often as
    convenient (every chunk is fine).

    Args:
        callback: Awaited with a ``Progress`` when the throttle fires;
            ``None`` still tracks rate/ETA without notifying.
        min_interval: Minimum seconds between callback invocations.
        min_delta: Also notify once ``done`` has advanced this much since
            the last notification; ``0`` disables the delta trigger.
        rate_window_seconds: Span of the windowed rate average.
        rate_sample_interval: Minimum seconds between rate samples.
        eta_warmup_seconds: ETA stays unknown until this much time has
            elapsed or ``eta_warmup_fraction`` of the total is done.
        eta_warmup_fraction: See ``eta_warmup_seconds``.
        now: Monotonic clock in seconds; injectable for tests.
    """

    def __init__(
        self,
        callback: ProgressCallback | None = None,
        *,
        min_interval: float = 0.1,
        min_delta: int = 0,
        rate_window_seconds: float = 3.0,
        rate_sample_interval: float = 0.2,
        eta_warmup_seconds: float = 2.0,
        eta_warmup_fraction: float = 0.01,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._callback = callback
        self._min_interval = min_interval
        self._min_delta = min_delta
        self._rate_window_seconds = rate_window_seconds
        self._rate_sample_interval = rate_sample_interval
        self._warmup_seconds = eta_warmup_seconds
        self._warmup_fraction = eta_warmup_fraction
        self._now = now

        self._start: float | None = None
        # (time, done) samples over the last ``rate_window_seconds`` (rules 2/3).
        self._samples: deque[tuple[float, int]] = deque()
        self._last_notify_time: float | None = None
        self._last_notify_done = 0
        self._rate: float = 0.0
        self._latest: Progress | None = None

    async def update(self, progress: Progress) -> None:
        """Record ``progress`` and, if rule 4's throttle says so, notify.

        Awaits the callback inline, so a slow UI callback back-pressures the
        producer rather than piling up tasks; it is awaited only when the
        throttle fires.
        """
        t = self._now()
        if self._start is None:
            self._start = t
            self._last_notify_time = None
            self._last_notify_done = progress.done

        if not self._samples or (t - self._samples[-1][0]) >= self._rate_sample_interval:
            self._samples.append((t, progress.done))
            cutoff = t - self._rate_window_seconds
            while len(self._samples) > 1 and self._samples[0][0] < cutoff:
                self._samples.popleft()
            oldest_t, oldest_done = self._samples[0]
            dt = t - oldest_t
            if dt > 0:
                self._rate = (progress.done - oldest_done) / dt
            # dt == 0 leaves the previous rate unchanged.

        self._latest = progress

        should_notify = (
            self._last_notify_time is None
            or (t - self._last_notify_time) >= self._min_interval
            or (self._min_delta > 0 and (progress.done - self._last_notify_done) >= self._min_delta)
        )
        if should_notify and self._callback is not None:
            await self._callback(progress)
            self._last_notify_time = t
            self._last_notify_done = progress.done

    @property
    def latest(self) -> Progress | None:
        """The most recent snapshot passed to ``update``, notified or not."""
        return self._latest

    @property
    def rate(self) -> float:
        """The smoothed rate, in ``unit``\\ s per second."""
        return self._rate

    @property
    def elapsed(self) -> timedelta:
        """Time since the first ``update``; zero before it."""
        if self._start is None:
            return timedelta(0)
        return timedelta(seconds=self._now() - self._start)

    def formatted(self, unit: str) -> FormattedProgress:
        """The current rate (per ``unit``), ETA and elapsed time as text."""
        rate = self._rate
        eta = self.eta
        return FormattedProgress(
            rate=format_rate(rate, unit) if rate > 0 else "",
            eta=format_duration(eta.total_seconds()) if eta is not None else "",
            elapsed=format_duration(self.elapsed.total_seconds()),
        )

    @property
    def eta(self) -> timedelta | None:
        """``None`` when the total is unknown, no progress has been
        recorded yet, or too little time/fraction has elapsed to trust a
        rate estimate (rule 2's warm-up window)."""
        latest = self._latest
        if latest is None or not latest.determinate or latest.total is None or latest.total <= 0:
            return None
        if self._rate <= 0:
            return None

        elapsed_s = self.elapsed.total_seconds()
        fraction = latest.done / latest.total
        if elapsed_s < self._warmup_seconds and fraction < self._warmup_fraction:
            return None

        remaining = latest.total - latest.done
        if remaining <= 0:
            return timedelta(0)
        seconds = remaining / self._rate
        return timedelta(seconds=_quantize(seconds))


def _quantize(seconds: float) -> float:
    """Round to a step size that scales with magnitude, so a displayed
    ETA doesn't visibly jitter tick to tick."""
    if seconds < 60:
        step = 1.0
    elif seconds < 600:
        step = 5.0
    else:
        step = 30.0
    return round(seconds / step) * step


def reading_progress_callback(meter: ProgressMeter) -> Callable[[int, int], Awaitable[None]]:
    """Adapts ``meter`` to the raw ``(done, total)`` shape of an export's
    ``progress`` callback (``run_export``), as a ``phase="reading"`` byte
    count."""

    async def on_progress(done: int, total: int) -> None:
        await meter.update(Progress(phase="reading", determinate=True, done=done, total=total, unit="bytes"))

    return on_progress
