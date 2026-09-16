"""``Progress``/``ProgressMeter``: the one progress contract CLI and
TUI both build on, so neither grows its own rate/ETA math that disagrees
with the other's.

Four rules, implemented here rather than left to each caller to reinvent:

1. The denominator is planned work, not logical size — a caller's job;
   this module just carries whatever ``total`` it's given (e.g. a sparse
   image only needs the chunks a planning pass found, not its full
   logical size).
2. Rate is a windowed average over the last ``rate_window_seconds`` of
   real elapsed time, not a cumulative average (mis-predicts ETA right
   after a rate change) and not a per-call EWMA (a burst of
   near-zero-``dt`` calls would drag a constant-α blend up to a spike
   value). ETA is suppressed until enough time/progress has accumulated
   (a warm-up window), and reported quantized so it doesn't jitter.
3. New rate *samples* are taken no more often than
   ``rate_sample_interval``, decoupling how often the windowed average is
   recomputed from how often ``ProgressMeter.update`` is called — this is
   what makes rule 2's fix actually hold, and keeps the sample deque
   bounded regardless of real call frequency.
4. Reporting itself is rate-limited inside the SDK — calling a UI callback
   per chunk would cost more than the decode it measures.
   ``ProgressMeter.update`` is cheap to call every tick; it only forwards
   to the wrapped callback every ``min_interval`` seconds or ``min_delta``
   units, whichever comes first.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class Progress:
    """One snapshot of a long-running operation's state.

    Attributes:
        phase: ``"discovering"``, ``"planning"``, ``"reading"``,
            ``"assembling"``, or ``"verifying"``.
        determinate: Whether ``total`` is meaningful.
        total: ``None`` when the total is unknown.
        unit: ``"bytes"``, ``"items"``, ``"chunks"``, or ``"buckets"``.
        detail: The file/object currently being processed, for UI display.
        found: Incremental result count when ``total`` is unknown.
    """

    phase: str
    determinate: bool
    done: int = 0
    total: int | None = None
    unit: str = "bytes"
    detail: str = ""
    found: int | None = None


class ProgressMeter:
    """Smooths raw ``Progress`` snapshots into a stable rate/ETA (rules
    2-3) and rate-limits how often the wrapped callback actually fires
    (rule 4). Construct one per operation; call ``update`` as often as
    convenient (every chunk is fine)."""

    def __init__(
        self,
        callback: Callable[[Progress], Awaitable[None]] | None = None,
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
        # (time, done) samples spanning the last ``rate_window_seconds``
        # of real elapsed time — see rules 2/3 above for why a new entry
        # is only appended every ``rate_sample_interval``, not every call.
        self._samples: deque[tuple[float, int]] = deque()
        self._last_notify_time: float | None = None
        self._last_notify_done = 0
        self._rate: float = 0.0
        self._latest: Progress | None = None

    async def update(self, progress: Progress) -> None:
        """Record ``progress`` and, if rule 4's throttle says so, notify.

        ``async def`` even though the sampling arithmetic itself is pure:
        the callback is ``Awaitable``, so making this method async (rather
        than sync with a fire-and-forget Task) is what keeps back-pressure
        intact — a slow UI callback slows the producer down instead of
        piling up unawaited Tasks. The callback is awaited only when the
        throttle decides to notify, so the per-tick cost stays cheap.
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
            # dt == 0 (the very first sample, or several samples that
            # landed on the same clock tick) leaves ``_rate`` at whatever
            # it already was — 0.0 initially, unchanged otherwise; never
            # a divide-by-zero.

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
        return self._latest

    @property
    def rate(self) -> float:
        """The smoothed rate, in ``unit``\\ s per second."""
        return self._rate

    @property
    def elapsed(self) -> timedelta:
        if self._start is None:
            return timedelta(0)
        return timedelta(seconds=self._now() - self._start)

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
    """Adapts ``meter`` to the raw ``(done, total)`` shape
    ``ContentSource.export_to``'s own ``progress`` callback calls, as a
    ``phase="reading"`` byte count — the one adapter both the CLI's
    ``export`` command and the TUI's export screen need, so a UI-facing
    "rate"/"ETA" can never disagree about what a raw export callback's
    numbers mean."""

    async def on_progress(done: int, total: int) -> None:
        await meter.update(Progress(phase="reading", determinate=True, done=done, total=total, unit="bytes"))

    return on_progress
