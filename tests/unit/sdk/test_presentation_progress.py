"""Unit tests for ``synology_apm_repo.sdk.presentation.progress``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta

from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter, reading_progress_callback


class _FakeClock:
    """A manually-advanceable clock so tests never depend on real time."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def __call__(self) -> float:
        return self.now


def _progress(done: int, total: int | None = 1000, determinate: bool = True) -> Progress:
    return Progress(phase="reading", determinate=determinate, done=done, total=total)


def _recording_callback() -> tuple[list[Progress], Callable[[Progress], Awaitable[None]]]:
    """A recording callback plus the list it records into.

    ``ProgressMeter``'s callback must be ``Callable[[Progress],
    Awaitable[None]]`` and is ``await``ed, so a bare ``list.append``
    can't be handed to it — the recorder has to be ``async def``. What
    each throttling test below asserts: how many times the callback
    actually fired.
    """
    calls: list[Progress] = []

    async def record(progress: Progress) -> None:
        calls.append(progress)

    return calls, record


class TestElapsed:
    async def test_zero_before_any_update(self) -> None:
        meter = ProgressMeter()
        assert meter.elapsed == timedelta(0)

    async def test_tracks_time_since_first_update(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock)
        await meter.update(_progress(0))
        clock.advance(5)
        assert meter.elapsed == timedelta(seconds=5)


class TestRateSmoothing:
    async def test_rate_is_zero_on_first_sample(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock)
        await meter.update(_progress(0))
        assert meter.rate == 0.0

    async def test_rate_converges_toward_a_steady_throughput(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock)
        await meter.update(_progress(0))
        for i in range(1, 30):
            clock.advance(1.0)
            await meter.update(_progress(i * 10))  # a steady 10 units/sec
        assert abs(meter.rate - 10.0) < 0.5

    async def test_rate_does_not_jump_instantly_to_a_new_rate(self) -> None:
        """The windowed average blends old and new activity for as long
        as the older sample stays inside the window — it isn't a plain
        "current instantaneous rate", which would jump immediately."""
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, rate_window_seconds=3.0, rate_sample_interval=0.2)
        await meter.update(_progress(0))
        clock.advance(1.0)
        await meter.update(_progress(10))  # instantaneous rate 10/s becomes the whole rate (first sample)
        first_rate = meter.rate
        clock.advance(1.0)
        await meter.update(_progress(110))  # instantaneous rate jumps to 100/s
        # The window still includes the t=0 sample, so the reported rate
        # averages over the whole 2s span, not just the latest second —
        # strictly between the old and new instantaneous rate.
        assert first_rate < meter.rate < 100.0

    async def test_zero_elapsed_between_samples_does_not_corrupt_rate(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock)
        await meter.update(_progress(0))
        await meter.update(_progress(5))  # same timestamp, dt == 0
        assert meter.rate == 0.0  # no divide-by-zero, no crash

    async def test_rate_stays_stable_despite_a_burst_of_near_zero_dt_calls(self) -> None:
        """A burst of near-zero-``dt`` calls (a run of already-cached/fast
        chunks) must not move the rate, since it spans a negligible
        fraction of the averaging window."""
        clock = _FakeClock()
        meter = ProgressMeter(now=clock)
        await meter.update(_progress(0))
        done = 0
        for _ in range(30):
            clock.advance(0.1)
            done += 10  # a steady 100 units/sec baseline
            await meter.update(_progress(done))
        steady_rate = meter.rate

        # A burst of 1000 near-instant calls, tiny delta each — real
        # elapsed time across the whole burst is still only 0.01s.
        for _ in range(1000):
            clock.advance(0.00001)
            done += 1
            await meter.update(_progress(done))

        assert meter.rate == steady_rate, (
            f"a burst of near-zero-dt calls moved the rate from {steady_rate} to {meter.rate} — "
            "it should not have added any new samples at all"
        )


class TestEta:
    async def test_none_when_no_update_yet(self) -> None:
        meter = ProgressMeter()
        assert meter.eta is None

    async def test_none_when_indeterminate(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock)
        await meter.update(_progress(10, total=None, determinate=False))
        clock.advance(5)
        await meter.update(_progress(20, total=None, determinate=False))
        assert meter.eta is None

    async def test_none_when_total_is_none_even_if_determinate(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock)
        await meter.update(_progress(10, total=None))
        clock.advance(5)
        await meter.update(_progress(20, total=None))
        assert meter.eta is None

    async def test_none_during_warmup_window(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=2.0, eta_warmup_fraction=0.01)
        await meter.update(_progress(0, total=1_000_000))
        clock.advance(0.5)  # under the 2s warmup AND under the 1% fraction
        await meter.update(_progress(1, total=1_000_000))
        assert meter.eta is None

    async def test_available_once_past_the_time_warmup(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=1.0, eta_warmup_fraction=0.5)
        await meter.update(_progress(0, total=100))
        clock.advance(2.0)  # past the 1s time warmup
        await meter.update(_progress(20, total=100))  # rate = 10/s
        assert meter.eta is not None

    async def test_available_once_past_the_fraction_warmup(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=100.0, eta_warmup_fraction=0.1)
        await meter.update(_progress(0, total=100))
        clock.advance(1.0)  # nowhere near the 100s time warmup
        await meter.update(_progress(20, total=100))  # 20% done, past the 10% fraction warmup
        assert meter.eta is not None

    async def test_zero_when_already_complete(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=0.0, eta_warmup_fraction=0.0)
        await meter.update(_progress(0, total=100))
        clock.advance(1.0)
        await meter.update(_progress(100, total=100))
        assert meter.eta == timedelta(0)

    async def test_zero_total_never_raises(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock)
        await meter.update(_progress(0, total=0))
        clock.advance(1.0)
        await meter.update(_progress(0, total=0))
        assert meter.eta is None

    async def test_none_when_rate_is_zero(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=0.0, eta_warmup_fraction=0.0)
        await meter.update(_progress(0, total=100))
        clock.advance(1.0)
        await meter.update(_progress(0, total=100))  # no progress made -> rate stays 0
        assert meter.eta is None

    async def test_roughly_matches_remaining_over_rate(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=0.0, eta_warmup_fraction=0.0)
        await meter.update(_progress(0, total=1000))
        clock.advance(1.0)
        await meter.update(_progress(100, total=1000))  # rate = 100/s, remaining = 900 -> ~9s
        eta = meter.eta
        assert eta is not None
        assert 8.0 <= eta.total_seconds() <= 10.0


class TestQuantization:
    async def test_short_etas_round_to_the_nearest_second(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=0.0, eta_warmup_fraction=0.0)
        await meter.update(_progress(0, total=100))
        clock.advance(1.0)
        await meter.update(_progress(50, total=100))  # rate=50/s, remaining=50 -> 1.0s exactly
        eta = meter.eta
        assert eta is not None
        assert eta == timedelta(seconds=1)

    async def test_medium_etas_round_to_a_5_second_step(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=0.0, eta_warmup_fraction=0.0)
        await meter.update(_progress(0, total=1000))
        clock.advance(1.0)
        await meter.update(_progress(2, total=1000))  # rate=2/s, remaining=998 -> 499s (between 60 and 600)
        eta = meter.eta
        assert eta is not None
        assert 60 <= eta.total_seconds() < 600
        assert eta.total_seconds() % 5 == 0

    async def test_long_etas_round_to_a_coarser_step(self) -> None:
        clock = _FakeClock()
        meter = ProgressMeter(now=clock, eta_warmup_seconds=0.0, eta_warmup_fraction=0.0)
        await meter.update(_progress(0, total=100_000))
        clock.advance(1.0)
        await meter.update(_progress(1, total=100_000))  # rate=1/s, remaining=99999s -> way over 600s
        eta = meter.eta
        assert eta is not None
        assert eta.total_seconds() % 30 == 0


class TestNotifyThrottling:
    async def test_first_update_always_notifies(self) -> None:
        calls, record = _recording_callback()
        clock = _FakeClock()
        meter = ProgressMeter(record, now=clock, min_interval=1.0)
        await meter.update(_progress(0))
        assert len(calls) == 1

    async def test_time_based_throttling_suppresses_rapid_updates(self) -> None:
        calls, record = _recording_callback()
        clock = _FakeClock()
        meter = ProgressMeter(record, now=clock, min_interval=1.0, min_delta=0)
        await meter.update(_progress(0))
        clock.advance(0.01)
        await meter.update(_progress(1))  # too soon, suppressed
        clock.advance(0.01)
        await meter.update(_progress(2))  # still too soon
        assert len(calls) == 1

    async def test_time_based_throttling_fires_after_the_interval(self) -> None:
        calls, record = _recording_callback()
        clock = _FakeClock()
        meter = ProgressMeter(record, now=clock, min_interval=1.0, min_delta=0)
        await meter.update(_progress(0))
        clock.advance(1.5)
        await meter.update(_progress(1))
        assert len(calls) == 2

    async def test_delta_based_throttling_fires_before_the_interval_if_enough_progress(self) -> None:
        calls, record = _recording_callback()
        clock = _FakeClock()
        meter = ProgressMeter(record, now=clock, min_interval=100.0, min_delta=10)
        await meter.update(_progress(0))
        clock.advance(0.001)  # nowhere near the 100s interval
        await meter.update(_progress(10))  # but delta >= 10
        assert len(calls) == 2

    async def test_delta_based_throttling_does_not_fire_below_the_delta(self) -> None:
        calls, record = _recording_callback()
        clock = _FakeClock()
        meter = ProgressMeter(record, now=clock, min_interval=100.0, min_delta=10)
        await meter.update(_progress(0))
        clock.advance(0.001)
        await meter.update(_progress(5))  # below both thresholds
        assert len(calls) == 1

    async def test_no_callback_is_fine(self) -> None:
        meter = ProgressMeter()
        await meter.update(_progress(0))  # must not raise


class TestLatest:
    async def test_none_before_any_update(self) -> None:
        meter = ProgressMeter()
        assert meter.latest is None

    async def test_reflects_the_most_recent_snapshot(self) -> None:
        meter = ProgressMeter()
        p = _progress(42)
        await meter.update(p)
        assert meter.latest is p


class TestReadingProgressCallback:
    """The ``export_to()``-shaped ``(done, total)`` adapter both the CLI's
    ``export`` command and the TUI's export screen wrap their own
    ``ProgressMeter`` with."""

    async def test_forwards_a_reading_phase_byte_count_snapshot(self) -> None:
        meter = ProgressMeter()
        callback = reading_progress_callback(meter)
        await callback(5, 10)
        assert meter.latest == Progress(phase="reading", determinate=True, done=5, total=10, unit="bytes")

    async def test_goes_through_the_given_meter_not_a_new_one(self) -> None:
        calls, record = _recording_callback()
        meter = ProgressMeter(record)
        callback = reading_progress_callback(meter)
        await callback(1, 2)
        assert len(calls) == 1
        assert calls[0].done == 1
        assert calls[0].total == 2
