"""Unit tests for ``synology_apm_repo.sdk.units.content.readahead`` over a
synthetic in-memory source that logs every read it serves."""

from __future__ import annotations

import asyncio

import pytest

from support.fakes import unchecked_fake
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.units.content import readahead as readahead_mod
from synology_apm_repo.sdk.units.content.readahead import (
    FIRST_WINDOW,
    MAX_BYTES,
    MAX_WINDOW,
    MIN_WINDOW,
    SequentialReadahead,
)

_SIZE = 64 << 20


@unchecked_fake("a ContentSource's positional read()")
class _Source:
    """``_SIZE`` bytes of a repeating pattern; logs each read's
    ``(offset, length)``. ``short_above`` halves any read longer than it;
    ``fail_above`` fails any read longer than it; ``gate``, when set, holds
    every read longer than ``MIN_WINDOW`` until released."""

    def __init__(self, *, short_above: int | None = None, fail_above: int | None = None) -> None:
        self.reads: list[tuple[int, int]] = []
        self.short_above = short_above
        self.fail_above = fail_above
        self.gate: asyncio.Event | None = None
        self.arrived = asyncio.Event()

    _PERIOD = bytes((i * 7 + 3) % 251 for i in range(251))

    @classmethod
    def expected(cls, offset: int, length: int) -> bytes:
        start = offset % 251
        return (cls._PERIOD * (length // 251 + 2))[start : start + length]

    async def read(self, offset: int, length: int) -> bytes:
        self.reads.append((offset, length))
        self.arrived.set()
        if self.gate is not None and length > MIN_WINDOW:
            await self.gate.wait()
        if self.fail_above is not None and length > self.fail_above:
            raise NotFoundError(f"read of {length} bytes failed")
        if self.short_above is not None and length > self.short_above:
            length //= 2
        await asyncio.sleep(0)
        return self.expected(offset, length)


async def _until_reads(source: _Source, count: int) -> None:
    while len(source.reads) < count:
        source.arrived.clear()
        await source.arrived.wait()


async def _read(ra: SequentialReadahead, offset: int, length: int) -> None:
    assert await ra.read(offset, length) == _Source.expected(offset, length)


async def test_sequential_small_reads_become_few_large_ones() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    for i in range(2048):
        await _read(ra, i * 4096, 4096)
    # 8 MiB through 256 KiB..4 MiB windows: a handful of reads, not 2048.
    assert len(source.reads) < 10
    assert source.reads[1] == (4096, FIRST_WINDOW)


async def test_random_reads_fetch_nothing_extra() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    for i in range(200):
        await _read(ra, (i * 7919 % 5000) * 8192, 4096)
    assert len(source.reads) == 200
    assert ra.held_bytes == 0


async def test_interleaved_streams_are_each_read_ahead_within_the_budget() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    for i in range(1024):
        await _read(ra, i * 8192, 8192)
        await _read(ra, (32 << 20) + i * 8192, 8192)
        assert ra.held_bytes <= MAX_BYTES
    assert len(source.reads) < 40


async def test_a_read_inside_a_window_is_served_from_it_even_backwards() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    for offset in (0, 4096, 8192):
        await _read(ra, offset, 4096)
    before = len(source.reads)
    await _read(ra, 5000, 3000)  # behind the stream, still inside the window
    await _read(ra, 20000, 5000)
    assert len(source.reads) == before


async def test_a_read_across_a_gap_between_windows_goes_to_the_source() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    await _read(ra, 4096, 4096)  # window [4096, 4096 + FIRST_WINDOW)
    far = 16 << 20
    await _read(ra, far, 4096)
    await _read(ra, far + 4096, 4096)  # a second stream's window
    before = len(source.reads)
    await _read(ra, FIRST_WINDOW, 2 * 4096 + far - FIRST_WINDOW - 4096)
    assert len(source.reads) == before + 1


async def test_short_windows_are_not_trusted() -> None:
    source = _Source(short_above=MIN_WINDOW)
    ra = SequentialReadahead(source.read, _SIZE)
    for i in range(64):
        await _read(ra, i * 4096, 4096)
    assert ra.held_bytes == 0
    # The short window is fetched once, not again for each read it covers.
    assert len(source.reads) == 64 + 1


async def test_a_failed_window_falls_back_to_a_direct_read() -> None:
    source = _Source(fail_above=MIN_WINDOW)
    ra = SequentialReadahead(source.read, _SIZE)
    for i in range(8):
        await _read(ra, i * 4096, 4096)
    assert ra.held_bytes == 0
    assert len(source.reads) == 8 + 1


async def test_a_direct_read_failing_raises_the_sources_own_error() -> None:
    source = _Source(fail_above=0)
    ra = SequentialReadahead(source.read, _SIZE)
    with pytest.raises(NotFoundError, match="4096 bytes"):
        await ra.read(0, 4096)


async def test_concurrent_readers_share_one_in_flight_window() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    source.gate = asyncio.Event()
    owner = asyncio.create_task(_read(ra, 4096, 4096))  # fetches the window, held at the gate
    await _until_reads(source, 2)
    joiners = [asyncio.create_task(_read(ra, 4096 * (2 + i), 4096)) for i in range(8)]
    for _ in range(10):
        await asyncio.sleep(0)
    source.gate.set()
    await asyncio.gather(owner, *joiners)
    assert len(source.reads) == 2


async def test_a_cancelled_fetch_sends_its_joiners_to_the_source() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    source.gate = asyncio.Event()
    owner = asyncio.create_task(ra.read(4096, 4096))
    await _until_reads(source, 2)
    joiner = asyncio.create_task(_read(ra, 8192, 4096))
    for _ in range(10):
        await asyncio.sleep(0)
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    await joiner
    assert source.reads[-1] == (8192, 4096)
    assert ra.held_bytes == 0


async def test_streams_past_the_limit_evict_the_least_recently_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(readahead_mod, "STREAMS", 2)
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    for base in (0, 8 << 20, 16 << 20):
        await _read(ra, base, 4096)
        await _read(ra, base + 4096, 4096)
    assert ra.held_bytes == 2 * FIRST_WINDOW


@pytest.mark.parametrize(("offset", "length"), [(0, 0), (_SIZE - 10, 20), (0, MAX_WINDOW)])
async def test_empty_oversized_and_past_the_end_reads_pass_through(offset: int, length: int) -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await ra.read(offset, length)
    await ra.read(offset + length, length)
    assert source.reads == [(offset, length), (offset + length, length)]
    assert ra.held_bytes == 0


async def test_a_window_is_clipped_to_the_source_and_skipped_when_too_small() -> None:
    size = FIRST_WINDOW
    source = _Source()
    ra = SequentialReadahead(source.read, size)
    await _read(ra, 0, 4096)
    await _read(ra, 4096, 4096)  # window [4096, size)
    assert source.reads[-1] == (4096, size - 4096)
    tail = SequentialReadahead(source.read, size)
    await tail.read(size - 8192, 4096)
    await tail.read(size - 4096, 4096)  # under MIN_WINDOW left: read directly
    assert source.reads[-1] == (size - 4096, 4096)
    assert tail.held_bytes == 0


async def test_a_read_straddling_a_windows_end_continues_into_the_next_window() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    await _read(ra, 4096, 4096)  # window [4096, 4096 + FIRST_WINDOW)
    straddle = 4096 + FIRST_WINDOW - 2048
    await _read(ra, straddle, 4096)  # fetches the next window, from the first one's end
    assert source.reads[-1][0] == 4096 + FIRST_WINDOW
    before = len(source.reads)
    await _read(ra, straddle - 1000, 8192)  # spans both windows
    assert len(source.reads) == before


async def test_a_cancelled_window_drops_the_windows_appended_after_it() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    await _read(ra, 4096, 4096)  # window a: [4096, a_end)
    a_end = 4096 + FIRST_WINDOW
    source.gate = asyncio.Event()
    owner = asyncio.create_task(ra.read(a_end - 2048, 4096))  # window b: [a_end, b_end), held
    await _until_reads(source, 3)
    b_end = source.reads[-1][0] + source.reads[-1][1]
    # From inside a to past b: appends window c at b_end, a still unretired.
    spanner = asyncio.create_task(_read(ra, a_end - 4096, b_end - a_end + 8192))
    await _until_reads(source, 4)
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    source.gate.set()
    await spanner
    # Were c kept without b, a and c would pass as covering this read.
    await _read(ra, a_end - 1000, b_end - a_end + 2000)


async def test_windows_are_dropped_once_reads_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(readahead_mod, "IDLE_SECONDS", 0)
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    await _read(ra, 4096, 4096)
    assert ra.held_bytes == FIRST_WINDOW
    for _ in range(5):
        await asyncio.sleep(0)
    assert ra.held_bytes == 0


async def test_an_evicted_streams_loading_window_still_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(readahead_mod, "STREAMS", 1)
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    source.gate = asyncio.Event()
    loading = asyncio.create_task(_read(ra, 4096, 4096))
    await _until_reads(source, 2)
    await _read(ra, 32 << 20, 4096)  # a second stream evicts the first
    assert ra.held_bytes == FIRST_WINDOW
    source.gate.set()
    await loading
    assert ra.held_bytes == 0


async def test_a_large_read_carries_the_stream_it_continues() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    await _read(ra, 4096, MAX_WINDOW)  # passed through
    await _read(ra, 4096 + MAX_WINDOW, 4096)  # the stream's second read: a window
    assert source.reads[-1][1] > 4096


async def test_a_read_from_one_window_is_bytes() -> None:
    source = _Source()
    ra = SequentialReadahead(source.read, _SIZE)
    await _read(ra, 0, 4096)
    assert isinstance(await ra.read(4096, 4096), bytes)
