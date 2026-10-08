"""Unit tests for ``synology_apm_repo.sdk.dedup.buffered_export_sink``."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from multiprocessing import shared_memory
from pathlib import Path
from typing import Any, Literal

import pytest

from synology_apm_repo.sdk.dedup import segment_buffers as segment_buffers_mod
from synology_apm_repo.sdk.dedup.buffered_export_sink import BufferedExportSink, FlushableSegment
from synology_apm_repo.sdk.dedup.export_sink import AbortOutcome, WorkerTarget
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileDescriptor

_SEG = 4096
_TIMEOUT = 5.0


class _Sink(BufferedExportSink):
    """Records every hook call and the bytes each segment held, optionally blocking or failing a flush."""

    def __init__(
        self,
        segment_size: int = _SEG,
        *,
        max_buffered_segments: int = 2,
        fail_flush_of: int | None = None,
        fail_create: bool = False,
        fail_finalize: bool = False,
        fail_discard: bool = False,
        discard_result: bool = True,
        storage: Literal["memory", "spool"] = "memory",
        spool_dir: Path | None = None,
    ) -> None:
        super().__init__(
            segment_size, max_buffered_segments=max_buffered_segments, storage=storage, spool_dir=spool_dir
        )
        self.events: list[tuple[str, int | None]] = []
        self.flushed: dict[int, bytes] = {}
        self.gate: asyncio.Event | None = None
        self.flush_started = asyncio.Event()
        self.kept_segment: FlushableSegment | None = None
        self._fail_flush_of = fail_flush_of
        self._fail_create = fail_create
        self._fail_finalize = fail_finalize
        self._fail_discard = fail_discard
        self._discard_result = discard_result

    def block_flushes(self) -> asyncio.Event:
        self.gate = asyncio.Event()
        return self.gate

    async def create_destination(self, logical_size: int, *, sparse: bool) -> None:
        self.events.append(("create", logical_size))
        if self._fail_create:
            raise RuntimeError("synthetic create failure")

    async def flush_segment(self, segment: FlushableSegment) -> None:
        self.events.append(("flush-start", segment.index))
        self.flush_started.set()
        if self.gate is not None:
            await self.gate.wait()
        if segment.index == self._fail_flush_of:
            raise RuntimeError(f"synthetic flush failure of segment {segment.index}")
        self.flushed[segment.index] = await segment.read_all()
        self.kept_segment = segment
        self.events.append(("flush-end", segment.index))

    async def finalize_destination(self) -> None:
        self.events.append(("finalize", None))
        if self._fail_finalize:
            raise RuntimeError("synthetic finalize failure")

    async def discard_destination(self) -> bool:
        self.events.append(("discard", None))
        if self._fail_discard:
            raise RuntimeError("synthetic discard failure")
        return self._discard_result

    def names(self) -> list[str]:
        return [name for name, _ in self.events]


async def _settle() -> None:
    """Lets every ready task run to its next suspension point."""
    for _ in range(10):
        await asyncio.sleep(0)


async def _fill(sink: _Sink, index: int, length: int, fill: int) -> None:
    """Writes one whole segment of ``fill`` bytes and completes it."""
    writer = await asyncio.wait_for(sink.begin_segment(index, index * _SEG, length), _TIMEOUT)
    await writer.write_at(0, bytes([fill]) * length)
    await writer.complete()


class TestConstruction:
    @pytest.mark.parametrize("size", [0, -4096, 1000, 4097])
    def test_a_segment_size_that_is_not_a_positive_multiple_of_4096_is_rejected(self, size: int) -> None:
        with pytest.raises(ValueError, match="multiple of 4096"):
            _Sink(size)

    def test_fewer_than_one_buffered_segment_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            _Sink(max_buffered_segments=0)

    def test_it_reports_its_segment_size(self) -> None:
        assert _Sink(8192).segment_size == 8192

    def test_an_unknown_storage_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="'memory' or 'spool'"):
            _Sink(storage="disk")  # type: ignore[arg-type]

    def test_a_spool_directory_needs_spool_storage(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="only applies to spool"):
            _Sink(storage="memory", spool_dir=tmp_path)

    def test_a_subclass_missing_a_hook_cannot_be_built(self) -> None:
        class _Incomplete(BufferedExportSink):
            async def create_destination(self, logical_size: int, *, sparse: bool) -> None: ...

        with pytest.raises(TypeError, match="abstract"):
            _Incomplete(_SEG)  # type: ignore[abstract]


class TestExport:
    async def test_segments_are_flushed_in_order_and_finalized_after_the_last(self) -> None:
        sink = _Sink()
        await sink.open(2 * _SEG + 100, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await _fill(sink, 1, _SEG, 2)
        await _fill(sink, 2, 100, 3)
        await sink.commit()

        assert sink.flushed == {0: bytes([1]) * _SEG, 1: bytes([2]) * _SEG, 2: bytes([3]) * 100}
        assert sink.names()[0] == "create"
        assert sink.names()[-1] == "finalize"
        assert [e for e in sink.events if e[0] == "flush-end"] == [("flush-end", 0), ("flush-end", 1), ("flush-end", 2)]

    async def test_writes_land_at_their_offsets_in_any_order_and_unwritten_bytes_are_zero(self) -> None:
        sink = _Sink()
        await sink.open(_SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        await writer.write_at(3000, b"cc")
        await writer.write_at(10, b"aa")
        await writer.write_at(1000, b"bb")
        await writer.complete()
        await sink.commit()

        expected = bytearray(_SEG)
        expected[10:12], expected[1000:1002], expected[3000:3002] = b"aa", b"bb", b"cc"
        assert sink.flushed[0] == bytes(expected)

    async def test_write_zero_overwrites_earlier_data_across_several_zero_blocks(self) -> None:
        size = 4 << 20
        sink = _Sink(size)
        await sink.open(size, sparse=False)
        writer = await sink.begin_segment(0, 0, size)
        await writer.write_at(0, b"\xff" * size)
        await writer.write_zero(100, (5 << 19) - 100)  # [100, 2.5 MiB): three passes of the 1 MiB zero block
        await writer.complete()
        await sink.commit()

        data = sink.flushed[0]
        assert data[:100] == b"\xff" * 100
        assert data[100 : 5 << 19] == bytes((5 << 19) - 100)
        assert data[5 << 19 :] == b"\xff" * (size - (5 << 19))

    async def test_a_segment_with_nothing_written_is_all_zero(self) -> None:
        sink = _Sink()
        await sink.open(_SEG, sparse=True)
        await (await sink.begin_segment(0, 0, _SEG)).complete()
        await sink.commit()
        assert sink.flushed[0] == bytes(_SEG)

    async def test_a_zero_length_export_finalizes_without_flushing(self) -> None:
        sink = _Sink()
        await sink.open(0, sparse=True)
        await sink.commit()
        assert sink.names() == ["create", "finalize"]

    async def test_the_writer_advertises_a_zeroed_buffer_that_can_stay_sparse(self) -> None:
        sink = _Sink()
        await sink.open(_SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        assert writer.caps.supports_sparse
        assert writer.preallocated

    async def test_flushed_bytes_are_only_valid_inside_the_hook(self) -> None:
        sink = _Sink()
        await sink.open(_SEG, sparse=True)
        await _fill(sink, 0, _SEG, 7)
        await sink.commit()

        assert sink.kept_segment is not None
        with pytest.raises(RuntimeError, match="only valid inside flush_segment"):
            await sink.kept_segment.read_all()

    async def test_blocks_yield_views_of_at_most_the_requested_size_in_order(self) -> None:
        async def read(offset: int, length: int) -> bytes:
            return bytes(length)

        segment = FlushableSegment(0, 0, 10, read)
        assert [len(block) async for block in segment.blocks(4)] == [4, 4, 2]


class TestBlocksAreReleased:
    @pytest.mark.parametrize("consumer", ["keeps every view", "breaks", "raises"])
    async def test_views_are_valid_until_the_hook_returns_and_then_released(self, consumer: str) -> None:
        """Every view ``blocks()`` yielded reads correctly until
        ``flush_segment`` returns and is released after, however the hook
        ends, so one still referenced (as a thread-pool work item briefly
        keeps one) can't keep the shared memory from closing."""
        kept: list[memoryview] = []
        # Referenced past the hook, so nothing finalizes a generator left
        # suspended by a break or a raise.
        generators: list[AsyncIterator[memoryview]] = []

        class _KeepingSink(_Sink):
            async def flush_segment(self, segment: FlushableSegment) -> None:
                generators.append(segment.blocks(_SEG // 4))
                async for view in generators[0]:
                    kept.append(view)
                    if consumer != "keeps every view":
                        break
                assert all(view.tobytes() == bytes([7]) * len(view) for view in kept)
                if consumer == "raises":
                    raise RuntimeError("synthetic flush failure")

        released_cleanly: list[bool] = []
        real_release = segment_buffers_mod.SharedPool._release

        def release(shm: shared_memory.SharedMemory) -> None:
            try:
                shm.close()
                released_cleanly.append(True)
            except BufferError:
                released_cleanly.append(False)
            real_release(shm)

        sink = _KeepingSink()
        await sink.open(_SEG, sparse=True)
        await _fill(sink, 0, _SEG, 7)
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(segment_buffers_mod.SharedPool, "_release", staticmethod(release))
            if consumer == "raises":
                with pytest.raises(RuntimeError, match="synthetic flush failure"):
                    await sink.commit()
                await sink.abort()
            else:
                await sink.commit()

        assert len(kept) == (4 if consumer == "keeps every view" else 1)
        for view in kept:
            with pytest.raises(ValueError, match="released memoryview"):
                view.tobytes()
        assert released_cleanly == [True]


class TestSegmentRules:
    async def test_a_write_outside_the_segment_is_rejected(self) -> None:
        sink = _Sink()
        await sink.open(100, sparse=True)
        writer = await sink.begin_segment(0, 0, 100)
        with pytest.raises(ValueError, match="outside segment 0"):
            await writer.write_at(90, bytes(20))
        with pytest.raises(ValueError, match="outside segment 0"):
            await writer.write_zero(0, 101)

    async def test_a_write_after_complete_is_rejected(self) -> None:
        sink = _Sink()
        await sink.open(_SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        await writer.complete()
        with pytest.raises(RuntimeError, match="already complete"):
            await writer.write_at(0, b"x")

    async def test_segments_must_come_in_order_at_the_right_place_and_size(self) -> None:
        sink = _Sink()
        await sink.open(2 * _SEG + 10, sparse=True)
        with pytest.raises(ValueError, match="expected segment 0"):
            await sink.begin_segment(1, _SEG, _SEG)
        with pytest.raises(ValueError, match="must be 4096 bytes at 0"):
            await sink.begin_segment(0, 0, 100)
        with pytest.raises(ValueError, match="must be 4096 bytes at 0"):
            await sink.begin_segment(0, 5, _SEG)

    async def test_the_last_segment_must_be_exactly_the_remainder(self) -> None:
        sink = _Sink()
        await sink.open(_SEG + 10, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        with pytest.raises(ValueError, match="must be 10 bytes at 4096"):
            await sink.begin_segment(1, _SEG, _SEG)

    async def test_a_new_segment_cannot_begin_before_the_last_completes(self) -> None:
        sink = _Sink()
        await sink.open(2 * _SEG, sparse=True)
        await sink.begin_segment(0, 0, _SEG)
        with pytest.raises(RuntimeError, match="segment 0 is not complete"):
            await sink.begin_segment(1, _SEG, _SEG)

    async def test_commit_requires_the_last_segment_to_be_complete(self) -> None:
        sink = _Sink()
        await sink.open(_SEG, sparse=True)
        await sink.begin_segment(0, 0, _SEG)
        with pytest.raises(RuntimeError, match="segment 0 is not complete"):
            await sink.commit()

    async def test_nothing_works_before_open_or_twice(self) -> None:
        sink = _Sink()
        with pytest.raises(RuntimeError, match="not open"):
            await sink.begin_segment(0, 0, _SEG)
        with pytest.raises(RuntimeError, match="not open"):
            await sink.commit()
        await sink.open(_SEG, sparse=True)
        with pytest.raises(RuntimeError, match="not new"):
            await sink.open(_SEG, sparse=True)

    async def test_nothing_works_after_commit(self) -> None:
        sink = _Sink()
        await sink.open(0, sparse=True)
        await sink.commit()
        with pytest.raises(RuntimeError, match="committed, not open"):
            await sink.begin_segment(0, 0, _SEG)


class TestBackpressure:
    async def _begin_in_background(self, sink: _Sink, index: int, length: int = _SEG) -> asyncio.Task[object]:
        task: asyncio.Task[object] = asyncio.ensure_future(sink.begin_segment(index, index * _SEG, length))
        await _settle()
        return task

    async def test_with_one_slot_the_next_segment_waits_for_the_previous_flush(self) -> None:
        sink = _Sink(max_buffered_segments=1)
        gate = sink.block_flushes()
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)

        waiting = await self._begin_in_background(sink, 1)
        assert not waiting.done()  # segment 0 is still being flushed

        gate.set()
        await asyncio.wait_for(waiting, _TIMEOUT)
        assert "flush-end" in sink.names()

    async def test_with_two_slots_one_segment_fills_while_another_flushes_and_a_third_waits(self) -> None:
        sink = _Sink(max_buffered_segments=2)
        gate = sink.block_flushes()
        await sink.open(3 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await _fill(sink, 1, _SEG, 2)  # begins while segment 0 is mid-flush

        third = await self._begin_in_background(sink, 2)
        assert not third.done()  # both slots are taken until segment 0's flush ends
        assert sink.names().count("flush-end") == 0

        gate.set()
        await asyncio.wait_for(third, _TIMEOUT)

    async def test_flushing_overlaps_the_next_segments_writes(self) -> None:
        sink = _Sink(max_buffered_segments=2)
        gate = sink.block_flushes()
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await asyncio.wait_for(sink.flush_started.wait(), _TIMEOUT)

        writer = await asyncio.wait_for(sink.begin_segment(1, _SEG, _SEG), _TIMEOUT)
        await writer.write_at(0, b"x" * _SEG)  # accepted while segment 0 is still flushing
        assert sink.events == [("create", 2 * _SEG), ("flush-start", 0)]

        gate.set()
        await writer.complete()
        await asyncio.wait_for(sink.commit(), _TIMEOUT)
        assert sink.flushed == {0: b"\x01" * _SEG, 1: b"x" * _SEG}


class TestFailure:
    async def test_a_failed_flush_fails_the_next_begin_complete_and_commit(self) -> None:
        sink = _Sink(fail_flush_of=0)
        await sink.open(3 * _SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        await writer.complete()
        await _settle()

        with pytest.raises(RuntimeError, match="synthetic flush failure of segment 0"):
            await sink.begin_segment(1, _SEG, _SEG)
        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await writer.complete()
        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await sink.commit()

    async def test_a_failure_stops_the_export_within_the_segment_being_written(self) -> None:
        sink = _Sink(fail_flush_of=0)
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        writer = await sink.begin_segment(1, _SEG, _SEG)
        await _settle()

        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await writer.write_at(0, b"x")
        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await writer.write_zero(0, 10)

    async def test_a_failure_wakes_a_begin_waiting_for_a_slot(self) -> None:
        sink = _Sink(max_buffered_segments=1, fail_flush_of=0)
        gate = sink.block_flushes()
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        waiting: asyncio.Task[object] = asyncio.ensure_future(sink.begin_segment(1, _SEG, _SEG))
        await _settle()
        assert not waiting.done()

        gate.set()
        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await asyncio.wait_for(waiting, _TIMEOUT)

    async def test_a_failure_in_the_last_flush_surfaces_from_commit(self) -> None:
        sink = _Sink(fail_flush_of=1)
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await _fill(sink, 1, _SEG, 2)

        with pytest.raises(RuntimeError, match="synthetic flush failure of segment 1"):
            await sink.commit()
        assert "finalize" not in sink.names()

    async def test_segments_queued_behind_a_failed_flush_are_dropped_and_their_slots_freed(self) -> None:
        sink = _Sink(max_buffered_segments=3, fail_flush_of=0)
        gate = sink.block_flushes()
        await sink.open(3 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await _fill(sink, 1, _SEG, 2)
        await _fill(sink, 2, _SEG, 3)
        gate.set()

        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await sink.commit()
        assert sink.names().count("flush-start") == 1  # the queued segments were never flushed
        assert sorted(sink._pool.free) == [0, 1, 2]  # every slot came back

    async def test_a_finalize_failure_surfaces_from_commit(self) -> None:
        sink = _Sink(fail_finalize=True)
        await sink.open(0, sparse=True)
        with pytest.raises(RuntimeError, match="synthetic finalize failure"):
            await sink.commit()


class TestFlusherStopped:
    async def test_a_flush_cut_short_by_a_base_exception_still_fails_a_waiting_begin(self) -> None:
        class _Cancelling(_Sink):
            async def flush_segment(self, segment: FlushableSegment) -> None:
                raise asyncio.CancelledError  # e.g. an inner await cancelled

        sink = _Cancelling(max_buffered_segments=1)
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)

        with pytest.raises(RuntimeError, match="flushing stopped"):
            await asyncio.wait_for(sink.begin_segment(1, _SEG, _SEG), _TIMEOUT)
        await sink.abort()


class TestOverlappingLifecycleCalls:
    async def test_an_abort_during_a_slow_create_is_not_undone_by_the_open_finishing(self) -> None:
        release = asyncio.Event()

        class _Slow(_Sink):
            async def create_destination(self, logical_size: int, *, sparse: bool) -> None:
                await release.wait()

        sink = _Slow()
        opening = asyncio.ensure_future(sink.open(_SEG, sparse=True))
        await _settle()
        aborting = asyncio.ensure_future(sink.abort())
        await _settle()
        release.set()

        with pytest.raises(RuntimeError, match="aborted while it was opening"):
            await asyncio.wait_for(opening, _TIMEOUT)
        await asyncio.wait_for(aborting, _TIMEOUT)
        with pytest.raises(RuntimeError, match="aborted, not open"):
            await sink.begin_segment(0, 0, _SEG)

    async def test_an_abort_during_finalize_waits_for_it_and_discards_nothing(self) -> None:
        release = asyncio.Event()
        finalize_started = asyncio.Event()

        class _SlowFinalize(_Sink):
            async def finalize_destination(self) -> None:
                self.events.append(("finalize-start", None))
                finalize_started.set()
                await release.wait()
                self.events.append(("finalize", None))

        sink = _SlowFinalize()
        await sink.open(_SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        committing = asyncio.ensure_future(sink.commit())
        # Not _settle(): commit reaches finalize only after the flusher has
        # released the slot, which goes through a thread.
        await asyncio.wait_for(finalize_started.wait(), _TIMEOUT)
        aborting = asyncio.ensure_future(sink.abort())
        await _settle()
        assert not aborting.done()

        release.set()
        await asyncio.wait_for(committing, _TIMEOUT)
        outcome = await asyncio.wait_for(aborting, _TIMEOUT)

        assert outcome == AbortOutcome(kept=True, ever_written=True)
        assert "discard" not in sink.names()


class TestCommitInterrupted:
    async def test_cancelling_commit_does_not_interrupt_the_flush_in_progress(self) -> None:
        sink = _Sink()
        gate = sink.block_flushes()
        await sink.open(_SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await asyncio.wait_for(sink.flush_started.wait(), _TIMEOUT)
        committing = asyncio.ensure_future(sink.commit())
        await _settle()

        committing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await committing
        gate.set()
        await asyncio.wait_for(sink.abort(), _TIMEOUT)

        assert "flush-end" in sink.names()  # the flush ran to its end

    async def test_an_abort_during_commit_stops_it_before_finalize(self) -> None:
        sink = _Sink()
        gate = sink.block_flushes()
        await sink.open(_SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await asyncio.wait_for(sink.flush_started.wait(), _TIMEOUT)
        committing = asyncio.ensure_future(sink.commit())
        await _settle()
        aborting = asyncio.ensure_future(sink.abort())
        await _settle()

        gate.set()
        with pytest.raises(RuntimeError, match="aborted, not open"):
            await asyncio.wait_for(committing, _TIMEOUT)
        await asyncio.wait_for(aborting, _TIMEOUT)

        assert "finalize" not in sink.names()
        assert sink.names().count("discard") == 1


class TestAbort:
    async def test_before_open_nothing_is_discarded(self) -> None:
        sink = _Sink()
        assert await sink.abort() == AbortOutcome(kept=False, ever_written=False)
        assert sink.events == []

    async def test_after_writes_the_destination_is_discarded_and_the_hooks_answer_is_reported(self) -> None:
        sink = _Sink(discard_result=False)
        await sink.open(_SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        await writer.write_at(0, b"x")

        outcome = await sink.abort()

        assert outcome == AbortOutcome(kept=False, ever_written=True)
        assert sink.names()[-1] == "discard"

    async def test_a_destination_left_behind_is_reported_as_kept(self) -> None:
        sink = _Sink(discard_result=True)
        await sink.open(_SEG, sparse=True)
        assert await sink.abort() == AbortOutcome(kept=True, ever_written=False)

    async def test_it_is_idempotent_and_repeats_the_first_outcome(self) -> None:
        sink = _Sink(discard_result=False)
        await sink.open(_SEG, sparse=True)
        first = await sink.abort()
        assert await sink.abort() == first
        assert sink.names().count("discard") == 1

    async def test_concurrent_aborts_discard_once_and_agree(self) -> None:
        sink = _Sink(discard_result=False)
        await sink.open(_SEG, sparse=True)

        first, second = await asyncio.gather(sink.abort(), sink.abort())

        assert first == second == AbortOutcome(kept=False, ever_written=False)
        assert sink.names().count("discard") == 1

    async def test_it_waits_for_the_flush_in_progress_before_discarding(self) -> None:
        sink = _Sink()
        gate = sink.block_flushes()
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await asyncio.wait_for(sink.flush_started.wait(), _TIMEOUT)

        aborting: asyncio.Task[AbortOutcome] = asyncio.ensure_future(sink.abort())
        await _settle()
        assert not aborting.done()
        assert "discard" not in sink.names()

        gate.set()
        await asyncio.wait_for(aborting, _TIMEOUT)
        assert sink.names().index("flush-end") < sink.names().index("discard")

    async def test_it_drops_the_segments_still_queued(self) -> None:
        sink = _Sink(max_buffered_segments=3)
        gate = sink.block_flushes()
        await sink.open(3 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await _fill(sink, 1, _SEG, 2)
        await asyncio.wait_for(sink.flush_started.wait(), _TIMEOUT)

        aborting: asyncio.Task[AbortOutcome] = asyncio.ensure_future(sink.abort())
        await _settle()
        gate.set()
        await asyncio.wait_for(aborting, _TIMEOUT)

        assert sink.names().count("flush-start") == 1

    async def test_it_wakes_a_begin_waiting_for_a_slot(self) -> None:
        sink = _Sink(max_buffered_segments=1)
        sink.block_flushes()
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        waiting: asyncio.Task[object] = asyncio.ensure_future(sink.begin_segment(1, _SEG, _SEG))
        await _settle()
        assert not waiting.done()

        gate = sink.gate
        assert gate is not None
        aborting: asyncio.Task[AbortOutcome] = asyncio.ensure_future(sink.abort())
        with pytest.raises(RuntimeError, match="aborted, not open"):
            await asyncio.wait_for(waiting, _TIMEOUT)
        gate.set()
        await asyncio.wait_for(aborting, _TIMEOUT)

    async def test_an_open_writer_cannot_write_after_abort(self) -> None:
        sink = _Sink()
        await sink.open(_SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        await sink.abort()
        with pytest.raises(RuntimeError, match="aborted, not open"):
            await writer.write_at(0, b"x")

    async def test_after_commit_nothing_is_discarded_and_the_destination_counts_as_kept(self) -> None:
        sink = _Sink()
        await sink.open(_SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await sink.commit()

        assert await sink.abort() == AbortOutcome(kept=True, ever_written=True)
        assert "discard" not in sink.names()

    async def test_a_failed_create_still_discards_what_it_left(self) -> None:
        sink = _Sink(fail_create=True, discard_result=False)
        with pytest.raises(RuntimeError, match="synthetic create failure"):
            await sink.open(_SEG, sparse=True)

        assert await sink.abort() == AbortOutcome(kept=False, ever_written=False)
        assert sink.names() == ["create", "discard"]

    async def test_a_failed_discard_raises_and_later_calls_report_the_destination_as_kept(self) -> None:
        sink = _Sink(fail_discard=True)
        await sink.open(_SEG, sparse=True)
        with pytest.raises(RuntimeError, match="synthetic discard failure"):
            await sink.abort()
        assert await sink.abort() == AbortOutcome(kept=True, ever_written=False)

    async def test_a_cancelled_export_can_be_aborted_while_a_flush_is_running(self) -> None:
        sink = _Sink()
        sink.block_flushes()
        await sink.open(2 * _SEG, sparse=True)

        async def export() -> None:
            await _fill(sink, 0, _SEG, 1)
            await asyncio.Event().wait()

        task = asyncio.ensure_future(export())
        await asyncio.wait_for(sink.flush_started.wait(), _TIMEOUT)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        gate = sink.gate
        assert gate is not None
        gate.set()
        assert (await asyncio.wait_for(sink.abort(), _TIMEOUT)).kept


def _spool_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("apm-export-*.spool"))


class TestSpoolStorage:
    """Spool storage, plus the worker-write and slot-zeroing rules memory storage shares."""

    async def test_the_segments_come_out_as_written_in_any_order(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(_SEG + 100, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        await writer.write_at(3000, b"cc")
        await writer.write_at(10, b"aa")
        await writer.write_zero(3000, 1)
        await writer.complete()
        await _fill(sink, 1, 100, 9)
        await sink.commit()

        expected = bytearray(_SEG)
        expected[10:12], expected[3000:3002] = b"aa", b"\x00c"
        assert sink.flushed == {0: bytes(expected), 1: bytes([9]) * 100}

    async def test_the_spool_file_exists_while_exporting_and_is_gone_after_commit(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(_SEG, sparse=True)
        [spool] = _spool_files(tmp_path)
        assert spool.stat().st_size == _SEG  # one segment needs one slot, like memory storage

        await _fill(sink, 0, _SEG, 1)
        await sink.commit()

        assert _spool_files(tmp_path) == []

    async def test_the_spool_holds_max_buffered_segments_slots_for_a_larger_export(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(5 * _SEG, sparse=True)
        [spool] = _spool_files(tmp_path)
        assert spool.stat().st_size == 2 * _SEG  # max_buffered_segments=2, sparse
        await sink.abort()

    async def test_the_spool_file_is_gone_after_abort(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(_SEG, sparse=True)
        await sink.begin_segment(0, 0, _SEG)

        await sink.abort()

        assert _spool_files(tmp_path) == []

    async def test_an_unusable_spool_directory_refuses_the_export_before_a_destination_exists(
        self, tmp_path: Path
    ) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path / "missing")

        with pytest.raises(FileNotFoundError):
            await sink.open(_SEG, sparse=True)

        assert sink.events == []  # create_destination never ran
        assert await sink.abort() == AbortOutcome(kept=False, ever_written=False)
        assert sink.events == []

    async def test_a_reused_slot_starts_zero_again(self, tmp_path: Path) -> None:
        """Segment 2 gets segment 0's slot back: nothing of segment 0 may show through."""
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(3 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 0xFF)
        await _fill(sink, 1, _SEG, 0xEE)
        writer = await asyncio.wait_for(sink.begin_segment(2, 2 * _SEG, _SEG), _TIMEOUT)
        await writer.write_at(100, b"new")
        await writer.complete()
        await sink.commit()

        expected = bytearray(_SEG)
        expected[100:103] = b"new"
        assert sink.flushed[2] == bytes(expected)
        assert sink.flushed[0] == bytes([0xFF]) * _SEG

    async def test_a_shared_memory_hole_is_zeroed_off_the_event_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        loop_thread = threading.get_ident()
        seen: list[int] = []
        real = segment_buffers_mod._zero

        def recording(buf: memoryview, start: int, end: int) -> None:
            seen.append(threading.get_ident())
            real(buf, start, end)

        monkeypatch.setattr(segment_buffers_mod, "_zero", recording)
        sink = _Sink()
        await sink.open(_SEG, sparse=False)
        writer = await sink.begin_segment(0, 0, _SEG)
        await writer.write_zero(0, _SEG)
        await sink.abort()

        assert seen and loop_thread not in seen

    async def test_a_hole_zeroed_by_write_zero_overwrites_earlier_data_in_the_slot(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(_SEG, sparse=False)
        writer = await sink.begin_segment(0, 0, _SEG)
        await writer.write_at(0, b"\xff" * _SEG)
        await writer.write_zero(100, 200)
        await writer.complete()
        await sink.commit()

        data = sink.flushed[0]
        assert data[:100] == b"\xff" * 100
        assert data[100:300] == bytes(200)
        assert data[300:] == b"\xff" * (_SEG - 300)

    async def test_the_writer_can_take_worker_writes_into_its_own_slot_of_the_spool(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(3 * _SEG, sparse=True)
        [spool] = _spool_files(tmp_path)

        first = await sink.begin_segment(0, 0, _SEG)
        assert first.worker_target() == WorkerTarget(LocalFileDescriptor(str(spool)), 0)
        await first.complete()
        second = await sink.begin_segment(1, _SEG, _SEG)
        assert second.worker_target() == WorkerTarget(LocalFileDescriptor(str(spool)), _SEG)
        await second.complete()
        third = await asyncio.wait_for(sink.begin_segment(2, 2 * _SEG, _SEG), _TIMEOUT)
        assert third.worker_target() == WorkerTarget(LocalFileDescriptor(str(spool)), 0)  # slot 0 again

    async def test_bytes_a_worker_wrote_are_flushed_and_the_slot_is_zeroed_for_reuse(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(3 * _SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        target = writer.worker_target()
        assert target is not None
        writer.note_worker_write()
        worker = target.descriptor.open_writer()
        worker.write_at(target.base_offset + 500, b"from a worker")
        worker.close()
        await writer.complete()
        await _fill(sink, 1, _SEG, 0)
        reused = await asyncio.wait_for(sink.begin_segment(2, 2 * _SEG, _SEG), _TIMEOUT)
        await reused.complete()
        await sink.commit()

        assert sink.flushed[0][500:513] == b"from a worker"
        assert sink.flushed[2] == bytes(_SEG)  # nothing of the worker's bytes survived the reuse

    async def test_a_worker_write_counts_as_the_destination_having_been_written(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path, discard_result=False)
        await sink.open(_SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        writer.note_worker_write()

        assert await sink.abort() == AbortOutcome(kept=False, ever_written=True)

    async def test_memory_writers_take_worker_writes_into_their_slot_of_shared_memory(self) -> None:
        sink = _Sink()
        await sink.open(2 * _SEG, sparse=True)
        first = await sink.begin_segment(0, 0, _SEG)
        target = first.worker_target()
        assert target is not None and target.base_offset == 0
        first.note_worker_write()
        worker = target.descriptor.open_writer()  # attaches by name, as a worker process would
        worker.write_at(target.base_offset + 7, b"shared")
        worker.close()
        await first.complete()
        second = await sink.begin_segment(1, _SEG, _SEG)
        assert second.worker_target().base_offset == _SEG  # type: ignore[union-attr]
        await second.complete()
        await sink.commit()

        assert sink.flushed[0][7:13] == b"shared"

    async def test_a_memory_slot_is_zeroed_again_after_a_worker_wrote_into_it(self) -> None:
        sink = _Sink(max_buffered_segments=1)
        await sink.open(2 * _SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        target = writer.worker_target()
        assert target is not None
        writer.note_worker_write()
        worker = target.descriptor.open_writer()
        worker.write_at(100, b"x" * 50)
        worker.close()
        await writer.complete()
        again = await asyncio.wait_for(sink.begin_segment(1, _SEG, _SEG), _TIMEOUT)
        await again.complete()
        await sink.commit()

        assert sink.flushed[1] == bytes(_SEG)

    async def test_a_small_export_does_not_claim_more_shared_memory_than_it_has_segments(self) -> None:
        sink = _Sink(1 << 20, max_buffered_segments=4)
        await sink.open(100, sparse=True)
        assert isinstance(sink._pool, segment_buffers_mod.SharedPool)
        assert sink._pool.buf.nbytes < 2 << 20  # one 1 MiB slot, not four
        await sink.abort()

    async def test_blocks_read_the_segment_from_the_spool_in_order(self, tmp_path: Path) -> None:
        seen: list[bytes] = []

        class _Blocks(_Sink):
            async def flush_segment(self, segment: FlushableSegment) -> None:
                seen.extend([bytes(block) async for block in segment.blocks(1000)])

        sink = _Blocks(storage="spool", spool_dir=tmp_path)
        await sink.open(2500, sparse=True)
        writer = await sink.begin_segment(0, 0, 2500)
        await writer.write_at(0, bytes(range(256)) * 9 + b"\x01" * 196)
        await writer.complete()
        await sink.commit()

        assert [len(block) for block in seen] == [1000, 1000, 500]
        assert b"".join(seen) == bytes(range(256)) * 9 + b"\x01" * 196

    async def test_the_spool_is_removed_after_a_failed_flush(self, tmp_path: Path) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path, fail_flush_of=0)
        await sink.open(_SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)

        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await sink.commit()
        await sink.abort()

        assert _spool_files(tmp_path) == []

    async def test_a_failed_flush_leaves_the_slot_unused_instead_of_reusing_it(self, tmp_path: Path) -> None:
        sink = _Sink(max_buffered_segments=1, storage="spool", spool_dir=tmp_path, fail_flush_of=0)
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 0xFF)
        await _settle()

        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await sink.begin_segment(1, _SEG, _SEG)
        with pytest.raises(RuntimeError, match="synthetic flush failure"):
            await sink.begin_segment(1, _SEG, _SEG)
        # Neither zeroed for reuse nor handed to the next segment.
        assert bytes(await sink._pool.read(0, _SEG)) == b"\xff" * _SEG
        assert ("flush-start", 1) not in sink.events
        await sink.abort()

    async def test_a_slot_that_cannot_be_zeroed_fails_the_export_instead_of_being_reused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def broken(fd: int, offset: int, length: int) -> None:
            raise OSError("synthetic zeroing failure")

        monkeypatch.setattr(segment_buffers_mod, "zero_range", broken)
        sink = _Sink(max_buffered_segments=1, storage="spool", spool_dir=tmp_path)
        await sink.open(2 * _SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)  # touches the slot, so releasing it must zero it

        with pytest.raises(OSError, match="synthetic zeroing failure"):
            await asyncio.wait_for(sink.begin_segment(1, _SEG, _SEG), _TIMEOUT)
        await sink.abort()
        assert _spool_files(tmp_path) == []

    async def test_closing_the_spool_waits_for_a_write_whose_caller_was_cancelled(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        await sink.open(_SEG, sparse=True)
        writer = await sink.begin_segment(0, 0, _SEG)
        started = asyncio.Event()
        release = asyncio.Event()
        spool = sink._pool
        assert isinstance(spool, segment_buffers_mod.Spool)
        real_run = spool.run

        async def slow_run(call: Any, *args: Any, **kwargs: Any) -> Any:
            started.set()
            await release.wait()
            return await real_run(call, *args, **kwargs)

        monkeypatch.setattr(spool, "run", slow_run)
        task = asyncio.ensure_future(writer.write_at(0, b"x"))
        await asyncio.wait_for(started.wait(), _TIMEOUT)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        release.set()
        await asyncio.wait_for(sink.abort(), _TIMEOUT)
        assert _spool_files(tmp_path) == []

    async def test_cancelling_abort_neither_discards_early_nor_stops_the_flush(self) -> None:
        sink = _Sink()
        gate = sink.block_flushes()
        await sink.open(_SEG, sparse=True)
        await _fill(sink, 0, _SEG, 1)
        await asyncio.wait_for(sink.flush_started.wait(), _TIMEOUT)

        aborting: asyncio.Task[AbortOutcome] = asyncio.ensure_future(sink.abort())
        await _settle()
        aborting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await aborting
        assert "discard" not in sink.names()

        gate.set()
        await asyncio.wait_for(sink.abort(), _TIMEOUT)  # a later abort still finishes the job
        assert sink.names().index("flush-end") < sink.names().index("discard")


class TestCancelledOpen:
    """A cancelled ``open`` leaves the thread creating the buffer running; what it makes is still released.
    ``open``'s own cleanup waits for that thread, so each test releases it before awaiting ``open``."""

    async def test_a_spool_created_after_the_cancel_is_removed_by_abort(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        started, release = threading.Event(), threading.Event()
        real = segment_buffers_mod.Spool._create

        def slow(self: segment_buffers_mod.Spool) -> None:
            started.set()
            release.wait(_TIMEOUT)
            real(self)

        monkeypatch.setattr(segment_buffers_mod.Spool, "_create", slow)
        sink = _Sink(storage="spool", spool_dir=tmp_path)
        opening = asyncio.ensure_future(sink.open(_SEG, sparse=True))
        await asyncio.to_thread(started.wait, _TIMEOUT)
        opening.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(opening, _TIMEOUT)
        await asyncio.wait_for(sink.abort(), _TIMEOUT)

        assert _spool_files(tmp_path) == []

    async def test_shared_memory_created_after_the_cancel_is_unlinked_by_abort(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        started, release = threading.Event(), threading.Event()
        names: list[str] = []
        real = shared_memory.SharedMemory

        def slow(*args: object, **kwargs: object) -> shared_memory.SharedMemory:
            started.set()
            release.wait(_TIMEOUT)
            shm = real(*args, **kwargs)  # type: ignore[arg-type]
            names.append(shm.name)
            return shm

        monkeypatch.setattr(shared_memory, "SharedMemory", slow)
        sink = _Sink()
        opening = asyncio.ensure_future(sink.open(_SEG, sparse=True))
        await asyncio.to_thread(started.wait, _TIMEOUT)
        opening.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(opening, _TIMEOUT)
        await asyncio.wait_for(sink.abort(), _TIMEOUT)

        with pytest.raises(FileNotFoundError):
            real(name=names[0])
