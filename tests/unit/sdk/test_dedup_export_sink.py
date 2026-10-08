"""Unit tests for ``synology_apm_repo.sdk.dedup.export_sink``."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from synology_apm_repo.sdk.dedup.export_sink import (
    AbortOutcome,
    OffsetWriter,
    RandomAccessExportSink,
    SinkCaps,
    WorkerTarget,
    needs_zero_fill,
    run_sink_export,
)
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileDescriptor


class TestNeedsZeroFill:
    """One rule for "must this export write its HOLE/ZERO ranges?"."""

    @staticmethod
    def _sink(*, supports_sparse: bool, preallocated: bool) -> _RecordingSink:
        sink = _RecordingSink()
        sink.caps = SinkCaps(supports_sparse=supports_sparse)
        sink.preallocated = preallocated
        return sink

    @pytest.mark.parametrize(
        ("supports_sparse", "sparse", "expected"),
        [
            pytest.param(True, True, False, id="a_sparse_export_into_a_sparse_capable_sink_writes_none"),
            pytest.param(True, False, True, id="a_dense_export_writes_them"),
            pytest.param(False, True, True, id="a_sink_that_cannot_leave_holes_gets_them_even_for_a_sparse_export"),
        ],
    )
    def test_needs_zero_fill(self, supports_sparse: bool, sparse: bool, expected: bool) -> None:
        assert (
            needs_zero_fill(self._sink(supports_sparse=supports_sparse, preallocated=False), sparse=sparse) is expected
        )

    def test_a_preallocated_sink_needs_none_either_way(self) -> None:
        assert not needs_zero_fill(self._sink(supports_sparse=True, preallocated=True), sparse=False)
        assert not needs_zero_fill(self._sink(supports_sparse=False, preallocated=True), sparse=True)


class _RecordingSink(RandomAccessExportSink):
    """A ``RandomAccessExportSink`` that records every call, optionally failing one."""

    caps = SinkCaps(supports_sparse=False)
    preallocated = False

    def __init__(self, *, fail_on: str | None = None) -> None:
        self.calls: list[tuple[str, object]] = []
        self._fail_on = fail_on

    def _record(self, name: str, detail: object = None) -> None:
        self.calls.append((name, detail))
        if name == self._fail_on:
            raise RuntimeError(f"synthetic {name} failure")

    async def open(self, logical_size: int, *, sparse: bool) -> None:
        self._record("open", (logical_size, sparse))

    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self._record("write_at", (offset, bytes(data)))

    async def write_zero(self, offset: int, length: int) -> None:
        self._record("write_zero", (offset, length))

    async def commit(self) -> None:
        self._record("commit")

    async def abort(self) -> AbortOutcome:
        self._record("abort")
        return AbortOutcome(kept=False, ever_written=False)


class _WorkerSink(_RecordingSink):
    """A ``_RecordingSink`` that also takes direct worker writes."""

    def __init__(self, target: WorkerTarget | None) -> None:
        super().__init__()
        self._target = target

    def worker_target(self) -> WorkerTarget | None:
        return self._target

    def note_worker_write(self) -> None:
        self.calls.append(("note_worker_write", None))


class TestRunSinkExport:
    async def test_runs_open_body_commit_in_order_and_returns_the_bodys_result(self) -> None:
        sink = _RecordingSink()

        async def body() -> str:
            await sink.write_at(0, b"x")
            return "result"

        assert await run_sink_export(sink, 10, sparse=False, body=body) == "result"
        assert [name for name, _ in sink.calls] == ["open", "write_at", "commit"]
        assert sink.calls[0] == ("open", (10, False))

    async def test_a_body_failure_aborts_once_and_reraises(self) -> None:
        sink = _RecordingSink()

        async def body() -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await run_sink_export(sink, 10, sparse=True, body=body)
        assert [name for name, _ in sink.calls] == ["open", "abort"]

    async def test_an_open_failure_still_aborts(self) -> None:
        sink = _RecordingSink(fail_on="open")

        async def body() -> None:
            raise AssertionError("body must not run")

        with pytest.raises(RuntimeError, match="synthetic open failure"):
            await run_sink_export(sink, 10, sparse=True, body=body)
        assert [name for name, _ in sink.calls] == ["open", "abort"]

    async def test_a_commit_failure_aborts(self) -> None:
        sink = _RecordingSink(fail_on="commit")

        async def body() -> None:
            return None

        with pytest.raises(RuntimeError, match="synthetic commit failure"):
            await run_sink_export(sink, 10, sparse=True, body=body)
        assert [name for name, _ in sink.calls] == ["open", "commit", "abort"]

    async def test_an_abort_failure_never_masks_the_original_error(self) -> None:
        sink = _RecordingSink(fail_on="abort")

        async def body() -> None:
            raise ValueError("the real failure")

        with pytest.raises(ValueError, match="the real failure") as exc_info:
            await run_sink_export(sink, 10, sparse=True, body=body)
        # The abort failure is reported on the real error, not dropped.
        assert len(exc_info.value.__notes__) == 1
        assert exc_info.value.__notes__[0].startswith("cleanup also failed:")

    async def test_cancellation_aborts_and_propagates(self) -> None:
        sink = _RecordingSink()
        entered = asyncio.Event()

        async def body() -> None:
            entered.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(run_sink_export(sink, 10, sparse=True, body=body))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert [name for name, _ in sink.calls] == ["open", "abort"]


class TestRandomAccessExportSink:
    """The one-segment case: the sink is its own writer and the whole export is segment 0."""

    async def test_it_has_no_fixed_segment_size_and_its_single_segment_writer_is_itself(self) -> None:
        sink = _RecordingSink()
        assert sink.segment_size is None
        assert await sink.begin_segment(0, 0, 4096) is sink

    async def test_completing_its_segment_does_nothing(self) -> None:
        sink = _RecordingSink()
        await sink.complete()
        assert sink.calls == []

    def test_by_default_it_takes_no_worker_writes(self) -> None:
        sink = _RecordingSink()
        assert sink.worker_target() is None
        sink.note_worker_write()  # a no-op
        assert sink.calls == []

    def test_a_subclass_overriding_the_worker_methods_supplies_its_target_and_hears_the_note(
        self, tmp_path: Path
    ) -> None:
        target = WorkerTarget(LocalFileDescriptor(str(tmp_path / "out.bin")))
        sink = _WorkerSink(target)
        assert sink.worker_target() == target
        sink.note_worker_write()
        assert sink.calls == [("note_worker_write", None)]


class TestOffsetWriter:
    async def test_shifts_writes_and_zero_fills(self) -> None:
        inner = _RecordingSink()
        writer = OffsetWriter(inner, 1000)
        await writer.write_at(5, b"ab")
        await writer.write_zero(10, 20)
        assert inner.calls == [("write_at", (1005, b"ab")), ("write_zero", (1010, 20))]

    def test_a_negative_base_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="must not be negative"):
            OffsetWriter(_RecordingSink(), -1)

    def test_reports_the_inner_writers_caps_and_preallocation(self) -> None:
        inner = _RecordingSink()
        inner.preallocated = True
        assert OffsetWriter(inner, 4096).caps == inner.caps
        assert OffsetWriter(inner, 4096).preallocated

    def test_worker_target_adds_its_own_base_offset_to_the_inner_one(self, tmp_path: Path) -> None:
        descriptor = LocalFileDescriptor(str(tmp_path / "out.bin"))
        inner = _WorkerSink(WorkerTarget(descriptor, 100))
        assert OffsetWriter(inner, 50).worker_target() == WorkerTarget(descriptor, 150)
        assert OffsetWriter(OffsetWriter(inner, 50), 25).worker_target() == WorkerTarget(descriptor, 175)

    def test_worker_target_is_none_when_the_inner_writer_takes_no_worker_writes(self) -> None:
        assert OffsetWriter(_RecordingSink(), 50).worker_target() is None
        assert OffsetWriter(_WorkerSink(None), 50).worker_target() is None

    def test_note_worker_write_reaches_the_inner_writer(self) -> None:
        inner = _WorkerSink(None)
        OffsetWriter(inner, 50).note_worker_write()
        assert inner.calls == [("note_worker_write", None)]
        OffsetWriter(_RecordingSink(), 50).note_worker_write()  # nothing to notify
