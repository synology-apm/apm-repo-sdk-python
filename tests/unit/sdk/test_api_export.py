"""Unit tests for ``synology_apm_repo.sdk.api.export``."""

from __future__ import annotations

import asyncio
import types
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk.api import export as api_export_mod
from synology_apm_repo.sdk.dedup.buffered_export_sink import BufferedExportSink, FlushableSegment
from synology_apm_repo.sdk.dedup.export_scheduler import ExportTuning
from synology_apm_repo.sdk.dedup.export_sink import (
    AbortOutcome,
    ExportWriter,
    RandomAccessExportSink,
    SegmentWriter,
    SinkCaps,
)
from synology_apm_repo.sdk.dedup.extent import ExportResult
from synology_apm_repo.sdk.dedup.pool import BucketReaderCache
from synology_apm_repo.sdk.export import LocalFileSink, run_export
from synology_apm_repo.sdk.units.base import ContentSource
from synology_apm_repo.sdk.units.content.saas_artifact import LazyArtifact


class _RecordingSink(RandomAccessExportSink):
    """A ``RandomAccessExportSink`` that records every lifecycle call."""

    caps = SinkCaps()
    preallocated = False

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def open(self, logical_size: int, *, sparse: bool) -> None:
        self.calls.append(("open", (logical_size, sparse)))

    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self.calls.append(("write_at", (offset, bytes(data))))

    async def write_zero(self, offset: int, length: int) -> None:
        self.calls.append(("write_zero", (offset, length)))

    async def commit(self) -> None:
        self.calls.append(("commit", None))

    async def abort(self) -> AbortOutcome:
        self.calls.append(("abort", None))
        return AbortOutcome(kept=False, ever_written=False)


@faithful_to(ContentSource)
class _SizedContent:
    """A content source with a known size whose export behaviour the test
    supplies."""

    def __init__(self, size: int, *, fail_with: BaseException | None = None) -> None:
        self.size = size
        self._fail_with = fail_with
        self.received_sparse: bool | None = None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        assert length == 0, "only run_export's zero-length size probe may read"
        return b""

    def stream(self, block: int = 0) -> object:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        self.received_sparse = sparse
        await sink.write_at(0, b"abc")
        if self._fail_with is not None:
            raise self._fail_with
        return ExportResult(bytes_written=3, logical_size=self.size, holes=0, zeros=0)


class TestRunExport:
    async def test_opens_at_the_content_size_writes_then_commits(self) -> None:
        sink = _RecordingSink()
        content = _SizedContent(10)

        result = await run_export(content, sink, sparse=False)  # type: ignore[arg-type]

        assert [name for name, _ in sink.calls] == ["open", "write_at", "commit"]
        assert sink.calls[0] == ("open", (10, False))
        assert content.received_sparse is False
        assert (result.bytes_written, result.logical_size) == (3, 10)

    async def test_forwards_progress_to_the_content(self) -> None:
        seen: list[tuple[int, int]] = []

        async def _progress(done: int, total: int) -> None:
            seen.append((done, total))

        async def build() -> bytes:
            return b"hello"

        await run_export(LazyArtifact(build), _RecordingSink(), progress=_progress)

        assert seen == [(5, 5)]

    async def test_assembles_a_lazily_built_source_to_learn_its_size(self, tmp_path: Path) -> None:
        async def build() -> bytes:
            return b"assembled bytes"

        artifact = LazyArtifact(build)
        assert artifact.size is None
        dst = tmp_path / "out.eml"

        result = await run_export(artifact, LocalFileSink(dst, staged=False))

        assert dst.read_bytes() == b"assembled bytes"
        assert result.bytes_written == len(b"assembled bytes")

    async def test_a_failure_aborts_the_sink_once_and_reraises(self) -> None:
        sink = _RecordingSink()

        with pytest.raises(RuntimeError, match="boom"):
            await run_export(_SizedContent(10, fail_with=RuntimeError("boom")), sink)  # type: ignore[arg-type]

        assert [name for name, _ in sink.calls] == ["open", "write_at", "abort"]

    async def test_cancellation_aborts_the_sink_and_propagates(self) -> None:
        sink = _RecordingSink()

        with pytest.raises(asyncio.CancelledError):
            await run_export(_SizedContent(10, fail_with=asyncio.CancelledError()), sink)  # type: ignore[arg-type]

        assert [name for name, _ in sink.calls] == ["open", "write_at", "abort"]

    async def test_a_staged_local_sink_is_renamed_on_success_and_removed_on_failure(self, tmp_path: Path) -> None:
        ok = tmp_path / "ok.bin"
        await run_export(_SizedContent(3), LocalFileSink(ok, staged=True))  # type: ignore[arg-type]
        assert ok.read_bytes() == b"abc"
        assert not (tmp_path / "ok.bin.part").exists()

        bad = tmp_path / "bad.bin"
        with pytest.raises(RuntimeError, match="boom"):
            await run_export(
                _SizedContent(3, fail_with=RuntimeError("boom")),  # type: ignore[arg-type]
                LocalFileSink(bad, staged=True),
            )
        assert not bad.exists()
        assert not (tmp_path / "bad.bin.part").exists()

    async def test_content_of_unknown_size_that_stays_unknown_is_rejected(self) -> None:
        content = _SizedContent(0)
        content.size = None  # type: ignore[assignment]

        with pytest.raises(ValueError, match="size is unknown"):
            await run_export(content, _RecordingSink())  # type: ignore[arg-type]


_SEG = 4096


@faithful_to(ContentSource)
class _RangeContent:
    """In-memory content whose ``export_range`` writes its own bytes and records each call."""

    def __init__(self, data: bytes, *, fail_at: int | None = None) -> None:
        self.data = data
        self.size = len(data)
        self.calls: list[tuple[int, int]] = []
        self.tunings: list[ExportTuning | None] = []
        self._fail_at = fail_at

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        writer: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: ExportTuning | None = None,
    ) -> ExportResult:
        self.calls.append((start, end))
        self.tunings.append(tuning)
        if start == self._fail_at:
            raise RuntimeError(f"synthetic failure at {start}")
        await writer.write_at(0, self.data[start:end])
        if callable(progress):
            await progress(end - start)
        return ExportResult(bytes_written=end - start, logical_size=end - start, holes=1, zeros=2)


class _CollectingSink(BufferedExportSink):
    """Concatenates every flushed segment."""

    def __init__(self, segment_size: int = _SEG, *, max_buffered_segments: int = 2) -> None:
        super().__init__(segment_size, max_buffered_segments=max_buffered_segments)
        self.output = bytearray()
        self.events: list[str] = []

    async def create_destination(self, logical_size: int, *, sparse: bool) -> None:
        self.events.append("create")

    async def flush_segment(self, segment: FlushableSegment) -> None:
        self.output += await segment.read_all()
        self.events.append(f"flush {segment.index}")

    async def finalize_destination(self) -> None:
        self.events.append("finalize")

    async def discard_destination(self) -> bool:
        self.events.append("discard")
        return False


class _SlowFlushSink(_CollectingSink):
    """One buffered segment; each flush but the last waits until the next
    segment is waiting for that slot, then takes ``flush_seconds`` on
    ``clock``, which the test makes the export's clock."""

    def __init__(self, segments: int, flush_seconds: float) -> None:
        super().__init__(max_buffered_segments=1)
        self.clock = 0.0
        self._flush_seconds = flush_seconds
        self._begun = [asyncio.Event() for _ in range(segments)]

    async def begin_segment(self, index: int, start: int, length: int) -> SegmentWriter:
        self._begun[index].set()
        return await super().begin_segment(index, start, length)

    async def flush_segment(self, segment: FlushableSegment) -> None:
        if segment.index + 1 < len(self._begun):
            await self._begun[segment.index + 1].wait()
        self.clock += self._flush_seconds
        await super().flush_segment(segment)


class TestRunExportSegmented:
    async def test_each_segment_is_exported_as_its_own_range_and_the_bytes_arrive_in_order(self) -> None:
        data = bytes(range(256)) * 40  # 10240 bytes: two full segments and a short last one
        content = _RangeContent(data)
        sink = _CollectingSink()

        result = await run_export(content, sink)

        assert content.calls == [(0, 4096), (4096, 8192), (8192, 10240)]
        assert bytes(sink.output) == data
        assert sink.events == ["create", "flush 0", "flush 1", "flush 2", "finalize"]
        assert (result.bytes_written, result.logical_size, result.holes, result.zeros) == (10240, 10240, 3, 6)

    async def test_a_content_that_fits_in_one_segment_is_one_range(self) -> None:
        content = _RangeContent(b"x" * 100)
        sink = _CollectingSink()

        await run_export(content, sink)

        assert content.calls == [(0, 100)]
        assert bytes(sink.output) == b"x" * 100

    async def test_empty_content_creates_and_finalizes_without_a_segment(self) -> None:
        content = _RangeContent(b"")
        sink = _CollectingSink()

        result = await run_export(content, sink)

        assert content.calls == []
        assert sink.events == ["create", "finalize"]
        assert result.logical_size == 0

    async def test_progress_is_reported_against_the_whole_export(self) -> None:
        content = _RangeContent(b"x" * 10240)
        seen: list[tuple[int, int]] = []

        async def _progress(done: int, total: int) -> None:
            seen.append((done, total))

        await run_export(content, _CollectingSink(), progress=_progress)

        assert seen == [(4096, 10240), (8192, 10240), (10240, 10240)]

    async def test_every_segment_shares_one_reader_cache_and_keeps_the_callers_other_knobs(self) -> None:
        content = _RangeContent(b"x" * 10240)

        await run_export(content, _CollectingSink(), tuning=ExportTuning(max_concurrent_reads=4))

        caches = [tuning.export_cache for tuning in content.tunings if tuning is not None]
        assert len(caches) == 3 and caches[0] is not None
        assert all(cache is caches[0] for cache in caches)
        assert {tuning.max_concurrent_reads for tuning in content.tunings if tuning is not None} == {4}

    async def test_a_cache_the_caller_supplied_is_the_one_shared(self) -> None:
        content = _RangeContent(b"x" * 8192)
        mine = BucketReaderCache()

        await run_export(content, _CollectingSink(), tuning=ExportTuning(export_cache=mine))

        assert all(tuning is not None and tuning.export_cache is mine for tuning in content.tunings)

    async def test_time_spent_waiting_for_the_sink_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        content = _RangeContent(b"x" * 12288)
        sink = _SlowFlushSink(segments=3, flush_seconds=0.05)
        monkeypatch.setattr(api_export_mod, "time", types.SimpleNamespace(perf_counter=lambda: sink.clock))

        result = await asyncio.wait_for(run_export(content, sink), 10)

        assert result.sink_wait_seconds == pytest.approx(0.1)  # segments 1 and 2 each waited out one flush

    async def test_a_sink_that_is_not_segmented_reports_no_wait(self) -> None:
        result = await run_export(_SizedContent(10), _RecordingSink())  # type: ignore[arg-type]
        assert result.sink_wait_seconds == 0.0

    async def test_a_failing_range_aborts_the_sink_and_discards_the_destination(self) -> None:
        content = _RangeContent(b"x" * 10240, fail_at=4096)
        sink = _CollectingSink()

        with pytest.raises(RuntimeError, match="synthetic failure at 4096"):
            await run_export(content, sink)

        assert sink.events[0] == "create"
        assert sink.events[-1] == "discard"
        assert "finalize" not in sink.events

    async def test_cancellation_aborts_the_sink(self, monkeypatch: pytest.MonkeyPatch) -> None:
        content = _RangeContent(b"x" * 10240)
        sink = _CollectingSink()
        entered = asyncio.Event()

        async def blocked(*args: object, **kwargs: object) -> ExportResult:
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        monkeypatch.setattr(content, "export_range", blocked)
        task = asyncio.ensure_future(run_export(content, sink))
        await entered.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert sink.events[-1] == "discard"

    @pytest.mark.parametrize("size", [0, -1, 1000])
    async def test_a_bad_segment_size_is_rejected_before_the_sink_is_opened(self, size: int) -> None:
        class _Bad(_RecordingSink):
            segment_size = size

        sink = _Bad()
        with pytest.raises(ValueError, match="multiple of 4096"):
            await run_export(_SizedContent(10), sink)  # type: ignore[arg-type]
        assert sink.calls == []

    async def test_a_lazily_sized_content_is_sized_before_the_segments_are_cut(self) -> None:
        async def build() -> bytes:
            return b"y" * 5000

        sink = _CollectingSink()
        await run_export(LazyArtifact(build), sink)

        assert bytes(sink.output) == b"y" * 5000
