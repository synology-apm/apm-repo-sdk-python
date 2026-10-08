"""Unit tests for ``synology_apm_repo.sdk.units.content.saas_artifact``."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk.dedup.export_sink import ExportWriter
from synology_apm_repo.sdk.export import LocalFileSink, run_export
from synology_apm_repo.sdk.units.content.saas_artifact import LazyArtifact
from unit.sdk.dedup_export_fakes import SegmentCollector


class TestLazyBuild:
    async def test_build_is_not_called_until_first_access(self) -> None:
        calls: list[int] = []

        async def build() -> bytes:
            calls.append(1)
            return b"data"

        artifact = LazyArtifact(build)
        assert calls == []
        await artifact.read()
        assert calls == [1]

    async def test_build_is_called_at_most_once(self) -> None:
        calls: list[int] = []

        async def build() -> bytes:
            calls.append(1)
            return b"data"

        artifact = LazyArtifact(build)
        await artifact.read()
        await artifact.read()
        _ = artifact.size
        assert calls == [1]


class TestSize:
    async def test_size_is_unknown_until_built_then_matches_built_bytes_length(self) -> None:
        """``size`` is synchronous, so it is ``None`` until built and then the
        built bytes' length."""

        async def build() -> bytes:
            return b"hello world"

        artifact = LazyArtifact(build)
        assert artifact.size is None
        await artifact.read()
        assert artifact.size == 11


@faithful_to(ExportWriter)
class _CollectingSink:
    def __init__(self) -> None:
        self.writes: list[tuple[int, bytes]] = []

    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self.writes.append((offset, bytes(data)))


class TestExportToSink:
    async def test_writes_the_assembled_bytes_once_and_reports_progress(self) -> None:
        async def build() -> bytes:
            return b"hello"

        sink = _CollectingSink()
        progress: list[int] = []

        async def _progress(written: int) -> None:
            progress.append(written)

        result = await LazyArtifact(build).export_range(sink, 0, 5, progress=_progress)  # type: ignore[arg-type]

        assert sink.writes == [(0, b"hello")]
        assert progress == [5]
        assert (result.bytes_written, result.logical_size) == (5, 5)

    async def test_an_empty_artifact_writes_nothing(self) -> None:
        async def build() -> bytes:
            return b""

        sink = _CollectingSink()
        result = await LazyArtifact(build).export_range(sink, 0, 0)  # type: ignore[arg-type]
        assert sink.writes == []
        assert result.bytes_written == 0

    async def test_export_to_creates_the_file_at_the_assembled_length(self, tmp_path: Path) -> None:
        async def build() -> bytes:
            return b"data"

        dst = tmp_path / "out.eml"
        await run_export(LazyArtifact(build), LocalFileSink(dst, staged=False))
        assert dst.read_bytes() == b"data"


class TestRead:
    async def test_read_default_returns_everything(self) -> None:
        async def build() -> bytes:
            return b"hello world"

        artifact = LazyArtifact(build)
        assert await artifact.read() == b"hello world"

    async def test_read_with_offset_and_length(self) -> None:
        async def build() -> bytes:
            return b"hello world"

        artifact = LazyArtifact(build)
        assert await artifact.read(6, 5) == b"world"

    async def test_read_offset_only(self) -> None:
        async def build() -> bytes:
            return b"hello world"

        artifact = LazyArtifact(build)
        assert await artifact.read(6) == b"world"

    async def test_negative_offset_raises(self) -> None:
        """A negative offset/length raises ``ValueError`` (as in every
        ``ContentSource``) instead of falling into negative-slice semantics."""

        async def build() -> bytes:
            return b"hello world"

        artifact = LazyArtifact(build)
        with pytest.raises(ValueError, match="non-negative"):
            await artifact.read(-1)

    async def test_negative_length_raises(self) -> None:
        async def build() -> bytes:
            return b"hello world"

        artifact = LazyArtifact(build)
        with pytest.raises(ValueError, match="non-negative"):
            await artifact.read(0, -1)


class TestStream:
    async def test_stream_yields_all_bytes_in_order(self) -> None:
        async def build() -> bytes:
            return b"x" * 100

        artifact = LazyArtifact(build)
        blocks = [block async for block in artifact.stream(block=30)]
        assert [off for off, _ in blocks] == [0, 30, 60, 90]
        assert b"".join(chunk for _, chunk in blocks) == b"x" * 100

    async def test_stream_is_cancelled_by_cancelling_its_task(self) -> None:
        """Cancelling during ``build`` (the only suspension point) yields
        nothing to the consumer."""
        started = asyncio.Event()
        release = asyncio.Event()
        blocks: list[tuple[int, bytes]] = []

        async def build() -> bytes:
            started.set()
            await release.wait()
            return b"x" * 100

        artifact = LazyArtifact(build)

        async def consume() -> None:
            blocks.extend([block async for block in artifact.stream(block=10)])

        task = asyncio.ensure_future(consume())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert blocks == []

    async def test_empty_artifact_streams_nothing(self) -> None:
        async def build() -> bytes:
            return b""

        artifact = LazyArtifact(build)
        assert [block async for block in artifact.stream()] == []


class TestExportTo:
    async def test_writes_the_built_bytes_to_disk(self, tmp_path: Path) -> None:
        async def build() -> bytes:
            return b"exported content"

        artifact = LazyArtifact(build)
        dst = tmp_path / "out.bin"
        result = await run_export(artifact, LocalFileSink(dst, staged=False))
        assert dst.read_bytes() == b"exported content"
        assert result.bytes_written == len(b"exported content")
        assert result.logical_size == result.bytes_written
        assert result.holes == 0
        assert result.zeros == 0

    async def test_calls_progress_with_final_totals(self, tmp_path: Path) -> None:
        async def build() -> bytes:
            return b"1234567890"

        artifact = LazyArtifact(build)
        calls: list[tuple[int, int]] = []

        async def progress(done: int, total: int) -> None:
            calls.append((done, total))

        await run_export(artifact, LocalFileSink(tmp_path / "out.bin", staged=False), progress=progress)
        assert calls == [(10, 10)]


class TestBuildFailurePropagates:
    async def test_exception_from_build_surfaces_on_first_access_not_construction(self) -> None:
        async def failing_build() -> bytes:
            raise ValueError("cannot assemble this META shape")

        artifact = LazyArtifact(failing_build)  # must not raise here
        with pytest.raises(ValueError, match="cannot assemble"):
            await artifact.read()


class TestExportRange:
    async def test_a_sub_range_is_written_from_offset_zero(self) -> None:
        async def build() -> bytes:
            return b"hello world"

        sink = _CollectingSink()
        result = await LazyArtifact(build).export_range(sink, 6, 11)  # type: ignore[arg-type]
        assert sink.writes == [(0, b"world")]
        assert (result.bytes_written, result.logical_size) == (5, 5)

    async def test_a_range_outside_the_artifact_is_rejected(self) -> None:
        async def build() -> bytes:
            return b"hello"

        artifact = LazyArtifact(build)
        with pytest.raises(ValueError, match="is not inside"):
            await artifact.export_range(_CollectingSink(), 2, 6)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="is not inside"):
            await artifact.planned_bytes(2, 6)

    async def test_planned_bytes_assembles_the_artifact_and_is_the_range_length(self) -> None:
        async def build() -> bytes:
            return b"hello world"

        artifact = LazyArtifact(build)
        assert await artifact.planned_bytes(2, 7) == 5
        assert artifact.size == 11


class TestSegmentedExport:
    async def test_the_artifact_comes_out_whole_across_segments(self) -> None:
        data = b"0123456789" * 1000

        async def build() -> bytes:
            return data

        sink = SegmentCollector(4096)
        result = await run_export(LazyArtifact(build), sink)

        assert bytes(sink.output) == data
        assert result.bytes_written == len(data)
