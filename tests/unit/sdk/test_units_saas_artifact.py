"""Unit tests for ``synology_apm_repo.sdk.units.content.saas_artifact``."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from synology_apm_repo.sdk.units.content.saas_artifact import LazyArtifact


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
        """``size`` stays a *synchronous* property on ``ContentSource``
        and a property cannot await, so the one implementer
        whose size genuinely isn't known without I/O reports ``None``
        until the artifact has actually been assembled — the ``int |
        None`` half of that contract. Once built, it reflects the
        built bytes' length.
        """

        async def build() -> bytes:
            return b"hello world"

        artifact = LazyArtifact(build)
        assert artifact.size is None
        await artifact.read()
        assert artifact.size == 11


class TestSupportsConcurrentExport:
    async def test_always_false(self) -> None:
        async def build() -> bytes:
            return b"data"

        assert LazyArtifact(build).supports_concurrent_export is False


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


class TestStream:
    async def test_stream_yields_all_bytes_in_order(self) -> None:
        async def build() -> bytes:
            return b"x" * 100

        artifact = LazyArtifact(build)
        blocks = [block async for block in artifact.stream(block=30)]
        assert [off for off, _ in blocks] == [0, 30, 60, 90]
        assert b"".join(chunk for _, chunk in blocks) == b"x" * 100

    async def test_stream_is_cancelled_by_cancelling_its_task(self) -> None:
        """Cancellation is ``asyncio.CancelledError`` arriving at the
        next ``await``, which for a ``LazyArtifact`` is its one real
        suspension point, the ``build`` callback: nothing further
        executes, so nothing is yielded to the consumer.
        """
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
        result = await artifact.export_to(dst)
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

        await artifact.export_to(tmp_path / "out.bin", progress=progress)
        assert calls == [(10, 10)]


class TestBuildFailurePropagates:
    async def test_exception_from_build_surfaces_on_first_access_not_construction(self) -> None:
        async def failing_build() -> bytes:
            raise ValueError("cannot assemble this META shape")

        artifact = LazyArtifact(failing_build)  # must not raise here
        with pytest.raises(ValueError, match="cannot assemble"):
            await artifact.read()
