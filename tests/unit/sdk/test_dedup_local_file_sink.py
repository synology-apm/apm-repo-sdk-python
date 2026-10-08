"""Unit tests for ``synology_apm_repo.sdk.dedup.local_file_sink``."""

from __future__ import annotations

import asyncio
import errno
import os
import pickle
import threading
from collections.abc import Callable
from pathlib import Path

import pytest

from synology_apm_repo.sdk import positional_io
from synology_apm_repo.sdk.dedup import local_file_io
from synology_apm_repo.sdk.dedup import local_file_sink as local_file_sink_mod
from synology_apm_repo.sdk.dedup.export_sink import AbortOutcome, WorkerTarget, run_sink_export
from synology_apm_repo.sdk.dedup.local_file_sink import (
    _WRITER_QUEUE_SIZE,
    LocalFileDescriptor,
    LocalFileSink,
    _DataJob,
    _GapJob,
    _WriterThread,
)


class _GatedPwrite:
    """A ``pwrite`` stand-in that parks the writer thread until ``release`` is
    set, then applies (or, with ``fail``, refuses) the write."""

    def __init__(self, *, fail: bool = False) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.fail = fail
        self.calls: list[tuple[int, int]] = []
        self._real = positional_io.pwrite

    def __call__(self, fd: int, data: bytes | memoryview, offset: int) -> None:
        self.calls.append((fd, offset))
        self.started.set()
        self.release.wait(10)
        if self.fail:
            raise OSError("synthetic disk-full")
        self._real(fd, data, offset)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> _GatedPwrite:
        monkeypatch.setattr(local_file_sink_mod, "pwrite", self)
        return self

    async def wait_started(self) -> None:
        await asyncio.to_thread(self.started.wait, 10)


def _probe_join(writer: _WriterThread, monkeypatch: pytest.MonkeyPatch) -> threading.Event:
    """Sets the returned event each time a caller enters the join on
    ``writer``'s thread, i.e. once ``close`` is waiting for the thread."""
    entered = threading.Event()
    real_join = writer._thread.join

    def join(timeout: float | None = None) -> None:
        entered.set()
        real_join(timeout)

    monkeypatch.setattr(writer._thread, "join", join)
    return entered


def _probe_blocking_put(writer: _WriterThread, monkeypatch: pytest.MonkeyPatch) -> threading.Event:
    """Sets the returned event once a blocking ``put`` enters ``writer``'s
    queue, i.e. once ``submit`` found the queue full."""
    entered = threading.Event()
    real_put = writer._queue.put

    def put(item: object, block: bool = True, timeout: float | None = None) -> None:
        if block:  # put_nowait() calls put(block=False)
            entered.set()
        real_put(item, block, timeout)

    monkeypatch.setattr(writer._queue, "put", put)
    return entered


async def _entered(event: threading.Event) -> None:
    assert await asyncio.to_thread(event.wait, 10), "the probed call was never entered"


def _failing_pwrite(fd: int, data: bytes | memoryview, offset: int) -> None:
    raise OSError("synthetic disk-full")


def _probe_failure_recorded(writer: _WriterThread, monkeypatch: pytest.MonkeyPatch) -> threading.Event:
    """Sets the returned event once ``writer``'s thread asks for its next job
    with a failure recorded, i.e. once ``error`` holds that failure."""
    recorded = threading.Event()
    real_get = writer._queue.get

    def get(block: bool = True, timeout: float | None = None) -> object:
        if writer.error is not None:
            recorded.set()
        return real_get(block, timeout)

    monkeypatch.setattr(writer._queue, "get", get)
    return recorded


async def _cancelled(task: asyncio.Task[object]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def _noop() -> None:
    pass


async def _write_one_byte(sink: LocalFileSink) -> None:
    await sink.write_at(0, b"x")


class TestLocalFileSinkUnstaged:
    async def test_open_creates_the_file_at_its_full_size_and_its_missing_parents(self, tmp_path: Path) -> None:
        dst = tmp_path / "a" / "b" / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        await sink.open(4096, sparse=True)
        await sink.commit()
        assert dst.stat().st_size == 4096
        assert dst.read_bytes() == bytes(4096)

    async def test_open_replaces_an_existing_file(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 10_000)
        sink = LocalFileSink(dst, staged=False)
        await sink.open(100, sparse=True)
        await sink.commit()
        assert dst.read_bytes() == bytes(100)

    async def test_writes_land_at_their_offsets_in_any_order(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        await sink.open(12, sparse=True)
        await sink.write_at(8, b"cccc")
        await sink.write_at(0, memoryview(b"aaaa"))
        await sink.write_zero(4, 4)
        await sink.commit()
        assert dst.read_bytes() == b"aaaa" + bytes(4) + b"cccc"

    async def test_zero_writes_overwrite_existing_bytes(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        await sink.open(8, sparse=False)
        await sink.write_at(0, b"\xff" * 8)
        await sink.write_zero(2, 4)
        await sink.commit()
        assert dst.read_bytes() == b"\xff\xff" + bytes(4) + b"\xff\xff"

    async def test_worker_target_names_the_destination_itself(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        target = LocalFileSink(dst, staged=False).worker_target()
        assert target == WorkerTarget(LocalFileDescriptor(str(dst)), 0)

    async def test_abort_keeps_a_file_that_has_data_in_it(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        await sink.open(8, sparse=True)
        await sink.write_at(0, b"abcd")
        outcome = await sink.abort()
        assert outcome == AbortOutcome(kept=True, ever_written=True)
        assert dst.read_bytes() == b"abcd" + bytes(4)

    async def test_abort_removes_a_file_nothing_was_written_to(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        await sink.open(8, sparse=True)
        outcome = await sink.abort()
        assert outcome == AbortOutcome(kept=False, ever_written=False)
        assert not dst.exists()

    async def test_abort_leaves_an_existing_file_alone_when_open_failed_before_creating_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"precious")

        def _refuse(*args: object, **kwargs: object) -> None:
            raise PermissionError(errno.EACCES, "synthetic: cannot open for writing")

        monkeypatch.setattr(local_file_sink_mod, "create_presized", _refuse)
        with pytest.raises(PermissionError, match="cannot open for writing"):
            await run_sink_export(LocalFileSink(dst, staged=False), 8, sparse=True, body=_noop)
        assert dst.read_bytes() == b"precious"

    async def test_abort_removes_the_file_when_open_failed_after_creating_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dst = tmp_path / "out.bin"
        real = local_file_io.create_presized

        def _fail_after_open(path: Path, size: int, *, sparse: bool, on_open: Callable[[], None]) -> None:
            real(path, 0, sparse=sparse, on_open=on_open)
            raise OSError(errno.ENOSPC, "synthetic disk-full while sizing")

        monkeypatch.setattr(local_file_sink_mod, "create_presized", _fail_after_open)
        with pytest.raises(OSError, match="disk-full"):
            await run_sink_export(LocalFileSink(dst, staged=False), 8, sparse=True, body=_noop)
        assert not dst.exists()

    async def test_abort_before_open_reports_nothing_kept_or_written(self, tmp_path: Path) -> None:
        sink = LocalFileSink(tmp_path / "never.bin", staged=False)
        assert await sink.abort() == AbortOutcome(kept=False, ever_written=False)

    async def test_abort_before_open_leaves_an_existing_part_file_alone(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        leftover = LocalFileSink(dst, staged=True).path
        leftover.write_bytes(b"from an earlier --keep-partial run")
        sink = LocalFileSink(dst, staged=True, keep_partial=True)
        assert await sink.abort() == AbortOutcome(kept=False, ever_written=False)
        assert leftover.read_bytes() == b"from an earlier --keep-partial run"


class TestLocalFileSinkStaged:
    async def test_bytes_go_to_the_part_file_until_commit_renames_it(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=True)
        await sink.open(4, sparse=True)
        await sink.write_at(0, b"abcd")
        assert sink.path == tmp_path / "out.bin.part"
        assert not dst.exists()
        await sink.commit()
        assert dst.read_bytes() == b"abcd"
        assert not (tmp_path / "out.bin.part").exists()

    async def test_worker_target_names_the_part_file(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        target = LocalFileSink(dst, staged=True).worker_target()
        assert target == WorkerTarget(LocalFileDescriptor(str(tmp_path / "out.bin.part")), 0)

    async def test_abort_deletes_the_part_file_by_default(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=True)
        await sink.open(4, sparse=True)
        await sink.write_at(0, b"abcd")
        outcome = await sink.abort()
        assert outcome == AbortOutcome(kept=False, ever_written=True)
        assert not (tmp_path / "out.bin.part").exists()
        assert not dst.exists()

    async def test_abort_keeps_a_part_file_with_data_when_asked_to(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=True, keep_partial=True)
        await sink.open(4, sparse=True)
        await sink.write_at(0, b"abcd")
        outcome = await sink.abort()
        assert outcome == AbortOutcome(kept=True, ever_written=True)
        assert (tmp_path / "out.bin.part").read_bytes() == b"abcd"

    async def test_abort_removes_an_untouched_part_file_even_when_keeping_partials(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=True, keep_partial=True)
        await sink.open(4, sparse=True)
        outcome = await sink.abort()
        assert outcome == AbortOutcome(kept=False, ever_written=False)
        assert not (tmp_path / "out.bin.part").exists()

    async def test_abort_interrupted_while_closing_the_writer_still_removes_the_part_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A cancellation landing while abort() stops the writer still lets it
        discard the part file, then propagates rather than being swallowed."""
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=True)
        await sink.open(4, sparse=True)
        await sink.write_at(0, b"abcd")
        real_close_writer = sink._close_writer

        async def interrupted_close_writer() -> BaseException | None:
            await real_close_writer()
            raise asyncio.CancelledError

        monkeypatch.setattr(sink, "_close_writer", interrupted_close_writer)
        with pytest.raises(asyncio.CancelledError):
            await sink.abort()
        assert not (tmp_path / "out.bin.part").exists()

    async def test_abort_is_idempotent_and_repeats_its_outcome(self, tmp_path: Path) -> None:
        """``run_export`` aborts a failed export, and the CLI calls ``abort()``
        again for the outcome."""
        sink = LocalFileSink(tmp_path / "out.bin", staged=True, keep_partial=True)
        await sink.open(4, sparse=True)
        await sink.write_at(0, b"abcd")
        first = await sink.abort()
        second = await sink.abort()
        assert first == second == AbortOutcome(kept=True, ever_written=True)
        assert (tmp_path / "out.bin.part").read_bytes() == b"abcd"

    async def test_abort_after_commit_does_not_touch_the_committed_file(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=True)
        await sink.open(4, sparse=True)
        await sink.write_at(0, b"abcd")
        await sink.commit()
        await sink.abort()
        assert dst.read_bytes() == b"abcd"

    async def test_abort_after_commit_keeps_an_unstaged_file_nothing_was_written_through_the_sink_to(
        self, tmp_path: Path
    ) -> None:
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        await sink.open(4, sparse=True)
        await sink.commit()
        await sink.abort()
        assert dst.read_bytes() == bytes(4)


class TestLocalFileSinkDense:
    """A dense export (``sparse=False``) reserves the whole file in ``open`` —
    failing there when there is no room — and then has no zero-fill to write."""

    @staticmethod
    def _preallocate_returning(monkeypatch: pytest.MonkeyPatch, result: bool | BaseException) -> list[tuple[int, int]]:
        calls: list[tuple[int, int]] = []

        def fake(fd: int, size: int) -> bool:
            calls.append((fd, size))
            if isinstance(result, BaseException):
                raise result
            return result

        monkeypatch.setattr(local_file_sink_mod, "preallocate", fake)
        return calls

    @staticmethod
    def _record_zero_fills(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int]]:
        filled: list[tuple[int, int]] = []

        def fake(fd: int, offset: int, length: int) -> None:
            filled.append((offset, length))

        monkeypatch.setattr(local_file_sink_mod, "write_zeros_at", fake)
        return filled

    async def test_open_reserves_the_whole_file_and_reports_it_preallocated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reserved = self._preallocate_returning(monkeypatch, True)
        sink = LocalFileSink(tmp_path / "out.bin", staged=False)

        await sink.open(8192, sparse=False)

        assert [size for _, size in reserved] == [8192]
        assert sink.preallocated is True
        await sink.commit()

    async def test_write_zero_still_zeroes_the_range_on_a_preallocated_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Skipping a zero-fill is the exporter's call (``needs_zero_fill``);
        ``write_zero`` itself always makes the range zero."""
        self._preallocate_returning(monkeypatch, True)
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        await sink.open(8, sparse=False)
        await sink.write_at(0, b"\xff" * 8)
        await sink.write_zero(2, 4)
        await sink.commit()
        assert dst.read_bytes() == b"\xff\xff" + bytes(4) + b"\xff\xff"

    async def test_without_preallocation_write_zero_writes_the_zeros(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._preallocate_returning(monkeypatch, False)
        filled = self._record_zero_fills(monkeypatch)
        sink = LocalFileSink(tmp_path / "out.bin", staged=False)

        await sink.open(8192, sparse=False)
        await sink.write_zero(4096, 4096)
        await sink.commit()

        assert filled == [(4096, 4096)]
        assert sink.preallocated is False

    async def test_a_sparse_export_never_reserves_anything(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reserved = self._preallocate_returning(monkeypatch, AssertionError("a sparse export must not preallocate"))
        sink = LocalFileSink(tmp_path / "out.bin", staged=False)

        await sink.open(8192, sparse=True)
        await sink.commit()

        assert reserved == []

    async def test_no_room_fails_in_open_before_the_body_runs_and_leaves_nothing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._preallocate_returning(monkeypatch, OSError(errno.ENOSPC, "No space left on device"))
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=True)
        ran = False

        async def body() -> None:
            nonlocal ran
            ran = True

        with pytest.raises(OSError) as excinfo:
            await run_sink_export(sink, 8192, sparse=False, body=body)

        assert excinfo.value.errno == errno.ENOSPC
        assert ran is False
        assert not dst.exists()
        assert not (tmp_path / "out.bin.part").exists()

    async def test_a_sparse_open_is_not_preallocated(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._preallocate_returning(monkeypatch, True)
        sink = LocalFileSink(tmp_path / "out.bin", staged=False)
        await sink.open(8192, sparse=True)
        assert sink.preallocated is False
        await sink.commit()


class TestWriterThread:
    """The thread that owns the fd: ordering, backpressure, failure and
    shutdown, against a real file."""

    @staticmethod
    def _start(tmp_path: Path, size: int = 64) -> tuple[Path, int, _WriterThread]:
        path = tmp_path / "w.bin"
        path.write_bytes(bytes(size))
        fd = local_file_io.open_destination(path)
        return path, fd, _WriterThread(fd)

    async def test_applies_data_and_zero_jobs_at_their_offsets(self, tmp_path: Path) -> None:
        path, _, writer = self._start(tmp_path, 12)
        path.write_bytes(b"\xff" * 12)
        await writer.submit(_DataJob(8, b"cccc"))
        await writer.submit(_DataJob(0, memoryview(b"aaaa")))
        await writer.submit(_GapJob(4, 4))
        await writer.close()
        assert path.read_bytes() == b"aaaa" + bytes(4) + b"cccc"

    async def test_a_full_queue_blocks_submit_in_a_worker_thread_instead_of_raising(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate = _GatedPwrite().install(monkeypatch)
        _, _, writer = self._start(tmp_path)
        try:
            await writer.submit(_DataJob(0, b"first"))
            await gate.wait_started()  # the thread is parked; the queue is empty again
            blocking_put = _probe_blocking_put(writer, monkeypatch)
            for _ in range(_WRITER_QUEUE_SIZE):
                await writer.submit(_DataJob(0, b"y"))
            assert not blocking_put.is_set()  # the queue's slots took every one without blocking

            overflow = asyncio.create_task(writer.submit(_DataJob(0, b"z")))
            await _entered(blocking_put)  # parked in a worker thread, the event loop free
            assert not overflow.done()
            gate.release.set()
            await asyncio.wait_for(overflow, 10)
            await writer.close()
            assert len(gate.calls) == 1 + _WRITER_QUEUE_SIZE + 1  # every job, the overflow included, was applied
        finally:
            gate.release.set()
            await writer.close()

    async def test_a_failed_write_surfaces_from_the_next_submit_and_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(local_file_sink_mod, "pwrite", _failing_pwrite)
        _, _, writer = self._start(tmp_path)
        failed = _probe_failure_recorded(writer, monkeypatch)
        assert writer.error is None
        await writer.submit(_DataJob(0, b"x"))
        await _entered(failed)
        with pytest.raises(OSError, match="synthetic disk-full"):
            await writer.submit(_DataJob(0, b"x"))
        assert isinstance(writer.error, OSError)
        await writer.close()

    async def test_jobs_queued_behind_a_failed_write_are_discarded_and_close_returns(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dead writer that stopped consuming would leave ``close``'s
        sentinel put blocked forever on the full queue."""
        gate = _GatedPwrite(fail=True).install(monkeypatch)
        _, _, writer = self._start(tmp_path)
        await writer.submit(_DataJob(0, b"first"))
        await gate.wait_started()
        for _ in range(_WRITER_QUEUE_SIZE):
            await writer.submit(_DataJob(0, b"y"))
        gate.release.set()
        await asyncio.wait_for(writer.close(), timeout=5)
        assert len(gate.calls) == 1
        assert writer._queue.empty()  # everything behind the failure was consumed

    async def test_close_applies_the_queued_jobs_then_closes_the_fd(self, tmp_path: Path) -> None:
        path, fd, writer = self._start(tmp_path, 8)
        await writer.submit(_DataJob(0, b"abcd"))
        await writer.close()
        assert path.read_bytes()[:4] == b"abcd"
        with pytest.raises(OSError):
            os.fstat(fd)

    async def test_close_is_idempotent_and_ends_submissions(self, tmp_path: Path) -> None:
        _, _, writer = self._start(tmp_path)
        assert writer.accepting
        await writer.close()
        await writer.close()
        assert writer._queue.empty()  # a repeated close does not queue a second sentinel
        assert not writer.accepting
        with pytest.raises(RuntimeError, match="not open"):
            await writer.submit(_DataJob(0, b"x"))

    async def test_a_cancelled_close_leaves_the_fd_open_and_a_second_close_waits_for_the_thread(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate = _GatedPwrite().install(monkeypatch)
        _, fd, writer = self._start(tmp_path)
        try:
            await writer.submit(_DataJob(0, b"first"))
            await writer.submit(_DataJob(0, b"second"))
            await gate.wait_started()
            joining = _probe_join(writer, monkeypatch)

            closing = asyncio.create_task(writer.close())
            await _entered(joining)
            await _cancelled(closing)
            os.fstat(fd)  # still open under the queued jobs

            joining.clear()
            second = asyncio.create_task(writer.close())
            await _entered(joining)  # waiting on the thread the gate still holds
            assert not second.done()
            gate.release.set()
            await asyncio.wait_for(second, 10)
            with pytest.raises(OSError):
                os.fstat(fd)
        finally:
            gate.release.set()
            await writer.close()


class TestLocalFileSinkLifecycle:
    """What the sink adds around its writer thread: failure reporting through
    ``commit``/``abort``, ``open`` cancellation, and what counts as written."""

    async def test_a_failed_write_surfaces_on_the_next_write_and_at_commit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(local_file_sink_mod, "pwrite", _failing_pwrite)
        sink = LocalFileSink(tmp_path / "out.bin", staged=False)
        await sink.open(8, sparse=True)
        assert sink._writer is not None
        failed = _probe_failure_recorded(sink._writer, monkeypatch)
        await sink.write_at(0, b"x")
        await _entered(failed)
        with pytest.raises(OSError, match="synthetic disk-full"):
            await sink.write_at(0, b"y")
        with pytest.raises(OSError, match="synthetic disk-full"):
            await sink.write_zero(0, 4)
        with pytest.raises(OSError, match="synthetic disk-full"):
            await sink.commit()
        await sink.abort()

    async def test_a_failed_commit_leaves_no_staged_file_after_abort(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(local_file_sink_mod, "pwrite", _failing_pwrite)
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=True)
        with pytest.raises(OSError, match="synthetic disk-full"):
            await run_sink_export(sink, 8, sparse=True, body=lambda: _write_one_byte(sink))
        assert not dst.exists()
        assert not (tmp_path / "out.bin.part").exists()

    async def test_data_written_only_by_workers_is_kept_on_abort(self, tmp_path: Path) -> None:
        sink = LocalFileSink(tmp_path / "out.bin", staged=True, keep_partial=True)
        await sink.open(8, sparse=True)
        sink.note_worker_write()
        assert await sink.abort() == AbortOutcome(kept=True, ever_written=True)
        assert (tmp_path / "out.bin.part").exists()

    async def test_using_the_sink_before_open_or_after_commit_raises_instead_of_hanging(self, tmp_path: Path) -> None:
        sink = LocalFileSink(tmp_path / "out.bin", staged=False)
        with pytest.raises(RuntimeError, match="not open"):
            await sink.write_at(0, b"x")
        await sink.open(8, sparse=True)
        await sink.commit()
        with pytest.raises(RuntimeError, match="not open"):
            await sink.write_zero(0, 4)

    async def test_a_second_abort_after_a_cancelled_commit_waits_for_the_writer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A cancelled commit leaves the writer thread running; ``abort``
        waits for it before deciding whether to keep the file."""
        gate = _GatedPwrite().install(monkeypatch)
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        await sink.open(64, sparse=True)
        try:
            await sink.write_at(0, b"first")
            await gate.wait_started()
            assert sink._writer is not None
            joining = _probe_join(sink._writer, monkeypatch)
            committing = asyncio.create_task(sink.commit())
            await _entered(joining)
            await _cancelled(committing)

            joining.clear()
            aborting = asyncio.create_task(sink.abort())
            await _entered(joining)  # waiting on the thread the gate still holds
            assert not aborting.done()
            assert dst.exists()
            gate.release.set()
            assert await asyncio.wait_for(aborting, 10) == AbortOutcome(kept=True, ever_written=True)
            assert dst.read_bytes()[:5] == b"first"
        finally:
            gate.release.set()

    async def test_a_cancelled_open_leaves_no_leaked_fd_after_abort(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        creating = threading.Event()
        release = threading.Event()
        fds: list[int] = []
        real_create_presized = local_file_io.create_presized
        real_open_destination = local_file_io.open_destination

        def slow_create_presized(path: Path, size: int, *, sparse: bool, on_open: Callable[[], None]) -> None:
            creating.set()
            release.wait(10)
            real_create_presized(path, size, sparse=sparse, on_open=on_open)

        def recording_open_destination(path: Path | str) -> int:
            fds.append(real_open_destination(path))
            return fds[-1]

        monkeypatch.setattr(local_file_sink_mod, "create_presized", slow_create_presized)
        monkeypatch.setattr(local_file_sink_mod, "open_destination", recording_open_destination)
        dst = tmp_path / "out.bin"
        sink = LocalFileSink(dst, staged=False)
        opening = asyncio.create_task(sink.open(8, sparse=True))
        await asyncio.to_thread(creating.wait, 10)
        await _cancelled(opening)

        release.set()
        await sink.abort()
        assert len(fds) == 1
        with pytest.raises(OSError):
            os.fstat(fds[0])  # closed by the writer thread
        assert not dst.exists()


class TestLocalFileDescriptor:
    def test_pickles_and_compares_by_value(self, tmp_path: Path) -> None:
        descriptor = LocalFileDescriptor(str(tmp_path / "out.bin"))
        assert pickle.loads(pickle.dumps(descriptor)) == descriptor
        assert descriptor != LocalFileDescriptor(str(tmp_path / "other.bin"))

    def test_open_writer_writes_positionally_without_truncating(self, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * 8)
        writer = LocalFileDescriptor(str(dst)).open_writer()
        try:
            writer.write_at(2, b"ab")
        finally:
            writer.close()
        assert dst.read_bytes() == b"\xff\xff" + b"ab" + b"\xff" * 4
