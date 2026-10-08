"""Unit tests for ``synology_apm_repo.sdk.dedup.export_workers``: ``ExportExecutor``'s
binding to one repository and destination, and the worker-process task/shutdown hooks."""

from __future__ import annotations

import atexit
import contextlib
from collections.abc import Iterator
from pathlib import Path

import pytest

from support.fakes import faithful_to
from support.repo_builders import write_bucket, write_composition_entries
from support.store_fakes import LoopCheckingStore
from synology_apm_repo.sdk import concurrency
from synology_apm_repo.sdk.dedup import export_workers as export_workers_mod
from synology_apm_repo.sdk.dedup.chunk_walk import ChunkRun
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.export_scheduler import ExportTuning
from synology_apm_repo.sdk.dedup.export_sink import WorkerTarget, WorkerWriter
from synology_apm_repo.sdk.dedup.export_workers import (
    ExportExecutor,
    ExportGroupWorkerArgs,
    _export_worker_init,
    _export_worker_shutdown,
    export_bucket_group_worker,
)
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileDescriptor
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.dedup_export_fakes import (
    CHUNK_PLAINTEXTS,
    SESSION_ID,
    SIZE,
    STREAM_ID,
    dedup_file_at,
    export_to,
    standard_entries,
)


@pytest.fixture
def dedup_file(tmp_path: Path) -> DedupFile:
    return dedup_file_at(tmp_path)


class TestExecutorIdentityCheck:
    """A caller-supplied executor is bound at spawn to one ``PoolDescriptor``
    and one destination; ``export_to_writer`` rejects any mismatch."""

    async def test_an_executor_built_for_a_different_pool_is_rejected(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        other_root = tmp_path / "other-repo"
        other_root.mkdir()
        other_store = LocalFsStore(other_root)
        other_pool = Pool(other_store, "Pool", DirCache(other_store))
        other_descriptor = PoolDescriptor.from_pool(other_pool)
        assert other_descriptor is not None
        executor = ExportExecutor(other_descriptor, LocalFileDescriptor(str(tmp_path / "unused.bin")))
        try:
            with pytest.raises(ValueError, match="was not built for"):
                await export_to(dedup_file, tmp_path / "out.bin", tuning=ExportTuning(executor=executor))
        finally:
            await executor.close()

    def test_an_unpicklable_sink_descriptor_is_rejected_before_any_worker_starts(self, dedup_file: DedupFile) -> None:
        class LocalDescriptor:  # defined inside a function, so a spawned worker could never import it
            def open_writer(self) -> WorkerWriter:
                raise AssertionError("must not be called")

        pool_descriptor = PoolDescriptor.from_pool(dedup_file.pool)
        assert pool_descriptor is not None

        with pytest.raises(ValueError, match=r"sink descriptor .*LocalDescriptor.* cannot be pickled"):
            ExportExecutor(pool_descriptor, LocalDescriptor())

    async def test_accepts_only_its_own_pool_and_destination(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        descriptor = PoolDescriptor.from_pool(dedup_file.pool)
        assert descriptor is not None
        own = LocalFileDescriptor(str(tmp_path / "a.bin"))
        executor = ExportExecutor(descriptor, own)
        try:
            assert executor.accepts(descriptor, WorkerTarget(own))
            assert not executor.accepts(descriptor, WorkerTarget(LocalFileDescriptor(str(tmp_path / "b.bin"))))
            assert not executor.accepts(descriptor, None)
            assert not executor.accepts(None, WorkerTarget(own))
        finally:
            await executor.close()

    async def test_an_executor_built_for_this_same_pool_is_accepted(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        descriptor = PoolDescriptor.from_pool(dedup_file.pool)
        assert descriptor is not None
        dst = tmp_path / "out.bin"
        executor = ExportExecutor(descriptor, LocalFileDescriptor(str(dst)))
        try:
            result = await export_to(dedup_file, dst, tuning=ExportTuning(executor=executor))
        finally:
            # Caller-owned: export_to_writer() does not shut it down.
            await executor.close()
        assert result.bytes_written > 0  # real DATA bytes were actually dispatched to this executor
        assert dst.stat().st_size == SIZE  # sparse: presized to the full logical size regardless

    async def test_an_executor_built_for_the_same_pool_but_a_different_dst_is_rejected(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        # A matching PoolDescriptor is not enough: the workers' writers are bound to dst_a.
        descriptor = PoolDescriptor.from_pool(dedup_file.pool)
        assert descriptor is not None
        dst_a = tmp_path / "out_a.bin"
        dst_b = tmp_path / "out_b.bin"
        executor = ExportExecutor(descriptor, LocalFileDescriptor(str(dst_a)))
        try:
            with pytest.raises(ValueError, match="was not built for"):
                await export_to(dedup_file, dst_b, tuning=ExportTuning(executor=executor))
        finally:
            await executor.close()


@pytest.fixture(autouse=True)
def _reset_export_worker_globals(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keeps the process-global worker state (``_worker``, ``_worker_writer``,
    ``concurrency._worker_runner``) from leaking between tests: monkeypatch
    restores the first two, after this closes a writer left open."""
    monkeypatch.setattr(export_workers_mod._worker, "store", None)
    monkeypatch.setattr(export_workers_mod._worker, "pool", None)
    monkeypatch.setattr(export_workers_mod, "_worker_writer", None)
    yield
    if export_workers_mod._worker_writer is not None:
        with contextlib.suppress(OSError):
            export_workers_mod._worker_writer.close()
    if concurrency._worker_runner is not None:
        concurrency.close_worker_loop()


class TestExportWorkerLoopReuse:
    """A worker's store outlives every task it runs, so all tasks must share
    one event loop (``concurrency.run_in_worker_loop``). Run in-process."""

    def test_second_task_in_the_same_worker_reuses_the_first_ones_loop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), session_id=SESSION_ID, stream_id=STREAM_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LoopCheckingStore(LocalFsStore(tmp_path))
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(4096))
        monkeypatch.setattr(export_workers_mod._worker, "pool", pool)
        monkeypatch.setattr(export_workers_mod, "_worker_writer", LocalFileDescriptor(str(dst)).open_writer())
        args = ExportGroupWorkerArgs(
            stream_id=StreamId(0), bucket_id=BucketId(0), runs=[ChunkRun(0, 1, 0)], size=4096, dst_offset=0
        )

        first = export_bucket_group_worker(args)  # binds the store to the worker loop
        second = export_bucket_group_worker(args)  # must reuse that loop, not raise "Event loop is closed"

        assert (first, second) == (4096, 4096)
        assert concurrency._worker_runner is not None
        assert store._bound_loop is concurrency._worker_runner.get_loop()
        assert dst.read_bytes() == CHUNK_PLAINTEXTS[0]


class TestExportWorkerShutdown:
    """``_export_worker_init`` registers ``_export_worker_shutdown`` with
    ``atexit``, which closes the worker's store and the destination
    writer even if ``close()`` raises. Covered by direct calls, since a
    spawned worker can't run a test-file-defined target."""

    def test_export_worker_init_registers_the_shutdown_hook(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), session_id=SESSION_ID, stream_id=STREAM_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        descriptor = PoolDescriptor.from_pool(pool)
        assert descriptor is not None
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")
        registered: list[object] = []
        monkeypatch.setattr(atexit, "register", registered.append)

        _export_worker_init(descriptor, LocalFileDescriptor(str(dst)))

        assert registered == [_export_worker_shutdown]

    def test_shutdown_closes_the_worker_store_and_the_writer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed: list[bool] = []

        @faithful_to(ObjectStore)
        class _FakeClosingStore:
            async def close(self) -> None:
                closed.append(True)

        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")
        writer = LocalFileDescriptor(str(dst)).open_writer()
        monkeypatch.setattr(export_workers_mod._worker, "store", _FakeClosingStore())
        monkeypatch.setattr(export_workers_mod, "_worker_writer", writer)

        _export_worker_shutdown()

        assert closed == [True]
        with pytest.raises(OSError):
            writer.write_at(0, b"x")  # the writer was closed
        monkeypatch.setattr(
            export_workers_mod, "_worker_writer", None
        )  # already closed; the autouse fixture must not close it again

    def test_shutdown_tolerates_the_store_s_close_raising(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        @faithful_to(ObjectStore)
        class _FailingClosingStore:
            async def close(self) -> None:
                raise RuntimeError("synthetic close failure")

        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")
        writer = LocalFileDescriptor(str(dst)).open_writer()
        monkeypatch.setattr(export_workers_mod._worker, "store", _FailingClosingStore())
        monkeypatch.setattr(export_workers_mod, "_worker_writer", writer)

        _export_worker_shutdown()  # must not raise despite close() failing

        with pytest.raises(OSError):
            writer.write_at(0, b"x")  # the writer still got closed
        monkeypatch.setattr(
            export_workers_mod, "_worker_writer", None
        )  # already closed; the autouse fixture must not close it again
