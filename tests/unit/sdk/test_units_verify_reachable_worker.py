"""Unit tests for ``synology_apm_repo.sdk.units.verify_reachable`` —
bucket sizing/progress/finding-ref plumbing, batch flushing, FULL's
multiprocess path and close-vs-walk-exception ordering — plus the verify
worker's process-global lifecycle (``dedup.verify_bucket_check``: loop
reuse, shutdown)."""

from __future__ import annotations

import asyncio
import atexit
import os
import struct
import zlib
from collections.abc import Iterator
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

import pytest
import zstandard

from support.fakes import faithful_to
from support.format_builders import (
    SIZE_STORE_REGION_LEN,
    encode_size_store,
    mapping_record,
)
from support.repo_builders import (
    write_bucket,
    write_composition_entries,
    write_file_map,
    write_inf_and_fgp,
    write_legacy_bucket,
)
from support.store_fakes import LoopCheckingStore
from synology_apm_repo.sdk import concurrency
from synology_apm_repo.sdk.dedup import verify_bucket_check as verify_bucket_check_module
from synology_apm_repo.sdk.dedup.pool import FULL_VERIFY, BucketReader, BucketReaderCache, Pool
from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_bucket_check import (
    VerifyExecutor,
    _verify_worker_shutdown,
    verify_bucket_worker,
    verify_worker_init,
)
from synology_apm_repo.sdk.dedup.verify_walk import ReachabilitySweep
from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError, StorageBackendError, WorkerProcessError
from synology_apm_repo.sdk.findings import Finding, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.presentation.progress import Progress
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units import verify_reachable as verify_reachable_module
from synology_apm_repo.sdk.units.verify_reachable import verify_reachable
from unit.sdk.verify_reachable_fakes import (
    open_vault_repo,
    write_fs_workload,
)

_STREAM_ID = 8
_SESSION_ID = 3
_COMP_OFFSET = 64  # right after the composition sub-file's 64-byte cMpS header


def _write_bucket_with_compacted_slot(path: Path, plaintexts: list[bytes], *, compacted_idx: int) -> None:
    """Like ``write_bucket``, but ``compacted_idx`` is stored as
    ``CompressType.COMPACTED`` (no chunk data, no ChunkCrcStore entry)
    instead of a ZSTD chunk."""
    payloads: list[bytes | None] = []
    entries: list[tuple[int, int]] = []
    for idx, plain in enumerate(plaintexts):
        if idx == compacted_idx:
            payloads.append(None)
            entries.append((CompressType.COMPACTED.value, 0))
        else:
            compressed = zstandard.ZstdCompressor().compress(plain)
            payloads.append(compressed)
            entries.append((CompressType.ZSTD.value, len(compressed)))
    tight = encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF

    chunk_crcs = [zlib.crc32(p) & 0xFFFFFFFF for p in payloads if p is not None]
    chunk_crc_store = b"".join(crc.to_bytes(4, "big") for crc in chunk_crcs)
    crc_of_chunk_crc = zlib.crc32(chunk_crc_store) & 0xFFFFFFFF

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[29:33] = struct.pack(">I", crc_of_chunk_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    sizestore_region = tight + b"\x00" * (SIZE_STORE_REGION_LEN - len(tight))
    chunk_data = b"".join(p for p in payloads if p is not None)
    trailer = chunk_crc_store + os.urandom(redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + chunk_data + trailer)


def _write_compacted_bucket(path: Path) -> None:
    """A minimal, valid, fully-``COMPACTED`` single-chunk bucket: it passes
    the header/SizeStore CRC and ``check_bucket_structure``, and
    ``non_compacted_chunk_ranges()`` is ``[]``, so no per-chunk check runs
    against it."""
    tight = encode_size_store([(CompressType.COMPACTED.value, 0)])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", 1)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (SIZE_STORE_REGION_LEN - len(tight))
    trailer = os.urandom(redundancy_size((1 * 15 + 7) >> 3, 256))  # 0 non-empty chunks -> no ChunkCrcStore bytes
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + trailer)


def _list_without_sizes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``LocalFsStore`` list names only, as a backend whose listing carries
    no sizes does, so sizing falls back to ``store.size()``."""

    real_listdir = LocalFsStore.listdir

    async def names_only(self: LocalFsStore, path: str) -> list[Entry]:
        return [Entry(entry.name, None) for entry in await real_listdir(self, path)]

    monkeypatch.setattr(LocalFsStore, "listdir", names_only)


def _record_bucket_sizes(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Every size ``ReachabilitySweep._size_one_bucket`` returns, in call order."""
    sizes: list[int] = []
    real = ReachabilitySweep._size_one_bucket

    async def recording(self: ReachabilitySweep, key: tuple[StreamId, BucketId]) -> int:
        sizes.append(await real(self, key))
        return sizes[-1]

    monkeypatch.setattr(ReachabilitySweep, "_size_one_bucket", recording)
    return sizes


class TestBucketSizing:
    """FULL-level sizing (``ReachabilitySweep._size_one_bucket``) turns any
    failure but ``StorageBackendError`` into a size of 0 instead of raising,
    which would cancel every concurrent sizing sibling; the check phase
    reports the bucket itself."""

    async def test_a_missing_bucket_sizes_as_zero_bytes_without_raising(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        # No bucket file: sizing and checking both hit NotFoundError.
        sizes = _record_bucket_sizes(monkeypatch)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert sizes == [0]
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.DATA_MISSING for f in findings)

    async def test_a_permission_denied_bucket_sizes_as_zero_bytes_without_raising(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The bucket itself is intact, so the run is clean once the check
        phase opens it."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        bucket_path = tmp_path / "@data" / "Pool" / "0" / "0.buk"
        write_bucket(bucket_path, plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        _list_without_sizes(monkeypatch)
        real_size = LocalFsStore.size

        calls = 0

        async def denying_size(self: LocalFsStore, path: str) -> int:
            nonlocal calls
            calls += 1
            # Only the sizing pass's call (the first) fails;
            # check_bucket_structure's later size() call must succeed.
            if path.endswith("0.buk") and calls == 1:
                raise PermissionDeniedError("simulated: permission denied", ref=path)
            return await real_size(self, path)

        monkeypatch.setattr(LocalFsStore, "size", denying_size)
        sizes = _record_bucket_sizes(monkeypatch)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert sizes == [0]
        assert findings == []

    async def test_an_unexpected_sizing_error_sizes_as_zero_bytes_without_raising(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An error outside ``ObjectStore.size()``'s documented outcomes."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        bucket_path = tmp_path / "@data" / "Pool" / "0" / "0.buk"
        write_bucket(bucket_path, plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        _list_without_sizes(monkeypatch)
        real_size = LocalFsStore.size

        calls = 0

        async def broken_size(self: LocalFsStore, path: str) -> int:
            nonlocal calls
            calls += 1
            if path.endswith("0.buk") and calls == 1:
                raise RuntimeError("simulated: unexpected backend error")
            return await real_size(self, path)

        monkeypatch.setattr(LocalFsStore, "size", broken_size)
        sizes = _record_bucket_sizes(monkeypatch)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert sizes == [0]
        assert findings == []


class TestStorageBackendFailure:
    """A transport failure says nothing about the data, so verify raises it
    instead of reporting the bucket as damaged -- and raises it bare, not
    wrapped in the concurrent dispatch's exception group."""

    def _write_one_bucket_workload(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

    async def test_while_sizing_it_aborts_the_verify(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._write_one_bucket_workload(tmp_path)
        _list_without_sizes(monkeypatch)
        real_size = LocalFsStore.size

        async def failing_size(self: LocalFsStore, path: str) -> int:
            if path.endswith("0.buk"):
                raise StorageBackendError("simulated: connection reset", ref=path)
            return await real_size(self, path)

        monkeypatch.setattr(LocalFsStore, "size", failing_size)

        repo = await open_vault_repo(tmp_path)
        try:
            with pytest.raises(StorageBackendError, match="connection reset"):
                await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()

    async def test_while_checking_it_aborts_the_verify(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._write_one_bucket_workload(tmp_path)
        real_read = LocalFsStore.read

        async def failing_read(self: LocalFsStore, path: str, offset: int = 0, length: int | None = None) -> bytes:
            if path.endswith("0.buk"):
                raise StorageBackendError("simulated: connection reset", ref=path)
            return await real_read(self, path, offset, length)

        monkeypatch.setattr(LocalFsStore, "read", failing_read)

        repo = await open_vault_repo(tmp_path)
        try:
            # QUICK checks in-process, through the bounded gather.
            with pytest.raises(StorageBackendError, match="connection reset"):
                await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()


class TestProgressCallback:
    """``progress`` reports ``phase="discovering"`` (``unit="items"``, one
    tick per ``(workload, version)`` pair) while claiming buckets, then
    ``phase="verifying"`` (one tick per bucket checked) once discovery is
    done, so its total no longer grows. The ``verifying`` unit is
    ``"bytes"`` (summed on-disk bucket sizes) at FULL and ``"buckets"`` at
    QUICK, which never reads chunk content and skips sizing."""

    async def test_full_level_reports_discovering_then_verifying_in_bytes(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        bucket_path = tmp_path / "@data" / "Pool" / "0" / "0.buk"
        write_bucket(bucket_path, plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)
        bucket_size = bucket_path.stat().st_size

        calls: list[Progress] = []

        async def on_progress(progress: Progress) -> None:
            calls.append(progress)

        repo = await open_vault_repo(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.FULL, progress=on_progress)
        finally:
            await repo.close()
        assert len(calls) == 2

        discovering = calls[0]
        assert discovering.phase == "discovering"
        assert discovering.determinate is True
        assert discovering.unit == "items"
        assert discovering.done == 0
        assert discovering.total == 1
        assert "fsA" in discovering.detail

        verifying = calls[1]
        assert verifying.phase == "verifying"
        assert verifying.determinate is True
        assert verifying.unit == "bytes"
        assert verifying.done == bucket_size
        assert verifying.total == bucket_size
        assert "fsA" in verifying.detail

    async def test_quick_level_reports_verifying_as_a_plain_bucket_count(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        calls: list[Progress] = []

        async def on_progress(progress: Progress) -> None:
            calls.append(progress)

        repo = await open_vault_repo(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.QUICK, progress=on_progress)
        finally:
            await repo.close()
        verifying = next(c for c in calls if c.phase == "verifying")
        assert verifying.unit == "buckets"
        assert verifying.done == 1
        assert verifying.total == 1

    async def test_reports_once_per_bucket_across_a_single_version_claiming_several(self, tmp_path: Path) -> None:
        bucket_ids = [0, 1, 2, 3, 4]
        entries = b"".join(
            mapping_record(i * 4096, bucket_id=bid, chunk_idx=0, map_num=1) for i, bid in enumerate(bucket_ids)
        )
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(bucket_ids) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=_SESSION_ID, entries=entries
        )
        # No bucket files: a DATA_MISSING bucket still counts as checked.

        calls: list[Progress] = []

        async def on_progress(progress: Progress) -> None:
            calls.append(progress)

        repo = await open_vault_repo(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.QUICK, progress=on_progress)
        finally:
            await repo.close()
        verifying_calls = [c for c in calls if c.phase == "verifying"]
        assert len(verifying_calls) == len(bucket_ids)
        assert all(c.total == len(bucket_ids) for c in verifying_calls)
        assert sorted(c.done for c in verifying_calls) == list(range(1, len(bucket_ids) + 1))


class TestFindingRef:
    """Every ``Finding`` carries the canonical ``cat:/wl:/ver:`` ref of the
    version it belongs to; for a bucket shared across versions, that's the
    version that claimed it first."""

    async def test_unresolvable_version_finding_carries_its_own_ref(self, tmp_path: Path) -> None:
        write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-broken",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            # target.db resolves; the missing db/file_map row is what makes
            # this version unresolvable.
        )
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].ref == "#cat:1/wl:10/ver:vuid-broken"

    async def test_a_deeper_stage_finding_carries_the_same_ref(self, tmp_path: Path) -> None:
        """A bucket-stage finding comes from ``check_all_buckets``, after
        discovery, so its ref is the one ``_bucket_claim`` recorded when the
        version claimed the bucket."""
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        # No bucket file: a Stage.BUCKET/DATA_MISSING finding.

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].ref == "#cat:1/wl:10/ver:vuid-1"


class TestBucketBatchFlushing:
    """``verify_reachable()``'s discovery loop calls
    ``finalize_pending_buckets()`` once ``_BUCKET_BATCH_SIZE`` buckets are
    pending, and once more after the loop. The threshold is checked only
    between versions, so one version claiming any number of buckets flushes
    once."""

    def _build_version_touching_buckets(
        self,
        tmp_path: Path,
        *,
        workload_id: int,
        version_uid: str,
        target_id: str,
        meta_dirname: str,
        session_id: int,
        bucket_ids: list[int],
    ) -> str:
        entries = b"".join(
            mapping_record(i * 4096, bucket_id=bid, chunk_idx=0, map_num=1) for i, bid in enumerate(bucket_ids)
        )
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=workload_id,
            version_uid=version_uid,
            target_id=target_id,
            meta_dirname=meta_dirname,
            dedup_version_id=1,
            dedup_img_size=len(bucket_ids) * 4096,
        )
        write_composition_entries(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, entries=entries
        )
        return dedup_img_path

    async def _count_meaningful_flushes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> int:
        """Counts only calls with something pending, so the unconditional
        post-loop call doesn't count when it has nothing to do."""
        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        flush_calls = 0
        original = verify_reachable_module._ReachabilityWalker.finalize_pending_buckets

        async def counting_flush(self: verify_reachable_module._ReachabilityWalker) -> None:
            nonlocal flush_calls
            if self.pending_bucket_count > 0:
                flush_calls += 1
            await original(self)

        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "finalize_pending_buckets", counting_flush)
        repo = await open_vault_repo(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        return flush_calls

    async def test_one_version_at_exactly_the_batch_size_flushes_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        batch_size = verify_reachable_module._BUCKET_BATCH_SIZE
        dedup_img_a = self._build_version_touching_buckets(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            session_id=_SESSION_ID,
            bucket_ids=list(range(batch_size)),
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        # No bucket files: only the flush count matters here.
        assert await self._count_meaningful_flushes(tmp_path, monkeypatch) == 1

    async def test_crossing_the_batch_boundary_across_two_versions_flushes_twice(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import synology_apm_repo.sdk.units.verify_reachable as verify_reachable_module

        batch_size = verify_reachable_module._BUCKET_BATCH_SIZE
        dedup_img_a = self._build_version_touching_buckets(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            session_id=_SESSION_ID,
            bucket_ids=list(range(batch_size)),
        )
        # One more unclaimed bucket stays under the threshold, so the
        # post-loop call is what flushes it.
        dedup_img_b = self._build_version_touching_buckets(
            tmp_path,
            workload_id=11,
            version_uid="vuid-2",
            target_id="fsB",
            meta_dirname="FSB_meta",
            session_id=_SESSION_ID + 1,
            bucket_ids=[batch_size],
        )
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID + 1, _COMP_OFFSET, 1, 2),
            ],
        )
        assert await self._count_meaningful_flushes(tmp_path, monkeypatch) == 2


class TestConcurrentBucketCheckIsolation:
    """``check_one_bucket`` turns an exception after the bucket opened into
    a ``Finding``, so a failing bucket never drops its concurrent siblings."""

    async def test_one_broken_bucket_does_not_prevent_its_batch_siblings_from_reporting(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        bucket_ids = [0, 1, 2]
        entries = b"".join(
            mapping_record(i * 4096, bucket_id=bid, chunk_idx=0, map_num=1) for i, bid in enumerate(bucket_ids)
        )
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(bucket_ids) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=_SESSION_ID, entries=entries
        )
        # Fully COMPACTED buckets: check_bucket_structure (patched below)
        # is the only check that runs.
        for bucket_id in bucket_ids:
            _write_compacted_bucket(tmp_path / "@data" / "Pool" / "0" / f"{bucket_id}.buk")

        import synology_apm_repo.sdk.dedup.verify_bucket_check as verify_bucket_check_module

        async def flaky_check_bucket_structure(
            repo: DedupRepo, reader: BucketReader, *, known_file_size: int | None = None
        ) -> list[Finding]:
            if reader.path.endswith("/1.buk"):
                raise RuntimeError("simulated unexpected failure")
            return []

        monkeypatch.setattr(verify_bucket_check_module, "check_bucket_structure", flaky_check_bucket_structure)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        bucket_findings = [f for f in findings if f.stage is Stage.BUCKET]
        assert len(bucket_findings) == 1
        assert bucket_findings[0].symptom is Symptom.CORRUPTION
        assert "unexpected error" in bucket_findings[0].detail
        assert bucket_findings[0].path.endswith("/1.buk")

    async def test_a_format_error_past_the_open_call_is_corruption_not_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``NotFoundError`` opening the bucket is ``DATA_MISSING``; one
        raised after it opened (here by ``check_bucket_structure``) is
        ``CORRUPTION``, from the ``NotFoundError``/``FormatError`` branch
        rather than the broad ``unexpected error`` one."""
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        _write_compacted_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk")

        import synology_apm_repo.sdk.dedup.verify_bucket_check as verify_bucket_check_module

        async def failing_check_bucket_structure(
            repo: DedupRepo, reader: BucketReader, *, known_file_size: int | None = None
        ) -> list[Finding]:
            raise NotFoundError("simulated: bucket vanished after its own open", ref=reader.path)

        monkeypatch.setattr(verify_bucket_check_module, "check_bucket_structure", failing_check_bucket_structure)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.CORRUPTION
        assert "unexpected error" not in findings[0].detail


class TestFullChecksEveryPhysicalChunk:
    """FULL checks every live chunk in a claimed bucket, not just the
    chunk-map-referenced ones, and skips ``COMPACTED`` slots."""

    async def test_full_skips_a_compacted_slot_and_checks_the_rest(self, tmp_path: Path) -> None:
        plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(6)]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,  # one chunk
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        # The chunk map references only chunk_idx=1; FULL still checks
        # chunks 1..5 (index 0 is compacted).
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 1, map_num=1),
        )
        _write_bucket_with_compacted_slot(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, compacted_idx=0)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []

    async def test_full_reports_damage_in_a_chunk_no_chunk_map_references(self, tmp_path: Path) -> None:
        plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(6)]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,  # one chunk
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 1, map_num=1),  # references chunk 1 only
        )
        _write_bucket_with_compacted_slot(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, compacted_idx=0)
        # Chunk 4's stored fingerprint no longer matches its content.
        fingerprinted = [*plaintexts[:4], b"\xee" * 4096, *plaintexts[5:]]
        write_inf_and_fgp(tmp_path / "@data" / "Pool", fingerprinted)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert [(f.stage, f.symptom, f.path) for f in findings] == [
            (Stage.BUCKET, Symptom.MISMATCH, "@data/Pool/0/0.buk")
        ]
        assert "chunk_idx=4" in findings[0].detail


class TestMultiVersionBucketRefClaim:
    """Two versions share one bucket and the second also touches another:
    each bucket's finding carries the ref of the version that claimed it
    first."""

    async def test_shared_and_unshared_buckets_carry_their_claiming_versions_ref(self, tmp_path: Path) -> None:
        dedup_img_a = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        dedup_img_b = write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-2",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=2 * 4096,
        )
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID + 1, _COMP_OFFSET, 1, 2),
            ],
        )
        # Version 1 references bucket 0 only.
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        # Version 2 references bucket 0 (already claimed by version 1)
        # and bucket 1.
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID + 1,
            entries=mapping_record(0, 0, 0, map_num=1) + mapping_record(4096, 1, 0, map_num=1),
        )
        # No bucket files: each bucket surfaces as one DATA_MISSING finding.

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        # mapping_record addresses Pool stream 0; _STREAM_ID addresses only
        # the composition record.
        bucket_findings = {f.path: f for f in findings if f.stage is Stage.BUCKET}
        assert len(bucket_findings) == 2
        assert bucket_findings["bucket 0/0"].ref == "#cat:1/wl:10/ver:vuid-1"
        assert bucket_findings["bucket 0/1"].ref == "#cat:1/wl:11/ver:vuid-2"


def _write_one_bucket_fs_repo(tmp_path: Path) -> None:
    """One FS workload over three chunks in one bucket, on a describable
    ``LocalFsStore``, so FULL level takes the real multiprocess path."""
    plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(3)]
    dedup_img_path = write_fs_workload(
        tmp_path,
        workload_id=10,
        version_uid="vuid-1",
        target_id="fsA",
        meta_dirname="FSA_meta",
        dedup_version_id=1,
        dedup_img_size=len(plaintexts) * 4096,
    )
    write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
    write_composition_entries(
        tmp_path / "@data" / "Composition",
        stream_id=_STREAM_ID,
        session_id=_SESSION_ID,
        entries=mapping_record(0, 0, 0, map_num=len(plaintexts)),
    )
    write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
    write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)


class TestMultiprocessExecutorTeardown:
    """FULL level's real multiprocess path (``LocalFsStore`` is
    describable): executor shutdown runs off the event loop (it blocks until
    in-flight workers finish), an executor built for another repository is
    refused, and a dead worker surfaces as ``WorkerProcessError``."""

    async def test_executor_shutdown_is_routed_through_to_thread(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from concurrent.futures import ProcessPoolExecutor

        _write_one_bucket_fs_repo(tmp_path)

        real_to_thread = asyncio.to_thread
        recorded: list[object] = []

        async def _recording_to_thread(func: object, *args: object, **kwargs: object) -> object:
            recorded.append(func)
            return await real_to_thread(func, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(asyncio, "to_thread", _recording_to_thread)

        repo = await open_vault_repo(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()

        shutdown_calls = [
            f for f in recorded if getattr(f, "__name__", None) == "shutdown" and getattr(f, "__self__", None)
        ]
        assert shutdown_calls, f"executor.shutdown() was never routed through asyncio.to_thread; saw: {recorded}"
        assert all(isinstance(f.__self__, ProcessPoolExecutor) for f in shutdown_calls)  # type: ignore[attr-defined]

    async def test_an_executor_built_for_another_repository_is_refused(self, tmp_path: Path) -> None:
        _write_one_bucket_fs_repo(tmp_path)
        repo = await open_vault_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        foreign = PoolDescriptor.from_pool(Pool(store, "elsewhere/Pool", DirCache(store)))
        assert foreign is not None
        try:
            async with VerifyExecutor(foreign) as executor:
                with pytest.raises(ValueError, match="not built for this repository"):
                    await verify_reachable(repo, VerifyLevel.FULL, executor=executor)
        finally:
            await repo.close()

    async def test_a_dead_worker_raises_worker_process_error(self, tmp_path: Path) -> None:
        _write_one_bucket_fs_repo(tmp_path)
        repo = await open_vault_repo(tmp_path)
        descriptor = PoolDescriptor.from_pool(repo.new_pool(verify=FULL_VERIFY))
        assert descriptor is not None
        executor = VerifyExecutor(descriptor)
        try:
            with pytest.raises(BrokenProcessPool, match="terminated abruptly"):
                executor.process_pool.submit(os._exit, 1).result()  # kills the one worker the pool spawned

            with pytest.raises(WorkerProcessError, match="verify worker process died") as excinfo:
                await verify_reachable(repo, VerifyLevel.FULL, executor=executor)

            assert isinstance(excinfo.value.__cause__, BrokenProcessPool)
        finally:
            await repo.close()
            await executor.close()


@pytest.fixture(autouse=True)
def _reset_verify_worker_globals(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """``verify_bucket_check``'s process-global ``_worker``/
    ``_worker_bucket_cache`` and ``concurrency._worker_runner`` are set once
    per worker process's lifetime: monkeypatch restores the first two after
    a test runs the worker in-process, and the worker loop is closed."""
    monkeypatch.setattr(verify_bucket_check_module._worker, "pool", None)
    monkeypatch.setattr(verify_bucket_check_module._worker, "store", None)
    monkeypatch.setattr(verify_bucket_check_module, "_worker_bucket_cache", None)
    yield
    if concurrency._worker_runner is not None:
        concurrency.close_worker_loop()


class TestVerifyWorkerLoopReuse:
    """A worker's ``ObjectStore`` is built once per process by
    ``verify_worker_init`` and may hold a loop-bound client
    (``S3Store._get_client``), so every ``verify_bucket_worker`` call must
    run on the same loop (``concurrency.run_in_worker_loop``); otherwise
    the second bucket reports a false "unexpected error: Event loop is
    closed" corruption finding, hence the assertion on findings rather than
    ``pytest.raises``.

    Run in-process with ``LoopCheckingStore``. The two bucket keys differ
    because a repeated key would hit ``_worker_bucket_cache`` and never
    reach the store."""

    def test_second_bucket_in_the_same_worker_reuses_the_first_ones_loop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        plaintexts = [b"x" * 4096]
        write_legacy_bucket(tmp_path / "Pool" / "0" / "0.buk", plaintexts)
        write_legacy_bucket(tmp_path / "Pool" / "0" / "1.buk", plaintexts)
        store = LoopCheckingStore(LocalFsStore(tmp_path))
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        monkeypatch.setattr(verify_bucket_check_module._worker, "pool", pool)
        monkeypatch.setattr(verify_bucket_check_module, "_worker_bucket_cache", BucketReaderCache())
        monkeypatch.setattr(verify_bucket_check_module._worker, "store", store)

        findings_a, _ = verify_bucket_worker((StreamId(0), BucketId(0)))
        findings_b, _ = verify_bucket_worker((StreamId(0), BucketId(1)))

        for findings in (findings_a, findings_b):
            assert not any("Event loop is closed" in f.detail for f in findings), findings


class TestVerifyWorkerShutdown:
    """``verify_worker_init`` registers ``_verify_worker_shutdown`` with
    ``atexit``; the hook closes the worker's store, tolerating ``close()``
    raising. Called directly: a test-file function can't be pickled into a
    ``spawn``-context worker to observe ``atexit`` firing."""

    def test_verify_worker_init_registers_the_shutdown_hook(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_legacy_bucket(tmp_path / "Pool" / "0" / "0.buk", [b"x" * 4096])
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        descriptor = PoolDescriptor.from_pool(pool)
        assert descriptor is not None
        registered: list[object] = []
        monkeypatch.setattr(atexit, "register", registered.append)

        verify_worker_init(descriptor)

        assert registered == [_verify_worker_shutdown]

    def test_shutdown_closes_the_worker_store(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed: list[bool] = []

        @faithful_to(ObjectStore)
        class _FakeClosingStore:
            async def close(self) -> None:
                closed.append(True)

        monkeypatch.setattr(verify_bucket_check_module._worker, "store", _FakeClosingStore())

        _verify_worker_shutdown()

        assert closed == [True]

    def test_shutdown_tolerates_the_store_s_close_raising(self, monkeypatch: pytest.MonkeyPatch) -> None:
        close_calls: list[bool] = []

        @faithful_to(ObjectStore)
        class _FailingClosingStore:
            async def close(self) -> None:
                close_calls.append(True)
                raise RuntimeError("synthetic close failure")

        monkeypatch.setattr(verify_bucket_check_module._worker, "store", _FailingClosingStore())

        _verify_worker_shutdown()  # must not raise despite close() failing

        assert close_calls == [True]
        assert concurrency._worker_runner is None  # the worker loop is still released


class TestCleanupCloseExceptionPriority:
    """A ``walker.close()`` failure in ``verify_reachable()`` never replaces
    an exception already propagating from the walk, and propagates when the
    walk succeeded. ``open_vault_repo()``'s repo has no ``workload_config`` table, so
    the walk visits no version."""

    async def test_close_failure_propagates_when_the_walk_otherwise_succeeded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def failing_close(self: verify_reachable_module._ReachabilityWalker) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "close", failing_close)

        repo = await open_vault_repo(tmp_path)
        try:
            with pytest.raises(RuntimeError, match="close failed"):
                await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

    async def test_close_failure_is_suppressed_when_the_walk_already_raised(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def failing_check_all_buckets(
            self: verify_reachable_module._ReachabilityWalker,
        ) -> list[Finding]:
            raise RuntimeError("walk failed")

        async def failing_close(self: verify_reachable_module._ReachabilityWalker) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "check_all_buckets", failing_check_all_buckets)
        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "close", failing_close)

        repo = await open_vault_repo(tmp_path)
        try:
            with pytest.raises(RuntimeError, match="walk failed"):
                await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

    async def test_close_failure_propagates_even_when_called_from_within_an_unrelated_except_block(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The caller's in-flight exception (visible via ``sys.exc_info()``)
        is not mistaken for a walk failure."""

        async def failing_close(self: verify_reachable_module._ReachabilityWalker) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(verify_reachable_module._ReachabilityWalker, "close", failing_close)

        repo = await open_vault_repo(tmp_path)
        try:
            try:
                raise ValueError("unrelated outer error")
            except ValueError:
                with pytest.raises(RuntimeError, match="close failed"):
                    await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()


class TestSizesFromTheListing:
    """A backend whose listing carries sizes (every built-in one) needs no
    ``store.size()`` for a bucket: ``Pool.bucket_size`` answers from the
    ``DirCache`` entry the path resolution already filled."""

    def _build(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

    @pytest.mark.parametrize("level", [VerifyLevel.QUICK, VerifyLevel.FULL])
    async def test_verifying_a_bucket_issues_no_size_request(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, level: VerifyLevel
    ) -> None:
        self._build(tmp_path)
        sized: list[str] = []
        real_size = LocalFsStore.size

        async def spying_size(self: LocalFsStore, path: str) -> int:
            sized.append(path)
            return await real_size(self, path)

        monkeypatch.setattr(LocalFsStore, "size", spying_size)
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, level)
        finally:
            await repo.close()

        assert findings == []
        assert not any(path.endswith(".buk") for path in sized)

    async def test_without_listed_sizes_each_bucket_still_costs_its_size_requests(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._build(tmp_path)
        _list_without_sizes(monkeypatch)
        sized: list[str] = []
        real_size = LocalFsStore.size

        async def spying_size(self: LocalFsStore, path: str) -> int:
            sized.append(path)
            return await real_size(self, path)

        monkeypatch.setattr(LocalFsStore, "size", spying_size)
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        assert findings == []
        assert [path for path in sized if path.endswith(".buk")] == ["@data/Pool/0/0.buk"]  # check_bucket_structure's

    async def test_the_full_progress_total_is_the_listed_bucket_size(self, tmp_path: Path) -> None:
        self._build(tmp_path)
        expected = (tmp_path / "@data" / "Pool" / "0" / "0.buk").stat().st_size
        totals: list[int | None] = []

        async def on_progress(progress: Progress) -> None:
            if progress.phase == "verifying":
                totals.append(progress.total)

        repo = await open_vault_repo(tmp_path)
        try:
            await verify_reachable(repo, VerifyLevel.FULL, progress=on_progress)
        finally:
            await repo.close()

        assert totals and set(totals) == {expected}
