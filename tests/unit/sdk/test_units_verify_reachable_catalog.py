"""Unit tests for ``units.verify_reachable``'s catalog/version enumeration
(orphaned file_map rows, unresolvable versions, catalog-enumeration failure)
plus full-vs-quick chunk coverage, cross-version memoization, and FULL's
per-chunk fingerprint check."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from support.format_builders import (
    mapping_record,
)
from support.model_factories import make_connection, make_workload
from support.repo_builders import (
    write_bucket,
    write_composition_entries,
    write_file_map,
    write_inf_and_fgp,
)
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup import verify_walk as verify_walk_module
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_walk import CompositionExtent
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError, StorageBackendError
from synology_apm_repo.sdk.findings import Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.units import verify_reachable as verify_reachable_module
from synology_apm_repo.sdk.units.verify_reachable import verify_reachable
from unit.sdk.verify_reachable_fakes import (
    open_vault_repo,
    write_fs_workload,
)

_STREAM_ID = 8
_SESSION_ID = 3
_COMP_OFFSET = 64  # right after the composition sub-file's own 64-byte cMpS header


class TestOrphanedFileMapRow:
    """A ``file_map`` row nothing in the catalog resolves to must never be
    visited/flagged by the top-down walk."""

    async def test_orphaned_row_is_never_visited(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-healthy",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                # Orphaned: no version_table/target.db points at this path.
                ("orphan/999/dedup.img", 99, 99, 0, 1, 2),
            ],
        )
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []
        assert not any("orphan" in f.path or "99" in f.path for f in findings)

    async def test_the_legitimate_row_is_still_actually_checked(self, tmp_path: Path) -> None:
        """Positive control: corrupting the legitimate row's chunk still surfaces a finding."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-healthy",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                ("orphan/999/dedup.img", 99, 99, 0, 1, 2),
            ],
        )
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)


class TestUnresolvableVersion:
    """A catalog-listed version that fails to resolve becomes a ``Stage.VERSION``
    finding classified by exception type: ``NotFoundError`` is
    ``Symptom.DATA_MISSING`` with stale/rotated wording; any other
    ``ApmRepoError`` is ``Symptom.CORRUPTION`` without it."""

    async def test_missing_target_db_is_a_version_stage_finding(self, tmp_path: Path) -> None:
        write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-broken",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            write_target_db=False,  # claims a target.db that's never actually written
        )
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.VERSION
        assert findings[0].symptom is Symptom.DATA_MISSING
        assert "stale/rotated" in findings[0].detail

    async def test_missing_file_map_row_is_also_data_missing(self, tmp_path: Path) -> None:
        """A ``NotFoundError`` deeper in resolution (``dedup.img`` has no
        ``db/file_map`` row) is also ``DATA_MISSING``: the exception type decides
        the symptom."""
        write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-broken",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            # write_target_db defaults to True -- target.db itself resolves fine.
        )
        # No db/file_map row for fsA/1/dedup.img.
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.VERSION
        assert findings[0].symptom is Symptom.DATA_MISSING

    async def test_corruption_during_resolution_is_symptom_corruption_not_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-``NotFoundError`` failure (corrupt, not absent) is
        ``CORRUPTION`` without the "stale/rotated" wording."""
        write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )

        import synology_apm_repo.sdk.units.verify_extents as verify_extents_module

        async def fake_fs_extents(repo: DedupRepo, version: Version) -> list[CompositionExtent]:
            raise DataCorruptError("simulated corrupt target.db", spec="test")

        monkeypatch.setattr(verify_extents_module, "_fs_extents", fake_fs_extents)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.VERSION
        assert findings[0].symptom is Symptom.CORRUPTION
        assert "stale/rotated" not in findings[0].detail


class TestCatalogEnumerationFailure:
    """A failing ``connections()``/``workloads()``/``versions()`` call becomes a
    ``Stage.VERSION`` ``Finding`` instead of propagating out of ``verify_reachable()``,
    except a ``StorageBackendError``."""

    async def test_connections_failure_is_a_version_stage_finding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        async def fake_connections(repo: DedupRepo) -> list[Connection]:
            raise DataCorruptError("simulated corrupt connection_config", spec="test")

        monkeypatch.setattr(verify_reachable_module, "connections", fake_connections)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.VERSION and f.symptom is Symptom.CORRUPTION for f in findings)

    async def test_a_storage_backend_failure_propagates_instead_of_becoming_a_finding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A network failure says nothing about the repository's data, so it
        must not be reported as damage."""

        async def fake_connections(repo: DedupRepo) -> list[Connection]:
            raise StorageBackendError("synthetic timeout")

        monkeypatch.setattr(verify_reachable_module, "connections", fake_connections)

        repo = await open_vault_repo(tmp_path)
        try:
            with pytest.raises(StorageBackendError, match="synthetic timeout"):
                await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

    async def test_one_connections_workloads_failure_does_not_abort_its_siblings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """connection-a's ``workloads()`` raising doesn't stop connection-b from
        being walked."""
        conn_a = make_connection(connection_id="conn-a", display_name="a", workload_count=0, version_count=0)
        conn_b = make_connection(
            connection_config_id=2, connection_id="conn-b", display_name="b", workload_count=0, version_count=0
        )
        seen_connections: list[Connection] = []

        async def fake_connections(repo: DedupRepo) -> list[Connection]:
            return [conn_a, conn_b]

        async def fake_workloads(repo: DedupRepo, connection: Connection) -> list[Workload]:
            seen_connections.append(connection)
            if connection is conn_a:
                raise NotFoundError("simulated missing workload_config", ref="workload_config")
            return []

        monkeypatch.setattr(verify_reachable_module, "connections", fake_connections)
        monkeypatch.setattr(verify_reachable_module, "workloads", fake_workloads)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert seen_connections == [conn_a, conn_b]  # conn_b was still reached after conn_a's failure
        assert any(f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING and f.path == "a" for f in findings)

    async def test_one_workloads_versions_failure_does_not_abort_its_siblings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same one level deeper: workload-1's ``versions()`` raising doesn't
        stop workload-2 from being walked."""
        conn_a = make_connection(connection_id="conn-a", display_name="a", workload_count=0, version_count=0)
        wl_1 = make_workload(display_name="workload-1")
        wl_2 = make_workload(workload_id=2, display_name="workload-2")
        seen_workloads: list[Workload] = []

        async def fake_connections(repo: DedupRepo) -> list[Connection]:
            return [conn_a]

        async def fake_workloads(repo: DedupRepo, connection: Connection) -> list[Workload]:
            return [wl_1, wl_2]

        async def fake_versions(repo: DedupRepo, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
            seen_workloads.append(workload)
            if workload is wl_1:
                raise NotFoundError("simulated missing copy_target_version", ref="copy_target_version")
            return []

        monkeypatch.setattr(verify_reachable_module, "connections", fake_connections)
        monkeypatch.setattr(verify_reachable_module, "workloads", fake_workloads)
        monkeypatch.setattr(verify_reachable_module, "versions", fake_versions)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert seen_workloads == [wl_1, wl_2]  # wl_2 was still reached after wl_1's failure
        assert any(
            f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING and f.path == "workload-1" for f in findings
        )


class TestFullVsQuickChunkCoverage:
    """FULL checks every live chunk in a touched bucket, the last included;
    QUICK never reads chunk content, so it misses a chunk-level corruption
    even in the first chunk."""

    def _build(self, tmp_path: Path, *, chunk_num: int, corrupt_chunk_crc_idx: int) -> None:
        plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(chunk_num)]
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
            entries=mapping_record(0, 0, 0, map_num=chunk_num),
        )
        write_bucket(
            tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=corrupt_chunk_crc_idx
        )
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

    async def test_quick_never_finds_bucket_content_corruption(self, tmp_path: Path) -> None:
        chunk_num = 3
        self._build(tmp_path, chunk_num=chunk_num, corrupt_chunk_crc_idx=0)
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert findings == []

    async def test_full_finds_a_corruption_in_the_last_chunk(self, tmp_path: Path) -> None:
        chunk_num = 3
        self._build(tmp_path, chunk_num=chunk_num, corrupt_chunk_crc_idx=chunk_num - 1)
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)


class TestMemoization:
    """A bucket/chunk two versions both reference (internal dedup) is
    checked only once, not once per referencing version."""

    async def test_shared_bucket_corruption_is_reported_only_once(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_a = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        dedup_img_b = write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-b",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        # Two file_map rows resolving to the same composition record, as
        # internal dedup produces.
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
            ],
        )
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        bucket_findings = [f for f in findings if f.stage is Stage.BUCKET]
        assert len(bucket_findings) == 1

    async def test_a_shared_record_window_is_walked_once(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Versions sharing one ``(record_key, start, end)`` window walk it once."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_a = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        dedup_img_b = write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-b",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_a, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_b, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
            ],
        )
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        from synology_apm_repo.sdk.dedup.chunk_walk import iter_bucket_keys as real_iter_bucket_keys
        from synology_apm_repo.sdk.dedup.dedup_file import DedupFile

        call_count = 0

        async def counting_iter_bucket_keys(
            base: DedupFile, start: int, end: int
        ) -> AsyncIterator[tuple[StreamId, BucketId]]:
            nonlocal call_count
            call_count += 1
            async for key in real_iter_bucket_keys(base, start, end):
                yield key

        monkeypatch.setattr(verify_walk_module, "iter_bucket_keys", counting_iter_bucket_keys)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        assert findings == []
        assert call_count == 1


class TestFullFingerprintMismatch:
    """FULL also checks the plaintext fingerprint: a wrong stored digest
    surfaces even when the chunk's ciphertext is intact."""

    async def test_wrong_fingerprint_is_a_finding(self, tmp_path: Path) -> None:
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
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts, wrong_digest_idx=0)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.symptom is Symptom.MISMATCH and "fingerprint" in f.detail for f in findings)
