"""Unit tests for ``synology_apm_repo.sdk.units.verify_reachable``'s
composition stage (``dedup.verify_walk.ReachabilitySweep.claim_extent``):
composition-stage findings and map-CRC repair propagation through the
shared composition-record cache."""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from support.format_builders import mapping_record, redundancy_blob_bytes
from support.repo_builders import (
    write_bucket,
    write_composition_entries,
    write_connection_config,
    write_file_map,
    write_inf_and_fgp,
    write_repo_info,
)
from synology_apm_repo.sdk.cachemanager import DEFAULT_LIMITS
from synology_apm_repo.sdk.dedup import verify_walk as verify_walk_module
from synology_apm_repo.sdk.dedup.composition_reader import CompositionRecord
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.findings import Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.format.const import REDUNDANCY_COVERAGE_COMPOSITION
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.verify_reachable import verify_reachable
from unit.sdk.verify_reachable_fakes import (
    write_damaged_composition,
    write_fs_workload,
    write_pcps_workload,
)

_STREAM_ID = 8
_SESSION_ID = 3
_COMP_OFFSET = 64  # right after the composition sub-file's own 64-byte cMpS header


async def _open(tmp_path: Path, *, composition_records: int | None = None) -> DedupRepo:
    write_repo_info(tmp_path / "repo_info")
    write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    if composition_records is None:
        return await DedupRepo.open(store, layout)
    return await DedupRepo.open(
        store, layout, limits=dataclasses.replace(DEFAULT_LIMITS, composition_records=composition_records)
    )


class TestCompositionStageFindings:
    """Composition-stage failures in ``ReachabilitySweep.claim_extent``
    become ``Finding``s rather than ending the ``verify_reachable()`` run."""

    async def test_corrupt_composition_header_is_a_finding(self, tmp_path: Path) -> None:
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
        write_damaged_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
            corrupt_header=True,
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.COMPOSITION and f.symptom is Symptom.CORRUPTION for f in findings)

    async def test_corrupt_record_head_is_caught_as_a_finding_not_a_crash(self, tmp_path: Path) -> None:
        """A broken ``RecordHead`` is a ``Stage.FILE_MAP`` finding, and
        ``claim_extent`` skips ``iter_bucket_keys`` for that extent."""
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
        write_damaged_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
            corrupt_record_head=True,
        )

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.FILE_MAP and f.symptom is Symptom.CORRUPTION for f in findings)

    async def test_corrupt_record_head_is_only_reported_once_across_a_shared_record(self, tmp_path: Path) -> None:
        """Two versions sharing a broken record: the "broken" outcome is
        memoized in ``_record_checks``, so the second visit also skips
        ``iter_bucket_keys``."""
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path_1 = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        dedup_img_path_2 = write_fs_workload(
            tmp_path,
            workload_id=11,
            version_uid="vuid-2",
            target_id="fsB",
            meta_dirname="FSB_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                (dedup_img_path_1, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                (dedup_img_path_2, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
            ],
        )
        write_damaged_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
            corrupt_record_head=True,
        )

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        record_head_findings = [f for f in findings if f.stage is Stage.FILE_MAP and f.symptom is Symptom.CORRUPTION]
        assert len(record_head_findings) == 1

    async def test_corrupt_individual_chunk_map_entry_is_a_finding_not_a_crash(self, tmp_path: Path) -> None:
        """``RecordHead`` and ``mapCrc`` are valid (the CRC covers the bad
        entry as written), but ``iter_bucket_keys`` raises on the entry's
        invalid kind nibble; ``claim_extent`` turns that into a ``Finding``."""
        bad_entry = bytes([0x0F]) + bytes(19)  # invalid kind nibble -- neither MAPPING(0) nor ZERO(1)
        good_entry = mapping_record(0, 0, 0, map_num=1)
        entries = bad_entry + good_entry
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=2 * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=_SESSION_ID, entries=entries
        )

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(
            f.stage is Stage.COMPOSITION and f.symptom is Symptom.CORRUPTION and "chunk-map walk failed" in f.detail
            for f in findings
        )

    async def test_cache_miss_record_fetch_failure_is_a_finding_not_a_crash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failure in ``extent.dedup_file.cached_record()``, called ahead
        of ``iter_bucket_keys``, is a ``Finding`` too."""
        from synology_apm_repo.sdk.dedup.dedup_file import DedupFile

        async def failing_get_record(self: DedupFile) -> None:
            raise NotFoundError("synthetic record-fetch failure", ref="synthetic")

        monkeypatch.setattr(DedupFile, "cached_record", failing_get_record)

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
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [bytes([1]) * 4096])
        write_inf_and_fgp(tmp_path / "@data" / "Pool", [bytes([1]) * 4096])

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(
            f.stage is Stage.COMPOSITION
            and f.symptom is Symptom.CORRUPTION
            and "synthetic record-fetch failure" in f.detail
            for f in findings
        )

    async def test_earlier_bucket_claim_survives_a_later_walk_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A bucket already yielded by ``iter_bucket_keys`` stays claimed
        when a later entry aborts the walk: ``claim_extent`` claims key by
        key. The walk is faked; a real mid-walk failure past a yielded key
        would need multi-page fixture data."""
        claimed_key = (StreamId(_STREAM_ID), BucketId(0))

        async def one_key_then_raise(*args: object, **kwargs: object) -> AsyncIterator[tuple[StreamId, BucketId]]:
            yield claimed_key
            raise DataCorruptError("synthetic later-window corruption", ref="synthetic")

        monkeypatch.setattr(verify_walk_module, "iter_bucket_keys", one_key_then_raise)

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
        # Bucket 0's only chunk fails its CRC: found only if the claim
        # before the faked walk's raise survived.
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, corrupt_chunk_crc_idx=0)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(
            f.stage is Stage.COMPOSITION
            and f.symptom is Symptom.CORRUPTION
            and "synthetic later-window corruption" in f.detail
            for f in findings
        )
        assert any(f.stage is Stage.BUCKET for f in findings)


class TestMapCrcRepairPropagation:
    """``check_map_and_attr_crc``'s repair is in-memory only; the repaired
    array must be seeded into the shared ``CompositionRecord``
    (``seed_pages_from_array``) so every extent's ``iter_bucket_keys`` walk
    reads the fixed entries, not the corrupted on-disk ones.

    Each test corrupts byte 13 of an all-zero ``ChunkAddress`` (inside its
    ``bucket_id`` bits), which decodes to ``bucket_id=255`` without raising:
    an unseeded walk would claim that nonexistent bucket and report it
    ``DATA_MISSING``."""

    async def test_both_versions_sharing_the_repaired_record_use_the_fixed_bytes(self, tmp_path: Path) -> None:
        """Two versions share one record whose sole ``ChunkMapRecord`` entry
        is corrupted on disk but parity-recoverable."""
        plaintexts = [bytes([1]) * 4096]
        good_entry = mapping_record(0, 0, 0, map_num=1)
        corrupted_entry = bytearray(good_entry)
        corrupted_entry[13] ^= 0xFF

        redundancy_blob = redundancy_blob_bytes(good_entry, coverage=REDUNDANCY_COVERAGE_COMPOSITION)

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
        write_damaged_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=good_entry,
            on_disk_entries=bytes(corrupted_entry),
            trailer=redundancy_blob,
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        repaired_findings = [f for f in findings if f.symptom is Symptom.REPAIRED_VIA_PARITY]
        assert len(repaired_findings) == 1  # memoized once per record, not once per sharing version
        bucket_findings = [f for f in findings if f.stage is Stage.BUCKET]
        assert bucket_findings == []  # neither version's walk wandered into the phantom bucket_id=255

    async def test_repair_propagation_survives_a_fully_evicted_composition_record_cache(self, tmp_path: Path) -> None:
        """The sibling test with ``composition_records=0``, so version B's
        visit is a cold refetch of the record: it is still reseeded from
        ``_record_checks``' ``repaired_map_array``, and the check (with its
        ``REPAIRED_VIA_PARITY`` finding) still runs once."""
        plaintexts = [bytes([1]) * 4096]
        good_entry = mapping_record(0, 0, 0, map_num=1)
        corrupted_entry = bytearray(good_entry)
        corrupted_entry[13] ^= 0xFF

        redundancy_blob = redundancy_blob_bytes(good_entry, coverage=REDUNDANCY_COVERAGE_COMPOSITION)

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
        write_damaged_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=good_entry,
            on_disk_entries=bytes(corrupted_entry),
            trailer=redundancy_blob,
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path, composition_records=0)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        repaired_findings = [f for f in findings if f.symptom is Symptom.REPAIRED_VIA_PARITY]
        assert len(repaired_findings) == 1  # still memoized once, even though every cache access was a miss
        bucket_findings = [f for f in findings if f.stage is Stage.BUCKET]
        assert bucket_findings == []  # version B's cold-refetched record was still correctly reseeded

    async def test_seeding_overrides_a_page_pcps_open_disk_already_prewarmed(self, tmp_path: Path) -> None:
        """``PcpsDiskTree.open_disk()`` calls ``CompositionRecord.extent()``,
        caching the first/last page off disk before ``claim_extent``'s repair.
        For this single-page record, ``seed_pages_from_array`` must replace
        that already-settled page, not keep the pre-repair one."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        good_entry = mapping_record(0, 0, 0, map_num=1)
        corrupted_entry = bytearray(good_entry)
        corrupted_entry[13] ^= 0xFF

        redundancy_blob = redundancy_blob_bytes(good_entry, coverage=REDUNDANCY_COVERAGE_COMPOSITION)

        write_pcps_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-pcps",
            target_id="PC-uid",
            fid=100,
            src_file_path=src_file_path,
            disk_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_damaged_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=good_entry,
            on_disk_entries=bytes(corrupted_entry),
            trailer=redundancy_blob,
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        assert any(f.symptom is Symptom.REPAIRED_VIA_PARITY for f in findings)
        assert [f for f in findings if f.stage is Stage.BUCKET] == []  # not the phantom bucket_id=255

    async def test_seed_pages_from_array_failure_is_its_own_finding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``seed_pages_from_array``'s length-mismatch ``ValueError`` is a
        ``Symptom.CORRUPTION`` finding."""

        async def failing_seed(self: CompositionRecord, array_raw: bytes) -> None:
            raise ValueError("synthetic reseed failure")

        monkeypatch.setattr(CompositionRecord, "seed_pages_from_array", failing_seed)

        plaintexts = [bytes([1]) * 4096]
        good_entry = mapping_record(0, 0, 0, map_num=1)
        corrupted_entry = bytearray(good_entry)
        corrupted_entry[13] ^= 0xFF

        redundancy_blob = redundancy_blob_bytes(good_entry, coverage=REDUNDANCY_COVERAGE_COMPOSITION)

        dedup_img = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_damaged_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=good_entry,
            on_disk_entries=bytes(corrupted_entry),
            trailer=redundancy_blob,
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await _open(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()

        reseed_findings = [f for f in findings if "synthetic reseed failure" in f.detail]
        assert len(reseed_findings) == 1
        assert reseed_findings[0].symptom is Symptom.CORRUPTION
        assert reseed_findings[0].stage is Stage.COMPOSITION

    async def test_unrelated_valueerror_from_chunk_walk_is_not_absorbed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``ValueError`` from the chunk-map walk (a broken invariant, not
        corruption) propagates instead of becoming a finding."""

        async def failing_walk(*args: object, **kwargs: object) -> AsyncIterator[tuple[StreamId, BucketId]]:
            raise ValueError("synthetic caller-invariant violation")
            yield  # pragma: no cover - never reached; makes this an async generator

        monkeypatch.setattr(verify_walk_module, "iter_bucket_keys", failing_walk)

        dedup_img = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-a",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [bytes([1]) * 4096])
        write_inf_and_fgp(tmp_path / "@data" / "Pool", [bytes([1]) * 4096])

        repo = await _open(tmp_path)
        try:
            with pytest.raises(ValueError, match="synthetic caller-invariant violation"):
                await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
