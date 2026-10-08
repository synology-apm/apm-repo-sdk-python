"""Unit tests for ``synology_apm_repo.sdk.units.verify_extents``'s
per-workload-type ``composition_extents_for_version`` branches
(FS/VM/PCPS/SaaS), driven through a ``verify_reachable`` walk, including
SaaS resolution labels, superseded generations and run-scoped caching."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from support.format_builders import (
    mapping_record,
)
from support.repo_builders import (
    version_spec_json,
    write_bucket,
    write_composition_entries,
    write_copy_target_file,
    write_copy_target_version,
    write_copy_target_version_meta,
    write_file_map,
    write_inf_and_fgp,
    write_pcps_file_meta,
    write_saas_snapshot_db,
    write_saas_version_db,
    write_workload_config,
)
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workload_by_id
from synology_apm_repo.sdk.errors import DataCorruptError
from synology_apm_repo.sdk.findings import Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.identifiers import WorkloadId
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.verify_extents import _annotate_with_saas_resolution
from synology_apm_repo.sdk.units.verify_reachable import verify_reachable
from unit.sdk.verify_reachable_fakes import (
    open_vault_repo,
    write_damaged_composition,
    write_file_meta_sizes,
    write_fs_workload,
    write_pcps_workload,
)

_STREAM_ID = 8
_SESSION_ID = 3
_COMP_OFFSET = 64  # right after the composition sub-file's own 64-byte cMpS header


def _write_vm_target_db(
    path: Path,
    *,
    config_device_id: int,
    disk_name: str,
    src_file_path: str,
    extra_objects: list[tuple[int, int, str, str, int, int | None]] | None = None,
) -> None:
    """One VM device with one dedup-object disk (``data_format=1``, plain
    dedup) -- the minimum ``DeviceProvider`` needs to resolve one disk.

    ``extra_objects``: ``(object_id, data_format, file_path, src_file_path,
    dedup_object, file_size)`` rows appended alongside the main disk.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_table(id INTEGER PRIMARY KEY, version_id INTEGER, data_format INTEGER, "
        "status INTEGER, folder_name TEXT)"
    )
    conn.execute("INSERT INTO version_table VALUES (1, 1, 1, 1, 'folder')")
    conn.execute(
        "CREATE TABLE device_table(device_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "device_uuid TEXT, host_name TEXT, os_name TEXT)"
    )
    conn.execute("INSERT INTO device_table VALUES (1, 1, ?, 'device-uuid', 'my-vm', 'Windows')", (config_device_id,))
    conn.execute(
        "CREATE TABLE object_table(object_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "data_format INTEGER, file_path TEXT, src_file_path TEXT, temp_postfix TEXT, dedup_object INTEGER, "
        "file_size INTEGER)"
    )
    conn.execute(
        "INSERT INTO object_table VALUES (1, 1, ?, 1, ?, ?, '', 1, 4096)",
        (config_device_id, disk_name, src_file_path),
    )
    for object_id, data_format, file_path, extra_src_path, dedup_object, file_size in extra_objects or []:
        conn.execute(
            "INSERT INTO object_table VALUES (?, 1, ?, ?, ?, ?, '', ?, ?)",
            (object_id, config_device_id, data_format, file_path, extra_src_path, dedup_object, file_size),
        )
    conn.commit()
    conn.close()


def _write_vm_workload(
    tmp_path: Path,
    *,
    workload_id: int,
    version_uid: str,
    target_id: str,
    meta_dirname: str,
    disk_name: str,
    src_file_path: str,
    extra_objects: list[tuple[int, int, str, str, int, int | None]] | None = None,
) -> None:
    """Register one resolvable VM workload+version, the VM counterpart to
    ``write_fs_workload``. The caller writes the disk's ``db/file_map`` row,
    keyed by ``src_file_path``."""
    vm_spec: dict[str, object] = {"namespace": "ns-a", "spec": {"workload_type": "VM", "workload_name": target_id}}
    write_workload_config(tmp_path / "db" / "workload_config", [(workload_id, f"{target_id}-uid", "VM", vm_spec)])
    write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                workload_id * 100,
                workload_id,
                1,
                version_uid,
                "VM",
                target_id,
                "",
                "",
                0,
                0,
                version_spec_json(1786000000, status="COMPLETED"),
            )
        ],
    )
    write_copy_target_version_meta(
        tmp_path / "db" / "copy_target_version_meta",
        [(version_uid, f"/pv/copy_meta_file/{meta_dirname}", ["target.db"], 1)],
    )
    _write_vm_target_db(
        tmp_path / "copy_meta_file" / meta_dirname / "target.db",
        config_device_id=1,
        disk_name=disk_name,
        src_file_path=src_file_path,
        extra_objects=extra_objects,
    )


def _write_saas_workload(
    tmp_path: Path,
    *,
    workload_id: int,
    version_uid: str,
    stream_uuid: str,
    connection_config_id: int,
    saas_obj_size: int,
) -> str:
    """Register one resolvable M365 workload+version: the catalog-level
    ``workload_config``/``copy_target_version`` rows plus the stream's own
    ``saas_snapshot``/``saas_version`` dbs.

    ``connection_config_id`` must have a ``db/connection_config`` row;
    ``open_vault_repo()`` writes one (``1`` -> ``connection_id="conn-a"``). Returns
    the ``saas_obj`` path for the caller's ``file_map``/``file_meta`` rows,
    with ``connection_id`` as its middle segment (the Copy form
    ``SaasStream`` tries first).
    """
    saas_spec: dict[str, object] = {
        "namespace": "ns-a",
        "spec": {"workload_type": "M365", "workload_name": stream_uuid},
    }
    write_workload_config(tmp_path / "db" / "workload_config", [(workload_id, f"{stream_uuid}-uid", "M365", saas_spec)])
    write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                workload_id * 100,
                workload_id,
                connection_config_id,
                version_uid,
                "M365",
                stream_uuid,
                stream_uuid,
                "snap-uuid-1",
                3,
                0,
                version_spec_json(1786000000, status="COMPLETED"),
            )
        ],
    )
    stream_db_dir = tmp_path / "saas" / str(connection_config_id) / stream_uuid / "db"
    write_saas_snapshot_db(
        stream_db_dir / "saas_snapshot",
        snapshots=[(1, "snap-uuid-1", 3, 1)],
        distribution=[(0, saas_obj_size, 1, 3)],
    )
    write_saas_version_db(stream_db_dir / "saas_version", versions=[(1, 3, 1, 0)], target_type="M365")
    return f"{stream_uuid}/conn-a/1/saas_obj"


def _write_saas_workload_multi_generation(
    tmp_path: Path,
    *,
    workload_id: int,
    stream_uuid: str,
    connection_config_id: int,
    live_stream_version: int,
    total_versions: int,
    latest_complete_version: int,
) -> str:
    """Like ``_write_saas_workload`` but registers ``total_versions``
    catalog versions (``saas_version_id`` 1..``total_versions``) in one
    snapshot, each mapped to the equal-numbered ``stream_version`` -- the
    shape left once older generations are server-side GC'd (FORMAT-SPEC.md:
    Generic SaaS object addressing). Returns the ``live_stream_version``
    generation's ``saas_obj`` path; the caller writes its rows."""
    saas_spec: dict[str, object] = {
        "namespace": "ns-a",
        "spec": {"workload_type": "M365", "workload_name": stream_uuid},
    }
    write_workload_config(tmp_path / "db" / "workload_config", [(workload_id, f"{stream_uuid}-uid", "M365", saas_spec)])
    write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                workload_id * 100 + n,
                workload_id,
                connection_config_id,
                f"vuid-saas-gen{n}",
                "M365",
                stream_uuid,
                stream_uuid,
                "snap-uuid-1",
                n,
                0,
                version_spec_json(1786000000 + n, status="COMPLETED"),
            )
            for n in range(1, total_versions + 1)
        ],
    )
    stream_db_dir = tmp_path / "saas" / str(connection_config_id) / stream_uuid / "db"
    write_saas_snapshot_db(
        stream_db_dir / "saas_snapshot",
        snapshots=[(1, "snap-uuid-1", 1, 1)],
        distribution=[(0, 4096, 1, n) for n in range(1, total_versions + 1)],
    )
    write_saas_version_db(
        stream_db_dir / "saas_version",
        versions=[(1, n, n, 0) for n in range(1, total_versions + 1)],
        target_type="M365",
        latest_complete_version=latest_complete_version,
    )
    return f"{stream_uuid}/conn-a/{live_stream_version}/saas_obj"


class TestFsExtentSizeUnavailable:
    """The FS branch returns no extent when the ``dedup.img`` size can't be
    resolved (no ``db/file_meta`` row)."""

    async def test_no_file_meta_row_yields_no_extents_and_no_crash(self, tmp_path: Path) -> None:
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,  # dedup_img_size omitted -- no db/file_meta row at all
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        # deliberately no bucket at all -- if this version's extent were
        # checked at all, the missing bucket would surface as a finding.

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert findings == []


class TestVmExtents:
    """``_vm_extents`` -- a VM version's disk objects, resolved via
    ``DeviceProvider``."""

    async def test_vm_disk_extent_is_checked(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        _write_vm_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-vm",
            target_id="VM-uid",
            meta_dirname="VM_meta",
            disk_name="disk.img",
            src_file_path=src_file_path,
        )
        write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
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

    async def test_non_dedup_unsupported_and_unresolved_size_objects_are_skipped(self, tmp_path: Path) -> None:
        """A sidecar file (``dedup_object=0``), a dedup object in an
        unreadable data_format (CBT, ``data_format=2``) and a dedup object
        with a ``NULL`` ``file_size`` are all skipped, not raised on."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        _write_vm_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-vm",
            target_id="VM-uid",
            meta_dirname="VM_meta",
            disk_name="disk.img",
            src_file_path=src_file_path,
            extra_objects=[
                (2, 1, "sidecar.txt", "VM-uid/ActiveBackup_2026-01-01/my-vm/sidecar.txt", 0, 10),
                (3, 1, "disk1.img", "VM-uid/ActiveBackup_2026-01-01/my-vm/disk1.img", 1, None),
                (4, 2, "cbt.img", "VM-uid/ActiveBackup_2026-01-01/my-vm/cbt.img", 1, 4096),
            ],
        )
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                (src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
                # The NULL-file_size disk resolves (a file_map miss would
                # be a per-disk finding instead, tested below); only its
                # content.size is None.
                ("VM-uid/ActiveBackup_2026-01-01/my-vm/disk1.img", _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2),
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

    async def test_one_unresolvable_disk_does_not_discard_another_resolvable_disks_findings(
        self, tmp_path: Path
    ) -> None:
        """A ``file_map`` miss on one disk becomes that disk's own
        ``Stage.VERSION``/``DATA_MISSING`` finding; the other disk is still
        checked."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        missing_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk1.img"
        _write_vm_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-vm",
            target_id="VM-uid",
            meta_dirname="VM_meta",
            disk_name="disk.img",
            src_file_path=src_file_path,
            extra_objects=[
                # A dedup disk with no db/file_map row below.
                (3, 1, "disk1.img", missing_file_path, 1, 4096),
            ],
        )
        write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
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
        assert any(
            f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING and "disk1.img" in f.path for f in findings
        )

    async def test_unresolvable_disk_with_a_non_notfound_error_is_a_corruption_finding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A disk failing with an ``ApmRepoError`` other than
        ``NotFoundError`` is ``Symptom.CORRUPTION``, not ``DATA_MISSING``."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        _write_vm_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-vm",
            target_id="VM-uid",
            meta_dirname="VM_meta",
            disk_name="disk.img",
            src_file_path=src_file_path,
        )
        write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        import synology_apm_repo.sdk.units.device as device_module

        async def fake_unit(self: device_module.DeviceProvider, node: Node) -> object:
            raise DataCorruptError("synthetic corruption")

        monkeypatch.setattr(device_module.DeviceProvider, "unit", fake_unit)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.VERSION and f.symptom is Symptom.CORRUPTION for f in findings)


class TestPcpsExtents:
    """``_pcps_extents`` -- one ``CompositionExtent`` per fragment of each
    PC/PS disk ``DeviceProvider`` resolves."""

    async def test_pcps_fragment_extent_is_checked(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
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

    async def test_one_unresolvable_disk_does_not_discard_another_resolvable_disks_findings(
        self, tmp_path: Path
    ) -> None:
        """The PC/PS counterpart of ``TestVmExtents``'s test of the same
        name. Neither filename matches the ``D(...)S(...)`` fragment-grouping
        convention, so each is its own disk; the second's only fragment has
        no ``file_map`` row, so ``PcpsDiskTree.open_disk()`` raises
        ``NotFoundError`` for that disk alone."""
        plaintexts = [bytes([1]) * 4096]
        src_file_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        missing_file_path = "PC-uid/ActiveBackup_2026-01-01/disk1.img"
        version_id = 1000
        write_workload_config(
            tmp_path / "db" / "workload_config",
            [
                (
                    10,
                    "PC-uid-uid",
                    "PC",
                    {"namespace": "ns-a", "spec": {"workload_type": "PC", "workload_name": "PC-uid"}},
                )
            ],
        )
        write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [
                (
                    version_id,
                    10,
                    1,
                    "vuid-pcps",
                    "PC",
                    "PC-uid",
                    "",
                    "",
                    0,
                    0,
                    version_spec_json(1786000000, status="COMPLETED"),
                )
            ],
        )
        write_copy_target_file(tmp_path / "db" / "copy_target_version", [(version_id, 100), (version_id, 200)])
        write_pcps_file_meta(
            tmp_path / "db" / "file_meta",
            [(100, src_file_path, len(plaintexts) * 4096), (200, missing_file_path, 4096)],
        )
        write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
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
        assert any(f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING for f in findings)

    async def test_unit_with_unexpected_content_type_is_skipped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Defensive branch: ``DeviceProvider.unit()`` returns a
        ``VirtualDiskContentSource`` for every PC/PS disk today, but any
        other content type is skipped rather than crashed on."""
        src_file_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        write_pcps_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-pcps",
            target_id="PC-uid",
            fid=100,
            src_file_path=src_file_path,
            disk_size=4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(src_file_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        # No bucket: checking the fragment would surface it as a finding.

        import synology_apm_repo.sdk.units.device as device_module

        async def fake_unit(self: device_module.DeviceProvider, node: Node) -> SimpleNamespace:
            return SimpleNamespace(content=object())

        monkeypatch.setattr(device_module.DeviceProvider, "unit", fake_unit)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert findings == []


class TestSaasExtents:
    """``_saas_extents`` -- an M365/GWS version's whole ``saas_obj``,
    resolved via ``SaasStreamCache.resolve_saas_obj``."""

    async def test_saas_obj_extent_is_checked(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        saas_obj_path = _write_saas_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-saas",
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            saas_obj_size=len(plaintexts) * 4096,
        )
        write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, len(plaintexts) * 4096)])
        write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
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

    async def test_saas_obj_size_unavailable_yields_no_extent_and_no_crash(self, tmp_path: Path) -> None:
        """The SaaS counterpart of ``TestFsExtentSizeUnavailable``."""
        saas_obj_path = _write_saas_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-saas",
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            saas_obj_size=4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        # deliberately no bucket at all -- if this version's extent were
        # checked at all, the missing bucket would surface as a finding.

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert findings == []


class TestSaasResolutionLabel:
    """``_annotate_with_saas_resolution`` reaches a ``Finding.path`` only via
    ``CompositionExtent.unit_label`` (composition-stage findings); bucket
    findings carry ``_bucket_claim``'s ref instead. The annotation is
    tested directly against a ``SaasStreamCache``: end to end,
    ``_checked_sessions`` checks a shared composition sub-file's header
    only for whichever catalog version is walked first, so a specific
    version's annotation can't be targeted that way."""

    async def test_annotates_when_a_substitution_happened(self, tmp_path: Path) -> None:
        saas_obj_path = _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,
            total_versions=3,
            latest_complete_version=3,
        )
        write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        repo = await open_vault_repo(tmp_path)
        try:
            workload = await workload_by_id(repo, WorkloadId(10))
            assert workload is not None
            all_versions = await versions(repo, workload)
            version = next(v for v in all_versions if v.saas_version_id == 1)
            async with SaasStreamCache(repo) as cache:
                label = _annotate_with_saas_resolution("SaaS saas_obj", await cache.resolve_saas_obj(version))
        finally:
            await repo.close()
        assert label == "SaaS saas_obj (stream_version 3, requested 1)"

    async def test_leaves_label_unannotated_when_no_substitution_happened(self, tmp_path: Path) -> None:
        saas_obj_path = _write_saas_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-saas",
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            saas_obj_size=4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        repo = await open_vault_repo(tmp_path)
        try:
            workload = await workload_by_id(repo, WorkloadId(10))
            assert workload is not None
            [version] = await versions(repo, workload)
            async with SaasStreamCache(repo) as cache:
                label = _annotate_with_saas_resolution("SaaS saas_obj", await cache.resolve_saas_obj(version))
        finally:
            await repo.close()
        assert label == "SaaS saas_obj"

    async def test_end_to_end_composition_corruption_finding_carries_the_label(self, tmp_path: Path) -> None:
        """``_saas_extents``'s label reaches ``Finding.path`` through the
        full ``verify_reachable()`` pipeline. Single-version (no
        substitution), for the reason in the class docstring."""
        plaintexts = [bytes([1]) * 4096]
        saas_obj_path = _write_saas_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-saas",
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            saas_obj_size=len(plaintexts) * 4096,
        )
        write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, len(plaintexts) * 4096)])
        write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_damaged_composition(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
            corrupt_header=True,
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        corruptions = [f for f in findings if f.stage is Stage.COMPOSITION and f.symptom is Symptom.CORRUPTION]
        assert corruptions
        assert all(f.path == "SaaS saas_obj" for f in corruptions)


class TestSaasSupersededGeneration:
    """Catalog versions whose ``stream_version`` generation was GC'd
    resolve forward to the live one instead of reporting ``DATA_MISSING``
    -- but only to a generation that really is complete and present."""

    async def test_older_generations_resolve_via_the_substituted_live_one(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        saas_obj_path = _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,
            total_versions=3,
            latest_complete_version=3,
        )
        write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, len(plaintexts) * 4096)])
        write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
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
        assert not any(f.symptom is Symptom.DATA_MISSING for f in findings)
        # The substituted generation is really checked, not skipped.
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH for f in findings)

    async def test_genuine_gap_still_reports_data_missing(self, tmp_path: Path) -> None:
        """No generation of the stream has a ``file_map`` row."""
        _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,  # never actually written to file_map below
            total_versions=3,
            latest_complete_version=3,
        )
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        missing = [f for f in findings if f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING]
        assert len(missing) == 3

    async def test_generation_past_latest_complete_version_is_not_used_as_substitute(self, tmp_path: Path) -> None:
        """A ``file_map`` row at ``stream_version=3`` past
        ``latest_complete_version=1`` (an incomplete write's leftover) is not
        a substitute."""
        _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,
            total_versions=3,
            latest_complete_version=1,
        )
        saas_obj_path = "stream-uuid-1/conn-a/3/saas_obj"
        write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, 4096)])
        write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [bytes([1]) * 4096])
        write_inf_and_fgp(tmp_path / "@data" / "Pool", [bytes([1]) * 4096])

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        missing = [f for f in findings if f.stage is Stage.VERSION and f.symptom is Symptom.DATA_MISSING]
        assert len(missing) == 3


class TestSaasCachingAcrossWalkerRun:
    """``_saas_extents`` resolves through the walker's run-scoped
    ``SaasStreamCache``, so a stream's forward-resolution caches are built
    once per run, not once per catalog version."""

    async def test_file_map_prefix_scan_runs_once_per_middle_not_once_per_catalog_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        plaintexts = [bytes([1]) * 4096]
        saas_obj_path = _write_saas_workload_multi_generation(
            tmp_path,
            workload_id=10,
            stream_uuid="stream-uuid-1",
            connection_config_id=1,
            live_stream_version=3,
            total_versions=3,
            latest_complete_version=3,
        )
        write_file_meta_sizes(tmp_path / "db" / "file_meta", [(saas_obj_path, len(plaintexts) * 4096)])
        write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)
        write_inf_and_fgp(tmp_path / "@data" / "Pool", plaintexts)

        repo = await open_vault_repo(tmp_path)
        calls = 0
        real_fn = repo.file_map_paths_with_prefix

        async def _counted(prefix: str, *, status: int | None = None) -> list[str]:
            nonlocal calls
            calls += 1
            return await real_fn(prefix, status=status)

        monkeypatch.setattr(repo, "file_map_paths_with_prefix", _counted)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert not any(f.symptom is Symptom.DATA_MISSING for f in findings)
        # 3 catalog versions sharing one stream, 2 candidate middles
        # (connection_id, numeric ccid): once each per run, not 6.
        assert calls == 2
