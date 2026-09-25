"""Unit tests for ``synology_apm_repo.sdk.catalog.workload`` —
synthetic repository roots written to real files, no sample repositories
required (see ``tests/integration/sdk/test_catalog_catalog.py`` for the
byte-for-byte cross-check against apv-sample-1's exact display names and
counts)."""

from __future__ import annotations

import json
import sqlite3
import zlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import ConnectionConfigId, WorkloadId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _write_repo_info(path: Path) -> None:
    payload = json.dumps({"repo_type": 2}).encode("utf-8")
    header = bytearray(64)
    header[0:4] = b"RpiF"
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[20:36] = b"a" * 16
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + payload)


def _db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(path)


def _write_connection_config(path: Path, rows: list[tuple[int, str, int]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, "
        "connection_id TEXT, version_type INTEGER)"
    )
    conn.executemany("INSERT INTO connection_config VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_vault_link_key(path: Path, keys: list[str]) -> None:
    conn = _db(path)
    conn.execute("CREATE TABLE vault_link_key(key TEXT)")
    conn.executemany("INSERT INTO vault_link_key VALUES (?)", [(k,) for k in keys])
    conn.commit()
    conn.close()


def _write_workload_config(path: Path, rows: list[tuple[int, str, str, dict[str, object]]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE workload_config(workload_id INTEGER PRIMARY KEY, workload_uid TEXT, "
        "workload_type TEXT, workload_spec TEXT)"
    )
    conn.executemany(
        "INSERT INTO workload_config VALUES (?, ?, ?, ?)",
        [(wid, uid, wtype, json.dumps(spec)) for wid, uid, wtype, spec in rows],
    )
    conn.commit()
    conn.close()


def _write_copy_target_version(path: Path, rows: list[tuple[object, ...]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE copy_target_version(version_id INTEGER PRIMARY KEY, workload_id INTEGER, "
        "connection_config_id INTEGER, version_uid TEXT, target_type TEXT, target_id TEXT, "
        "saas_stream_uuid TEXT, saas_snapshot_uuid TEXT, saas_version_id INTEGER, deleted INTEGER, "
        "version_spec TEXT)"
    )
    conn.executemany("INSERT INTO copy_target_version VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _version_spec_json(start_time: object = None, end_time: object = None, status: object = None) -> str:
    """A minimal real-shaped ``version_spec`` blob — just the
    ``status.start_time``/``status.end_time``/``status.status`` fields
    this module actually reads (real production values are
    protobuf-JSON int64-as-string, e.g. ``"1786024626"``); omitted keys
    model a field genuinely absent from a real row, not just
    zero/empty."""
    status_obj: dict[str, object] = {}
    if start_time is not None:
        status_obj["start_time"] = str(start_time)
    if end_time is not None:
        status_obj["end_time"] = str(end_time)
    if status is not None:
        status_obj["status"] = status
    return json.dumps({"status": status_obj})


def _write_copy_target_version_meta(path: Path, rows: list[tuple[str, str, list[str], int]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE copy_target_version_meta(version_uid TEXT PRIMARY KEY, target_meta_path TEXT, "
        "meta_filenames TEXT, status INTEGER)"
    )
    conn.executemany(
        "INSERT INTO copy_target_version_meta VALUES (?, ?, ?, ?)",
        [(uid, path_, json.dumps(names), status) for uid, path_, names, status in rows],
    )
    conn.commit()
    conn.close()


_VM_SPEC: dict[str, Any] = {
    "namespace": "ns-a",
    "spec": {
        "workload_type": "VM",
        "workload_name": "my-vm",
        "config_vm": {"os_name": "Windows 10", "hypervisor_name": "Cluster-02"},
    },
}
_FS_SPEC: dict[str, Any] = {
    "namespace": "ns-a",
    "spec": {"workload_type": "FS", "workload_name": "10.0.0.1", "config_fs": {"os_name": "smb"}},
}
_MAIL_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "MAIL"},
    "status": {"entity_meta": {"spec": {"user_info": {"name": "Alice", "email": "alice@x.com"}}}},
}
_SITE_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "SITE"},
    "status": {"entity_meta": {"spec": {"site_info": {"site_name": "My Site"}}}},
}
_GROUP_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "TEAM_DRIVE"},
    "status": {"entity_meta": {"spec": {"group_info": {"display_name": "My Group"}}}},
}
_TEAM_DRIVE_SPEC: dict[str, Any] = {
    # Real shape (apv-sample-1): no user_info/site_info/
    # group_info at all, only team_drive_info.
    "namespace": "ns-b",
    "spec": {"workload_type": "TEAM_DRIVE"},
    "status": {
        "entity_meta": {
            "spec": {
                "user_info": None,
                "team_drive_info": {"id": "drive-1", "name": "Test-Workload-03", "team_drive_status": "AVAILABLE"},
                "group_info": None,
            }
        }
    },
}
_TEAM_SPEC: dict[str, Any] = {
    # Real shape (apv-sample-1): a TEAMS workload's
    # entity_meta.spec has no user_info/site_info/group_info/
    # team_drive_info at all, only team_info.
    "namespace": "ns-b",
    "spec": {"workload_type": "TEAMS"},
    "status": {
        "entity_meta": {
            "spec": {
                "user_info": None,
                "group_info": None,
                "team_info": {"id": "team-1", "name": "Teams-Workload-02", "visibility": 0},
            }
        }
    },
}


@pytest.fixture
def repo_root(tmp_path: Path) -> Path:
    _write_repo_info(tmp_path / "repo_info")
    _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1), (2, "conn-b", 1)])
    _write_vault_link_key(
        tmp_path / "db" / "vault_link_key",
        ["conn-a_9053e422-uuid_Test-Workload-02", "conn-b_2d90eeaf-uuid_Test-Workload-01"],
    )
    _write_workload_config(
        tmp_path / "db" / "workload_config",
        [
            (10, "vm-uid", "VM", _VM_SPEC),
            (11, "fs-uid", "FS", _FS_SPEC),
            (12, "mail-uid", "GW", _MAIL_SPEC),
            (13, "site-uid", "M365", _SITE_SPEC),
            (14, "group-uid", "GW", _GROUP_SPEC),
            (15, "unknown-uid", "WEIRD", {"namespace": "ns-c", "spec": {}}),
        ],
    )
    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                100,
                10,
                1,
                "vuid-100",
                "VM",
                "target-1",
                "",
                "",
                0,
                0,
                _version_spec_json(start_time=1786024626, status="COMPLETED"),
            ),
            (
                101,
                11,
                1,
                "vuid-101",
                "FS",
                "target-2",
                "",
                "",
                0,
                0,
                _version_spec_json(start_time=1786024439, status="COMPLETED"),
            ),
            (
                102,
                12,
                2,
                "vuid-102",
                "GW",
                "target-3",
                "",
                "",
                0,
                0,
                _version_spec_json(start_time=1785999588, status="COMPLETED"),
            ),
            (
                103,
                13,
                2,
                "vuid-103",
                "M365",
                "target-4",
                "",
                "",
                0,
                1,  # deleted
                _version_spec_json(start_time=1786026760, status="COMPLETED"),
            ),
            (
                104,
                14,
                2,
                "vuid-104",
                "GW",
                "target-5",
                "",
                "",
                0,
                0,
                _version_spec_json(start_time=1786027665, status="COMPLETED"),
            ),
        ],
    )
    _write_copy_target_version_meta(
        tmp_path / "db" / "copy_target_version_meta",
        [("vuid-100", "/pv/copy_meta_file/VM_vuid-100", ["target.db"], 1)],
    )
    return tmp_path


@pytest.fixture
async def repo(repo_root: Path) -> AsyncIterator[DedupRepo]:
    store = LocalFsStore(repo_root)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as opened:
        yield opened


class TestWorkloads:
    async def test_device_display_names(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wls = {w.workload_id: w for w in await workloads(repo, conns[ConnectionConfigId(1)])}
        assert wls[WorkloadId(10)].display_name == "my-vm"
        assert wls[WorkloadId(10)].subtitle == "Windows 10"
        assert wls[WorkloadId(10)].sub_type is None
        # no sub_type -> type_hint falls back to workload_type,
        # deliberately not ``subtitle`` (which is "Windows 10" here, an OS
        # name, not a type classification).
        assert wls[WorkloadId(10)].type_hint == "VM"
        assert wls[WorkloadId(11)].display_name == "10.0.0.1"
        assert wls[WorkloadId(11)].subtitle == "smb"

    async def test_sorted_by_display_name(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wls = await workloads(repo, conns[ConnectionConfigId(1)])
        # "10.0.0.1" precedes "my-vm" alphabetically, opposite of
        # workload_id order (11 registered after 10).
        assert [w.display_name for w in wls] == ["10.0.0.1", "my-vm"]

    def test_vm_falls_back_to_hypervisor_name_when_os_name_missing(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _device_display_name

        spec_inner = {"workload_name": "vm2", "config_vm": {"hypervisor_name": "ESXi"}}
        name, subtitle = _device_display_name("VM", spec_inner)
        assert name == "vm2"
        assert subtitle == "ESXi"

    def test_unnamed_device_workload_degrades_gracefully(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _device_display_name

        name, subtitle = _device_display_name("VM", {})
        assert name == "VM (unnamed)"
        assert subtitle is None

    async def test_saas_mail_display_name(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wls = {w.workload_id: w for w in await workloads(repo, conns[ConnectionConfigId(2)])}
        assert wls[WorkloadId(12)].display_name == "Alice <alice@x.com>"
        assert wls[WorkloadId(12)].sub_type == "MAIL"
        assert wls[WorkloadId(12)].subtitle == "MAIL"
        # sub_type present -> type_hint uses it directly.
        assert wls[WorkloadId(12)].type_hint == "MAIL"

    async def test_saas_site_display_name(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wls = {w.workload_id: w for w in await workloads(repo, conns[ConnectionConfigId(2)])}
        assert wls[WorkloadId(13)].display_name == "My Site"
        assert wls[WorkloadId(13)].sub_type == "SITE"

    async def test_saas_group_display_name(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wls = {w.workload_id: w for w in await workloads(repo, conns[ConnectionConfigId(2)])}
        assert wls[WorkloadId(14)].display_name == "My Group"

    def test_saas_team_drive_display_name(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        name = _saas_display_name(_TEAM_DRIVE_SPEC, "TEAM_DRIVE")
        assert name == "Test-Workload-03"

    def test_saas_team_drive_falls_back_when_name_missing(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        spec = {
            "spec": {"workload_type": "TEAM_DRIVE"},
            "status": {"entity_meta": {"spec": {"team_drive_info": {"id": "drive-1"}}}},
        }
        name = _saas_display_name(spec, "TEAM_DRIVE")
        assert name == "unnamed team drive"

    def test_saas_team_display_name(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        name = _saas_display_name(_TEAM_SPEC, "TEAMS")
        assert name == "Teams-Workload-02"

    def test_saas_team_falls_back_when_name_missing(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        spec = {
            "spec": {"workload_type": "TEAMS"},
            "status": {"entity_meta": {"spec": {"team_info": {"id": "team-1"}}}},
        }
        name = _saas_display_name(spec, "TEAMS")
        assert name == "unnamed team"

    def test_saas_display_name_handles_an_explicit_json_null_status(self) -> None:
        # CLAUDE.md's ``raw.get(key) or default`` convention exists
        # specifically for a key present with a JSON null, not just
        # absent -- ``_saas_display_name``'s own
        # ``status = spec_json.get("status") or {}`` (and the
        # entity_meta/entity_spec gets right after it) must not crash on
        # this, only on a missing "status" key.
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        spec = {"spec": {"workload_type": "SITE"}, "status": None}
        name = _saas_display_name(spec, "SITE")
        assert name == "SITE workload"

    async def test_unknown_connector_type_degrades_gracefully(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(tmp_path / "db" / "workload_config", [(20, "12345678-abcd", "WEIRD", {"spec": {}})])
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(200, 20, 1, "vuid-200", "WEIRD", "t", "", "", 0, 0, "2026-08-06 00:00:00")],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            cat = (await connections(repo))[0]
            wl = (await workloads(repo, cat))[0]
            assert wl.display_name == "WEIRD 12345678"
            assert wl.sub_type is None

    async def test_explicit_json_null_workload_spec_spec_degrades_gracefully(self, tmp_path: Path) -> None:
        # ``_workload_from_row``'s own
        # ``spec_inner = spec_json.get("spec") or {}`` -- present-but-null
        # "spec" (not just an absent key) must not crash
        # ``_device_display_name``'s own ``spec_inner.get(...)`` calls.
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(tmp_path / "db" / "workload_config", [(20, "vm-uid", "VM", {"spec": None})])
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(200, 20, 1, "vuid-200", "VM", "t", "", "", 0, 0, "2026-08-06 00:00:00")],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            cat = (await connections(repo))[0]
            wl = (await workloads(repo, cat))[0]
            assert wl.display_name == "VM (unnamed)"
            assert wl.sub_type is None

    async def test_no_workloads_for_connection_returns_empty_list(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(tmp_path / "db" / "workload_config", [])
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            cat = (await connections(repo))[0]
            assert await workloads(repo, cat) == []


class TestTenantAndDomain:
    """Direct coverage for ``Workload.tenant_id``/``domain`` — each
    reads its own real spec key (``tenant_id``, distinct from
    ``workload_spec``'s top-level ``namespace``; ``domain``, unlike
    ``tenant_id``, never just a GUID). Not folded into the shared
    ``repo_root`` fixture above, whose M365/GW spec fixtures don't
    carry either field."""

    async def test_m365_tenant_id_read_from_spec(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(
            tmp_path / "db" / "workload_config",
            [
                (
                    20,
                    "m365-uid",
                    "M365",
                    {"spec": {"workload_type": "SITE", "tenant_id": "87c467dd-ac00-45d8-babb-e2b0787e2d13"}},
                )
            ],
        )
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(200, 20, 1, "vuid-200", "M365", "t", "", "", 0, 0, _version_spec_json(start_time=1786024626))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            assert wl.tenant_id == "87c467dd-ac00-45d8-babb-e2b0787e2d13"
            assert wl.domain is None

    async def test_gw_domain_read_from_spec(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(
            tmp_path / "db" / "workload_config",
            [(21, "gw-uid", "GW", {"spec": {"workload_type": "MAIL", "domain": "gwsdemo.example.com"}})],
        )
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(201, 21, 1, "vuid-201", "GW", "t", "", "", 0, 0, _version_spec_json(start_time=1786024626))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            assert wl.domain == "gwsdemo.example.com"
            assert wl.tenant_id is None

    def test_both_none_for_a_device_workload_missing_both_keys(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import Workload
        from synology_apm_repo.sdk.identifiers import WorkloadId, WorkloadUid

        wl = Workload(
            workload_id=WorkloadId(1),
            workload_uid=WorkloadUid("vm-uid"),
            workload_type="VM",
            sub_type=None,
            display_name="my-vm",
            subtitle=None,
            spec={"spec": {"workload_type": "VM", "workload_name": "my-vm"}},
        )
        assert wl.tenant_id is None
        assert wl.domain is None

    def test_both_none_when_spec_itself_is_an_explicit_json_null(self) -> None:
        # Distinct from
        # ``test_both_none_for_a_device_workload_missing_both_keys``
        # (an absent "spec" key): ``tenant_id``/``domain``'s own
        # ``(spec.get("spec") or {}).get(...)`` must also handle "spec"
        # present with a JSON null, not crash on ``None.get(...)``.
        from synology_apm_repo.sdk.catalog.workload import Workload
        from synology_apm_repo.sdk.identifiers import WorkloadId, WorkloadUid

        wl = Workload(
            workload_id=WorkloadId(1),
            workload_uid=WorkloadUid("vm-uid"),
            workload_type="VM",
            sub_type=None,
            display_name="my-vm",
            subtitle=None,
            spec={"spec": None},
        )
        assert wl.tenant_id is None
        assert wl.domain is None
