"""Unit tests for ``synology_apm_repo.sdk.catalog.workload`` against
synthetic repository roots."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from support.model_factories import make_workload
from support.repo_builders import (
    open_db,
    version_spec_json,
    write_connection_config,
    write_copy_target_version,
    write_repo_info,
    write_vault_link_key,
    write_workload_config,
)
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.workload import Workload, workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError
from synology_apm_repo.sdk.identifiers import ConnectionConfigId, WorkloadId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.catalog_fakes import write_catalog_repo

_TEAM_DRIVE_SPEC: dict[str, Any] = {
    # Real shape: user_info/group_info are null; only team_drive_info is set.
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
    # Real shape: user_info/group_info are null; only team_info is set.
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
    write_catalog_repo(tmp_path)
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
        # No sub_type: type_hint is workload_type, not the OS-name subtitle.
        assert wls[WorkloadId(10)].type_hint == "VM"
        assert wls[WorkloadId(11)].display_name == "10.0.0.1"
        assert wls[WorkloadId(11)].subtitle == "smb"

    async def test_sorted_by_display_name(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wls = await workloads(repo, conns[ConnectionConfigId(1)])
        # Opposite of workload_id order.
        assert [w.display_name for w in wls] == ["10.0.0.1", "my-vm"]

    def test_vm_falls_back_to_hypervisor_name_when_os_name_missing(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _device_display_name

        spec_inner = {"workload_name": "vm2", "config_vm": {"hypervisor_name": "ESXi"}}
        name, subtitle = _device_display_name("VM", spec_inner)
        assert name == "vm2"
        assert subtitle == "ESXi"

    @pytest.mark.parametrize(
        "spec_inner", [{}, {"workload_name": {"en": "vm"}}, {"workload_name": 7}], ids=["absent", "object", "number"]
    )
    def test_unnamed_device_workload_degrades_gracefully(self, spec_inner: dict[str, object]) -> None:
        from synology_apm_repo.sdk.catalog.workload import _device_display_name

        name, subtitle = _device_display_name("VM", spec_inner)
        assert name == "VM (unnamed)"
        assert subtitle is None

    async def test_saas_mail_display_name(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wls = {w.workload_id: w for w in await workloads(repo, conns[ConnectionConfigId(2)])}
        assert wls[WorkloadId(12)].display_name == "Alice <alice@x.com>"
        assert wls[WorkloadId(12)].sub_type == "MAIL"
        assert wls[WorkloadId(12)].subtitle == "MAIL"
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

    @pytest.mark.parametrize(
        ("workload_type", "info_key", "info_id", "expected"),
        [
            pytest.param("TEAM_DRIVE", "team_drive_info", "drive-1", "unnamed team drive", id="team_drive"),
            pytest.param("TEAMS", "team_info", "team-1", "unnamed team", id="team"),
        ],
    )
    def test_saas_falls_back_when_name_missing(
        self, workload_type: str, info_key: str, info_id: str, expected: str
    ) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        spec = {
            "spec": {"workload_type": workload_type},
            "status": {"entity_meta": {"spec": {info_key: {"id": info_id}}}},
        }
        name = _saas_display_name(spec, workload_type)
        assert name == expected

    def test_saas_team_display_name(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        name = _saas_display_name(_TEAM_SPEC, "TEAMS")
        assert name == "Teams-Workload-02"

    @pytest.mark.parametrize(
        ("info_key", "value", "workload_type"),
        [
            pytest.param("user_info", ["alice@example.com"], "MAIL", id="user_info_list"),
            pytest.param("user_info", "Alice", "MAIL", id="user_info_string"),
            pytest.param("site_info", "My Site", "SITE", id="site_info_string"),
        ],
    )
    def test_saas_display_name_treats_an_entity_that_is_not_an_object_as_absent(
        self, info_key: str, value: object, workload_type: str
    ) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        spec = {"spec": {"workload_type": workload_type}, "status": {"entity_meta": {"spec": {info_key: value}}}}
        assert _saas_display_name(spec, workload_type) == f"{workload_type} workload"

    @pytest.mark.parametrize(
        "status",
        [
            pytest.param("x", id="status_string"),
            pytest.param({"entity_meta": [1]}, id="entity_meta_list"),
            pytest.param({"entity_meta": {"spec": "x"}}, id="entity_spec_string"),
        ],
    )
    def test_a_level_above_the_entity_that_is_not_an_object_counts_as_absent(self, status: object) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        spec = {"spec": {"workload_type": "MAIL"}, "status": status}
        assert _saas_display_name(spec, "MAIL") == "MAIL workload"
        assert make_workload(spec=spec).user_info is None

    @pytest.mark.parametrize(
        ("entity", "expected"),
        [
            pytest.param(
                {"user_info": {"name": {"x": 1}, "email": "alice@example.com"}},
                "alice@example.com",
                id="user_name_object",
            ),
            pytest.param({"user_info": {"name": 7, "email": None}}, "unnamed user", id="user_name_number"),
            pytest.param({"site_info": {"site_name": ["Docs"]}}, "unnamed site", id="site_name_list"),
            pytest.param(
                {"group_info": {"display_name": 1, "mail": "team@example.com"}},
                "team@example.com",
                id="group_falls_through",
            ),
        ],
    )
    def test_a_name_field_that_is_not_a_string_counts_as_absent(self, entity: dict[str, object], expected: str) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        spec = {"spec": {"workload_type": "MAIL"}, "status": {"entity_meta": {"spec": entity}}}
        assert _saas_display_name(spec, "MAIL") == expected

    def test_saas_display_name_handles_an_explicit_json_null_status(self) -> None:
        from synology_apm_repo.sdk.catalog.workload import _saas_display_name

        spec = {"spec": {"workload_type": "SITE"}, "status": None}
        name = _saas_display_name(spec, "SITE")
        assert name == "SITE workload"

    async def test_unknown_connector_type_degrades_gracefully(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(20, "12345678-abcd", "WEIRD", {"spec": {}})])
        write_copy_target_version(
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
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(20, "vm-uid", "VM", {"spec": None})])
        write_copy_target_version(
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

    @pytest.mark.parametrize("raw_spec", ["{not json", "[1, 2]", "null"])
    async def test_a_corrupt_workload_spec_raises_data_corrupt_error(self, tmp_path: Path, raw_spec: str) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(20, "vm-uid", "VM", {})])
        conn = open_db(tmp_path / "db" / "workload_config")
        conn.execute("UPDATE workload_config SET workload_spec = ?", (raw_spec,))
        conn.commit()
        conn.close()
        write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(200, 20, 1, "vuid-200", "VM", "t", "", "", 0, 0, "2026-08-06 00:00:00")],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            with pytest.raises(DataCorruptError, match="workload_spec") as exc_info:
                cat = (await connections(repo))[0]
                await workloads(repo, cat)
            assert exc_info.value.ref == "vm-uid"

    async def test_no_workloads_for_connection_returns_empty_list(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [])
        write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            cat = (await connections(repo))[0]
            assert await workloads(repo, cat) == []


def _workload_from_spec(workload_type: str, spec: object) -> Workload:
    from synology_apm_repo.sdk.catalog.workload import _workload_from_row

    return _workload_from_row(
        {"workload_id": 20, "workload_uid": "uid-20", "workload_type": workload_type, "workload_spec": json.dumps(spec)}
    )


class TestWrongJsonTypes:
    """A ``workload_spec`` level, or a display field, of the wrong JSON type
    counts as absent."""

    @pytest.mark.parametrize("inner", [pytest.param([1], id="array"), pytest.param("x", id="string")])
    def test_a_spec_that_is_not_an_object_counts_as_empty(self, inner: object) -> None:
        workload = _workload_from_spec("M365", {"spec": inner})
        assert (workload.display_name, workload.sub_type, workload.tenant_id) == ("SaaS workload", None, None)

    @pytest.mark.parametrize("sub_type", [pytest.param(7, id="number"), pytest.param({"a": 1}, id="object")])
    def test_a_sub_type_that_is_not_a_string_is_none(self, sub_type: object) -> None:
        workload = _workload_from_spec("GW", {"spec": {"workload_type": sub_type}})
        assert (workload.sub_type, workload.subtitle, workload.display_name) == (None, None, "SaaS workload")

    @pytest.mark.parametrize(
        ("workload_type", "inner"),
        [
            pytest.param("VM", {"config_vm": ["Linux"]}, id="config_vm_an_array"),
            pytest.param("VM", {"config_vm": {"os_name": 7, "hypervisor_name": ["ESXi"]}}, id="vm_names_not_strings"),
            pytest.param("FS", {"config_fs": "Linux"}, id="config_fs_a_string"),
            pytest.param("FS", {"config_fs": {"os_name": {"name": "Linux"}}}, id="fs_os_name_an_object"),
        ],
    )
    def test_a_device_subtitle_source_of_the_wrong_type_gives_no_subtitle(
        self, workload_type: str, inner: dict[str, object]
    ) -> None:
        workload = _workload_from_spec(workload_type, {"spec": {"workload_name": "host-a", **inner}})
        assert (workload.display_name, workload.subtitle) == ("host-a", None)


class TestTenantAndDomain:
    """``Workload.tenant_id``/``domain`` read ``workload_spec.spec``'s
    ``tenant_id``/``domain`` (not the top-level ``namespace``), and
    ``user_info`` reads ``status.entity_meta.spec``. The shared ``repo_root``
    fixture's specs carry no tenant_id/domain."""

    async def test_m365_tenant_id_read_from_spec(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(
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
        write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(200, 20, 1, "vuid-200", "M365", "t", "", "", 0, 0, version_spec_json(start_time=1786024626))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            assert wl.tenant_id == "87c467dd-ac00-45d8-babb-e2b0787e2d13"
            assert wl.domain is None

    async def test_user_info_read_from_status_entity_meta(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        user_info = {"email": "alice@example.com", "name": "Alice"}
        write_workload_config(
            tmp_path / "db" / "workload_config",
            [
                (
                    20,
                    "m365-uid",
                    "M365",
                    {"spec": {"workload_type": "MAIL"}, "status": {"entity_meta": {"spec": {"user_info": user_info}}}},
                ),
                (21, "vm-uid", "VM", {"status": None}),
            ],
        )
        write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [
                (200, 20, 1, "vuid-200", "M365", "t", "", "", 0, 0, version_spec_json(start_time=1786024626)),
                (201, 21, 1, "vuid-201", "VM", "t", "", "", 0, 0, version_spec_json(start_time=1786024626)),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            by_id = {wl.workload_id: wl for wl in await workloads(repo, (await connections(repo))[0])}
            assert by_id[WorkloadId(20)].user_info == user_info
            assert by_id[WorkloadId(21)].user_info is None

    async def test_gws_domain_read_from_spec(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(
            tmp_path / "db" / "workload_config",
            [(21, "gws-uid", "GW", {"spec": {"workload_type": "MAIL", "domain": "gwsdemo.example.com"}})],
        )
        write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(201, 21, 1, "vuid-201", "GW", "t", "", "", 0, 0, version_spec_json(start_time=1786024626))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            assert wl.domain == "gwsdemo.example.com"
            assert wl.tenant_id is None

    def test_both_none_for_a_device_workload_missing_both_keys(self) -> None:
        wl = make_workload(
            workload_uid="vm-uid",
            display_name="my-vm",
            spec={"spec": {"workload_type": "VM", "workload_name": "my-vm"}},
        )
        assert wl.tenant_id is None
        assert wl.domain is None

    def test_both_none_when_spec_itself_is_an_explicit_json_null(self) -> None:
        wl = make_workload(workload_uid="vm-uid", display_name="my-vm", spec={"spec": None})
        assert wl.tenant_id is None
        assert wl.domain is None
