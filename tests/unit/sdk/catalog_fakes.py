"""The synthetic catalog the ``test_catalog_*`` files share: two
connections, one workload of each kind with a version, and the specs
tests assert against."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from support.repo_builders import (
    version_spec_json,
    write_connection_config,
    write_copy_target_version,
    write_copy_target_version_meta,
    write_repo_info,
    write_vault_link_key,
    write_workload_config,
)

VM_SPEC: dict[str, Any] = {
    "namespace": "ns-a",
    "spec": {
        "workload_type": "VM",
        "workload_name": "my-vm",
        "config_vm": {"os_name": "Windows 10", "hypervisor_name": "Cluster-02"},
    },
}
FS_SPEC: dict[str, Any] = {
    "namespace": "ns-a",
    "spec": {"workload_type": "FS", "workload_name": "10.0.0.1", "config_fs": {"os_name": "smb"}},
}
MAIL_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "MAIL"},
    "status": {"entity_meta": {"spec": {"user_info": {"name": "Alice", "email": "alice@x.com"}}}},
}
SITE_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "SITE"},
    "status": {"entity_meta": {"spec": {"site_info": {"site_name": "My Site"}}}},
}
GROUP_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "TEAM_DRIVE"},
    "status": {"entity_meta": {"spec": {"group_info": {"display_name": "My Group"}}}},
}


def write_catalog_repo(root: Path) -> None:
    """``repo_info``, two connections with their link keys, one
    ``workload_config`` row per spec above plus an unknown type, and one
    version of each known workload (``vuid-103`` deleted, ``vuid-100``
    with a meta row)."""
    write_repo_info(root / "repo_info")
    write_connection_config(root / "db" / "connection_config", [(1, "conn-a", 1), (2, "conn-b", 1)])
    write_vault_link_key(
        root / "db" / "vault_link_key",
        ["conn-a_9053e422-uuid_Test-Workload-02", "conn-b_2d90eeaf-uuid_Test-Workload-01"],
    )
    write_workload_config(
        root / "db" / "workload_config",
        [
            (10, "vm-uid", "VM", VM_SPEC),
            (11, "fs-uid", "FS", FS_SPEC),
            (12, "mail-uid", "GW", MAIL_SPEC),
            (13, "site-uid", "M365", SITE_SPEC),
            (14, "group-uid", "GW", GROUP_SPEC),
            (15, "unknown-uid", "WEIRD", {"namespace": "ns-c", "spec": {}}),
        ],
    )
    write_copy_target_version(
        root / "db" / "copy_target_version",
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
                version_spec_json(start_time=1786024626, status="COMPLETED"),
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
                version_spec_json(start_time=1786024439, status="COMPLETED"),
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
                version_spec_json(start_time=1785999588, status="COMPLETED"),
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
                version_spec_json(start_time=1786026760, status="COMPLETED"),
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
                version_spec_json(start_time=1786027665, status="COMPLETED"),
            ),
        ],
    )
    write_copy_target_version_meta(
        root / "db" / "copy_target_version_meta",
        [("vuid-100", "/pv/copy_meta_file/VM_vuid-100", ["target.db"], 1)],
    )
