"""``Workload``: one ``db/workload_config`` row, with its own display-name/
subtitle extraction (``_device_display_name``/``_saas_display_name``).

``workloads()``'s per-connection lookup, ``_workload_ids_for_connection``,
relies on the same ``copy_target_version`` join ``catalog/connection.py``
uses in its batched form.
"""

from __future__ import annotations

import dataclasses
import enum
import json
from typing import Any

from ..dedup.repository import DedupRepo
from ..identifiers import ConnectionConfigId, WorkloadId, WorkloadUid
from ..storage.table import Table, as_int, as_str, sql_placeholders
from .connection import Connection
from .workload_config import _WORKLOAD_COLUMNS


class TargetType(enum.StrEnum):
    """The six real ``Workload.workload_type``/``Version.target_type``
    values — device workloads (VM/PC/PS/FS) vs. SaaS connector kinds
    (GW/M365). A ``str`` subclass, so existing sites comparing against a
    plain ``"VM"``/``"GW"`` literal keep working unchanged."""

    VM = "VM"
    PC = "PC"
    PS = "PS"
    FS = "FS"
    GW = "GW"
    M365 = "M365"


_DEVICE_TYPES = frozenset({TargetType.VM, TargetType.PC, TargetType.PS, TargetType.FS})
_SAAS_TYPES = frozenset({TargetType.GW, TargetType.M365})


@dataclasses.dataclass(frozen=True)
class Workload:
    """One ``db/workload_config`` row, with display fields extracted
    per this module's docstring."""

    workload_id: WorkloadId
    workload_uid: WorkloadUid
    workload_type: str  # one of TargetType's values, straight off the on-disk DB column
    sub_type: str | None  # SaaS only: "MAIL" / "DRIVE" / "CONTACT" / "SITE" / ...
    display_name: str
    subtitle: str | None
    spec: dict[str, object]

    @property
    def type_hint(self) -> str:
        """A short, human-meaningful classification for disambiguation —
        ``sub_type`` where present, else the top-level ``workload_type``.
        Not ``subtitle``, which carries workload-specific detail (an OS
        name, a host IP), not a type classification. Feeds
        ``disambiguate``'s ``hints`` parameter."""
        return self.sub_type or self.workload_type

    def _spec_str(self, key: str) -> str | None:
        spec: dict[str, Any] = self.spec
        value = (spec.get("spec") or {}).get(key)
        return str(value) if value else None

    @property
    def tenant_id(self) -> str | None:
        """The real M365 tenant GUID — ``workload_spec.spec.tenant_id``,
        not the same as ``workload_spec``'s own ``namespace`` field (a
        backup-server-internal bookkeeping UUID). ``None`` for non-M365
        workloads; GW's tenant-equivalent is ``domain`` instead."""
        return self._spec_str("tenant_id")

    @property
    def domain(self) -> str | None:
        """The real GWS domain — ``workload_spec.spec.domain``, a plain
        human-readable string, unlike M365's GUID-only ``tenant_id``.
        ``None`` for non-GW workloads."""
        return self._spec_str("domain")


async def _workload_ids_for_connection(repo: DedupRepo, connection_config_id: ConnectionConfigId) -> list[WorkloadId]:
    """Workloads for one connection — the single-connection form of the
    ``copy_target_version`` join ``catalog/connection.py`` uses."""
    conn = await repo.db("copy_target_version")
    cursor = await conn.execute(
        "SELECT DISTINCT workload_id FROM copy_target_version WHERE connection_config_id = ?",
        (connection_config_id,),
    )
    rows = await cursor.fetchall()
    return [r[0] for r in rows]


async def workloads(repo: DedupRepo, connection: Connection) -> list[Workload]:
    """Sorted by ``display_name`` (case-insensitive), same reasoning as
    ``connections()``."""
    workload_ids = await _workload_ids_for_connection(repo, connection.connection_config_id)
    if not workload_ids:
        return []
    placeholders = sql_placeholders(len(workload_ids))
    table = await Table.create(await repo.db("workload_config"), "workload_config", _WORKLOAD_COLUMNS)
    result = [_workload_from_row(row) async for row in table.select(f"workload_id IN ({placeholders})", workload_ids)]
    result.sort(key=lambda workload: workload.display_name.casefold())
    return result


async def workload_by_id(repo: DedupRepo, workload_id: WorkloadId) -> Workload | None:
    """A single ``Workload`` by its own ``workload_config`` primary key —
    a direct ``WHERE workload_id = ?`` lookup rather than scanning every
    connection's workloads, since no ``Workload`` field depends on which
    connection it belongs to. ``None`` if ``workload_id`` doesn't resolve."""
    table = await Table.create(await repo.db("workload_config"), "workload_config", _WORKLOAD_COLUMNS)
    row = await table.select_one("workload_id = ?", (workload_id,))
    return _workload_from_row(row) if row is not None else None


def _workload_from_row(row: dict[str, object]) -> Workload:
    workload_type = as_str(row["workload_type"])
    spec_json: dict[str, Any] = json.loads(as_str(row["workload_spec"]))
    spec_inner: dict[str, Any] = spec_json.get("spec") or {}

    sub_type: str | None
    if workload_type in _DEVICE_TYPES:
        display_name, subtitle = _device_display_name(workload_type, spec_inner)
        sub_type = None
    elif workload_type in _SAAS_TYPES:
        sub_type = spec_inner.get("workload_type")
        display_name = _saas_display_name(spec_json, sub_type)
        subtitle = sub_type
    else:  # unknown connector kind — degrade rather than fail
        display_name = f"{workload_type} {as_str(row['workload_uid'])[:8]}"
        subtitle = None
        sub_type = None

    return Workload(
        workload_id=WorkloadId(as_int(row["workload_id"])),
        workload_uid=WorkloadUid(as_str(row["workload_uid"])),
        workload_type=workload_type,
        sub_type=sub_type,
        display_name=display_name,
        subtitle=subtitle,
        spec=spec_json,
    )


def _device_display_name(workload_type: str, spec_inner: dict[str, Any]) -> tuple[str, str | None]:
    # display_name from workload_name; subtitle from an OS/hypervisor name
    # where this device type actually carries one (VM: config_vm.os_name/
    # hypervisor_name; FS: config_fs.os_name; PC/PS have no subtitle).
    name = spec_inner.get("workload_name")
    if not name:
        return f"{workload_type} (unnamed)", None
    subtitle: str | None = None
    if workload_type == "VM":
        config_vm = spec_inner.get("config_vm") or {}
        subtitle = config_vm.get("os_name") or config_vm.get("hypervisor_name")
    elif workload_type == "FS":
        config_fs = spec_inner.get("config_fs") or {}
        subtitle = config_fs.get("os_name")
    return str(name), subtitle


#: ``(entity_spec key, name fields to try in order, label for the
#: "unnamed ..." fallback)`` — every ``_saas_display_name`` case past
#: ``user_info`` (the one genuinely special case: it combines *two*
#: fields into one ``"name <email>"`` display, not just a first-truthy
#: pick) follows this same shape.
_SAAS_INFO_FALLBACKS = [
    ("site_info", ("site_name",), "site"),
    ("group_info", ("display_name", "mail"), "group"),
    ("team_drive_info", ("name",), "team drive"),
    ("team_info", ("name",), "team"),
]


def _saas_display_name(spec_json: dict[str, Any], sub_type: str | None) -> str:
    # Exactly one of user_info/site_info/group_info/team_drive_info/
    # team_info is ever populated for a given workload; checked in this
    # order. team_drive_info/team_info are each specific to their own
    # sub_type (TEAM_DRIVE/TEAMS respectively). ``sub_type`` is the
    # caller's own already-computed ``Workload.sub_type`` (same
    # ``spec.workload_type`` field) — passed in rather than recomputed
    # here, so the two never have a chance to diverge.
    status = spec_json.get("status") or {}
    entity_meta = status.get("entity_meta") or {}
    entity_spec = entity_meta.get("spec") or {}

    user_info = entity_spec.get("user_info")
    if user_info:
        name, email = user_info.get("name"), user_info.get("email")
        if name and email:
            return f"{name} <{email}>"
        return str(name or email or "unnamed user")

    for key, name_fields, label in _SAAS_INFO_FALLBACKS:
        info = entity_spec.get(key)
        if info:
            for field in name_fields:
                value = info.get(field)
                if value:
                    return str(value)
            return f"unnamed {label}"

    return f"{sub_type or 'SaaS'} workload"
