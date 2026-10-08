"""``Workload``: one ``db/workload_config`` row with its display name and
subtitle extracted, and the queries that list or look one up."""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Mapping
from typing import Any

from .._util.jsonparse import json_object
from ..dedup.repository import DedupRepo
from ..identifiers import WorkloadId, WorkloadUid
from ..storage.table import Table, as_int, as_str, sql_placeholders
from .connection import Connection, workload_ids_by_connection
from .workload_config import WORKLOAD_COLUMNS, workload_spec_from_row


class TargetType(enum.StrEnum):
    """The ``Workload.workload_type``/``Version.target_type`` values: device
    workloads (VM/PC/PS/FS) and SaaS connector kinds (GWS/M365). A ``str``
    enum, so members compare equal to the raw column values; each member is
    named after its value except ``GWS``, stored on disk as ``"GW"``."""

    VM = "VM"
    PC = "PC"
    PS = "PS"
    FS = "FS"
    GWS = "GW"
    M365 = "M365"


DEVICE_TARGET_TYPES = frozenset({TargetType.VM, TargetType.PC, TargetType.PS, TargetType.FS})
SAAS_TARGET_TYPES = frozenset({TargetType.GWS, TargetType.M365})


class SaasSubType(enum.StrEnum):
    """The known ``Workload.sub_type`` values: a SaaS workload's
    ``spec.workload_type``. ``Workload.sub_type`` holds the raw string,
    which a member compares equal to, and may be a value not listed here."""

    MAIL = "MAIL"
    CONTACT = "CONTACT"
    CALENDAR = "CALENDAR"
    DRIVE = "DRIVE"
    TEAM_DRIVE = "TEAM_DRIVE"
    USER_EXCHANGE = "USER_EXCHANGE"
    GROUP_EXCHANGE = "GROUP_EXCHANGE"
    USER_DRIVE = "USER_DRIVE"
    USER_CHAT = "USER_CHAT"
    SITE = "SITE"
    TEAMS = "TEAMS"


@dataclasses.dataclass(frozen=True, slots=True)
class Workload:
    """One ``db/workload_config`` row, with display fields extracted.

    Attributes:
        workload_type: The raw column value, normally a ``TargetType``
            value.
        sub_type: For SaaS, ``spec.workload_type`` (``"MAIL"``,
            ``"DRIVE"``, ``"SITE"``, ...); ``None`` otherwise, or when it
            is not a non-empty string.
        display_name: The device's or SaaS entity's name, or a
            placeholder naming the workload's kind when it has none.
        subtitle: A device's OS or hypervisor name, a SaaS workload's
            ``sub_type``, or ``None``.
        spec: The parsed ``workload_spec`` JSON.
    """

    workload_id: WorkloadId
    workload_uid: WorkloadUid
    workload_type: str
    sub_type: str | None
    display_name: str
    subtitle: str | None
    spec: Mapping[str, object]

    @property
    def is_saas(self) -> bool:
        """Whether this is a SaaS (GWS/M365) workload."""
        return self.workload_type in SAAS_TARGET_TYPES

    @property
    def type_hint(self) -> str:
        """The classification ``disambiguate``'s ``hints`` uses to tell
        same-named workloads apart; ``sub_type``, else ``workload_type``."""
        return self.sub_type or self.workload_type

    def _spec_str(self, key: str) -> str | None:
        spec: Mapping[str, Any] = self.spec
        value = json_object(spec.get("spec")).get(key)
        return str(value) if value else None

    @property
    def tenant_id(self) -> str | None:
        """The M365 tenant GUID, ``workload_spec.spec.tenant_id`` (not the
        internal ``namespace`` UUID); ``None`` for other workloads."""
        return self._spec_str("tenant_id")

    @property
    def domain(self) -> str | None:
        """The Google Workspace domain, ``workload_spec.spec.domain``;
        ``None`` for other workloads."""
        return self._spec_str("domain")

    @property
    def user_info(self) -> dict[str, object] | None:
        """The backed-up SaaS account's profile (``email``, ``name``, ...),
        ``workload_spec.status.entity_meta.spec.user_info``; ``None`` when
        absent."""
        user_info = _entity_spec(self.spec).get("user_info")
        return user_info if isinstance(user_info, dict) else None


async def workloads(repo: DedupRepo, connection: Connection) -> list[Workload]:
    """``connection``'s workloads, sorted by ``display_name``
    (case-insensitive).

    Raises:
        DataCorruptError: A row's ``workload_spec`` is not a JSON object.
    """
    connection_config_id = connection.connection_config_id
    workload_ids = (await workload_ids_by_connection(repo, [connection_config_id])).get(connection_config_id, [])
    if not workload_ids:
        return []
    placeholders = sql_placeholders(len(workload_ids))
    table = await Table.create(await repo.db("workload_config"), "workload_config", WORKLOAD_COLUMNS)
    result = [_workload_from_row(row) async for row in table.select(f"workload_id IN ({placeholders})", workload_ids)]
    result.sort(key=lambda workload: workload.display_name.casefold())
    return result


async def workload_by_id(repo: DedupRepo, workload_id: WorkloadId) -> Workload | None:
    """The ``Workload`` with primary key ``workload_id``, or ``None``.

    Raises:
        DataCorruptError: The row's ``workload_spec`` is not a JSON object.
    """
    table = await Table.create(await repo.db("workload_config"), "workload_config", WORKLOAD_COLUMNS)
    row = await table.select_one("workload_id = ?", (workload_id,))
    return _workload_from_row(row) if row is not None else None


def _workload_from_row(row: dict[str, object]) -> Workload:
    workload_type = as_str(row["workload_type"])
    spec_json = workload_spec_from_row(row)
    spec_inner = json_object(spec_json.get("spec"))

    sub_type: str | None
    if workload_type in DEVICE_TARGET_TYPES:
        display_name, subtitle = _device_display_name(workload_type, spec_inner)
        sub_type = None
    elif workload_type in SAAS_TARGET_TYPES:
        sub_type = _nonempty_str(spec_inner.get("workload_type"))
        display_name = _saas_display_name(spec_json, sub_type)
        subtitle = sub_type
    else:  # unknown connector kind
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
    # Subtitle: VM's config_vm.os_name/hypervisor_name, FS's
    # config_fs.os_name; PC/PS have none.
    name = _nonempty_str(spec_inner.get("workload_name"))
    if name is None:
        return f"{workload_type} (unnamed)", None
    subtitle: str | None = None
    if workload_type == TargetType.VM:
        config_vm = json_object(spec_inner.get("config_vm"))
        subtitle = _nonempty_str(config_vm.get("os_name")) or _nonempty_str(config_vm.get("hypervisor_name"))
    elif workload_type == TargetType.FS:
        subtitle = _nonempty_str(json_object(spec_inner.get("config_fs")).get("os_name"))
    return name, subtitle


def _nonempty_str(value: object) -> str | None:
    """``value`` if it is a non-empty JSON string, else ``None``."""
    return value if isinstance(value, str) and value else None


#: ``(entity_spec key, name fields to try in order, "unnamed ..." label)``
#: for every ``_saas_display_name`` case after ``user_info``.
_SAAS_INFO_FALLBACKS = [
    ("site_info", ("site_name",), "site"),
    ("group_info", ("display_name", "mail"), "group"),
    ("team_drive_info", ("name",), "team drive"),
    ("team_info", ("name",), "team"),
]


def _entity_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    """``spec.status.entity_meta.spec``; a level that is absent or not a JSON
    object counts as empty."""
    return json_object(json_object(json_object(spec.get("status")).get("entity_meta")).get("spec"))


def _saas_display_name(spec_json: dict[str, Any], sub_type: str | None) -> str:
    # One *_info entity is populated per workload; user_info is checked
    # first, then _SAAS_INFO_FALLBACKS in order. An entity that isn't a JSON
    # object, or a name field that isn't a string, counts as absent.
    entity_spec = _entity_spec(spec_json)
    user_info = entity_spec.get("user_info")
    if isinstance(user_info, dict) and user_info:
        name, email = _nonempty_str(user_info.get("name")), _nonempty_str(user_info.get("email"))
        if name and email:
            return f"{name} <{email}>"
        return name or email or "unnamed user"

    for key, name_fields, label in _SAAS_INFO_FALLBACKS:
        info = entity_spec.get(key)
        if isinstance(info, dict) and info:
            for field in name_fields:
                value = _nonempty_str(info.get(field))
                if value:
                    return value
            return f"unnamed {label}"

    return f"{sub_type or 'SaaS'} workload"
