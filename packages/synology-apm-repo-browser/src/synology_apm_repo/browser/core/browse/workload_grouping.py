"""Grouping and display labels for ``BrowseScreen``'s column 2 (workload
tree)."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.sdk import SaasSubType, TargetType, Workload

#: Product terminology for every SaaS sub_type; GWS and M365 tokens are
#: disjoint, so one flat table works. Device types need no mapping.
_TYPE_LABELS: dict[str, str] = {
    # SaaS platform headers, and a SaaS workload's type_hint when sub_type is absent.
    TargetType.GWS: "Google Workspace",
    TargetType.M365: "Microsoft 365",
    # GWS (Google Workspace)
    SaasSubType.MAIL: "Mail",
    SaasSubType.CALENDAR: "Calendars",
    SaasSubType.CONTACT: "Contacts",
    SaasSubType.DRIVE: "Drives",
    SaasSubType.TEAM_DRIVE: "Shared Drives",
    # M365 (Microsoft 365)
    SaasSubType.USER_EXCHANGE: "Exchange",
    SaasSubType.USER_DRIVE: "OneDrive",
    SaasSubType.USER_CHAT: "Chat",
    SaasSubType.GROUP_EXCHANGE: "Groups",
    SaasSubType.SITE: "SharePoint",
    SaasSubType.TEAMS: "Teams",
}

#: The two SaaS ``workload_type`` tokens that get an extra tenant/domain
#: grouping level below the platform header; device types never do. Order
#: here is also the SaaS platform headers' display order.
SAAS_PLATFORM_TYPES = (TargetType.M365, TargetType.GWS)

#: Defensive fallback for a malformed workload missing tenant_id/domain.
_UNKNOWN_TENANT_LABEL = "(unknown tenant)"


def saas_group_key(workload: Workload) -> str:
    """The tenant/domain grouping key: M365's ``tenant_id`` or GWS's
    ``domain``, whichever ``workload_type`` selects."""
    key = workload.tenant_id if workload.workload_type == TargetType.M365 else workload.domain
    return key or _UNKNOWN_TENANT_LABEL


@dataclasses.dataclass(frozen=True, slots=True)
class _WorkloadGrouping:
    """The result of ``group_workloads``.

    Attributes:
        device_groups: type_hint -> workloads, device (VM/FS/PC/PS) only.
        saas_groups: platform_type -> tenant/domain key -> sub_type ->
            workloads, for platforms present in the input.
    """

    device_groups: dict[str, list[Workload]]
    saas_groups: dict[str, dict[str, dict[str, list[Workload]]]]


def group_workloads(workloads: list[Workload]) -> _WorkloadGrouping:
    """Groups one connection's workloads for column 2's tree: device
    workloads stay a flat ``type_hint -> workloads``; each SaaS platform
    present gets its own tenant/domain grouping level (``saas_group_key``)
    between the platform and the sub_type group, since a connection can mix
    more than one M365 tenant. Rendering order is fixed: device groups,
    then Microsoft 365, then Google Workspace, each in first-seen order."""
    device_groups: dict[str, list[Workload]] = {}
    for workload in workloads:
        if not workload.is_saas:
            device_groups.setdefault(workload.type_hint, []).append(workload)

    saas_groups: dict[str, dict[str, dict[str, list[Workload]]]] = {}
    for platform_type in SAAS_PLATFORM_TYPES:
        platform_workloads = [w for w in workloads if w.workload_type == platform_type]
        if not platform_workloads:
            continue
        tenant_groups: dict[str, dict[str, list[Workload]]] = {}
        for workload in platform_workloads:
            sub_groups = tenant_groups.setdefault(saas_group_key(workload), {})
            sub_groups.setdefault(workload.type_hint, []).append(workload)
        saas_groups[platform_type] = tenant_groups

    return _WorkloadGrouping(device_groups=device_groups, saas_groups=saas_groups)


def humanize_type(type_hint: str) -> str:
    """The display label for a workload-tree group header; an unknown token
    is shown as-is."""
    return _TYPE_LABELS.get(type_hint, type_hint)
