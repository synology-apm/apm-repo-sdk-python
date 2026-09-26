"""Pure grouping/display-label helpers for ``BrowseScreen``'s column 2
(workload tree) — carry no ``Tree``/widget state at all, so the actual
algorithms are directly testable without a running Textual app. See
``tests/unit/browser/test_browser_browse_screen_grouping.py`` and
``test_browser_browse_screen_labels.py``.
"""

from __future__ import annotations

import dataclasses

from synology_apm_repo.sdk.api import Workload

#: Vendor product terminology for every real SaaS sub_type (see
#: ``units/dispatch.py``'s ``_SAAS_SUB_TYPE_CANDIDATES`` for the 11 real
#: tokens). GWS and M365 tokens are disjoint, so one flat table is
#: unambiguous. Device types (VM/FS/PC/PS) are already short and need no
#: mapping.
_TYPE_LABELS: dict[str, str] = {
    # SaaS platform fallback, reached only if sub_type is absent.
    "GW": "Google Workspace",
    "M365": "Microsoft 365",
    # GWS (Google Workspace)
    "MAIL": "Mail",
    "CALENDAR": "Calendars",
    "CONTACT": "Contacts",
    "DRIVE": "Drives",
    "TEAM_DRIVE": "Shared Drives",
    # M365 (Microsoft 365)
    "USER_EXCHANGE": "Exchange",
    "USER_DRIVE": "OneDrive",
    "USER_CHAT": "Chat",
    "GROUP_EXCHANGE": "Groups",
    "SITE": "SharePoint",
    "TEAMS": "Teams",
}

#: The two SaaS ``workload_type`` tokens that get an extra tenant/domain
#: grouping level below the platform header; device types never do. Order
#: here is also the SaaS platform headers' display order.
_SAAS_PLATFORM_TYPES = ("M365", "GW")

#: Defensive fallback for a malformed workload missing tenant_id/domain.
_UNKNOWN_TENANT_LABEL = "(unknown tenant)"


def _saas_group_key(workload: Workload) -> str:
    """The tenant/domain grouping key: M365's ``tenant_id`` or GW's
    ``domain``, whichever ``workload_type`` selects."""
    key = workload.tenant_id if workload.workload_type == "M365" else workload.domain
    return key or _UNKNOWN_TENANT_LABEL


@dataclasses.dataclass(frozen=True)
class _WorkloadGrouping:
    """The pure result of grouping one connection's workloads for column
    2's tree — no ``Tree``/widget state, so ``_group_workloads`` is
    directly testable.

    Attributes:
        device_groups: type_hint -> workloads, device (VM/FS/PC/PS) only.
        saas_groups: platform_type -> tenant/domain key -> sub_type ->
            workloads, for platforms present in the input.
    """

    device_groups: dict[str, list[Workload]]
    saas_groups: dict[str, dict[str, dict[str, list[Workload]]]]


def _group_workloads(workloads: list[Workload]) -> _WorkloadGrouping:
    """Groups one connection's workloads for column 2's tree: device
    workloads stay a flat ``type_hint -> workloads``; each SaaS platform
    present gets its own tenant/domain grouping level (``_saas_group_key``)
    between the platform and the sub_type group, since a connection can mix
    more than one M365 tenant. Rendering order is fixed: device groups,
    then Microsoft 365, then Google Workspace, each in first-seen order."""
    device_groups: dict[str, list[Workload]] = {}
    for workload in workloads:
        if workload.workload_type not in _SAAS_PLATFORM_TYPES:
            device_groups.setdefault(workload.type_hint, []).append(workload)

    saas_groups: dict[str, dict[str, dict[str, list[Workload]]]] = {}
    for platform_type in _SAAS_PLATFORM_TYPES:
        platform_workloads = [w for w in workloads if w.workload_type == platform_type]
        if not platform_workloads:
            continue
        tenant_groups: dict[str, dict[str, list[Workload]]] = {}
        for workload in platform_workloads:
            sub_groups = tenant_groups.setdefault(_saas_group_key(workload), {})
            sub_groups.setdefault(workload.type_hint, []).append(workload)
        saas_groups[platform_type] = tenant_groups

    return _WorkloadGrouping(device_groups=device_groups, saas_groups=saas_groups)


def _humanize_type(type_hint: str) -> str:
    """Cosmetic-only display label for a ``col-workloads`` group header.
    An unrecognized future token falls back to the raw token verbatim."""
    return _TYPE_LABELS.get(type_hint, type_hint)
