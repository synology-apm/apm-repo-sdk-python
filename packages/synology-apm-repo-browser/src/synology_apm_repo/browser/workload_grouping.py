"""Pure grouping/display-label helpers for ``BrowseScreen``'s column 2
(workload tree) — carry no ``Tree``/widget state at all, so the actual
algorithms are directly testable without a running Textual app. See
``tests/unit/browser/test_browser_browse_screen_grouping.py`` and
``test_browser_browse_screen_labels.py``.
"""

from __future__ import annotations

import dataclasses

from synology_apm_repo.sdk.api import Workload

#: Explicit proper-naming table for every real SaaS sub_type
#: this SDK dispatches on — see ``units/dispatch.py``'s own
#: ``_SAAS_SUB_TYPE_CANDIDATES``, the source of truth for which 11
#: tokens are real; this is that same set, mapped to the vendor's own
#: product terminology instead of the internal wire token. GWS (Google
#: Workspace, 5 tokens) and M365 (Microsoft 365, 6 tokens) are disjoint
#: token sets — a GWS "MAIL" workload and an M365 mailbox never share a
#: token (M365's own is "USER_EXCHANGE"), so one flat table is
#: unambiguous without also needing ``Workload.workload_type`` ("GW" vs
#: "M365") to disambiguate. Device workload types (VM/FS/PC/PS, no
#: sub_type) are already short, correct acronyms and need no mapping.
_TYPE_LABELS: dict[str, str] = {
    # SaaS platform fallback — only reached if a workload's sub_type is
    # somehow absent (every real dispatched workload has one, per
    # units/dispatch.py's own table), so type_hint falls back to the
    # bare top-level workload_type.
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

#: The two SaaS ``Workload.workload_type`` tokens that get an extra
#: tenant/domain grouping level (below the platform header, above the
#: sub_type group) — device types (VM/FS/PC/PS) never do, real backup
#: workloads don't have a "tenant" concept. Order here is also the
#: display order both SaaS platform headers show up in under one
#: connection, matching the order the user themselves specified.
_SAAS_PLATFORM_TYPES = ("M365", "GW")

#: Every real M365/GW workload carries a ``tenant_id``/``domain`` —
#: this is a defensive degrade for the hypothetical case of a
#: future/malformed workload missing it, not a normal path.
_UNKNOWN_TENANT_LABEL = "(unknown tenant)"


def _saas_group_key(workload: Workload) -> str:
    """The tenant/domain grouping key one level below a SaaS platform
    header — M365's real tenant GUID (``workload.tenant_id``) or GW's
    real domain (``workload.domain``); which one depends on
    ``workload.workload_type``, never ``sub_type`` (both fields are
    already ``workload_type``-specific — a GW workload's own
    ``tenant_id`` is always ``None``, and vice versa, see
    ``catalog/workload.py``'s own docstrings on both properties)."""
    key = workload.tenant_id if workload.workload_type == "M365" else workload.domain
    return key or _UNKNOWN_TENANT_LABEL


@dataclasses.dataclass(frozen=True)
class _WorkloadGrouping:
    """The pure result of grouping one connection's workloads for column
    2's tree — carries no ``Tree``/widget state at all, so the actual
    grouping algorithm (``_group_workloads``) is directly testable without a
    running Textual app. ``BrowseScreen._set_workloads`` is a thin renderer
    consuming this, not where the real logic lives.

    Attributes:
        device_groups: type_hint -> workloads, device (VM/FS/PC/PS) only
            — flat, no further grouping level (device workloads have no
            "tenant").
        saas_groups: platform_type -> tenant/domain key -> sub_type ->
            workloads, only for a platform actually present in the input
            — in ``_SAAS_PLATFORM_TYPES``'s own fixed order (dict insertion
            order is a real language guarantee, not incidental here).
    """

    device_groups: dict[str, list[Workload]]
    saas_groups: dict[str, dict[str, dict[str, list[Workload]]]]


def _group_workloads(workloads: list[Workload]) -> _WorkloadGrouping:
    """Groups one connection's workloads for column 2's tree: device
    workloads (VM/FS/PC/PS) stay a flat ``type_hint -> workloads``; each
    SaaS platform present (M365/Google Workspace) gets its own
    tenant/domain grouping level (``_saas_group_key``) between the platform
    and the sub_type group — a connection's workloads can mix
    more than one M365 tenant, so sub_type alone isn't sufficient to
    group by at the SaaS level. Rendering order is fixed: every device
    group together, followed by Microsoft 365 entirely, then Google
    Workspace entirely, each keyed by its own first-seen order — never
    whatever order the underlying catalog query returned workloads in."""
    device_groups: dict[str, list[Workload]] = {}
    # Its own pass, separate from each SaaS platform's pass below, so the
    # docstring's fixed top-level ordering (device groups together, then
    # each SaaS platform entirely) falls out of iteration order for free.
    for workload in workloads:
        if workload.workload_type not in _SAAS_PLATFORM_TYPES:
            device_groups.setdefault(workload.type_hint, []).append(workload)

    saas_groups: dict[str, dict[str, dict[str, list[Workload]]]] = {}
    # One pass per platform, in _SAAS_PLATFORM_TYPES's fixed order, for the
    # same reason as the device-groups pass above: keeps platforms from
    # interleaving in the result.
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
    """Cosmetic-only display label for a ``col-workloads`` group header —
    ``Workload.type_hint`` and ``disambiguate()``'s own hint suffix stay
    untouched elsewhere (settled, CLI-shared behavior); this only
    prettifies the header shown here. Looks up ``_TYPE_LABELS``; an
    unrecognized future token falls back to the raw token verbatim rather
    than guessing a plausible name."""
    return _TYPE_LABELS.get(type_hint, type_hint)
