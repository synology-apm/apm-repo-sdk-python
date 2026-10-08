"""Small helpers several SaaS workloads share that are not part of the
``SaasWorkloadProvider`` skeleton: group-name and membership lookups over
prefetched maps, and the owning account's profile. Teams, which is not
built on that skeleton, uses ``owning_account_user_info`` too."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping

from ...catalog.version import Version
from ...catalog.workload import workload_by_id
from ...dedup.repository import DedupRepo
from ...errors import DataCorruptError, NotFoundError


def group_display_name_resolver(names: dict[str, str] | None) -> Callable[[str], str]:
    """A ``group_display_name`` callable for ``SyntheticGroupedTree``:
    the group's name from ``names``, else the raw group key."""

    def _group_display_name(group_id: str) -> str:
        return names[group_id] if names is not None and group_id in names else group_id

    return _group_display_name


def membership_detail(groups: Mapping[str, list[str]], row_id: object, detail_name: str) -> dict[str, object]:
    """``{detail_name: <names>}`` for ``row_id`` in a prefetched ``{row id:
    [group/label name, ...]}`` map (GWS mail labels, GWS contact groups), or
    ``{}`` when it has none."""
    found = groups.get(str(row_id))
    return {detail_name: found} if found else {}


async def owning_account_user_info(repo: DedupRepo, version: Version) -> dict[str, object] | None:
    """The backed-up account's profile (``email``, ``name``, ...): the
    owning workload's ``Workload.user_info``, or ``None`` when the workload
    row is missing or unparseable; a store failure still raises."""
    try:
        workload = await workload_by_id(repo, version.workload_id)
    except (NotFoundError, ValueError, DataCorruptError, sqlite3.DatabaseError):
        return None
    return workload.user_info if workload is not None else None
