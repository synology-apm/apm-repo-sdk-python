"""Regression tests for ``synology_apm_repo.sdk.units.saas.calendar``
against real GWS/M365 Calendar workloads, replayed from committed fixtures.

Each test proves only that a real Calendar workload dispatches to
``open_calendar_provider`` and lists its calendars; none lists or reads an event,
whose fields are content and would be recorded into the fixture. ICS
building is covered by ``tests/unit/sdk/test_units_saas_calendar.py``.
Every test still passes ``allow_content=True``: provider creation reads the
object-name index (an internal routing table) through
``dedup_file.read()``.

Fixtures, recorded against ``vault-plain/@ActiveProtectVault``, each by
its one test:

- ``units_saas_calendar_gws_vault_plain.json.gz`` — index resolution and the
  calendar list of two GWS Calendar streams (``_GWS_CALENDAR_STREAMS``).
- ``units_saas_calendar_m365_grace_vault_plain.json.gz`` — an M365 Exchange
  Calendar workload (``USER_EXCHANGE``, version_id 91): same scope.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout
from synology_apm_repo.sdk.units.saas.calendar import open_calendar_provider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_GWS_CALENDAR_STREAMS = ("KxMWSUvtSZiaDTDy", "tfUJpbJdYextKnPE")

_M365_CALENDAR_WORKLOAD_ID = 19


async def _open_provider(repo: DedupRepo, saas_streams: SaasStreamCache, stream_uuid: str) -> SaasWorkloadProvider[Any]:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c) if w.sub_type == "CALENDAR"]
    candidates = [
        v for w in all_workloads for v in await versions(repo, w) if v.saas_stream_uuid == stream_uuid and not v.deleted
    ]
    latest = max(candidates, key=lambda v: v.version_id)
    return await open_calendar_provider(repo, latest, saas_streams)


async def test_replayed_gws_calendar_streams_resolve_and_list_their_calendars(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_calendar_gws_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        for stream_uuid in _GWS_CALENDAR_STREAMS:
            provider = await _open_provider(repo, saas_streams, stream_uuid)
            try:
                calendars = await provider.children(provider.root())
                assert calendars, (stream_uuid, "at least one real calendar expected")
                assert all(not c.is_leaf for c in calendars)
            finally:
                await provider.close()


async def test_replayed_m365_calendar_workload_resolves_and_lists_its_calendars(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_calendar_m365_grace_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        workload = next(w for w in all_workloads if w.workload_id == _M365_CALENDAR_WORKLOAD_ID)
        version = next(v for v in await versions(repo, workload) if v.version_id == 91)
        provider = await open_calendar_provider(repo, version, saas_streams)
        try:
            # With no ownership signal on this account, every calendar lands
            # under the one "My Calendars" category.
            [my_calendars] = await provider.children(provider.root())
            calendars = await provider.children(my_calendars)
            assert len(calendars) == 3
            assert all(not c.is_leaf for c in calendars)
        finally:
            await provider.close()
