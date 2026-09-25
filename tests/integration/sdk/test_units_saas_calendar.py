"""Regression test for ``synology_apm_repo.sdk.units.saas.calendar``
against real GWS/M365 Calendar workloads — replayed from committed
fixtures, with **no external dependency**: these always run, on CI or
anywhere else, because they go through ``ReplayStore`` instead of a real
``LocalFsStore``.

Deliberately narrow: this module only proves that a real GWS/M365
Calendar workload dispatches to ``CalendarProvider`` and lists its
calendar(s) correctly — it never lists or reads an individual real
event. An event's own summary/organizer/dates *are* its content (unlike a
Device/FS/Drive node, whose name is just a filename): reading one anyway
would make ``RecordingStore`` capture that real content into the
committed fixture regardless of what the test then asserts, since
narrowing the assertion can't undo the capture. ICS-building correctness is covered
synthetically by ``tests/unit/sdk/test_units_saas_calendar.py`` instead.

Every test below still needs ``record_target(..., allow_content=True)``:
``SaasWorkloadProvider.create()`` itself resolves its own object-name index
via a real ``dedup_file.read()`` (an internal routing table, not an
individual event) before this module's own narrower scope even begins —
see ``tests/CLAUDE.md``'s "structural oracle" phrase and
``test_units_saas_raw_object.py``'s equivalent index read for the same
pattern applied to a different SaaS provider.

The fixtures (``tests/fixtures/``, recorded against a real store — see
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...``/``make record-fixture`` workflow that
(re-)records these):

- ``units_saas_calendar_gws_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``: just the index resolution and
  each real GWS Calendar stream's own calendar list, not any event.
- ``units_saas_calendar_m365_grace_apv1.json.gz`` — same real root, a
  real M365 Exchange Calendar workload (``USER_EXCHANGE``, version_id
  91): same narrow scope.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.saas.calendar import CalendarProvider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_STREAMS = [("KxMWSUvtSZiaDTDy", 3), ("tfUJpbJdYextKnPE", 3)]

#: An internal catalog identifier -- stable and non-identifying (never
#: touched by catalog-metadata anonymization).
_M365_CALENDAR_WORKLOAD_ID = 19


async def _open_provider(repo: DedupRepo, saas_streams: SaasStreamCache, stream_uuid: str) -> SaasWorkloadProvider:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c) if w.sub_type == "CALENDAR"]
    candidates = [
        v for w in all_workloads for v in await versions(repo, w) if v.saas_stream_uuid == stream_uuid and not v.deleted
    ]
    latest = max(candidates, key=lambda v: v.version_id)
    return await CalendarProvider(repo, latest, saas_streams)


async def test_replayed_gws_calendar_streams_resolve_and_list_their_calendars(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_calendar_gws_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        for stream_uuid, _ccid in _STREAMS:
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
    store = await record_target("units_saas_calendar_m365_grace_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        workload = next(w for w in all_workloads if w.workload_id == _M365_CALENDAR_WORKLOAD_ID)
        version = next(v for v in await versions(repo, workload) if v.version_id == 91)
        provider = await CalendarProvider(repo, version, saas_streams)
        try:
            # This real M365 account carries no calendar_type-equivalent
            # ownership signal -- every calendar lands under the sole
            # "My Calendars" category.
            [my_calendars] = await provider.children(provider.root())
            calendars = await provider.children(my_calendars)
            assert len(calendars) == 3  # Calendar / Taiwan Holidays / Birthdays
            assert all(not c.is_leaf for c in calendars)
        finally:
            await provider.close()


__all__: list[str] = []
