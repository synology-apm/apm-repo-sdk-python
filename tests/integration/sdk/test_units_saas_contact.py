"""Regression tests for ``synology_apm_repo.sdk.units.saas.contact``
against real GWS/M365 Contact workloads, replayed from committed fixtures.

Each test proves only that a real Contact workload dispatches to
``open_contact_provider`` and lists its top-level bucket; none lists or reads a
contact, whose name/email/groups are content and would be recorded into the
fixture. Per-contact listing, grouping and JSON content are covered by
``tests/unit/sdk/test_units_saas_contact.py``.

Fixtures, recorded against ``vault-plain/@ActiveProtectVault``, each by
its one test:

- ``units_saas_contact_gws_vault_plain.json.gz`` — a GWS Contact workload: index
  resolution and the top-level "Contacts" bucket's existence.
- ``units_saas_contact_m365_empty_vault_plain.json.gz`` — an empty M365 Exchange
  Contacts workload (``USER_EXCHANGE``, version_id 96).
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
from synology_apm_repo.sdk.units.saas.contact import open_contact_provider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_GWS_CONTACT_WORKLOAD_ID = 7
_M365_EMPTY_CONTACT_WORKLOAD_ID = 24


async def _open_gws_provider(repo: DedupRepo, saas_streams: SaasStreamCache) -> SaasWorkloadProvider[Any]:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == _GWS_CONTACT_WORKLOAD_ID)
    version = (await versions(repo, workload))[-1]  # latest
    return await open_contact_provider(repo, version, saas_streams)


async def test_replayed_gws_contact_workload_resolves_to_its_top_level_contacts_bucket(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_contact_gws_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_gws_provider(repo, saas_streams)
        try:
            [bucket] = await provider.children(provider.root())
            assert bucket.name == "Contacts"
        finally:
            await provider.close()


async def test_replayed_m365_contacts_construct_successfully_even_when_genuinely_empty(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_contact_m365_empty_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        workload = next(w for w in all_workloads if w.workload_id == _M365_EMPTY_CONTACT_WORKLOAD_ID)
        version = next(v for v in await versions(repo, workload) if v.version_id == 96)
        provider = await open_contact_provider(repo, version, saas_streams)
        try:
            top = await provider.children(provider.root())
            assert top == []  # a real, empty contact_table — a legitimate zero-contacts tenant, not a gap
        finally:
            await provider.close()
