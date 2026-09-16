"""Regression test for ``synology_apm_repo.sdk.units.saas.contact``
against real GWS/M365 Contact workloads — replayed from committed
fixtures, with **no external dependency**: these always run, on CI or
anywhere else, because they go through ``ReplayStore`` instead of a real
``LocalFsStore``.

Deliberately narrow: this module only proves that a real GWS/M365 Contact
workload dispatches to ``ContactProvider`` and lists its top-level
bucket/folder correctly — it never lists or reads individual real
contacts. A real contact's own name/email/group membership is content, not
structure (unlike a Device/FS/Drive node, whose name is just a filename)
— see ``test_units_saas_mail.py``'s own docstring for why reading it
anyway would defeat the point. Per-contact listing, grouping, and
JSON-content correctness are fully covered synthetically by
``tests/unit/sdk/test_units_saas_contact.py`` instead (this workload type
has never had a real-sample regression check beyond what's here).

The fixtures (``tests/fixtures/``, recorded against a real store rooted at
``apv-sample-1/@ActiveProtectVault`` — see ``tests/CLAUDE.md``'s
"Recording a fixture" section for the ``pytest --record-against=...``/
``make record-fixture`` workflow that (re-)records these):

- ``units_saas_contact_gws_apv1.json.gz`` — a real GWS Contact workload:
  just the index resolution and the top-level "Contacts" bucket's own
  existence, not its contents.
- ``units_saas_contact_m365_empty_apv1.json.gz`` — a real,
  genuinely empty M365 Exchange Contacts workload (``USER_EXCHANGE``,
  version_id 96).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.saas.contact import ContactProvider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider

#: Both workload ids are internal catalog identifiers -- stable and
#: non-identifying (never touched by catalog-metadata anonymization, so
#: the same values resolve the right workload whether replaying the
#: anonymized fixture or recording fresh against the real backend).
_GWS_CONTACT_WORKLOAD_ID = 7
_M365_EMPTY_CONTACT_WORKLOAD_ID = 24


async def _open_gws_provider(repo: DedupRepo) -> SaasWorkloadProvider:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == _GWS_CONTACT_WORKLOAD_ID)
    version = (await versions(repo, workload))[-1]  # latest
    return await ContactProvider(repo, version)


async def test_replayed_gws_contact_workload_resolves_to_its_top_level_contacts_bucket(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_contact_gws_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo:
        provider = await _open_gws_provider(repo)
        try:
            [bucket] = await provider.children(provider.root())
            assert bucket.name == "Contacts"
        finally:
            await provider.close()


async def test_replayed_m365_contacts_construct_successfully_even_when_genuinely_empty(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_contact_m365_empty_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        workload = next(w for w in all_workloads if w.workload_id == _M365_EMPTY_CONTACT_WORKLOAD_ID)
        version = next(v for v in await versions(repo, workload) if v.version_id == 96)
        provider = await ContactProvider(repo, version)
        try:
            top = await provider.children(provider.root())
            assert top == []  # a real, empty contact_table — a legitimate zero-contacts tenant, not a gap
        finally:
            await provider.close()


__all__: list[str] = []
