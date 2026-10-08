"""Regression tests for SaaS Mail/Calendar dispatch and listing, replayed
from committed fixtures recorded against real bytes.

Each replayed test proves only that a real workload dispatches to the right
provider and lists its folder/calendar/event counts; none reads a message
body or an event's fields. Those are content, and reading them would record
it into the fixture whatever the test asserts. Mail's ``X-ABL-ID``
reassembly and attachment fidelity are covered by
``tests/unit/sdk/test_units_saas_mail.py``; Calendar's ICS building by
``tests/unit/sdk/test_units_saas_calendar.py``. Drive is covered by
``test_units_saas_drive.py``.

Fixtures, recorded against ``vault-plain/@ActiveProtectVault``:

- ``saas_content_mail_family_vault_plain.json.gz`` — a MAIL workload, a
  GROUP_EXCHANGE workload's Mail and Calendar siblings, and a
  USER_EXCHANGE workload's Archive Mail sibling. The three tests using it
  read different workloads, so recording needs all three run together.
- ``saas_content_mail_m365_grace_vault_plain.json.gz`` — an M365 USER_EXCHANGE
  workload; its one test.
- ``saas_content_calendar_alice_vault_plain.json.gz`` — the CALENDAR workload on
  stream ``KxMWSUvtSZiaDTDy``; its one test.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout
from synology_apm_repo.sdk.units.base import Node, UnitProvider
from synology_apm_repo.sdk.units.dispatch import saas_provider_for
from synology_apm_repo.sdk.units.saas.calendar import open_calendar_provider
from synology_apm_repo.sdk.units.saas.composite_provider import CompositeSaasProvider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_MAIL_WORKLOAD_ID = 5
_GROUP_EXCHANGE_WORKLOAD_ID = 21
_ARCHIVE_MAIL_WORKLOAD_ID = 24
_M365_EXCHANGE_MAIL_WORKLOAD_ID = 19


async def _open_repo(record_target: Callable[..., Awaitable[ObjectStore]], fixture_name: str) -> DedupRepo:
    # allow_content=True: provider creation reads the object-name index, an
    # internal routing table, through dedup_file.read().
    store = await record_target(fixture_name, allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    return await DedupRepo.open(store, layout)


@pytest.fixture
async def mail_family_repo(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> AsyncIterator[DedupRepo]:
    """The repository recorded in ``saas_content_mail_family_vault_plain.json.gz``,
    for the three tests below."""
    async with await _open_repo(record_target, "saas_content_mail_family_vault_plain.json.gz") as r:
        yield r


async def test_alice_mail_workload_resolves_to_its_folder_and_lists_its_messages_replayed(
    mail_family_repo: DedupRepo,
) -> None:
    repo = mail_family_repo
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == _MAIL_WORKLOAD_ID)
    version = next(v for v in await versions(repo, workload) if v.version_id == 9)
    async with SaasStreamCache(repo) as saas_streams:
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, SaasWorkloadProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            [folder] = await provider.children(provider.root())
            mails = await provider.children(folder)
            assert len(mails) == 25
        finally:
            await provider.close()


async def test_group_exchange_mail_and_calendar_resolve_via_the_object_name_index_replayed(
    mail_family_repo: DedupRepo,
) -> None:
    repo = mail_family_repo
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == _GROUP_EXCHANGE_WORKLOAD_ID)
    version = next(v for v in await versions(repo, workload) if v.version_id == 93)
    async with SaasStreamCache(repo) as saas_streams:
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, CompositeSaasProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            groups = await provider.children(provider.root())
            group_names = {g.name for g in groups}
            assert group_names == {"Mail", "Calendars"}, group_names

            mail_group = next(g for g in groups if g.name == "Mail")
            [folder] = await provider.children(mail_group)
            mails = await provider.children(folder)
            assert len(mails) == 1

            calendar_group = next(g for g in groups if g.name == "Calendars")
            # Calendar adds a My/Other Calendars level under the group.
            [my_calendars] = await provider.children(calendar_group)
            [calendar_node] = await provider.children(my_calendars)
            events = await provider.children(calendar_node)
            assert events == []
        finally:
            await provider.close()


async def test_archive_mail_is_a_real_sibling_alongside_regular_mail_replayed(
    mail_family_repo: DedupRepo,
) -> None:
    repo = mail_family_repo
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == _ARCHIVE_MAIL_WORKLOAD_ID)
    version = next(v for v in await versions(repo, workload) if v.version_id == 96)
    async with SaasStreamCache(repo) as saas_streams:
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, CompositeSaasProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            groups = await provider.children(provider.root())
            group_names = {g.name for g in groups}
            assert group_names == {"Mail", "Contacts", "Calendars", "Archive"}, group_names

            archive_group = next(g for g in groups if g.name == "Archive")
            folders = await provider.children(archive_group)
            assert folders == []
        finally:
            await provider.close()


async def _count_leaves(provider: UnitProvider, node: Node) -> int:
    """Counts the leaves under ``node`` at any depth: M365 Mail
    (``RecursiveGroupFlatTree``) nests messages in subfolders."""
    total = 0
    for child in await provider.children(node):
        total += 1 if child.is_leaf else await _count_leaves(provider, child)
    return total


async def test_m365_exchange_mail_workload_resolves_to_its_folder_and_lists_its_messages_replayed(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with (
        await _open_repo(record_target, "saas_content_mail_m365_grace_vault_plain.json.gz") as repo,
        SaasStreamCache(repo) as saas_streams,
    ):
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        workload = next(w for w in all_workloads if w.workload_id == _M365_EXCHANGE_MAIL_WORKLOAD_ID)
        version = next(v for v in await versions(repo, workload) if v.version_id == 91)
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, CompositeSaasProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            groups = await provider.children(provider.root())
            mail_group = next(g for g in groups if g.name == "Mail")
            assert await _count_leaves(provider, mail_group) == 122
        finally:
            await provider.close()


async def test_calendar_workload_resolves_and_lists_a_real_event_count_replayed(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with (
        await _open_repo(record_target, "saas_content_calendar_alice_vault_plain.json.gz") as repo,
        SaasStreamCache(repo) as saas_streams,
    ):
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        candidates = [
            v
            for w in all_workloads
            if w.sub_type == "CALENDAR"
            for v in await versions(repo, w)
            if v.saas_stream_uuid == "KxMWSUvtSZiaDTDy" and not v.deleted
        ]
        version = max(candidates, key=lambda v: v.version_id)
        provider = await open_calendar_provider(repo, version, saas_streams)
        try:
            checked = 0
            for category in await provider.children(provider.root()):
                for calendar in await provider.children(category):
                    checked += len(await provider.children(calendar))
            assert checked >= 41
        finally:
            await provider.close()
