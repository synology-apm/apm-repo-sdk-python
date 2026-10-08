"""Regression test for ``catalog.connection``/``catalog.workload``/
``catalog.version``, replayed from a committed fixture recorded against
real bytes.

Fixture: ``catalog_catalog_vault_plain.json.gz``, recorded against
``vault-plain/@ActiveProtectVault``: ``connections()``/``workloads()``
across every connection, plus ``versions()`` for the Windows VM workload
(``_WINDOWS_VM_WORKLOAD_ID``). ``test_replayed_versions_are_named_by_backup_time``
is its recording recipe (a superset of every other test's calls).
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

# Looked up by id: vault-plain has two VM and two FS workloads, so
# ``workload_type`` alone is ambiguous.
_WINDOWS_VM_WORKLOAD_ID = 2
_FS_WORKLOAD_ID = 1
#: The MAIL workload sharing one anonymized persona with its
#: CALENDAR/CONTACT/DRIVE siblings (workload_ids 6/7/8); persona
#: correlation itself is tested by
#: ``tests/unit/support/test_recording_anonymize_catalog_metadata.py``.
_MAIL_PERSONA_WORKLOAD_ID = 5


@pytest.fixture
async def repo(record_target: Callable[[str], Awaitable[ObjectStore]]) -> AsyncIterator[DedupRepo]:
    """The recorded ``vault-plain`` vault, opened read-only."""
    store = await record_target("catalog_catalog_vault_plain.json.gz")
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as r:
        yield r


async def test_replayed_exactly_two_top_level_connections(repo: DedupRepo) -> None:
    conns = await connections(repo)
    assert len(conns) == 2


async def test_replayed_total_workload_and_version_counts_match_vault_plain(repo: DedupRepo) -> None:
    conns = await connections(repo)
    assert sum(c.workload_count for c in conns) == 25
    assert sum(c.version_count for c in conns) == 109


async def test_replayed_device_workload_types_and_subtitles(repo: DedupRepo) -> None:
    conns = await connections(repo)
    all_workloads = [w for c in conns for w in await workloads(repo, c)]

    vm = next(w for w in all_workloads if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
    assert vm.workload_type == "VM"
    assert vm.subtitle == "Windows 10 (64-bit)"

    fs = next(w for w in all_workloads if w.workload_id == _FS_WORKLOAD_ID)
    assert fs.workload_type == "FS"
    assert fs.subtitle == "smb"


async def test_replayed_saas_workload_sub_type_counts_match_vault_plain(repo: DedupRepo) -> None:
    conns = await connections(repo)
    all_workloads = [w for c in conns for w in await workloads(repo, c)]

    assert len([w for w in all_workloads if w.sub_type == "SITE"]) == 2

    mail = next(w for w in all_workloads if w.workload_id == _MAIL_PERSONA_WORKLOAD_ID)
    assert mail.sub_type == "MAIL"

    assert len([w for w in all_workloads if w.sub_type == "TEAM_DRIVE"]) == 2
    assert len([w for w in all_workloads if w.sub_type == "TEAMS"]) == 2
    assert len([w for w in all_workloads if w.sub_type == "GROUP_EXCHANGE"]) == 1


async def test_replayed_m365_and_gws_workloads_carry_the_real_tenant_id_and_domain(repo: DedupRepo) -> None:
    """M365 workloads carry the tenant GUID and GWS workloads one shared
    ``domain``. ``tenant_id`` is never anonymized, so it is hardcoded;
    ``domain`` is, so only its sharing is asserted, which holds both against
    the anonymized fixture and when recording fresh."""
    conns = await connections(repo)
    all_workloads = [w for c in conns for w in await workloads(repo, c)]
    m365_workloads = [w for w in all_workloads if w.workload_type == "M365"]
    gws_workloads = [w for w in all_workloads if w.workload_type == "GW"]
    assert m365_workloads and gws_workloads

    for w in m365_workloads:
        assert w.tenant_id == "87c467dd-ac00-45d8-babb-e2b0787e2d13", (w.display_name, w.sub_type, w.tenant_id)
        assert w.domain is None, (w.display_name, w.sub_type, w.domain)
    gws_domains = set()
    for w in gws_workloads:
        assert w.domain is not None, (w.display_name, w.sub_type, w.domain)
        assert w.tenant_id is None, (w.display_name, w.sub_type, w.tenant_id)
        gws_domains.add(w.domain)
    assert len(gws_domains) == 1, gws_domains  # every real GWS workload here shares one tenant domain

    for w in all_workloads:
        if w.workload_type not in ("M365", "GW"):
            assert w.tenant_id is None and w.domain is None, (w.display_name, w.workload_type)


async def test_replayed_no_saas_workload_falls_back_to_the_generic_sub_type_placeholder(
    repo: DedupRepo,
) -> None:
    """No real SaaS workload falls back to the generic
    ``f"{sub_type} workload"``/``"SaaS workload"`` name
    ``_saas_display_name()`` uses when no known ``entity_spec`` shape
    matches."""
    conns = await connections(repo)
    all_workloads = [w for c in conns for w in await workloads(repo, c)]
    saas_workloads = [w for w in all_workloads if w.sub_type is not None]
    assert saas_workloads
    for w in saas_workloads:
        assert not w.display_name.endswith(" workload"), (w.display_name, w.sub_type)


async def test_replayed_versions_are_named_by_backup_time(repo: DedupRepo) -> None:
    conns = await connections(repo)
    all_workloads = [w for c in conns for w in await workloads(repo, c)]
    vm = next(w for w in all_workloads if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
    vs = await versions(repo, vm)
    assert len(vs) >= 1
    for v in vs:
        assert v.display_name != v.version_uid
        assert len(v.display_name) == len("2026-08-06 21:59:59")
    assert {v.display_name for v in vs} == {
        "2026-08-06 21:53:28",
        "2026-08-06 21:57:06",
        "2026-08-07 09:00:10",
    }
