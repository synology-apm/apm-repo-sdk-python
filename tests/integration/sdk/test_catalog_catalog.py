"""Regression test for ``catalog.connection``/``catalog.workload``/
``catalog.version`` — replayed from a committed
fixture recorded against real bytes, with **no external dependency**:
this always runs, on CI or anywhere else, because it goes through
``ReplayStore`` instead of a real ``LocalFsStore``.

The fixture (``tests/fixtures/catalog_catalog_apv1.json.gz``) was
produced once by ``RecordingStore`` wrapping a real store rooted at
``apv-sample-1/@ActiveProtectVault``, recording every ``ObjectStore`` call
``connections()``/``workloads()`` make across every connection, plus
``versions()`` for the real Windows VM workload (``_WINDOWS_VM_WORKLOAD_ID``
below).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout

#: Internal catalog identifiers -- stable and non-identifying (never
#: touched by catalog-metadata anonymization, so each resolves the same
#: real workload whether replaying the anonymized fixture or recording
#: fresh against the real backend). apv-sample-1 has two FS workloads
#: (ids 1 and 4) -- ``workload_type == "FS"`` alone would be ambiguous
#: between them.
_WINDOWS_VM_WORKLOAD_ID = 2
_FS_WORKLOAD_ID = 1
#: The real MAIL workload sharing one anonymized persona with its
#: CALENDAR/CONTACT/DRIVE siblings (workload_ids 6/7/8) -- the
#: persona-correlation itself is unit-tested (``test_anonymize_catalog_
#: metadata.py``'s ``test_user_info_persona_is_correlated_across_fields``);
#: this only needs the id to confirm sub_type against real data below.
_MAIL_PERSONA_WORKLOAD_ID = 5


@pytest.fixture
async def repo(record_target: Callable[[str], Awaitable[ObjectStore]]) -> AsyncIterator[DedupRepo]:
    """``ReplayStore`` answers every call from a static in-memory dict keyed
    by exact call parameters, with no call-order tracking at all, so
    nothing here is order- or interleaving-sensitive across
    tests, and every test below only ever reads, never mutates, the opened
    repository."""
    store = await record_target("catalog_catalog_apv1.json.gz")
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as r:
        yield r


async def test_replayed_exactly_two_top_level_connections(repo: DedupRepo) -> None:
    conns = await connections(repo)
    assert len(conns) == 2


async def test_replayed_total_workload_and_version_counts_match_apv_sample_1(repo: DedupRepo) -> None:
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


async def test_replayed_saas_workload_sub_type_counts_match_apv_sample_1(repo: DedupRepo) -> None:
    conns = await connections(repo)
    all_workloads = [w for c in conns for w in await workloads(repo, c)]

    assert len([w for w in all_workloads if w.sub_type == "SITE"]) == 2

    mail = next(w for w in all_workloads if w.workload_id == _MAIL_PERSONA_WORKLOAD_ID)
    assert mail.sub_type == "MAIL"

    assert len([w for w in all_workloads if w.sub_type == "TEAM_DRIVE"]) == 2
    assert len([w for w in all_workloads if w.sub_type == "TEAMS"]) == 2
    assert len([w for w in all_workloads if w.sub_type == "GROUP_EXCHANGE"]) == 1


async def test_replayed_m365_and_gw_workloads_carry_the_real_tenant_id_and_domain(repo: DedupRepo) -> None:
    """Direct lock-in of ``Workload.tenant_id``/``domain`` against
    apv-sample-1's own real data. ``tenant_id`` (an M365 tenant
    GUID) is never touched by catalog-metadata anonymization, so it's
    safe to hardcode; ``domain`` (a GWS domain string) *is* -- every real
    domain collapses to the same fixed ``gws_domain`` placeholder, so this
    only asserts the structural invariant the placeholder also preserves
    (every GW workload here shares one tenant domain) rather than
    hardcoding either the real value or the placeholder, which would make
    this test's own recording recipe impossible to run against a real,
    not-yet-anonymized backend."""
    conns = await connections(repo)
    all_workloads = [w for c in conns for w in await workloads(repo, c)]
    m365_workloads = [w for w in all_workloads if w.workload_type == "M365"]
    gw_workloads = [w for w in all_workloads if w.workload_type == "GW"]
    assert m365_workloads and gw_workloads

    for w in m365_workloads:
        assert w.tenant_id == "87c467dd-ac00-45d8-babb-e2b0787e2d13", (w.display_name, w.sub_type, w.tenant_id)
        assert w.domain is None, (w.display_name, w.sub_type, w.domain)
    gw_domains = set()
    for w in gw_workloads:
        assert w.domain is not None, (w.display_name, w.sub_type, w.domain)
        assert w.tenant_id is None, (w.display_name, w.sub_type, w.tenant_id)
        gw_domains.add(w.domain)
    assert len(gw_domains) == 1, gw_domains  # every real GW workload here shares one tenant domain

    for w in all_workloads:
        if w.workload_type not in ("M365", "GW"):
            assert w.tenant_id is None and w.domain is None, (w.display_name, w.workload_type)


async def test_replayed_no_saas_workload_falls_back_to_the_generic_sub_type_placeholder(
    repo: DedupRepo,
) -> None:
    """Every real SaaS workload in this fixture must resolve to a real
    name, never the generic ``f"{sub_type} workload"``/``"SaaS
    workload"`` fallback ``_saas_display_name()`` only reaches when none
    of its known ``entity_spec`` shapes matched."""
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


__all__: list[str] = []
