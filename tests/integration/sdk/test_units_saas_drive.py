"""Regression test for ``synology_apm_repo.sdk.units.saas.drive``,
replayed from a committed fixture recorded against real bytes, with **no
external dependency**: this always runs, on CI or anywhere else, because
it goes through ``ReplayStore`` instead of a real ``LocalFsStore``.

The fixture (``tests/fixtures/units_saas_drive_apv1.json.gz``) was
produced by ``RecordingStore`` wrapping a real store rooted at
``apv-sample-1/@ActiveProtectVault`` via this module's own
``record_target()`` call — see ``tests/conftest.py`` and
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...`` workflow that (re-)records this: recording
apv-sample-1's real ``TEAM_DRIVE`` workload (``_TEAM_DRIVE_WORKLOAD_ID``
below), both a direct ``DriveProvider(...)`` construction (pinned to
stream ``XfGkaDjWyGhXVoRC``) and the workload's latest version resolved
through the real ``dispatch.py`` routing.

No real leaf's content is ever read — every real file, ``P/C.jpg``
included, only has its ``kind``/``size``/``attrs`` checked against the
index's own ``item_table`` metadata
(``test_replayed_every_real_files_kind_and_size_match_item_table``
below). Dedup content reconstruction itself (does a chunk/composition
layout decode to the right plaintext at all) is proven synthetically,
with zero real-sample dependency, by
``tests/unit/sdk/test_dedup_dedup_file.py`` instead — this is the same
"push real-content-dependent proof to a synthetic test, keep the replay
narrow" idea the Teams-chat fixture's own docstring describes for its
"many stickers" scenario, applied here because ``P/C.jpg``'s own real
bytes are a third-party illustration, not inert sample data, and can't
be committed to a public fixture at all.

Every test below still needs ``record_target(..., allow_content=True)``:
``SaasWorkloadProvider.create()``/``DriveProvider(...)`` itself resolves
its own object-name index via a real ``dedup_file.read()`` (an internal
routing table, not any leaf's own content) before any of the above even
begins.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.base import Node, UnitKind
from synology_apm_repo.sdk.units.dispatch import saas_provider_for
from synology_apm_repo.sdk.units.saas.drive import DriveProvider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider

#: Internal catalog identifier -- stable and non-identifying (never
#: touched by catalog-metadata anonymization, so it resolves the same
#: real workload whether replaying the anonymized fixture or recording
#: fresh against the real backend). ``saas_stream_uuid`` alone isn't
#: enough to pin this one down: apv-sample-1 reuses stream
#: ``XfGkaDjWyGhXVoRC`` across several unrelated workloads/sub_types
#: (MAIL, CONTACT, CALENDAR, DRIVE, TEAM_DRIVE).
_TEAM_DRIVE_WORKLOAD_ID = 10


async def _open_repo(record_target: Callable[..., Awaitable[ObjectStore]]) -> DedupRepo:
    store = await record_target("units_saas_drive_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    return await DedupRepo.open(store, layout)


async def _open_provider(repo: DedupRepo) -> SaasWorkloadProvider:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    version = await anext(
        v
        for w in all_workloads
        for v in await versions(repo, w)
        if w.workload_id == _TEAM_DRIVE_WORKLOAD_ID and v.saas_stream_uuid == "XfGkaDjWyGhXVoRC" and not v.deleted
    )
    return await DriveProvider(repo, version)


async def test_replayed_root_and_nested_tree_match_known_real_layout(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with await _open_repo(record_target) as repo:
        provider = await _open_provider(repo)
        try:
            top = await provider.children(provider.root())
            # A folder/file name here is a real, backed-up filename choice
            # (unlike a catalog identifier, this repository's anonymization
            # never touches it) -- assert shape only, never a literal name.
            assert len(top) == 7
            leaf_names = {n.name for n in top if n.is_leaf}
            assert leaf_names == {"L.jpg", "B.zip", "test.docx", "Team_F.docx", "1.txt"}
            folders = [n for n in top if not n.is_leaf]
            assert len(folders) == 2

            children_by_folder = [(f, await provider.children(f)) for f in folders]
            non_empty = [c for f, c in children_by_folder if c]
            empty = [c for f, c in children_by_folder if not c]
            assert len(non_empty) == 1 and len(empty) == 1
            assert [n.name for n in non_empty[0]] == ["C.jpg"]
        finally:
            await provider.close()


async def test_replayed_every_real_files_kind_and_size_match_item_table(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """Every real leaf's ``kind``/``size`` is checked against the catalog's
    own ``item_table`` metadata; no leaf's content is ever read — see this
    module's own docstring for why."""
    async with await _open_repo(record_target) as repo:
        provider = await _open_provider(repo)
        try:
            checked = 0

            async def _walk(node: Node) -> None:
                nonlocal checked
                for child in await provider.children(node):
                    if child.is_leaf:
                        assert child.kind is UnitKind.DRIVE_ITEM
                        assert child.size is not None and child.size > 0
                        checked += 1
                    else:
                        await _walk(child)

            await _walk(provider.root())
            assert checked == 6  # C.jpg, L.jpg, B.zip, test.docx, Team_F.docx, 1.txt
        finally:
            await provider.close()


async def test_replayed_team_drive_dispatches_and_resolves_real_content_via_the_catalog_index(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with await _open_repo(record_target) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        workload = next(w for w in all_workloads if w.workload_id == _TEAM_DRIVE_WORKLOAD_ID)
        version = (await versions(repo, workload))[-1]  # latest
        provider = await saas_provider_for(repo, workload, version)
        assert isinstance(provider, SaasWorkloadProvider), type(provider)
        try:
            top = await provider.children(provider.root())
            assert len(top) == 7
            assert {n.name for n in top if n.is_leaf} == {"L.jpg", "B.zip", "test.docx", "Team_F.docx", "1.txt"}
        finally:
            await provider.close()


__all__: list[str] = []
