"""Regression tests for ``synology_apm_repo.sdk.units.saas.drive``,
replayed from a committed fixture recorded against real bytes.

Fixture: ``units_saas_drive_vault_plain.json.gz``, recorded against
``vault-plain/@ActiveProtectVault``: the ``TEAM_DRIVE`` workload
(``_TEAM_DRIVE_WORKLOAD_ID``), both through a direct ``open_drive_provider(...)``
pinned to stream ``XfGkaDjWyGhXVoRC`` and through ``dispatch.py`` for its
latest version. Every test makes the same calls, so any one of them is
the recording recipe.

No leaf's content is read: each file's ``kind``/``size`` comes from the
index's ``item_table`` metadata; dedup content reconstruction is proven by
``tests/unit/sdk/test_dedup_dedup_file.py``. Every test still passes
``allow_content=True``: provider creation reads the object-name index (an
internal routing table) through ``dedup_file.read()``.
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
from synology_apm_repo.sdk.units.base import Node, UnitKind
from synology_apm_repo.sdk.units.dispatch import saas_provider_for
from synology_apm_repo.sdk.units.saas.drive import open_drive_provider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

#: The stream alone doesn't pin the workload: vault-plain shares stream
#: ``XfGkaDjWyGhXVoRC`` across several sub_types.
_TEAM_DRIVE_WORKLOAD_ID = 10


async def _open_repo(record_target: Callable[..., Awaitable[ObjectStore]]) -> DedupRepo:
    store = await record_target("units_saas_drive_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    return await DedupRepo.open(store, layout)


async def _open_provider(repo: DedupRepo, saas_streams: SaasStreamCache) -> SaasWorkloadProvider[Any]:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    version = await anext(
        v
        for w in all_workloads
        for v in await versions(repo, w)
        if w.workload_id == _TEAM_DRIVE_WORKLOAD_ID and v.saas_stream_uuid == "XfGkaDjWyGhXVoRC" and not v.deleted
    )
    return await open_drive_provider(repo, version, saas_streams)


async def test_replayed_root_and_nested_tree_match_known_real_layout(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with await _open_repo(record_target) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams)
        try:
            top = await provider.children(provider.root())
            # Backed-up file names aren't anonymized, so they're asserted literally.
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
    """Every real leaf is a ``DRIVE_ITEM`` whose positive ``size`` is its
    ``item_table`` row's, read from the same session's index; no leaf's
    content is read."""
    async with await _open_repo(record_target) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams)
        try:
            listed: dict[str, int | None] = {}

            async def _walk(node: Node) -> None:
                for child in await provider.children(node):
                    if child.is_leaf:
                        assert child.kind is UnitKind.DRIVE_ITEM
                        listed[child.ref.extra_segments[-1]] = child.size
                    else:
                        await _walk(child)

            await _walk(provider.root())
            cursor = await provider.table("item_table").execute("SELECT item_id, size FROM item_table WHERE type = 1")
            item_table_sizes = {str(item_id): size for item_id, size in await cursor.fetchall()}
            assert listed == item_table_sizes
            assert len(listed) == 6  # C.jpg, L.jpg, B.zip, test.docx, Team_F.docx, 1.txt
            assert all(size is not None and size > 0 for size in listed.values())
        finally:
            await provider.close()


async def test_replayed_team_drive_dispatches_via_the_object_name_index_and_lists_its_root(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with await _open_repo(record_target) as repo, SaasStreamCache(repo) as saas_streams:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        workload = next(w for w in all_workloads if w.workload_id == _TEAM_DRIVE_WORKLOAD_ID)
        version = (await versions(repo, workload))[-1]  # latest
        provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(provider, SaasWorkloadProvider), type(provider)
        try:
            top = await provider.children(provider.root())
            assert len(top) == 7
            assert {n.name for n in top if n.is_leaf} == {"L.jpg", "B.zip", "test.docx", "Team_F.docx", "1.txt"}
        finally:
            await provider.close()
