"""Regression tests for the FS resolution chain — catalog
(``connections``/``workloads``/``versions``) → ``FsProvider`` → a file
node. None reads a file's ``Composition``/``Pool`` content;
``DedupFile.read()``'s decoding is covered by
``tests/unit/sdk/test_dedup_dedup_file.py``. Each fixture's one test is its
recording recipe:

- ``fs_config_json_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``: the catalog tables, FS workload 1's
  ``target.db`` + ``version.db.zst``, and ``db/file_map``, down to a known
  file's node and its content's ``size``.
- ``fs_no_duplicate_children_vault_plain.json.gz`` — same root, FS workload 4's
  top-level children only.
- ``fs_repo_root_vault_plain.json.gz`` — FS workload 1 again, recorded against
  ``vault-plain`` itself (a non-empty ``repo_root`` from
  ``iter_repository_layouts``); asserts only listed ``kind``/``is_leaf``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import WorkloadId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout, iter_repository_layouts
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.fs import FsProvider

#: Looked up by id: two FS workloads in this sample share one display name.
_FS_WORKLOAD_ID = WorkloadId(1)
_FS_NO_DUP_CHILDREN_WORKLOAD_ID = WorkloadId(4)


async def test_replayed_fs_tree_resolves_to_a_known_json_config_file(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("fs_config_json_vault_plain.json.gz")
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        fs = next(w for w in all_workloads if w.workload_id == _FS_WORKLOAD_ID)
        # The oldest of three versions: the one this fixture recorded.
        version = next(v for v in await versions(repo, fs) if v.version_uid == "f72e8124-7e8f-43f7-afe5-52726835b3f3")

        async with FsProvider(repo, version) as provider:
            top = await provider.children(provider.root())
            assert {n.name for n in top} == {"ActiveBackupforBusiness", "docker", "test", "web", "web_packages"}
            assert all(not n.is_leaf for n in top)

            test_dir = next(n for n in top if n.name == "test")
            test_children = await provider.children(test_dir)
            config = next(n for n in test_children if n.name == "config.json")
            assert config.size == 2420

            # The unit's content reports its size without reading any bytes.
            content = (await provider.unit(config)).content
            assert content.size == 2420


async def test_replayed_fs_tree_has_no_duplicate_entries_at_the_same_level(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("fs_no_duplicate_children_vault_plain.json.gz")
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        fs = next(w for w in all_workloads if w.workload_id == _FS_NO_DUP_CHILDREN_WORKLOAD_ID)
        version = next(v for v in await versions(repo, fs) if v.meta is not None)

        async with FsProvider(repo, version) as provider:
            top = await provider.children(provider.root())
            names = [n.name for n in top]
            assert len(names) == len(set(names))


async def test_replayed_fs_files_listed_when_repo_root_is_non_empty(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """``FsProvider`` lists files under a non-empty ``repo_root``, as
    ``iter_repository_layouts`` yields when the store is rooted above the
    vault."""
    store = await record_target("fs_repo_root_vault_plain.json.gz")
    layout = await anext(
        layout
        async for repo in iter_repository_layouts(store)
        for layout in catalog_repo_layouts(repo)
        if layout.repo_root
    )
    assert layout.repo_root == "@ActiveProtectVault"

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        fs = next(w for w in all_workloads if w.workload_id == _FS_WORKLOAD_ID)
        version = next(v for v in await versions(repo, fs) if v.meta is not None)

        async with FsProvider(repo, version) as provider:
            top = await provider.children(provider.root())
            assert {n.name for n in top} == {"ActiveBackupforBusiness", "docker", "test", "web", "web_packages"}

            test_dir = next(n for n in top if n.name == "test")
            config = next(n for n in await provider.children(test_dir) if n.name == "config.json")
            assert config.is_leaf and config.kind is UnitKind.FILE
