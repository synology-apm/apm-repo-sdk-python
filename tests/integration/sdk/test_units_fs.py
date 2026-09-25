"""Regression test for the full FS single-file resolution chain — catalog
(``connections``/``workloads``/``versions``) → ``FsProvider`` →
``DedupFile`` content — replayed from committed fixtures recorded
against real bytes, with **no external dependency**: these always run, on
CI or anywhere else, because they go through ``ReplayStore`` instead of a
real ``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded against a real store rooted at
``apv-sample-1`` -- see ``tests/CLAUDE.md``'s "Recording a fixture"
section for the ``pytest --record-against=...``/``make record-fixture``
workflow that (re-)records these):

- ``fs_config_json_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``,
  ``db/connection_config``/``workload_config``/``copy_target_version(_meta)``
  (catalog), the FS workload id 1's ``target.db`` + ``version.db.zst``
  (``FsProvider``'s tree), and ``db/file_map`` — the resolution chain down
  to a known FS file's node, stopping at ``.open()`` (metadata only).
  Deliberately narrow: this fixture never reads the file's real
  ``Composition``/``Pool`` chunk content — ``DedupFile.read()``'s own
  decoding correctness is already covered synthetically, with zero real
  data, by ``tests/unit/sdk/test_units_fs.py``.
- ``fs_no_duplicate_children_apv1.json.gz`` — same root, but FS
  workload id 4 (the first ``workload_type == "FS"`` entry catalog
  enumeration yields), top-level children listing only.
- ``fs_repo_root_apv1.json.gz`` — FS workload id 1 again, rooted at
  ``apv-sample-1`` itself (a non-empty ``repo_root``, discovered via
  ``iter_layouts``) rather than directly at ``@ActiveProtectVault``.
  Deliberately narrow the same way ``fs_config_json_apv1.json.gz`` is:
  never reads a file's real ``Composition``/``Pool`` chunk content, only
  its listed ``kind``/``is_leaf`` — a real file's own backed-up content is
  never asserted on here, only structural resolution.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import WorkloadId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout, iter_layouts
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.fs import FsProvider

#: Internal catalog identifiers -- stable and non-identifying (never
#: touched by catalog-metadata anonymization). Two FS workloads in this
#: sample's real data share the same display name ("192.0.2.10", two
#: connections connecting to the same host), so a workload_type-only
#: filter would be ambiguous between them.
_FS_WORKLOAD_ID = WorkloadId(1)
_FS_NO_DUP_CHILDREN_WORKLOAD_ID = WorkloadId(4)


async def test_replayed_fs_tree_resolves_to_a_known_json_config_file(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("fs_config_json_apv1.json.gz")
    layout = await detect_layout(store)

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        fs = next(w for w in all_workloads if w.workload_id == _FS_WORKLOAD_ID)
        # This fixture's real apv-sample-1 data has three FS versions with
        # meta; versions() returns them newest-first, so picking by
        # version_uid pins down the exact one the fixture actually
        # recorded a full tree walk for (2026-08-06 21:53:59, the
        # *oldest* of the three, not the newest).
        version = next(v for v in await versions(repo, fs) if v.version_uid == "f72e8124-7e8f-43f7-afe5-52726835b3f3")

        async with FsProvider(repo, version) as provider:
            top = await provider.children(provider.root())
            assert {n.name for n in top} == {"ActiveBackupforBusiness", "docker", "test", "web", "web_packages"}
            assert all(not n.is_leaf for n in top)

            test_dir = next(n for n in top if n.name == "test")
            test_children = await provider.children(test_dir)
            config = next(n for n in test_children if n.name == "config.json")
            assert config.size == 2420

            # .open() only reports metadata (no real I/O) -- this test
            # deliberately never reads the file's real Composition/Pool
            # chunk content: DedupFile.read()'s own decoding correctness
            # is already covered synthetically, with zero real data, by
            # tests/unit/sdk/test_units_fs.py.
            content = (await provider.unit(config)).open()
            assert content.size == 2420


async def test_replayed_fs_tree_has_no_duplicate_entries_at_the_same_level(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("fs_no_duplicate_children_apv1.json.gz")
    layout = await detect_layout(store)

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        fs = next(w for w in all_workloads if w.workload_id == _FS_NO_DUP_CHILDREN_WORKLOAD_ID)
        version = next(v for v in await versions(repo, fs) if v.meta is not None)

        async with FsProvider(repo, version) as provider:
            top = await provider.children(provider.root())
            names = [n.name for n in top]
            assert len(names) == len(set(names))


async def test_replayed_fs_files_readable_when_repo_root_is_non_empty(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """Exercises a non-empty ``repo_root`` (via ``iter_layouts``, matching
    ``Session.discover()``'s real usage), unlike the tests above which root
    ``ReplayStore`` directly at ``@ActiveProtectVault`` (``repo_root == ""``)."""
    store = await record_target("fs_repo_root_apv1.json.gz")
    layout = await anext(layout async for layout in iter_layouts(store) if layout.repo_root)
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


__all__: list[str] = []
