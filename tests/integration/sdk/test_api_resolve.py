"""Regression tests for ``Session.resolve``, replayed from a committed
fixture recorded against real bytes.

Fixture: ``api_resolve_vault_plain_walk.json.gz``, recorded against
``vault-plain``. No test's calls are a superset of the others', so
recording needs the whole file run together.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.identifiers import CatalogId, VersionUid, WorkloadId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import RestorableUnit, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef, disambiguate

_WINDOWS_VM_WORKLOAD_ID = 2


async def test_replayed_resolve_canonical_ref_to_a_real_vm_disk_image(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        # allow_content=True: only the MBR/GPT signature bytes are read.
        store = await record_target("api_resolve_vault_plain_walk.json.gz", allow_content=True)
        [repo] = await session.open(store)
        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        catalog, vm = next((c, w) for c, w in workload_pairs if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
        version = next(v for v in await catalog.versions(vm) if v.meta is not None)

        provider = await catalog.provider(version)
        device = (await provider.children(provider.root()))[0]
        objects = await provider.children(device)
        disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)

        resolved_str = await (await session.resolve(str(disk.ref))).unit()
        resolved_obj = await (await session.resolve(disk.ref)).unit()
        for resolved in (resolved_str, resolved_obj):
            assert resolved.name == disk.name
            assert isinstance(resolved, RestorableUnit)
            content = resolved.content
            header = await content.read(0, 520)
            assert header[510:512] == bytes.fromhex("55aa")
            assert header[512:520] == b"EFI PART"


async def test_replayed_resolve_raw_ref_to_a_real_file_map_leaf(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        store = await record_target("api_resolve_vault_plain_walk.json.gz")
        [repo] = await session.open(store)
        tree = await repo.file_map_tree()
        node = tree.root()
        path = []
        while not node.is_leaf:
            children = await tree.children(node)
            assert children, "expected at least one leaf under the file_map tree"
            node = children[0]
            path.append(node.name)

        assert path == ["1fd2d9bd-faab-4b26-a610-109ccb5a093e", "50", "dedup.img"]
        assert str(node.ref) == "@ActiveProtectVault#raw/1fd2d9bd-faab-4b26-a610-109ccb5a093e/50/dedup.img"

        resolved = await (await session.resolve(str(node.ref))).unit()
        assert resolved.name == "dedup.img"
        assert isinstance(resolved, RestorableUnit)
        assert resolved.content.size == 893317120


async def test_replayed_resolve_human_ref_down_to_a_real_device_disk(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        store = await record_target("api_resolve_vault_plain_walk.json.gz")
        [repo] = await session.open(store)
        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        catalog, vm = next((c, w) for c, w in workload_pairs if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
        version = next(v for v in await catalog.versions(vm) if v.meta is not None)

        provider = await catalog.provider(version)
        device = (await provider.children(provider.root()))[0]
        objects = await provider.children(device)
        disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)

        versions_list = await catalog.versions(vm)
        disambiguated = disambiguate([(v.display_name, v.version_uid) for v in versions_list])
        version_display = next(
            name for name, v in zip(disambiguated, versions_list, strict=True) if v.version_uid == version.version_uid
        )

        human_ref = NodeRef.human(
            repo.layout.repo_root, catalog.display_name, vm.display_name, version_display, device.name, disk.name
        )
        resolved = await (await session.resolve(str(human_ref))).unit()
        assert resolved.name == disk.name


async def test_replayed_resolve_nested_drive_item_by_flat_item_id(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """Unlike the other providers, which add one ref segment per tree
    level, Drive addresses every item by a single flat ``item_id``; checked
    on a real item inside a top-level folder."""
    async with api.Session() as session:
        # allow_content=True: catalog.provider() reads the object-name
        # index, an internal routing table, through dedup_file.read().
        store = await record_target("api_resolve_vault_plain_walk.json.gz", allow_content=True)
        [repo] = await session.open(store)
        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        drive_versions = [
            (c, v) for c, w in workload_pairs if w.sub_type == "DRIVE" for v in await c.versions(w) if not v.deleted
        ]
        for catalog, version in drive_versions:
            provider = await catalog.provider(version)
            top = await provider.children(provider.root())
            folder = next((n for n in top if not n.is_leaf), None)
            if folder is None:
                continue
            nested = await provider.children(folder)
            if not nested:
                continue
            target = nested[0]
            assert len(target.ref.extra_segments) == 1  # the flat-id convention itself
            resolved = (await session.resolve(str(target.ref))).node
            assert resolved is not None
            assert resolved.name == target.name
            return
        raise AssertionError("expected at least one real Drive version with a nested folder item")


async def test_replayed_resolve_unknown_human_segment_raises_not_found(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        store = await record_target("api_resolve_vault_plain_walk.json.gz")
        [repo] = await session.open(store)
        with pytest.raises(NotFoundError, match="no backup source named"):
            await session.resolve(f"{repo.layout.repo_root}#does-not-exist/foo/bar")


async def test_replayed_resolve_ref_from_unopened_repo_raises_not_found(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        store = await record_target("api_resolve_vault_plain_walk.json.gz")
        await session.open(store)
        bogus = NodeRef.canonical(
            "some/other/repo_root",
            catalog_id=CatalogId("1"),
            workload_id=WorkloadId(1),
            version_uid=VersionUid("x"),
        )
        with pytest.raises(NotFoundError, match="no open repository matches ref"):
            await session.resolve(str(bogus))
