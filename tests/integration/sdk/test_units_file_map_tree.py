"""Regression test for ``synology_apm_repo.sdk.units.file_map_tree``'s
fallback browsing axis, replayed from a committed fixture recorded against
real bytes.

Fixture: ``file_map_tree_vault_m365.json.gz``, recorded against
``vault-m365/@ActiveProtectVault``, whose ``copy_meta_file`` is empty: the
tree's top-level listing, a
first-child walk down to a leaf, and that leaf's first 4096 bytes. The
one test is its recording recipe.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout
from synology_apm_repo.sdk.units.file_map_tree import FileMapTreeProvider


async def test_replayed_vault_m365_is_still_browsable_via_file_map_tree(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: the leaf's first 4096 bytes are a structural
    # oracle (all-zero, sparse), never the object's own meaning.
    store = await record_target("file_map_tree_vault_m365.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))

    async with await DedupRepo.open(store, layout) as repo:
        provider = FileMapTreeProvider(repo)
        top = await provider.children(provider.root())
        # Every file_map path in this sample shares one top-level segment.
        assert [n.name for n in top] == ["LNJAQtstRVxciJWy"]

        # Each level's name is pinned: a sort-order regression would still
        # return a node, just a different one.
        node = top[0]
        expected_path = ["DTIQL713Wkjn", "1", "saas_obj"]
        for expected_name in expected_path:
            assert not node.is_leaf
            children = await provider.children(node)
            assert [c.name for c in children] == [expected_name]
            node = children[0]
        assert node.is_leaf
        assert node.name == "saas_obj"

        content = (await provider.unit(node)).content
        assert content.size == 117236076544
        data = await content.read(0, 4096)
        assert data == b"\x00" * 4096
