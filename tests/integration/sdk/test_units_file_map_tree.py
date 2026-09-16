"""Regression test for ``synology_apm_repo.sdk.units.file_map_tree``'s
fallback browsing axis: apv-sample-3's browsable-despite-empty-
``copy_meta_file`` scenario, replayed from a committed fixture recorded
against real bytes, with **no external dependency** -- this always runs,
on CI or anywhere else, because it goes through ``ReplayStore`` instead of
a real ``LocalFsStore``.

The fixture (``tests/fixtures/file_map_tree_apv3.json.gz``) was
produced against a real store rooted at ``apv-sample-3/@ActiveProtectVault``
(a repository whose ``copy_meta_file`` is empty, browsable and exportable only
through this fallback axis) -- see ``tests/CLAUDE.md``'s "Recording a
fixture" section for the ``pytest --record-against=...``/``make
record-fixture`` workflow that (re-)records it. It records the tree's
top-level listing, a walk down the first child at each level to a leaf,
then just that leaf's first 4096 bytes -- never a full read of the much
larger logical object that leaf actually is.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.file_map_tree import FileMapTreeProvider


async def test_replayed_apv_sample_3_is_still_browsable_via_file_map_tree(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: reads a sparse object's first 4096 bytes as a
    # structural oracle (asserts they're all-zero, never the object's own
    # meaning) -- see the module docstring.
    store = await record_target("file_map_tree_apv3.json.gz", allow_content=True)
    layout = await detect_layout(store)

    async with await DedupRepo.open(store, layout) as repo:
        provider = FileMapTreeProvider(repo)
        top = await provider.children(provider.root())
        # This fixture's recorded tree has exactly one top-level fallback
        # bucket, keyed by the object's ``file_map`` id.
        assert [n.name for n in top] == ["LNJAQtstRVxciJWy"]

        # Drill down through the recorded path, pinning each level's name --
        # a sorted-order regression would still return something non-empty
        # but a different node.
        node = top[0]
        expected_path = ["DTIQL713Wkjn", "1", "saas_obj"]
        for expected_name in expected_path:
            assert not node.is_leaf
            children = await provider.children(node)
            assert [c.name for c in children] == [expected_name]
            node = children[0]
        assert node.is_leaf
        assert node.name == "saas_obj"

        content = (await provider.unit(node)).open()
        # This real (recorded) object's exact size -- sparse/zero-filled at
        # its first 4096 bytes, which is what the fixture actually recorded
        # a chunk read for.
        assert content.size == 117236076544
        data = await content.read(0, 4096)
        assert data == b"\x00" * 4096


__all__: list[str] = []
