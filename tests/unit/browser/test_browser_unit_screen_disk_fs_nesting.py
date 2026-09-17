"""Unit tests for ``UnitScreen``'s tree rendering of a disk-image node's
"(filesystem)" sibling — driven through a real Textual ``Pilot``
(``app.run_test()``), against a fake provider whose children mirror the
exact shape ``DeviceProvider`` builds for a dedup disk-image object with
disk-fs filesystem parsing available (see that module's own docstring): a
plain, exportable ``is_leaf=True`` disk-image node immediately followed
in the same ``children()`` page by its ``is_leaf=False`` "``<name>``
(filesystem)" sibling, tagged via ``disk_fs_sibling_ref``.
"""

from __future__ import annotations

from typing import Any

from textual.app import App, ComposeResult
from textual.widgets import Tree

from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Version
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.units.base import Node, UnitKind
from synology_apm_repo.sdk.units.device_disk_fs import DISK_FS_SIBLING_REF_ATTR
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _version() -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid-1"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type="VM",
        target_id=TargetId("target"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


class _FakeProvider:
    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]]) -> None:
        self._root = root
        self._children_by_ref = children_by_ref

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        all_children = self._children_by_ref.get(str(node.ref), [])
        stop = offset + limit if limit is not None else None
        return all_children[offset:stop]

    async def unit(self, node: Node) -> Node:
        return node


class _FakeCatalog:
    def __init__(self, provider: _FakeProvider) -> None:
        self._provider = provider

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _FakeProvider:
        return self._provider


class _FakeApp(App[None]):
    """See ``test_browser_unit_screen_pagination.py``'s own identical
    class for why a bare ``App`` (not ``ApmRepoBrowserApp``) is enough
    here — duplicated rather than imported (no test module imports
    another, see ``tests/CLAUDE.md``). ``self.repo`` only needs to be
    non-``None`` — none of this file's tests exercise ``action_refresh``'s
    ``invalidate_directory_cache()`` call, the one thing ``_load_root``
    itself still reads off it."""

    def __init__(self, version: Version, catalog: _FakeCatalog) -> None:
        super().__init__()
        self.repo = object()
        self.verbose = False
        self._version = version
        self._catalog = catalog

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(UnitScreen(self._catalog, self._version))  # type: ignore[arg-type]


def _build_root_with_disk_image_and_fs_sibling() -> tuple[Node, dict[str, list[Node]]]:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)

    image_ref = NodeRef("repo", ("object", "5"))
    image_node = Node(ref=image_ref, name="disk-1.img", is_leaf=True, kind=UnitKind.DISK_IMAGE)

    fs_ref = image_ref.child("fs")
    fs_node = Node(
        ref=fs_ref,
        name="disk-1.img (filesystem)",
        is_leaf=False,
        kind=UnitKind.DISK_FILESYSTEM,
        attrs={DISK_FS_SIBLING_REF_ATTR: image_ref},
    )

    partition_ref = fs_ref.child("p0")
    partition_node = Node(ref=partition_ref, name="NTFS", is_leaf=False, kind=UnitKind.DISK_FILESYSTEM)

    return root, {
        str(root_ref): [image_node, fs_node],
        str(fs_ref): [partition_node],
    }


async def test_filesystem_sibling_nests_under_the_disk_image_node_not_beside_it(
    wait_until: Any, move_cursor_to: Any
) -> None:
    root, children_by_ref = _build_root_with_disk_image_and_fs_sibling()
    provider = _FakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=1.5, interval=0.05)

        # Only the disk-image node is a direct child of root -- its
        # "(filesystem)" sibling isn't a second, same-level entry.
        assert len(tree.root.children) == 1
        image_tree_node = tree.root.children[0]
        assert image_tree_node.data is not None
        assert image_tree_node.data.name == "disk-1.img"
        assert image_tree_node.data.is_leaf is True  # still exportable, unchanged
        assert image_tree_node.allow_expand is True  # now expandable, unlike a plain leaf

        # The sibling is nested one level under it instead.
        assert len(image_tree_node.children) == 1
        fs_tree_node = image_tree_node.children[0]
        assert fs_tree_node.data is not None
        assert fs_tree_node.data.name == "disk-1.img (filesystem)"
        assert fs_tree_node.data.is_leaf is False
        # The widget's own label drops the now-redundant "disk-1.img"
        # prefix nesting already conveys — ``data.name`` above stays the
        # real Node the detail pane/export/filter/CLI all still use.
        assert str(fs_tree_node.label) == "Filesystem"


async def test_expanding_the_disk_image_node_reveals_the_nested_filesystem_child(
    wait_until: Any, move_cursor_to: Any
) -> None:
    root, children_by_ref = _build_root_with_disk_image_and_fs_sibling()
    provider = _FakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=1.5, interval=0.05)
        image_tree_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, image_tree_node)
        await pilot.press("enter")
        await wait_until(pilot, lambda: image_tree_node.is_expanded, timeout=1.5, interval=0.05)
        assert image_tree_node.is_expanded

        fs_tree_node = image_tree_node.children[0]
        await move_cursor_to(pilot, tree, fs_tree_node)
        await pilot.press("enter")
        await wait_until(pilot, lambda: len(fs_tree_node.children) > 0, timeout=1.5, interval=0.05)

        # Expanding the nested sibling still browses its own real
        # children (a partition, here) via the provider, unaffected by
        # where its own TreeNode sits in the widget tree.
        assert len(fs_tree_node.children) == 1
        assert fs_tree_node.children[0].data is not None
        assert fs_tree_node.children[0].data.name == "NTFS"
