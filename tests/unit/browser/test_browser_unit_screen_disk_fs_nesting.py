"""Unit tests for ``UnitScreen``'s rendering of a disk-image node's
"(filesystem)" sibling — driven through a real Textual ``Pilot``
(``app.run_test()``), against a fake provider whose children mirror the
exact shape ``DeviceProvider`` builds for a dedup disk-image object with
disk-fs filesystem parsing available: every dedup disk-image object gets
one additional, purely-additive sibling node built by
``units.content.disk_fs`` when Dissect recognizes a filesystem inside it --
a container ``is_leaf=False`` "``<name>`` (filesystem)" sibling (tagged via
``DISK_FS_SIBLING_REF_ATTR``) immediately followed in the same
``children()`` page by its own plain, exportable ``is_leaf=True``
disk-image node (``disk_fs_containers_before_leaves()`` orders every real
provider's own listing this way).

The folder tree shows only containers, so the (leaf) disk-image node is
excluded from it entirely: it appears as an ordinary file-table row under
root instead, and the sibling renders as an ordinary top-level
folder-tree entry, under its own real name (the folder tree only ever
shows containers -- a plain leaf only ever appears as a ``FileRow`` in
its folder's file table, never as a tree entry). The pure selector-level proof of this lives in
``tests/unit/browser/test_browser_core_unit_select.py``; this file proves
the same shape end-to-end through a real screen/provider round-trip.
"""

from __future__ import annotations

from typing import Any, cast

from textual.app import App, ComposeResult
from textual.widgets import Tree

from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Repository, Session, Version
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
    """A bare ``App`` (not ``ApmRepoBrowserApp``) is enough here —
    duplicated rather than imported, since no test module imports another.
    ``self.repo_handle`` only needs to resolve to something non-``None`` —
    none of this file's tests exercise ``action_refresh``'s
    ``invalidate_directory_cache()`` call, the one thing ``_load_root``
    itself still reads off it."""

    def __init__(self, version: Version, catalog: _FakeCatalog) -> None:
        super().__init__()
        self.verbose = False
        self.jobs: dict[JobId, Job] = {}
        self.resources = ResourceTable(cast(Session, object()))
        self.repo_handle: RepoHandle | None = self.resources.put_repo(cast(Repository, object()))
        self._version = version
        self._catalog = catalog

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

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
        str(root_ref): [fs_node, image_node],
        str(fs_ref): [partition_node],
    }


async def test_disk_image_is_a_file_table_row_and_its_sibling_is_a_top_level_folder(
    wait_until: Any, sdk_timeout: float
) -> None:
    root, children_by_ref = _build_root_with_disk_image_and_fs_sibling()
    provider = _FakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=sdk_timeout, interval=0.05)

        # Only the (container) "(filesystem)" sibling is tree-shown -- the
        # (leaf) disk-image node is excluded entirely.
        assert len(tree.root.children) == 1
        fs_tree_node = tree.root.children[0]
        assert fs_tree_node.data is not None
        assert fs_tree_node.data.payload.name == "disk-1.img (filesystem)"
        assert fs_tree_node.data.payload.is_leaf is False
        assert fs_tree_node.allow_expand is True
        assert str(fs_tree_node.label) == "disk-1.img (filesystem)"  # its own real name, never relabeled

        # The disk-image leaf itself shows up as an ordinary file-table
        # row under root instead.
        await wait_until(pilot, lambda: len(screen._file_table._nodes) > 0, timeout=sdk_timeout, interval=0.05)
        names = {n.name for n in screen._file_table._nodes if n is not None}
        assert "disk-1.img" in names
        assert "disk-1.img (filesystem)" in names  # both files and subfolders list here


async def test_expanding_the_fs_sibling_reveals_its_own_nested_child(
    wait_until: Any, move_cursor_to: Any, sdk_timeout: float
) -> None:
    root, children_by_ref = _build_root_with_disk_image_and_fs_sibling()
    provider = _FakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=sdk_timeout, interval=0.05)
        fs_tree_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, fs_tree_node)
        await pilot.press("enter")
        await wait_until(pilot, lambda: len(fs_tree_node.children) > 0, timeout=sdk_timeout, interval=0.05)

        # A partition is is_leaf=False (kind=DISK_FILESYSTEM, same as
        # fs_node itself) in this fixture, so it stays tree-shown too --
        # unaffected either way by where fs_node's own TreeNode sits.
        assert len(fs_tree_node.children) == 1
        assert fs_tree_node.children[0].data is not None
        assert fs_tree_node.children[0].data.payload.name == "NTFS"
