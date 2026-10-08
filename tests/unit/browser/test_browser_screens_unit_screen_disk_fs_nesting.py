"""``Pilot`` tests for how ``UnitScreen`` renders a disk image's
``"(filesystem)"`` sibling, against a fake provider shaped like
``DeviceProvider``'s listing: a container ``"<name> (filesystem)"`` node
followed by its plain, exportable disk-image leaf
(``disk_fs_containers_before_leaves()``'s order).

The folder tree shows only containers, so the sibling is a top-level tree
entry under its own name and the disk image is a file-table row under root.
"""

from __future__ import annotations

from textual.widgets import Tree

from support.model_factories import make_version
from support.pilot import SDK_TIMEOUT, move_cursor_to, wait_until
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.units.base import Node, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    FakeApp,
    FakeRepo,
    PagedTreeProvider,
)


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
    )

    partition_ref = fs_ref.child("p0")
    partition_node = Node(ref=partition_ref, name="NTFS", is_leaf=False, kind=UnitKind.DISK_FILESYSTEM)

    return root, {
        str(root_ref): [fs_node, image_node],
        str(fs_ref): [partition_node],
    }


async def test_disk_image_is_a_file_table_row_and_its_sibling_is_a_top_level_folder() -> None:
    root, children_by_ref = _build_root_with_disk_image_and_fs_sibling()
    provider = PagedTreeProvider(root, children_by_ref)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=SDK_TIMEOUT, interval=0.05)

        assert len(tree.root.children) == 1
        fs_tree_node = tree.root.children[0]
        assert fs_tree_node.data is not None
        assert fs_tree_node.data.payload.name == "disk-1.img (filesystem)"
        assert fs_tree_node.data.payload.is_leaf is False
        assert fs_tree_node.allow_expand is True
        assert str(fs_tree_node.label) == "disk-1.img (filesystem)"  # not relabeled

        await wait_until(pilot, lambda: len(screen._file_table._nodes) > 0, timeout=SDK_TIMEOUT, interval=0.05)
        names = {n.name for n in screen._file_table._nodes if n is not None}
        assert "disk-1.img" in names
        assert "disk-1.img (filesystem)" in names  # both files and subfolders list here


async def test_expanding_the_fs_sibling_reveals_its_own_nested_child() -> None:
    root, children_by_ref = _build_root_with_disk_image_and_fs_sibling()
    provider = PagedTreeProvider(root, children_by_ref)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=SDK_TIMEOUT, interval=0.05)
        fs_tree_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, fs_tree_node)
        await pilot.press("enter")
        await wait_until(pilot, lambda: len(fs_tree_node.children) > 0, timeout=SDK_TIMEOUT, interval=0.05)

        # A partition is a container, so it is tree-shown too.
        assert len(fs_tree_node.children) == 1
        assert fs_tree_node.children[0].data is not None
        assert fs_tree_node.children[0].data.payload.name == "NTFS"
