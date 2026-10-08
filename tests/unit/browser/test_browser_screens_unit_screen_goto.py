"""UnitScreen goto-ref: version lookup, switching versions, and walking to a target (found, or failing to load)."""

from __future__ import annotations

import dataclasses

import pytest
from textual.widgets import Tree

from support.model_factories import make_version
from support.pilot import wait_until
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.browser.view.reconcile import find_node
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    ConfigurableProvider,
    FakeApp,
    FakeRepo,
)


async def test_submit_goto_version_lookup_failure_notifies(monkeypatch: pytest.MonkeyPatch) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {})
    repo = FakeRepo(provider, version_for_ref_error=ApmRepoError("unknown version"))
    app = FakeApp(make_version(), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        other_ref = NodeRef.canonical(
            "repo",
            catalog_id=CatalogId("catalog-1"),
            workload_id=WorkloadId(1),
            version_uid=VersionUid("some-other-version"),
        )
        screen._submit_goto(str(other_ref))
        await wait_until(pilot, lambda: bool(warnings))
        assert warnings == ["unknown version"]


async def test_submit_goto_a_different_version_pushes_a_new_unit_screen() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {})
    other_version = dataclasses.replace(
        make_version(), version_uid=VersionUid("some-other-version"), version_id=VersionId(2)
    )
    repo = FakeRepo(provider, version_for_ref_result=other_version)
    app = FakeApp(make_version(), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        other_ref = NodeRef.canonical(
            "repo",
            catalog_id=CatalogId("catalog-1"),
            workload_id=WorkloadId(1),
            version_uid=VersionUid("some-other-version"),
        )
        screen._submit_goto(str(other_ref))
        await wait_until(pilot, lambda: app.screen is not screen)
        assert isinstance(app.screen, UnitScreen)


async def test_goto_children_error_notifies_and_expands_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A provider failure during the walk reaches the user as ``GotoFailed``'s warning."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    target_ref = NodeRef("repo", ("root", "target"))
    provider = ConfigurableProvider(root, {}, raise_children_for={str(root_ref)})
    warnings: list[str] = []
    monkeypatch.setattr(UnitScreen, "notify", lambda self, message, **kwargs: warnings.append(message))
    app = FakeApp(make_version(), FakeRepo(provider), target_ref=target_ref)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: bool(warnings) and tree.root.is_expanded)
        assert warnings == [f"boom at {root_ref}"]
        assert tree.root.is_expanded


async def test_goto_a_disk_fs_sibling_finds_it_as_a_top_level_folder() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    image_ref = NodeRef("repo", ("object", "5"))
    image_node = Node(ref=image_ref, name="disk-1.img", is_leaf=True)
    fs_ref = image_ref.child("fs")
    fs_node = Node(ref=fs_ref, name="disk-1.img (filesystem)", is_leaf=False)
    provider = ConfigurableProvider(root, {str(root_ref): [image_node, fs_node]})
    app = FakeApp(make_version(), FakeRepo(provider), target_ref=fs_ref)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)

        assert len(tree.root.children) == 1  # only the (container) fs sibling is tree-shown
        fs_tree_node = tree.root.children[0]
        assert fs_tree_node.data is not None
        assert fs_tree_node.data.key == fs_ref
        await wait_until(pilot, lambda: tree.cursor_node is fs_tree_node)  # goto landed the cursor on it
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        assert screen.store.model.selected == fs_ref  # the target folder itself is now selected


async def test_goto_a_leaf_expands_its_parent_folder_and_focuses_its_row() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=root_ref.child("docs"), name="docs", is_leaf=False)
    subfolder = Node(ref=folder.ref.child("old"), name="old", is_leaf=False)
    leaf = Node(ref=folder.ref.child("a.txt"), name="a.txt", is_leaf=True)
    provider = ConfigurableProvider(root, {str(root_ref): [folder], str(folder.ref): [subfolder, leaf]})
    app = FakeApp(make_version(), FakeRepo(provider), target_ref=leaf.ref)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: screen.store.model.landing is not None)
        folder_node = find_node(tree.root, folder.ref)
        assert folder_node is not None
        await wait_until(pilot, lambda: tree.cursor_node is folder_node)

        assert folder_node.is_expanded
        assert [child.data.key for child in folder_node.children if child.data is not None] == [subfolder.ref]
        await wait_until(pilot, lambda: screen.focused is screen.file_table)
