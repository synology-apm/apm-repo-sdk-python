"""Unit tests for ``GotoChainWalker.expand_to_chain`` — driven directly
against a real ``Store``/``Tree`` pair wired the same minimal way
``UnitScreen`` itself wires them (a ``folder_tree_spec``/
``reconcile_and_restore_cursor`` subscription — ``Store.dispatch`` is
fully synchronous, draining every subscriber notification and command
before returning, so ``GotoChainWalker`` can rely on the tree already
reflecting a just-dispatched ``ChainStepResolved`` by the time it reads
it back)."""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.widgets import Tree

from synology_apm_repo.browser.core.unit.model import UnitModel
from synology_apm_repo.browser.core.unit.msg import FolderSelected, UnitMsg
from synology_apm_repo.browser.core.unit.select import folder_tree_spec
from synology_apm_repo.browser.core.unit.update import update
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens.goto_walker import GotoChainWalker
from synology_apm_repo.browser.view.reconcile import Binding, reconcile_and_restore_cursor, update_node
from synology_apm_repo.browser.widgets.fast_tree import FastLabelTree
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _container(name: str, ref: NodeRef | None = None) -> Node:
    return Node(ref=ref or NodeRef("repo", ("root", name)), name=name, is_leaf=False)


def _leaf(name: str, ref: NodeRef | None = None) -> Node:
    return Node(ref=ref or NodeRef("repo", ("root", name)), name=name, is_leaf=True)


class _FakeScreen:
    """The narrow surface (``unit_tree``, ``store``,
    ``_select_folder_ref``) ``GotoChainWalker`` reaches into as a private
    collaborator -- never a real ``UnitScreen``."""

    def __init__(self, tree: Tree[Binding[NodeRef]], store: Store[UnitModel, UnitMsg, object]) -> None:
        self.unit_tree = tree
        self.store = store
        self.selected: list[NodeRef] = []

    def _select_folder_ref(self, node: Node) -> None:
        self.selected.append(node.ref)
        self.store.dispatch(FolderSelected(ref=node.ref))


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        yield FastLabelTree[Binding[NodeRef]]("root")


def _make_screen(tree: Tree[Binding[NodeRef]], root: Node) -> _FakeScreen:
    store: Store[UnitModel, UnitMsg, object] = Store(UnitModel(root=root, selected=root.ref), update, lambda cmd: None)
    store.subscribe(folder_tree_spec, lambda spec: _render(tree, spec), init=True)
    return _FakeScreen(tree, store)


def _render(tree: Tree[Binding[NodeRef]], spec: object) -> None:
    # The exact two-line body FolderTreeView.render uses, so the tree
    # here is wired the same way UnitScreen itself wires it.
    if spec is None:
        tree.root.remove_children()
        return
    update_node(tree.root, spec)  # type: ignore[arg-type]
    reconcile_and_restore_cursor(tree, spec.children or ())  # type: ignore[attr-defined]


async def test_full_descent_lands_on_and_selects_the_target_folder() -> None:
    root = _container("root", NodeRef("repo", ("root",)))
    folder_a = _container("folder_a")
    folder_b = _container("folder_b")
    chain = [root, folder_a, folder_b]

    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(FastLabelTree)
        screen = _make_screen(tree, root)
        walker = GotoChainWalker(screen)  # type: ignore[arg-type]

        result = walker.expand_to_chain(chain, [[folder_a], [folder_b]])

        assert result is folder_b
        assert screen.selected == [folder_b.ref]
        assert screen.store.model.loaded[root.ref].children == (folder_a,)
        assert screen.store.model.loaded[folder_a.ref].children == (folder_b,)
        folder_a_node = next(c for c in tree.root.children if c.data is not None and c.data.key == folder_a.ref)
        assert folder_a_node.is_expanded
        folder_b_node = next(c for c in folder_a_node.children if c.data is not None and c.data.key == folder_b.ref)
        assert tree.cursor_node is folder_b_node


async def test_a_leaf_target_stops_descent_one_level_early() -> None:
    root = _container("root", NodeRef("repo", ("root",)))
    folder_a = _container("folder_a")
    target = _leaf("target")
    chain = [root, folder_a, target]

    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(FastLabelTree)
        screen = _make_screen(tree, root)
        walker = GotoChainWalker(screen)  # type: ignore[arg-type]

        result = walker.expand_to_chain(chain, [[folder_a], [target]])

        # The leaf itself is returned to the caller (to locate in the
        # file table), but never becomes a tree node or the selection --
        # its own parent, folder_a, is what gets selected/landed on.
        assert result is target
        assert screen.selected == [folder_a.ref]
        folder_a_node = next(c for c in tree.root.children if c.data is not None and c.data.key == folder_a.ref)
        assert folder_a_node.is_expanded
        assert tree.cursor_node is folder_a_node


async def test_find_chain_child_locates_the_right_sibling_among_several() -> None:
    root = _container("root", NodeRef("repo", ("root",)))
    siblings = [_container(f"child-{i}") for i in range(4)]
    target = siblings[2]

    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(FastLabelTree)
        screen = _make_screen(tree, root)
        walker = GotoChainWalker(screen)  # type: ignore[arg-type]

        result = walker.expand_to_chain([root, target], [siblings])

        assert result is target
        assert screen.selected == [target.ref]
