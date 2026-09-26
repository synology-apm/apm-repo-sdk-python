"""``GotoChainWalker``: expands ``UnitScreen``'s folder tree onto a
goto-ref (``g``) target's already-resolved provider chain, dispatching
each step's own exhaustive sibling list into the screen's ``Store`` and
landing the cursor on the deepest folder reached. Held by ``UnitScreen``
as a private collaborator, reaching back into it only through
``unit_tree``/``store``/``_select_folder_ref`` -- the same convention
``units/device_pcps.py``/``units/device_disk_fs.py`` use on the SDK side.

Each step's dispatch (``ChainStepResolved``) unconditionally replaces
whatever that node's own ``model.loaded`` entry was with the full list
``find_path_with_children`` already fetched. ``Store.dispatch`` is
fully synchronous, so the tree widget already reflects the new children
by the time each dispatch call below returns.

The folder tree only ever shows containers -- a leaf or a SharePoint
List-overview group's own items never appear there, only in a file
table row -- so a goto target that's a leaf stops descent one level
early, at its parent folder, leaving the leaf for the caller to locate
in the file table."""

from __future__ import annotations

from typing import TYPE_CHECKING

from synology_apm_repo.browser.core.unit.msg import ChainStepResolved
from synology_apm_repo.browser.view.reconcile import Binding, force_tree_line_cache
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

if TYPE_CHECKING:
    from textual.widgets import Tree
    from textual.widgets.tree import TreeNode

    from synology_apm_repo.browser.screens.unit_screen import UnitScreen


class GotoChainWalker:
    def __init__(self, screen: UnitScreen) -> None:
        self._screen = screen

    def expand_to_chain(self, chain: list[Node], children_by_step: list[list[Node]]) -> Node:
        """Expands/loads every step of ``chain`` onto the screen's folder
        tree and returns the target ``Node`` (``chain``'s own last
        element).

        Stops descent one level early whenever the *next* chain node is
        a leaf -- the current (parent) folder is selected and its
        cursor parked, the target left for the caller to locate in the
        file table. A SharePoint List-overview group is not a leaf and
        stays tree-descendable; only its own items are never
        tree-navigable.

        When the walk runs to completion (the target is a folder), the
        target's own ref is also selected before returning, so the file
        table shows the target folder's contents rather than whatever
        was selected before."""
        screen = self._screen
        tree = screen.unit_tree
        tree_node = tree.root
        for i, children in enumerate(children_by_step):
            step_ref = chain[i].ref
            screen.store.dispatch(ChainStepResolved(ref=step_ref, children=tuple(children)))
            if not tree_node.is_expanded:
                tree_node.expand()
            next_node = chain[i + 1]
            if next_node.is_leaf:
                # next_node is the target itself -- children_by_step's own
                # contract never includes it, so this is the only place
                # its own is_leaf gets checked.
                self._land_cursor(tree, tree_node, chain[i])
                return next_node
            tree_node = self._find_chain_child(tree_node, next_node.ref)
        self._land_cursor(tree, tree_node, chain[-1])
        return chain[-1]

    def _land_cursor(self, tree: Tree[Binding[NodeRef]], tree_node: TreeNode[Binding[NodeRef]], node: Node) -> None:
        """Parks ``tree``'s cursor on ``tree_node`` and selects ``node``'s
        ref -- shared by both of ``expand_to_chain``'s landing points. A
        no-op on the selection half for a SharePoint List-overview
        group, which never has real file-table contents of its own."""
        self._screen._select_folder_ref(node)  # noqa: SLF001 - reaches back into UnitScreen only through unit_tree/store/_select_folder_ref
        force_tree_line_cache(tree)  # forces the rebuild a freshly expanded node needs -- see reconcile.py
        tree.move_cursor(tree_node)
        tree.scroll_to_node(tree_node)

    @staticmethod
    def _find_chain_child(tree_node: TreeNode[Binding[NodeRef]], ref: NodeRef) -> TreeNode[Binding[NodeRef]]:
        """A goto-ref chain step's target is always a direct child of
        ``tree_node`` -- the folder tree only shows containers one level
        at a time; a leaf or List-overview group's items surface only as
        file-table rows."""
        for child in tree_node.children:
            if child.data is not None and child.data.key == ref:
                return child
        raise StopIteration  # pragma: no cover - defensive: ref always came from a just-reconciled child
