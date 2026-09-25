"""``GotoChainWalker``: expands ``UnitScreen``'s folder tree onto a
goto-ref (``g``) target's already-resolved provider chain -- dispatching
each step's own exhaustive sibling list into the screen's own ``Store``
and landing the cursor on the deepest folder reached. Held by ``UnitScreen``
as a private collaborator, reaching back into it only through the small
public surface it exposes for this (``unit_tree``, ``store``,
``_select_folder_ref``) -- the same convention ``units/device_pcps.py``/
``units/device_disk_fs.py`` establish on the SDK side for a collaborator
reaching back into the class that owns it.

Each step's own dispatch (``ChainStepResolved``) unconditionally
replaces whatever that node's own ``model.loaded`` entry was --
whether nothing yet (an ordinary lazy level) or only a partial page
from earlier browsing -- with the full, exhaustive list
``find_path_with_children`` already fetched, so there's no separate
"is this already loaded" branch to maintain here. ``Store.dispatch``
is fully synchronous (drains,
notifies every subscriber, performs every command, all before
returning), so by the time each dispatch call below returns, the tree
widget already reflects the new children via ``FolderTreeView.render``'s
own ``reconcile_and_restore_cursor`` call -- this walk can rely on that
ordering without waiting on anything itself.

The folder tree only ever shows containers -- a plain leaf, or a
SharePoint List-overview group's own items, never appear there, only
ever in a file table row -- so a goto target that's a leaf needs special
handling: descent stops one level early, at the leaf's parent folder,
leaving the leaf itself for the caller to locate in the file table."""

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

        Stops tree-descent one level early whenever the *next* chain
        node is a leaf -- it was never going to be tree-shown to begin
        with, so there's nothing to descend onto for it. At that point
        the current (parent) folder is selected via
        ``UnitScreen._select_folder_ref`` and its tree cursor parked --
        the target itself is left for the caller to locate in the file
        table. A SharePoint List-overview group is *not* a leaf and
        stays tree-descendable like an ordinary folder; only its own
        items are never tree-navigable, not the group node itself.

        When the walk instead runs to completion (the target itself is a
        folder, reached by full descent), the target's own ref is also
        selected before returning -- not just its tree cursor parked --
        so the file table shows the target folder's own contents rather
        than whatever was selected before this walk started."""
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
                # its own is_leaf gets checked. Descent stops here: the
                # current (parent) folder is selected and its tree cursor
                # parked, leaving the leaf target for the caller to locate
                # in the file table.
                self._land_cursor(tree, tree_node, chain[i])
                return next_node
            tree_node = self._find_chain_child(tree_node, next_node.ref)
        self._land_cursor(tree, tree_node, chain[-1])
        return chain[-1]

    def _land_cursor(self, tree: Tree[Binding[NodeRef]], tree_node: TreeNode[Binding[NodeRef]], node: Node) -> None:
        """Parks ``tree``'s own cursor on ``tree_node`` and selects
        ``node``'s own ref -- shared by both of ``expand_to_chain``'s own
        landing points (the early-stop-on-leaf branch, where ``node`` is
        the leaf target's own parent; the full-descent-to-a-folder
        branch, where it's the target itself). A no-op on the selection
        half when ``node`` is a SharePoint List-overview group -- such a
        group never has real file-table contents of its own, so
        ``_select_folder_ref`` skips pointing the file table at a level
        that can never be loaded."""
        self._screen._select_folder_ref(node)  # noqa: SLF001 - this class reaches back into UnitScreen only through its unit_tree/store/_select_folder_ref surface
        # A freshly .add()ed/.expand()ed node's own ._line is still its
        # never-updated constructor default of -1 until Textual's lazy
        # line-cache rebuilds; move_cursor()/scroll_to_node() read
        # ._line directly, so without forcing the rebuild first the
        # cursor would silently land on the root instead of tree_node.
        force_tree_line_cache(tree)
        tree.move_cursor(tree_node)
        tree.scroll_to_node(tree_node)

    @staticmethod
    def _find_chain_child(tree_node: TreeNode[Binding[NodeRef]], ref: NodeRef) -> TreeNode[Binding[NodeRef]]:
        """A goto-ref chain step's target is always a direct child of
        ``tree_node`` -- the folder tree only ever shows containers one
        level at a time, since a plain leaf or a SharePoint List-overview
        group's own items surface only as file-table rows, never as
        deeper tree nodes."""
        for child in tree_node.children:
            if child.data is not None and child.data.key == ref:
                return child
        raise StopIteration  # pragma: no cover - defensive: ref always came from a just-reconciled child
