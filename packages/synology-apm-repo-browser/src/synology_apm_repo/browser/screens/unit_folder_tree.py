"""``FolderTreeView``: ``UnitScreen``'s folder ``Tree`` -- rendering,
focus/auto-expand and node lookup. It reaches the screen only through
``unit_tree``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.view.reconcile import (
    Binding,
    NodeSpec,
    reconcile_and_restore_cursor,
    update_node,
)
from synology_apm_repo.sdk import Node, NodeRef

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen


class FolderTreeView:
    def __init__(self, screen: UnitScreen) -> None:
        self._screen = screen

    def render(self, spec: NodeSpec[NodeRef] | None) -> None:
        """The ``folder_tree_spec`` subscription's render: reconciles the
        tree, root included."""
        tree = self._screen.unit_tree
        if spec is None:
            # A reset is in flight: clear the stale tree.
            tree.root.remove_children()
            return
        update_node(tree.root, spec)
        reconcile_and_restore_cursor(tree, spec.children or ())

    def focus_and_maybe_autoexpand(self, root: Node, *, skip_autoexpand: bool) -> None:
        """Focuses the tree and expands a container root, unless
        ``skip_autoexpand`` (a pending goto's ``Landing`` expands it)."""
        tree = self._screen.unit_tree
        tree.focus()
        if not root.is_leaf and not skip_autoexpand:
            tree.root.expand()

    def node_of(self, tree_node: TreeNode[Binding[NodeRef]] | None) -> Node | None:
        """The ``Node`` a reconciled ``TreeNode`` shows; ``None`` for no
        node or the error leaf."""
        if tree_node is None or tree_node.data is None:
            return None
        return cast(Node, tree_node.data.payload)

    def expand_ancestors(self, tree_node: TreeNode[Binding[NodeRef]]) -> None:
        """Expands every ancestor of ``tree_node``, root included, as
        ``move_cursor_keyed`` requires (``find_node`` also finds nodes under
        collapsed ancestors)."""
        ancestor = tree_node.parent
        while ancestor is not None:
            if not ancestor.is_expanded:
                ancestor.expand()
            ancestor = ancestor.parent
