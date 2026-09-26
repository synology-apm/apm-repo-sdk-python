"""``FolderTreeView``: owns ``UnitScreen``'s folder ``Tree`` widget --
rendering, focus/auto-expand, and node lookup. Held by ``UnitScreen`` as a
private collaborator, reaching back into it only through the small
surface every ``Screen`` already exposes (``unit_tree``).

The ``@work``-decorated goto-ref walk that *drives* this tree stays on
``UnitScreen`` itself, since Textual's ``@work`` requires a ``DOMNode``
``self``, which this plain collaborator isn't.
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
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen


class FolderTreeView:
    def __init__(self, screen: UnitScreen) -> None:
        self._screen = screen

    def render(self, spec: NodeSpec[NodeRef] | None) -> None:
        """The one place ``UnitModel`` -> real folder ``Tree`` widget
        happens, via ``core/unit/select.py``'s ``folder_tree_spec``
        selector and ``view/reconcile.py``'s
        ``reconcile_and_restore_cursor``."""
        tree = self._screen.unit_tree
        if spec is None:
            # A reset (refresh/verbose-toggle) discarded the old root
            # without a new one loaded yet -- clear rather than leave
            # stale content on screen.
            tree.root.remove_children()
            return
        update_node(tree.root, spec)
        reconcile_and_restore_cursor(tree, spec.children or ())

    def focus_and_maybe_autoexpand(self, root: Node, *, skip_autoexpand: bool) -> None:
        """Focuses the tree and, unless ``skip_autoexpand`` (a ``g`` jump
        about to walk this same root, which would otherwise race
        ``GotoChainWalker.expand_to_chain``'s own call), expands the root
        when it's a real container."""
        tree = self._screen.unit_tree
        tree.focus()
        if not root.is_leaf and not skip_autoexpand:
            tree.root.expand()

    def ensure_root_expanded(self) -> None:
        tree = self._screen.unit_tree
        root_node = self.node_of(tree.root)
        if root_node is not None and not root_node.is_leaf and not tree.root.is_expanded:
            tree.root.expand()

    def node_of(self, tree_node: TreeNode[Binding[NodeRef]] | None) -> Node | None:
        """Unwraps a reconciled ``TreeNode``'s ``Binding.payload`` back
        into the real ``Node`` it represents -- ``None`` both when nothing
        is selected and for the synthetic error leaf (``payload`` is
        ``None`` by construction)."""
        if tree_node is None or tree_node.data is None:
            return None
        return cast(Node, tree_node.data.payload)

    def expand_ancestors(self, tree_node: TreeNode[Binding[NodeRef]]) -> None:
        """Expands every one of ``tree_node``'s ancestors, root included --
        ``move_cursor_keyed``'s precondition (every ancestor expanded)
        doesn't hold just because ``tree_node`` was found: ``find_node``
        walks regardless of collapsed ancestors, and ``Tree.move_cursor``
        silently no-ops on an unreachable node rather than raising."""
        ancestor = tree_node.parent
        while ancestor is not None:
            if not ancestor.is_expanded:
                ancestor.expand()
            ancestor = ancestor.parent
