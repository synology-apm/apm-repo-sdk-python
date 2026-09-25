"""``FolderTreeView``: owns ``UnitScreen``'s folder ``Tree`` widget --
rendering, focus/auto-expand, and node lookup. Held by ``UnitScreen`` as a
private collaborator, reaching back into it only through the small
surface every ``Screen`` already exposes for this (``unit_tree``) — the
same convention ``GotoChainWalker``/``DetailPane`` establish for a
``UnitScreen`` collaborator.

The ``@work``-decorated goto-ref walk that *drives* this tree (deciding
when to expand to a target chain) stays on ``UnitScreen`` itself: Textual's
``@work`` requires a ``DOMNode`` ``self``, which this plain collaborator
isn't. This class owns only the tree-widget mechanics those workers (and
the screen's own message handlers) call into.
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
        happens -- every dispatch that changes anything about the tree's
        own shape (a root load, a children fetch landing anywhere in it, a
        filter keystroke, a goto-ref chain step) re-renders through here,
        via ``core/unit/select.py``'s own ``folder_tree_spec`` selector and
        ``view/reconcile.py``'s ``reconcile_and_restore_cursor`` (keyed
        ``reconcile_children`` plus locating the cursor's own surviving
        domain key and moving the cursor back onto it explicitly --
        keeping a ``TreeNode`` alive doesn't keep the line-position-
        derived cursor on it, which every one of this package's
        tree-rendering methods needs)."""
        tree = self._screen.unit_tree
        if spec is None:
            # A reset (refresh/verbose-toggle) discarded the old root
            # without a new one having loaded yet -- clear rather than
            # leave stale content on screen for however long the new
            # fetch takes.
            tree.root.remove_children()
            return
        update_node(tree.root, spec)
        reconcile_and_restore_cursor(tree, spec.children or ())

    def focus_and_maybe_autoexpand(self, root: Node, *, skip_autoexpand: bool) -> None:
        """Focuses the tree and, unless ``skip_autoexpand`` (a ``g`` jump
        about to walk this same root itself -- auto-expanding here would
        populate the root's own children independently of, and racing
        with, ``GotoChainWalker.expand_to_chain``'s own call for the
        same level), expands the root when it's a real container."""
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
        """Unwraps a reconciled ``TreeNode``'s own ``Binding.payload`` back
        into the real ``Node`` it represents — ``None`` both when nothing is
        selected and for the one node with no real ``Node`` behind it at all
        (the synthetic error leaf ``select.py``'s ``error_leaf_ref`` builds,
        whose own ``payload`` is ``None`` by construction)."""
        if tree_node is None or tree_node.data is None:
            return None
        return cast(Node, tree_node.data.payload)

    def expand_ancestors(self, tree_node: TreeNode[Binding[NodeRef]]) -> None:
        """Expands every one of ``tree_node``'s own ancestors, root included
        -- ``move_cursor_keyed``'s own precondition (every ancestor expanded,
        all the way up to the tree's root) doesn't hold just because
        ``tree_node`` itself was found and expanded: ``find_node`` walks
        every reconciled ``TreeNode`` regardless of any ancestor's own
        collapsed state (a user can collapse an intermediate folder in the
        tree without touching ``model.selected`` at all), and
        ``Tree.move_cursor`` silently no-ops on an unreachable node rather
        than raising."""
        ancestor = tree_node.parent
        while ancestor is not None:
            if not ancestor.is_expanded:
                ancestor.expand()
            ancestor = ancestor.parent
