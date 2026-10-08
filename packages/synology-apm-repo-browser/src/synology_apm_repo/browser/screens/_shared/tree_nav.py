"""Shared ``Tree`` cursor helpers."""

from __future__ import annotations

from typing import Any

from textual.widgets import Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.view.reconcile import force_tree_line_cache


def move_cursor_to_parent(tree: Tree[Any]) -> None:
    """Backspace in every ``Tree``: moves the cursor to its node's parent
    and collapses that parent, unless it is the tree's root."""
    node = tree.cursor_node
    if node is None or node.parent is None:
        return  # nothing selected, or already at the root
    parent = node.parent
    if parent is not tree.root:
        parent.collapse()
    force_tree_line_cache(tree)
    tree.move_cursor(parent)


def current_listing_tree_node(tree: Tree[Any]) -> TreeNode[Any]:
    """The node whose children the cursor is among: its parent, or the
    root when the cursor is on the root or nowhere."""
    cursor = tree.cursor_node
    return cursor.parent if cursor is not None and cursor.parent is not None else tree.root
