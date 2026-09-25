"""Shared ``Tree`` cursor helpers."""

from __future__ import annotations

from typing import Any

from textual.widgets import Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.view.reconcile import force_tree_line_cache


def move_cursor_to_parent(tree: Tree[Any]) -> None:
    """Backspace's shared behavior across every ``Tree`` in this app
    (``BrowseScreen``'s/``UnitScreen``'s via ``NavigableScreen`` below,
    ``ConnectDialog``'s own local directory tree directly since it isn't
    one): jumps the cursor to the current node's parent and
    collapses it, reaching a different branch in one keystroke instead
    of walking back up one line at a time with plain ``Tree``'s own Up.

    Never collapses the tree's own root, even when the cursor's parent
    *is* the root — ``BrowseScreen``'s two roots are permanent
    containers no one should collapse, and there's nothing to gain by
    collapsing a root with no sibling to jump to anyway. The cursor
    still moves there; only the collapse is skipped, so root stays a
    normal "nothing higher" landing spot rather than a special case
    every caller has to know about."""
    node = tree.cursor_node
    if node is None or node.parent is None:
        return  # nothing selected, or already at the tree's own root — nowhere further up
    parent = node.parent
    if parent is not tree.root:
        parent.collapse()
    force_tree_line_cache(tree)
    tree.move_cursor(parent)


def current_listing_tree_node(tree: Tree[Any]) -> TreeNode[Any]:
    """The tree node whose own children the cursor is currently
    browsing: the cursor's parent, or the tree's own root when the
    cursor sits on the root itself (no parent) or nothing is focused
    yet. Shared by ``BrowseScreen``'s tree-filter and ``UnitScreen``'s
    "load more"/filter, both resolving the same "which level is the
    cursor inside" question."""
    cursor = tree.cursor_node
    return cursor.parent if cursor is not None and cursor.parent is not None else tree.root
