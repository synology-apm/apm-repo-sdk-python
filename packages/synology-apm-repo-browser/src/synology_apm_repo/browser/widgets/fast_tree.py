"""``FastLabelTree``: a ``Tree`` whose ``get_label_width`` computes the
width from the label and icon directly. ``Tree._build()`` measures every
line on every cache invalidation, and the base implementation renders a
throwaway ``Text`` per line to do it.
"""

from __future__ import annotations

from typing import override

from rich.cells import cell_len
from rich.text import Text
from textual.widgets import Tree
from textual.widgets.tree import TreeNode


class FastLabelTree[TreeDataType](Tree[TreeDataType]):
    """Drop-in ``Tree`` replacement, cheaper to measure; screens use it
    instead of ``textual.widgets.Tree``."""

    _icon_collapsed_width = cell_len(Tree.ICON_NODE)
    _icon_expanded_width = cell_len(Tree.ICON_NODE_EXPANDED)

    @override
    def get_label_width(self, node: TreeNode[TreeDataType]) -> int:
        label = node.label
        label_width = label.cell_len if isinstance(label, Text) else cell_len(label)
        if not node.allow_expand:
            return label_width
        prefix_width = self._icon_expanded_width if node.is_expanded else self._icon_collapsed_width
        return label_width + prefix_width
