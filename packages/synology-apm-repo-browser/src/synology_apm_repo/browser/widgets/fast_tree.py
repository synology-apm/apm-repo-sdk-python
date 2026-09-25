"""``FastLabelTree``: a plain ``Tree`` overriding ``get_label_width`` to
avoid rebuilding a ``Text`` object per line on every render.

``Tree._build()`` calls ``get_label_width()`` for *every* line on every
cache invalidation (a node add/remove, a relabel), and the base
implementation (``Tree.render_label`` -> ``Text.assemble(prefix,
node_label)`` -> ``.cell_len``) constructs a fresh ``Text`` just to throw
it away — Textual's ``get_label_width`` docstring says the method
"may be overridden in a sub-class if it can be done more efficiently."
A node's label text and
expand-icon prefix are already known without rendering anything, so this
computes the same cell width directly.
"""

from __future__ import annotations

from typing import TypeVar

from rich.cells import cell_len
from rich.text import Text
from textual.widgets import Tree
from textual.widgets.tree import TreeNode

TreeDataType = TypeVar("TreeDataType")


class FastLabelTree(Tree[TreeDataType]):
    """Drop-in ``Tree`` replacement; behaves identically, just cheaper to
    measure. Every screen in this package should use this instead of
    ``textual.widgets.Tree`` directly."""

    #: Computed once from the actual icon glyphs (not hardcoded), so an
    #: ``ICON_NODE``/``ICON_NODE_EXPANDED`` override elsewhere still
    #: measures correctly.
    _icon_collapsed_width = cell_len(Tree.ICON_NODE)
    _icon_expanded_width = cell_len(Tree.ICON_NODE_EXPANDED)

    def get_label_width(self, node: TreeNode[TreeDataType]) -> int:
        label = node.label
        label_width = label.cell_len if isinstance(label, Text) else cell_len(label)
        if not node.allow_expand:
            return label_width
        prefix_width = self._icon_expanded_width if node.is_expanded else self._icon_collapsed_width
        return label_width + prefix_width
