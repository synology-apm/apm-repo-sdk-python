"""``DirsOnlyTree``: a directories-only ``DirectoryTree`` with a ``..``
entry and type-ahead, used by ``ConnectDialog``'s local-path picker."""

from __future__ import annotations

import time
from collections.abc import Iterable
from pathlib import Path
from typing import override

from rich.cells import cell_len
from rich.text import Text
from textual import events
from textual.widgets import DirectoryTree
from textual.widgets._directory_tree import DirEntry  # private: no public "directories only" hook exists at all
from textual.widgets.tree import TreeNode


class DirsOnlyTree(DirectoryTree):
    """Lists directories only (``filter_paths``), offers a ``..`` leaf
    above the root, and jumps to the first sibling starting with what's
    typed. The ``..`` leaf overrides private Textual internals
    (``_populate_node``, ``DirEntry``), which a Textual upgrade can break.
    """

    #: Seconds without a keystroke after which type-ahead starts over.
    _TYPEAHEAD_TIMEOUT = 1.0

    #: ``DirectoryTree``'s icon widths, for ``get_label_width``.
    _icon_folder_width = cell_len(DirectoryTree.ICON_NODE)
    _icon_folder_expanded_width = cell_len(DirectoryTree.ICON_NODE_EXPANDED)
    #: Every ``DirectoryTree`` leaf, ``..`` included, has an icon prefix.
    _icon_file_width = cell_len(DirectoryTree.ICON_FILE)

    @override
    def get_label_width(self, node: TreeNode[DirEntry]) -> int:
        """Measured directly, as ``FastLabelTree`` does, with
        ``DirectoryTree``'s icon-prefix rule."""
        label = node.label
        label_width = label.cell_len if isinstance(label, Text) else cell_len(label)
        if not node.allow_expand:
            return label_width + self._icon_file_width
        prefix_width = self._icon_folder_expanded_width if node.is_expanded else self._icon_folder_width
        return label_width + prefix_width

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._typeahead_buffer = ""
        self._typeahead_last_time = 0.0

    @override
    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return [p for p in paths if p.is_dir()]

    @override
    def _populate_node(self, node: TreeNode[DirEntry], content: Iterable[Path]) -> None:
        node.remove_children()
        assert node.data is not None
        # Only the root gets a ".." leaf: a subdirectory's parent is already
        # visible above it. Its DirEntry is the real parent directory, so
        # selecting it needs no special handling.
        if node is self.root:
            parent_dir = node.data.path.parent
            if parent_dir != node.data.path:
                # Added first so it lists first (TreeNode can't insert).
                node.add_leaf("..", data=DirEntry(parent_dir))
        for path in content:
            node.add(path.name, data=DirEntry(path), allow_expand=self._safe_is_dir(path))
        node.expand()

    @override
    async def _on_key(self, event: events.Key) -> None:
        # Only printable keys feed type-ahead; arrows, Enter etc. fall through.
        if not event.is_printable:
            return
        assert event.character is not None
        event.stop()
        event.prevent_default()

        now = time.monotonic()
        if now - self._typeahead_last_time > self._TYPEAHEAD_TIMEOUT:
            self._typeahead_buffer = ""
        self._typeahead_last_time = now

        extended = self._typeahead_buffer + event.character
        if self._jump_to_sibling_starting_with(extended):
            self._typeahead_buffer = extended
            return
        # No match: start over from this keystroke.
        if self._jump_to_sibling_starting_with(event.character):
            self._typeahead_buffer = event.character
        else:
            # No match at all: the cursor stays.
            self._typeahead_buffer = ""

    def _jump_to_sibling_starting_with(self, prefix: str) -> bool:
        """Moves the cursor to the first sibling of the cursor node (or root
        child) whose name starts with ``prefix``; whether one matched."""
        node = self.cursor_node
        siblings = node.parent.children if node is not None and node.parent is not None else self.root.children
        needle = prefix.lower()
        for sibling in siblings:
            if str(sibling.label).lower().startswith(needle):
                self.move_cursor(sibling)
                return True
        return False
