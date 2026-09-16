"""``DirsOnlyTree``: a directories-only ``DirectoryTree`` with real
keyboard-navigation conveniences a plain ``DirectoryTree`` has no
built-in answer for — used by ``ConnectDialog`` for its local-path
picker (the one real caller today, but self-contained enough to reuse
anywhere else a directory-only tree is needed)."""

from __future__ import annotations

import time
from collections.abc import Iterable
from pathlib import Path

from textual import events
from textual.widgets import DirectoryTree
from textual.widgets._directory_tree import DirEntry  # private: no public "directories only" hook exists at all
from textual.widgets.tree import TreeNode


class DirsOnlyTree(DirectoryTree):
    """``DirectoryTree`` has no built-in "directories only" mode:
    ``filter_paths`` is the documented override point, called once per
    directory as its contents load.

    Also implements two things ``DirectoryTree`` has no public override
    point for at all -- both reach into private Textual internals
    (``_populate_node``, ``DirEntry``), flagged here since either could break
    on a Textual upgrade: going up out of the rooted directory via a
    synthetic ``".."`` leaf (``_populate_node``, selection handled by the
    caller's own ``on_directory_tree_directory_selected``); and
    type-ahead, jumping the cursor to the first sibling whose name starts
    with what's been typed so far (``_on_key``, since neither ``Tree`` nor
    ``DirectoryTree`` define one of their own).
    """

    #: Reset the type-ahead search buffer after this long without a
    #: keystroke — the common desktop convention (macOS Finder's list
    #: view, GTK's "interactive search") for a short-lived buffer
    #: rather than one that lasts the whole time this dialog is open.
    _TYPEAHEAD_TIMEOUT = 1.0

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._typeahead_buffer = ""
        self._typeahead_last_time = 0.0

    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return [p for p in paths if p.is_dir()]

    def _populate_node(self, node: TreeNode[DirEntry], content: Iterable[Path]) -> None:
        node.remove_children()
        assert node.data is not None
        # Only the tree's own root ever gets the synthetic ".." leaf: a plain
        # DirectoryTree's root is fixed at construction with nothing above it
        # ever visible, whereas an already-expanded subdirectory's own parent
        # is just collapsed instead -- that already un-shows its children, so
        # it needs no ".." entry of its own. DirEntry.path already resolves
        # ".." to the real parent directory, so selecting it needs no special
        # handling here; the base class's own selection handling already
        # treats it as an ordinary, real, selectable directory.
        if node is self.root:
            parent_dir = node.data.path.parent
            if parent_dir != node.data.path:
                # Prepended, not appended: TreeNode has no "insert at a
                # given index" API, so this has to be added before any
                # real entry to land first, matching the classic
                # file-manager convention of ".." always listed first.
                node.add_leaf("..", data=DirEntry(parent_dir))
        for path in content:
            node.add(path.name, data=DirEntry(path), allow_expand=self._safe_is_dir(path))
        node.expand()

    async def _on_key(self, event: events.Key) -> None:
        # Only genuinely printable keys feed the type-ahead buffer -- same
        # ``is_printable`` check Input's own ``_on_key`` uses, and for the same
        # reason: arrows, Enter, Tab, etc. must fall through untouched.
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
        # The extended buffer matched nothing — start over with just
        # this one keystroke instead of getting stuck on an unmatchable
        # prefix for the rest of the timeout window.
        if self._jump_to_sibling_starting_with(event.character):
            self._typeahead_buffer = event.character
        else:
            # Not even a single-character match: leave the cursor exactly
            # where it was rather than jump somewhere the user didn't type.
            self._typeahead_buffer = ""

    def _jump_to_sibling_starting_with(self, prefix: str) -> bool:
        """Searches only the current cursor node's own siblings (its
        parent's children — the tree's own root's children if nothing
        is selected yet), not the whole tree: this is a "which folder
        *here* starts with what I'm typing" jump, matching every other
        type-ahead list/tree convention, not a global search."""
        node = self.cursor_node
        siblings = node.parent.children if node is not None and node.parent is not None else self.root.children
        needle = prefix.lower()
        for sibling in siblings:
            if str(sibling.label).lower().startswith(needle):
                self.move_cursor(sibling)
                return True
        return False
