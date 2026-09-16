"""``GotoChainWalker``: expands ``UnitScreen``'s Tree widget onto a
goto-ref (``g``) target's already-resolved provider chain —
loading whatever isn't loaded yet, topping up whatever's only partially
loaded, and landing the cursor on the target. Held by ``UnitScreen`` as a
private collaborator, reaching back into it only through the small public
surface ``UnitScreen`` exposes for this (``unit_tree``, ``is_loaded``,
``mark_loaded_exhaustive``, ``loaded_children``, ``add_child_nodes``) —
the same convention ``units/device_pcps.py``/``units/device_disk_fs.py``
establish on the SDK side for a collaborator reaching back into the class
that owns it."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.screens._shared import force_tree_line_cache
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen


class GotoChainWalker:
    def __init__(self, screen: UnitScreen) -> None:
        self._screen = screen

    def expand_to_chain(self, chain: list[Node], children_by_step: list[list[Node]]) -> Node | None:
        """Expands/loads every step of ``chain`` onto the screen's Tree,
        moves the cursor onto the final node, and returns its ``Node``
        (``None`` only if the tree's own root itself somehow has no
        ``data`` yet — the real caller, ``UnitScreen._walk_to_target``,
        uses the return value to decide whether to show the target's
        detail pane)."""
        screen = self._screen
        tree = screen.unit_tree
        tree_node = tree.root
        for i, children in enumerate(children_by_step):
            if not screen.is_loaded(tree_node):
                # find_path_with_children() fetches each step's full,
                # exhaustive list (it needs every real sibling to
                # guarantee finding the goto target) — exhausted=True
                # reflects that accurately, not a page-sized guess.
                screen.mark_loaded_exhaustive(tree_node, children)
                screen.add_child_nodes(tree_node, children)
            else:
                # Already loaded, but via ordinary browsing rather than a
                # prior goto-ref walk — that only guarantees one page,
                # which may not reach the target further down this chain.
                # Top up with this step's exhaustive ``children`` list so
                # the search just below is guaranteed to find it instead
                # of raising StopIteration.
                self._top_up_to_exhaustive(tree_node, children)
            if not tree_node.is_expanded:
                tree_node.expand()
            next_ref = chain[i + 1].ref
            tree_node = self._find_chain_child(tree_node, next_ref)
        # See screens/_shared.py::force_tree_line_cache's own docstring
        # for why this is needed before move_cursor()/scroll_to_node() on
        # a freshly .add()ed/.expand()ed node.
        force_tree_line_cache(tree)
        tree.move_cursor(tree_node)
        tree.scroll_to_node(tree_node)
        return tree_node.data

    @staticmethod
    def _find_chain_child(tree_node: TreeNode[Node], ref: NodeRef) -> TreeNode[Node]:
        """A goto-ref chain step's target is normally a direct child of
        ``tree_node`` -- a "(filesystem)" disk-fs sibling is the one
        exception, since ``UnitScreen.add_child_nodes`` nests it one
        level under its own disk-image node instead of beside it (see
        that method's own comment). Falls back to one level deeper for
        that case, expanding the intermediate image node so the match
        doesn't end up hidden under a still-collapsed ancestor."""
        for child in tree_node.children:
            if child.data is not None and child.data.ref == ref:
                return child
        for child in tree_node.children:
            for grandchild in child.children:
                if grandchild.data is not None and grandchild.data.ref == ref:
                    if not child.is_expanded:
                        child.expand()
                    return grandchild
        raise StopIteration  # pragma: no cover - defensive: ref always came from tree_node's own just-added children

    def _top_up_to_exhaustive(self, tree_node: TreeNode[Node], children: list[Node]) -> None:
        """Adds every member of ``children`` (this step's exhaustive list
        from ``find_path_with_children``) not already among
        ``tree_node.children``, and marks it fully loaded — bringing a
        node loaded only up to a partial page in line with one loaded via
        ``expand_to_chain``'s own from-scratch branch. Checks one
        level deeper too — a "(filesystem)" disk-fs sibling already present
        nests under its own disk-image node (see ``UnitScreen.add_child_nodes``'s
        own comment) rather than being a direct child here, so a
        direct-only check would misread it as still missing and re-add a
        duplicate."""
        existing_refs = {tc.data.ref for tc in tree_node.children if tc.data is not None}
        existing_refs |= {gc.data.ref for tc in tree_node.children for gc in tc.children if gc.data is not None}
        missing = [child for child in children if child.ref not in existing_refs]
        screen = self._screen
        if missing:
            screen.add_child_nodes(tree_node, missing)
        # _LoadedChildren is frozen — mark_loaded_exhaustive already builds
        # the exact same fully-loaded replacement by hand, so reuse it
        # here rather than duplicating its shape.
        if screen.loaded_children(tree_node) is not None:
            screen.mark_loaded_exhaustive(tree_node, children)
