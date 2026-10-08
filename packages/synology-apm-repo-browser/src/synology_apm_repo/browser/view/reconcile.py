"""Keyed reconciliation of a ``Tree``: updates a node's children in place
to match the ``NodeSpec``s a screen wants, keyed by domain identity, so a
survivor keeps its ``TreeNode`` (and its expansion/cursor state).

A reconciled ``TreeNode`` was created by ``reconcile_children`` (its
``.data`` is a ``Binding``); a foreign child (``.data`` is ``None``) is left
alone. Reconciliation recurses only as far as ``NodeSpec.children``
describes, and only adds and removes: a survivor whose relative order
changed is not moved.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence
from typing import Any

from textual.widgets import Tree
from textual.widgets.tree import TreeNode


@dataclasses.dataclass(frozen=True, slots=True)
class Binding[NodeKeyT]:
    """A reconciled ``TreeNode``'s ``.data``: its domain ``key``, the
    ``label`` reconciliation last set (compared instead of the live label,
    to which a loading sink may append), and the ``payload`` a screen's
    selection handler reads back."""

    key: NodeKeyT
    label: str
    payload: object = None


@dataclasses.dataclass(frozen=True, slots=True)
class NodeSpec[NodeKeyT]:
    """One wanted child. ``children=None`` leaves its subtree alone; ``()``
    empties it."""

    key: NodeKeyT
    label: str
    payload: object = None
    allow_expand: bool = False
    children: tuple[NodeSpec[NodeKeyT], ...] | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class Diff[NodeKeyT]:
    """What one ``reconcile_children`` call did to one level.
    ``before``/``after`` are its key order; ``rebuilt`` means the
    bulk-removal fallback re-created every child."""

    before: tuple[NodeKeyT, ...]
    after: tuple[NodeKeyT, ...]
    added: tuple[NodeKeyT, ...] = ()
    removed: tuple[NodeKeyT, ...] = ()
    relabelled: tuple[NodeKeyT, ...] = ()
    rebuilt: bool = False


def _binding[NodeKeyT](spec: NodeSpec[NodeKeyT]) -> Binding[NodeKeyT]:
    return Binding(key=spec.key, label=spec.label, payload=spec.payload)


def _add_spec[NodeKeyT](
    parent: TreeNode[Binding[NodeKeyT]],
    spec: NodeSpec[NodeKeyT],
    *,
    before: int,
    on_diff: Callable[[NodeKeyT | None, Diff[NodeKeyT]], None] | None = None,
    on_node: Callable[[NodeKeyT, TreeNode[Binding[NodeKeyT]]], None] | None = None,
) -> TreeNode[Binding[NodeKeyT]]:
    data = _binding(spec)
    node = (
        parent.add(spec.label, data=data, before=before, allow_expand=True)
        if spec.allow_expand
        else parent.add_leaf(spec.label, data=data, before=before)
    )
    if on_node is not None:
        on_node(spec.key, node)
    if spec.children is not None:
        reconcile_children(node, spec.children, on_diff=on_diff, on_node=on_node)
    return node


def update_node[NodeKeyT](node: TreeNode[Binding[NodeKeyT]], spec: NodeSpec[NodeKeyT]) -> bool:
    """Updates ``node`` in place to match ``spec``; whether the label
    changed (against the stored ``Binding.label``). ``set_label()`` is
    skipped when unchanged, since it always repaints. Public for a screen
    whose tree root is itself a domain node (``UnitScreen``), which
    ``reconcile_children`` never touches."""
    relabelled = node.data is None or node.data.label != spec.label
    if relabelled:
        node.set_label(spec.label)
    node.data = _binding(spec)
    if node.allow_expand != spec.allow_expand:
        node.allow_expand = spec.allow_expand
    return relabelled


def reconcile_children[NodeKeyT](
    parent: TreeNode[Binding[NodeKeyT]],
    specs: Sequence[NodeSpec[NodeKeyT]],
    *,
    on_diff: Callable[[NodeKeyT | None, Diff[NodeKeyT]], None] | None = None,
    on_node: Callable[[NodeKeyT, TreeNode[Binding[NodeKeyT]]], None] | None = None,
) -> Diff[NodeKeyT]:
    """Updates ``parent``'s children to match ``specs``, keyed by
    ``NodeSpec.key``: only what's new or gone is added or removed.

    When more than half the current children would be removed and none is
    foreign, all are removed and the wanted ones re-added, which is cheaper
    at that scale but loses survivors' identity.

    ``on_diff`` is called once per level reconciled, nested ones included,
    with that level's parent key (``None`` for a root without a
    ``Binding``). ``on_node`` is called for every spec's resolved
    ``TreeNode``, survivor or new."""
    existing: dict[NodeKeyT, TreeNode[Binding[NodeKeyT]]] = {
        node.data.key: node for node in parent.children if node.data is not None
    }
    has_foreign_child = any(node.data is None for node in parent.children)
    before = tuple(existing)
    wanted = tuple(spec.key for spec in specs)
    wanted_set = set(wanted)
    removed = tuple(key for key in before if key not in wanted_set)

    if not has_foreign_child and len(removed) * 2 > len(existing):
        parent.remove_children()
        for index, spec in enumerate(specs):
            _add_spec(parent, spec, before=index, on_diff=on_diff, on_node=on_node)
        diff = Diff(before=before, after=wanted, added=wanted, removed=removed, rebuilt=True)
        if on_diff is not None:
            on_diff(parent.data.key if parent.data is not None else None, diff)
        return diff

    for key in removed:
        existing[key].remove()

    added: list[NodeKeyT] = []
    relabelled: list[NodeKeyT] = []
    for insert_at, spec in enumerate(specs):
        survivor = existing.get(spec.key)
        if survivor is not None:
            if update_node(survivor, spec):
                relabelled.append(spec.key)
            if on_node is not None:
                on_node(spec.key, survivor)
            if spec.children is not None:
                reconcile_children(survivor, spec.children, on_diff=on_diff, on_node=on_node)
        else:
            _add_spec(parent, spec, before=insert_at, on_diff=on_diff, on_node=on_node)
            added.append(spec.key)

    diff = Diff(before=before, after=wanted, added=tuple(added), removed=removed, relabelled=tuple(relabelled))
    if on_diff is not None:
        on_diff(parent.data.key if parent.data is not None else None, diff)
    return diff


def next_cursor_key[NodeKeyT](cursor_key: NodeKeyT, diff: Diff[NodeKeyT]) -> NodeKeyT | None:
    """Where the cursor goes after ``diff``: ``cursor_key`` if it
    survived, else the nearest surviving sibling by original position
    (forward, then backward), else ``None``."""
    if cursor_key in diff.after:
        return cursor_key
    if cursor_key not in diff.before:
        return None
    cursor_index = diff.before.index(cursor_key)
    after_set = set(diff.after)
    for key in diff.before[cursor_index + 1 :]:
        if key in after_set:
            return key
    for key in reversed(diff.before[:cursor_index]):
        if key in after_set:
            return key
    return None


def find_node[NodeKeyT](root: TreeNode[Binding[NodeKeyT]], key: NodeKeyT) -> TreeNode[Binding[NodeKeyT]] | None:
    """The ``TreeNode`` under ``root`` (itself included) whose key is
    ``key``."""
    if root.data is not None and root.data.key == key:
        return root
    for child in root.children:
        found = find_node(child, key)
        if found is not None:
            return found
    return None


def force_tree_line_cache(tree: Tree[Any]) -> None:
    """Forces Textual's lazy ``Tree`` line-cache rebuild before
    ``move_cursor()``/``scroll_to_node()`` — otherwise a freshly-added
    node's unset ``_line`` (-1) makes the cursor silently land on the
    root instead of the intended node."""
    _ = tree._tree_lines  # noqa: SLF001 - Textual's own private attr, forces Tree._build(), see docstring above


def move_cursor_keyed(tree: Tree[Any], node: TreeNode[Any]) -> None:
    """Moves ``tree``'s cursor to ``node``, skipping the repaint when it is
    already there. ``node`` must be reachable (every ancestor expanded):
    ``Tree.move_cursor`` silently ignores an unreachable one."""
    if tree.cursor_node is node and node._line != -1:  # noqa: SLF001 - unbuilt nodes read -1 here; see force_tree_line_cache
        return
    force_tree_line_cache(tree)
    tree.move_cursor(node)


def reconcile_and_restore_cursor[NodeKeyT](
    tree: Tree[Binding[NodeKeyT]], specs: Sequence[NodeSpec[NodeKeyT]]
) -> Diff[NodeKeyT]:
    """Reconciles ``tree.root``'s children to ``specs``, then restores the
    cursor by domain key, since ``Tree.cursor_node`` follows line position,
    not node identity: to the same key, else its nearest surviving sibling,
    else its nearest surviving ancestor, else ``tree.root``."""
    cursor_node = tree.cursor_node
    cursor_key: NodeKeyT | None = None
    ancestor_keys: list[NodeKeyT] = []
    if cursor_node is not None and cursor_node.data is not None:
        cursor_key = cursor_node.data.key
        ancestor = cursor_node.parent
        while ancestor is not None and ancestor.data is not None:
            ancestor_keys.append(ancestor.data.key)
            ancestor = ancestor.parent

    diffs: dict[NodeKeyT | None, Diff[NodeKeyT]] = {}
    nodes: dict[NodeKeyT, TreeNode[Binding[NodeKeyT]]] = {}
    if tree.root.data is not None:
        nodes[tree.root.data.key] = tree.root
    diff = reconcile_children(tree.root, specs, on_diff=diffs.__setitem__, on_node=nodes.__setitem__)

    if cursor_key is None:
        return diff
    survivor = nodes.get(cursor_key)
    if survivor is not None:
        move_cursor_keyed(tree, survivor)
        return diff

    immediate_parent_key = ancestor_keys[0] if ancestor_keys else None
    fallback_node: TreeNode[Binding[NodeKeyT]] | None = None
    parent_diff = diffs.get(immediate_parent_key)
    if parent_diff is not None:
        fallback_key = next_cursor_key(cursor_key, parent_diff)
        if fallback_key is not None:
            fallback_node = nodes.get(fallback_key)
    if fallback_node is None:
        for ancestor_key in ancestor_keys:
            fallback_node = nodes.get(ancestor_key)
            if fallback_node is not None:
                break
    move_cursor_keyed(tree, fallback_node if fallback_node is not None else tree.root)
    return diff
