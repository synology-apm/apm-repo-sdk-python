"""Keyed reconciliation for one ``Tree`` level: given the domain
``NodeSpec``s a screen wants a level to show, updates a real ``Tree``'s
children in place, indexed by domain identity, so a survivor keeps its
own ``TreeNode`` (and its expansion/cursor state) instead of a full
rebuild — and never keys off ``id(TreeNode)``, since CPython can reuse a
destroyed node's address for an unrelated later one.

**Precondition**: a ``TreeNode`` this module reconciles must have been
created by a previous ``reconcile_children`` call (its ``.data`` is a
``Binding``) — this module owns 100% of ``parent``'s children, not just
the ones it recognizes. A foreign child (``.data`` isn't a ``Binding``)
is left untouched, never counted as a survivor or removed.

**Scope**: reconciles one level only (never recurses past what
``NodeSpec.children`` describes) and only adds/removes — it does not
reposition a survivor whose relative order changed.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence
from typing import Any, Generic, TypeVar

from textual.widgets import Tree
from textual.widgets.tree import TreeNode

NodeKeyT = TypeVar("NodeKeyT")


@dataclasses.dataclass(frozen=True, slots=True)
class Binding(Generic[NodeKeyT]):
    """A reconciled ``TreeNode``'s own ``.data``: domain identity
    (``key``), the label actually on screen (``label`` — compared
    against, not the live ``node.label``, so a suffix another widget
    appends directly, e.g. a loading spinner's, stays invisible to this
    comparison), and whatever payload a screen's own
    ``on_tree_node_selected`` handler wants back (a ``CatalogEntry``, a
    ``Workload``, a ``Node``, ...)."""

    key: NodeKeyT
    label: str
    payload: object = None


@dataclasses.dataclass(frozen=True, slots=True)
class NodeSpec(Generic[NodeKeyT]):
    """One level's wanted shape for one child. ``children=None`` means
    "not modelled" — this module leaves that subtree alone rather than
    treating it as "should have zero children". An explicit ``()``
    reconciles down to genuinely empty."""

    key: NodeKeyT
    label: str
    payload: object = None
    allow_expand: bool = False
    children: tuple[NodeSpec[NodeKeyT], ...] | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class Diff(Generic[NodeKeyT]):
    """What one ``reconcile_children`` call did. ``before``/``after``
    are this level's own key order. ``rebuilt=True`` means the
    bulk-removal fallback fired — every survivor's own ``TreeNode``
    identity was lost that pass, same as a full rebuild."""

    before: tuple[NodeKeyT, ...]
    after: tuple[NodeKeyT, ...]
    added: tuple[NodeKeyT, ...] = ()
    removed: tuple[NodeKeyT, ...] = ()
    relabelled: tuple[NodeKeyT, ...] = ()
    rebuilt: bool = False


def _binding(spec: NodeSpec[NodeKeyT]) -> Binding[NodeKeyT]:
    return Binding(key=spec.key, label=spec.label, payload=spec.payload)


def _add_spec(
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


def update_node(node: TreeNode[Binding[NodeKeyT]], spec: NodeSpec[NodeKeyT]) -> bool:
    """Updates ``node`` in place to match ``spec`` — returns whether the
    label actually changed. Compares against the *stored*
    ``node.data.label``, never the live ``node.label``, since something
    else (e.g. ``TreeNodeLoadingSink``'s per-node suffix) can append to
    the live label directly. Skips ``set_label()`` when the label is
    unchanged, since it unconditionally schedules a repaint. Exported
    separately for a screen whose own tree root is itself a domain node
    (``UnitScreen``'s), since ``reconcile_children`` only ever touches a
    parent's children, never the parent node itself."""
    relabelled = node.data is None or node.data.label != spec.label
    if relabelled:
        node.set_label(spec.label)
    node.data = _binding(spec)
    if node.allow_expand != spec.allow_expand:
        node.allow_expand = spec.allow_expand
    return relabelled


def reconcile_children(
    parent: TreeNode[Binding[NodeKeyT]],
    specs: Sequence[NodeSpec[NodeKeyT]],
    *,
    on_diff: Callable[[NodeKeyT | None, Diff[NodeKeyT]], None] | None = None,
    on_node: Callable[[NodeKeyT, TreeNode[Binding[NodeKeyT]]], None] | None = None,
) -> Diff[NodeKeyT]:
    """Updates ``parent``'s children to match ``specs``, keyed by domain
    identity (``NodeSpec.key``) rather than position — a survivor keeps
    its own ``TreeNode`` (and its expansion/cursor state); only what's
    new or gone is added or removed.

    Bulk-removal fallback: when more than half of ``parent``'s current
    managed children would be removed, removes all of them
    (``remove_children()``) and re-adds every wanted child fresh instead
    of one at a time — cheaper at that scale, though survivors lose
    their own identity, same as a full rebuild. Only taken when
    ``parent`` has no foreign child: ``remove_children()`` wipes every
    real child unconditionally, with no way to spare one this module
    didn't create.

    ``on_diff``, when given, is called once per level reconciled (this
    one, plus every nested level recursed into), keyed by that level's
    own parent key (``None`` for a root with no ``Binding`` of its own).
    ``on_node``, when given, is called once for every spec resolved to a
    real ``TreeNode`` — survivor or freshly added — keyed by its domain
    key."""
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


def next_cursor_key(cursor_key: NodeKeyT, diff: Diff[NodeKeyT]) -> NodeKeyT | None:
    """Where the cursor should move after a reconcile removed the node
    it was on — the nearest surviving sibling by original position,
    forward then backward; ``None`` if nothing at this level survived
    (the caller then climbs the ancestor chain instead). Returns
    ``cursor_key`` unchanged when it's still present, so callers can call
    this unconditionally without checking first."""
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


def find_node(root: TreeNode[Binding[NodeKeyT]], key: NodeKeyT) -> TreeNode[Binding[NodeKeyT]] | None:
    """A bounded walk of whatever's currently reconciled under ``root``
    to find the ``TreeNode`` representing ``key`` — a survivor keeping
    its own ``TreeNode`` (this module's core guarantee) doesn't mean the
    cursor followed it, since ``Tree.cursor_node`` is derived from line
    position, not node identity."""
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
    """Moves ``tree``'s cursor to ``node`` — a no-op when the cursor is
    already there, since ``Tree.move_cursor`` always schedules a repaint
    even when unchanged. Call ``force_tree_line_cache`` first if
    ``node`` is new or the cursor is actually moving. ``node`` must be
    reachable (every ancestor expanded); ``Tree.move_cursor`` silently
    no-ops on an unreachable node rather than raising."""
    if tree.cursor_node is node and node._line != -1:  # noqa: SLF001 - unbuilt nodes read -1 here; see force_tree_line_cache
        return
    force_tree_line_cache(tree)
    tree.move_cursor(node)


def reconcile_and_restore_cursor(tree: Tree[Binding[NodeKeyT]], specs: Sequence[NodeSpec[NodeKeyT]]) -> Diff[NodeKeyT]:
    """Reconciles ``tree.root``'s children to ``specs``, then restores
    the cursor by domain key rather than trusting ``TreeNode`` identity,
    since ``Tree.cursor_node`` is derived from line position.

    If the cursor's key didn't survive, moves to the nearest surviving
    sibling (``next_cursor_key``); if none did either, climbs the
    captured ancestor chain to the nearest surviving ancestor (an entire
    subtree can vanish in one reconcile); falls back to ``tree.root`` if
    nothing survived."""
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
