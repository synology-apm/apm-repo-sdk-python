"""Keyed reconciliation for one ``Tree`` level: given the domain
``NodeSpec``s a screen wants a level to show, updates a real ``Tree``'s
children in place, indexed by domain identity, instead of destroying and
rebuilding them. This is what lets a filtered level keep its
already-loaded ``TreeNode``s (their own expansion/cursor state survives
with them) rather than paying for and immediately discarding a full
rebuild on every keystroke — and it deletes the whole class of bug that
comes from re-deriving state from ``id(TreeNode)``: CPython can reuse a
destroyed node's address for an unrelated later one. A reconciled
level's index is rebuilt fresh from the *domain* key on every call, so
there is nothing to purge.

**Precondition**: a ``TreeNode`` this module reconciles must have been
created by a previous call to ``reconcile_children`` (its own ``.data``
is a ``Binding``) — this module owns 100% of ``parent``'s children, not
just the ones it happens to recognize. A child it didn't create (``.data``
isn't a ``Binding``) is left untouched rather than crashing, but is also
never counted as a survivor or removed on ``parent``'s behalf.

**Scope, deliberately**: this reconciles *one level* (never recurses
past what ``NodeSpec.children`` actually describes) and only ever
adds/removes — it does not reposition a survivor that's still wanted but
whose ``specs`` order relative to *other* survivors changed. Every real
consumer in this package (a ``/`` filter narrowing or widening a level)
only ever adds/removes against an otherwise-stable order, since the
underlying grouping/sort is already stable — see ``workload_grouping.py``.
A caller whose own ordering can actually change relative order needs a
different mechanism, not a speculative one added here ahead of a real
need.

No ``DataTable`` equivalent exists here on purpose: ``DataTable.add_row``
can't insert at a position (append-only), and ``DataTable.add_column``
raises ``NoActiveAppError`` with no running ``App`` — unlike every method
this module actually calls (``Tree.add``/``add_leaf``/``remove``/
``remove_children``/``set_label``, all directly runnable with no ``App``
at all), so a table reconciler would also be untestable the same way
this one is. A version table's own
filter re-render is a plain ``clear()`` + ``add_row`` loop with the
cursor position saved/restored around it instead — see
``browse_screen.py``'s ``_render_versions``.
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
    """A reconciled ``TreeNode``'s own ``.data`` — domain identity
    (``key``), the label actually on screen right now (``label`` —
    compared against, not the live ``node.label``, so a widget owned by
    something else, e.g. a per-node loading spinner's own suffix
    appended directly to the widget, is invisible to this comparison
    and never fought over), and whatever payload a screen's own
    ``on_tree_node_selected`` handler ultimately wants back
    (a ``CatalogEntry``, a ``Workload``, a ``Node``, ...)."""

    key: NodeKeyT
    label: str
    payload: object = None


@dataclasses.dataclass(frozen=True, slots=True)
class NodeSpec(Generic[NodeKeyT]):
    """One level's own wanted shape for one child, as a screen's
    selector computes it fresh from the model on every dispatch.
    ``children=None`` means "not modelled" — a level the user has never
    expanded — and this module leaves that subtree completely alone
    rather than treating an absent value as "should have zero
    children". An explicit ``()`` reconciles down to genuinely empty."""

    key: NodeKeyT
    label: str
    payload: object = None
    allow_expand: bool = False
    children: tuple[NodeSpec[NodeKeyT], ...] | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class Diff(Generic[NodeKeyT]):
    """What one ``reconcile_children`` call actually did — ``before``/
    ``after`` are this level's own key order, kept here so
    ``next_cursor_key`` doesn't need its own separate pass over the
    tree to reconstruct them. ``rebuilt=True`` means the bulk-removal
    fallback fired (more than half of ``parent``'s current managed
    children would have been removed one at a time) — every
    survivor's own ``TreeNode`` identity was lost that pass, same as
    the destroy-and-rebuild this module otherwise avoids."""

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
    label actually changed. Compared against the *stored* ``node.data.label``
    (``None`` only before this node's very first update), never the live
    ``node.label``, because something
    else (a per-node loading spinner's own suffix, ``TreeNodeLoadingSink``)
    can append directly to the live label without this module ever
    knowing, and comparing against that live text would see its own
    unrelated suffix as a label change and wipe it mid-animation.
    ``set_label()`` unconditionally bumps the node's own update counter
    and schedules a repaint even when the text is identical -- the same
    no-op-cost pattern ``FastLabelTree.get_label_width`` avoids for label
    *measurement* (rebuilding a fresh ``Text`` object on every render
    regardless of whether the label changed); skipping the call itself
    when nothing changed avoids the analogous cost here at the source
    instead. Exported for a screen whose
    own tree root is itself a domain node (``UnitScreen``'s, unlike
    ``BrowseScreen``'s permanent non-domain roots) to reuse directly,
    since ``reconcile_children``/``reconcile_and_restore_cursor`` only
    ever touch a parent's *children*, never the parent node itself."""
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
    its own ``TreeNode`` object (and so its own expansion/cursor state)
    across the call; only what's genuinely new or gone is added or
    removed.

    Bulk-removal fallback: if more than half of ``parent``'s current
    *managed* children would be removed, this removes all of them
    (``TreeNode.remove_children()``) and adds every wanted child fresh
    instead of removing them one at a time. ``remove_children()``
    triggers exactly one ``Tree._invalidate()`` for the whole batch,
    while ``TreeNode.remove()`` triggers one *per call* — for a level
    narrowing from 200 children to
    3 (a realistic filter keystroke in this package), one-at-a-time
    removal costs ~197 invalidations against the 4 total
    (``remove_children()`` + 3 ``add()``s) the fallback costs instead,
    the same order of magnitude as never reconciling at all. Below that
    threshold, one-at-a-time is already at least as cheap and preserves
    survivors' own identity, which the fallback cannot. Only taken when
    ``parent`` has no foreign child at all: ``remove_children()`` wipes
    *every* real Textual child unconditionally, with no way to spare a
    child this module didn't create -- taking it with one present would
    silently violate this module's own precondition above (a foreign
    child left untouched), so a foreign child anywhere under ``parent``
    forces the one-at-a-time path regardless of how many managed
    children would be removed.

    ``on_diff``, when given, is called once per level this call actually
    reconciles (this one, plus every nested level recursed into),
    keyed by that level's own *parent* key (``None`` for a parent with
    no ``Binding`` of its own, e.g. a permanent non-domain tree root) --
    ``reconcile_and_restore_cursor``'s own way of finding the *specific*
    level a stale cursor's own former parent belongs to, since a single
    top-level return value can't describe every nested level a
    recursive call like this one touches.

    ``on_node``, when given, is called once for every spec this call (and
    every nested level recursed into) resolves to a real ``TreeNode`` --
    survivor or freshly added -- keyed by that node's own domain key.
    ``reconcile_and_restore_cursor`` uses this to build an index of every
    reconciled node as a side effect of the walk this function already
    does, rather than paying for a second, separate tree walk
    (``find_node``) to locate one by key afterward."""
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
    """Where the cursor should move to after a reconcile that removed
    the node it was on — the nearest surviving sibling by *original*
    position, forward first, then backward; ``None`` when nothing at
    this level survived at all (the caller then climbs the cursor's own
    ancestor chain looking for the nearest still-present node instead).

    Returns ``cursor_key`` itself, unchanged, when it's still present —
    the caller does not need to call this at all in that case (the
    survivor kept its own ``TreeNode``, so the cursor already didn't
    move); it's handled here too only so callers can call this
    unconditionally without checking first."""
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
    (never the full domain tree — only nodes some earlier
    ``reconcile_children`` call actually attached) to find the
    ``TreeNode`` currently representing ``key``. Needed because a
    survivor keeping its own ``TreeNode`` object (this module's own
    core guarantee) is not the same thing as the cursor staying on it:
    ``Tree.cursor_node`` is derived from ``cursor_line``, a line
    *position*, not a node identity, so a survivor whose siblings above
    it were added or removed still needs ``move_cursor_keyed`` pointed
    at it explicitly by whoever wants the cursor to actually follow it."""
    if root.data is not None and root.data.key == key:
        return root
    for child in root.children:
        found = find_node(child, key)
        if found is not None:
            return found
    return None


def force_tree_line_cache(tree: Tree[Any]) -> None:
    """Force Textual's lazy line-cache rebuild before touching
    ``move_cursor()``/``scroll_to_node()``: ``Tree._build()`` (the
    thing that assigns each ``TreeNode`` a real ``.line``) only runs
    lazily, either on the next ``on_idle`` tick or whenever something
    reads the private ``_tree_lines`` property; ``.add()``/``.expand()``
    only *invalidate* the cache (``_tree_lines_cached = None``), they
    don't rebuild it. ``Tree.move_cursor(node)`` reads ``node._line``
    directly without forcing a rebuild first, so a freshly ``.add()``ed
    node (whose ``_line`` is still its never-updated constructor
    default of -1) makes ``cursor_line`` become -1, which
    ``validate_cursor_line`` then clamps to 0 — i.e. the cursor
    silently lands on the root instead of raising or visibly failing.
    Touching ``_tree_lines`` here forces the rebuild synchronously so
    ``node.line`` is correct by the time ``move_cursor()``/
    ``scroll_to_node()`` reads it."""
    _ = tree._tree_lines  # noqa: SLF001 - Textual's own private attr, forces Tree._build(), see docstring above


def move_cursor_keyed(tree: Tree[Any], node: TreeNode[Any]) -> None:
    """Moves ``tree``'s cursor to ``node`` — a no-op, with no reactive
    write at all, when the cursor is already there (the common case
    after a reconcile whose survivor kept its own ``TreeNode``):
    ``Tree.move_cursor`` always writes ``cursor_line``, a reactive, even
    when the value doesn't change, which schedules a repaint regardless
    — this check is what keeps a reconcile that changed nothing about
    the cursor from costing one anyway. ``force_tree_line_cache`` still
    needs to run before moving to a node that actually changed, or is
    only fresh from ``.add()``.

    ``node`` must actually be reachable — every one of its ancestors
    expanded, all the way up to ``tree.root`` (a fresh ``Tree``'s own
    root defaults to *collapsed*, same as every one of this package's
    own multi-column trees before a screen's own ``on_mount`` expands
    it). ``Tree.move_cursor`` silently no-ops on an unreachable node
    (``cursor_node`` stays wherever it already was) rather than raising
    — this function does not detect or correct that."""
    if tree.cursor_node is node and node._line != -1:  # noqa: SLF001 - unbuilt nodes read -1 here; see force_tree_line_cache
        return
    force_tree_line_cache(tree)
    tree.move_cursor(node)


def reconcile_and_restore_cursor(tree: Tree[Binding[NodeKeyT]], specs: Sequence[NodeSpec[NodeKeyT]]) -> Diff[NodeKeyT]:
    """Reconciles ``tree.root``'s children to ``specs``, then restores the
    cursor by domain key rather than trusting ``TreeNode`` identity:
    ``Tree.cursor_node`` is derived from ``cursor_line``, a line
    *position*, so a survivor whose siblings changed still needs the
    cursor pointed at it explicitly.

    If the cursor's key didn't survive, moves to the nearest surviving
    sibling at its former level (``next_cursor_key``); if none did
    either, climbs the captured ancestor chain to the nearest surviving
    ancestor, since an entire subtree can vanish in one reconcile (e.g.
    a catalog switch dropping the whole former group chain at once);
    falls back to ``tree.root`` -- always reachable -- if nothing else
    survived.

    Builds its node/diff index from ``reconcile_children``'s own
    ``on_node``/``on_diff`` hooks during that single reconciling walk,
    rather than a second ``find_node`` walk per candidate. ``on_node``
    never fires for ``parent`` itself, so a domain-keyed tree root is
    seeded into the index separately."""
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
