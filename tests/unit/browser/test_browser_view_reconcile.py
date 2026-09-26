"""Unit tests for ``browser.view.reconcile`` — a real ``Tree``/``TreeNode``
throughout, but **no ``App``/``Pilot`` anywhere except the two
``move_cursor_keyed`` tests at the end**: ``Tree.add``/``add_leaf``/
``remove``/``remove_children``/``set_label``/``_tree_lines`` all run with
no running App; only ``Tree.move_cursor`` itself raises
``NoActiveAppError`` without one. A ``DataTable`` reconciler wouldn't be
testable this way either — ``add_row`` can't insert at a position and
``add_column`` needs a running ``App`` — which is why the source module
has none.
"""

from __future__ import annotations

from typing import Any

from textual.app import App, ComposeResult
from textual.widgets import Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.view.reconcile import (
    Binding,
    Diff,
    NodeSpec,
    find_node,
    move_cursor_keyed,
    next_cursor_key,
    reconcile_and_restore_cursor,
    reconcile_children,
)


def _tree() -> Tree[Binding[str]]:
    return Tree("root")


def _spec(key: str, label: str | None = None, **kwargs: Any) -> NodeSpec[str]:
    return NodeSpec(key=key, label=label or key, **kwargs)


def _keys(nodes: list[TreeNode[Binding[str]]]) -> list[str]:
    return [n.data.key for n in nodes if n.data is not None]


# -- reconcile_children: building from empty -------------------------------


def test_reconcile_from_empty_adds_every_spec_in_order() -> None:
    tree = _tree()
    diff = reconcile_children(tree.root, [_spec("a"), _spec("b"), _spec("c")])
    assert _keys(list(tree.root.children)) == ["a", "b", "c"]
    assert diff == Diff(before=(), after=("a", "b", "c"), added=("a", "b", "c"), removed=())


def test_reconcile_sets_label_and_payload_on_a_new_node() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", "Alpha", payload="p1")])
    node = tree.root.children[0]
    assert str(node.label) == "Alpha"
    assert node.data == Binding(key="a", label="Alpha", payload="p1")


def test_reconcile_leaf_by_default_is_not_expandable() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a")])
    assert tree.root.children[0].allow_expand is False


def test_reconcile_allow_expand_true_makes_the_node_expandable() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", allow_expand=True)])
    assert tree.root.children[0].allow_expand is True


# -- reconcile_children: survivor identity ---------------------------------


def test_reconcile_again_with_the_same_keys_keeps_the_same_treenode_objects() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a"), _spec("b")])
    before = list(tree.root.children)

    reconcile_children(tree.root, [_spec("a"), _spec("b")])
    after = list(tree.root.children)

    assert before[0] is after[0]
    assert before[1] is after[1]


def test_reconcile_expansion_state_survives_on_a_kept_node() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", allow_expand=True)])
    tree.root.children[0].expand()
    assert tree.root.children[0].is_expanded

    reconcile_children(tree.root, [_spec("a", allow_expand=True)])
    assert tree.root.children[0].is_expanded


# -- reconcile_children: below-threshold add/remove -------------------------


def test_reconcile_removes_a_node_no_longer_wanted() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a"), _spec("b"), _spec("c")])

    diff = reconcile_children(tree.root, [_spec("a"), _spec("c")])

    assert _keys(list(tree.root.children)) == ["a", "c"]
    assert diff.removed == ("b",)
    assert diff.rebuilt is False


def test_reconcile_inserts_a_new_node_between_two_survivors() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a"), _spec("c")])
    a_before, c_before = tree.root.children[0], tree.root.children[1]

    diff = reconcile_children(tree.root, [_spec("a"), _spec("b"), _spec("c")])

    assert _keys(list(tree.root.children)) == ["a", "b", "c"]
    assert diff.added == ("b",)
    assert tree.root.children[0] is a_before
    assert tree.root.children[2] is c_before


def test_reconcile_inserts_a_new_node_at_the_front() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a"), _spec("b")])
    diff = reconcile_children(tree.root, [_spec("x"), _spec("a"), _spec("b")])
    assert _keys(list(tree.root.children)) == ["x", "a", "b"]
    assert diff.added == ("x",)


def test_reconcile_below_threshold_does_not_set_rebuilt() -> None:
    """Removing 1 of 5 (``1 * 2 <= 5``) stays under the bulk-removal
    fallback's own threshold (more than half of the current managed
    children would be removed)."""
    tree = _tree()
    reconcile_children(tree.root, [_spec(k) for k in "abcde"])
    survivors_before = list(tree.root.children)[:1]  # just need identity of one to prove no rebuild

    diff = reconcile_children(tree.root, [_spec(k) for k in "abcd"])

    assert diff.rebuilt is False
    assert tree.root.children[0] is survivors_before[0]


# -- reconcile_children: bulk-removal fallback -------------------------------


def test_reconcile_above_threshold_sets_rebuilt_and_recreates_every_node() -> None:
    """Removing 3 of 4 (``3 * 2 > 4``) crosses the threshold -- every
    node, including the one survivor by key, is a genuinely new
    ``TreeNode`` object afterward (the honest cost of the fallback, not
    a bug: it removes every child via ``remove_children()`` and re-adds
    every wanted one fresh, trading survivor identity for one
    invalidation instead of one per removed node)."""
    tree = _tree()
    reconcile_children(tree.root, [_spec(k) for k in "abcd"])
    a_before = tree.root.children[0]

    diff = reconcile_children(tree.root, [_spec("a")])

    assert diff.rebuilt is True
    assert diff.removed == ("b", "c", "d")
    assert _keys(list(tree.root.children)) == ["a"]
    assert tree.root.children[0] is not a_before  # identity NOT preserved through the fallback


def test_reconcile_above_threshold_with_a_foreign_child_present_falls_back_to_one_at_a_time() -> None:
    """The bulk-removal path's own ``remove_children()`` wipes *every*
    real Textual child unconditionally -- with a foreign (non-``Binding``)
    child present, taking it would silently violate this module's own
    precondition (a foreign child is never removed on ``parent``'s
    behalf, per ``test_reconcile_leaves_a_foreign_non_binding_child_untouched``,
    which only exercises the *below*-threshold case and so never caught
    this). A foreign child anywhere under ``parent`` must force the
    one-at-a-time path regardless of how many managed children would be
    removed."""
    tree = _tree()
    foreign = tree.root.add_leaf("unmanaged", data=None)
    reconcile_children(tree.root, [_spec(k) for k in "abcd"])

    diff = reconcile_children(tree.root, [_spec("a")])  # removes 3 of 4 -- above the threshold

    assert foreign.parent is tree.root  # still there, never touched
    assert diff.rebuilt is False  # the bulk path was correctly skipped
    assert diff.removed == ("b", "c", "d")
    assert _keys([n for n in tree.root.children if n.data is not None]) == ["a"]


# -- reconcile_children: relabelling -----------------------------------------


def test_reconcile_relabels_a_survivor_whose_text_changed() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", "Old")])
    diff = reconcile_children(tree.root, [_spec("a", "New")])
    assert str(tree.root.children[0].label) == "New"
    assert diff.relabelled == ("a",)


def test_reconcile_does_not_call_set_label_when_the_text_is_unchanged(monkeypatch: Any) -> None:
    """Not just "the label looks the same afterward" -- proves the call
    itself is skipped, since a called ``set_label()`` always bumps the
    node's update counter and triggers a repaint, whether or not the
    text actually changed."""
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", "Same")])
    node = tree.root.children[0]
    calls: list[str] = []
    monkeypatch.setattr(node, "set_label", lambda label: calls.append(str(label)))

    diff = reconcile_children(tree.root, [_spec("a", "Same")])

    assert calls == []
    assert diff.relabelled == ()


def test_reconcile_does_not_wipe_a_foreign_live_suffix_when_the_stored_label_is_unchanged() -> None:
    """A per-node loading spinner (``TreeNodeLoadingSink``) appends its
    own suffix directly onto the live ``node.label`` without touching
    ``node.data`` at all, so it's invisible to ``update_node``'s label
    comparison, which checks the *stored* ``Binding.label``, not the live
    widget text. A reconcile
    triggered by something unrelated (a sibling's own fetch completing)
    must not see that live suffix as a label change and wipe it mid-
    animation; the comparison has to be against the *stored*
    ``Binding.label``, not the live text ``set_label`` last wrote."""
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", "Repo")])
    node = tree.root.children[0]
    node.set_label("Repo  | Loading")  # simulates TreeNodeLoadingSink.show()

    diff = reconcile_children(tree.root, [_spec("a", "Repo")])

    assert str(node.label) == "Repo  | Loading"
    assert diff.relabelled == ()


def test_reconcile_updates_payload_even_when_the_label_is_unchanged() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", "Same", payload="old")])
    reconcile_children(tree.root, [_spec("a", "Same", payload="new")])
    assert tree.root.children[0].data == Binding(key="a", label="Same", payload="new")


def test_reconcile_updates_allow_expand_on_a_survivor() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", allow_expand=False)])
    reconcile_children(tree.root, [_spec("a", allow_expand=True)])
    assert tree.root.children[0].allow_expand is True


# -- reconcile_children: nested children -------------------------------------


def test_reconcile_children_none_leaves_an_existing_subtree_untouched() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("x", allow_expand=True, children=(_spec("p"), _spec("q")))])
    x_node = tree.root.children[0]
    p_before = x_node.children[0]

    # children=None this time -- "not modelled", not "modelled as empty".
    reconcile_children(tree.root, [_spec("x", allow_expand=True, children=None)])

    assert _keys(list(x_node.children)) == ["p", "q"]
    assert x_node.children[0] is p_before


def test_reconcile_children_empty_tuple_clears_an_existing_subtree() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("x", allow_expand=True, children=(_spec("p"), _spec("q")))])
    x_node = tree.root.children[0]

    reconcile_children(tree.root, [_spec("x", allow_expand=True, children=())])

    assert list(x_node.children) == []


def test_reconcile_recurses_into_a_brand_new_nodes_own_children() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("x", allow_expand=True, children=(_spec("p", "Pea"), _spec("q", "Cue")))])
    x_node = tree.root.children[0]
    assert _keys(list(x_node.children)) == ["p", "q"]
    assert [str(c.label) for c in x_node.children] == ["Pea", "Cue"]


# -- reconcile_children: on_diff ----------------------------------------------


def test_on_diff_reports_the_top_level_keyed_by_the_roots_own_data() -> None:
    """``tree.root`` here has no ``Binding`` of its own (a bare ``Tree``,
    same as ``BrowseScreen``'s two permanent, non-domain tree roots) --
    the top level's own diff is reported under ``None``."""
    tree = _tree()
    diffs: dict[str | None, Diff[str]] = {}
    reconcile_children(tree.root, [_spec("a"), _spec("b")], on_diff=diffs.__setitem__)

    assert set(diffs) == {None}
    assert diffs[None].after == ("a", "b")


def test_on_diff_reports_a_nested_levels_own_diff_keyed_by_its_parent() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("x", allow_expand=True, children=(_spec("p"), _spec("q")))])

    diffs: dict[str | None, Diff[str]] = {}
    reconcile_children(tree.root, [_spec("x", allow_expand=True, children=(_spec("p"),))], on_diff=diffs.__setitem__)

    assert set(diffs) == {None, "x"}
    assert diffs["x"].before == ("p", "q")
    assert diffs["x"].after == ("p",)
    assert diffs["x"].removed == ("q",)


# -- reconcile_children: precondition (foreign, non-Binding children) -------


def test_reconcile_leaves_a_foreign_non_binding_child_untouched() -> None:
    """A child this module didn't create (``.data`` isn't a ``Binding``
    -- every ``TreeNode`` this module reconciles must have been created
    by a previous ``reconcile_children`` call) is neither counted as a
    survivor nor removed on ``parent``'s behalf."""
    tree = _tree()
    foreign = tree.root.add_leaf("unmanaged", data=None)

    diff = reconcile_children(tree.root, [_spec("a")])

    assert foreign.parent is tree.root  # still there, never touched
    assert _keys(list(tree.root.children)) == ["a"]
    assert diff.removed == ()  # the foreign node was never "existing" to this call at all


# -- next_cursor_key ----------------------------------------------------------


def test_next_cursor_key_returns_the_same_key_when_still_present() -> None:
    diff: Diff[str] = Diff(before=("a", "b", "c"), after=("a", "b", "c"))
    assert next_cursor_key("b", diff) == "b"


def test_next_cursor_key_picks_the_nearest_forward_survivor() -> None:
    diff: Diff[str] = Diff(before=("a", "b", "c", "d"), after=("a", "d"))
    assert next_cursor_key("b", diff) == "d"


def test_next_cursor_key_falls_back_to_the_nearest_backward_survivor() -> None:
    diff: Diff[str] = Diff(before=("a", "b", "c", "d"), after=("a",))
    assert next_cursor_key("c", diff) == "a"


def test_next_cursor_key_prefers_forward_over_backward() -> None:
    diff: Diff[str] = Diff(before=("a", "b", "c", "d", "e"), after=("a", "e"))
    assert next_cursor_key("c", diff) == "e"


def test_next_cursor_key_returns_none_when_nothing_at_this_level_survived() -> None:
    diff: Diff[str] = Diff(before=("a", "b"), after=())
    assert next_cursor_key("a", diff) is None


def test_next_cursor_key_returns_none_for_a_key_not_in_before_either() -> None:
    diff: Diff[str] = Diff(before=("a", "b"), after=("a", "b"))
    assert next_cursor_key("z", diff) is None


# -- find_node ---------------------------------------------------------------


def test_find_node_locates_a_direct_child() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a"), _spec("b")])
    b_node = tree.root.children[1]
    assert find_node(tree.root, "b") is b_node


def test_find_node_locates_a_grandchild() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a", children=(_spec("a1"),))])
    a1_node = tree.root.children[0].children[0]
    assert find_node(tree.root, "a1") is a1_node


def test_find_node_returns_none_for_an_absent_key() -> None:
    tree = _tree()
    reconcile_children(tree.root, [_spec("a")])
    assert find_node(tree.root, "z") is None


def test_find_node_ignores_a_foreign_non_binding_child() -> None:
    tree = _tree()
    tree.root.add_leaf("plain", data=None)
    assert find_node(tree.root, "anything") is None


# -- move_cursor_keyed (needs a real running App -- Tree.move_cursor itself
# raises NoActiveAppError without one) ---------------------------------------


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        yield Tree("root")


async def test_move_cursor_keyed_moves_the_cursor_to_a_different_node() -> None:
    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(Tree)
        # a fresh Tree's own root defaults to collapsed, and move_cursor_keyed
        # requires every ancestor up to tree.root expanded -- Tree.move_cursor
        # silently no-ops on an unreachable node rather than raising, so an
        # un-expanded root here would leave the cursor assertions below passing
        # against a stale position instead of catching the bug.
        tree.root.expand()
        reconcile_children(tree.root, [_spec("a"), _spec("b")])
        a_node, b_node = tree.root.children[0], tree.root.children[1]
        move_cursor_keyed(tree, a_node)
        assert tree.cursor_node is a_node

        move_cursor_keyed(tree, b_node)
        assert tree.cursor_node is b_node


async def test_move_cursor_keyed_is_a_no_op_when_already_there(monkeypatch: Any) -> None:
    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(Tree)
        tree.root.expand()
        reconcile_children(tree.root, [_spec("a")])
        a_node = tree.root.children[0]
        move_cursor_keyed(tree, a_node)
        assert tree.cursor_node is a_node

        calls: list[object] = []
        monkeypatch.setattr(tree, "move_cursor", lambda node, **kw: calls.append(node))
        move_cursor_keyed(tree, a_node)
        assert calls == []


# -- reconcile_and_restore_cursor (needs a real running App, same reason as
# move_cursor_keyed above) ---------------------------------------------------


async def test_reconcile_and_restore_cursor_follows_a_surviving_node() -> None:
    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(Tree)
        tree.root.expand()
        reconcile_and_restore_cursor(tree, [_spec("a"), _spec("b")])
        b_node = tree.root.children[1]
        move_cursor_keyed(tree, b_node)

        # "a" gains a label change but keeps its own key -- b survives
        # untouched, and the cursor (on b) must not move at all.
        reconcile_and_restore_cursor(tree, [_spec("a", "A!"), _spec("b")])

        assert tree.cursor_node is b_node


async def test_reconcile_and_restore_cursor_prefers_the_nearest_forward_sibling() -> None:
    """Mirrors ``next_cursor_key``'s own forward-first preference,
    wired all the way through a real reconcile this time -- filtering
    out the cursored middle child of three leaves a sibling on each
    side; the cursor must land on the *forward* one."""
    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(Tree)
        tree.root.expand()
        reconcile_and_restore_cursor(tree, [_spec("a"), _spec("b"), _spec("c")])
        a_node, b_node, c_node = tree.root.children
        move_cursor_keyed(tree, b_node)

        reconcile_and_restore_cursor(tree, [_spec("a"), _spec("c")])

        assert tree.cursor_node is c_node
        assert tree.cursor_node is not a_node


async def test_reconcile_and_restore_cursor_falls_back_to_the_parent_when_no_sibling_survives() -> None:
    """When the cursored leaf's *entire* level is filtered out (no
    surviving sibling for ``next_cursor_key`` to find either), the
    cursor falls back to that level's own parent node -- never all the
    way to the tree's root, and never drifting into an unrelated
    sibling *group*'s own content the way trusting Textual's raw,
    unclamped ``cursor_line`` position alone would: the group being
    emptied out shifts every line below it up by one, so a stale
    ``cursor_line`` would silently point at the next group over."""
    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(Tree)
        tree.root.expand()
        reconcile_and_restore_cursor(
            tree,
            [
                _spec("group-a", allow_expand=True, children=(_spec("a1"),)),
                _spec("group-b", allow_expand=True, children=(_spec("b1"),)),
            ],
        )
        group_a_node = tree.root.children[0]
        group_b_node = tree.root.children[1]
        group_a_node.expand()
        group_b_node.expand()
        a1_node = group_a_node.children[0]
        b1_node = group_b_node.children[0]
        move_cursor_keyed(tree, a1_node)

        # group-a's own only child is filtered out entirely; group-b is
        # untouched.
        reconcile_and_restore_cursor(
            tree,
            [
                _spec("group-a", allow_expand=True, children=()),
                _spec("group-b", allow_expand=True, children=(_spec("b1"),)),
            ],
        )

        assert tree.cursor_node is group_a_node
        assert tree.cursor_node is not b1_node
        assert tree.cursor_node is not group_b_node
        assert tree.cursor_node is not tree.root


async def test_reconcile_and_restore_cursor_climbs_past_a_removed_immediate_parent() -> None:
    """When the cursored leaf's own *immediate parent* is removed in the
    same reconcile (not just the leaf's own level narrowed) -- e.g.
    ``BrowseScreen``'s workload tree rebuilding wholesale on a catalog
    switch, dropping the cursor's entire former group along with it --
    the fallback must climb past that missing parent to the next
    surviving ancestor, rather than stopping at the immediate parent
    and leaving the cursor uncorrected because neither a sibling nor
    the parent itself was ever visited this pass."""
    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(Tree)
        tree.root.expand()
        reconcile_and_restore_cursor(
            tree,
            [
                _spec(
                    "grandparent",
                    allow_expand=True,
                    children=(_spec("group-a", allow_expand=True, children=(_spec("a1"),)),),
                )
            ],
        )
        grandparent_node = tree.root.children[0]
        grandparent_node.expand()
        group_a_node = grandparent_node.children[0]
        group_a_node.expand()
        a1_node = group_a_node.children[0]
        move_cursor_keyed(tree, a1_node)

        # group-a (the cursor's own immediate parent) is removed entirely,
        # not just narrowed -- grandparent survives with a different child.
        reconcile_and_restore_cursor(
            tree,
            [_spec("grandparent", allow_expand=True, children=(_spec("group-c", allow_expand=True, children=()),))],
        )

        assert tree.cursor_node is grandparent_node
        assert tree.cursor_node is not group_a_node
        assert tree.cursor_node is not tree.root


async def test_reconcile_and_restore_cursor_falls_back_to_root_when_a_non_domain_roots_top_level_is_wiped() -> None:
    """``BrowseScreen``'s own trees have a permanent, non-domain root
    (``tree.root.data`` stays ``None``) -- when the cursor sits on a
    top-level item directly under that root and every top-level item is
    removed at once (e.g. a rescan wiping every discovered repository),
    there is no sibling and no domain-keyed parent to fall back to, but
    the cursor must still land somewhere deliberate (the tree's own
    root) rather than being left uncorrected."""
    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(Tree)
        tree.root.expand()
        reconcile_and_restore_cursor(tree, [_spec("a"), _spec("b")])
        a_node = tree.root.children[0]
        move_cursor_keyed(tree, a_node)

        reconcile_and_restore_cursor(tree, [])

        assert tree.cursor_node is tree.root


async def test_reconcile_and_restore_cursor_falls_back_to_a_domain_keyed_tree_root() -> None:
    """``UnitScreen``'s own tree root is itself a domain node (unlike
    ``BrowseScreen``'s permanent non-domain roots) -- when the cursored
    leaf's entire level is filtered out and its parent *is* the tree's
    own root, the fallback must still find it. ``reconcile_children``'s
    own ``on_node`` hook (what the fallback lookup uses instead of a
    second ``find_node`` walk) only ever fires for a *child* spec, so
    the root's own entry has to be seeded separately -- this is exactly
    the case that seeding covers."""
    app = _FakeApp()
    async with app.run_test():
        tree = app.query_one(Tree)
        tree.root.expand()
        tree.root.data = Binding(key="root", label="root")
        reconcile_and_restore_cursor(tree, [_spec("a1")])
        a1_node = tree.root.children[0]
        move_cursor_keyed(tree, a1_node)

        reconcile_and_restore_cursor(tree, [])

        assert tree.cursor_node is tree.root


__all__: list[str] = []
