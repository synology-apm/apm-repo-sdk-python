"""Unit tests for ``synology_apm_repo.sdk.units.resolve`` — the shared
prefix-guided descent behind ``Repository.resolve``
and the TUI's goto-ref, plus the
``SupportsDirectRefLookup`` dispatch
for a provider whose ``extra_segments`` don't grow with depth (Drive's
shape — exercised here against a fake, not the real ``RecursiveTree``; see
``test_units_saas_tree_strategy.py`` for that)."""

from __future__ import annotations

from synology_apm_repo.sdk.units.base import Node, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.resolve import find_node, find_path_with_children


class _RecordingProvider:
    """A plain ``UnitProvider`` over a hand-built ``ref -> children`` map,
    recording every ``children()`` call it actually received — used to
    assert the descent visits only nodes on the real path, never a
    non-matching sibling's subtree."""

    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]]) -> None:
        self._root = root
        self._children_by_ref = children_by_ref
        self.children_calls: list[str] = []

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        self.children_calls.append(str(node.ref))
        return self._children_by_ref.get(str(node.ref), [])[offset : offset + limit if limit is not None else None]

    async def unit(self, node: Node) -> RestorableUnit:
        raise NotImplementedError


class _FlatIdProvider:
    """A fake implementing ``SupportsDirectRefLookup`` directly — stands
    in for Drive's real shape (flat, depth-independent ``item_id``
    addressing) without any real ``RecursiveTree`` plumbing."""

    def __init__(self, nodes_by_id: dict[str, Node], parent_by_id: dict[str, str | None]) -> None:
        self._nodes_by_id = nodes_by_id
        self._parent_by_id = parent_by_id

    def root(self) -> Node:
        raise NotImplementedError

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        item_id = node.ref.extra_segments[0]
        kids = [n for iid, n in self._nodes_by_id.items() if self._parent_by_id.get(iid) == item_id]
        return kids[offset : offset + limit if limit is not None else None]

    async def unit(self, node: Node) -> RestorableUnit:
        raise NotImplementedError

    async def resolve_extra(self, extra_segments: tuple[str, ...]) -> Node | None:
        if len(extra_segments) != 1:
            return None
        return self._nodes_by_id.get(extra_segments[0])

    async def parent_of(self, node: Node) -> Node | None:
        parent_id = self._parent_by_id.get(node.ref.extra_segments[0])
        return self._nodes_by_id.get(parent_id) if parent_id is not None else None


def _node(*segments: str, is_leaf: bool = False) -> Node:
    return Node(ref=NodeRef("repo", segments), name=segments[-1] if segments else "root", is_leaf=is_leaf)


class TestFindNodeGenericDescent:
    async def test_root_itself_is_the_target(self) -> None:
        root = _node("root")
        provider = _RecordingProvider(root, {})

        assert await find_node(provider, root.ref) == root
        assert provider.children_calls == []

    async def test_finds_a_target_nested_several_levels_deep(self) -> None:
        nodes = [_node(*[f"n{j}" for j in range(i + 1)], is_leaf=(i == 3)) for i in range(4)]
        children_by_ref = {str(nodes[i].ref): [nodes[i + 1]] for i in range(3)}
        provider = _RecordingProvider(nodes[0], children_by_ref)

        assert await find_node(provider, nodes[3].ref) == nodes[3]
        assert provider.children_calls == [str(n.ref) for n in nodes[:3]]

    async def test_target_not_present_anywhere_returns_none(self) -> None:
        root = _node("root")
        child = _node("root", "child", is_leaf=True)
        missing_ref = NodeRef("repo", ("root", "missing"))
        provider = _RecordingProvider(root, {str(root.ref): [child]})

        assert await find_node(provider, missing_ref) is None

    async def test_leaf_children_are_never_recursed_into(self) -> None:
        root = _node("root")
        leaf = _node("root", "leaf", is_leaf=True)
        missing_ref = NodeRef("repo", ("root", "missing"))
        provider = _RecordingProvider(root, {str(root.ref): [leaf]})

        assert await find_node(provider, missing_ref) is None
        assert provider.children_calls == [str(root.ref)]  # leaf's own children() never called

    async def test_never_recurses_into_a_non_matching_sibling_subtree(self) -> None:
        """The core fix this module exists for: an "expensive" non-leaf
        sibling listed *before* the real target must never have its own
        ``children()`` called at all."""
        expensive_dir = _node("root", "expensive")
        expensive_child = _node("root", "expensive", "deep", is_leaf=True)
        target = _node("root", "target", is_leaf=True)
        root = _node("root")
        provider = _RecordingProvider(
            root,
            {
                str(root.ref): [expensive_dir, target],  # expensive listed FIRST
                str(expensive_dir.ref): [expensive_child],
            },
        )

        assert await find_node(provider, target.ref) == target
        assert provider.children_calls == [str(root.ref)]

    async def test_stops_paginating_a_wide_level_as_soon_as_the_match_is_found(self) -> None:
        """``find_node`` never needs a level's remaining siblings once
        the match is located — proven here by a level wider than
        ``_PAGE_SIZE`` where the match sits on the very first page."""
        from synology_apm_repo.sdk.units.resolve import _PAGE_SIZE

        target = _node("root", "target", is_leaf=True)
        siblings = [target] + [_node("root", f"other{i}", is_leaf=True) for i in range(_PAGE_SIZE * 3)]
        root = _node("root")
        provider = _RecordingProvider(root, {str(root.ref): siblings})

        assert await find_node(provider, target.ref) == target
        # Exactly one children() call (one page) -- never paged through
        # the remaining (_PAGE_SIZE * 3) irrelevant siblings.
        assert provider.children_calls == [str(root.ref)]

    async def test_continues_to_a_later_page_when_the_match_is_not_on_the_first(self) -> None:
        from synology_apm_repo.sdk.units.resolve import _PAGE_SIZE

        target = _node("root", "target", is_leaf=True)
        first_page = [_node("root", f"other{i}", is_leaf=True) for i in range(_PAGE_SIZE)]
        root = _node("root")
        provider = _RecordingProvider(root, {str(root.ref): [*first_page, target]})

        assert await find_node(provider, target.ref) == target
        assert provider.children_calls == [str(root.ref), str(root.ref)]  # two pages fetched

    async def test_a_leaf_that_is_only_a_partial_prefix_match_is_not_descended_into(self) -> None:
        """Defensive case: a well-formed provider never produces a leaf
        whose own ref is a *shorter*, non-equal prefix of the target's
        (a leaf has no children to descend into further) -- confirms
        the descent doesn't crash and simply reports no match rather
        than assuming every prefix match is safe to recurse into."""
        confused_leaf = _node("root", "branch", is_leaf=True)
        root = _node("root")
        missing_ref = NodeRef("repo", ("root", "branch", "deeper"))
        provider = _RecordingProvider(root, {str(root.ref): [confused_leaf]})

        assert await find_node(provider, missing_ref) is None

    async def test_a_root_marked_as_a_leaf_is_never_descended_into(self) -> None:
        """Defensive case: no real provider's root is ever a leaf, but
        the top-level entry point still guards against one rather than
        assuming it's safe to call children() on it."""
        leaf_root = _node("root", is_leaf=True)
        missing_ref = NodeRef("repo", ("root", "child"))
        provider = _RecordingProvider(leaf_root, {})

        assert await find_node(provider, missing_ref) is None
        assert provider.children_calls == []

    async def test_a_prefix_match_whose_own_subtree_never_actually_reaches_the_target(self) -> None:
        """A child's ref can be a genuine prefix of the target's without
        the target actually living under it (e.g. divergent siblings one
        level further down) -- the descent must propagate that
        "not found" back up rather than mistaking a shallow prefix hit
        for the real thing."""
        target_ref = NodeRef("repo", ("a", "b", "c"))
        root = _node("a")
        branch = _node("a", "b")
        unrelated_grandchild = _node("a", "b", "d", is_leaf=True)
        provider = _RecordingProvider(root, {str(root.ref): [branch], str(branch.ref): [unrelated_grandchild]})

        assert await find_node(provider, target_ref) is None
        assert provider.children_calls == [str(root.ref), str(branch.ref)]


class TestFindPathWithChildrenGenericDescent:
    async def test_root_itself_is_the_target(self) -> None:
        root = _node("root")
        provider = _RecordingProvider(root, {})

        assert await find_path_with_children(provider, root.ref) == ([root], [])
        assert provider.children_calls == []

    async def test_finds_a_target_nested_several_levels_deep(self) -> None:
        nodes = [_node(*[f"n{j}" for j in range(i + 1)], is_leaf=(i == 3)) for i in range(4)]
        children_by_ref = {str(nodes[i].ref): [nodes[i + 1]] for i in range(3)}
        provider = _RecordingProvider(nodes[0], children_by_ref)

        result = await find_path_with_children(provider, nodes[3].ref)

        assert result is not None
        chain, children_by_step = result
        assert chain == nodes
        assert children_by_step == [[nodes[1]], [nodes[2]], [nodes[3]]]

    async def test_target_not_present_anywhere_returns_none(self) -> None:
        root = _node("root")
        child = _node("root", "child", is_leaf=True)
        missing_ref = NodeRef("repo", ("root", "missing"))
        provider = _RecordingProvider(root, {str(root.ref): [child]})

        assert await find_path_with_children(provider, missing_ref) is None

    async def test_full_sibling_list_is_still_returned_even_though_the_match_is_never_re_scanned(self) -> None:
        """Unlike ``find_node``, this entry point must return every real
        sibling at each visited level (for the TUI to populate the
        widget with), even past the one that matched."""
        expensive_dir = _node("root", "expensive")
        target = _node("root", "target", is_leaf=True)
        root = _node("root")
        provider = _RecordingProvider(root, {str(root.ref): [target, expensive_dir]})  # target listed FIRST this time

        result = await find_path_with_children(provider, target.ref)

        assert result is not None
        chain, children_by_step = result
        assert chain == [root, target]
        assert children_by_step == [[target, expensive_dir]]
        # expensive_dir's own children() still never called -- it was
        # never a candidate to descend into, just a sibling to report.
        assert provider.children_calls == [str(root.ref)]


class TestSupportsDirectRefLookupDispatch:
    """Drive's shape: every node's ``extra_segments`` is a flat,
    depth-independent id, so the generic prefix descent above can't be
    used at all -- resolution goes through
    ``SupportsDirectRefLookup.resolve_extra``/
    ``SupportsDirectRefLookup.parent_of``
    instead."""

    def _provider(self) -> _FlatIdProvider:
        root = _node("root_sentinel")
        folder = _node("folder1")
        leaf = _node("leaf1", is_leaf=True)
        return _FlatIdProvider(
            nodes_by_id={"root_sentinel": root, "folder1": folder, "leaf1": leaf},
            parent_by_id={"folder1": "root_sentinel", "leaf1": "folder1"},
        )

    async def test_find_node_resolves_directly_without_any_children_call(self) -> None:
        provider = self._provider()
        leaf = provider._nodes_by_id["leaf1"]

        assert await find_node(provider, leaf.ref) == leaf

    async def test_find_node_returns_none_for_an_unknown_id(self) -> None:
        provider = self._provider()
        missing_ref = NodeRef("repo", ("does-not-exist",))

        assert await find_node(provider, missing_ref) is None

    async def test_find_path_with_children_rebuilds_the_nested_ancestor_chain(self) -> None:
        provider = self._provider()
        root, folder, leaf = (provider._nodes_by_id[k] for k in ("root_sentinel", "folder1", "leaf1"))

        result = await find_path_with_children(provider, leaf.ref)

        assert result is not None
        chain, children_by_step = result
        assert chain == [root, folder, leaf]
        assert children_by_step == [[folder], [leaf]]

    async def test_find_path_with_children_returns_none_for_an_unknown_id(self) -> None:
        provider = self._provider()
        missing_ref = NodeRef("repo", ("does-not-exist",))

        assert await find_path_with_children(provider, missing_ref) is None

    async def test_find_path_with_children_pages_through_a_wide_ancestor_level(self) -> None:
        """An ancestor's own sibling list is fetched via ``_all_children``
        on this path (not the prefix-matching walker) -- confirm it pages
        to full exhaustion rather than stopping at one page."""
        from synology_apm_repo.sdk.units.resolve import _PAGE_SIZE

        nodes_by_id = {"root_sentinel": _node("root_sentinel"), "leaf1": _node("leaf1", is_leaf=True)}
        parent_by_id: dict[str, str | None] = {"leaf1": "root_sentinel"}
        for i in range(_PAGE_SIZE):
            child_id = f"sibling{i}"
            nodes_by_id[child_id] = _node(child_id, is_leaf=True)
            parent_by_id[child_id] = "root_sentinel"
        provider = _FlatIdProvider(nodes_by_id, parent_by_id)
        leaf = nodes_by_id["leaf1"]

        result = await find_path_with_children(provider, leaf.ref)

        assert result is not None
        _, children_by_step = result
        assert len(children_by_step[0]) == _PAGE_SIZE + 1  # every sibling, both pages combined


__all__: list[str] = []
