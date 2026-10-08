"""Unit tests for ``synology_apm_repo.sdk.units.resolve`` — the
prefix-guided descent behind ``Repository.resolve`` and the TUI's goto-ref,
plus the ``SupportsDirectRefLookup`` dispatch for a provider whose
``extra_segments`` don't grow with depth (Drive's shape, here a fake)."""

from __future__ import annotations

from support.fakes import faithful_to
from synology_apm_repo.sdk.units.base import Node, RestorableUnit, SupportsDirectRefLookup, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.resolve import find_node, find_path_with_children


@faithful_to(UnitProvider)
class _RecordingProvider:
    """A ``UnitProvider`` over a hand-built ``ref -> children`` map that
    records the ref of every ``children()`` call."""

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


@faithful_to(UnitProvider, SupportsDirectRefLookup)
class _FlatIdProvider:
    """A ``SupportsDirectRefLookup`` provider with flat, depth-independent
    ids, as Drive's are."""

    def __init__(self, nodes_by_id: dict[str, Node], parent_by_id: dict[str, str | None]) -> None:
        self._nodes_by_id = nodes_by_id
        self._parent_by_id = parent_by_id
        self.children_calls: list[str] = []

    def root(self) -> Node:
        raise NotImplementedError

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        self.children_calls.append(str(node.ref))
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
        assert provider.children_calls == [str(root.ref)]  # the leaf is never listed

    async def test_never_recurses_into_a_non_matching_sibling_subtree(self) -> None:
        """A non-leaf sibling listed before the target is never listed."""
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
        """A level wider than ``_PAGE_SIZE`` whose match is on the first
        page."""
        from synology_apm_repo.sdk.units.resolve import _PAGE_SIZE

        target = _node("root", "target", is_leaf=True)
        siblings = [target] + [_node("root", f"other{i}", is_leaf=True) for i in range(_PAGE_SIZE * 3)]
        root = _node("root")
        provider = _RecordingProvider(root, {str(root.ref): siblings})

        assert await find_node(provider, target.ref) == target
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
        """A leaf whose ref is a proper prefix of the target's, which a
        well-formed provider never produces."""
        confused_leaf = _node("root", "branch", is_leaf=True)
        root = _node("root")
        missing_ref = NodeRef("repo", ("root", "branch", "deeper"))
        provider = _RecordingProvider(root, {str(root.ref): [confused_leaf]})

        assert await find_node(provider, missing_ref) is None

    async def test_a_root_marked_as_a_leaf_is_never_descended_into(self) -> None:
        leaf_root = _node("root", is_leaf=True)
        missing_ref = NodeRef("repo", ("root", "child"))
        provider = _RecordingProvider(leaf_root, {})

        assert await find_node(provider, missing_ref) is None
        assert provider.children_calls == []

    async def test_a_prefix_match_whose_own_subtree_never_actually_reaches_the_target(self) -> None:
        """A child whose ref is a prefix of the target's, but whose subtree
        diverges one level down."""
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
        """Unlike ``find_node``, this returns every sibling at each visited
        level (the TUI repopulates its tree with them), but still lists no
        non-matching subtree."""
        expensive_dir = _node("root", "expensive")
        target = _node("root", "target", is_leaf=True)
        root = _node("root")
        provider = _RecordingProvider(root, {str(root.ref): [target, expensive_dir]})  # target listed FIRST this time

        result = await find_path_with_children(provider, target.ref)

        assert result is not None
        chain, children_by_step = result
        assert chain == [root, target]
        assert children_by_step == [[target, expensive_dir]]
        assert provider.children_calls == [str(root.ref)]


class TestSupportsDirectRefLookupDispatch:
    """Drive's shape: ``extra_segments`` is a flat, depth-independent id,
    so resolution goes through ``SupportsDirectRefLookup.resolve_extra``/
    ``parent_of`` instead of the prefix descent."""

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
        assert provider.children_calls == []

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
        """On this path an ancestor's children come from ``all_children``,
        which pages to exhaustion."""
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
