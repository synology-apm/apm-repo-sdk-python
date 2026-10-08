"""``Pilot`` tests for ``UnitEffects._load_children``'s exception handling
in ``UnitScreen``: a provider whose ``children()`` raises a
non-``ApmRepoError`` (a third-party parser failure) or ``KeyRequiredError``
(an encrypted repository with no key yet) gets an error leaf.
"""

from __future__ import annotations

from textual.widgets import Tree

from support.fakes import faithful_to
from support.model_factories import make_version
from support.pilot import SDK_TIMEOUT, wait_until
from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.units.base import Node, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    FakeApp,
    FakeRepo,
)


@faithful_to(UnitProvider)
class _RaisingProvider:
    def __init__(self, root: Node, error: Exception) -> None:
        self._root = root
        self._error = error

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        raise self._error

    async def unit(self, node: Node) -> Node:
        return node


async def test_a_non_apm_repo_error_from_children_shows_an_error_leaf_not_a_stuck_node() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _RaisingProvider(root, EOFError("not enough bytes to read struct"))
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=SDK_TIMEOUT, interval=0.05)

        assert len(tree.root.children) == 1
        error_node = tree.root.children[0]
        assert error_node.data is not None
        assert error_node.data.payload is None  # no real Node -- selecting it is a no-op
        assert "error:" in str(error_node.label)
        assert "not enough bytes to read struct" in str(error_node.label)


async def test_key_required_from_children_shows_an_error_leaf_not_a_stuck_node() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _RaisingProvider(root, KeyRequiredError("vault key required"))
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=SDK_TIMEOUT, interval=0.05)

        assert len(tree.root.children) == 1
        error_node = tree.root.children[0]
        assert error_node.data is not None
        assert error_node.data.payload is None  # no real Node -- selecting it is a no-op
        assert "error:" in str(error_node.label)
        assert "vault key required" in str(error_node.label)
