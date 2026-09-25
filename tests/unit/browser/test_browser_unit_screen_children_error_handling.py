"""Unit tests for ``UnitScreen``'s ``_load_children`` exception handling —
driven through a real Textual ``Pilot`` (``app.run_test()``), against a
fake provider whose ``children()`` raises either something other than
``ApmRepoError`` (a third-party parser failure) or ``KeyRequiredError``
(expanding a folder in an encrypted-but-no-key-yet repository).
"""

from __future__ import annotations

from typing import Any, cast

from textual.app import App, ComposeResult
from textual.widgets import Tree

from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Repository, Session, Version
from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _version() -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid-1"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type="VM",
        target_id=TargetId("target"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


class _RaisingProvider:
    """Mimics a real provider whose ``children()`` fails — either with a
    ``KeyRequiredError`` (an encrypted-but-no-key-yet repository, the documented
    trigger for this ``_load_children`` catch) or with something that
    isn't even an ``ApmRepoError`` at all — a real case: a third-party
    filesystem parser (e.g. ``dissect.apfs``) can
    raise its own exception type, which propagates all the way up through
    ``provider.children()``."""

    def __init__(self, root: Node, error: Exception) -> None:
        self._root = root
        self._error = error

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        raise self._error

    async def unit(self, node: Node) -> Node:
        return node


class _FakeCatalog:
    def __init__(self, provider: _RaisingProvider) -> None:
        self._provider = provider

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _RaisingProvider:
        return self._provider


class _FakeApp(App[None]):
    """A bare ``App`` (not ``ApmRepoBrowserApp``) is enough here —
    duplicated rather than imported, since no test module imports another.
    ``self.repo_handle`` only needs to resolve to something non-``None`` —
    none of this file's tests exercise ``action_refresh``'s
    ``invalidate_directory_cache()`` call, the one thing ``_load_root``
    itself still reads off it."""

    def __init__(self, version: Version, catalog: _FakeCatalog) -> None:
        super().__init__()
        self.verbose = False
        self.jobs: dict[JobId, Job] = {}
        self.resources = ResourceTable(cast(Session, object()))
        self.repo_handle: RepoHandle | None = self.resources.put_repo(cast(Repository, object()))
        self._version = version
        self._catalog = catalog

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(UnitScreen(self._catalog, self._version))  # type: ignore[arg-type]


async def test_a_non_apm_repo_error_from_children_shows_an_error_leaf_not_a_stuck_node(
    wait_until: Any, sdk_timeout: float
) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _RaisingProvider(root, EOFError("not enough bytes to read struct"))
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=sdk_timeout, interval=0.05)

        assert len(tree.root.children) == 1
        error_node = tree.root.children[0]
        assert error_node.data is not None
        assert error_node.data.payload is None  # no real Node -- selecting it is a no-op
        assert "error:" in str(error_node.label)
        assert "not enough bytes to read struct" in str(error_node.label)


async def test_key_required_from_children_shows_an_error_leaf_not_a_stuck_node(
    wait_until: Any, sdk_timeout: float
) -> None:
    """The documented trigger this catch exists for: expanding a folder
    in an encrypted-but-no-key-yet repository raises ``KeyRequiredError`` (an
    ``ApmRepoError`` subclass), which must get the same error-leaf
    treatment as any other exception here, not a stuck-forever tree
    node."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _RaisingProvider(root, KeyRequiredError("vault key required"))
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0, timeout=sdk_timeout, interval=0.05)

        assert len(tree.root.children) == 1
        error_node = tree.root.children[0]
        assert error_node.data is not None
        assert error_node.data.payload is None  # no real Node -- selecting it is a no-op
        assert "error:" in str(error_node.label)
        assert "vault key required" in str(error_node.label)
