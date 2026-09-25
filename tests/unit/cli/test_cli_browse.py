"""Unit tests for ``synology_apm_repo.cli.browse`` — ref parsing and tree
walking shared by ``ls``/``tree``/``cat``/``export``."""

from __future__ import annotations

from typing import Any, cast

import pytest

from synology_apm_repo.cli.browse import ParsedRef, parse_ref_argument, walk_ref
from synology_apm_repo.sdk.api import Catalog, Connection, Repository
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import ConnectionConfigId, ConnectionId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

# -- parse_ref_argument -------------------------------------------------


def test_parse_ref_argument_bare_path_has_empty_segments() -> None:
    parsed = parse_ref_argument("/some/path")
    assert parsed == ParsedRef(fs_path="/some/path", node_ref=NodeRef("/some/path", ()))


def test_parse_ref_argument_splits_on_first_hash() -> None:
    parsed = parse_ref_argument("/some/path#Test-Workload-02/my-vm")
    assert parsed.fs_path == "/some/path"
    assert parsed.node_ref.segments == ("Test-Workload-02", "my-vm")


# -- walk_ref() ------------------------------------------------------------
#
# The human-ref walk itself (backup source -> workload -> version -> item
# tree) is Repository.walk_human_ref()'s own behavior, tested directly
# against the SDK in tests/unit/sdk/test_api.py; the tests here only cover
# walk_ref()'s own job of picking human-ref vs. canonical/raw-ref
# resolution and wrapping either into the same Frame shape.


class _FakeDedupRepo:
    """A minimal stand-in for ``dedup.repository.DedupRepo``, exposing
    just what ``Catalog.catalog_id`` reads (``layout.repo_id``)."""

    def __init__(self) -> None:
        self.layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")


def _fake_object_store() -> ObjectStore:
    return cast(ObjectStore, object())


def _make_connection(ccid: int = 1) -> Connection:
    return Connection(
        connection_config_id=ConnectionConfigId(ccid),
        connection_id=ConnectionId("cc"),
        display_name="Source",
        namespaces=(),
        workload_count=1,
        version_count=1,
    )


def _make_catalog(connection: Connection) -> Catalog:
    dedup_repo = cast(DedupRepo, _FakeDedupRepo())
    return Catalog(
        dedup_repo,
        connection,
        saas_streams=SaasStreamCache(dedup_repo),
        track=lambda p: p,
        require_key_verified=lambda: None,
    )


class _FakeProvider:
    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]]) -> None:
        self._root = root
        self._children_by_ref = children_by_ref

    def root(self) -> Node:
        # ``UnitProvider.root()`` stays synchronous; ``children()``/``unit()`` are async.
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children_by_ref.get(str(node.ref), [])

    async def unit(self, node: Node) -> Node:
        return node


def _repo_with(monkeypatch: pytest.MonkeyPatch, *, catalogs: list[Catalog] | None = None) -> Repository:
    layout = RepositoryLayout(kind=RepoKind.VAULT, repo_root="")
    repo = Repository(_fake_object_store(), layout, keys=None, key_verification=None)

    async def fake_catalogs() -> list[Catalog]:
        return catalogs or []

    monkeypatch.setattr(repo, "catalogs", fake_catalogs)
    return repo


async def test_walk_ref_human_delegates_to_repository_walk_human_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = _make_catalog(_make_connection())
    repo = _repo_with(monkeypatch, catalogs=[catalog])
    node_ref = NodeRef.human("", "Source")
    frame = await walk_ref(repo, node_ref)
    assert frame.level == "catalog"
    assert frame.catalog is catalog


async def test_walk_ref_raw_resolves_via_repository_resolve(monkeypatch: pytest.MonkeyPatch) -> None:
    leaf = Node(ref=NodeRef.raw("", "some/path"), name="path", is_leaf=True)
    layout = RepositoryLayout(kind=RepoKind.VAULT, repo_root="")
    repo = Repository(_fake_object_store(), layout, keys=None, key_verification=None)
    file_map_provider = _FakeProvider(leaf, {})

    async def fake_resolve(ref: object, *, object_db_id: str | None = None) -> Node:
        return leaf

    monkeypatch.setattr(repo, "resolve", fake_resolve)

    async def fake_file_map_tree() -> _FakeProvider:
        return file_map_provider

    monkeypatch.setattr(repo, "file_map_tree", fake_file_map_tree)
    node_ref = NodeRef.raw("", "some/path")
    frame = await walk_ref(repo, node_ref)
    assert frame.level == "node"
    assert frame.node is leaf
    assert cast(Any, frame.provider) is file_map_provider
