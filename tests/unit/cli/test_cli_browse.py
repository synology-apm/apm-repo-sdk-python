"""Unit tests for ``synology_apm_repo.cli.browse``'s REF parsing, and for
``Repository.locate`` landing on the frames the listing commands branch on."""

from __future__ import annotations

from typing import Any, cast

import pytest

from support.fakes import faithful_to
from support.model_factories import make_catalog, make_connection
from synology_apm_repo.cli.browse import ParsedRef, parse_ref_argument
from synology_apm_repo.sdk.api import Catalog, CatalogFrame, NodeFrame, Repository
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from synology_apm_repo.sdk.units.base import Node, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.cli.listing_fakes import (
    FakeDedupRepo,
)

# -- parse_ref_argument -------------------------------------------------


def test_parse_ref_argument_bare_path_has_empty_segments() -> None:
    parsed = parse_ref_argument("/some/path")
    assert parsed == ParsedRef(fs_path="/some/path", node_ref=NodeRef("/some/path", ()))


def test_parse_ref_argument_splits_on_first_hash() -> None:
    parsed = parse_ref_argument("/some/path#Test-Workload-02/my-vm")
    assert parsed.fs_path == "/some/path"
    assert parsed.node_ref.segments == ("Test-Workload-02", "my-vm")


# -- Repository.locate() as the listing commands call it -------------------
#
# The human-ref walk itself is tested in tests/unit/sdk/test_api_repository.py;
# these tests cover a human ref and a raw ref landing on the frame the
# listing commands branch on.


def _fake_object_store() -> ObjectStore:
    return cast(ObjectStore, object())


@faithful_to(UnitProvider)
class _FakeProvider:
    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]]) -> None:
        self._root = root
        self._children_by_ref = children_by_ref

    def root(self) -> Node:
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


async def test_locate_human_ref_stops_at_the_catalog_it_names(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = make_catalog(make_connection(), dedup_repo=cast(DedupRepo, FakeDedupRepo()))
    repo = _repo_with(monkeypatch, catalogs=[catalog])
    node_ref = NodeRef.human("", "Source")
    frame = await repo.locate(node_ref)
    assert isinstance(frame, CatalogFrame)
    assert frame.catalog is catalog


async def test_locate_raw_ref_reaches_its_file_map_node(monkeypatch: pytest.MonkeyPatch) -> None:
    leaf = Node(ref=NodeRef.raw("", "some/path"), name="path", is_leaf=True)
    layout = RepositoryLayout(kind=RepoKind.VAULT, repo_root="")
    repo = Repository(_fake_object_store(), layout, keys=None, key_verification=None)
    file_map_provider = _FakeProvider(leaf, {})

    async def fake_file_map_tree() -> _FakeProvider:
        return file_map_provider

    monkeypatch.setattr(repo, "file_map_tree", fake_file_map_tree)
    node_ref = NodeRef.raw("", "some/path")
    frame = await repo.locate(node_ref)
    assert isinstance(frame, NodeFrame)
    assert frame.node is leaf
    assert cast(Any, frame.provider) is file_map_provider
