"""Unit tests for ``synology_apm_repo.cli.commands.tree``'s catalog-level
tree building (``_catalog_tree``/``_workload_entries``/``_version_entries``)
and the ``tree`` command's own leaf-ref branch — none of which any existing
``tests/integration/cli/test_cli_tree.py`` scenario reaches, since
every real fixture's REF there resolves to a folder-shaped item, not a
bare catalog/workload REF or a REF naming a single leaf directly."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.commands.tree as tree_module
from synology_apm_repo.cli.browse import Frame
from synology_apm_repo.cli.commands.tree import (
    TreeEntry,
    _catalog_tree,
    _root_catalog_entries,
    _version_entries,
    _workload_entries,
)
from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.api import Catalog, Connection, Version, Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    ConnectionId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
    WorkloadUid,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

runner = CliRunner()


def _make_connection(ccid: int = 1) -> Connection:
    return Connection(
        connection_config_id=ConnectionConfigId(ccid),
        connection_id=ConnectionId("cc"),
        display_name="Source",
        namespaces=(),
        workload_count=1,
        version_count=1,
    )


def _make_workload(workload_id: int = 1, *, display_name: str = "Workload", sub_type: str | None = None) -> Workload:
    return Workload(
        workload_id=WorkloadId(workload_id),
        workload_uid=WorkloadUid(f"wl-uid-{workload_id}"),
        workload_type="VM",
        sub_type=sub_type,
        display_name=display_name,
        subtitle=None,
        spec={},
    )


def _make_version(version_id: int = 1, name: str = "2026-01-01 00:00") -> Version:
    return Version(
        version_id=VersionId(version_id),
        version_uid=VersionUid(f"vuid-{version_id}"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type="VM",
        target_id=TargetId("target"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name=name,
        meta=None,
    )


class _FakeDedupRepo:
    """A minimal stand-in for ``dedup.repository.DedupRepo``, exposing
    just what ``Catalog.catalog_id`` reads (``layout.repo_id``)."""

    def __init__(self) -> None:
        self.layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")


class _FakeCatalog(Catalog):
    """A real ``Catalog`` with its ``workloads()``/``versions()``
    overridden to return fixed lists instead of reading a
    ``DedupRepo``."""

    def __init__(
        self,
        connection: Connection,
        *,
        workloads: list[Workload] | None = None,
        versions: list[Version] | None = None,
    ) -> None:
        super().__init__(
            cast(DedupRepo, _FakeDedupRepo()), connection, track=lambda p: p, require_key_verified=lambda: None
        )
        self._fake_workloads = workloads or []
        self._fake_versions = versions or []

    async def workloads(self) -> list[Workload]:
        return self._fake_workloads

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        return self._fake_versions


class _FakeRepo:
    def __init__(self, *, catalogs: list[Catalog] | None = None) -> None:
        self._catalogs = catalogs or []

    async def catalogs(self) -> list[Catalog]:
        return self._catalogs


# -- _version_entries / _workload_entries --------------------------------


async def test_version_entries_are_leaves_named_by_disambiguated_display_name() -> None:
    versions = [_make_version(1, "a"), _make_version(2, "a")]  # same display_name -> disambiguated
    catalog = _FakeCatalog(_make_connection(), versions=versions)
    entries = await _version_entries(catalog, _make_workload())
    assert [e.is_leaf for e in entries] == [True, True]
    assert len({e.name for e in entries}) == 2  # disambiguation kept the names distinct


async def test_workload_entries_with_versions_recurses_into_version_entries() -> None:
    versions = [_make_version(1, "only")]
    workloads = [_make_workload(1)]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads, versions=versions)
    entries = await _workload_entries(catalog, with_versions=True)
    assert len(entries) == 1
    assert entries[0].is_leaf is False
    assert [c.name for c in entries[0].children] == ["only"]


async def test_workload_entries_without_versions_leaves_children_empty() -> None:
    workloads = [_make_workload(1)]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads)
    entries = await _workload_entries(catalog, with_versions=False)
    assert entries[0].children == []


async def test_workload_entries_disambiguates_colliding_display_names_by_sub_type_hint() -> None:
    """Mirrors ``test_version_entries_are_leaves_named_by_disambiguated_
    display_name`` above, but at the workload level: several ``Workload``
    rows can share one real display name (a GWS/M365 account name/email
    reused across its MAIL/CALENDAR/CONTACT/DRIVE personas), so
    ``workload_pairs``'s own ``type_hint`` (``sub_type`` here) resolves the
    collision instead of falling back straight to a hash suffix — this is
    the one production code path (`disambiguate(pairs, hints=hints)` in
    ``_workload_entries``) that only ``tests/integration/cli/
    test_cli_tree.py``'s real-fixture replay exercised before this."""
    workloads = [
        _make_workload(1, display_name="Alice", sub_type="MAIL"),
        _make_workload(2, display_name="Alice", sub_type="CALENDAR"),
        _make_workload(3, display_name="Alice", sub_type="CONTACT"),
        _make_workload(4, display_name="Alice", sub_type="DRIVE"),
    ]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads)
    entries = await _workload_entries(catalog, with_versions=False)
    names = {e.name for e in entries}
    assert names == {"Alice · MAIL", "Alice · CALENDAR", "Alice · CONTACT", "Alice · DRIVE"}


# -- _catalog_tree ----------------------------------------------------------


async def test_catalog_tree_at_workload_level_depth_zero_has_no_children() -> None:
    catalog = _FakeCatalog(_make_connection())
    frame = Frame(level="workload", catalog=catalog, workload=_make_workload())
    entry = await _catalog_tree(frame, depth=0)
    assert entry == TreeEntry(name="Workload", is_leaf=False, children=[])


async def test_catalog_tree_at_workload_level_depth_one_lists_versions() -> None:
    catalog = _FakeCatalog(_make_connection(), versions=[_make_version(1, "v1")])
    frame = Frame(level="workload", catalog=catalog, workload=_make_workload())
    entry = await _catalog_tree(frame, depth=1)
    assert [c.name for c in entry.children] == ["v1"]


async def test_catalog_tree_at_catalog_level_depth_one_lists_workloads_without_versions() -> None:
    workloads = [_make_workload(1)]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads)
    frame = Frame(level="catalog", catalog=catalog)
    entry = await _catalog_tree(frame, depth=1)
    assert len(entry.children) == 1
    assert entry.children[0].children == []  # depth 1 at catalog level: workloads, no versions yet


async def test_catalog_tree_at_catalog_level_depth_two_lists_workloads_with_versions() -> None:
    workloads = [_make_workload(1)]
    versions = [_make_version(1, "v1")]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads, versions=versions)
    frame = Frame(level="catalog", catalog=catalog)
    entry = await _catalog_tree(frame, depth=2)
    assert [c.name for c in entry.children[0].children] == ["v1"]


async def test_catalog_tree_rejects_a_frame_level_it_does_not_handle() -> None:
    # "root"/"node" frames are handled by tree()'s own dispatch before
    # _catalog_tree is ever called (see its own docstring) -- this pins
    # down the defensive assertion for that invariant, not a real
    # reachable-in-production path.
    with pytest.raises(AssertionError, match="unexpected frame.level"):
        await _catalog_tree(Frame(level="root"), depth=0)


# -- tree command: catalog/workload-level dispatch --------------------------


class _FakeRepoCtx:
    def __init__(self, repo: object) -> None:
        self._repo = repo

    async def __aenter__(self) -> object:
        return self._repo

    async def __aexit__(self, *exc: object) -> None:
        return None


def test_tree_command_at_catalog_level_lists_workloads(monkeypatch: pytest.MonkeyPatch) -> None:
    workloads = [_make_workload(1, display_name="alpha")]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads)
    frame = Frame(level="catalog", catalog=catalog)

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(_FakeRepo()))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["tree", "/some/path#Source", "--depth", "1"])
    assert result.exit_code == 0, result.output
    assert "alpha" in result.output


# -- _root_catalog_entries --------------------------------------------------
# The true bare-root case: no wrapping "/"-named TreeEntry (that used to
# print as literal "//" and leak a fake object into --json) -- a bare
# list of catalog entries instead, matching _catalog_tree's own
# "catalog"/"workload" cases in always showing themselves regardless of
# --depth.


async def test_root_catalog_entries_depth_zero_lists_catalogs_with_no_children() -> None:
    catalog = _FakeCatalog(_make_connection())
    repo = _FakeRepo(catalogs=[catalog])
    entries = await _root_catalog_entries(cast(Any, repo), depth=0)
    assert entries == [TreeEntry(name="Source", is_leaf=False, children=[])]


async def test_root_catalog_entries_depth_one_lists_workloads_without_versions() -> None:
    workloads = [_make_workload(1)]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads)
    repo = _FakeRepo(catalogs=[catalog])
    entries = await _root_catalog_entries(cast(Any, repo), depth=1)
    assert len(entries) == 1
    assert [c.name for c in entries[0].children] == ["Workload"]
    assert entries[0].children[0].children == []  # depth 1 at root: workloads, no versions yet


# -- tree command: leaf-ref branch (frame.level == "node", is_leaf) ------


def test_tree_command_on_a_single_leaf_ref_prints_just_that_item(monkeypatch: pytest.MonkeyPatch) -> None:
    leaf = Node(ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=42)
    frame = Frame(level="node", node=leaf, provider=None)

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(object()))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["tree", "/some/path#item"])
    assert result.exit_code == 0, result.output
    assert "item.bin" in result.output
    # A leaf entry has no children — nothing recursed into via _node_entry.
    assert result.output.count("\n") == 1


# -- tree command: bare-root rendering (no synthetic "/" wrapper) -----------


def test_tree_command_at_bare_root_human_mode_has_no_synthetic_root_line(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = _FakeCatalog(_make_connection())
    repo = _FakeRepo(catalogs=[catalog])
    frame = Frame(level="root")

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(repo))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["tree", "/some/path", "--depth", "0"])
    assert result.exit_code == 0, result.output
    assert "//" not in result.output
    assert "Source" in result.output


def test_tree_command_at_bare_root_json_mode_is_a_plain_array(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = _FakeCatalog(_make_connection())
    repo = _FakeRepo(catalogs=[catalog])
    frame = Frame(level="root")

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(repo))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["--json", "tree", "/some/path", "--depth", "0"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["name"] == "Source"


# -- tree command: a version ref pointing at a provider's own root ----------
# (e.g. a real PC/VM version with no item-level segments) must not print
# that root's own placeholder label (device.py's "Devices"/"Disks",
# fs.py's "/") as if it were an ordinary, addressable child.


class _FakeRootProvider:
    def __init__(self, root_node: Node, children: list[Node]) -> None:
        self._root_node = root_node
        self._children = children

    def root(self) -> Node:
        return self._root_node

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children if node == self._root_node else []

    async def unit(self, node: Node) -> Any:
        raise NotImplementedError


def test_tree_command_on_a_version_ref_at_provider_root_peels_the_synthetic_heading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="Devices", is_leaf=False)
    child = Node(ref=NodeRef("repo", ("ver", "disk0")), name="disk0.img", is_leaf=True, size=1)
    provider = _FakeRootProvider(root_node, [child])
    frame = Frame(level="node", node=root_node, provider=cast(Any, provider))

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(object()))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["tree", "/some/path#ver"])
    assert result.exit_code == 0, result.output
    assert "disk0.img" in result.output
    assert "Devices" not in result.output


__all__: list[str] = []
