"""Unit tests for ``synology_apm_repo.cli.commands.tree``'s catalog-level
tree building (``_catalog_tree``/``_workload_entries``/``_version_entries``)
and the ``tree`` command's own leaf-ref branch — none of which any existing
``tests/integration/cli/test_cli_tree.py`` scenario reaches, since
every real fixture's REF there resolves to a folder-shaped item, not a
bare catalog/workload REF or a REF naming a single leaf directly."""

from __future__ import annotations

import asyncio
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
from synology_apm_repo.sdk.units.base import FileState, Node, UnitKind, diagnostic_node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

runner = CliRunner()


def _make_connection(ccid: int = 1, *, display_name: str = "Source") -> Connection:
    return Connection(
        connection_config_id=ConnectionConfigId(ccid),
        connection_id=ConnectionId("cc"),
        display_name=display_name,
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
        dedup_repo = cast(DedupRepo, _FakeDedupRepo())
        super().__init__(
            dedup_repo,
            connection,
            saas_streams=SaasStreamCache(dedup_repo),
            track=lambda p: p,
            require_key_verified=lambda: None,
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
    collision instead of falling back straight to a hash suffix."""
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


class _EventGatedVersionsCatalog(Catalog):
    """A real ``Catalog`` whose ``versions()`` only resolves once every
    sibling workload's own call has started -- proves
    ``_workload_entries``' per-workload ``_version_entries`` calls run
    concurrently, not one at a time (a serial ``for`` loop would deadlock
    here instead, the same way ``test_cli_doctor_report.py``'s
    ``_EventGatedCatalog`` proves it for ``doctor``'s own gather)."""

    def __init__(
        self, connection: Connection, *, workloads: list[Workload], started: list[str], release: asyncio.Event
    ) -> None:
        dedup_repo = cast(DedupRepo, _FakeDedupRepo())
        super().__init__(
            dedup_repo,
            connection,
            saas_streams=SaasStreamCache(dedup_repo),
            track=lambda p: p,
            require_key_verified=lambda: None,
        )
        self._fake_workloads = workloads
        self._started = started
        self._release = release

    async def workloads(self) -> list[Workload]:
        return self._fake_workloads

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        self._started.append(workload.display_name)
        if len(self._started) >= len(self._fake_workloads):
            self._release.set()
        else:
            await self._release.wait()
        return []


async def test_workload_entries_awaits_version_entries_concurrently_not_serially() -> None:
    started: list[str] = []
    workloads = [_make_workload(1, display_name="wl-a"), _make_workload(2, display_name="wl-b")]
    catalog = _EventGatedVersionsCatalog(
        _make_connection(), workloads=workloads, started=started, release=asyncio.Event()
    )
    entries = await _workload_entries(catalog, with_versions=True)
    assert set(started) == {"wl-a", "wl-b"}
    # Order matches named_workloads()'s own order regardless of which
    # sibling's versions() actually resolved first.
    assert [e.name for e in entries] == ["wl-a", "wl-b"]


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
    # "root"/"node" frames are handled by _result_for_frame()'s own
    # match statement (its "root"/"node" cases return directly) before
    # falling through to its catch-all case, which is the only one that
    # calls _catalog_tree -- this pins down the defensive assertion for
    # that invariant, not a real reachable-in-production path.
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
# The true bare-root case: a bare list of catalog entries, no wrapping
# "/"-named TreeEntry, matching _catalog_tree's own "catalog"/"workload"
# cases in always showing themselves regardless of --depth.


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


class _EventGatedWorkloadsCatalog(Catalog):
    """A real ``Catalog`` whose ``workloads()`` only resolves once every
    sibling catalog's own call has started -- proves
    ``_root_catalog_entries``' per-catalog ``_workload_entries`` calls run
    concurrently (same technique as
    ``_EventGatedVersionsCatalog`` above, one level up)."""

    def __init__(self, connection: Connection, *, started: list[str], total: int, release: asyncio.Event) -> None:
        dedup_repo = cast(DedupRepo, _FakeDedupRepo())
        super().__init__(
            dedup_repo,
            connection,
            saas_streams=SaasStreamCache(dedup_repo),
            track=lambda p: p,
            require_key_verified=lambda: None,
        )
        self._started = started
        self._total = total
        self._release = release

    async def workloads(self) -> list[Workload]:
        self._started.append(self.connection.display_name)
        if len(self._started) >= self._total:
            self._release.set()
        else:
            await self._release.wait()
        return []


async def test_root_catalog_entries_awaits_workload_entries_concurrently_not_serially() -> None:
    started: list[str] = []
    release = asyncio.Event()
    catalogs = [
        _EventGatedWorkloadsCatalog(
            _make_connection(1, display_name="cat-a"), started=started, total=2, release=release
        ),
        _EventGatedWorkloadsCatalog(
            _make_connection(2, display_name="cat-b"), started=started, total=2, release=release
        ),
    ]
    repo = _FakeRepo(catalogs=cast(list[Catalog], catalogs))
    entries = await _root_catalog_entries(cast(Any, repo), depth=1)
    assert set(started) == {"cat-a", "cat-b"}
    assert [e.name for e in entries] == ["cat-a", "cat-b"]


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


def test_tree_command_on_a_single_leaf_ref_shows_the_cloud_file_icon(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression test: this branch (``frame.node.is_leaf`` — a REF naming
    one leaf directly, no ``_node_entry`` recursion at all) builds its own
    ``TreeEntry`` by hand rather than going through ``_node_entry``, so it
    must read ``Node.attrs["file_state"]`` itself too, the same way
    ``_node_entry`` already does — this is the one path that missed it."""
    leaf = Node(
        ref=NodeRef("repo", ("item",)),
        name="item.bin",
        is_leaf=True,
        size=42,
        attrs={"file_state": FileState.CLOUD_ONLY},
    )
    frame = Frame(level="node", node=leaf, provider=None)

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(object()))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["tree", "/some/path#item"])
    assert result.exit_code == 0, result.output
    assert "item.bin ☁" in result.output

    json_result = runner.invoke(app, ["--json", "tree", "/some/path#item"])
    assert json_result.exit_code == 0, json_result.output
    assert json.loads(json_result.output)["file_state"] == "cloud_only"


def test_tree_command_on_a_single_leaf_ref_shows_the_diagnostic_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same regression-branch reasoning as the cloud-icon test above, for
    ``diagnostic_node()``'s own placeholder attrs."""
    leaf = diagnostic_node(NodeRef("repo", ("item",)), "(no filesystem recognized on this disk)", {"diagnostic": "x"})
    frame = Frame(level="node", node=leaf, provider=None)

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(object()))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["tree", "/some/path#item"])
    assert result.exit_code == 0, result.output
    assert "(no filesystem recognized on this disk) ⚠" in result.output

    json_result = runner.invoke(app, ["--json", "tree", "/some/path#item"])
    assert json_result.exit_code == 0, json_result.output
    assert json.loads(json_result.output)["diagnostic"] is True


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


# -- tree command: cloud-sync/EFS hint (Node.attrs["file_state"]) -----------


def test_tree_command_shows_the_cloud_file_icon_in_human_and_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    normal_child = Node(ref=NodeRef("repo", ("ver", "normal.txt")), name="normal.txt", is_leaf=True, size=1)
    cloud_child = Node(
        ref=NodeRef("repo", ("ver", "cloud.txt")),
        name="cloud.txt",
        is_leaf=True,
        size=1,
        attrs={"file_state": FileState.CLOUD_ONLY},
    )
    provider = _FakeRootProvider(root_node, [normal_child, cloud_child])
    frame = Frame(level="node", node=root_node, provider=cast(Any, provider))

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(object()))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["tree", "/some/path#ver"])
    assert result.exit_code == 0, result.output
    assert "normal.txt\n" in result.output  # no hint suffix on this line
    assert "cloud.txt ☁" in result.output

    json_result = runner.invoke(app, ["--json", "tree", "/some/path#ver"])
    assert json_result.exit_code == 0, json_result.output
    data = json.loads(json_result.output)
    # frame.node is the provider's own root (test_tree_command_on_a_version_ref_at_provider_root_peels_the_synthetic_heading's
    # own scenario, above) -- its children print as a bare list, not wrapped in a single root entry.
    entries_by_name = {e["name"]: e for e in data}
    assert "file_state" not in entries_by_name["normal.txt"]
    assert entries_by_name["cloud.txt"]["file_state"] == "cloud_only"


def test_tree_command_shows_the_diagnostic_marker_in_human_and_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    normal_child = Node(ref=NodeRef("repo", ("ver", "normal.txt")), name="normal.txt", is_leaf=True, size=1)
    diagnostic_child = diagnostic_node(
        NodeRef("repo", ("ver", "(missing)")), "(1 registered object(s) not found in current data)", {"diagnostic": "x"}
    )
    provider = _FakeRootProvider(root_node, [normal_child, diagnostic_child])
    frame = Frame(level="node", node=root_node, provider=cast(Any, provider))

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(object()))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    result = runner.invoke(app, ["tree", "/some/path#ver"])
    assert result.exit_code == 0, result.output
    assert "normal.txt\n" in result.output  # no diagnostic marker on this line
    assert "(1 registered object(s) not found in current data) ⚠" in result.output

    json_result = runner.invoke(app, ["--json", "tree", "/some/path#ver"])
    assert json_result.exit_code == 0, json_result.output
    data = json.loads(json_result.output)
    entries_by_name = {e["name"]: e for e in data}
    assert "diagnostic" not in entries_by_name["normal.txt"]
    assert entries_by_name["(1 registered object(s) not found in current data)"]["diagnostic"] is True


# -- tree command: unit-kind parity with `ls --json` ------------------------


def test_tree_command_json_reports_kind_matching_ls(monkeypatch: pytest.MonkeyPatch) -> None:
    """``tree --json``'s per-node ``kind`` field must match ``ls --json``'s
    for the same node -- both derive it from the shared
    ``units.base.node_kind_label``."""
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    mail = Node(ref=NodeRef("repo", ("ver", "msg")), name="msg", is_leaf=True, kind=UnitKind.MAIL)
    folder = Node(ref=NodeRef("repo", ("ver", "sub")), name="sub", is_leaf=False)
    provider = _FakeRootProvider(root_node, [mail, folder])
    frame = Frame(level="node", node=root_node, provider=cast(Any, provider))

    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(tree_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(object()))
    monkeypatch.setattr(tree_module, "walk_ref", fake_walk_ref)

    json_result = runner.invoke(app, ["--json", "tree", "/some/path#ver", "--depth", "0"])
    assert json_result.exit_code == 0, json_result.output
    data = json.loads(json_result.output)
    entries_by_name = {e["name"]: e for e in data}
    assert entries_by_name["msg"]["kind"] == "mail"
    assert entries_by_name["sub"]["kind"] == "folder"


__all__: list[str] = []
