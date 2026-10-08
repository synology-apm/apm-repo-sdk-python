"""Unit tests for ``synology_apm_repo.cli.commands.tree``: catalog-level tree
building (``_root_catalog_entries``/``_catalog_tree``/``_workload_entries``/
``_version_entries``) and the command's rendering of each frame it can land on."""

from __future__ import annotations

import asyncio
import json
from typing import Any, cast

import pytest
from inline_snapshot import snapshot

from support.cli import invoke
from support.fakes import faithful_to
from support.model_factories import (
    make_connection,
    make_version,
    make_workload,
)
from synology_apm_repo.cli.commands.tree import (
    TreeEntry,
    _catalog_tree,
    _root_catalog_entries,
    _version_entries,
    _workload_entries,
)
from synology_apm_repo.sdk.api import (
    Catalog,
    CatalogFrame,
    Connection,
    Frame,
    NodeFrame,
    RootFrame,
    Version,
    Workload,
    WorkloadFrame,
)
from synology_apm_repo.sdk.units.base import FileState, Node, UnitKind, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.provider_kit import diagnostic_node
from unit.cli.listing_fakes import (
    FakeCatalog,
    FakeRepo,
    FlatProvider,
    patch_walked_frame,
)

# -- _version_entries / _workload_entries --------------------------------


async def test_version_entries_are_leaves_named_by_disambiguated_display_name() -> None:
    versions = [
        make_version(version_id=1, display_name="a"),
        make_version(version_id=2, display_name="a"),
    ]
    catalog = FakeCatalog(make_connection(), versions=versions)
    entries = await _version_entries(catalog, make_workload())
    assert [e.is_leaf for e in entries] == [True, True]
    assert len({e.name for e in entries}) == 2  # disambiguation kept the names distinct


async def test_workload_entries_with_versions_recurses_into_version_entries() -> None:
    versions = [make_version(version_id=1, display_name="only")]
    workloads = [make_workload(workload_id=1)]
    catalog = FakeCatalog(make_connection(), workloads=workloads, versions=versions)
    entries = await _workload_entries(catalog, with_versions=True)
    assert len(entries) == 1
    assert entries[0].is_leaf is False
    assert [c.name for c in entries[0].children] == ["only"]


async def test_workload_entries_without_versions_leaves_children_empty() -> None:
    workloads = [make_workload(workload_id=1)]
    catalog = FakeCatalog(make_connection(), workloads=workloads)
    entries = await _workload_entries(catalog, with_versions=False)
    assert entries[0].children == []


async def test_workload_entries_disambiguates_colliding_display_names_by_sub_type_hint() -> None:
    """One SaaS account's MAIL/CALENDAR/CONTACT/DRIVE workloads share a display
    name; the ``sub_type`` hint, not a hash suffix, tells them apart."""
    workloads = [
        make_workload(workload_id=1, display_name="Alice", sub_type="MAIL"),
        make_workload(workload_id=2, display_name="Alice", sub_type="CALENDAR"),
        make_workload(workload_id=3, display_name="Alice", sub_type="CONTACT"),
        make_workload(workload_id=4, display_name="Alice", sub_type="DRIVE"),
    ]
    catalog = FakeCatalog(make_connection(), workloads=workloads)
    entries = await _workload_entries(catalog, with_versions=False)
    names = {e.name for e in entries}
    assert names == {"Alice · MAIL", "Alice · CALENDAR", "Alice · CONTACT", "Alice · DRIVE"}


class _EventGatedVersionsCatalog(FakeCatalog):
    """A ``FakeCatalog`` whose ``versions()`` resolves only once every
    workload's call has started, so a serial loop over workloads deadlocks."""

    def __init__(
        self, connection: Connection, *, workloads: list[Workload], started: list[str], release: asyncio.Event
    ) -> None:
        super().__init__(connection, workloads=workloads)
        self._started = started
        self._release = release

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        self._started.append(workload.display_name)
        if len(self._started) >= len(self._fake_workloads):
            self._release.set()
        else:
            await self._release.wait()
        return []


async def test_workload_entries_awaits_version_entries_concurrently_not_serially() -> None:
    started: list[str] = []
    workloads = [make_workload(workload_id=1, display_name="wl-a"), make_workload(workload_id=2, display_name="wl-b")]
    catalog = _EventGatedVersionsCatalog(
        make_connection(), workloads=workloads, started=started, release=asyncio.Event()
    )
    entries = await _workload_entries(catalog, with_versions=True)
    assert set(started) == {"wl-a", "wl-b"}
    # Order follows named_workloads(), not which versions() resolved first.
    assert [e.name for e in entries] == ["wl-a", "wl-b"]


# -- _catalog_tree ----------------------------------------------------------


async def test_catalog_tree_at_workload_level_depth_zero_has_no_children() -> None:
    catalog = FakeCatalog(make_connection())
    frame = WorkloadFrame(catalog, make_workload())
    entry = await _catalog_tree(frame, depth=0)
    assert entry == TreeEntry(name="Workload", kind="workload", is_leaf=False, children=[])


async def test_catalog_tree_at_workload_level_depth_one_lists_versions() -> None:
    catalog = FakeCatalog(make_connection(), versions=[make_version(version_id=1, display_name="v1")])
    frame = WorkloadFrame(catalog, make_workload())
    entry = await _catalog_tree(frame, depth=1)
    assert [c.name for c in entry.children] == ["v1"]


async def test_catalog_tree_at_catalog_level_depth_one_lists_workloads_without_versions() -> None:
    workloads = [make_workload(workload_id=1)]
    catalog = FakeCatalog(make_connection(), workloads=workloads)
    frame = CatalogFrame(catalog)
    entry = await _catalog_tree(frame, depth=1)
    assert len(entry.children) == 1
    assert entry.children[0].children == []  # depth 1 at catalog level: workloads, no versions yet


async def test_catalog_tree_at_catalog_level_depth_two_lists_workloads_with_versions() -> None:
    workloads = [make_workload(workload_id=1)]
    versions = [make_version(version_id=1, display_name="v1")]
    catalog = FakeCatalog(make_connection(), workloads=workloads, versions=versions)
    frame = CatalogFrame(catalog)
    entry = await _catalog_tree(frame, depth=2)
    assert [c.name for c in entry.children[0].children] == ["v1"]


# -- tree command: catalog/workload-level dispatch --------------------------


def test_tree_command_at_catalog_level_lists_workloads(monkeypatch: pytest.MonkeyPatch) -> None:
    workloads = [make_workload(workload_id=1, display_name="alpha")]
    catalog = FakeCatalog(make_connection(), workloads=workloads)
    frame = CatalogFrame(catalog)

    patch_walked_frame(monkeypatch, frame)

    result = invoke(["tree", "/some/path#Source", "--depth", "1"])
    assert result.stdout == "Source/\n  alpha/\n"
    assert result.stderr == ""


# -- _root_catalog_entries --------------------------------------------------
# A bare list of catalog entries, with no wrapping "/"-named TreeEntry.


async def test_root_catalog_entries_depth_zero_lists_catalogs_with_no_children() -> None:
    catalog = FakeCatalog(make_connection())
    repo = FakeRepo(catalogs=[catalog])
    entries = await _root_catalog_entries(cast(Any, repo), depth=0)
    assert entries == [TreeEntry(name="Source", kind="catalog", is_leaf=False, children=[])]


async def test_root_catalog_entries_depth_one_lists_workloads_without_versions() -> None:
    workloads = [make_workload(workload_id=1)]
    catalog = FakeCatalog(make_connection(), workloads=workloads)
    repo = FakeRepo(catalogs=[catalog])
    entries = await _root_catalog_entries(cast(Any, repo), depth=1)
    assert len(entries) == 1
    assert [c.name for c in entries[0].children] == ["Workload"]
    assert entries[0].children[0].children == []  # depth 1 at root: workloads, no versions yet


class _EventGatedWorkloadsCatalog(FakeCatalog):
    """``_EventGatedVersionsCatalog``'s technique one level up: ``workloads()``
    resolves only once every sibling catalog's call has started."""

    def __init__(self, connection: Connection, *, started: list[str], total: int, release: asyncio.Event) -> None:
        super().__init__(connection)
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
            make_connection(connection_config_id=1, display_name="cat-a"), started=started, total=2, release=release
        ),
        _EventGatedWorkloadsCatalog(
            make_connection(connection_config_id=2, display_name="cat-b"), started=started, total=2, release=release
        ),
    ]
    repo = FakeRepo(catalogs=cast(list[Catalog], catalogs))
    entries = await _root_catalog_entries(cast(Any, repo), depth=1)
    assert set(started) == {"cat-a", "cat-b"}
    assert [e.name for e in entries] == ["cat-a", "cat-b"]


# -- tree command: leaf-ref branch (a NodeFrame on a leaf) -------------------


def test_tree_command_on_a_single_leaf_ref_prints_just_that_item(monkeypatch: pytest.MonkeyPatch) -> None:
    leaf = Node(ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=42)
    frame = NodeFrame(cast(Any, object()), leaf)

    patch_walked_frame(monkeypatch, frame)

    result = invoke(["tree", "/some/path#item"])
    assert result.stdout == "item.bin\n"
    assert result.stderr == ""


def test_tree_command_on_a_single_leaf_ref_shows_the_cloud_file_icon(monkeypatch: pytest.MonkeyPatch) -> None:
    leaf = Node(
        ref=NodeRef("repo", ("item",)),
        name="item.bin",
        is_leaf=True,
        size=42,
        file_state=FileState.CLOUD_ONLY,
    )
    frame = NodeFrame(cast(Any, object()), leaf)

    patch_walked_frame(monkeypatch, frame)

    assert invoke(["tree", "/some/path#item"]).stdout == snapshot("item.bin ☁\n")
    assert invoke(["--json", "tree", "/some/path#item"]).stdout == snapshot("""\
[
  {
    "name": "item.bin",
    "kind": "item",
    "size": 42,
    "is_leaf": true,
    "file_state": "cloud_only",
    "children": []
  }
]
""")


def test_tree_command_on_a_single_leaf_ref_shows_the_diagnostic_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    leaf = diagnostic_node(NodeRef("repo", ("item",)), "(no filesystem recognized on this disk)", "x")
    frame = NodeFrame(cast(Any, object()), leaf)

    patch_walked_frame(monkeypatch, frame)

    assert invoke(["tree", "/some/path#item"]).stdout == snapshot("(no filesystem recognized on this disk) ⚠\n")
    assert invoke(["--json", "tree", "/some/path#item"]).stdout == snapshot("""\
[
  {
    "name": "(no filesystem recognized on this disk)",
    "kind": "file",
    "size": null,
    "is_leaf": true,
    "diagnostic": true,
    "children": []
  }
]
""")


# -- tree command: bare-root rendering (no synthetic "/" wrapper) -----------


def test_tree_command_at_bare_root_human_mode_has_no_synthetic_root_line(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = FakeCatalog(make_connection())
    repo = FakeRepo(catalogs=[catalog])
    frame = RootFrame()

    patch_walked_frame(monkeypatch, frame, repo)

    result = invoke(["tree", "/some/path", "--depth", "0"])
    assert result.stdout == "Source/\n"
    assert result.stderr == ""


def test_tree_command_at_bare_root_json_mode_is_a_plain_array(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = FakeCatalog(make_connection())
    repo = FakeRepo(catalogs=[catalog])
    frame = RootFrame()

    patch_walked_frame(monkeypatch, frame, repo)

    result = invoke(["--json", "tree", "/some/path", "--depth", "0"])
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["name"] == "Source"
    assert (data[0]["kind"], data[0]["size"]) == ("catalog", None)  # the keys ls --json gives a catalog row


# -- tree command: a version ref pointing at a provider's own root ----------
# That root's placeholder label (device.py's "Devices"/"Disks") is not
# printed as if it were an addressable entry.


def test_tree_command_on_a_version_ref_at_provider_root_peels_the_synthetic_heading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="Devices", is_leaf=False)
    child = Node(ref=NodeRef("repo", ("ver", "disk0")), name="disk0.img", is_leaf=True, size=1)
    provider = FlatProvider(root_node, [child])
    frame = NodeFrame(cast(Any, provider), root_node)

    patch_walked_frame(monkeypatch, frame)

    result = invoke(["tree", "/some/path#ver"])
    assert result.stdout == "disk0.img\n"
    assert result.stderr == ""


# -- tree command: per-row markers (Node.file_state, diagnostic placeholders) --


def test_tree_command_shows_the_cloud_file_icon_in_human_and_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    normal_child = Node(ref=NodeRef("repo", ("ver", "normal.txt")), name="normal.txt", is_leaf=True, size=1)
    cloud_child = Node(
        ref=NodeRef("repo", ("ver", "cloud.txt")),
        name="cloud.txt",
        is_leaf=True,
        size=1,
        file_state=FileState.CLOUD_ONLY,
    )
    provider = FlatProvider(root_node, [normal_child, cloud_child])
    frame = NodeFrame(cast(Any, provider), root_node)

    patch_walked_frame(monkeypatch, frame)

    assert invoke(["tree", "/some/path#ver"]).stdout == snapshot("""\
normal.txt
cloud.txt ☁
""")
    # frame.node is the provider's root, so its children print as a bare list.
    assert invoke(["--json", "tree", "/some/path#ver"]).stdout == snapshot("""\
[
  {
    "name": "normal.txt",
    "kind": "item",
    "size": 1,
    "is_leaf": true,
    "children": []
  },
  {
    "name": "cloud.txt",
    "kind": "item",
    "size": 1,
    "is_leaf": true,
    "file_state": "cloud_only",
    "children": []
  }
]
""")


def test_tree_command_shows_the_diagnostic_marker_in_human_and_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    normal_child = Node(ref=NodeRef("repo", ("ver", "normal.txt")), name="normal.txt", is_leaf=True, size=1)
    diagnostic_child = diagnostic_node(
        NodeRef("repo", ("ver", "(missing)")), "(1 registered object(s) not found in current data)", "x"
    )
    provider = FlatProvider(root_node, [normal_child, diagnostic_child])
    frame = NodeFrame(cast(Any, provider), root_node)

    patch_walked_frame(monkeypatch, frame)

    assert invoke(["tree", "/some/path#ver"]).stdout == snapshot("""\
normal.txt
(1 registered object(s) not found in current data) ⚠
""")
    assert invoke(["--json", "tree", "/some/path#ver"]).stdout == snapshot("""\
[
  {
    "name": "normal.txt",
    "kind": "item",
    "size": 1,
    "is_leaf": true,
    "children": []
  },
  {
    "name": "(1 registered object(s) not found in current data)",
    "kind": "file",
    "size": null,
    "is_leaf": true,
    "diagnostic": true,
    "children": []
  }
]
""")


# -- tree command: unit-kind parity with `ls --json` ------------------------


def test_tree_command_json_reports_kind_matching_ls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both commands take ``kind`` from ``units.base.node_kind_label``."""
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    mail = Node(ref=NodeRef("repo", ("ver", "msg")), name="msg", is_leaf=True, kind=UnitKind.MAIL)
    folder = Node(ref=NodeRef("repo", ("ver", "sub")), name="sub", is_leaf=False)
    provider = FlatProvider(root_node, [mail, folder])
    frame = NodeFrame(cast(Any, provider), root_node)

    patch_walked_frame(monkeypatch, frame)

    json_result = invoke(["--json", "tree", "/some/path#ver", "--depth", "0"])
    data = json.loads(json_result.output)
    entries_by_name = {e["name"]: e for e in data}
    assert entries_by_name["msg"]["kind"] == "mail"
    assert entries_by_name["sub"]["kind"] == "folder"


# -- tree command: a nested item tree, --depth and --ref --------------------


@faithful_to(UnitProvider)
class _NestedProvider:
    """A provider whose ``children()`` answers per parent node (ref-keyed), unlike ``FlatProvider``."""

    def __init__(self, root_node: Node, children_by_ref: dict[NodeRef, list[Node]]) -> None:
        self._root_node = root_node
        self._children_by_ref = children_by_ref

    def root(self) -> Node:
        return self._root_node

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children_by_ref.get(node.ref, [])

    async def unit(self, node: Node) -> Any:
        raise NotImplementedError


def _nested_frame() -> Frame:
    """A frame on ``ver``, below the provider's root, so ``tree`` prints it as the top entry."""
    version = Node(ref=NodeRef("repo", ("ver",)), name="ver", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("ver", "d")), name="d", is_leaf=False)
    nested = Node(ref=NodeRef("repo", ("ver", "d", "f.txt")), name="f.txt", is_leaf=True, size=5)
    top = Node(ref=NodeRef("repo", ("ver", "g.txt")), name="g.txt", is_leaf=True, size=1)
    provider = _NestedProvider(
        Node(ref=NodeRef("repo", ()), name="Devices", is_leaf=False),
        {version.ref: [folder, top], folder.ref: [nested]},
    )
    return NodeFrame(cast(Any, provider), version)


def _invoke_nested(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> Any:
    frame = _nested_frame()

    patch_walked_frame(monkeypatch, frame)
    result = invoke(args)
    assert result.stderr == ""
    return result


@pytest.mark.parametrize(
    ("depth", "expected"),
    [
        ("0", "ver/\n"),
        ("1", "ver/\n  d/\n  g.txt\n"),
        ("2", "ver/\n  d/\n    f.txt\n  g.txt\n"),
    ],
)
def test_tree_depth_bounds_how_far_the_item_tree_is_walked(
    monkeypatch: pytest.MonkeyPatch, depth: str, expected: str
) -> None:
    result = _invoke_nested(monkeypatch, ["tree", "/p#ver", "--depth", depth])

    assert result.stdout == expected


def test_tree_ref_adds_each_entrys_canonical_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _invoke_nested(monkeypatch, ["tree", "/p#ver", "--ref"])

    assert result.stdout == "ver/  /p#ver\n  d/  /p#ver/d\n    f.txt  /p#ver/d/f.txt\n  g.txt  /p#ver/g.txt\n"


def test_tree_ref_in_json_adds_a_ref_to_every_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _invoke_nested(monkeypatch, ["--json", "tree", "/p#ver", "--depth", "1", "--ref"])

    assert json.loads(result.stdout) == [
        {
            "name": "ver",
            "kind": "folder",
            "size": None,
            "is_leaf": False,
            "ref": "/p#ver",
            "children": [
                {"name": "d", "kind": "folder", "size": None, "is_leaf": False, "ref": "/p#ver/d", "children": []},
                {"name": "g.txt", "kind": "item", "size": 1, "is_leaf": True, "ref": "/p#ver/g.txt", "children": []},
            ],
        }
    ]


def test_tree_json_without_ref_leaves_the_ref_out(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _invoke_nested(monkeypatch, ["--json", "tree", "/p#ver", "--depth", "0"])

    assert json.loads(result.stdout) == [
        {
            "name": "ver",
            "kind": "folder",
            "size": None,
            "is_leaf": False,
            "children": [],
        }
    ]
