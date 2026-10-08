"""Unit tests for ``synology_apm_repo.cli.commands.ls``: the rows for each
frame ``Repository.locate`` can land on, the ids and refs ``--ref``/``--verbose``
add, and the per-row ``Node.file_state``/diagnostic markers.
The replay tests in ``tests/integration/cli/test_cli_commands_ls.py`` reach neither
the catalog/workload levels nor a cloud-sync placeholder file."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest

from support.cli import invoke
from support.model_factories import (
    make_connection,
    make_version,
    make_workload,
)
from synology_apm_repo.sdk.api import CatalogFrame, NodeFrame, RootFrame, WorkloadFrame
from synology_apm_repo.sdk.units.base import FileState, Node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.provider_kit import diagnostic_node
from unit.cli.listing_fakes import (
    FakeCatalog,
    FakeRepo,
    FlatProvider,
    patch_walked_frame,
)


def _stdout(result: Any) -> str:
    """The command's stdout, after checking it succeeded and printed nothing on stderr."""
    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    return str(result.stdout)


def test_ls_at_root_level_lists_the_catalogs(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = FakeRepo(catalogs=[FakeCatalog(make_connection(connection_config_id=3))])
    patch_walked_frame(monkeypatch, RootFrame(), repo)

    assert _stdout(invoke(["ls", "/some/path"], exit_code=None)) == "Source\n"


def test_ls_at_connection_level_lists_workloads(monkeypatch: pytest.MonkeyPatch) -> None:
    workloads = [make_workload(workload_id=1, display_name="alpha"), make_workload(workload_id=2, display_name="beta")]
    catalog = FakeCatalog(make_connection(), workloads=workloads)
    patch_walked_frame(monkeypatch, CatalogFrame(catalog))

    assert _stdout(invoke(["ls", "/some/path#Source"], exit_code=None)) == "alpha\nbeta\n"


def test_ls_at_workload_level_lists_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    versions = [
        make_version(version_id=1, display_name="2026-01-01 00:00"),
        make_version(version_id=2, display_name="2026-01-02 00:00"),
    ]
    catalog = FakeCatalog(make_connection(), versions=versions)
    patch_walked_frame(monkeypatch, WorkloadFrame(catalog, make_workload()))

    assert (
        _stdout(invoke(["ls", "/some/path#Source/Workload"], exit_code=None)) == "2026-01-01 00:00\n2026-01-02 00:00\n"
    )


def test_ls_renders_a_bracketed_display_name_literally_not_as_rich_markup(monkeypatch: pytest.MonkeyPatch) -> None:
    # A workload name Rich would otherwise parse as a markup tag and
    # silently drop must render in full, byte-for-byte.
    workloads = [make_workload(workload_id=1, display_name="[limitation] deep_hierarchy")]
    catalog = FakeCatalog(make_connection(), workloads=workloads)
    patch_walked_frame(monkeypatch, CatalogFrame(catalog))

    assert _stdout(invoke(["ls", "/some/path#Source"], exit_code=None)) == "[limitation] deep_hierarchy\n"


def test_ls_without_verbose_or_ref_omits_stable_id(monkeypatch: pytest.MonkeyPatch) -> None:
    versions = [make_version(version_id=7, display_name="2026-01-01 00:00")]
    catalog = FakeCatalog(make_connection(), versions=versions)
    patch_walked_frame(monkeypatch, WorkloadFrame(catalog, make_workload()))

    assert _stdout(invoke(["ls", "/some/path#Source/Workload"], exit_code=None)) == "2026-01-01 00:00\n"


# -- ls --ref / --verbose: the stable ids and canonical refs they add ----------


@pytest.mark.parametrize("flags", [["--verbose", "ls"], ["ls", "--ref"]], ids=["verbose", "ref"])
class TestStableIds:
    """``--verbose`` and ``ls --ref`` both add the internal id (``catalog_id``/``workload_id``/``version_id``)."""

    def test_at_root_and_connection_level(self, monkeypatch: pytest.MonkeyPatch, flags: list[str]) -> None:
        catalog = FakeCatalog(
            make_connection(connection_config_id=3),
            workloads=[
                make_workload(workload_id=5, display_name="alpha"),
                make_workload(workload_id=2, display_name="beta"),
            ],
        )
        repo = FakeRepo(catalogs=[catalog])

        patch_walked_frame(monkeypatch, RootFrame(), repo)
        # catalog_id falls back to connection_config_id (no repo_id on the fake)
        assert _stdout(invoke([*flags, "/some/path"], exit_code=None)) == "Source  id=3\n"

        patch_walked_frame(monkeypatch, CatalogFrame(catalog), repo)
        assert _stdout(invoke([*flags, "/some/path#Source"], exit_code=None)) == "alpha  id=5\nbeta  id=2\n"

    def test_at_workload_level(self, monkeypatch: pytest.MonkeyPatch, flags: list[str]) -> None:
        catalog = FakeCatalog(make_connection(), versions=[make_version(version_id=7, display_name="2026-01-01 00:00")])
        patch_walked_frame(monkeypatch, WorkloadFrame(catalog, make_workload()))

        assert _stdout(invoke([*flags, "/some/path#Source/Workload"], exit_code=None)) == "2026-01-01 00:00  id=7\n"


def test_ls_json_carries_the_id_only_with_ref_or_verbose(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = FakeCatalog(make_connection(), versions=[make_version(version_id=7, display_name="2026-01-01 00:00")])
    patch_walked_frame(monkeypatch, WorkloadFrame(catalog, make_workload()))
    row = {"name": "2026-01-01 00:00", "kind": "version", "size": None}

    assert json.loads(_stdout(invoke(["--json", "ls", "/p#Source/Workload"], exit_code=None))) == [row]
    assert json.loads(_stdout(invoke(["--json", "ls", "/p#Source/Workload", "--ref"], exit_code=None))) == [
        {**row, "id": 7}
    ]


def test_ls_ref_adds_each_items_canonical_ref_in_human_and_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    children = [
        Node(ref=NodeRef("repo", ("ver", "a.txt")), name="a.txt", is_leaf=True, size=1),
        Node(ref=NodeRef("repo", ("ver", "d")), name="d", is_leaf=False),
    ]
    provider = FlatProvider(root_node, children)
    patch_walked_frame(monkeypatch, NodeFrame(cast(Any, provider), root_node))

    assert _stdout(invoke(["ls", "/p#ver", "--ref"], exit_code=None)) == "a.txt (1 B)  /p#ver/a.txt\nd  /p#ver/d\n"
    assert json.loads(_stdout(invoke(["--json", "ls", "/p#ver", "--ref"], exit_code=None))) == [
        {"name": "a.txt", "kind": "item", "size": 1, "ref": "/p#ver/a.txt"},
        {"name": "d", "kind": "folder", "size": None, "ref": "/p#ver/d"},
    ]
    assert _stdout(invoke(["ls", "/p#ver"], exit_code=None)) == "a.txt (1 B)\nd\n"


def test_ls_ref_on_a_single_item_shows_just_that_item(monkeypatch: pytest.MonkeyPatch) -> None:
    item = Node(ref=NodeRef("repo", ("ver", "a.txt")), name="a.txt", is_leaf=True, size=1)
    provider = FlatProvider(item, [])
    patch_walked_frame(monkeypatch, NodeFrame(cast(Any, provider), item))

    assert _stdout(invoke(["ls", "/p#ver/a.txt", "--ref"], exit_code=None)) == "a.txt (1 B)  /p#ver/a.txt\n"


@pytest.mark.parametrize("flag", ["-q", "--quiet"])
def test_quiet_leaves_a_listing_alone(monkeypatch: pytest.MonkeyPatch, flag: str) -> None:
    """``--quiet`` only drops success confirmations; a command's primary report stays."""
    catalog = FakeCatalog(make_connection(), workloads=[make_workload(workload_id=1, display_name="alpha")])
    patch_walked_frame(monkeypatch, CatalogFrame(catalog))

    assert _stdout(invoke([flag, "ls", "/some/path#Source"], exit_code=None)) == "alpha\n"


def test_ls_of_an_empty_folder_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    provider = FlatProvider(root_node, [])
    patch_walked_frame(monkeypatch, NodeFrame(cast(Any, provider), root_node))

    assert _stdout(invoke(["ls", "/p#ver"], exit_code=None)) == "(empty)\n"


# -- per-row markers: Node.file_state and diagnostic placeholders ------------


def test_ls_shows_the_cloud_file_icon_in_human_and_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
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
    patch_walked_frame(monkeypatch, NodeFrame(cast(Any, provider), root_node))

    assert _stdout(invoke(["ls", "/some/path#ver"], exit_code=None)) == "normal.txt (1 B)\ncloud.txt (1 B) ☁\n"

    json_result = invoke(["--json", "ls", "/some/path#ver"])
    rows_by_name = {row["name"]: row for row in json.loads(json_result.output)}
    assert "file_state" not in rows_by_name["normal.txt"]
    assert rows_by_name["cloud.txt"]["file_state"] == "cloud_only"


def test_ls_shows_the_encrypted_icon_in_human_and_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    encrypted_child = Node(
        ref=NodeRef("repo", ("ver", "secret.docx")),
        name="secret.docx",
        is_leaf=True,
        size=1,
        file_state=FileState.ENCRYPTED,
    )
    provider = FlatProvider(root_node, [encrypted_child])
    patch_walked_frame(monkeypatch, NodeFrame(cast(Any, provider), root_node))

    assert _stdout(invoke(["ls", "/some/path#ver"], exit_code=None)) == "secret.docx (1 B) 🔒\n"

    json_result = invoke(["--json", "ls", "/some/path#ver"])
    rows_by_name = {row["name"]: row for row in json.loads(json_result.output)}
    assert rows_by_name["secret.docx"]["file_state"] == "encrypted"


def test_ls_shows_the_diagnostic_marker_for_a_diagnostic_node_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    normal_child = Node(ref=NodeRef("repo", ("ver", "normal.txt")), name="normal.txt", is_leaf=True, size=1)
    diagnostic_child = diagnostic_node(
        NodeRef("repo", ("ver", "(missing fragments)")),
        "(2 registered object(s) not found in current data)",
        "some fragments never resolved",
    )
    provider = FlatProvider(root_node, [normal_child, diagnostic_child])
    patch_walked_frame(monkeypatch, NodeFrame(cast(Any, provider), root_node))

    assert _stdout(invoke(["ls", "/some/path#ver"], exit_code=None)) == (
        "normal.txt (1 B)\n(2 registered object(s) not found in current data) ⚠\n"
    )

    json_result = invoke(["--json", "ls", "/some/path#ver"])
    rows_by_name = {row["name"]: row for row in json.loads(json_result.output)}
    assert "diagnostic" not in rows_by_name["normal.txt"]
    assert rows_by_name["(2 registered object(s) not found in current data)"]["diagnostic"] is True
