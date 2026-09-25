"""Unit tests for ``synology_apm_repo.cli.commands.ls``'s own
catalog-level/workload-level row-building (``case "catalog":``/
``case "workload":`` in ``ls()`` itself), plus the ``Node.attrs["file_state"]``
hint below — none of which any existing
``tests/integration/cli/test_cli_ls.py`` scenario reaches (the
human-ref/catalog- and workload-level navigation, and a cloud-sync
placeholder file, aren't things a real sample fixture is expected to
contain), so both are proven here, synthetically, rather than against a
real backend."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.commands.ls as ls_module
from synology_apm_repo.cli.browse import Frame
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
from synology_apm_repo.sdk.units.base import FileState, Node, diagnostic_node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

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


def _make_workload(workload_id: int = 1, name: str = "Workload") -> Workload:
    return Workload(
        workload_id=WorkloadId(workload_id),
        workload_uid=WorkloadUid("wl-uid"),
        workload_type="VM",
        sub_type=None,
        display_name=name,
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
    """A real ``Catalog`` (so ``isinstance(obj, Catalog)`` checks in
    ``ls.py``'s own ``_stable_id`` still hold) with its ``workloads()``/
    ``versions()`` overridden to return fixed lists instead of reading a
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


class _FakeRepoCtx:
    def __init__(self, repo: _FakeRepo) -> None:
        self._repo = repo

    async def __aenter__(self) -> _FakeRepo:
        return self._repo

    async def __aexit__(self, *exc: object) -> None:
        return None


def _patch_frame(monkeypatch: pytest.MonkeyPatch, repo: _FakeRepo, frame: Frame) -> None:
    async def fake_walk_ref(repo: object, node_ref: object, *, object_db_id: object = None) -> Frame:
        return frame

    monkeypatch.setattr(ls_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(repo))
    monkeypatch.setattr(ls_module, "walk_ref", fake_walk_ref)


def test_ls_at_connection_level_lists_workloads(monkeypatch: pytest.MonkeyPatch) -> None:
    workloads = [_make_workload(1, "alpha"), _make_workload(2, "beta")]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads)
    _patch_frame(monkeypatch, _FakeRepo(), Frame(level="catalog", catalog=catalog))

    result = runner.invoke(app, ["ls", "/some/path#Source"])
    assert result.exit_code == 0, result.output
    assert "alpha" in result.output
    assert "beta" in result.output


def test_ls_at_workload_level_lists_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    versions = [_make_version(1, "2026-01-01 00:00"), _make_version(2, "2026-01-02 00:00")]
    catalog = _FakeCatalog(_make_connection(), versions=versions)
    _patch_frame(monkeypatch, _FakeRepo(), Frame(level="workload", catalog=catalog, workload=_make_workload()))

    result = runner.invoke(app, ["ls", "/some/path#Source/Workload"])
    assert result.exit_code == 0, result.output
    assert "2026-01-01 00:00" in result.output
    assert "2026-01-02 00:00" in result.output


def test_ls_renders_a_bracketed_display_name_literally_not_as_rich_markup(monkeypatch: pytest.MonkeyPatch) -> None:
    # A real workload name Rich would otherwise mistake for a markup tag
    # and silently drop (e.g. "[limitation] deep_hierarchy" -> "
    # deep_hierarchy") -- must render in full, byte-for-byte.
    workloads = [_make_workload(1, "[limitation] deep_hierarchy")]
    catalog = _FakeCatalog(_make_connection(), workloads=workloads)
    _patch_frame(monkeypatch, _FakeRepo(), Frame(level="catalog", catalog=catalog))

    result = runner.invoke(app, ["ls", "/some/path#Source"])
    assert result.exit_code == 0, result.output
    assert "[limitation] deep_hierarchy" in result.output


def test_ls_verbose_shows_stable_id_at_workload_and_version_level(monkeypatch: pytest.MonkeyPatch) -> None:
    versions = [_make_version(7, "2026-01-01 00:00")]
    catalog = _FakeCatalog(_make_connection(), versions=versions)
    _patch_frame(monkeypatch, _FakeRepo(), Frame(level="workload", catalog=catalog, workload=_make_workload()))

    result = runner.invoke(app, ["--verbose", "ls", "/some/path#Source/Workload"])
    assert result.exit_code == 0, result.output
    assert "id=7" in result.output


def test_ls_verbose_shows_stable_id_at_root_and_connection_level(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = _FakeCatalog(_make_connection(3), workloads=[_make_workload(5, "alpha")])
    repo = _FakeRepo(catalogs=[catalog])

    _patch_frame(monkeypatch, repo, Frame(level="root"))
    root_result = runner.invoke(app, ["--verbose", "ls", "/some/path"])
    assert root_result.exit_code == 0, root_result.output
    assert "id=3" in root_result.output  # catalog_id falls back to connection_config_id (no repo_id on the fake)

    _patch_frame(monkeypatch, repo, Frame(level="catalog", catalog=catalog))
    catalog_result = runner.invoke(app, ["--verbose", "ls", "/some/path#Source"])
    assert catalog_result.exit_code == 0, catalog_result.output
    assert "id=5" in catalog_result.output


def test_ls_without_verbose_or_ref_omits_stable_id(monkeypatch: pytest.MonkeyPatch) -> None:
    versions = [_make_version(7, "2026-01-01 00:00")]
    catalog = _FakeCatalog(_make_connection(), versions=versions)
    _patch_frame(monkeypatch, _FakeRepo(), Frame(level="workload", catalog=catalog, workload=_make_workload()))

    result = runner.invoke(app, ["ls", "/some/path#Source/Workload"])
    assert result.exit_code == 0, result.output
    assert "id=" not in result.output


# -- ls command: cloud-sync/EFS hint (Node.attrs["file_state"]) ------------


class _FakeNodeProvider:
    def __init__(self, root_node: Node, children: list[Node]) -> None:
        self._root_node = root_node
        self._children = children

    def root(self) -> Node:
        return self._root_node

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children if node == self._root_node else []

    async def unit(self, node: Node) -> Any:
        raise NotImplementedError


def test_ls_shows_the_cloud_file_icon_in_human_and_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    normal_child = Node(ref=NodeRef("repo", ("ver", "normal.txt")), name="normal.txt", is_leaf=True, size=1)
    cloud_child = Node(
        ref=NodeRef("repo", ("ver", "cloud.txt")),
        name="cloud.txt",
        is_leaf=True,
        size=1,
        attrs={"file_state": FileState.CLOUD_ONLY},
    )
    provider = _FakeNodeProvider(root_node, [normal_child, cloud_child])
    _patch_frame(monkeypatch, _FakeRepo(), Frame(level="node", node=root_node, provider=cast(Any, provider)))

    result = runner.invoke(app, ["ls", "/some/path#ver"])
    assert result.exit_code == 0, result.output
    assert "normal.txt (1 B)\n" in result.output  # no hint suffix on this line
    assert "cloud.txt (1 B) ☁" in result.output

    json_result = runner.invoke(app, ["--json", "ls", "/some/path#ver"])
    assert json_result.exit_code == 0, json_result.output
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
        attrs={"file_state": FileState.ENCRYPTED},
    )
    provider = _FakeNodeProvider(root_node, [encrypted_child])
    _patch_frame(monkeypatch, _FakeRepo(), Frame(level="node", node=root_node, provider=cast(Any, provider)))

    result = runner.invoke(app, ["ls", "/some/path#ver"])
    assert result.exit_code == 0, result.output
    assert "secret.docx (1 B) 🔒" in result.output

    json_result = runner.invoke(app, ["--json", "ls", "/some/path#ver"])
    assert json_result.exit_code == 0, json_result.output
    rows_by_name = {row["name"]: row for row in json.loads(json_result.output)}
    assert rows_by_name["secret.docx"]["file_state"] == "encrypted"


def test_ls_shows_the_diagnostic_marker_for_a_diagnostic_node_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("repo", ("ver",)), name="root", is_leaf=False)
    normal_child = Node(ref=NodeRef("repo", ("ver", "normal.txt")), name="normal.txt", is_leaf=True, size=1)
    diagnostic_child = diagnostic_node(
        NodeRef("repo", ("ver", "(missing fragments)")),
        "(2 registered object(s) not found in current data)",
        {"diagnostic": "some fragments never resolved"},
    )
    provider = _FakeNodeProvider(root_node, [normal_child, diagnostic_child])
    _patch_frame(monkeypatch, _FakeRepo(), Frame(level="node", node=root_node, provider=cast(Any, provider)))

    result = runner.invoke(app, ["ls", "/some/path#ver"])
    assert result.exit_code == 0, result.output
    assert "normal.txt (1 B)\n" in result.output  # no diagnostic marker on this line
    assert "(2 registered object(s) not found in current data) ⚠" in result.output

    json_result = runner.invoke(app, ["--json", "ls", "/some/path#ver"])
    assert json_result.exit_code == 0, json_result.output
    rows_by_name = {row["name"]: row for row in json.loads(json_result.output)}
    assert "diagnostic" not in rows_by_name["normal.txt"]
    assert rows_by_name["(2 registered object(s) not found in current data)"]["diagnostic"] is True


__all__: list[str] = []
