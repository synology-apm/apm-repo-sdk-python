"""Unit tests for ``synology_apm_repo.cli.browse`` — the shared
``ls``/``tree``/``cat``/``export`` navigation logic, tested directly
against fakes (real end-to-end coverage lives in
``tests/unit/cli/test_cli_browse.py``)."""

from __future__ import annotations

from typing import Any, cast

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli.browse import (
    ParsedRef,
    open_single_repo,
    parse_ref_argument,
    walk_ref,
)
from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.api import Catalog, Connection, Repository
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.identifiers import ConnectionConfigId, ConnectionId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

runner = CliRunner()

# -- parse_ref_argument -------------------------------------------------


def test_parse_ref_argument_bare_path_has_empty_segments() -> None:
    parsed = parse_ref_argument("/some/path")
    assert parsed == ParsedRef(fs_path="/some/path", node_ref=NodeRef("/some/path", ()))


def test_parse_ref_argument_splits_on_first_hash() -> None:
    parsed = parse_ref_argument("/some/path#Test-Workload-02/my-vm")
    assert parsed.fs_path == "/some/path"
    assert parsed.node_ref.segments == ("Test-Workload-02", "my-vm")


# -- open_single_repo -----------------------------------------------------


class _FakeSession:
    def __init__(self, repos: list[object]) -> None:
        self._repos = repos
        self.open_remote_calls: list[dict[str, object]] = []

    async def open(
        self, fs_path: object, key: object, *, progress: object = None, trace: object = None
    ) -> list[object]:
        return self._repos

    async def open_remote(
        self, store: object, key: object = None, *, root: str = "", progress: object = None, trace: object = None
    ) -> list[object]:
        self.open_remote_calls.append({"store": store, "key": key, "root": root})
        return self._repos


async def test_open_single_repo_returns_the_only_repo() -> None:
    sentinel = object()
    session = cast(Any, _FakeSession([sentinel]))
    assert await open_single_repo(session, "/some/path", None) is sentinel


async def test_open_single_repo_raises_not_found_on_zero_repos() -> None:
    session = cast(Any, _FakeSession([]))
    with pytest.raises(NotFoundError, match="no repository found"):
        await open_single_repo(session, "/some/path", None)


async def test_open_single_repo_with_store_uses_open_remote_with_root() -> None:
    sentinel = object()
    fake_session = _FakeSession([sentinel])
    store = cast(Any, object())
    result = await open_single_repo(cast(Any, fake_session), "sub/path", None, store=store)
    assert result is sentinel
    assert fake_session.open_remote_calls == [{"store": store, "key": None, "root": "sub/path"}]


async def test_open_single_repo_without_store_uses_open() -> None:
    fake_session = _FakeSession([object()])
    await open_single_repo(cast(Any, fake_session), "/some/path", None)
    assert fake_session.open_remote_calls == []


async def test_open_single_repo_raises_not_found_on_multiple_repos() -> None:
    class _FakeRepo:
        def __init__(self, root: str) -> None:
            self.layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root=root)

    session = cast(Any, _FakeSession([_FakeRepo("a"), _FakeRepo("b")]))
    with pytest.raises(NotFoundError, match="2 repositories found"):
        await open_single_repo(session, "/some/path", None)


# -- opened_repo(): friendly_message()'s verbose gate wired end to end -----
#
# open_single_repo()'s own NotFoundError (asserted directly above) is exactly
# the kind of internal-store-path-bearing error opened_repo() must now
# redact by default and restore under --verbose (see cli/errors.py's
# friendly_message() docstring).


class _RaisingSession:
    async def open(self, *args: object, **kwargs: object) -> list[object]:
        # Deliberately doesn't repeat the ref in the message text itself
        # (unlike open_single_repo()'s own NotFoundError above) -- isolates
        # what friendly_message()'s redaction actually strips: the
        # structured "[ref=...]" tag, not incidental text.
        raise NotFoundError("nothing readable at this location", ref="/some/internal/store/path")

    async def close(self) -> None:
        pass


def test_ref_bearing_error_is_redacted_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("synology_apm_repo.cli.browse.Session", _RaisingSession)
    result = runner.invoke(app, ["ls", "/some/path"])
    assert result.exit_code == 1
    assert "/some/internal/store/path" not in result.output


def test_ref_bearing_error_is_restored_under_verbose(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("synology_apm_repo.cli.browse.Session", _RaisingSession)
    result = runner.invoke(app, ["--verbose", "ls", "/some/path"])
    assert result.exit_code == 1
    assert "/some/internal/store/path" in result.output


class _CrashingSession:
    async def open(self, *args: object, **kwargs: object) -> list[object]:
        raise RuntimeError("boom")  # deliberately not an ApmRepoError -- a bug, not an expected failure

    async def close(self) -> None:
        pass


def test_unexpected_non_apmrepoerror_still_clears_a_live_progress_line(monkeypatch: pytest.MonkeyPatch) -> None:
    """``opened_repo()``'s own ``except ApmRepoError`` doesn't catch this
    at all -- ``finish_live_progress`` must still fire via the shared
    ``finally``, not just that branch's own explicit call, or a bug's own
    traceback would land on top of a dangling progress line instead of a
    clean one."""
    monkeypatch.setattr("synology_apm_repo.cli.browse.Session", _CrashingSession)
    result = runner.invoke(app, ["--progress", "always", "ls", "/some/path"])
    assert result.exit_code == 1
    assert "\x1b[2K" in result.stderr


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
    return Catalog(cast(DedupRepo, _FakeDedupRepo()), connection, track=lambda p: p, require_key_verified=lambda: None)


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
