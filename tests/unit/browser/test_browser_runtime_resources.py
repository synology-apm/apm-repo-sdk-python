"""Unit tests for ``browser.runtime.resources.ResourceTable`` — handle
minting/lookup, and specifically that repository disposal goes through
``Session.close_repo()`` rather than a bare ``Repository.close()`` -- a bare
``.close()`` leaves that repository's S3/Azure/SMB connector referenced by
the ``Session`` for the rest of the process.
``test_browser_repository_disposal_convention.py`` enforces
the same rule structurally, across the whole package; this file proves
the one real call site actually behaves that way at runtime.

``_fake_repository``/``_ClosableProvider`` here follow the same
duplicated-not-imported convention as this package's other
runtime/provider-lifecycle test files."""

from __future__ import annotations

from typing import Any, cast

import pytest

from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _fake_repository(repo_root: str = "@ActiveProtectData/repo-1") -> Repository:
    """A real ``Repository``, backed by placeholder store/layout —
    ``Repository.__init__`` does no I/O itself, so nothing here ever
    touches a real ``DedupRepo``."""
    return Repository(
        cast(ObjectStore, object()),
        RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=repo_root),
        None,
        None,
        encrypted=False,
    )


class _ClosableProvider:
    """A minimal ``ClosableUnitProvider`` tracking its own ``close()``
    calls — matches ``test_browser_unit_screen_provider_lifecycle.py``'s
    own fake of the same shape."""

    def __init__(self) -> None:
        self.close_calls = 0
        self._root = Node(ref=NodeRef("repo", ()), name="root", is_leaf=True)

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return []

    async def unit(self, node: Node) -> Any:  # pragma: no cover - never exercised here
        raise NotImplementedError

    async def close(self) -> None:
        self.close_calls += 1

    async def __aenter__(self) -> _ClosableProvider:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


class _NonClosableProvider:
    """A plain ``UnitProvider`` with no ``close()`` at all — the shape a
    real provider with no sqlite connection to close
    (``FileMapTreeProvider``) actually has: it implements only
    ``UnitProvider``, never ``ClosableUnitProvider``. ``isinstance`` against a
    ``runtime_checkable`` ``Protocol`` checks attribute *presence*, so
    this type deliberately has no ``close`` attribute to be found."""

    def __init__(self) -> None:
        self._root = Node(ref=NodeRef("repo", ()), name="root", is_leaf=True)

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return []

    async def unit(self, node: Node) -> Any:  # pragma: no cover - never exercised here
        raise NotImplementedError


def _tracked_close_repo(monkeypatch: pytest.MonkeyPatch, session: Session) -> list[Repository]:
    calls: list[Repository] = []

    async def _fake_close_repo(repo: Repository) -> None:
        calls.append(repo)

    monkeypatch.setattr(session, "close_repo", _fake_close_repo)
    return calls


def test_put_repo_then_repo_returns_the_same_object() -> None:
    table = ResourceTable(Session())
    repo = _fake_repository()
    handle = table.put_repo(repo)
    assert table.repo(handle) is repo


def test_two_puts_mint_distinct_handles() -> None:
    table = ResourceTable(Session())
    handle_a = table.put_repo(_fake_repository("a"))
    handle_b = table.put_repo(_fake_repository("b"))
    assert handle_a != handle_b


def test_repo_returns_none_for_an_unknown_handle() -> None:
    table = ResourceTable(Session())
    handle = table.put_repo(_fake_repository())
    other_table = ResourceTable(Session())  # handle minted by a different table entirely
    assert other_table.repo(handle) is None


async def test_release_repo_closes_via_session_close_repo_not_repository_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = Session()
    close_repo_calls = _tracked_close_repo(monkeypatch, session)
    bare_close_called = False

    async def _fail_if_called() -> None:
        nonlocal bare_close_called
        bare_close_called = True

    table = ResourceTable(session)
    repo = _fake_repository()
    monkeypatch.setattr(repo, "close", _fail_if_called)
    handle = table.put_repo(repo)

    await table.release_repo(handle)

    assert close_repo_calls == [repo]
    assert bare_close_called is False


async def test_release_repo_removes_the_handle() -> None:
    table = ResourceTable(Session())
    handle = table.put_repo(_fake_repository())
    await table.release_repo(handle)
    assert table.repo(handle) is None


async def test_release_repo_is_a_no_op_for_an_already_released_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    """A duplicate release racing its own original (e.g. two separate
    ``CloseRepo`` effects for the same handle) must not double-close --
    ``release_repo`` pops before awaiting, so the second call sees
    nothing there."""
    session = Session()
    close_repo_calls = _tracked_close_repo(monkeypatch, session)
    table = ResourceTable(session)
    handle = table.put_repo(_fake_repository())

    await table.release_repo(handle)
    await table.release_repo(handle)

    assert len(close_repo_calls) == 1


def test_put_provider_then_provider_returns_the_same_object() -> None:
    table = ResourceTable(Session())
    provider = _ClosableProvider()
    handle = table.put_provider(provider)
    assert table.provider(handle) is provider


async def test_release_provider_closes_a_closable_provider() -> None:
    table = ResourceTable(Session())
    provider = _ClosableProvider()
    handle = table.put_provider(provider)

    await table.release_provider(handle)

    assert provider.close_calls == 1
    assert table.provider(handle) is None


async def test_release_provider_does_not_call_close_on_a_non_closable_provider() -> None:
    """Matches ``UnitScreen._close_provider``'s existing
    ``isinstance(provider, ClosableUnitProvider)`` guard -- a provider
    with no ``close()`` at all (no connection to release) is simply
    dropped, never dereferenced as closable."""
    table = ResourceTable(Session())
    provider = _NonClosableProvider()
    handle = table.put_provider(provider)

    await table.release_provider(handle)  # must not raise AttributeError

    assert table.provider(handle) is None


async def test_release_provider_is_a_no_op_for_an_already_released_handle() -> None:
    table = ResourceTable(Session())
    provider = _ClosableProvider()
    handle = table.put_provider(provider)

    await table.release_provider(handle)
    await table.release_provider(handle)

    assert provider.close_calls == 1
