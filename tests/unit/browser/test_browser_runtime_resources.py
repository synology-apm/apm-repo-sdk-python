"""Unit tests for ``browser.runtime.resources.ResourceTable``: handle
minting/lookup, and that repository disposal goes through
``Session.close_repo()``, which also releases the repository's store."""

from __future__ import annotations

from typing import Any, cast

import pytest

from support.fakes import faithful_to
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.units.base import ClosableUnitProvider, Node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.browse_screen_fakes import fake_repository


@faithful_to(ClosableUnitProvider)
class _ClosableProvider:
    """A minimal ``ClosableUnitProvider`` counting its ``close()`` calls."""

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


def _tracked_close_repo(monkeypatch: pytest.MonkeyPatch, session: Session) -> list[Repository]:
    calls: list[Repository] = []

    async def _fake_close_repo(repo: Repository) -> None:
        calls.append(repo)

    monkeypatch.setattr(session, "close_repo", _fake_close_repo)
    return calls


def test_put_repo_then_repo_returns_the_same_object() -> None:
    table = ResourceTable(Session())
    repo = fake_repository()
    handle = table.put_repo(repo)
    assert table.repo(handle) is repo


def test_two_puts_mint_distinct_handles() -> None:
    table = ResourceTable(Session())
    handle_a = table.put_repo(fake_repository("a"))
    handle_b = table.put_repo(fake_repository("b"))
    assert handle_a != handle_b


def test_repo_returns_none_for_an_unknown_handle() -> None:
    table = ResourceTable(Session())
    handle = table.put_repo(fake_repository())
    other_table = ResourceTable(Session())  # handle minted by a different table entirely
    assert other_table.repo(handle) is None


async def test_release_repo_closes_via_session_close_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    session = Session()
    close_repo_calls = _tracked_close_repo(monkeypatch, session)
    table = ResourceTable(session)
    repo = fake_repository()
    handle = table.put_repo(repo)

    await table.release_repo(handle)

    assert close_repo_calls == [repo]


async def test_release_repo_removes_the_handle() -> None:
    table = ResourceTable(Session())
    handle = table.put_repo(fake_repository())
    await table.release_repo(handle)
    assert table.repo(handle) is None


async def test_release_repo_is_a_no_op_for_an_already_released_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    """A duplicate release of one handle (e.g. from two overlapping
    ``CloseRepos`` commands) must not double-close."""
    session = Session()
    close_repo_calls = _tracked_close_repo(monkeypatch, session)
    table = ResourceTable(session)
    handle = table.put_repo(fake_repository())

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


async def test_release_provider_is_a_no_op_for_an_already_released_handle() -> None:
    table = ResourceTable(Session())
    provider = _ClosableProvider()
    handle = table.put_provider(provider)

    await table.release_provider(handle)
    await table.release_provider(handle)

    assert provider.close_calls == 1


@faithful_to(Repository)
class _TrackingRepo:
    """Records ``release_provider`` calls, as ``Repository`` does."""

    def __init__(self) -> None:
        self.released: list[object] = []

    async def release_provider(self, provider: object) -> None:
        self.released.append(provider)
        if isinstance(provider, ClosableUnitProvider):
            await provider.close()


async def test_release_provider_goes_through_the_repository_that_handed_it_out() -> None:
    table = ResourceTable(Session())
    provider = _ClosableProvider()
    repo = _TrackingRepo()
    handle = table.put_provider(provider, cast(Repository, repo))

    await table.release_provider(handle)

    assert repo.released == [provider]
    assert provider.close_calls == 1
    assert table.provider(handle) is None


async def test_releasing_the_same_provider_handle_twice_releases_it_through_the_repository_once() -> None:
    table = ResourceTable(Session())
    repo = _TrackingRepo()
    handle = table.put_provider(_ClosableProvider(), cast(Repository, repo))

    await table.release_provider(handle)
    await table.release_provider(handle)

    assert len(repo.released) == 1
