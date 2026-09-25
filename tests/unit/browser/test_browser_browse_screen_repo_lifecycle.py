"""Unit tests for ``BrowseScreen``'s own repo close-on-rescan behavior —
``RescanStarted`` (reconnecting to a new source via ``c`` ->
``ConnectDialog`` -> a fresh scan) must close every previously-discovered
``Repository`` it's discarding rather than just dropping the reference,
the same leaked-connection concern ``test_browser_unit_screen_provider_
lifecycle.py`` covers for ``UnitScreen``'s own provider — a user
reconnecting to several sources within one long session would otherwise
accumulate open ``Repository``/``DedupRepo`` connections until the
whole app finally closes."""

from __future__ import annotations

from typing import Any, cast

import pytest
from textual.app import App, ComposeResult
from textual.widgets import Static, Tree

from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.browse.msg import RescanStarted
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout


def _fake_repository(repo_root: str) -> Repository:
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


def _open_repos(screen: BrowseScreen) -> list[Repository]:
    """The real ``Repository`` objects ``screen.store.model.repos`` (a
    ``Mapping[RepoHandle, RepoState]``) currently resolves to, in
    ``model.repos``' own insertion order -- the model itself never holds
    a real ``Repository`` (it owns an ``aiosqlite`` connection, which
    can't live in a frozen model), only the opaque handle
    ``ResourceTable`` resolves it through."""
    resources = screen.app_state.resources
    repos = [resources.repo(handle) for handle in screen.store.model.repos]
    return [repo for repo in repos if repo is not None]


class _FakeApp(App[None]):
    """A bare ``App`` (not ``ApmRepoBrowserApp``) is enough here, plus
    ``session``/``resources``, which ``BrowseEffects`` now needs
    (``Session.close_repo`` releases a discarded repo's own store, not
    just the repo itself). A real, empty ``Session()`` costs no I/O
    to construct and correctly no-ops its own bookkeeping for a repo/
    store it never tracked (these fakes are built directly via
    ``_fake_repository``, never through ``session.discover()``), while
    still calling the real ``repo.close()`` these tests assert on."""

    def __init__(self) -> None:
        super().__init__()
        self.repo_handle: RepoHandle | None = None
        self.verbose = False
        self.jobs: dict[JobId, Job] = {}
        self.session = Session()
        self.resources = ResourceTable(self.session)

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(BrowseScreen())


def _tracked_close(monkeypatch: pytest.MonkeyPatch, repo: Repository) -> list[int]:
    """Replaces ``repo``'s own ``close()`` with one that just counts its
    calls in the returned list's single element — real ``Repository.
    close()`` is exercised elsewhere (``test_api.py``); what these tests
    need is only "was it called, how many times", not its own internals."""
    calls = [0]

    async def _fake_close() -> None:
        calls[0] += 1

    monkeypatch.setattr(repo, "close", _fake_close)
    return calls


async def test_reconnecting_closes_the_previous_scans_repos(wait_until: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)

        first = _fake_repository("@ActiveProtectData/repo-1")
        first_calls = _tracked_close(monkeypatch, first)
        screen._apply_discovered([first], "/scan/one")
        assert _open_repos(screen) == [first]

        second = _fake_repository("@ActiveProtectData/repo-2")
        second_calls = _tracked_close(monkeypatch, second)
        screen._apply_discovered([second], "/scan/two")
        assert _open_repos(screen) == [second]

        await wait_until(pilot, lambda: first_calls[0] > 0)
        assert first_calls[0] == 1
        # The repo from the new scan is still in active use -- only the
        # discarded one gets closed.
        assert second_calls[0] == 0


async def test_reconnecting_closes_every_repo_from_a_multi_repo_scan(
    wait_until: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)

        repo_a = _fake_repository("@ActiveProtectData/repo-a")
        repo_b = _fake_repository("@ActiveProtectData/repo-b")
        calls_a = _tracked_close(monkeypatch, repo_a)
        calls_b = _tracked_close(monkeypatch, repo_b)
        screen._apply_discovered([repo_a, repo_b], "/scan/multi")

        screen._apply_discovered([_fake_repository("@ActiveProtectData/repo-c")], "/scan/next")

        await wait_until(pilot, lambda: calls_a[0] > 0 and calls_b[0] > 0)
        assert calls_a[0] == 1
        assert calls_b[0] == 1


async def test_first_scan_has_nothing_to_close() -> None:
    """``RescanStarted`` on an empty ``model.repos`` (the very first scan
    of a session) schedules no close worker at all -- nothing to assert
    a close call on, just confirms no crash from closing zero repos."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        assert screen.store.model.repos == {}

        screen.store.dispatch(RescanStarted(scan_path="/scan/first"))
        assert screen.store.model.repos == {}


async def test_action_go_back_with_a_repo_open_closes_it_and_reopens_connect(
    wait_until: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Esc's fallback on ``BrowseScreen``'s root -- nothing else to close
    (no filter/goto box open) -- closes whatever's currently connected,
    the same real close ``_reset_for_new_scan`` already does for a
    rescan, and reopens ``ConnectDialog`` (the ``c``/app-startup flow),
    rather than the old no-op."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)

        repo = _fake_repository("@ActiveProtectData/repo-1")
        close_calls = _tracked_close(monkeypatch, repo)
        screen._apply_discovered([repo], "/scan/one")
        # _apply_discovered's own RepoAdded dispatch already sets
        # app_state.repo_handle synchronously (the first-repo case in
        # core/browse/update.py) -- nothing further needed here.
        assert _open_repos(screen) == [repo]

        connect_calls = [0]
        monkeypatch.setattr(screen, "action_connect_remote", lambda: connect_calls.__setitem__(0, connect_calls[0] + 1))

        screen.action_go_back()
        await wait_until(pilot, lambda: close_calls[0] > 0)

        assert close_calls[0] == 1
        assert connect_calls[0] == 1
        assert screen.store.model.repos == {}
        assert screen.app_state.repo_handle is None
        assert not screen.query_one("#col-catalogs", Tree).root.children
        assert str(screen.query_one("#open-status", Static).render()) == ""


async def test_action_go_back_with_nothing_open_still_reopens_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same fallback with nothing currently connected -- the close
    step is a no-op (nothing in ``model.repos``), but Connect still
    reopens, matching the unconditional "always reopen" behavior rather
    than only doing so when there was something to close."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        assert screen.store.model.repos == {}

        connect_calls = [0]
        monkeypatch.setattr(screen, "action_connect_remote", lambda: connect_calls.__setitem__(0, connect_calls[0] + 1))

        screen.action_go_back()
        await pilot.pause()

        assert connect_calls[0] == 1
        assert screen.store.model.repos == {}


__all__: list[str] = []
