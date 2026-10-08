"""``BrowseScreen``'s Esc on the root: it closes every open ``Repository``,
as a rescan does, and reopens the connect dialog."""

from __future__ import annotations

import pytest
from textual.widgets import Static, Tree

from support.pilot import wait_until
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.sdk.api import Repository
from unit.browser.browse_screen_fakes import fake_repository, open_browse_screen


def _open_repos(screen: BrowseScreen) -> list[Repository]:
    """The ``Repository`` objects ``model.repos``' handles resolve to, in insertion order."""
    resources = screen.app_state.resources
    repos = [resources.repo(handle) for handle in screen.store.model.repos]
    return [repo for repo in repos if repo is not None]


def _tracked_close(monkeypatch: pytest.MonkeyPatch, repo: Repository) -> list[int]:
    """Replaces ``repo._close`` (which ``Session.close_repo`` calls) with a
    counter in the returned list's single element."""
    calls = [0]

    async def _fake_close() -> None:
        calls[0] += 1

    monkeypatch.setattr(repo, "_close", _fake_close)
    return calls


async def test_action_go_back_with_a_repo_open_closes_it_and_reopens_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository("@ActiveProtectData/repo-1")
        close_calls = _tracked_close(monkeypatch, repo)
        screen._apply_discovered([repo], "/scan/one")
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
    async with open_browse_screen() as (_app, pilot, screen):
        assert screen.store.model.repos == {}

        connect_calls = [0]
        monkeypatch.setattr(screen, "action_connect_remote", lambda: connect_calls.__setitem__(0, connect_calls[0] + 1))

        screen.action_go_back()
        await wait_until(pilot, lambda: connect_calls[0] == 1)
        assert screen.store.model.repos == {}
