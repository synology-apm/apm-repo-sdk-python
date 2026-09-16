"""Unit tests for ``BrowseScreen``'s own repo close-on-rescan behavior —
``_reset_for_new_scan`` (reconnecting to a new source via ``c`` ->
``ConnectDialog`` -> a fresh scan) must close every previously-discovered
``Repository`` it's discarding rather than just dropping the reference,
the same leaked-connection concern ``test_browser_unit_screen_provider_
lifecycle.py`` covers for ``UnitScreen``'s own provider — a user
reconnecting to several sources within one long session would otherwise
accumulate open ``Repository``/``DedupRepo`` connections until the
whole app finally closes. Same ``_fake_repository``/``_FakeApp``
convention as ``test_browser_browse_screen_gaps.py`` (duplicated here
rather than imported — see ``tests/CLAUDE.md``'s "no test module ever
imports from another")."""

from __future__ import annotations

from typing import Any, cast

import pytest
from textual.app import App, ComposeResult

from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.sdk.api import Repository
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


class _FakeApp(App[None]):
    """See ``test_browser_browse_screen_gaps.py``'s own identical class
    for why a bare ``App`` (not ``ApmRepoBrowserApp``) is enough here."""

    def __init__(self) -> None:
        super().__init__()
        self.repo: Repository | None = None
        self.verbose = False

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
        assert screen._repos == [first]

        second = _fake_repository("@ActiveProtectData/repo-2")
        second_calls = _tracked_close(monkeypatch, second)
        screen._apply_discovered([second], "/scan/two")
        assert screen._repos == [second]

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
    """``_reset_for_new_scan`` on an empty ``self._repos`` (the very
    first scan of a session) schedules no close worker at all -- nothing
    to assert a close call on, just confirms no crash from closing
    zero repos."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        assert screen._repos == []

        screen._reset_for_new_scan("/scan/first")
        assert screen._repos == []


__all__: list[str] = []
