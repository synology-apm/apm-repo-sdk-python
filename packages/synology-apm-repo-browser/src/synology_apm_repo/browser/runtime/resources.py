"""``ResourceTable``: the one place a live, closable SDK object (a
``Repository``, a ``UnitProvider``) is actually held, addressed
everywhere else by an opaque ``RepoHandle``/``ProviderHandle`` rather
than by the object itself -- a frozen ``core.*`` model can't hold either
directly, since both own a non-daemon-thread ``aiosqlite`` connection.

Repository disposal always goes through ``Session.close_repo()`` — never
a bare ``Repository.close()``, which leaves an S3/Azure/SMB connector
referenced by the ``Session`` for the rest of the process.
``tests/unit/browser/test_browser_repository_disposal_convention.py``
enforces this structurally across the whole package.
"""

from __future__ import annotations

from synology_apm_repo.browser.core.keys import ProviderHandle, RepoHandle
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.units.base import ClosableUnitProvider, UnitProvider


class ResourceTable:
    """One instance per running app, not per screen — a repository opened
    by ``ConnectDialog`` outlives the screen that opened it."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._repos: dict[RepoHandle, Repository] = {}
        self._providers: dict[ProviderHandle, UnitProvider] = {}
        self._next_repo_handle = 1
        self._next_provider_handle = 1

    def put_repo(self, repo: Repository) -> RepoHandle:
        handle = RepoHandle(self._next_repo_handle)
        self._next_repo_handle += 1
        self._repos[handle] = repo
        return handle

    def repo(self, handle: RepoHandle) -> Repository | None:
        """``None`` means ``handle`` was already released (or never
        put) — every caller must treat that as "nothing to do" rather
        than an error, since a duplicate release/close request racing
        its own original is expected, not exceptional."""
        return self._repos.get(handle)

    def put_provider(self, provider: UnitProvider) -> ProviderHandle:
        handle = ProviderHandle(self._next_provider_handle)
        self._next_provider_handle += 1
        self._providers[handle] = provider
        return handle

    def provider(self, handle: ProviderHandle) -> UnitProvider | None:
        """Same "already released" contract as ``repo()`` above."""
        return self._providers.get(handle)

    async def release_repo(self, handle: RepoHandle) -> None:
        """Pops ``handle`` and closes it via ``Session.close_repo()`` —
        popped before the ``await``, so a concurrent duplicate release
        sees it already gone."""
        repo = self._repos.pop(handle, None)
        if repo is not None:
            await self._session.close_repo(repo)

    async def release_provider(self, handle: ProviderHandle) -> None:
        """Pops ``handle`` and closes it if it's a ``ClosableUnitProvider``
        — same pop-before-``await`` guard as ``release_repo``. Draining
        workers still reading through this provider is the caller's
        responsibility, not this method's."""
        provider = self._providers.pop(handle, None)
        if provider is not None and isinstance(provider, ClosableUnitProvider):
            await provider.close()
