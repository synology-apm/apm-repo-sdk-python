"""``ResourceTable``: the one place a live, closable SDK object (a
``Repository``, a ``UnitProvider``) is actually held, addressed
everywhere else by an opaque ``RepoHandle``/``ProviderHandle`` (see
``core/keys.py``) rather than by the object itself.

This split exists because a frozen ``core.*`` model can't hold either
type directly: both own an ``aiosqlite`` connection whose background
thread is non-daemon, so an unclosed one leaks threads across repeated
version visits. A handle is a plain, comparable, hashable value a model
*can* hold; only this module ever dereferences one into the real object.

Repository disposal always goes through ``Session.close_repo()`` — never
a bare ``Repository.close()``, which leaves an S3/Azure/SMB connector
referenced by the ``Session`` for the rest of the process.
``tests/unit/browser/test_browser_repository_disposal_convention.py``
enforces this structurally across the whole package, not just here.
"""

from __future__ import annotations

from synology_apm_repo.browser.core.keys import ProviderHandle, RepoHandle
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.units.base import ClosableUnitProvider, UnitProvider


class ResourceTable:
    """One instance per running app (constructed alongside the single
    ``Session``), not per screen — a repository opened by
    ``ConnectDialog`` outlives the screen that opened it."""

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
        popped *before* the ``await``, so a concurrent duplicate release
        of the same handle sees it already gone and does nothing, rather
        than both racing to close the same repository."""
        repo = self._repos.pop(handle, None)
        if repo is not None:
            await self._session.close_repo(repo)

    async def release_provider(self, handle: ProviderHandle) -> None:
        """Pops ``handle`` and closes it if it's a
        ``ClosableUnitProvider`` — same pop-before-``await`` guard as
        ``release_repo``. Waiting out whatever workers were still
        reading through this provider is the caller's own
        responsibility (a screen's own ``on_unmount`` drains its
        still-running workers before this release runs), not this
        method's — a provider close and the fetch of this screen's
        *next* provider never share a connection, so nothing here needs
        to know about that ordering."""
        provider = self._providers.pop(handle, None)
        if provider is not None and isinstance(provider, ClosableUnitProvider):
            await provider.close()
