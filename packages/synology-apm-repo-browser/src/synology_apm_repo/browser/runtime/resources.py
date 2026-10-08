"""``ResourceTable``: the one holder of the app's live, closable SDK objects
(each ``Repository`` and ``UnitProvider``), which models address by
``RepoHandle``/``ProviderHandle``.

A repository is disposed of through ``Session.close_repo()``, which also
releases its store once no other repository uses it.
"""

from __future__ import annotations

from synology_apm_repo.browser.core.keys import ProviderHandle, RepoHandle
from synology_apm_repo.browser.runtime.load_gate import LoadGate
from synology_apm_repo.sdk import ClosableUnitProvider, Repository, Session


class ResourceTable:
    """One instance per running app, not per screen — a repository opened
    by ``ConnectDialog`` outlives the screen that opened it."""

    def __init__(self, session: Session) -> None:
        self._session = session
        #: Shared by every effect that calls into a repository: loads hold it
        #: shared, a cache invalidation holds it exclusively (see ``LoadGate``).
        self.load_gate = LoadGate()
        self._repos: dict[RepoHandle, Repository] = {}
        self._providers: dict[ProviderHandle, ClosableUnitProvider] = {}
        self._provider_repos: dict[ProviderHandle, Repository] = {}
        self._next_repo_handle = 1
        self._next_provider_handle = 1

    def put_repo(self, repo: Repository) -> RepoHandle:
        handle = RepoHandle(self._next_repo_handle)
        self._next_repo_handle += 1
        self._repos[handle] = repo
        return handle

    def repo(self, handle: RepoHandle) -> Repository | None:
        """``None`` when ``handle`` was already released, which callers
        treat as nothing to do: a duplicate release is expected."""
        return self._repos.get(handle)

    def put_provider(self, provider: ClosableUnitProvider, repo: Repository | None = None) -> ProviderHandle:
        """Hold ``provider``; pass the ``repo`` that handed it out so releasing
        the handle also stops that repository tracking it."""
        handle = ProviderHandle(self._next_provider_handle)
        self._next_provider_handle += 1
        self._providers[handle] = provider
        if repo is not None:
            self._provider_repos[handle] = repo
        return handle

    def provider(self, handle: ProviderHandle) -> ClosableUnitProvider | None:
        """``None`` when ``handle`` was already released, as for ``repo()``."""
        return self._providers.get(handle)

    async def release_repo(self, handle: RepoHandle) -> None:
        """Pops ``handle`` (before the ``await``, so a duplicate release
        finds it gone) and closes it via ``Session.close_repo()``."""
        repo = self._repos.pop(handle, None)
        if repo is not None:
            await self._session.close_repo(repo)

    async def release_provider(self, handle: ProviderHandle) -> None:
        """Pops ``handle`` like ``release_repo`` and closes it, through its
        repository when it has one. The caller first drains workers still
        reading through it."""
        provider = self._providers.pop(handle, None)
        repo = self._provider_repos.pop(handle, None)
        if provider is None:
            return
        if repo is not None:
            await repo.release_provider(provider)
        else:
            await provider.close()
