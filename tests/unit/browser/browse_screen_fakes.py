"""Fakes the BrowseScreen Pilot tests drive the screen with: a repository,
catalogs (a call-counting one and an id-only one), a minimal app hosting the
screen, and ``open_browse_screen``."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

from textual.pilot import Pilot
from textual.screen import Screen

from support.fakes import faithful_to
from support.model_factories import make_version
from support.pilot import wait_for_screen
from synology_apm_repo.browser.core.keys import RepoHandle
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.sdk.api import Catalog, Repository, Version, Workload
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from unit.browser.screen_host_fakes import ScreenHostApp


def fake_repository(repo_root: str = "@ActiveProtectData/repo-1") -> Repository:
    """A real ``Repository`` over a placeholder store/layout."""
    return Repository(
        cast(ObjectStore, object()),
        RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=repo_root),
        None,
        None,
        encrypted=False,
    )


class FakeApp(ScreenHostApp):
    """Hosts ``BrowseScreen`` with no repository selected yet."""

    def screens(self) -> list[Screen[Any]]:
        return [BrowseScreen()]


@asynccontextmanager
async def open_browse_screen() -> AsyncIterator[tuple[FakeApp, Pilot[None], BrowseScreen]]:
    """Run a fresh ``FakeApp`` and yield ``(app, pilot, screen)`` once its
    ``BrowseScreen`` is the active, mounted screen."""
    app = FakeApp()
    async with app.run_test() as pilot:
        yield app, pilot, await wait_for_screen(pilot, BrowseScreen)


async def discover(screen: BrowseScreen, repo: Repository, *, label: str = "/scan") -> RepoHandle:
    """Registers ``repo`` via ``_apply_discovered`` and returns its ``RepoHandle``."""
    screen._apply_discovered([repo], label)
    handles = list(screen.store.model.repos)
    return handles[-1]


@faithful_to(Catalog)
class CountingCatalog:
    """A ``Catalog``-shaped fake that counts ``workloads()``/``versions()``
    calls, so tests can prove whether a fetch happened."""

    def __init__(self, catalog_id: CatalogId, workloads: list[Workload]) -> None:
        self.catalog_id = catalog_id
        self.display_name = "catalog"
        self._workloads = workloads
        self.workloads_calls = 0
        self.versions_calls = 0

    async def workloads(self) -> list[Workload]:
        self.workloads_calls += 1
        return self._workloads

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        self.versions_calls += 1
        return [make_version()]


@faithful_to(Catalog)
class FakeCatalogWithId:
    """A ``Catalog`` duck-type with only what ``BrowseEffects`` touches on reload."""

    def __init__(self, catalog_id: CatalogId) -> None:
        self.catalog_id = catalog_id
        self.display_name = "catalog"
        self.workloads_calls = 0

    async def workloads(self) -> list[Workload]:
        self.workloads_calls += 1
        return []
