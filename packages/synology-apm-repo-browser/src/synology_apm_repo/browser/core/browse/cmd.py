"""``BrowseCmd``: every effect ``update()`` can ask
``runtime/browse_effects.py`` to perform.

Pushing a screen is not a ``Cmd``, except ``PromptForKey``, whose effect
dispatches ``KeyVerified`` once ``KeyDialog`` resolves."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import Epoch, RepoHandle, RequestId
from synology_apm_repo.browser.core.notify import Notify as Notify
from synology_apm_repo.sdk import Catalog, CatalogId, Workload


@dataclasses.dataclass(frozen=True, slots=True)
class CloseRepos:
    """Releases every repository a rescan or unmount discarded."""

    repos: tuple[RepoHandle, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class SetCurrentRepo:
    repo: RepoHandle


@dataclasses.dataclass(frozen=True, slots=True)
class LoadCatalogsFor:
    repo: RepoHandle
    epoch: Epoch
    request: RequestId


@dataclasses.dataclass(frozen=True, slots=True)
class LoadWorkloads:
    """No ``epoch``/``request`` -- keyed by the selected catalog itself.
    ``invalidate`` drops the repository's caches before fetching (a refresh)."""

    repo: RepoHandle
    catalog: Catalog
    invalidate: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class PromptForKey:
    repo: RepoHandle
    catalog: Catalog


@dataclasses.dataclass(frozen=True, slots=True)
class ReloadCatalogsAfterKeyVerified:
    repo: RepoHandle
    catalog_id: CatalogId


@dataclasses.dataclass(frozen=True, slots=True)
class LoadVersions:
    """``repo`` lets the effect build the result's ``WorkloadKey`` without
    reading the live selection. ``invalidate`` drops the repository's caches
    before fetching (a refresh)."""

    repo: RepoHandle
    catalog: Catalog
    workload: Workload
    invalidate: bool = False


BrowseCmd = (
    CloseRepos
    | SetCurrentRepo
    | LoadCatalogsFor
    | LoadWorkloads
    | PromptForKey
    | ReloadCatalogsAfterKeyVerified
    | LoadVersions
    | Notify
)
