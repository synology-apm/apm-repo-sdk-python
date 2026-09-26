"""``BrowseCmd``: every effect ``update()`` can ask
``runtime/browse_effects.py`` to perform -- data, never a callable, so
``assert cmds == (LoadWorkloads(...),)`` is a one-line test with no
Pilot involved.

Pushing a screen is deliberately not a ``Cmd`` -- a synchronous navigation
with no effect on this model. ``PromptForKey`` is the exception: an
effect, not a screen method, must dispatch ``KeyVerified`` once
``KeyDialog`` resolves."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import Epoch, RepoHandle, RequestId
from synology_apm_repo.browser.core.notify import Notify as Notify
from synology_apm_repo.sdk.api import Catalog, Workload
from synology_apm_repo.sdk.identifiers import CatalogId


@dataclasses.dataclass(frozen=True)
class CloseRepos:
    """Releases every repository discarded by a rescan/unmount -- plural,
    since one scan can discover more than one repository."""

    repos: tuple[RepoHandle, ...]


@dataclasses.dataclass(frozen=True)
class SetCurrentRepo:
    repo: RepoHandle


@dataclasses.dataclass(frozen=True)
class LoadCatalogsFor:
    repo: RepoHandle
    epoch: Epoch
    request: RequestId


@dataclasses.dataclass(frozen=True)
class LoadWorkloads:
    """No ``epoch``/``request`` -- keyed by the selected catalog itself."""

    repo: RepoHandle
    catalog: Catalog


@dataclasses.dataclass(frozen=True)
class PromptForKey:
    repo: RepoHandle
    catalog: Catalog


@dataclasses.dataclass(frozen=True)
class ReloadCatalogsAfterKeyVerified:
    repo: RepoHandle
    catalog_id: CatalogId


@dataclasses.dataclass(frozen=True)
class LoadVersions:
    """``repo`` travels alongside ``catalog`` so the effect can compute
    this fetch's ``WorkloadKey`` without reading ``model.selected_catalog``
    live inside the worker."""

    repo: RepoHandle
    catalog: Catalog
    workload: Workload


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
