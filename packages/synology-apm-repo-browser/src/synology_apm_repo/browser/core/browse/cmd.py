"""``BrowseCmd``: every effect ``update()`` can ask
``runtime/browse_effects.py`` to perform -- data, never a callable, so
``assert cmds == (LoadWorkloads(...),)`` is a one-line test with no
Pilot involved.

Pushing a screen (``KeyDialog``, a fresh ``UnitScreen``, ``ConnectDialog``)
is deliberately *not* a ``Cmd`` here -- a synchronous navigation with no
effect on this model, the same posture ``UnitScreen`` already takes for
its own ``ExportScreen``/``HexPreviewScreen`` pushes. ``PromptForKey``
below is the one exception: it still needs a ``Cmd`` (an effect, not a
screen method, owns dispatching ``KeyVerified`` back once ``KeyDialog``
resolves), so it carries no callback of its own -- see
``BrowseEffects.perform``'s own handler for where that closure actually
lives."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import Epoch, RepoHandle, RequestId
from synology_apm_repo.browser.core.notify import Notify as Notify
from synology_apm_repo.sdk.api import Catalog, Workload
from synology_apm_repo.sdk.identifiers import CatalogId


@dataclasses.dataclass(frozen=True)
class CloseRepos:
    """Releases every repository discarded by a rescan/unmount -- plural,
    unlike ``UnitCmd.CloseProvider``, since a rescan can discard several
    at once (one scan can discover more than one repository)."""

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
    """No ``epoch``/``request`` -- unlike a catalogs fetch, this result is
    keyed by the currently-selected catalog itself, so a
    differently-selected fetch's late result can never overwrite what's
    rendered."""

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
    """``repo`` travels alongside ``catalog`` (not re-derivable from it
    alone) so the effect can compute this fetch's own ``WorkloadKey``
    without ever reading ``model.selected_catalog`` live -- capture at
    dispatch, never re-read live state inside the worker."""

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
