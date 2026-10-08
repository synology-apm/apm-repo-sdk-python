"""``BrowseMsg``: every event ``BrowseScreen``'s store handles."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.browse.model import FilterTree
from synology_apm_repo.browser.core.keys import CatalogKey, Epoch, RepoHandle, RequestId, WorkloadKey
from synology_apm_repo.browser.core.remote_data import FailureInfo
from synology_apm_repo.sdk import Catalog, CatalogId, KeyStatus, RepositoryLayout, Version, Workload


@dataclasses.dataclass(frozen=True, slots=True)
class RescanStarted:
    """A fresh ``ConnectDialog`` scan is about to be rendered -- discards
    every previously discovered repository/catalog/workload/version."""

    scan_path: str


@dataclasses.dataclass(frozen=True, slots=True)
class RepoAdded:
    """One repository from the current scan, with its ``layout``/
    ``key_status`` read at dispatch time."""

    repo: RepoHandle
    layout: RepositoryLayout
    key_status: KeyStatus


@dataclasses.dataclass(frozen=True, slots=True)
class RepoSelected:
    """A bare repository node selected in column 1 -- sets the current
    repository, never ``selected_catalog``."""

    repo: RepoHandle


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogsRequested:
    repo: RepoHandle


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogsLoaded:
    epoch: Epoch
    request: RequestId
    repo: RepoHandle
    catalogs: tuple[Catalog, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogsLoadFailed:
    epoch: Epoch
    request: RequestId
    repo: RepoHandle
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogSelected:
    repo: RepoHandle
    catalog: Catalog


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadsLoaded:
    """No ``epoch``/``request``: keyed by the selected catalog itself."""

    catalog: CatalogKey
    workloads: tuple[Workload, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadsLoadFailed:
    """``real_catalog`` travels alongside for the ``KEY_REQUIRED`` case,
    which needs it to open ``KeyDialog`` against."""

    catalog: CatalogKey
    real_catalog: Catalog
    info: FailureInfo


@dataclasses.dataclass(frozen=True, slots=True)
class RepoKeyStatusRefreshed:
    """``repo.key_status`` re-read whenever ``KeyDialog`` dismisses, even
    on cancel; dispatched before ``KeyVerified`` so the repository label
    updates with the reload."""

    repo: RepoHandle
    key_status: KeyStatus


@dataclasses.dataclass(frozen=True, slots=True)
class KeyVerified:
    repo: RepoHandle
    catalog_id: CatalogId


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogsRefreshed:
    """Every catalog under ``repo``, re-resolved after a key verification,
    which reopens all of them, not just the one that prompted for it."""

    repo: RepoHandle
    catalogs: tuple[Catalog, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogsRefreshFailed:
    repo: RepoHandle
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadSelected:
    workload: Workload


@dataclasses.dataclass(frozen=True, slots=True)
class VersionsLoaded:
    """No ``epoch``/``request``: keyed by the selected workload itself."""

    workload: WorkloadKey
    versions: tuple[Version, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class VersionsLoadFailed:
    """A plain message: a workload is selectable only once its catalog's
    ``workloads()`` succeeded, so this is never ``KEY_REQUIRED``."""

    workload: WorkloadKey
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class RefreshRequested:
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class TreeFilterOpened:
    tree: FilterTree
    parent_key: object


@dataclasses.dataclass(frozen=True, slots=True)
class TreeFilterTextChanged:
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class TreeFilterClosed:
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class VersionFilterOpened:
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class VersionFilterTextChanged:
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class VersionFilterClosed:
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class VerboseSet:
    verbose: bool


BrowseMsg = (
    RescanStarted
    | RepoAdded
    | RepoSelected
    | CatalogsRequested
    | CatalogsLoaded
    | CatalogsLoadFailed
    | CatalogSelected
    | WorkloadsLoaded
    | WorkloadsLoadFailed
    | RepoKeyStatusRefreshed
    | KeyVerified
    | CatalogsRefreshed
    | CatalogsRefreshFailed
    | WorkloadSelected
    | VersionsLoaded
    | VersionsLoadFailed
    | RefreshRequested
    | TreeFilterOpened
    | TreeFilterTextChanged
    | TreeFilterClosed
    | VersionFilterOpened
    | VersionFilterTextChanged
    | VersionFilterClosed
    | VerboseSet
)
