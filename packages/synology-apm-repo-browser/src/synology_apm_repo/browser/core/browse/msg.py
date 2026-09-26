"""``BrowseMsg``: every event ``BrowseScreen``'s store can react to.

Only the column-1 (catalogs) fetch-result variants carry an
``epoch``/``request`` pair, to detect and discard a stale result from a
widget-level collapse/re-expand racing a fetch already in flight. The
other fetch results are keyed by the currently-selected catalog/workload
object itself, so a differently-selected fetch's late result can never
overwrite what's rendered."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import CatalogKey, Epoch, RepoHandle, RequestId, WorkloadKey
from synology_apm_repo.browser.core.remote_data import FailureInfo
from synology_apm_repo.sdk.api import Catalog, KeyStatus, RepositoryLayout, Version, Workload
from synology_apm_repo.sdk.identifiers import CatalogId


@dataclasses.dataclass(frozen=True)
class RescanStarted:
    """A fresh ``ConnectDialog`` scan is about to be rendered -- discards
    every previously discovered repository/catalog/workload/version."""

    scan_path: str


@dataclasses.dataclass(frozen=True)
class RepoAdded:
    """One repository from the current scan -- ``layout``/``key_status``
    are captured straight off the real ``Repository`` at dispatch time;
    only this snapshot lives in the model, since the real object owns a
    live ``aiosqlite`` connection a frozen model can't hold."""

    repo: RepoHandle
    layout: RepositoryLayout
    key_status: KeyStatus


@dataclasses.dataclass(frozen=True)
class RepoSelected:
    """A bare repository node selected in column 1 -- sets the current
    repository, never ``selected_catalog``."""

    repo: RepoHandle


@dataclasses.dataclass(frozen=True)
class CatalogsRequested:
    repo: RepoHandle


@dataclasses.dataclass(frozen=True)
class CatalogsLoaded:
    epoch: Epoch
    request: RequestId
    repo: RepoHandle
    catalogs: tuple[Catalog, ...]


@dataclasses.dataclass(frozen=True)
class CatalogsLoadFailed:
    epoch: Epoch
    request: RequestId
    repo: RepoHandle
    message: str


@dataclasses.dataclass(frozen=True)
class CatalogSelected:
    repo: RepoHandle
    catalog: Catalog


@dataclasses.dataclass(frozen=True)
class WorkloadsLoaded:
    """No ``epoch``/``request``: keyed by the selected catalog itself."""

    catalog: CatalogKey
    workloads: tuple[Workload, ...]


@dataclasses.dataclass(frozen=True)
class WorkloadsLoadFailed:
    """``real_catalog`` travels alongside for the ``KEY_REQUIRED`` case,
    which needs it to open ``KeyDialog`` against."""

    catalog: CatalogKey
    real_catalog: Catalog
    info: FailureInfo


@dataclasses.dataclass(frozen=True)
class RepoKeyStatusRefreshed:
    """``repo.key_status`` re-read live the instant ``KeyDialog`` dismisses,
    unconditionally -- a wrong-key retry can still change it, and a bare
    cancel needs the label refreshed too. Dispatched before ``KeyVerified``
    so column 1's label is never a dispatch behind the rest of the reload."""

    repo: RepoHandle
    key_status: KeyStatus


@dataclasses.dataclass(frozen=True)
class KeyVerified:
    repo: RepoHandle
    catalog_id: CatalogId


@dataclasses.dataclass(frozen=True)
class CatalogsRefreshed:
    """Every sibling catalog under ``repo``, re-resolved after a key
    verification (``Repository.set_key()`` replaces every already-opened
    ``DedupRepo`` this repository holds, not just the one that triggered
    ``KeyDialog``)."""

    repo: RepoHandle
    catalogs: tuple[Catalog, ...]


@dataclasses.dataclass(frozen=True)
class CatalogsRefreshFailed:
    repo: RepoHandle
    message: str


@dataclasses.dataclass(frozen=True)
class WorkloadSelected:
    workload: Workload


@dataclasses.dataclass(frozen=True)
class VersionsLoaded:
    """No ``epoch``/``request``: keyed by the selected workload itself."""

    workload: WorkloadKey
    versions: tuple[Version, ...]


@dataclasses.dataclass(frozen=True)
class VersionsLoadFailed:
    """Unlike ``workloads()``, a ``versions()`` failure is never
    ``KEY_REQUIRED``-shaped: a workload is only selectable once its own
    catalog's ``workloads()`` already succeeded. A plain message is enough
    -- rendered as column 3's single error row."""

    workload: WorkloadKey
    message: str


@dataclasses.dataclass(frozen=True)
class RefreshRequested:
    pass


@dataclasses.dataclass(frozen=True)
class TreeFilterOpened:
    tree: str
    parent_key: object


@dataclasses.dataclass(frozen=True)
class TreeFilterTextChanged:
    text: str


@dataclasses.dataclass(frozen=True)
class TreeFilterClosed:
    pass


@dataclasses.dataclass(frozen=True)
class VersionFilterOpened:
    pass


@dataclasses.dataclass(frozen=True)
class VersionFilterTextChanged:
    text: str


@dataclasses.dataclass(frozen=True)
class VersionFilterClosed:
    pass


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
)
