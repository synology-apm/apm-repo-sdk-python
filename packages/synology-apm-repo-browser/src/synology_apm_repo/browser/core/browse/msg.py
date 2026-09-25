"""``BrowseMsg``: every event ``BrowseScreen``'s own store can react to.

Only the two column-1 (catalogs) fetch-result variants carry an
``epoch``/``request`` pair: a widget-level collapse/re-expand can
dispatch a second ``CatalogsRequested`` for the same node while the first
fetch is still in flight, so column 1 needs to detect and discard a stale
result. The other fetch results are keyed by the currently-selected
catalog/workload object itself, so a differently-selected fetch's late
result can never overwrite what's rendered regardless of arrival order,
and the underlying reads are idempotent, so even a same-key overlap just
gets overwritten by an equivalent result."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import CatalogKey, Epoch, RepoHandle, RequestId, WorkloadKey
from synology_apm_repo.browser.core.remote_data import FailureInfo
from synology_apm_repo.sdk.api import Catalog, KeyStatus, RepositoryLayout, Version, Workload
from synology_apm_repo.sdk.identifiers import CatalogId


@dataclasses.dataclass(frozen=True)
class RescanStarted:
    """A fresh, already-completed ``ConnectDialog`` scan is about to be
    rendered -- discards every previously discovered repository/catalog/
    workload/version."""

    scan_path: str


@dataclasses.dataclass(frozen=True)
class RepoAdded:
    """One repository from the current scan -- ``layout``/``key_status``
    are captured by the screen straight off the real ``Repository`` at
    dispatch time (cheap, synchronous properties; only this snapshot lives
    in the model since the real ``Repository`` owns a live ``aiosqlite``
    connection, which can't live in a frozen model)."""

    repo: RepoHandle
    layout: RepositoryLayout
    key_status: KeyStatus


@dataclasses.dataclass(frozen=True)
class RepoSelected:
    """A bare repository node (not one of its catalogs) selected in
    column 1 -- only ever sets the app-wide "current repository", never
    ``selected_catalog``, since selecting a repository doesn't imply
    selecting any one of its catalogs."""

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
    """No ``epoch``/``request``: keyed by the currently-selected catalog
    itself, so a differently-selected fetch's late result can never
    overwrite what's rendered."""

    catalog: CatalogKey
    workloads: tuple[Workload, ...]


@dataclasses.dataclass(frozen=True)
class WorkloadsLoadFailed:
    """``real_catalog`` travels alongside ``catalog``/``info`` here --
    unlike ``CatalogsLoadFailed``, a ``KEY_REQUIRED`` failure needs the
    real ``Catalog`` to open ``KeyDialog`` against."""

    catalog: CatalogKey
    real_catalog: Catalog
    info: FailureInfo


@dataclasses.dataclass(frozen=True)
class RepoKeyStatusRefreshed:
    """``repo.key_status`` re-read live off the real ``Repository`` the
    instant ``KeyDialog`` dismisses -- unconditionally, regardless of
    whether it actually verified anything, since a wrong-key retry can
    still change ``key_status`` from ``NO_KEY_PROVIDED`` to ``INVALID``,
    and a bare cancel needs the label refreshed too. Dispatched before
    ``KeyVerified`` so column 1's own label is never a dispatch behind
    the rest of the reload."""

    repo: RepoHandle
    key_status: KeyStatus


@dataclasses.dataclass(frozen=True)
class KeyVerified:
    repo: RepoHandle
    catalog_id: CatalogId


@dataclasses.dataclass(frozen=True)
class CatalogsRefreshed:
    """Every sibling catalog under ``repo``, re-resolved after a key
    verification (``Repository.set_key()`` closes and replaces every
    already-opened ``DedupRepo`` this repository holds, not just the one
    that triggered ``KeyDialog``)."""

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
    """No ``epoch``/``request``: keyed by the currently-selected workload
    itself, so a differently-selected fetch's late result can never
    overwrite what's rendered."""

    workload: WorkloadKey
    versions: tuple[Version, ...]


@dataclasses.dataclass(frozen=True)
class VersionsLoadFailed:
    """Unlike ``workloads()``, a ``catalog.versions()`` failure is never
    ``KEY_REQUIRED``-shaped: a workload is only ever selectable once its
    own catalog's ``workloads()`` already succeeded, i.e. already
    key-verified. A plain message is enough -- rendered as column 3's
    own single error row (``select.py``'s ``version_load_error``), the
    flat-``DataTable`` counterpart to a tree's synthetic error leaf."""

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
