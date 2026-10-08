"""``BrowseModel``: ``BrowseScreen``'s three-column navigation state --
discovered repositories and catalogs, lazily fetched workload and version
lists, the selection, and two filters (the tree filter over columns 1/2,
the version filter over column 3).

``epoch`` is bumped by a fresh scan, invalidating every in-flight fetch;
``inflight`` tracks one ``RequestId`` per ``Slot``.

``repos``/``catalog_workloads``/``workload_versions`` double as a
skip-refetch cache: a ``Success`` is served again on reselect, a
``FailureInfo`` is retried."""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Mapping

from synology_apm_repo.browser.core.keys import CatalogKey, Epoch, RepoHandle, RequestId, Slot, WorkloadKey
from synology_apm_repo.browser.core.remote_data import NotAsked, RemoteData
from synology_apm_repo.sdk import Catalog, KeyStatus, RepositoryLayout, Version, Workload


class FilterTree(enum.Enum):
    """Which of ``BrowseScreen``'s two trees a ``/`` filter narrows."""

    CATALOGS = "catalogs"
    WORKLOADS = "workloads"


def catalogs_slot(repo: RepoHandle) -> Slot:
    return Slot(kind="catalogs", key=repo)


def catalog_key(repo: RepoHandle, catalog: Catalog) -> CatalogKey:
    return CatalogKey(repo=repo, catalog_id=catalog.catalog_id)


def workload_key(catalog: CatalogKey, workload: Workload) -> WorkloadKey:
    return WorkloadKey(catalog=catalog, workload_uid=workload.workload_uid)


@dataclasses.dataclass(frozen=True, slots=True)
class RepoState:
    """One discovered repository's snapshot: ``layout``, ``key_status``
    (refreshed by ``RepoKeyStatusRefreshed``) and its lazily loaded
    ``catalogs``."""

    layout: RepositoryLayout
    key_status: KeyStatus
    catalogs: RemoteData[tuple[Catalog, ...]] = dataclasses.field(default_factory=NotAsked)


@dataclasses.dataclass(frozen=True, slots=True)
class SelectedCatalog:
    """The selected catalog, set by ``CatalogSelected`` only."""

    repo: RepoHandle
    catalog: Catalog

    @property
    def key(self) -> CatalogKey:
        return catalog_key(self.repo, self.catalog)


@dataclasses.dataclass(frozen=True, slots=True)
class TreeFilterState:
    """The open ``/`` filter on a tree level: ``tree`` picks the tree,
    ``parent_key`` is the filtered level's domain key."""

    tree: FilterTree
    parent_key: object
    text: str = ""


@dataclasses.dataclass(frozen=True, slots=True)
class VersionFilterState:
    text: str = ""


@dataclasses.dataclass(frozen=True, slots=True)
class BrowseModel:
    epoch: Epoch = Epoch(0)
    inflight: Mapping[Slot, RequestId] = dataclasses.field(default_factory=dict)
    next_request: RequestId = RequestId(1)

    scan_path: str = ""
    #: In discovery order.
    repos: Mapping[RepoHandle, RepoState] = dataclasses.field(default_factory=dict)
    catalog_workloads: Mapping[CatalogKey, RemoteData[tuple[Workload, ...]]] = dataclasses.field(default_factory=dict)
    workload_versions: Mapping[WorkloadKey, RemoteData[tuple[Version, ...]]] = dataclasses.field(default_factory=dict)

    selected_catalog: SelectedCatalog | None = None
    selected_workload: Workload | None = None
    #: A failed catalog-list reload after ``KeyVerified``, which has no
    #: ``CatalogKey`` to store a ``FailureInfo`` under; column 2's error leaf.
    reload_failure: str | None = None

    tree_filter: TreeFilterState | None = None
    version_filter: VersionFilterState | None = None
    #: The app's verbose flag, set by the screen through ``VerboseSet``.
    verbose: bool = False

    @property
    def selected_workload_key(self) -> WorkloadKey | None:
        """The selected workload's key, or ``None`` until both a catalog and
        a workload are selected."""
        if self.selected_catalog is None or self.selected_workload is None:
            return None
        return workload_key(self.selected_catalog.key, self.selected_workload)
