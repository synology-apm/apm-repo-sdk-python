"""``BrowseModel``: ``BrowseScreen``'s three-column navigation state --
discovered repositories/catalogs, each catalog's lazily-fetched workload
list, each selected workload's lazily-fetched version list, the current
selection, and the two independent filters (tree filter over columns 1/2,
version filter over column 3).

Two staleness tokens (same convention as ``core/unit/model.py``): ``epoch``
is bumped by a fresh scan, invalidating every in-flight fetch at once;
``inflight`` tracks one ``RequestId`` per ``Slot`` for narrower fetches.

``repos``/``catalog_workloads``/``workload_versions`` double as a
skip-refetch cache: a ``Success`` is served again on reselect; a
``FailureInfo`` never is, so a reselect after fixing a failure retries."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping

from synology_apm_repo.browser.core.keys import CatalogKey, Epoch, RepoHandle, RequestId, Slot, WorkloadKey
from synology_apm_repo.browser.core.remote_data import NotAsked, RemoteData
from synology_apm_repo.sdk.api import Catalog, KeyStatus, RepositoryLayout, Version, Workload


def catalogs_slot(repo: RepoHandle) -> Slot:
    return Slot(kind="catalogs", key=repo)


def workloads_slot(catalog: CatalogKey) -> Slot:
    return Slot(kind="workloads", key=catalog)


def versions_slot(workload: WorkloadKey) -> Slot:
    return Slot(kind="versions", key=workload)


def catalog_key(repo: RepoHandle, catalog: Catalog) -> CatalogKey:
    return CatalogKey(repo=repo, catalog_id=catalog.catalog_id)


def workload_key(catalog: CatalogKey, workload: Workload) -> WorkloadKey:
    return WorkloadKey(catalog=catalog, workload_uid=workload.workload_uid)


@dataclasses.dataclass(frozen=True, slots=True)
class RepoState:
    """One discovered repository's presentation snapshot -- ``layout``/
    ``key_status`` plus its lazily-loaded ``catalogs``. Never the real
    ``Repository`` object: a frozen model can't hold its live ``aiosqlite``
    connection. ``key_status`` is refreshed explicitly by
    ``CatalogsRefreshed`` after a key verification, never read live."""

    layout: RepositoryLayout
    key_status: KeyStatus
    catalogs: RemoteData[tuple[Catalog, ...]] = dataclasses.field(default_factory=NotAsked)


@dataclasses.dataclass(frozen=True, slots=True)
class SelectedCatalog:
    """``repo``/``catalog`` are always set together, by ``CatalogSelected``
    alone -- selecting a bare repository node never touches this field."""

    repo: RepoHandle
    catalog: Catalog


@dataclasses.dataclass(frozen=True, slots=True)
class TreeFilterState:
    """Which column-1/2 tree level is narrowed by ``/`` -- ``tree`` picks
    ``"catalogs"``/``"workloads"``, ``parent_key`` is that level's domain
    key."""

    tree: str
    parent_key: object
    text: str = ""


@dataclasses.dataclass(frozen=True, slots=True)
class VersionFilterState:
    text: str = ""


@dataclasses.dataclass(frozen=True)
class BrowseModel:
    epoch: Epoch = Epoch(0)
    inflight: Mapping[Slot, RequestId] = dataclasses.field(default_factory=dict)
    next_request: RequestId = RequestId(1)

    scan_path: str = ""
    #: Discovery order -- a plain ``dict`` preserves insertion order, the
    #: same convention ``UnitModel.loaded`` already relies on.
    repos: Mapping[RepoHandle, RepoState] = dataclasses.field(default_factory=dict)
    catalog_workloads: Mapping[CatalogKey, RemoteData[tuple[Workload, ...]]] = dataclasses.field(default_factory=dict)
    workload_versions: Mapping[WorkloadKey, RemoteData[tuple[Version, ...]]] = dataclasses.field(default_factory=dict)

    selected_catalog: SelectedCatalog | None = None
    selected_workload: Workload | None = None
    #: A ``KeyVerified``-triggered reload's failure, found before any
    #: catalog is selected so there's no ``CatalogKey`` to hang a normal
    #: ``FailureInfo`` off of. Rendered as column 2's single error leaf.
    reload_failure: str | None = None

    tree_filter: TreeFilterState | None = None
    version_filter: VersionFilterState | None = None
