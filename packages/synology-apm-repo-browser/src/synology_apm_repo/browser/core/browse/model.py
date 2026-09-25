"""``BrowseModel``: ``BrowseScreen``'s own three-column navigation state --
which repositories/catalogs a scan discovered, each catalog's own
lazily-fetched workload list, and each selected workload's own
lazily-fetched version list, plus the currently-selected catalog/workload
and the two independent filter mechanisms (the tree filter shared by
columns 1/2, the version filter over column 3).

Two independent staleness tokens, the same convention as
``core/unit/model.py``: ``epoch`` is bumped by a fresh scan (``
RescanStarted``), which invalidates every in-flight fetch across every
repository/catalog/workload at once; ``inflight`` tracks one
``RequestId`` per ``Slot`` for everything narrower (one repository's own
catalogs fetch, one catalog's own workloads fetch, one workload's own
versions fetch, each independently superseded by a fresh dispatch into
that same slot).

Each of ``repos``/``catalog_workloads``/``workload_versions`` doubles as
this screen's own skip-refetch cache -- a catalog's ``Success`` workload
list, once fetched, is served again on a later reselect without a new
``LoadWorkloads`` ``Cmd``. A ``FailureInfo`` -- including a key-required
failure -- is never cached as a hit, so reselecting after fixing
whatever failed always retries."""

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
    """One discovered repository's own presentation-relevant snapshot --
    ``layout``/``key_status`` are everything ``repo_labels.py``'s pure
    label functions need -- plus its own lazily-loaded ``catalogs``.
    Never the real ``Repository`` object itself: a frozen model can't
    hold its live ``aiosqlite`` connection. ``key_status`` is a
    snapshot taken when the repository was discovered, refreshed
    explicitly by ``CatalogsRefreshed`` after a key verification -- never
    read live off a ``Repository``, which this model never holds."""

    layout: RepositoryLayout
    key_status: KeyStatus
    catalogs: RemoteData[tuple[Catalog, ...]] = dataclasses.field(default_factory=NotAsked)


@dataclasses.dataclass(frozen=True, slots=True)
class SelectedCatalog:
    """``repo``/``catalog`` are always set together, by ``CatalogSelected``
    alone -- a bare repository-node click (``RepoSelected``) never
    touches this field at all, since selecting a repository doesn't
    imply selecting any one of its catalogs."""

    repo: RepoHandle
    catalog: Catalog


@dataclasses.dataclass(frozen=True, slots=True)
class TreeFilterState:
    """Which column-1/2 tree level is currently narrowed by ``/`` --
    ``tree`` picks which of the two trees ('"catalogs"' or
    '"workloads"'), ``parent_key`` is that tree's own domain key for the
    level being filtered (a ``RepoHandle`` for a column-1 repository
    node, a ``select.py``-defined ``WorkloadGroupKey`` for a column-2
    group node)."""

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
    #: A ``KeyVerified``-triggered reload's own failure -- the narrow
    #: race where the catalog that just verified a key is already gone
    #: (or fails to re-open) by the time ``repo.catalog_by_id()`` re-runs,
    #: found *before* any catalog was ever selected, so there's no
    #: ``CatalogKey`` to hang a normal ``catalog_workloads`` ``FailureInfo``
    #: entry off of. Rendered as column 2's own single error leaf (see
    #: ``select.py``'s ``workload_tree_spec``), the same "root_error"-style
    #: field ``core/unit/model.py``'s ``UnitModel`` uses for its own
    #: analogous "nothing selected yet to hang this failure off of" case.
    reload_failure: str | None = None

    tree_filter: TreeFilterState | None = None
    version_filter: VersionFilterState | None = None
