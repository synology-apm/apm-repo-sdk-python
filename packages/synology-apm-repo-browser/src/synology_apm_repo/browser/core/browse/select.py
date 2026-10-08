"""Selectors over ``BrowseModel``: ``NodeSpec`` trees for columns 1
(catalogs) and 2 (workloads), plain rows for column 3's ``DataTable``
(versions), and the breadcrumb.

``catalog_tree_spec``/``workload_tree_spec`` return the children of their
tree's fixed root ("Catalogs"/"Workloads"), for
``reconcile_children(tree.root, ...)``."""

from __future__ import annotations

import dataclasses
from typing import cast

from synology_apm_repo.browser.core.browse.model import BrowseModel, FilterTree, RepoState, catalog_key, workload_key
from synology_apm_repo.browser.core.browse.msg import TreeFilterOpened
from synology_apm_repo.browser.core.browse.repo_labels import catalog_label, repo_label, repo_path_component
from synology_apm_repo.browser.core.browse.workload_grouping import (
    group_workloads,
    humanize_type,
    saas_group_key,
)
from synology_apm_repo.browser.core.keys import CatalogKey, RepoHandle, WorkloadKey
from synology_apm_repo.browser.core.remote_data import (
    FailureInfo,
    Loading,
    NotAsked,
    NoValue,
    RemoteData,
    Success,
    has_ever_resolved,
    value_or_stale,
)
from synology_apm_repo.browser.core.text_filter import matches_filter
from synology_apm_repo.browser.view.reconcile import NodeSpec
from synology_apm_repo.sdk import (
    Version,
    Workload,
    disambiguate_catalogs,
    disambiguate_versions,
    disambiguate_workloads,
)
from synology_apm_repo.sdk.presentation import safe


@dataclasses.dataclass(frozen=True, slots=True)
class RepoErrorKey:
    """Key of the error leaf under a repository whose ``catalogs()`` fetch
    failed, scoped by ``repo`` so keys stay unique across the tree."""

    repo: RepoHandle


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadGroupKey:
    """Path-encoded, globally unique within column 2's tree: ``(type_hint,)``
    for a device group, ``(platform_type,)`` for a SaaS platform header,
    ``(platform_type, tenant_key)`` for a tenant/domain node,
    ``(platform_type, tenant_key, type_hint)`` for a nested sub_type group."""

    path: tuple[str, ...]


#: Column 2's single error leaf, which replaces the whole tree.
WORKLOAD_ERROR_KEY = WorkloadGroupKey(path=("__workload_error__",))

CatalogTreeKey = RepoHandle | CatalogKey | RepoErrorKey
WorkloadTreeKey = WorkloadGroupKey | WorkloadKey


def _tree_needle(model: BrowseModel, tree: FilterTree, parent_key: object) -> str:
    filter_state = model.tree_filter
    if filter_state is not None and filter_state.tree == tree and filter_state.parent_key == parent_key:
        return filter_state.text
    return ""


def _catalog_children_spec(
    model: BrowseModel, repo: RepoHandle, state: RepoState, *, verbose: bool
) -> tuple[NodeSpec[CatalogTreeKey], ...] | None:
    if isinstance(state.catalogs, NotAsked | Loading):
        return None  # NodeSpec's "children not modelled" value
    if isinstance(state.catalogs, FailureInfo):
        # Exception text is arbitrary; escape it for the Tree label.
        error_spec: NodeSpec[CatalogTreeKey] = NodeSpec(
            key=RepoErrorKey(repo=repo),
            label=f"error: {safe(state.catalogs.message)}",
            payload=None,
            allow_expand=False,
        )
        return (error_spec,)
    catalogs = state.catalogs.value
    names = disambiguate_catalogs(list(catalogs))
    needle = _tree_needle(model, FilterTree.CATALOGS, repo)
    specs: list[NodeSpec[CatalogTreeKey]] = []
    for catalog, name in zip(catalogs, names, strict=True):
        if not matches_filter(needle, catalog.display_name):
            continue
        label = catalog_label(catalog, name, verbose=verbose)
        specs.append(NodeSpec(key=catalog_key(repo, catalog), label=label, payload=catalog, allow_expand=False))
    return tuple(specs)


def catalog_tree_spec(model: BrowseModel) -> tuple[NodeSpec[CatalogTreeKey], ...]:
    """Column 1's top-level ``NodeSpec``s, one per discovered repository in
    discovery order, each with its ``RepoHandle`` as payload."""
    specs: list[NodeSpec[CatalogTreeKey]] = []
    for repo, state in model.repos.items():
        label = repo_label(state.layout, state.key_status, model.scan_path, verbose=model.verbose)
        children = _catalog_children_spec(model, repo, state, verbose=model.verbose)
        specs.append(NodeSpec(key=repo, label=label, payload=repo, allow_expand=True, children=children))
    return tuple(specs)


def _workload_leaf_specs(
    model: BrowseModel, catalog: CatalogKey, group_key: WorkloadGroupKey, workloads: list[Workload]
) -> tuple[NodeSpec[WorkloadTreeKey], ...]:
    """``workloads`` share one ``type_hint``, which their group already
    shows, so disambiguation doesn't use it."""
    needle = _tree_needle(model, FilterTree.WORKLOADS, group_key)
    names = disambiguate_workloads(workloads, use_type_hint=False)
    specs: list[NodeSpec[WorkloadTreeKey]] = []
    for workload, name in zip(workloads, names, strict=True):
        if not matches_filter(needle, workload.display_name):
            continue
        # Backup-derived content: escape for the Tree label.
        specs.append(
            NodeSpec(key=workload_key(catalog, workload), label=safe(name), payload=workload, allow_expand=False)
        )
    return tuple(specs)


def _device_group_spec(model: BrowseModel, catalog: CatalogKey, workloads: list[Workload]) -> NodeSpec[WorkloadTreeKey]:
    type_hint = workloads[0].type_hint
    group_key = WorkloadGroupKey(path=(type_hint,))
    leaves = _workload_leaf_specs(model, catalog, group_key, workloads)
    return NodeSpec(
        key=group_key, label=humanize_type(type_hint), payload=type_hint, allow_expand=True, children=leaves
    )


def _sub_type_group_spec(
    model: BrowseModel, catalog: CatalogKey, platform_type: str, tenant_key: str, workloads: list[Workload]
) -> NodeSpec[WorkloadTreeKey]:
    type_hint = workloads[0].type_hint
    group_key = WorkloadGroupKey(path=(platform_type, tenant_key, type_hint))
    leaves = _workload_leaf_specs(model, catalog, group_key, workloads)
    return NodeSpec(
        key=group_key, label=humanize_type(type_hint), payload=type_hint, allow_expand=True, children=leaves
    )


def _tenant_spec(
    model: BrowseModel,
    catalog: CatalogKey,
    platform_type: str,
    tenant_key: str,
    sub_groups: dict[str, list[Workload]],
) -> NodeSpec[WorkloadTreeKey]:
    group_key = WorkloadGroupKey(path=(platform_type, tenant_key))
    children = tuple(
        _sub_type_group_spec(model, catalog, platform_type, tenant_key, workloads_in_group)
        for workloads_in_group in sub_groups.values()
    )
    # tenant_key is real tenant/domain text: escape for the Tree label.
    return NodeSpec(key=group_key, label=safe(tenant_key), payload=tenant_key, allow_expand=True, children=children)


def _platform_spec(
    model: BrowseModel, catalog: CatalogKey, platform_type: str, tenant_groups: dict[str, dict[str, list[Workload]]]
) -> NodeSpec[WorkloadTreeKey]:
    group_key = WorkloadGroupKey(path=(platform_type,))
    children = tuple(
        _tenant_spec(model, catalog, platform_type, tenant_key, sub_groups)
        for tenant_key, sub_groups in tenant_groups.items()
    )
    return NodeSpec(
        key=group_key, label=humanize_type(platform_type), payload=platform_type, allow_expand=True, children=children
    )


def workload_tree_spec(model: BrowseModel) -> tuple[NodeSpec[WorkloadTreeKey], ...]:
    """Column 2's top-level ``NodeSpec``s: empty when there's nothing to
    show, one error leaf on a failed catalog reload or workloads fetch, else
    the device/SaaS grouping."""
    if model.reload_failure is not None:
        # Exception text is arbitrary; escape it for the Tree label.
        error_spec: NodeSpec[WorkloadTreeKey] = NodeSpec(
            key=WORKLOAD_ERROR_KEY, label=f"error: {safe(model.reload_failure)}", payload=None, allow_expand=False
        )
        return (error_spec,)
    if model.selected_catalog is None:
        return ()
    catalog = model.selected_catalog.key
    state = model.catalog_workloads.get(catalog, NotAsked())
    if isinstance(state, FailureInfo):
        workloads_error_spec: NodeSpec[WorkloadTreeKey] = NodeSpec(
            key=WORKLOAD_ERROR_KEY, label=f"error: {safe(state.message)}", payload=None, allow_expand=False
        )
        return (workloads_error_spec,)
    workloads = value_or_stale(state)
    if isinstance(workloads, NoValue):
        return ()  # NotAsked, or a first Loading with nothing stale to show yet
    grouping = group_workloads(list(workloads))
    device_specs = [
        _device_group_spec(model, catalog, workloads_in_group) for workloads_in_group in grouping.device_groups.values()
    ]
    saas_specs = [
        _platform_spec(model, catalog, platform_type, tenant_groups)
        for platform_type, tenant_groups in grouping.saas_groups.items()
    ]
    return tuple(device_specs + saas_specs)


def _current_versions_state(model: BrowseModel) -> RemoteData[tuple[Version, ...]] | None:
    """The current workload's ``workload_versions`` entry, or ``None``
    before a catalog/workload is selected."""
    key = model.selected_workload_key
    return model.workload_versions.get(key, NotAsked()) if key is not None else None


def version_rows(model: BrowseModel) -> tuple[tuple[int, str], ...]:
    """The disambiguated ``(original_index, name)`` pairs for the selected
    workload's versions, unfiltered (``visible_version_rows`` applies
    ``model.version_filter``). Empty when nothing's selected or its fetch
    hasn't succeeded yet."""
    state = _current_versions_state(model)
    if state is None:
        return ()
    versions = value_or_stale(state)
    if isinstance(versions, NoValue):
        return ()
    names = disambiguate_versions(list(versions))
    return tuple(enumerate(names))


def visible_version_rows(model: BrowseModel) -> tuple[tuple[int, str], ...]:
    """``version_rows`` narrowed by ``model.version_filter``."""
    needle = model.version_filter.text if model.version_filter is not None else ""
    return tuple(row for row in version_rows(model) if matches_filter(needle, row[1]))


def version_fetch_pending(model: BrowseModel) -> bool:
    """Whether the current workload's versions have never resolved (a
    failure counts as resolved), so the empty-state placeholder stays
    hidden while loading."""
    state = _current_versions_state(model)
    return state is not None and not isinstance(state, FailureInfo) and not has_ever_resolved(state)


def version_load_error(model: BrowseModel) -> str | None:
    """The current workload's ``versions()`` failure message, shown as
    column 3's single error row, or ``None``."""
    state = _current_versions_state(model)
    return state.message if isinstance(state, FailureInfo) else None


def is_workload_current(model: BrowseModel, key: WorkloadKey) -> bool:
    """Whether ``key`` is still the selected workload, so a stale
    ``LoadVersions`` fetch's loading row stays out of column 3."""
    return model.selected_workload_key == key


def current_workloads(model: BrowseModel) -> tuple[Workload, ...] | None:
    """The selected catalog's loaded workloads, or ``None`` until they
    have loaded."""
    if model.selected_catalog is None:
        return None
    state = model.catalog_workloads.get(model.selected_catalog.key)
    return state.value if isinstance(state, Success) else None


def breadcrumb(model: BrowseModel) -> str:
    """The selection path above the three columns, Rich-escaped (names
    are backup content), or ``/`` with nothing selected."""
    parts = []
    if model.selected_catalog is not None:
        repo_state = model.repos.get(model.selected_catalog.repo)
        if repo_state is not None:
            parts.append(safe(repo_path_component(repo_state.layout, model.scan_path)))
        parts.append(safe(model.selected_catalog.catalog.display_name))
    if model.selected_workload is not None:
        workload = model.selected_workload
        if workload.is_saas:
            parts.append(safe(humanize_type(workload.workload_type)))
            parts.append(safe(saas_group_key(workload)))
        parts.append(safe(humanize_type(workload.type_hint)))
        parts.append(safe(workload.display_name))
    return " › ".join(parts) if parts else "/"


def tree_filter_target(model: BrowseModel, tree: FilterTree, parent_key: object) -> TreeFilterOpened | None:
    """The filter to open over ``tree``'s listing under ``parent_key``, or
    ``None`` when nothing loaded there can be narrowed: a catalog-tree
    cursor below a repository, a repository whose catalogs never loaded,
    or a workload leaf rather than a group."""
    if tree is FilterTree.CATALOGS:
        if parent_key is None or isinstance(parent_key, CatalogKey | RepoErrorKey):
            return None
        state = model.repos.get(cast(RepoHandle, parent_key))
        if state is None or not isinstance(state.catalogs, Success):
            return None
        return TreeFilterOpened(tree=FilterTree.CATALOGS, parent_key=parent_key)
    if not isinstance(parent_key, WorkloadGroupKey):
        return None
    return TreeFilterOpened(tree=FilterTree.WORKLOADS, parent_key=parent_key)
