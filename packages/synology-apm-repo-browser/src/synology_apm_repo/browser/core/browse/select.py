"""Pure ``BrowseModel`` -> ``NodeSpec`` tree translation for
``view/reconcile.py``, for columns 1 (catalogs) and 2 (workloads). Column 3
(versions) has no reconciler equivalent -- ``DataTable`` can't be
reconciled the way ``Tree`` is -- so its own pure projections,
``version_rows``/``version_load_error``, hand back plain rows/an error
message instead.

Both trees' root is a permanent, non-domain container ("Catalogs"/
"Workloads") that never changes shape, so ``catalog_tree_spec``/
``workload_tree_spec`` return that root's children directly, always a
plain tuple, for the screen to hand to ``reconcile_children(tree.root, ...)``.

The device/SaaS grouping algorithm is ``workload_grouping.py``'s
``_group_workloads``; this module only turns its result into ``NodeSpec``s
and applies the tree filter."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.browse.model import BrowseModel, RepoState, catalog_key, workload_key
from synology_apm_repo.browser.core.keys import CatalogKey, RepoHandle, WorkloadKey
from synology_apm_repo.browser.core.remote_data import (
    FailureInfo,
    Loading,
    NotAsked,
    NoValue,
    RemoteData,
    has_ever_resolved,
    value_or_stale,
)
from synology_apm_repo.browser.repo_labels import _catalog_label, _repo_label
from synology_apm_repo.browser.view.reconcile import NodeSpec
from synology_apm_repo.browser.workload_grouping import _group_workloads, _humanize_type
from synology_apm_repo.sdk.api import Version, Workload
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.node_ref import disambiguate_catalogs, disambiguate_versions, disambiguate_workloads


@dataclasses.dataclass(frozen=True, slots=True)
class RepoErrorKey:
    """Synthetic key for the error leaf a repository whose ``catalogs()``
    fetch failed renders under itself -- scoped by ``repo`` so
    ``find_node``'s whole-tree walk resolves the right repository's error
    leaf when more than one is failing at once."""

    repo: RepoHandle


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadGroupKey:
    """Path-encoded, globally unique within column 2's tree: ``(type_hint,)``
    for a device group, ``(platform_type,)`` for a SaaS platform header,
    ``(platform_type, tenant_key)`` for a tenant/domain node,
    ``(platform_type, tenant_key, type_hint)`` for a nested sub_type group."""

    path: tuple[str, ...]


#: Column 2's single error leaf, replacing the whole tree -- only one
#: catalog is ever selected at a time, so no cross-selection scoping needed.
WORKLOAD_ERROR_KEY = WorkloadGroupKey(path=("__workload_error__",))

CatalogTreeKey = RepoHandle | CatalogKey | RepoErrorKey
WorkloadTreeKey = WorkloadGroupKey | WorkloadKey


def _tree_needle(model: BrowseModel, tree: str, parent_key: object) -> str:
    filter_state = model.tree_filter
    if filter_state is not None and filter_state.tree == tree and filter_state.parent_key == parent_key:
        return filter_state.text.lower()
    return ""


def _catalog_children_spec(
    model: BrowseModel, repo: RepoHandle, state: RepoState, *, verbose: bool
) -> tuple[NodeSpec[CatalogTreeKey], ...] | None:
    if isinstance(state.catalogs, NotAsked | Loading):
        return None  # not yet expanded, or still loading -- NodeSpec's own "not modelled" contract
    if isinstance(state.catalogs, FailureInfo):
        # state.catalogs.message is an exception's own str() -- arbitrary
        # text, escaped before reaching this Tree label so a literal `[`
        # in it can't be misread as a Rich markup tag.
        error_spec: NodeSpec[CatalogTreeKey] = NodeSpec(
            key=RepoErrorKey(repo=repo),
            label=f"error: {safe(state.catalogs.message)}",
            payload=None,
            allow_expand=False,
        )
        return (error_spec,)
    catalogs = state.catalogs.value
    names = disambiguate_catalogs(list(catalogs))
    needle = _tree_needle(model, "catalogs", repo)
    specs: list[NodeSpec[CatalogTreeKey]] = []
    for catalog, name in zip(catalogs, names, strict=True):
        if needle and needle not in catalog.display_name.lower():
            continue
        label = _catalog_label(catalog, name, verbose=verbose)
        specs.append(NodeSpec(key=catalog_key(repo, catalog), label=label, payload=catalog, allow_expand=False))
    return tuple(specs)


def catalog_tree_spec(model: BrowseModel, *, scan_path: str, verbose: bool) -> tuple[NodeSpec[CatalogTreeKey], ...]:
    """Column 1's top-level ``NodeSpec``s, one per discovered repository,
    in ``model.repos``'s insertion order. ``payload`` is the bare
    ``RepoHandle`` -- resolved back to the real ``Repository`` only
    inside an effect."""
    specs: list[NodeSpec[CatalogTreeKey]] = []
    for repo, state in model.repos.items():
        label = _repo_label(state.layout, state.key_status, scan_path, verbose=verbose)
        children = _catalog_children_spec(model, repo, state, verbose=verbose)
        specs.append(NodeSpec(key=repo, label=label, payload=repo, allow_expand=True, children=children))
    return tuple(specs)


def _workload_leaf_specs(
    model: BrowseModel, catalog: CatalogKey, group_key: WorkloadGroupKey, workloads: list[Workload]
) -> tuple[NodeSpec[WorkloadTreeKey], ...]:
    """``workloads`` already share one ``type_hint`` (one sub_type group's
    siblings), so ``use_type_hint=False``: hint-based disambiguation would
    just repeat what the grouping already shows."""
    needle = _tree_needle(model, "workloads", group_key)
    names = disambiguate_workloads(workloads, use_type_hint=False)
    specs: list[NodeSpec[WorkloadTreeKey]] = []
    for workload, name in zip(workloads, names, strict=True):
        if needle and needle not in workload.display_name.lower():
            continue
        # name is workload.display_name, disambiguated -- real,
        # backup-derived content, escaped before reaching this Tree label.
        specs.append(
            NodeSpec(key=workload_key(catalog, workload), label=safe(name), payload=workload, allow_expand=False)
        )
    return tuple(specs)


def _device_group_spec(model: BrowseModel, catalog: CatalogKey, workloads: list[Workload]) -> NodeSpec[WorkloadTreeKey]:
    type_hint = workloads[0].type_hint
    group_key = WorkloadGroupKey(path=(type_hint,))
    leaves = _workload_leaf_specs(model, catalog, group_key, workloads)
    return NodeSpec(
        key=group_key, label=_humanize_type(type_hint), payload=type_hint, allow_expand=True, children=leaves
    )


def _sub_type_group_spec(
    model: BrowseModel, catalog: CatalogKey, platform_type: str, tenant_key: str, workloads: list[Workload]
) -> NodeSpec[WorkloadTreeKey]:
    type_hint = workloads[0].type_hint
    group_key = WorkloadGroupKey(path=(platform_type, tenant_key, type_hint))
    leaves = _workload_leaf_specs(model, catalog, group_key, workloads)
    return NodeSpec(
        key=group_key, label=_humanize_type(type_hint), payload=type_hint, allow_expand=True, children=leaves
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
        _sub_type_group_spec(model, catalog, platform_type, tenant_key, group_workloads)
        for group_workloads in sub_groups.values()
    )
    # tenant_key is a real M365 tenant GUID or GW domain name
    # (_saas_group_key) -- escaped before reaching this Tree label.
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
        key=group_key, label=_humanize_type(platform_type), payload=platform_type, allow_expand=True, children=children
    )


def workload_tree_spec(model: BrowseModel, *, verbose: bool) -> tuple[NodeSpec[WorkloadTreeKey], ...]:
    """Column 2's top-level ``NodeSpec``s -- empty when there's nothing to
    show, one synthetic error leaf on ``FailureInfo``, or the device/SaaS
    grouping on ``Success``. ``verbose`` has nothing to add at this level
    yet, but is threaded through so a future verbose-only detail doesn't
    need a new plumbing pass."""
    if model.reload_failure is not None:
        # An exception's own str() -- arbitrary text, escaped before
        # reaching this Tree label, same reasoning as
        # _catalog_children_spec's own identical error leaf above.
        error_spec: NodeSpec[WorkloadTreeKey] = NodeSpec(
            key=WORKLOAD_ERROR_KEY, label=f"error: {safe(model.reload_failure)}", payload=None, allow_expand=False
        )
        return (error_spec,)
    if model.selected_catalog is None:
        return ()
    catalog = catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog)
    state = model.catalog_workloads.get(catalog, NotAsked())
    if isinstance(state, FailureInfo):
        workloads_error_spec: NodeSpec[WorkloadTreeKey] = NodeSpec(
            key=WORKLOAD_ERROR_KEY, label=f"error: {safe(state.message)}", payload=None, allow_expand=False
        )
        return (workloads_error_spec,)
    # value_or_stale: a refresh's own Loading carries the last Success
    # forward -- rendered here exactly like a real Success so column 2
    # stays populated, stale-but-real, while the refetch is in flight,
    # instead of blanking to empty.
    workloads = value_or_stale(state)
    if isinstance(workloads, NoValue):
        return ()  # NotAsked, or a first Loading with nothing stale to show yet
    grouping = _group_workloads(list(workloads))
    device_specs = [
        _device_group_spec(model, catalog, group_workloads) for group_workloads in grouping.device_groups.values()
    ]
    saas_specs = [
        _platform_spec(model, catalog, platform_type, tenant_groups)
        for platform_type, tenant_groups in grouping.saas_groups.items()
    ]
    return tuple(device_specs + saas_specs)


def _current_versions_state(model: BrowseModel) -> RemoteData[tuple[Version, ...]] | None:
    """The current workload's ``workload_versions`` entry, or ``None``
    before a catalog/workload is selected."""
    if model.selected_catalog is None or model.selected_workload is None:
        return None
    catalog = catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog)
    workload = workload_key(catalog, model.selected_workload)
    return model.workload_versions.get(workload, NotAsked())


def version_rows(model: BrowseModel) -> tuple[tuple[int, str], ...]:
    """The disambiguated ``(original_index, name)`` pairs for the selected
    workload's versions -- unfiltered; the screen applies
    ``model.version_filter``'s text itself, since that's
    ``DataTable``-rendering-local, not a ``NodeSpec``-tree concern. Empty
    when nothing's selected or its fetch hasn't succeeded yet."""
    state = _current_versions_state(model)
    if state is None:
        return ()
    # Same stale-while-revalidate rendering as workload_tree_spec above --
    # a refresh's own Loading carries the last Success forward.
    versions = value_or_stale(state)
    if isinstance(versions, NoValue):
        return ()
    names = disambiguate_versions(list(versions))
    return tuple(enumerate(names))


def version_fetch_pending(model: BrowseModel) -> bool:
    """True exactly when the current workload's versions fetch hasn't
    resolved even once yet. ``False`` for a failed fetch too -- that's a
    resolution, rendered separately via ``version_load_error``. Read by
    ``_render_versions`` to decide when its empty-state placeholder is
    safe to show, rather than coexisting with the debounced loading row."""
    state = _current_versions_state(model)
    return state is not None and not isinstance(state, FailureInfo) and not has_ever_resolved(state)


def version_load_error(model: BrowseModel) -> str | None:
    """The current workload's ``versions()`` failure message, if any --
    column 3's single error row, the flat-``DataTable`` counterpart to a
    tree's synthetic error leaf. ``None`` when nothing's selected or its
    fetch didn't fail."""
    state = _current_versions_state(model)
    return state.message if isinstance(state, FailureInfo) else None


def is_workload_current(model: BrowseModel, key: WorkloadKey) -> bool:
    """True when ``key`` is still the selected workload -- read by a
    ``LoadVersions`` fetch's ``DataTableLoadingRowSink`` before writing
    into column 3, so a fetch for a workload navigated away from can't
    leak a stray row."""
    if model.selected_catalog is None or model.selected_workload is None:
        return False
    catalog = catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog)
    return workload_key(catalog, model.selected_workload) == key
