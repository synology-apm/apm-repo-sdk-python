"""Pure unit tests for ``core/browse/select.py`` -- ``BrowseModel`` ->
``NodeSpec``/row translation, no Textual/App/Pilot involved at all. Same
convention as ``test_browser_core_unit_select.py``."""

from __future__ import annotations

from typing import cast

from synology_apm_repo.browser.core.browse.model import BrowseModel, RepoState, SelectedCatalog, TreeFilterState
from synology_apm_repo.browser.core.browse.model import catalog_key as _catalog_key_of
from synology_apm_repo.browser.core.browse.model import workload_key as _workload_key_of
from synology_apm_repo.browser.core.browse.select import (
    WORKLOAD_ERROR_KEY,
    RepoErrorKey,
    WorkloadGroupKey,
    catalog_tree_spec,
    is_workload_current,
    version_fetch_pending,
    version_load_error,
    version_rows,
    workload_tree_spec,
)
from synology_apm_repo.browser.core.keys import RepoHandle
from synology_apm_repo.browser.core.remote_data import FailureInfo, Loading, Success
from synology_apm_repo.sdk.api import Catalog, Connection, KeyStatus, Version, Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.repo_info import RepoInfo
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
    ConnectionConfigId,
    ConnectionId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache


def _layout(repo_root: str = "repo-1") -> RepositoryLayout:
    return RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=repo_root)


class _FakeCatalog:
    def __init__(self, catalog_id: str, display_name: str) -> None:
        self.catalog_id = CatalogId(catalog_id)
        self.display_name = display_name


def _catalog(catalog_id: str, display_name: str) -> Catalog:
    return cast(Catalog, _FakeCatalog(catalog_id, display_name))


def _real_catalog(connection_id: str, display_name: str) -> Catalog:
    """A genuine ``Catalog`` (not the duck-typed ``_FakeCatalog`` above)
    -- needed only where verbose-mode label formatting reaches
    ``catalog.info``/``catalog.connection`` (``repo_labels.py``'s
    ``_catalog_label``), which a duck-type can't satisfy. No I/O:
    ``Catalog.__init__`` does none itself, same reasoning as
    ``test_browser_browse_screen_gaps.py``'s own ``_fake_catalog``."""
    import types

    connection = Connection(
        connection_config_id=ConnectionConfigId(1),
        connection_id=ConnectionId(connection_id),
        display_name=display_name,
        namespaces=(),
        workload_count=1,
        version_count=1,
    )
    info = RepoInfo(
        uuid="repo-uuid",
        major=1,
        minor=0,
        repo_type=None,
        repo_flag=None,
        is_global_dedup_supported=None,
        is_worm_supported=None,
        compress_algorithm=None,
        encrypt_algorithm=None,
        raw={},
    )
    dedup_repo = types.SimpleNamespace(layout=RepoLayout(kind=RepoKind.VAULT, repo_root=""), info=info)
    typed_dedup_repo = cast(DedupRepo, dedup_repo)
    return Catalog(
        typed_dedup_repo,
        connection,
        saas_streams=SaasStreamCache(typed_dedup_repo),
        track=lambda provider: provider,
        require_key_verified=lambda: None,
    )


def _device_workload(workload_id: int, display_name: str, type_hint: str = "VM") -> Workload:
    return Workload(
        workload_id=WorkloadId(workload_id),
        workload_uid=f"wl-{workload_id}",  # type: ignore[arg-type]
        workload_type=type_hint,
        sub_type=None,
        display_name=display_name,
        subtitle=None,
        spec={},
    )


def _saas_workload(
    workload_id: int,
    display_name: str,
    *,
    workload_type: str,
    sub_type: str,
    tenant_id: str | None = None,
    domain: str | None = None,
) -> Workload:
    spec: dict[str, object] = {}
    if tenant_id is not None:
        spec["spec"] = {"tenant_id": tenant_id}
    if domain is not None:
        spec["spec"] = {"domain": domain}
    return Workload(
        workload_id=WorkloadId(workload_id),
        workload_uid=f"wl-{workload_id}",  # type: ignore[arg-type]
        workload_type=workload_type,
        sub_type=sub_type,
        display_name=display_name,
        subtitle=None,
        spec=spec,
    )


def _version(uid: str = "vuid-1", display_name: str = "2026-01-01 00:00") -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid(uid),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type="VM",
        target_id=TargetId("target"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name=display_name,
        meta=None,
    )


# -- catalog_tree_spec (column 1) -----------------------------------------


def test_catalog_tree_spec_is_empty_with_no_repos() -> None:
    assert catalog_tree_spec(BrowseModel(), scan_path="", verbose=False) == ()


def test_catalog_tree_spec_a_not_asked_repo_has_no_children_modeled() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)})
    specs = catalog_tree_spec(model, scan_path="/scan", verbose=False)

    assert len(specs) == 1
    assert specs[0].key == handle
    assert specs[0].payload == handle
    assert specs[0].allow_expand is True
    assert specs[0].children is None  # not yet expanded -- NodeSpec's own "not modelled" contract


def test_catalog_tree_spec_a_loading_repo_also_has_no_children_modeled() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED, catalogs=Loading())}
    )
    specs = catalog_tree_spec(model, scan_path="", verbose=False)

    assert specs[0].children is None


def test_catalog_tree_spec_a_failed_repo_renders_a_single_error_leaf() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(
        repos={
            handle: RepoState(
                layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED, catalogs=FailureInfo(message="boom [/] bang")
            )
        }
    )
    specs = catalog_tree_spec(model, scan_path="", verbose=False)

    assert specs[0].children is not None
    assert len(specs[0].children) == 1
    error_spec = specs[0].children[0]
    # An exception's own str() is arbitrary text -- escaped before
    # reaching this Tree label (Tree.process_label re-parses a plain str
    # as Rich markup, same as Static, and an unescaped bracket can crash
    # the widget outright with a markup error).
    assert error_spec.label == r"error: boom \[/] bang"
    assert error_spec.key == RepoErrorKey(repo=handle)
    assert error_spec.allow_expand is False


def test_catalog_tree_spec_renders_a_leaf_per_successfully_loaded_catalog() -> None:
    handle = RepoHandle(1)
    cat_a, cat_b = _catalog("a", "Catalog A"), _catalog("b", "Catalog B")
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.VERIFIED, catalogs=Success((cat_a, cat_b)))}
    )
    specs = catalog_tree_spec(model, scan_path="", verbose=False)

    assert specs[0].children is not None
    assert [c.label for c in specs[0].children] == ["Catalog A", "Catalog B"]
    assert [c.key for c in specs[0].children] == [_catalog_key_of(handle, cat_a), _catalog_key_of(handle, cat_b)]
    assert [c.payload for c in specs[0].children] == [cat_a, cat_b]
    assert all(c.allow_expand is False for c in specs[0].children)


def test_catalog_tree_spec_preserves_discovery_order_across_repos() -> None:
    first, second = RepoHandle(1), RepoHandle(2)
    model = BrowseModel(
        repos={
            first: RepoState(layout=_layout("first"), key_status=KeyStatus.NOT_ENCRYPTED),
            second: RepoState(layout=_layout("second"), key_status=KeyStatus.NOT_ENCRYPTED),
        }
    )
    specs = catalog_tree_spec(model, scan_path="", verbose=False)
    assert [s.key for s in specs] == [first, second]


def test_catalog_tree_spec_filter_narrows_only_the_targeted_repos_catalogs() -> None:
    repo_a, repo_b = RepoHandle(1), RepoHandle(2)
    cat_apple, cat_banana = _catalog("apple", "Apple"), _catalog("banana", "Banana")
    cat_other = _catalog("x", "Apple")
    model = BrowseModel(
        repos={
            repo_a: RepoState(
                layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED, catalogs=Success((cat_apple, cat_banana))
            ),
            repo_b: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED, catalogs=Success((cat_other,))),
        },
        tree_filter=TreeFilterState(tree="catalogs", parent_key=repo_a, text="apple"),
    )
    specs = catalog_tree_spec(model, scan_path="", verbose=False)

    by_key = {s.key: s for s in specs}
    repo_a_children = by_key[repo_a].children
    repo_b_children = by_key[repo_b].children
    assert repo_a_children is not None
    assert [c.label for c in repo_a_children] == ["Apple"]
    # repo_b's own catalogs are untouched by a filter scoped to repo_a.
    assert repo_b_children is not None
    assert [c.label for c in repo_b_children] == ["Apple"]


def test_catalog_tree_spec_verbose_flag_reaches_the_repo_and_catalog_labels() -> None:
    handle = RepoHandle(1)
    catalog = _real_catalog("a", "Catalog A")
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED, catalogs=Success((catalog,)))}
    )
    plain = catalog_tree_spec(model, scan_path="", verbose=False)
    verbose = catalog_tree_spec(model, scan_path="", verbose=True)

    assert plain[0].label != verbose[0].label  # repo label gains a "(layout: ...)" suffix
    assert plain[0].children is not None and verbose[0].children is not None
    assert plain[0].children[0].label != verbose[0].children[0].label  # catalog label gains a uuid/id suffix


# -- workload_tree_spec (column 2) ----------------------------------------


def _selected(repo: RepoHandle, catalog: Catalog) -> SelectedCatalog:
    return SelectedCatalog(repo=repo, catalog=catalog)


def test_workload_tree_spec_a_reload_failure_renders_a_single_error_leaf_and_ignores_selection() -> None:
    model = BrowseModel(
        selected_catalog=_selected(RepoHandle(1), _catalog("a", "A")), reload_failure="sibling [/] gone"
    )
    specs = workload_tree_spec(model, verbose=False)

    assert len(specs) == 1
    assert specs[0].key == WORKLOAD_ERROR_KEY
    assert specs[0].label == r"error: sibling \[/] gone"


def test_workload_tree_spec_is_empty_with_no_catalog_selected() -> None:
    assert workload_tree_spec(BrowseModel(), verbose=False) == ()


def test_workload_tree_spec_is_empty_while_the_catalogs_own_workloads_are_not_yet_loaded() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    model = BrowseModel(selected_catalog=_selected(handle, catalog))
    assert workload_tree_spec(model, verbose=False) == ()

    key = _catalog_key_of(handle, catalog)
    loading_model = BrowseModel(selected_catalog=_selected(handle, catalog), catalog_workloads={key: Loading()})
    assert workload_tree_spec(loading_model, verbose=False) == ()


def test_workload_tree_spec_a_refreshing_loading_state_renders_the_stale_previous_workloads() -> None:
    """``RefreshRequested`` carries the last ``Success`` forward as
    ``Loading.previous``, rendered here exactly like a real ``Success`` so
    column 2 stays populated, stale-but-real, instead of blanking to
    empty while the refetch is in flight."""
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    key = _catalog_key_of(handle, catalog)
    vm = _device_workload(1, "VM-1", type_hint="VM")
    model = BrowseModel(selected_catalog=_selected(handle, catalog), catalog_workloads={key: Loading(previous=(vm,))})
    specs = workload_tree_spec(model, verbose=False)

    assert [s.key for s in specs] == [WorkloadGroupKey(path=("VM",))]
    assert specs[0].children is not None
    assert [c.label for c in specs[0].children] == ["VM-1"]


def test_workload_tree_spec_a_failed_workloads_fetch_renders_a_single_error_leaf() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    key = _catalog_key_of(handle, catalog)
    model = BrowseModel(
        selected_catalog=_selected(handle, catalog), catalog_workloads={key: FailureInfo(message="boom [/] bang")}
    )
    specs = workload_tree_spec(model, verbose=False)

    assert len(specs) == 1
    assert specs[0].key == WORKLOAD_ERROR_KEY
    assert specs[0].label == r"error: boom \[/] bang"


def test_workload_tree_spec_groups_device_workloads_by_type_hint() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    key = _catalog_key_of(handle, catalog)
    vm = _device_workload(1, "VM-1", type_hint="VM")
    fs = _device_workload(2, "FS-1", type_hint="FS")
    model = BrowseModel(selected_catalog=_selected(handle, catalog), catalog_workloads={key: Success((vm, fs))})
    specs = workload_tree_spec(model, verbose=False)

    assert [s.key for s in specs] == [WorkloadGroupKey(path=("VM",)), WorkloadGroupKey(path=("FS",))]
    vm_group = specs[0]
    assert vm_group.allow_expand is True
    assert vm_group.children is not None
    assert [c.label for c in vm_group.children] == ["VM-1"]
    assert vm_group.children[0].key == _workload_key_of(key, vm)
    assert vm_group.children[0].payload is vm


def test_workload_tree_spec_escapes_a_display_name_shaped_like_rich_markup() -> None:
    """``workload.display_name`` is real, backup-derived content -- both
    ``Tree`` and ``DataTable`` re-parse a plain ``str`` label/cell as
    Rich markup, so it must be escaped before reaching this leaf's own
    label: an unescaped bracket can otherwise crash the widget outright
    with a markup error."""
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    key = _catalog_key_of(handle, catalog)
    vm = _device_workload(1, "a[/]b", type_hint="VM")
    model = BrowseModel(selected_catalog=_selected(handle, catalog), catalog_workloads={key: Success((vm,))})
    specs = workload_tree_spec(model, verbose=False)

    assert specs[0].children is not None
    assert [c.label for c in specs[0].children] == [r"a\[/]b"]


def test_workload_tree_spec_nests_saas_workloads_platform_tenant_subtype() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    key = _catalog_key_of(handle, catalog)
    mail = _saas_workload(1, "alice@corp.com", workload_type="M365", sub_type="USER_EXCHANGE", tenant_id="tenant-x")
    model = BrowseModel(selected_catalog=_selected(handle, catalog), catalog_workloads={key: Success((mail,))})
    specs = workload_tree_spec(model, verbose=False)

    assert len(specs) == 1
    platform_spec = specs[0]
    assert platform_spec.key == WorkloadGroupKey(path=("M365",))
    assert platform_spec.label == "Microsoft 365"
    assert platform_spec.children is not None and len(platform_spec.children) == 1

    tenant_spec = platform_spec.children[0]
    assert tenant_spec.key == WorkloadGroupKey(path=("M365", "tenant-x"))
    assert tenant_spec.label == "tenant-x"
    assert tenant_spec.children is not None and len(tenant_spec.children) == 1

    sub_type_spec = tenant_spec.children[0]
    assert sub_type_spec.key == WorkloadGroupKey(path=("M365", "tenant-x", "USER_EXCHANGE"))
    assert sub_type_spec.label == "Exchange"
    assert sub_type_spec.children is not None
    assert sub_type_spec.children[0].payload is mail


def test_workload_tree_spec_escapes_a_tenant_key_shaped_like_rich_markup() -> None:
    """``tenant_key`` is a real M365 tenant GUID or GW domain name
    (``_saas_group_key``) -- escaped before reaching this Tree label,
    same reasoning as the display-name test above."""
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    key = _catalog_key_of(handle, catalog)
    mail = _saas_workload(1, "alice", workload_type="M365", sub_type="USER_EXCHANGE", tenant_id="a[/]b")
    model = BrowseModel(selected_catalog=_selected(handle, catalog), catalog_workloads={key: Success((mail,))})
    specs = workload_tree_spec(model, verbose=False)

    assert specs[0].children is not None
    tenant_spec = specs[0].children[0]
    assert tenant_spec.label == r"a\[/]b"
    assert tenant_spec.key == WorkloadGroupKey(path=("M365", "a[/]b"))  # the raw key itself stays unescaped


def test_workload_tree_spec_filter_narrows_only_the_targeted_groups_leaves() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    key = _catalog_key_of(handle, catalog)
    apple = _device_workload(1, "Apple")
    banana = _device_workload(2, "Banana")
    group_key = WorkloadGroupKey(path=("VM",))
    model = BrowseModel(
        selected_catalog=_selected(handle, catalog),
        catalog_workloads={key: Success((apple, banana))},
        tree_filter=TreeFilterState(tree="workloads", parent_key=group_key, text="apple"),
    )
    specs = workload_tree_spec(model, verbose=False)

    assert specs[0].children is not None
    assert [c.label for c in specs[0].children] == ["Apple"]


def test_workload_tree_spec_filter_scoped_to_a_different_group_does_not_narrow_this_one() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    key = _catalog_key_of(handle, catalog)
    apple = _device_workload(1, "Apple")
    model = BrowseModel(
        selected_catalog=_selected(handle, catalog),
        catalog_workloads={key: Success((apple,))},
        tree_filter=TreeFilterState(
            tree="workloads", parent_key=WorkloadGroupKey(path=("SOMETHING_ELSE",)), text="zzz"
        ),
    )
    specs = workload_tree_spec(model, verbose=False)

    assert specs[0].children is not None
    assert [c.label for c in specs[0].children] == ["Apple"]


# -- version_rows / version_load_error (column 3) -------------------------


def test_version_rows_is_empty_with_nothing_selected() -> None:
    assert version_rows(BrowseModel()) == ()
    assert version_load_error(BrowseModel()) is None


def test_version_rows_is_empty_while_still_loading_or_not_asked() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    workload = _device_workload(1, "W")
    key = _workload_key_of(_catalog_key_of(handle, catalog), workload)
    model = BrowseModel(
        selected_catalog=_selected(handle, catalog), selected_workload=workload, workload_versions={key: Loading()}
    )
    assert version_rows(model) == ()
    assert version_load_error(model) is None


def test_version_rows_a_refreshing_loading_state_renders_the_stale_previous_versions() -> None:
    """Same stale-while-revalidate rendering as
    ``test_workload_tree_spec_a_refreshing_loading_state_renders_the_stale_previous_workloads``,
    for column 3."""
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    workload = _device_workload(1, "W")
    key = _workload_key_of(_catalog_key_of(handle, catalog), workload)
    v1 = _version("v1", "2026-01-01 00:00")
    model = BrowseModel(
        selected_catalog=_selected(handle, catalog),
        selected_workload=workload,
        workload_versions={key: Loading(previous=(v1,))},
    )

    assert version_rows(model) == ((0, "2026-01-01 00:00"),)
    assert version_load_error(model) is None


def test_version_rows_returns_disambiguated_index_name_pairs_on_success() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    workload = _device_workload(1, "W")
    key = _workload_key_of(_catalog_key_of(handle, catalog), workload)
    v1 = _version("v1", "2026-01-01 00:00")
    v2 = _version("v2", "2026-01-02 00:00")
    model = BrowseModel(
        selected_catalog=_selected(handle, catalog),
        selected_workload=workload,
        workload_versions={key: Success((v1, v2))},
    )
    rows = version_rows(model)

    assert rows == ((0, "2026-01-01 00:00"), (1, "2026-01-02 00:00"))
    assert version_load_error(model) is None


def test_version_load_error_returns_the_failure_message_and_no_rows() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    workload = _device_workload(1, "W")
    key = _workload_key_of(_catalog_key_of(handle, catalog), workload)
    model = BrowseModel(
        selected_catalog=_selected(handle, catalog),
        selected_workload=workload,
        workload_versions={key: FailureInfo(message="versions boom")},
    )

    assert version_rows(model) == ()
    assert version_load_error(model) == "versions boom"


def test_version_fetch_pending_is_false_with_nothing_selected() -> None:
    assert version_fetch_pending(BrowseModel()) is False


def test_version_fetch_pending_is_true_before_a_first_fetch_has_resolved() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    workload = _device_workload(1, "W")
    key = _workload_key_of(_catalog_key_of(handle, catalog), workload)
    model = BrowseModel(
        selected_catalog=_selected(handle, catalog), selected_workload=workload, workload_versions={key: Loading()}
    )
    assert version_fetch_pending(model) is True


def test_version_fetch_pending_is_false_once_resolved_including_a_failure() -> None:
    """A failed fetch has still resolved -- ``version_fetch_pending`` only
    tracks whether the first resolution has happened yet, not whether it
    succeeded."""
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    workload = _device_workload(1, "W")
    key = _workload_key_of(_catalog_key_of(handle, catalog), workload)
    v1 = _version("v1", "2026-01-01 00:00")

    success_model = BrowseModel(
        selected_catalog=_selected(handle, catalog),
        selected_workload=workload,
        workload_versions={key: Success((v1,))},
    )
    stale_loading_model = BrowseModel(
        selected_catalog=_selected(handle, catalog),
        selected_workload=workload,
        workload_versions={key: Loading(previous=(v1,))},
    )
    failure_model = BrowseModel(
        selected_catalog=_selected(handle, catalog),
        selected_workload=workload,
        workload_versions={key: FailureInfo(message="versions boom")},
    )

    assert version_fetch_pending(success_model) is False
    assert version_fetch_pending(stale_loading_model) is False
    assert version_fetch_pending(failure_model) is False


def test_is_workload_current_is_false_with_nothing_selected() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    workload = _device_workload(1, "W")
    key = _workload_key_of(_catalog_key_of(handle, catalog), workload)
    assert is_workload_current(BrowseModel(), key) is False


def test_is_workload_current_is_false_for_a_different_workload() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    selected = _device_workload(1, "Selected")
    other = _device_workload(2, "Other")
    other_key = _workload_key_of(_catalog_key_of(handle, catalog), other)
    model = BrowseModel(selected_catalog=_selected(handle, catalog), selected_workload=selected)
    assert is_workload_current(model, other_key) is False


def test_is_workload_current_is_true_for_the_selected_workload() -> None:
    handle, catalog = RepoHandle(1), _catalog("a", "A")
    workload = _device_workload(1, "W")
    key = _workload_key_of(_catalog_key_of(handle, catalog), workload)
    model = BrowseModel(selected_catalog=_selected(handle, catalog), selected_workload=workload)
    assert is_workload_current(model, key) is True


__all__: list[str] = []
