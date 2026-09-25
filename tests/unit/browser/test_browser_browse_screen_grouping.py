"""Unit tests for ``browser.workload_grouping``'s ``_group_workloads`` —
the pure grouping algorithm behind column 2's
tree, extracted specifically so it's testable without a running Textual
app/real ``Tree`` widget (``BrowseScreen._set_workloads`` is just the
thin rendering layer on top of this). Real end-to-end tree-rendering
coverage (headers, expand-to-first-match, cursor) lives in
``tests/integration/browser/test_browser_pilot.py`` against real
recorded sample data."""

from __future__ import annotations

from synology_apm_repo.browser.workload_grouping import _UNKNOWN_TENANT_LABEL, _group_workloads
from synology_apm_repo.sdk.api import Workload
from synology_apm_repo.sdk.identifiers import WorkloadId, WorkloadUid

_next_id = iter(range(1, 10_000))


def _workload(
    *,
    workload_type: str,
    sub_type: str | None = None,
    tenant_id: str | None = None,
    domain: str | None = None,
    name: str | None = None,
) -> Workload:
    """A minimal real ``Workload`` — ``tenant_id``/``domain`` are
    properties derived from ``spec``, so building the underlying dict
    directly (matching test_catalog_workload.py's own construction
    pattern) exercises the exact same real derivation
    ``_group_workloads``/``_saas_group_key`` read from, not a shortcut
    fake."""
    n = next(_next_id)
    spec: dict[str, object] = {"spec": {}}
    if tenant_id is not None:
        spec["spec"]["tenant_id"] = tenant_id  # type: ignore[index]
    if domain is not None:
        spec["spec"]["domain"] = domain  # type: ignore[index]
    return Workload(
        workload_id=WorkloadId(n),
        workload_uid=WorkloadUid(f"uid-{n}"),
        workload_type=workload_type,
        sub_type=sub_type,
        display_name=name or f"workload-{n}",
        subtitle=None,
        spec=spec,
    )


def test_device_workloads_group_by_type_hint_flat() -> None:
    vm = _workload(workload_type="VM")
    fs = _workload(workload_type="FS")
    result = _group_workloads([vm, fs])
    assert result.device_groups == {"VM": [vm], "FS": [fs]}
    assert result.saas_groups == {}


def test_multiple_device_workloads_of_the_same_type_share_one_group() -> None:
    vm1 = _workload(workload_type="VM")
    vm2 = _workload(workload_type="VM")
    result = _group_workloads([vm1, vm2])
    assert result.device_groups == {"VM": [vm1, vm2]}


def test_empty_workload_list_yields_empty_groupings() -> None:
    result = _group_workloads([])
    assert result.device_groups == {}
    assert result.saas_groups == {}


def test_a_single_m365_workload_gets_its_own_platform_tenant_subtype_chain() -> None:
    wl = _workload(workload_type="M365", sub_type="USER_EXCHANGE", tenant_id="tenant-a")
    result = _group_workloads([wl])
    assert result.device_groups == {}
    assert result.saas_groups == {"M365": {"tenant-a": {"USER_EXCHANGE": [wl]}}}


def test_two_m365_tenants_form_two_separate_tenant_groups() -> None:
    wl_a = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    wl_b = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-b")
    result = _group_workloads([wl_a, wl_b])
    assert set(result.saas_groups["M365"].keys()) == {"tenant-a", "tenant-b"}
    assert result.saas_groups["M365"]["tenant-a"] == {"MAIL": [wl_a]}
    assert result.saas_groups["M365"]["tenant-b"] == {"MAIL": [wl_b]}


def test_one_tenant_with_multiple_sub_types_groups_each_separately() -> None:
    mail = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    drive = _workload(workload_type="M365", sub_type="USER_DRIVE", tenant_id="tenant-a")
    result = _group_workloads([mail, drive])
    assert result.saas_groups["M365"]["tenant-a"] == {"MAIL": [mail], "USER_DRIVE": [drive]}


def test_gw_workload_groups_by_domain_not_tenant_id() -> None:
    wl = _workload(workload_type="GW", sub_type="DRIVE", domain="example.com")
    result = _group_workloads([wl])
    assert result.saas_groups == {"GW": {"example.com": {"DRIVE": [wl]}}}


def test_m365_workload_missing_a_real_tenant_id_falls_back_to_the_unknown_label() -> None:
    wl = _workload(workload_type="M365", sub_type="MAIL", tenant_id=None)
    result = _group_workloads([wl])
    assert result.saas_groups == {"M365": {_UNKNOWN_TENANT_LABEL: {"MAIL": [wl]}}}


def test_platform_order_is_always_m365_then_gw_regardless_of_input_order() -> None:
    """``_SAAS_PLATFORM_TYPES`` fixes the top-level display order — GW
    listed *before* M365 in the input must not change the grouping's own
    key order, since a caller renders ``saas_groups.items()`` in dict
    insertion order."""
    gw = _workload(workload_type="GW", sub_type="DRIVE", domain="example.com")
    m365 = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    result = _group_workloads([gw, m365])
    assert list(result.saas_groups.keys()) == ["M365", "GW"]


def test_a_platform_with_no_workloads_present_is_simply_absent_not_an_empty_group() -> None:
    m365 = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    result = _group_workloads([m365])
    assert "GW" not in result.saas_groups


def test_mixed_device_and_saas_workloads_populate_both_independently() -> None:
    vm = _workload(workload_type="VM")
    m365 = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    result = _group_workloads([vm, m365])
    assert result.device_groups == {"VM": [vm]}
    assert result.saas_groups == {"M365": {"tenant-a": {"MAIL": [m365]}}}


def test_group_membership_preserves_first_seen_order_within_a_group() -> None:
    first = _workload(workload_type="VM", name="first")
    second = _workload(workload_type="VM", name="second")
    third = _workload(workload_type="VM", name="third")
    result = _group_workloads([first, second, third])
    assert result.device_groups["VM"] == [first, second, third]


__all__: list[str] = []
