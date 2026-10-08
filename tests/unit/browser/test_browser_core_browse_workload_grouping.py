"""Unit tests for ``browser.core.browse.workload_grouping``'s ``group_workloads``,
the grouping behind column 2's tree."""

from __future__ import annotations

from support.model_factories import make_workload
from synology_apm_repo.browser.core.browse.workload_grouping import _UNKNOWN_TENANT_LABEL, group_workloads
from synology_apm_repo.sdk.api import Workload

_next_id = iter(range(1, 10_000))


def _workload(
    *,
    workload_type: str,
    sub_type: str | None = None,
    tenant_id: str | None = None,
    domain: str | None = None,
    name: str | None = None,
) -> Workload:
    """A ``Workload`` whose ``tenant_id``/``domain`` come from ``spec``."""
    n = next(_next_id)
    spec: dict[str, object] = {"spec": {}}
    if tenant_id is not None:
        spec["spec"]["tenant_id"] = tenant_id  # type: ignore[index]
    if domain is not None:
        spec["spec"]["domain"] = domain  # type: ignore[index]
    return make_workload(
        workload_id=n,
        workload_uid=f"uid-{n}",
        workload_type=workload_type,
        sub_type=sub_type,
        display_name=name or f"workload-{n}",
        spec=spec,
    )


def test_device_workloads_group_by_type_hint_flat() -> None:
    vm = _workload(workload_type="VM")
    fs = _workload(workload_type="FS")
    result = group_workloads([vm, fs])
    assert result.device_groups == {"VM": [vm], "FS": [fs]}
    assert result.saas_groups == {}


def test_multiple_device_workloads_of_the_same_type_share_one_group() -> None:
    vm1 = _workload(workload_type="VM")
    vm2 = _workload(workload_type="VM")
    result = group_workloads([vm1, vm2])
    assert result.device_groups == {"VM": [vm1, vm2]}


def test_empty_workload_list_yields_empty_groupings() -> None:
    result = group_workloads([])
    assert result.device_groups == {}
    assert result.saas_groups == {}


def test_a_single_m365_workload_gets_its_own_platform_tenant_subtype_chain() -> None:
    wl = _workload(workload_type="M365", sub_type="USER_EXCHANGE", tenant_id="tenant-a")
    result = group_workloads([wl])
    assert result.device_groups == {}
    assert result.saas_groups == {"M365": {"tenant-a": {"USER_EXCHANGE": [wl]}}}


def test_two_m365_tenants_form_two_separate_tenant_groups() -> None:
    wl_a = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    wl_b = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-b")
    result = group_workloads([wl_a, wl_b])
    assert set(result.saas_groups["M365"].keys()) == {"tenant-a", "tenant-b"}
    assert result.saas_groups["M365"]["tenant-a"] == {"MAIL": [wl_a]}
    assert result.saas_groups["M365"]["tenant-b"] == {"MAIL": [wl_b]}


def test_one_tenant_with_multiple_sub_types_groups_each_separately() -> None:
    mail = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    drive = _workload(workload_type="M365", sub_type="USER_DRIVE", tenant_id="tenant-a")
    result = group_workloads([mail, drive])
    assert result.saas_groups["M365"]["tenant-a"] == {"MAIL": [mail], "USER_DRIVE": [drive]}


def test_gws_workload_groups_by_domain_not_tenant_id() -> None:
    wl = _workload(workload_type="GW", sub_type="DRIVE", domain="example.com")
    result = group_workloads([wl])
    assert result.saas_groups == {"GW": {"example.com": {"DRIVE": [wl]}}}


def test_m365_workload_missing_a_real_tenant_id_falls_back_to_the_unknown_label() -> None:
    wl = _workload(workload_type="M365", sub_type="MAIL", tenant_id=None)
    result = group_workloads([wl])
    assert result.saas_groups == {"M365": {_UNKNOWN_TENANT_LABEL: {"MAIL": [wl]}}}


def test_platform_order_is_always_m365_then_gws_regardless_of_input_order() -> None:
    """``SAAS_PLATFORM_TYPES`` fixes the key order of ``saas_groups``."""
    gws = _workload(workload_type="GW", sub_type="DRIVE", domain="example.com")
    m365 = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    result = group_workloads([gws, m365])
    assert list(result.saas_groups.keys()) == ["M365", "GW"]


def test_a_platform_with_no_workloads_present_is_simply_absent_not_an_empty_group() -> None:
    m365 = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    result = group_workloads([m365])
    assert "GW" not in result.saas_groups


def test_mixed_device_and_saas_workloads_populate_both_independently() -> None:
    vm = _workload(workload_type="VM")
    m365 = _workload(workload_type="M365", sub_type="MAIL", tenant_id="tenant-a")
    result = group_workloads([vm, m365])
    assert result.device_groups == {"VM": [vm]}
    assert result.saas_groups == {"M365": {"tenant-a": {"MAIL": [m365]}}}


def test_group_membership_preserves_first_seen_order_within_a_group() -> None:
    first = _workload(workload_type="VM", name="first")
    second = _workload(workload_type="VM", name="second")
    third = _workload(workload_type="VM", name="third")
    result = group_workloads([first, second, third])
    assert result.device_groups["VM"] == [first, second, third]
