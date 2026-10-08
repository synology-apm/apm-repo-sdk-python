"""Unit tests for ``browser.core.keys``: every type here is a ``dict``/``set``
key, so these check equality and hashing."""

from __future__ import annotations

import pytest

from synology_apm_repo.browser.core.keys import (
    CatalogKey,
    Epoch,
    JobId,
    ProviderHandle,
    RepoHandle,
    RequestId,
    Slot,
    WorkloadKey,
    is_stale,
)
from synology_apm_repo.sdk.identifiers import CatalogId, WorkloadUid


def test_a_repo_handle_is_a_hashable_value_of_its_own_type() -> None:
    # Its own type, so BrowseScreen can tell a repository node's payload
    # apart from any other payload with isinstance.
    assert RepoHandle(3) == RepoHandle(3)
    assert {RepoHandle(3): "repo"}[RepoHandle(3)] == "repo"
    assert not isinstance(RepoHandle(3), int)


def test_catalog_key_disambiguates_same_catalog_id_across_repos() -> None:
    """A vault's CatalogId can fall back to ``str(connection_config_id)``,
    which two repositories can share."""
    same_id = CatalogId("1")
    key_a = CatalogKey(repo=RepoHandle(1), catalog_id=same_id)
    key_b = CatalogKey(repo=RepoHandle(2), catalog_id=same_id)
    assert key_a != key_b
    assert len({key_a, key_b}) == 2


def test_catalog_key_equal_fields_compare_equal_and_hash_equal() -> None:
    key_a = CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1"))
    key_b = CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1"))
    assert key_a == key_b
    assert hash(key_a) == hash(key_b)


def test_workload_key_nests_catalog_key() -> None:
    catalog = CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1"))
    other_catalog = CatalogKey(repo=RepoHandle(2), catalog_id=CatalogId("cat-1"))
    same_uid = WorkloadUid("wl-1")
    key_a = WorkloadKey(catalog=catalog, workload_uid=same_uid)
    key_b = WorkloadKey(catalog=other_catalog, workload_uid=same_uid)
    assert key_a != key_b
    assert len({key_a, key_b}) == 2


def test_slot_defaults_to_no_key() -> None:
    slot = Slot(kind="provider")
    assert slot.key is None


def test_slot_kind_and_key_together_determine_identity() -> None:
    catalog = CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1"))
    slot_a = Slot(kind="workloads", key=catalog)
    slot_b = Slot(kind="workloads", key=catalog)
    slot_c = Slot(kind="versions", key=catalog)
    assert slot_a == slot_b
    assert slot_a != slot_c
    assert len({slot_a, slot_b, slot_c}) == 2


def test_request_id_and_epoch_are_plain_ints_at_runtime() -> None:
    assert RequestId(1) + 1 == 2
    assert Epoch(0) == 0


def test_job_id_is_a_plain_int_at_runtime() -> None:
    assert JobId(1) + 1 == 2
    assert JobId(1) in {1}  # noqa: FURB171 - checks hashing, not equality


@pytest.mark.parametrize(
    ("current_epoch", "inflight_request", "expected"),
    [
        pytest.param(1, 3, False, id="false_when_epoch_and_request_both_match"),
        pytest.param(2, 3, True, id="true_on_an_epoch_mismatch_even_with_the_right_request"),
        # A newer dispatch already superseded request 3.
        pytest.param(1, 4, True, id="true_on_a_request_mismatch_within_the_same_epoch"),
    ],
)
def test_is_stale(current_epoch: int, inflight_request: int, expected: bool) -> None:
    slot = Slot(kind="provider")
    inflight = {slot: RequestId(inflight_request)}
    assert is_stale(Epoch(current_epoch), inflight, slot, Epoch(1), RequestId(3)) is expected


def test_is_stale_is_true_when_the_slot_has_nothing_in_flight_at_all() -> None:
    slot = Slot(kind="provider")
    assert is_stale(Epoch(1), {}, slot, Epoch(1), RequestId(3)) is True


def test_a_provider_handle_never_equals_a_repo_handle_with_the_same_id() -> None:
    assert RepoHandle(1) != ProviderHandle(1)  # type: ignore[comparison-overlap]
