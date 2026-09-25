"""Unit tests for ``browser.core.keys`` — every type here exists to be a
correct, hashable ``dict``/``set`` key, so that's what these tests check:
equality/hashing behavior, and specifically that ``CatalogKey``'s
``repo`` field actually disambiguates two independently-opened vault
repositories that can end up sharing the same ``CatalogId``."""

from __future__ import annotations

from synology_apm_repo.browser.core.keys import (
    CatalogKey,
    Epoch,
    JobId,
    ProviderHandle,
    RepoHandle,
    RequestId,
    Slot,
    VersionKey,
    WorkloadKey,
    is_stale,
)
from synology_apm_repo.sdk.identifiers import CatalogId, VersionUid, WorkloadUid


def test_repo_handle_is_a_plain_int_at_runtime() -> None:
    # NewType is erased at runtime -- confirms nothing here accidentally
    # wraps the value in a real class that would break `==`/hashing
    # against a plain int (e.g. a dict keyed by RepoHandle looked up
    # with a bare int, which happens naturally once a handle is stored
    # and re-read from JSON/logging/a test fixture).
    handle = RepoHandle(3)
    assert handle == 3
    assert hash(handle) == hash(3)


def test_catalog_key_disambiguates_same_catalog_id_across_repos() -> None:
    """The exact collision this key exists to make unrepresentable:
    a vault's own CatalogId degrades to str(connection_config_id), which
    two independently-opened repositories can legitimately share."""
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
    assert key_a != key_b  # same workload_uid, different owning catalog
    assert len({key_a, key_b}) == 2


def test_version_key_nests_workload_key() -> None:
    catalog = CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1"))
    workload = WorkloadKey(catalog=catalog, workload_uid=WorkloadUid("wl-1"))
    key_a = VersionKey(workload=workload, version_uid=VersionUid("ver-1"))
    key_b = VersionKey(workload=workload, version_uid=VersionUid("ver-1"))
    key_c = VersionKey(workload=workload, version_uid=VersionUid("ver-2"))
    assert key_a == key_b
    assert key_a != key_c
    assert len({key_a, key_b, key_c}) == 2


def test_slot_defaults_to_no_key() -> None:
    """A slot with only one instance in flight at a time (e.g. "the
    provider currently loading") never needs a domain key."""
    slot = Slot(kind="provider")
    assert slot.key is None


def test_slot_kind_and_key_together_determine_identity() -> None:
    catalog = CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1"))
    slot_a = Slot(kind="workloads", key=catalog)
    slot_b = Slot(kind="workloads", key=catalog)
    slot_c = Slot(kind="versions", key=catalog)  # same key, different kind
    assert slot_a == slot_b
    assert slot_a != slot_c
    assert len({slot_a, slot_b, slot_c}) == 2


def test_request_id_and_epoch_are_plain_ints_at_runtime() -> None:
    assert RequestId(1) + 1 == 2  # NewType erases to int at runtime -- ordinary arithmetic just works
    assert Epoch(0) == 0


def test_job_id_is_a_plain_int_at_runtime() -> None:
    assert JobId(1) + 1 == 2
    assert JobId(1) in {1}


def test_is_stale_is_false_when_epoch_and_request_both_match() -> None:
    slot = Slot(kind="provider")
    inflight = {slot: RequestId(3)}
    assert is_stale(Epoch(1), inflight, slot, Epoch(1), RequestId(3)) is False


def test_is_stale_is_true_on_an_epoch_mismatch_even_with_the_right_request() -> None:
    slot = Slot(kind="provider")
    inflight = {slot: RequestId(3)}
    assert is_stale(Epoch(2), inflight, slot, Epoch(1), RequestId(3)) is True


def test_is_stale_is_true_on_a_request_mismatch_within_the_same_epoch() -> None:
    slot = Slot(kind="provider")
    inflight = {slot: RequestId(4)}  # a newer dispatch already superseded request 3
    assert is_stale(Epoch(1), inflight, slot, Epoch(1), RequestId(3)) is True


def test_is_stale_is_true_when_the_slot_has_nothing_in_flight_at_all() -> None:
    slot = Slot(kind="provider")
    assert is_stale(Epoch(1), {}, slot, Epoch(1), RequestId(3)) is True


def test_provider_handle_is_distinct_from_repo_handle_only_to_mypy() -> None:
    # At runtime both are plain ints -- the same integer value from each
    # constructor is indistinguishable; the whole point of NewType is a
    # *static* guard (mypy rejects passing a RepoHandle where a
    # ProviderHandle is expected), not a runtime one. Compared against a
    # plain int rather than against each other -- mypy (rightly) refuses
    # to compare two distinct NewTypes at all, which is the guard this
    # test is confirming doesn't also apply at runtime.
    assert RepoHandle(1) == 1
    assert ProviderHandle(1) == 1
