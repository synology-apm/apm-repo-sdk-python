"""Unit tests for ``api.Repository``: ``key_status``/``is_encrypted``/
``set_key``, ``workload_is_supported``, ``catalogs()``/``catalog_by_id()``'s
open and exception posture, ``verify()``'s key gate, ``file_map_tree``,
``invalidate_caches``/``cache_names``/``cache_stats``, ``release_provider``,
close cleanup, and ``locate()``/``resolve()`` on human, raw and canonical
refs. ``Session`` and
``Catalog`` are covered in ``test_api_session.py``/``test_api_catalog.py``.
Collaborators are faked at the module boundary (the names ``api.repository``/
``api.catalog`` imported); ``tests/integration/sdk/test_api.py`` covers the
real wiring.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, Self, cast

import pytest

from support.fakes import faithful_to
from support.model_factories import make_connection, make_version, make_workload
from synology_apm_repo.sdk import api
from synology_apm_repo.sdk._util.closing import leaf_exceptions
from synology_apm_repo.sdk.api import catalog as api_catalog
from synology_apm_repo.sdk.api import provider_registry
from synology_apm_repo.sdk.api import repository as api_repository
from synology_apm_repo.sdk.cachemanager import DEFAULT_LIMITS, CacheLimits
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import KeyMismatchError, KeyRequiredError, NotFoundError
from synology_apm_repo.sdk.findings import Finding, Stage, Symptom
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout
from synology_apm_repo.sdk.units.base import ClosableUnitProvider, Node, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef, RefKind
from unit.sdk.api_fakes import (
    VALID_B64_KEY,
    FakeDedupRepo,
    FakeRaisingDedupRepo,
    FakeStore,
    as_dedup_repo,
    as_object_store,
    async_returning,
    no_encryption_keys,
    patch_dedup_open,
    repo_catalog,
    repo_with_fake_dedup,
    some_key,
    vault_layout,
    vault_repository_layout,
)

# -- KeyStatus / key_status -------------------------------------------------


def test_key_status_no_key_provided_when_encrypted_status_still_unknown() -> None:
    # ``encrypted`` defaults to None ("never resolved"); Session never
    # leaves a real Repository in this state.
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    assert repo.key_status is api.KeyStatus.NO_KEY_PROVIDED


def test_key_status_no_key_provided_when_probe_confirmed_encrypted() -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    assert repo.key_status is api.KeyStatus.NO_KEY_PROVIDED


def test_key_status_not_encrypted_when_probe_confirmed_unencrypted_even_without_a_key() -> None:
    # NO_KEY_PROVIDED is reserved for a confirmed-encrypted (or unresolved)
    # repository, never a confirmed-unencrypted one.
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    assert repo.key_status is api.KeyStatus.NOT_ENCRYPTED


def test_key_status_not_encrypted_for_no_encryption_key_material() -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=no_encryption_keys(), key_verification=None
    )
    assert repo.key_status is api.KeyStatus.NOT_ENCRYPTED


def test_key_status_verified_when_verification_ok() -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=keys, key_verification=verification
    )
    assert repo.key_status is api.KeyStatus.VERIFIED
    assert repo.key_verification is verification


def test_key_status_invalid_when_verification_failed() -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=keys, key_verification=verification
    )
    assert repo.key_status is api.KeyStatus.INVALID


# -- is_encrypted -------------------------------------------------------------


def test_is_encrypted_none_when_never_resolved_and_no_key_given() -> None:
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    assert repo.is_encrypted is None


@pytest.mark.parametrize(
    "encrypted",
    [
        pytest.param(True, id="true_when_probe_confirmed_encrypted"),
        pytest.param(False, id="false_when_probe_confirmed_unencrypted"),
    ],
)
def test_is_encrypted(encrypted: bool) -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=encrypted
    )
    assert repo.is_encrypted is encrypted


def test_is_encrypted_true_once_a_real_key_is_given_regardless_of_verification_outcome() -> None:
    # is_encrypted reflects that a real key was given, not whether it is
    # correct (key_status/key_verification's job).
    keys = some_key("some-id-0000")
    ok_repo = api.Repository(
        as_object_store(FakeStore()),
        vault_repository_layout(),
        keys=keys,
        key_verification=KeyVerification(gcm_ok=True, vault_key=b"x" * 32),
    )
    assert ok_repo.is_encrypted is True

    bad_repo = api.Repository(
        as_object_store(FakeStore()),
        vault_repository_layout(),
        keys=keys,
        key_verification=KeyVerification(gcm_ok=False, vault_key=None),
    )
    assert bad_repo.is_encrypted is True


def test_is_encrypted_false_for_no_encryption_key_material() -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=no_encryption_keys(), key_verification=None
    )
    assert repo.is_encrypted is False


# -- set_key -----------------------------------------------------------------
#
# Each test resolves catalog 0 first, so set_key() has an already-opened
# catalog to swap.


async def test_set_key_wrong_key_reports_invalid_not_no_key_provided(monkeypatch: pytest.MonkeyPatch) -> None:
    """A rejected key reports INVALID, not NO_KEY_PROVIDED, and leaves the
    working connection open."""
    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None, connections=[])
    await repo._open_catalogs.resolve(0)

    bad_verification = KeyVerification(gcm_ok=False, vault_key=None)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(bad_verification))

    result = await repo.set_key("bad-id-00000@" + VALID_B64_KEY)

    assert result.verification is bad_verification
    assert result.reopen_errors == ()
    assert result.warning is None
    assert repo.key_status is api.KeyStatus.INVALID
    assert fake_repo.closed is False


async def test_set_key_correct_key_swaps_in_new_dedup_repo_and_closes_old(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None, connections=[])
    await repo._open_catalogs.resolve(0)

    good_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(good_verification))
    new_repo = FakeDedupRepo(vault_layout())
    patch_dedup_open(monkeypatch, lambda *a, **k: new_repo)

    result = await repo.set_key("good-id-0000@" + VALID_B64_KEY)

    assert result.verification is good_verification
    assert result.reopen_errors == ()
    assert result.warning is None
    assert repo.key_status is api.KeyStatus.VERIFIED
    assert fake_repo.closed is True
    assert (await repo._open_catalogs.resolve(0)).dedup_repo is as_dedup_repo(new_repo)


async def test_set_key_reports_a_failed_reopen_in_the_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """A catalog's reopen can fail even though the key verified; set_key()
    reports it in ``reopen_errors``/``warning`` rather than raising or
    swallowing it."""
    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None, connections=[])
    await repo._open_catalogs.resolve(0)

    good_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(good_verification))

    async def failing_open(*a: object, **k: object) -> DedupRepo:
        raise KeyMismatchError("synthetic reopen failure", ref="repo_info")

    patch_dedup_open(monkeypatch, failing_open)

    result = await repo.set_key("good-id-0000@" + VALID_B64_KEY)
    assert result.verification is good_verification
    assert len(result.reopen_errors) == 1
    assert isinstance(result.reopen_errors[0], KeyMismatchError)
    assert result.warning is not None
    assert "synthetic reopen failure" in result.warning
    assert "1 open catalog" in result.warning
    # The key was accepted, so key_status still reflects the verification.
    assert repo.key_status is api.KeyStatus.VERIFIED


async def test_set_key_still_attempts_every_close_when_an_earlier_reopen_succeeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The replaced ``DedupRepo``'s ``close()`` failing is reported in
    ``reopen_errors`` without abandoning the swap."""
    fake_repo = FakeRaisingDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None, connections=[])
    await repo._open_catalogs.resolve(0)

    good_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(good_verification))
    new_repo = FakeDedupRepo(vault_layout())
    patch_dedup_open(monkeypatch, lambda *a, **k: new_repo)

    result = await repo.set_key("good-id-0000@" + VALID_B64_KEY)
    assert len(result.reopen_errors) == 1
    assert isinstance(result.reopen_errors[0], RuntimeError)
    assert fake_repo.closed is True  # the failing close was still attempted, not skipped
    assert repo.key_status is api.KeyStatus.VERIFIED
    assert (await repo._open_catalogs.resolve(0)).dedup_repo is as_dedup_repo(new_repo)


async def test_set_key_then_correct_key_afterwards_recovers_to_verified(monkeypatch: pytest.MonkeyPatch) -> None:
    """wrong key -> INVALID, then a correct key afterwards -> VERIFIED."""
    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None, connections=[])
    await repo._open_catalogs.resolve(0)

    bad_verification = KeyVerification(gcm_ok=False, vault_key=None)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(bad_verification))
    await repo.set_key("bad-id-00000@" + VALID_B64_KEY)
    status_after_bad_key = repo.key_status
    assert status_after_bad_key is api.KeyStatus.INVALID

    good_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(good_verification))
    patch_dedup_open(monkeypatch, lambda *a, **k: FakeDedupRepo(vault_layout()))
    await repo.set_key("good-id-0000@" + VALID_B64_KEY)
    status_after_good_key = repo.key_status
    assert status_after_good_key is api.KeyStatus.VERIFIED


class TestWorkloadIsSupported:
    """``Repository.workload_is_supported`` delegates to
    ``units.dispatch.is_supported`` (covered by
    ``test_units_dispatch_saas.py``); this only confirms the facade exposes
    it."""

    def test_recognized_sub_type_is_supported(self) -> None:
        repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
        assert repo.workload_is_supported(make_workload(workload_type="M365", sub_type="MAIL")) is True

    def test_unrecognized_sub_type_is_not_supported(self) -> None:
        repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
        unrecognized = dataclasses.replace(
            make_workload(workload_type="M365", sub_type="MAIL"),
            workload_type="GW",
            sub_type="SOME_FUTURE_CONNECTOR_TYPE",
        )
        assert repo.workload_is_supported(unrecognized) is False


# -- catalogs() / Catalog -----------------------------------------------------
#
# Repository.catalogs() wraps every connections() row as a Catalog sharing
# the Repository's DedupRepo, and is never key-gated (connection_config rows
# are plaintext).


async def test_open_catalog_resources_uses_the_larger_interactive_bucket_cache_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``Repository._open_catalog_resources()`` passes limits whose
    ``bucket_readers`` is the larger ``bucket_readers_interactive``, since
    this ``Pool`` is shared by every non-bulk consumer of the catalog."""
    captured_kwargs: dict[str, object] = {}
    fake = FakeDedupRepo(vault_layout())

    def capturing_factory(*args: object, **kwargs: object) -> FakeDedupRepo:
        captured_kwargs.update(kwargs)
        return fake

    patch_dedup_open(monkeypatch, capturing_factory)
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), None, None)
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))

    await repo.catalogs()

    limits = captured_kwargs.get("limits")
    assert isinstance(limits, CacheLimits)
    assert limits.bucket_readers == DEFAULT_LIMITS.bucket_readers_interactive


@pytest.mark.parametrize(
    "failure", [RuntimeError("synthetic corrupt connection_config"), asyncio.CancelledError()], ids=["error", "cancel"]
)
async def test_open_catalog_resources_closes_the_dedup_repo_when_connections_fails(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    """``connections()`` raising or being cancelled after ``DedupRepo.open()``
    succeeded must not leak the opened repo."""
    fake = FakeDedupRepo(vault_layout())
    patch_dedup_open(monkeypatch, lambda *a, **k: fake)
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), None, None)

    async def failing_connections(dedup_repo: object) -> list[Connection]:
        raise failure

    monkeypatch.setattr(api_repository, "connections", failing_connections)

    with pytest.raises(type(failure), match=str(failure) or None):
        await repo.catalogs()

    assert fake.closed is True


async def test_catalogs_succeeds_even_when_key_required_and_not_yet_provided(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), encrypted=True, connections=[])
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))
    catalogs = await repo.catalogs()
    assert len(catalogs) == 1


async def test_catalogs_succeeds_even_when_a_previous_key_was_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = repo_with_fake_dedup(
        monkeypatch, FakeDedupRepo(vault_layout()), keys=keys, key_verification=verification, connections=[]
    )
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))
    catalogs = await repo.catalogs()
    assert len(catalogs) == 1


async def test_catalogs_returns_one_catalog_per_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), encrypted=False, connections=[])
    first = make_connection(display_name="Source 1")
    second = make_connection(connection_config_id=2, display_name="Source 2")
    monkeypatch.setattr(api_repository, "connections", async_returning([first, second]))

    catalogs = await repo.catalogs()

    assert [c.connection for c in catalogs] == [first, second]
    assert [c.display_name for c in catalogs] == ["Source 1", "Source 2"]


async def test_catalogs_share_the_owning_repositorys_dedup_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sibling Catalogs of one vault share its single ``DedupRepo``."""
    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, encrypted=False, connections=[])
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))

    (catalog,) = await repo.catalogs()

    assert catalog._dedup_repo is as_dedup_repo(fake_repo)


async def test_catalogs_raises_instead_of_silently_dropping_a_sibling_that_fails_to_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sibling failing to open surfaces, so a broken catalog isn't mistaken
    for an absent one."""
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=["good", "bad"])

    async def selective_open(store: object, repo_layout: RepoLayout, keys: object, **kwargs: object) -> DedupRepo:
        if repo_layout.repo_id == "bad":
            raise NotFoundError("synthetic corrupt repo_info", ref="repo_info")
        return as_dedup_repo(FakeDedupRepo(repo_layout))

    patch_dedup_open(monkeypatch, selective_open)
    repo = api.Repository(as_object_store(FakeStore()), layout, keys=None, key_verification=None, encrypted=False)
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))

    with pytest.raises(NotFoundError, match="synthetic corrupt repo_info"):
        await repo.catalogs()


async def test_catalogs_reraises_cancellederror(monkeypatch: pytest.MonkeyPatch) -> None:
    """``catalogs()`` re-raises ``CancelledError`` captured by
    ``gather(..., return_exceptions=True)`` instead of swallowing it."""
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=["a"])

    async def cancelled_open(store: object, repo_layout: RepoLayout, keys: object, **kwargs: object) -> DedupRepo:
        raise asyncio.CancelledError

    patch_dedup_open(monkeypatch, cancelled_open)
    repo = api.Repository(as_object_store(FakeStore()), layout, keys=None, key_verification=None, encrypted=False)

    with pytest.raises(asyncio.CancelledError):
        await repo.catalogs()


async def test_catalogs_reraises_a_non_apmrepoerror_too(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-``ApmRepoError`` failure is surfaced like any other sibling-open
    failure."""
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=["a"])

    async def failing_open(store: object, repo_layout: RepoLayout, keys: object, **kwargs: object) -> DedupRepo:
        raise RuntimeError("synthetic transient I/O error")

    patch_dedup_open(monkeypatch, failing_open)
    repo = api.Repository(as_object_store(FakeStore()), layout, keys=None, key_verification=None, encrypted=False)

    with pytest.raises(RuntimeError, match="synthetic transient I/O error"):
        await repo.catalogs()


async def test_catalog_by_id_never_opens_a_sibling_whose_own_repo_id_cannot_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``catalog_by_id`` skips a sibling by ``repo_id`` alone, with no I/O,
    unlike ``catalogs()``."""
    # "other" comes first so the loop must skip a mismatched sibling.
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=["other", "wanted"])
    opened: list[str | None] = []

    async def recording_open(store: object, repo_layout: RepoLayout, keys: object, **kwargs: object) -> DedupRepo:
        opened.append(repo_layout.repo_id)
        return as_dedup_repo(FakeDedupRepo(repo_layout))

    patch_dedup_open(monkeypatch, recording_open)
    repo = api.Repository(as_object_store(FakeStore()), layout, keys=None, key_verification=None, encrypted=False)
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))

    catalog = await repo.catalog_by_id(CatalogId("wanted"))

    assert catalog is not None
    assert catalog.catalog_id == "wanted"
    assert opened == ["wanted"]  # "other" never opened at all


async def test_catalog_by_id_raises_when_a_candidate_it_cannot_rule_out_fails_to_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the one entry ``catalog_by_id`` can't rule out by ``repo_id``
    fails to open, that failure is surfaced rather than reported as "not
    found"."""
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=None)

    async def failing_open(store: object, repo_layout: RepoLayout, keys: object, **kwargs: object) -> DedupRepo:
        raise NotFoundError("synthetic corrupt repo_info", ref="repo_info")

    patch_dedup_open(monkeypatch, failing_open)
    repo = api.Repository(as_object_store(FakeStore()), layout, keys=None, key_verification=None, encrypted=False)

    with pytest.raises(NotFoundError, match="synthetic corrupt repo_info"):
        await repo.catalog_by_id(CatalogId("anything"))


# -- verify()'s own key gate ---------------------------------------------
#
# Without this gate, verify() on an encrypted repository with no/wrong key
# would walk zero versions (the browsable-status filter drops every row whose
# version_spec it can't decrypt) and report a misleadingly clean result.


async def test_repository_verify_raises_key_required_when_no_key_provided() -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    with pytest.raises(KeyRequiredError, match="this repository is encrypted"):
        await repo.verify()


async def test_repository_verify_raises_key_mismatch_when_key_invalid() -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=keys, key_verification=verification
    )
    with pytest.raises(KeyMismatchError, match="the key previously supplied for this repository was rejected"):
        await repo.verify()


async def test_repository_verify_succeeds_when_not_encrypted(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unencrypted repository passes the gate and reaches the reachability
    walk once per opened ``DedupRepo``."""
    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, encrypted=False, connections=[])
    sentinel = [Finding(stage=Stage.REPO_INFO, symptom=Symptom.MISMATCH, path="p", detail="d")]
    calls: list[object] = []

    async def fake_verify_reachable(
        dedup_repo: object, level: object, *, progress: object = None, executor: object = None
    ) -> list[Finding]:
        calls.append(dedup_repo)
        return sentinel

    monkeypatch.setattr(api_repository, "verify_reachable", fake_verify_reachable)

    result = await repo.verify()

    assert result == sentinel
    assert calls == [fake_repo]


# -- file_map_tree -------------------------------------------------


async def test_file_map_tree_builds_a_file_map_tree_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), connections=[])
    sentinel = object()
    monkeypatch.setattr(api_repository, "FileMapTreeProvider", lambda r: sentinel)
    assert await repo.file_map_tree() is sentinel


def _register_counting_cache(fake: FakeDedupRepo, name: str, calls: list[str]) -> None:
    async def _invalidate() -> None:
        calls.append(name)

    fake.caches.register(name, _invalidate, dict, bounded_by="test")


async def test_invalidate_caches_with_no_names_invalidates_every_cache_of_every_opened_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDedupRepo(vault_layout())
    calls: list[str] = []
    _register_counting_cache(fake, "dir_scan", calls)
    _register_counting_cache(fake, "db_sources", calls)
    repo = repo_with_fake_dedup(monkeypatch, fake, connections=[])
    await repo._open_catalogs.resolve(0)  # only an already-opened catalog is touched

    await repo.invalidate_caches()

    assert calls == ["db_sources", "dir_scan"]  # last registered first


async def test_invalidate_caches_with_names_invalidates_only_those(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeDedupRepo(vault_layout())
    calls: list[str] = []
    _register_counting_cache(fake, "dir_scan", calls)
    _register_counting_cache(fake, "pool", calls)
    repo = repo_with_fake_dedup(monkeypatch, fake, connections=[])
    await repo._open_catalogs.resolve(0)

    await repo.invalidate_caches("pool")

    assert calls == ["pool"]


async def test_invalidate_caches_before_any_catalog_is_opened_does_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), connections=[])

    await repo.invalidate_caches("dir_scan")  # a valid name, but nothing is cached yet

    assert repo.cache_names() == []


async def test_invalidate_caches_rejects_an_unknown_name_before_dropping_anything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDedupRepo(vault_layout())
    calls: list[str] = []
    _register_counting_cache(fake, "dir_scan", calls)
    repo = repo_with_fake_dedup(monkeypatch, fake, connections=[])
    await repo._open_catalogs.resolve(0)

    with pytest.raises(KeyError, match="nope"):
        await repo.invalidate_caches("dir_scan", "nope")

    assert calls == []


async def test_invalidate_caches_rereads_the_connection_list_after_the_db_sources_are_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDedupRepo(vault_layout())
    _register_counting_cache(fake, "db_sources", [])
    repo = repo_with_fake_dedup(monkeypatch, fake, connections=[])
    first = await repo._open_catalogs.resolve(0)
    fresh: list[Connection] = [cast(Connection, object())]
    monkeypatch.setattr(api_repository, "connections", async_returning(fresh))

    await repo.invalidate_caches()

    refreshed = await repo._open_catalogs.resolve(0)
    assert refreshed.connections == fresh
    assert refreshed.dedup_repo is first.dedup_repo  # in place: the catalog's DedupRepo is kept
    assert refreshed.saas_streams is first.saas_streams


async def test_invalidate_caches_keeps_the_connection_list_when_db_sources_is_not_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDedupRepo(vault_layout())
    _register_counting_cache(fake, "db_sources", [])
    _register_counting_cache(fake, "pool", [])
    repo = repo_with_fake_dedup(monkeypatch, fake, connections=[])
    first = await repo._open_catalogs.resolve(0)
    monkeypatch.setattr(api_repository, "connections", async_returning([cast(Connection, object())]))

    await repo.invalidate_caches("pool")

    assert (await repo._open_catalogs.resolve(0)).connections == first.connections


async def test_invalidate_caches_reports_every_failure_after_attempting_all(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeDedupRepo(vault_layout())
    attempted: list[str] = []

    def _recording(name: str) -> Callable[[], Awaitable[None]]:
        async def _invalidate() -> None:
            attempted.append(name)

        return _invalidate

    async def _boom() -> None:
        attempted.append("boom")
        raise OSError("boom")

    fake.caches.register("before_boom", _recording("before_boom"), dict, bounded_by="test")
    fake.caches.register("boom", _boom, dict, bounded_by="test")
    fake.caches.register("after_boom", _recording("after_boom"), dict, bounded_by="test")
    repo = repo_with_fake_dedup(monkeypatch, fake, connections=[])
    await repo._open_catalogs.resolve(0)

    with pytest.raises(
        ExceptionGroup, match=r"Repository\.invalidate_caches\(\) failed for one or more resources"
    ) as exc_info:
        await repo.invalidate_caches()

    # Last registered first, and the ones after the failure still ran.
    assert attempted == ["after_boom", "boom", "before_boom"]
    assert [str(leaf) for leaf in leaf_exceptions(exc_info.value)] == ["boom"]


async def test_invalidate_caches_raises_on_a_closed_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), connections=[])
    await repo._close()

    with pytest.raises(RuntimeError, match="closed"):
        await repo.invalidate_caches()


async def test_cache_names_and_stats_cover_every_opened_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeDedupRepo(vault_layout())
    fake.caches.keyed("composition_records", maxsize=4)
    repo = repo_with_fake_dedup(monkeypatch, fake, connections=[])
    assert repo.cache_names() == []  # nothing opened yet

    await repo._open_catalogs.resolve(0)

    assert repo.cache_names() == ["composition_records", "saas_streams"]  # saas_streams: registered by Repository
    assert set(repo.cache_stats()) == {"0.composition_records", "0.saas_streams"}


async def test_release_provider_closes_it_and_stops_tracking_it(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), connections=[])
    provider = _FakeClosableProvider()
    repo._providers.track(provider)

    await repo.release_provider(provider)

    assert provider.closed is True
    assert not repo._providers._providers
    provider.closed = False
    await repo._close()  # nothing left to close a second time
    assert provider.closed is False


async def test_a_release_cancelled_mid_close_still_finishes_and_close_waits_for_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A release cancelled during shutdown (Textual cancels the worker
    running it) must not leave the provider half-closed: its close runs on,
    and the repository's close waits for it."""
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), connections=[])
    started, release = asyncio.Event(), asyncio.Event()

    @faithful_to(ClosableUnitProvider)
    class _SlowProvider(_FakeClosableProvider):
        async def close(self) -> None:
            started.set()
            await release.wait()
            await super().close()

    provider = _SlowProvider()
    repo._providers.track(provider)
    releasing = asyncio.ensure_future(repo.release_provider(provider))
    await started.wait()
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing
    assert provider.closed is False

    closing = asyncio.ensure_future(repo._close())
    await asyncio.sleep(0)
    assert not closing.done()  # waiting for the cancelled release's close
    release.set()
    await closing
    assert provider.closed is True


# -- Repository close ---------------------------------------------------------


async def test_repository_close_closes_underlying_dedup_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, connections=[])
    await repo._open_catalogs.resolve(0)  # only an already-opened catalog gets closed
    await repo._close()
    assert fake_repo.closed is True


@faithful_to(ClosableUnitProvider)
class _FakeClosableProvider:
    """A ``ClosableUnitProvider`` that only records ``close()``; no test here
    walks it."""

    def __init__(self) -> None:
        self.closed = False

    def root(self) -> Node:
        raise AssertionError("not needed for this test")

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        raise AssertionError("not needed for this test")

    async def unit(self, node: Node) -> RestorableUnit:
        raise AssertionError("not needed for this test")

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.close()


async def test_close_closes_every_tracked_closable_provider_not_just_the_last_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every provider handed out by ``Catalog.provider()``/``file_map_tree()``
    is closed, not just the last one tracked."""
    fake_dedup_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_dedup_repo, connections=[])
    catalog = repo_catalog(repo, (await repo._open_catalogs.resolve(0)).dedup_repo)
    first, second = _FakeClosableProvider(), _FakeClosableProvider()
    remaining = [first, second]

    async def fake_provider_for(r: object, v: object) -> object:
        return remaining.pop(0)

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    version_a = make_version(workload_id=1, target_type="VM")
    version_b = make_version(workload_id=2, target_type="VM")
    assert await catalog.provider(version_a) is first
    assert await catalog.provider(version_b) is second

    await repo._close()

    assert first.closed is True
    assert second.closed is True
    assert fake_dedup_repo.closed is True


async def test_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second ``close()`` is a no-op."""
    fake_dedup_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_dedup_repo, connections=[])
    catalog = repo_catalog(repo, (await repo._open_catalogs.resolve(0)).dedup_repo)
    provider = _FakeClosableProvider()

    async def fake_provider_for(r: object, v: object) -> object:
        return provider

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)
    await catalog.provider(make_version(workload_id=1, target_type="VM"))

    await repo._close()
    await repo._close()

    assert fake_dedup_repo.close_count == 1


@faithful_to(ClosableUnitProvider)
class _FakeRaisingClosableProvider(_FakeClosableProvider):
    """Like ``_FakeClosableProvider``, but ``close()`` raises."""

    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("synthetic close failure")


async def test_close_still_closes_every_remaining_item_when_an_earlier_one_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One tracked provider's ``close()`` raising doesn't stop later items
    (including the ``DedupRepo``) from closing; failures are reported after
    every close was attempted."""
    fake_dedup_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_dedup_repo, connections=[])
    catalog = repo_catalog(repo, (await repo._open_catalogs.resolve(0)).dedup_repo)
    first, second = _FakeRaisingClosableProvider(), _FakeClosableProvider()
    remaining = [first, second]

    async def fake_provider_for(r: object, v: object) -> object:
        return remaining.pop(0)

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    version_a = make_version(workload_id=1, target_type="VM")
    version_b = make_version(workload_id=2, target_type="VM")
    assert await catalog.provider(version_a) is first
    assert await catalog.provider(version_b) is second

    with pytest.raises(
        ExceptionGroup, match="closing the repository failed to close every tracked resource"
    ) as exc_info:
        await repo._close()

    assert first.closed is True
    assert second.closed is True  # still closed despite first's failure
    assert fake_dedup_repo.closed is True  # still closed despite first's failure
    assert len(exc_info.value.exceptions) == 1
    assert isinstance(exc_info.value.exceptions[0], RuntimeError)


@faithful_to(ClosableUnitProvider)
class _FakeHangingClosableProvider(_FakeClosableProvider):
    """Like ``_FakeClosableProvider``, but ``close()`` never returns, which
    ``RESOURCE_CLOSE_TIMEOUT`` must bound."""

    async def close(self) -> None:
        await asyncio.Event().wait()


async def test_close_does_not_hang_forever_when_one_providers_close_hangs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hanging provider ``close()`` is bounded by
    ``RESOURCE_CLOSE_TIMEOUT`` (patched down here) and doesn't block closing
    the rest."""
    monkeypatch.setattr(provider_registry, "RESOURCE_CLOSE_TIMEOUT", 0.05)
    fake_dedup_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_dedup_repo, connections=[])
    catalog = repo_catalog(repo, (await repo._open_catalogs.resolve(0)).dedup_repo)
    first, second = _FakeHangingClosableProvider(), _FakeClosableProvider()
    remaining = [first, second]

    async def fake_provider_for(r: object, v: object) -> object:
        return remaining.pop(0)

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    version_a = make_version(workload_id=1, target_type="VM")
    version_b = make_version(workload_id=2, target_type="VM")
    assert await catalog.provider(version_a) is first
    assert await catalog.provider(version_b) is second

    with pytest.raises(
        ExceptionGroup, match="closing the repository failed to close every tracked resource"
    ) as exc_info:
        await asyncio.wait_for(repo._close(), timeout=5.0)  # the outer bound this test itself enforces

    assert second.closed is True  # still closed despite first hanging
    assert fake_dedup_repo.closed is True  # still closed despite first hanging
    assert len(exc_info.value.exceptions) == 1
    assert isinstance(exc_info.value.exceptions[0], TimeoutError)


async def test_close_also_closes_a_dedup_catalog_that_finishes_opening_after_close_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A catalog whose open is still in flight when ``close()`` runs is
    closed once it lands; ``close()`` settles in-flight opens
    (``settle_all()``), not just settled ones."""
    fake_repo = FakeDedupRepo(vault_layout())
    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_open(*a: object, **k: object) -> DedupRepo:
        started.set()
        await release.wait()
        return as_dedup_repo(fake_repo)

    patch_dedup_open(monkeypatch, _slow_open)
    monkeypatch.setattr(api_repository, "connections", async_returning([]))
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), None, None)

    resolve_task = asyncio.create_task(repo._open_catalogs.resolve(0))
    await started.wait()  # the open is in flight (owner determined), not yet settled

    close_task = asyncio.create_task(repo._close())
    await asyncio.sleep(0)  # let close() start settling while index 0 is still in flight
    release.set()

    await resolve_task
    await close_task

    assert fake_repo.closed is True


async def test_close_reports_but_does_not_abort_when_an_in_flight_open_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An in-flight open that fails while ``close()`` settles it is reported
    in ``close()``'s ``ExceptionGroup`` without aborting the rest."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_failing_open(*a: object, **k: object) -> DedupRepo:
        started.set()
        await release.wait()
        raise RuntimeError("synthetic open failure")

    patch_dedup_open(monkeypatch, _slow_failing_open)
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), None, None)

    resolve_task = asyncio.create_task(repo._open_catalogs.resolve(0))
    await started.wait()

    close_task = asyncio.create_task(repo._close())
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(RuntimeError, match="synthetic open failure"):
        await resolve_task
    with pytest.raises(
        ExceptionGroup, match="closing the repository failed to close every tracked resource"
    ) as exc_info:
        await close_task
    assert len(exc_info.value.exceptions) == 1
    assert isinstance(exc_info.value.exceptions[0], RuntimeError)


# -- Repository.locate() on a human ref ---------------------------------------


@faithful_to(ClosableUnitProvider)
class _FakeWalkProvider:
    """A minimal ``UnitProvider`` for ``locate``, which never calls
    ``unit()``."""

    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]]) -> None:
        self._root = root
        self._children_by_ref = children_by_ref
        self.closed = False

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children_by_ref.get(str(node.ref), [])

    async def close(self) -> None:
        self.closed = True


def _repo_with_catalog_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    connections: list[Connection] | None = None,
    workloads: list[Workload] | None = None,
    versions: list[Version] | None = None,
    provider: _FakeWalkProvider | None = None,
) -> api.Repository:
    """Fakes the module-level collaborators ``locate`` reaches
    through (``connections``/``workloads``/``versions``/``provider_for``).
    Every version is ``"VM"``, so faking ``provider_for`` alone controls
    ``Catalog.provider()``."""
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), connections=[])
    monkeypatch.setattr(api_repository, "connections", async_returning(connections or []))
    monkeypatch.setattr(api_catalog, "workloads", async_returning(workloads or []))
    monkeypatch.setattr(api_catalog, "versions", async_returning(versions or []))
    if provider is not None:
        monkeypatch.setattr(api_catalog, "provider_for", async_returning(provider))
    return repo


async def test_locate_human_ref_empty_segments_returns_root_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_catalog_stubs(monkeypatch)
    frame = await repo.locate(NodeRef.human(""))
    assert isinstance(frame, api.RootFrame)


async def test_locate_human_ref_one_segment_returns_catalog_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = make_connection()
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[connection])
    frame = await repo.locate(NodeRef.human("", connection.display_name))
    assert isinstance(frame, api.CatalogFrame)
    assert frame.catalog is not None
    assert frame.catalog.connection is connection


async def test_locate_human_ref_unknown_catalog_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[make_connection()])
    with pytest.raises(NotFoundError, match="no backup source named"):
        await repo.locate(NodeRef.human("", "NoSuchSource"))


async def test_locate_human_ref_two_segments_returns_workload_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = make_connection()
    workload = make_workload(workload_type="M365", sub_type="MAIL")
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[connection], workloads=[workload])
    frame = await repo.locate(NodeRef.human("", connection.display_name, workload.display_name))
    assert isinstance(frame, api.WorkloadFrame)
    assert frame.workload is workload


async def test_locate_human_ref_unknown_workload_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = make_connection()
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[make_workload(workload_type="M365", sub_type="MAIL")]
    )
    with pytest.raises(NotFoundError, match="no workload named"):
        await repo.locate(NodeRef.human("", connection.display_name, "NoSuchWorkload"))


async def test_locate_human_ref_ambiguous_workload_raises_not_found_with_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Same display_name and sub_type: a collision, not a miss, so the message
    # must be the ambiguity one (see match_or_raise).
    connection = make_connection()
    dup_a = make_workload(
        workload_id=1, workload_uid="wl-a", workload_type="M365", sub_type="MAIL", display_name="Alice"
    )
    dup_b = make_workload(
        workload_id=2, workload_uid="wl-b", workload_type="M365", sub_type="MAIL", display_name="Alice"
    )
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[connection], workloads=[dup_a, dup_b])
    with pytest.raises(NotFoundError, match="workload named 'Alice' is ambiguous \\(2 matches\\) — use one of:"):
        await repo.locate(NodeRef.human("", connection.display_name, dup_a.display_name))


async def test_locate_human_ref_three_segments_returns_node_frame_at_provider_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = make_connection()
    workload = make_workload(workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=1, target_type="VM")
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    provider = _FakeWalkProvider(root_node, {})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    frame = await repo.locate(NodeRef.human("", connection.display_name, workload.display_name, version.display_name))
    assert isinstance(frame, api.NodeFrame)
    assert frame.node is root_node
    assert cast(Any, frame.provider) is provider


async def test_locate_human_ref_unknown_version_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, workload = make_connection(), make_workload(workload_type="M365", sub_type="MAIL")
    repo = _repo_with_catalog_stubs(
        monkeypatch,
        connections=[connection],
        workloads=[workload],
        versions=[make_version(workload_id=1, target_type="VM")],
    )
    with pytest.raises(NotFoundError, match="no version named"):
        await repo.locate(NodeRef.human("", connection.display_name, workload.display_name, "NoSuchVersion"))


async def test_locate_human_ref_descends_into_provider_tree_by_name(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, workload = make_connection(), make_workload(workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=1, target_type="VM")
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    child_node = Node(ref=NodeRef("", ("root", "child")), name="Child", is_leaf=True)
    provider = _FakeWalkProvider(root_node, {str(root_node.ref): [child_node]})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    frame = await repo.locate(
        NodeRef.human("", connection.display_name, workload.display_name, version.display_name, "Child")
    )
    assert isinstance(frame, api.NodeFrame)
    assert frame.node is child_node


async def test_locate_human_ref_unknown_item_name_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, workload = make_connection(), make_workload(workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=1, target_type="VM")
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    provider = _FakeWalkProvider(root_node, {str(root_node.ref): []})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    with pytest.raises(NotFoundError, match="no item named"):
        await repo.locate(
            NodeRef.human("", connection.display_name, workload.display_name, version.display_name, "NoSuchChild")
        )


async def test_locate_human_ref_releases_the_provider_when_an_item_segment_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The caller never receives a provider from a failed walk, so the walk
    itself must release it instead of leaving it open until the repository closes."""
    connection, workload = make_connection(), make_workload(workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=1, target_type="VM")
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    provider = _FakeWalkProvider(root_node, {str(root_node.ref): []})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    with pytest.raises(NotFoundError, match="no item named"):
        await repo.locate(
            NodeRef.human("", connection.display_name, workload.display_name, version.display_name, "NoSuchChild")
        )

    assert provider.closed
    assert not repo._providers._providers  # the only provider this walk built


async def test_locate_raw_ref_releases_the_provider_when_no_node_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("", ("raw",)), name="root", is_leaf=False)
    provider = _FakeWalkProvider(root_node, {})
    repo = _repo_with_catalog_stubs(monkeypatch)
    monkeypatch.setattr(api_repository, "FileMapTreeProvider", lambda r: provider)

    with pytest.raises(NotFoundError, match="no node in provider tree"):
        await repo.locate(NodeRef.raw("", "VM-uid/no-such-disk.img"))

    assert provider.closed
    assert not repo._providers._providers


async def test_locate_reports_not_found_even_when_releasing_the_provider_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingClose(_FakeWalkProvider):
        async def close(self) -> None:
            raise OSError("close failed")

    provider = _FailingClose(Node(ref=NodeRef("", ("raw",)), name="root", is_leaf=False), {})
    repo = _repo_with_catalog_stubs(monkeypatch)
    monkeypatch.setattr(api_repository, "FileMapTreeProvider", lambda r: provider)

    with pytest.raises(NotFoundError, match="no node in provider tree matches ref") as exc_info:
        await repo.locate(NodeRef.raw("", "VM-uid/no-such-disk.img"))
    assert any("close failed" in note for note in exc_info.value.__notes__)
    assert not repo._providers._providers


async def test_locate_canonical_ref_releases_the_provider_when_no_node_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection, workload = make_connection(), make_workload(workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=1, target_type="VM")
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    provider = _FakeWalkProvider(root_node, {})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    (catalog,) = await repo.catalogs()

    async def _located(_repo: api.Repository, _ref: NodeRef) -> api.VersionLocation:
        return api.VersionLocation(catalog, workload, version)

    monkeypatch.setattr(api_repository, "_locate_canonical_ref", _located)
    ref = NodeRef.canonical(
        "", catalog_id=catalog.catalog_id, workload_id=workload.workload_id, version_uid=version.version_uid
    ).child("no-such-item")

    with pytest.raises(NotFoundError, match="no node in provider tree"):
        await repo.locate(ref)

    assert provider.closed
    assert not repo._providers._providers


async def test_locate_human_ref_past_a_leaf_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, workload = make_connection(), make_workload(workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=1, target_type="VM")
    leaf_root = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=True)
    provider = _FakeWalkProvider(leaf_root, {})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    with pytest.raises(NotFoundError, match="more levels than the tree has"):
        await repo.locate(
            NodeRef.human("", connection.display_name, workload.display_name, version.display_name, "too-deep")
        )


async def test_resolve_forwards_the_raw_view_for_a_human_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    repo = _repo_with_catalog_stubs(monkeypatch)
    seen: list[api.RawView | None] = []

    async def fake_locate(ref: NodeRef, *, raw: api.RawView | None = None) -> api.Frame:
        seen.append(raw)
        return api.NodeFrame(cast(Any, _FakeWalkProvider(root_node, {})), root_node)

    monkeypatch.setattr(repo, "locate", fake_locate)
    node_ref = NodeRef("", ("Source", "Workload", "Version"))
    assert node_ref.kind is RefKind.HUMAN
    await repo.resolve(node_ref, raw=api.RawView("stream_0_42"))
    assert seen == [api.RawView("stream_0_42")]
