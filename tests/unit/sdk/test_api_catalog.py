"""Unit tests for ``api.Catalog``: ``provider()``'s dispatch, the
``workloads()``/``versions()``/``verify()`` key gate, delegation of
``workloads``/``versions``/``provider``/``verify``, and ``info``.
``Repository.catalogs()``/``catalog_by_id()``'s own open/exception posture is
covered in ``test_api_repository.py``. Collaborators are faked at the module
boundary (the names ``api.repository``/``api.catalog`` imported);
``tests/integration/sdk/test_api.py`` covers the real wiring."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from support.model_factories import make_connection, make_version, make_workload
from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.api import catalog as api_catalog
from synology_apm_repo.sdk.api import repository as api_repository
from synology_apm_repo.sdk.catalog.version import VersionMeta
from synology_apm_repo.sdk.dedup.keys import KeyVerification
from synology_apm_repo.sdk.errors import KeyMismatchError, KeyRequiredError
from synology_apm_repo.sdk.findings import Finding, Stage, Symptom
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider
from unit.sdk.api_fakes import (
    FakeDedupRepo,
    FakeStore,
    as_dedup_repo,
    as_object_store,
    async_returning,
    repo_catalog,
    repo_with_fake_dedup,
    some_key,
    vault_layout,
    vault_repository_layout,
)


async def test_provider_routes_device_fs_target_types_to_provider_for(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    dedup_repo = as_dedup_repo(FakeDedupRepo(vault_layout()))
    catalog = repo_catalog(repo, dedup_repo)
    version = make_version(workload_id=1, target_type="VM")
    sentinel = object()
    calls: list[Any] = []

    async def fake_provider_for(r: object, v: object) -> object:
        calls.append((r, v))
        return sentinel

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    result = await catalog.provider(version)

    assert result is sentinel
    assert calls == [(dedup_repo, version)]


async def test_provider_routes_saas_target_type_via_workload_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    dedup_repo = as_dedup_repo(FakeDedupRepo(vault_layout()))
    catalog = repo_catalog(repo, dedup_repo)
    workload = make_workload(workload_id=42, workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=42, target_type="M365")
    sentinel = object()
    captured: list[Any] = []

    async def fake_saas_provider_for(
        r: object, w: object, v: object, saas_streams: object, *, object_db_id: object = None
    ) -> object:
        captured.append((r, w, v))
        return sentinel

    monkeypatch.setattr(api_catalog, "workload_by_id", async_returning(workload))
    monkeypatch.setattr(api_catalog, "saas_provider_for", fake_saas_provider_for)

    result = await catalog.provider(version)

    assert result is sentinel
    assert captured == [(dedup_repo, workload, version)]


async def test_provider_falls_back_to_raw_when_workload_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    version = make_version(workload_id=999, target_type="M365")
    sentinel = object()

    monkeypatch.setattr(api_catalog, "workload_by_id", async_returning(None))
    monkeypatch.setattr(RawObjectProvider, "create", async_returning(sentinel))

    result = await catalog.provider(version)

    assert result is sentinel


async def test_provider_raw_view_skips_the_workload_lookup_and_saas_candidates_entirely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For a non-device version, ``raw=RawView()`` goes straight to
    ``RawObjectProvider.create()`` even when ``saas_provider_for()`` would
    recognize it."""
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    dedup_repo = as_dedup_repo(FakeDedupRepo(vault_layout()))
    catalog = repo_catalog(repo, dedup_repo)
    workload = make_workload(workload_id=42, workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=42, target_type="M365")
    sentinel = object()
    saas_called = False

    async def fake_saas_provider_for(*a: object, **k: object) -> object:
        nonlocal saas_called
        saas_called = True
        return object()

    captured: list[Any] = []

    async def fake_raw_object_provider_create(
        r: object, v: object, saas_streams: object, *, object_db_id: object = None
    ) -> object:
        captured.append((r, v))
        return sentinel

    monkeypatch.setattr(api_catalog, "workload_by_id", async_returning(workload))
    monkeypatch.setattr(api_catalog, "saas_provider_for", fake_saas_provider_for)
    monkeypatch.setattr(RawObjectProvider, "create", fake_raw_object_provider_create)

    result = await catalog.provider(version, raw=api.RawView())

    assert result is sentinel
    assert captured == [(dedup_repo, version)]
    assert saas_called is False


async def test_provider_raw_view_is_ignored_for_device_fs_target_types(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    version = make_version(workload_id=1, target_type="VM")
    sentinel = object()

    async def fake_provider_for(r: object, v: object) -> object:
        return sentinel

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    result = await catalog.provider(version, raw=api.RawView())

    assert result is sentinel


# -- workloads()/versions() key gate ------------------------------------------
#
# An encrypted, not yet key-verified repository refuses to browse
# workloads/versions rather than silently returning less data.
# connections() is exempt: connection_config rows are plaintext.


async def test_workloads_raises_key_required_when_no_key_provided() -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    with pytest.raises(KeyRequiredError, match="this repository is encrypted"):
        await catalog.workloads()


async def test_workloads_raises_key_mismatch_when_key_invalid() -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=keys, key_verification=verification
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    with pytest.raises(KeyMismatchError, match="the key previously supplied for this repository was rejected"):
        await catalog.workloads()


async def test_workloads_succeeds_when_key_verified(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=keys, key_verification=verification
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    sentinel = [make_workload(workload_type="M365", sub_type="MAIL")]
    monkeypatch.setattr(api_catalog, "workloads", async_returning(sentinel))
    assert await catalog.workloads() == sentinel


async def test_workloads_succeeds_when_not_encrypted(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    sentinel = [make_workload(workload_type="M365", sub_type="MAIL")]
    monkeypatch.setattr(api_catalog, "workloads", async_returning(sentinel))
    assert await catalog.workloads() == sentinel


async def test_versions_raises_key_required_when_no_key_provided() -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    with pytest.raises(KeyRequiredError, match="this repository is encrypted"):
        await catalog.versions(make_workload(workload_type="M365", sub_type="MAIL"))


async def test_versions_raises_key_mismatch_when_key_invalid() -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=keys, key_verification=verification
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    with pytest.raises(KeyMismatchError, match="the key previously supplied for this repository was rejected"):
        await catalog.versions(make_workload(workload_type="M365", sub_type="MAIL"))


async def test_versions_succeeds_when_key_verified(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=keys, key_verification=verification
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    monkeypatch.setattr(api_catalog, "versions", async_returning([]))
    assert await catalog.versions(make_workload(workload_type="M365", sub_type="MAIL")) == []


async def test_versions_succeeds_when_not_encrypted(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    monkeypatch.setattr(api_catalog, "versions", async_returning([]))
    assert await catalog.versions(make_workload(workload_type="M365", sub_type="MAIL")) == []


async def test_catalog_workloads_delegates_to_the_same_workloads_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, encrypted=False)
    connection = make_connection()
    monkeypatch.setattr(api_repository, "connections", async_returning([connection]))
    sentinel = [make_workload(workload_type="M365", sub_type="MAIL")]
    captured: list[Any] = []

    async def fake_workloads(r: object, c: object) -> object:
        captured.append((r, c))
        return sentinel

    monkeypatch.setattr(api_catalog, "workloads", fake_workloads)
    (catalog,) = await repo.catalogs()

    assert await catalog.workloads() == sentinel
    assert captured == [(as_dedup_repo(fake_repo), connection)]


async def test_catalog_versions_returns_the_raw_result_unfiltered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``Catalog.versions()`` adds no filter over ``catalog.version.versions()``:
    a version with no ``meta`` still comes back."""
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), encrypted=False)
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))
    (catalog,) = await repo.catalogs()

    raw = [make_version(workload_id=1, target_type="VM")]
    monkeypatch.setattr(api_catalog, "versions", async_returning(raw))

    assert await catalog.versions(make_workload(workload_type="M365", sub_type="MAIL")) == raw


async def test_catalog_provider_is_tracked_by_the_owning_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()), encrypted=False)
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))
    (catalog,) = await repo.catalogs()

    version = make_version(workload_id=1, target_type="VM")
    sentinel = object()

    async def fake_provider_for(r: object, v: object) -> object:
        return sentinel

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    result = await catalog.provider(version)

    assert result is sentinel
    assert list(repo._providers._providers.values()) == [sentinel]


async def test_catalog_verify_delegates_to_the_reachability_walk(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Catalog.verify()`` runs ``verify_reachable()`` on its shared ``DedupRepo``."""
    calls: list[tuple[object, object]] = []
    sentinel = [Finding(stage=Stage.REPO_INFO, symptom=Symptom.MISMATCH, path="p", detail="d")]

    async def fake_verify_reachable(dedup_repo: object, level: object, *, progress: object = None) -> list[Finding]:
        calls.append((dedup_repo, level))
        return sentinel

    fake_repo = FakeDedupRepo(vault_layout())
    repo = repo_with_fake_dedup(monkeypatch, fake_repo, encrypted=False)
    monkeypatch.setattr(api_repository, "connections", async_returning([make_connection()]))
    monkeypatch.setattr(api_catalog, "verify_reachable", fake_verify_reachable)
    (catalog,) = await repo.catalogs()

    result = await catalog.verify(api.VerifyLevel.QUICK)

    assert result == sentinel
    assert calls == [(fake_repo, api.VerifyLevel.QUICK)]


# -- verify()'s own key gate ---------------------------------------------
#
# catalog.version.versions() silently drops every row whose version_spec it
# can't decrypt, so without this gate verify() with no/wrong key would walk
# zero versions and report a clean result.


async def test_catalog_verify_raises_key_required_when_no_key_provided() -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    with pytest.raises(KeyRequiredError, match="this repository is encrypted"):
        await catalog.verify()


async def test_catalog_verify_raises_key_mismatch_when_key_invalid() -> None:
    keys = some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=keys, key_verification=verification
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    with pytest.raises(KeyMismatchError, match="the key previously supplied for this repository was rejected"):
        await catalog.verify()


async def test_versions_delegates_to_catalog_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(
        as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    catalog = repo_catalog(repo, as_dedup_repo(FakeDedupRepo(vault_layout())))
    workload = make_workload(workload_type="M365", sub_type="MAIL")
    sentinel = [
        dataclasses.replace(
            make_version(workload_id=1, target_type="FS"),
            meta=VersionMeta(target_meta_path="/p/x", meta_filenames=("target.db", "0_version.db.zst"), status=1),
        )
    ]
    monkeypatch.setattr(api_catalog, "versions", async_returning(sentinel))

    assert await catalog.versions(workload) == sentinel


def test_catalog_info_delegates_to_dedup_repo() -> None:
    """``info`` lives on ``Catalog``: it is per-catalog for object storage."""
    fake_repo = FakeDedupRepo(vault_layout(), info="some-repo-info")
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    catalog = repo_catalog(repo, as_dedup_repo(fake_repo))
    info: Any = catalog.info
    assert info == "some-repo-info"
