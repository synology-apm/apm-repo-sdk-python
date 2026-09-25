"""Unit tests for ``synology_apm_repo.sdk.api``'s ``Catalog`` -- the part
of ``test_api.py`` (split three ways, alongside ``test_api_repository.py``/
``test_api_session.py``, by primary subject under test) covering
``Catalog.provider()``'s dispatch, the ``workloads()``/``versions()`` key
gate, ``Catalog.workloads``/``versions``/``provider``/``verify``'s own
delegation, ``versions()``'s per-target-type availability filters
(``_filtered_versions``), and ``Catalog.info``. ``Repository.catalogs()``/
``catalog_by_id()``'s own concurrent-open/exception posture lives in
``test_api_repository.py`` instead -- this file's own catalog-obtaining
tests still bootstrap through ``repo.catalogs()`` (faking
``api_repository.connections`` to do so), same rationale as that split
elsewhere in this codebase: classify by primary subject under test, not
every module a test's own setup happens to touch. These tests fake
every collaborator at the module boundary (monkeypatching the names
``api.repository``/``api.catalog`` imported them under) rather than
standing up a real repository on disk -- see
``tests/integration/sdk/test_api.py`` for the real end-to-end wiring.
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest

from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.api import catalog as api_catalog
from synology_apm_repo.sdk.api import repository as api_repository
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version, VersionMeta
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import Finding, Stage, Symptom
from synology_apm_repo.sdk.errors import KeyMismatchError, KeyRequiredError
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    ConnectionId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
    WorkloadUid,
)
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache


class _FakeStore:
    """A plain duck-typed fake, not a real ``ObjectStore`` implementer.
    ``exists()`` always answers ``False`` -- the one real call a discover
    test can reach without a monkeypatch: ``Session``'s own
    ``_probe_encrypted`` (``dedup.keys.probe_encrypted``) runs against the
    real store *before* any ``Repository`` opens a ``DedupRepo``, and
    a ``False`` here short-circuits it to ``None`` (its own "record
    genuinely absent" case) without needing a real ``db/
    vault_encryption_key`` to read. A test that cares about a specific
    encrypted/not-encrypted outcome monkeypatches ``api_session.
    _probe_encrypted`` directly instead of relying on this."""

    async def exists(self, path: str) -> bool:
        return False


def _as_object_store(fake: _FakeStore) -> ObjectStore:
    """Same cast-at-the-call-site convention as ``_as_dedup_repo`` — a
    plain duck-typed fake, not a real ``ObjectStore`` implementer, handed
    to ``Session.discover_remote``/``Session.open_remote``."""
    return cast(ObjectStore, fake)


class _FakeDedupRepo:
    """Stands in for ``DedupRepo`` — just enough surface for
    ``Repository`` to wire through: ``store``/``layout``/``info``/``close``.
    Cast to ``DedupRepo`` at each call site that hands one to
    ``api.Repository``/``api.Session`` — a plain duck-typed fake, matching
    ``test_units_dispatch.py``'s existing convention for this codebase."""

    def __init__(self, layout: RepoLayout, *, info: object = "fake-info", encrypted: bool | None = None) -> None:
        self.store = _FakeStore()
        self.layout = layout
        self.info = info
        self.closed = False
        # Unused by Session.discover() itself now (it probes via the
        # free function dedup.keys.probe_encrypted() against the store,
        # before any DedupRepo is opened); kept for a caller that
        # constructs a Repository directly with an explicit `encrypted=`
        # and wants this fake's own probe_encrypted() to agree with it.
        self._encrypted = encrypted

    async def close(self) -> None:
        self.closed = True

    async def open_file(self, path: str) -> str:
        return f"opened:{path}"

    async def probe_encrypted(self) -> bool | None:
        return self._encrypted


def _as_dedup_repo(fake: _FakeDedupRepo) -> DedupRepo:
    return cast(DedupRepo, fake)


def _layout(repo_root: str = "") -> RepoLayout:
    return RepoLayout(kind=RepoKind.VAULT, repo_root=repo_root)


def _repository_layout(repo_root: str = "") -> RepositoryLayout:
    """The ``RepositoryLayout``-level counterpart to ``_layout()`` above —
    what ``api.Repository.__init__`` itself now takes (a single opened
    bucket/vault, not one already-opened ``DedupRepo``). Every test
    here uses ``RepoKind.VAULT``, matching ``_layout()``'s own default:
    ``catalog_repo_layouts()`` always resolves a ``VAULT``
    ``RepositoryLayout`` to exactly one derived ``RepoLayout`` (a vault's
    own catalogs come from querying ``db/connection_config`` after
    opening, not a separate directory per catalog), so a single-catalog
    fake (``_FakeDedupRepo``) is always the right shape regardless of
    which of the two layout types a given test builds by hand."""
    return RepositoryLayout(kind=RepoKind.VAULT, repo_root=repo_root)


def _repo_with_fake_dedup(
    monkeypatch: pytest.MonkeyPatch,
    fake: _FakeDedupRepo,
    *,
    keys: KeyMaterial | None = None,
    key_verification: KeyVerification | None = None,
    encrypted: bool | None = None,
    layout: RepositoryLayout | None = None,
) -> api.Repository:
    """Construct an ``api.Repository`` backed by ``fake`` — the new-model
    replacement for directly constructing ``api.Repository(_as_dedup_repo(fake), ...)``,
    now that ``Repository`` opens its own ``DedupRepo``(s) lazily via
    ``DedupRepo.open()`` rather than taking one ready-made. Monkeypatches
    ``DedupRepo.open`` to hand back ``fake`` regardless of which derived
    ``RepoLayout`` it's called with — fine for every test here, which only
    ever has one single-catalog vault layout to open (every test here
    builds a ``VAULT`` layout, which always resolves to exactly one
    ``RepoLayout``). Construction itself never
    opens anything (only ``Session``'s own ``_confirm_real()`` or an
    explicit ``catalogs()``/``_open_catalogs.resolve()`` call does) — a
    test that only checks ``key_status``/``is_encrypted`` right after
    construction doesn't need this helper at all, a bare ``api.Repository(
    _as_object_store(_FakeStore()), _repository_layout(), ...)`` is enough."""
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda *a, **k: fake))
    return api.Repository(
        _as_object_store(_FakeStore()),
        layout if layout is not None else _repository_layout(),
        keys,
        key_verification,
        encrypted=encrypted,
    )


def _async_iter_repository_layouts(layouts: list[RepositoryLayout]) -> Any:
    """``storage.layout.iter_repository_layouts`` is an **async
    generator** now, so a stand-in for it has to be one too — a plain
    ``iter(...)`` would blow up at ``discover()``'s ``async for``."""

    async def _iter(store: object, root: str = "") -> AsyncIterator[RepositoryLayout]:
        for layout in layouts:
            yield layout

    return _iter


def _async_returning(value: Any) -> Any:
    """A coroutine function that ignores its arguments and returns
    ``value`` — the async replacement for the ``lambda *a, **k: value``
    stubs these tests used before every collaborator became awaitable."""

    async def _fn(*a: object, **k: object) -> Any:
        return value

    return _fn


def _async_open(fn: Any) -> Any:
    """``DedupRepo.open`` is an ``async`` classmethod; wrap a plain
    sync factory so monkeypatched replacements stay one-liners."""

    async def _open(cls: object, /, *a: object, **k: object) -> DedupRepo:
        return cast(DedupRepo, fn(*a, **k))

    return classmethod(_open)


# parse_key_string (format/crypto.py) requires a 12-char userKeyID and a
# userKey that decodes to exactly 32 raw bytes, regardless of the id — it
# deliberately does not special-case "NoEncryption" (see its docstring).
_VALID_B64_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def _no_encryption_keys() -> KeyMaterial:
    return KeyMaterial.from_key_string(f"NoEncryption@{_VALID_B64_KEY}")


def _some_key(user_key_id: str) -> KeyMaterial:
    assert len(user_key_id) == 12
    return KeyMaterial.from_key_string(f"{user_key_id}@{_VALID_B64_KEY}")


def _make_workload(workload_id: int = 1) -> Workload:
    return Workload(
        workload_id=WorkloadId(workload_id),
        workload_uid=WorkloadUid("wl-uid"),
        workload_type="M365",
        sub_type="MAIL",
        display_name="Some User",
        subtitle=None,
        spec={},
    )


def _make_version(workload_id: int, target_type: str) -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("ver-uid"),
        workload_id=WorkloadId(workload_id),
        connection_config_id=ConnectionConfigId(1),
        target_type=target_type,
        target_id=TargetId("target-id"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-08-07 09:00",
        meta=None,
    )


def _make_connection() -> Connection:
    return Connection(
        connection_config_id=ConnectionConfigId(1),
        connection_id=ConnectionId("cc-1"),
        display_name="Source 1",
        namespaces=(),
        workload_count=1,
        version_count=1,
    )


def _make_catalog(
    repo: api.Repository, dedup_repo: DedupRepo, *, connection: Connection | None = None
) -> api_catalog.Catalog:
    """A ``Catalog`` sharing ``repo``'s own tracking (``_track``) and key
    gate (``_require_key_verified``) — for a test that needs a ``Catalog``
    in isolation, without going through a full ``repo.catalogs()``
    round-trip (which would additionally need ``connections()``/
    ``DedupRepo.open()`` faked). Builds its own ``SaasStreamCache`` against
    ``dedup_repo`` — none of this file's tests need to inspect it directly,
    only that ``Catalog`` has one."""
    return api_catalog.Catalog(
        dedup_repo,
        connection or _make_connection(),
        saas_streams=SaasStreamCache(dedup_repo),
        track=repo._track,
        require_key_verified=repo._require_key_verified,
    )


async def test_provider_routes_device_fs_target_types_to_provider_for(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    dedup_repo = _as_dedup_repo(_FakeDedupRepo(_layout()))
    catalog = _make_catalog(repo, dedup_repo)
    version = _make_version(workload_id=1, target_type="VM")
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
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    dedup_repo = _as_dedup_repo(_FakeDedupRepo(_layout()))
    catalog = _make_catalog(repo, dedup_repo)
    workload = _make_workload(workload_id=42)
    version = _make_version(workload_id=42, target_type="M365")
    sentinel = object()
    captured: list[Any] = []

    async def fake_saas_provider_for(
        r: object, w: object, v: object, saas_streams: object, *, object_db_id: object = None
    ) -> object:
        captured.append((r, w, v))
        return sentinel

    monkeypatch.setattr(api_catalog, "workload_by_id", _async_returning(workload))
    monkeypatch.setattr(api_catalog, "saas_provider_for", fake_saas_provider_for)

    result = await catalog.provider(version)

    assert result is sentinel
    assert captured == [(dedup_repo, workload, version)]


async def test_provider_falls_back_to_raw_when_workload_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    version = _make_version(workload_id=999, target_type="M365")
    sentinel = object()

    monkeypatch.setattr(api_catalog, "workload_by_id", _async_returning(None))
    monkeypatch.setattr(api_catalog, "raw_fallback_provider_for", _async_returning(sentinel))

    result = await catalog.provider(version)

    assert result is sentinel


async def test_provider_force_raw_skips_the_workload_lookup_and_saas_candidates_entirely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``force_raw=True`` goes straight to ``raw_fallback_provider_for()``
    even for a version whose workload *would* resolve and whose sub_type
    *would* otherwise dispatch through ``saas_provider_for()`` — the TUI's
    ``d``-mode override (``UnitScreen``'s own ``force_raw=self.app_state.verbose``)
    depends on this short-circuit happening unconditionally, not only
    when nothing else recognizes the version."""
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    dedup_repo = _as_dedup_repo(_FakeDedupRepo(_layout()))
    catalog = _make_catalog(repo, dedup_repo)
    workload = _make_workload(workload_id=42)
    version = _make_version(workload_id=42, target_type="M365")
    sentinel = object()
    saas_called = False

    async def fake_saas_provider_for(*a: object, **k: object) -> object:
        nonlocal saas_called
        saas_called = True
        return object()

    captured: list[Any] = []

    async def fake_raw_fallback_provider_for(
        r: object, v: object, saas_streams: object, *, object_db_id: object = None
    ) -> object:
        captured.append((r, v))
        return sentinel

    monkeypatch.setattr(api_catalog, "workload_by_id", _async_returning(workload))
    monkeypatch.setattr(api_catalog, "saas_provider_for", fake_saas_provider_for)
    monkeypatch.setattr(api_catalog, "raw_fallback_provider_for", fake_raw_fallback_provider_for)

    result = await catalog.provider(version, force_raw=True)

    assert result is sentinel
    assert captured == [(dedup_repo, version)]
    assert saas_called is False


async def test_provider_force_raw_is_ignored_for_device_fs_target_types(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    version = _make_version(workload_id=1, target_type="VM")
    sentinel = object()

    async def fake_provider_for(r: object, v: object) -> object:
        return sentinel

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    result = await catalog.provider(version, force_raw=True)

    assert result is sentinel


# -- workloads()/versions() key gate ------------------------------------------
#
# A repository that's encrypted and not yet key-verified must refuse to browse
# workloads/versions rather than silently returning less data than it
# should. connections() is deliberately exempt -- connection_config rows
# are plaintext regardless of encryption, so nothing there needs a key --
# so it has no equivalent tests here.


async def test_workloads_raises_key_required_when_no_key_provided() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    with pytest.raises(KeyRequiredError):
        await catalog.workloads()


async def test_workloads_raises_key_mismatch_when_key_invalid() -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=keys, key_verification=verification
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    with pytest.raises(KeyMismatchError):
        await catalog.workloads()


async def test_workloads_succeeds_when_key_verified(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=keys, key_verification=verification
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    sentinel = [_make_workload()]
    monkeypatch.setattr(api_catalog, "workloads", _async_returning(sentinel))
    assert await catalog.workloads() == sentinel


async def test_workloads_succeeds_when_not_encrypted(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    sentinel = [_make_workload()]
    monkeypatch.setattr(api_catalog, "workloads", _async_returning(sentinel))
    assert await catalog.workloads() == sentinel


async def test_versions_raises_key_required_when_no_key_provided() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    with pytest.raises(KeyRequiredError):
        await catalog.versions(_make_workload())


async def test_versions_raises_key_mismatch_when_key_invalid() -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=keys, key_verification=verification
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    with pytest.raises(KeyMismatchError):
        await catalog.versions(_make_workload())


async def test_versions_succeeds_when_key_verified(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=keys, key_verification=verification
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    monkeypatch.setattr(api_catalog, "versions", _async_returning([]))
    assert await catalog.versions(_make_workload()) == []


async def test_versions_succeeds_when_not_encrypted(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    monkeypatch.setattr(api_catalog, "versions", _async_returning([]))
    assert await catalog.versions(_make_workload()) == []


async def test_catalog_workloads_delegates_to_the_same_workloads_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, encrypted=False)
    connection = _make_connection()
    monkeypatch.setattr(api_repository, "connections", _async_returning([connection]))
    sentinel = [_make_workload()]
    captured: list[Any] = []

    async def fake_workloads(r: object, c: object) -> object:
        captured.append((r, c))
        return sentinel

    monkeypatch.setattr(api_catalog, "workloads", fake_workloads)
    (catalog,) = await repo.catalogs()

    assert await catalog.workloads() == sentinel
    assert captured == [(_as_dedup_repo(fake_repo), connection)]


async def test_catalog_versions_returns_the_raw_result_unfiltered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``Catalog.versions()`` is the raw catalog read, nothing filtered
    out in advance — a version with no ``meta`` at all (which would once
    have been dropped by a listing-time availability check) still comes
    back; a version that turns out unresolvable raises when actually
    opened instead."""
    repo = _repo_with_fake_dedup(monkeypatch, _FakeDedupRepo(_layout()), encrypted=False)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))
    (catalog,) = await repo.catalogs()

    raw = [_make_version(workload_id=1, target_type="VM")]
    monkeypatch.setattr(api_catalog, "versions", _async_returning(raw))

    assert await catalog.versions(_make_workload()) == raw


async def test_catalog_provider_delegates_to_the_same_dispatch_as_repository_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _repo_with_fake_dedup(monkeypatch, _FakeDedupRepo(_layout()), encrypted=False)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))
    (catalog,) = await repo.catalogs()

    version = _make_version(workload_id=1, target_type="VM")
    sentinel = object()

    async def fake_provider_for(r: object, v: object) -> object:
        return sentinel

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    result = await catalog.provider(version)

    assert result is sentinel
    assert repo._providers == [sentinel]  # tracked by the owning Repository, not the Catalog


async def test_catalog_verify_delegates_to_the_reachability_walk(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Catalog.verify()`` calls ``units.verify_reachable.verify_reachable()``
    (the top-down walk) on its own shared ``DedupRepo``."""
    calls: list[tuple[object, object]] = []
    sentinel = [Finding(stage=Stage.REPO_INFO, symptom=Symptom.MISMATCH, path="p", detail="d")]

    async def fake_verify_reachable(dedup_repo: object, level: object, *, progress: object = None) -> list[Finding]:
        calls.append((dedup_repo, level))
        return sentinel

    fake_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, encrypted=False)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))
    monkeypatch.setattr(api_catalog, "verify_reachable", fake_verify_reachable)
    (catalog,) = await repo.catalogs()

    result = await catalog.verify(api.VerifyLevel.QUICK)

    assert result == sentinel
    assert calls == [(fake_repo, api.VerifyLevel.QUICK)]


# -- verify()'s own key gate ---------------------------------------------
#
# catalog.versions()'s own browsable-status filter silently drops every
# row it can't decrypt version_spec for -- indistinguishable, from inside
# that filter, from a genuinely non-browsable version. Without this gate,
# verify() against an encrypted repository with no/wrong key would walk zero
# versions and report a misleadingly clean result instead of refusing to
# run -- same "workloads()/versions() gate" shape as the tests above.


async def test_catalog_verify_raises_key_required_when_no_key_provided() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    with pytest.raises(KeyRequiredError):
        await catalog.verify()


async def test_catalog_verify_raises_key_mismatch_when_key_invalid() -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=keys, key_verification=verification
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    with pytest.raises(KeyMismatchError):
        await catalog.verify()


async def test_versions_delegates_to_catalog_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    catalog = _make_catalog(repo, _as_dedup_repo(_FakeDedupRepo(_layout())))
    workload = _make_workload()
    sentinel = [
        dataclasses.replace(
            _make_version(workload_id=1, target_type="FS"),
            meta=VersionMeta(target_meta_path="/p/x", meta_filenames=("target.db", "0_version.db.zst"), status=1),
        )
    ]
    monkeypatch.setattr(api_catalog, "versions", _async_returning(sentinel))

    assert await catalog.versions(workload) == sentinel


def test_catalog_info_delegates_to_dedup_repo() -> None:
    """``info`` lives on ``Catalog``, not ``Repository`` — genuinely
    per-catalog for object storage (each repo-id has its own marker
    file)."""
    fake_repo = _FakeDedupRepo(_layout(), info="some-repo-info")
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    catalog = _make_catalog(repo, _as_dedup_repo(fake_repo))
    info: Any = catalog.info
    assert info == "some-repo-info"


__all__: list[str] = []
