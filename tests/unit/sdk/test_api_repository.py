"""Unit tests for ``synology_apm_repo.sdk.api``'s ``Repository`` -- the
part of ``test_api.py`` (split three ways, alongside ``test_api_catalog.py``/
``test_api_session.py``, by primary subject under test) covering
``key_status``/``is_encrypted``/``set_key``, ``workload_is_supported``,
``catalogs()``/``catalog_by_id()``'s own concurrent-open/exception
posture, ``Repository.verify()``'s key gate, ``file_map_tree``/
``invalidate_directory_cache``, close/context-manager cleanup, and
``walk_human_ref()``'s first (catalog-picking) level -- Session lives in
``test_api_session.py``, Catalog's own workload/version/provider/verify
methods in ``test_api_catalog.py``. These tests fake every collaborator
at the module boundary (monkeypatching the names ``api.repository``/
``api.catalog`` imported them under) rather than standing up a real
repository on disk -- see ``tests/integration/sdk/test_api.py`` for the
real end-to-end wiring.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest

from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.api import catalog as api_catalog
from synology_apm_repo.sdk.api import repository as api_repository
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification
from synology_apm_repo.sdk.dedup.pool import INTERACTIVE_BUCKET_CACHE_SIZE
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import Finding, Stage, Symptom
from synology_apm_repo.sdk.errors import KeyMismatchError, KeyRequiredError, NotFoundError
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
    WorkloadUid,
)
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout
from synology_apm_repo.sdk.units.base import Node, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef
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
        self.close_count = 0
        # Unused by Session.discover() itself now (it probes via the
        # free function dedup.keys.probe_encrypted() against the store,
        # before any DedupRepo is opened); kept for a caller that
        # constructs a Repository directly with an explicit `encrypted=`
        # and wants this fake's own probe_encrypted() to agree with it.
        self._encrypted = encrypted

    async def close(self) -> None:
        self.closed = True
        self.close_count += 1

    async def open_file(self, path: str) -> str:
        return f"opened:{path}"

    async def probe_encrypted(self) -> bool | None:
        return self._encrypted


def _as_dedup_repo(fake: _FakeDedupRepo) -> DedupRepo:
    return cast(DedupRepo, fake)


class _FakeRaisingDedupRepo(_FakeDedupRepo):
    """Like ``_FakeDedupRepo``, but ``close()`` itself raises — for
    exercising ``Repository.set_key()``'s own "attempt every item, then
    report" posture (the reopen-succeeded-but-close-failed case)."""

    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("synthetic close failure")


def _layout(repo_root: str = "") -> RepoLayout:
    return RepoLayout(kind=RepoKind.VAULT, repo_root=repo_root)


def _repository_layout(repo_root: str = "") -> RepositoryLayout:
    """The ``RepositoryLayout``-level counterpart to ``_layout()`` — what
    ``api.Repository.__init__`` takes (a single opened bucket/vault, not
    one already-opened ``DedupRepo``). Every test here uses
    ``RepoKind.VAULT``, which ``catalog_repo_layouts()`` always resolves
    to exactly one derived ``RepoLayout``, so a single-catalog fake
    (``_FakeDedupRepo``) is always the right shape."""
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
    """Construct an ``api.Repository`` backed by ``fake``. Monkeypatches
    ``DedupRepo.open`` to return ``fake`` regardless of which derived
    ``RepoLayout`` it's called with, and fakes ``api_repository.connections``
    to return ``[]`` by default (override via ``monkeypatch.setattr(
    api_repository, "connections", ...)`` for a specific list — any test
    forcing a catalog open, even just via ``_open_catalogs.resolve(0)``,
    needs it faked too). Construction alone opens nothing — only
    ``Session._confirm_real()`` or an explicit ``catalogs()``/
    ``_open_catalogs.resolve()`` call does; a test checking only
    ``key_status``/``is_encrypted`` right after construction doesn't need
    this helper, a bare ``api.Repository(_as_object_store(_FakeStore()),
    _repository_layout(), ...)`` is enough."""
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda *a, **k: fake))
    monkeypatch.setattr(api_repository, "connections", _async_returning([]))
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
    ``dedup_repo`` -- none of this file's tests need to inspect it
    directly, only that ``Catalog`` has one."""
    return api_catalog.Catalog(
        dedup_repo,
        connection or _make_connection(),
        saas_streams=SaasStreamCache(dedup_repo),
        track=repo._track,
        require_key_verified=repo._require_key_verified,
    )


# -- KeyStatus / key_status -------------------------------------------------


def test_key_status_no_key_provided_when_encrypted_status_still_unknown() -> None:
    # ``encrypted`` left at its default (None) — the raw "never resolved"
    # case a bare Repository(...) constructor call gives; Session.discover()
    # itself never leaves this unresolved for a real caller.
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    assert repo.key_status is api.KeyStatus.NO_KEY_PROVIDED


def test_key_status_no_key_provided_when_probe_confirmed_encrypted() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    assert repo.key_status is api.KeyStatus.NO_KEY_PROVIDED


def test_key_status_not_encrypted_when_probe_confirmed_unencrypted_even_without_a_key() -> None:
    # The whole point of resolving ``encrypted`` eagerly: a repository confirmed
    # NOT encrypted must report NOT_ENCRYPTED even though no key was
    # ever given — never NO_KEY_PROVIDED, which is reserved for a
    # *confirmed*-encrypted repository (or the rare case encryption status
    # genuinely couldn't be resolved at all).
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    assert repo.key_status is api.KeyStatus.NOT_ENCRYPTED


def test_key_status_not_encrypted_for_no_encryption_key_material() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=_no_encryption_keys(), key_verification=None
    )
    assert repo.key_status is api.KeyStatus.NOT_ENCRYPTED


def test_key_status_verified_when_verification_ok() -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=keys, key_verification=verification
    )
    assert repo.key_status is api.KeyStatus.VERIFIED
    assert repo.key_verification is verification


def test_key_status_invalid_when_verification_failed() -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=keys, key_verification=verification
    )
    assert repo.key_status is api.KeyStatus.INVALID


# -- is_encrypted -------------------------------------------------------------


def test_is_encrypted_none_when_never_resolved_and_no_key_given() -> None:
    # Same "never resolved" bare-constructor case as key_status's own
    # equivalent test above — Session.discover() itself never leaves a
    # real Repository in this state.
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    assert repo.is_encrypted is None


def test_is_encrypted_true_when_probe_confirmed_encrypted() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    assert repo.is_encrypted is True


def test_is_encrypted_false_when_probe_confirmed_unencrypted() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=False
    )
    assert repo.is_encrypted is False


def test_is_encrypted_true_once_a_real_key_is_given_regardless_of_verification_outcome() -> None:
    # Handing in a non-NoEncryption key at all already implies the
    # caller believes this repository is encrypted — is_encrypted answers that
    # question, not "is the key correct" (key_status/key_verification's
    # job) — so this must read True whether the key then turns out
    # right or wrong.
    keys = _some_key("some-id-0000")
    ok_repo = api.Repository(
        _as_object_store(_FakeStore()),
        _repository_layout(),
        keys=keys,
        key_verification=KeyVerification(gcm_ok=True, vault_key=b"x" * 32),
    )
    assert ok_repo.is_encrypted is True

    bad_repo = api.Repository(
        _as_object_store(_FakeStore()),
        _repository_layout(),
        keys=keys,
        key_verification=KeyVerification(gcm_ok=False, vault_key=None),
    )
    assert bad_repo.is_encrypted is True


def test_is_encrypted_false_for_no_encryption_key_material() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=_no_encryption_keys(), key_verification=None
    )
    assert repo.is_encrypted is False


# -- set_key -----------------------------------------------------------------


async def test_set_key_wrong_key_reports_invalid_not_no_key_provided(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: a rejected key attempt must not be indistinguishable
    from "no key was ever tried" — key_status must report INVALID, not
    silently fall back to NO_KEY_PROVIDED."""
    fake_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None)
    await repo._open_catalogs.resolve(0)  # populate the "already opened" catalog set_key() swaps

    bad_verification = KeyVerification(gcm_ok=False, vault_key=None)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(bad_verification))

    result = await repo.set_key("bad-id-00000@" + _VALID_B64_KEY)

    assert result is bad_verification
    assert repo.key_status is api.KeyStatus.INVALID
    assert fake_repo.closed is False  # rejected key must not tear down the working connection


async def test_set_key_correct_key_swaps_in_new_dedup_repo_and_closes_old(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None)
    await repo._open_catalogs.resolve(0)  # populate the "already opened" catalog set_key() swaps

    good_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(good_verification))
    new_repo = _FakeDedupRepo(_layout())
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda *a, **k: new_repo))

    result = await repo.set_key("good-id-0000@" + _VALID_B64_KEY)

    assert result is good_verification
    assert repo.key_status is api.KeyStatus.VERIFIED
    assert fake_repo.closed is True
    assert (await repo._open_catalogs.resolve(0)).dedup_repo is _as_dedup_repo(new_repo)


async def test_set_key_reraises_as_exception_group_when_a_reopen_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A specific already-opened catalog's own re-open can independently
    fail (its own corrupt ``repo_info``, say) even though the bucket-wide
    key itself verified fine — set_key() must still report that failure
    (as an ``ExceptionGroup``, the same "attempt every item, then report"
    posture ``close()``/``Session.close()`` already use), not swallow it
    just because the overall key was accepted."""
    fake_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None)
    await repo._open_catalogs.resolve(0)  # populate the "already opened" catalog set_key() swaps

    good_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(good_verification))

    async def failing_open(cls: object, /, *a: object, **k: object) -> DedupRepo:
        raise KeyMismatchError("synthetic reopen failure", ref="repo_info")

    monkeypatch.setattr(DedupRepo, "open", classmethod(failing_open))

    with pytest.raises(ExceptionGroup) as exc_info:
        await repo.set_key("good-id-0000@" + _VALID_B64_KEY)
    assert isinstance(exc_info.value.exceptions[0], KeyMismatchError)
    # The key itself was still accepted (verification.ok was True) --
    # only the specific catalog's own reopen failed -- so key_status
    # still reflects the successful verification, same as every other
    # "attempt all, then report" cleanup in this codebase never undoing
    # the state change its own failures are reported alongside.
    assert repo.key_status is api.KeyStatus.VERIFIED


async def test_set_key_still_attempts_every_close_when_an_earlier_reopen_succeeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The old, now-replaced ``DedupRepo``'s own ``close()`` can
    independently fail without that abandoning ``set_key()``'s own
    reporting — same posture as the reopen-failure test above, exercised
    from the other end of the swap."""
    fake_repo = _FakeRaisingDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None)
    await repo._open_catalogs.resolve(0)  # populate the "already opened" catalog set_key() swaps

    good_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(good_verification))
    new_repo = _FakeDedupRepo(_layout())
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda *a, **k: new_repo))

    with pytest.raises(ExceptionGroup) as exc_info:
        await repo.set_key("good-id-0000@" + _VALID_B64_KEY)
    assert isinstance(exc_info.value.exceptions[0], RuntimeError)
    assert fake_repo.closed is True  # the failing close was still attempted, not skipped
    assert repo.key_status is api.KeyStatus.VERIFIED
    assert (await repo._open_catalogs.resolve(0)).dedup_repo is _as_dedup_repo(new_repo)


async def test_set_key_then_correct_key_afterwards_recovers_to_verified(monkeypatch: pytest.MonkeyPatch) -> None:
    """wrong key -> INVALID, then a correct key afterwards -> VERIFIED."""
    fake_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, keys=None, key_verification=None)
    await repo._open_catalogs.resolve(0)  # populate the "already opened" catalog set_key() swaps

    bad_verification = KeyVerification(gcm_ok=False, vault_key=None)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(bad_verification))
    await repo.set_key("bad-id-00000@" + _VALID_B64_KEY)
    status_after_bad_key = repo.key_status
    assert status_after_bad_key is api.KeyStatus.INVALID

    good_verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(good_verification))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda *a, **k: _FakeDedupRepo(_layout())))
    await repo.set_key("good-id-0000@" + _VALID_B64_KEY)
    status_after_good_key = repo.key_status
    assert status_after_good_key is api.KeyStatus.VERIFIED


class TestWorkloadIsSupported:
    """A plain, no-I/O delegator to
    ``synology_apm_repo.sdk.units.dispatch.is_supported`` — see that
    function's own tests (``tests/unit/sdk/test_units_dispatch_saas.py``)
    for the actual VM/PC/PS/FS/sub_type coverage; this just confirms
    ``Repository`` exposes it, since that's the one thing the CLI's
    ``doctor`` command (and anything else outside ``sdk.units``) is
    allowed to call instead of importing ``units.dispatch`` directly."""

    def test_recognized_sub_type_is_supported(self) -> None:
        repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
        assert repo.workload_is_supported(_make_workload()) is True  # M365/MAIL, per _make_workload's own default

    def test_unrecognized_sub_type_is_not_supported(self) -> None:
        repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
        unrecognized = dataclasses.replace(_make_workload(), workload_type="GW", sub_type="SOME_FUTURE_CONNECTOR_TYPE")
        assert repo.workload_is_supported(unrecognized) is False


# -- catalogs() / Catalog -----------------------------------------------------
#
# Repository.catalogs() wraps every connections() row as a Catalog sharing
# this Repository's own already-opened DedupRepo — unlike
# workloads()/versions() above, catalogs() itself is never gated on the key
# (connections() reads plaintext connection_config rows regardless of
# encryption), and each Catalog's
# own workloads()/versions()/provider()/verify() delegate to the exact same
# module-level helpers Repository's own methods use, so a fake swapped in at
# the module boundary is observed identically either way.


async def test_open_catalog_resources_uses_the_larger_interactive_bucket_cache_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``Repository._open_catalog_resources()`` passes an explicit, larger
    ``bucket_cache_size`` at this one call site — not the shared
    ``DEFAULT_BUCKET_CACHE_SIZE`` every other ``Pool``-constructing call
    site implicitly gets — since this is the one ``Pool`` every non-bulk
    consumer of a catalog shares."""
    captured_kwargs: dict[str, object] = {}
    fake = _FakeDedupRepo(_layout())

    def capturing_factory(*args: object, **kwargs: object) -> _FakeDedupRepo:
        captured_kwargs.update(kwargs)
        return fake

    monkeypatch.setattr(DedupRepo, "open", _async_open(capturing_factory))
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), None, None)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))

    await repo.catalogs()

    assert captured_kwargs.get("bucket_cache_size") == INTERACTIVE_BUCKET_CACHE_SIZE


async def test_open_catalog_resources_closes_the_dedup_repo_when_connections_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``connections(dedup_repo)`` raising (a corrupt ``connection_config``
    table) after ``DedupRepo.open()`` already succeeded must not leak the
    real sqlite connections that open already made -- the same
    close-what-was-already-opened posture every other partial-construction
    failure in this codebase already follows."""
    fake = _FakeDedupRepo(_layout())
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda *a, **k: fake))
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), None, None)

    async def failing_connections(dedup_repo: object) -> list[Connection]:
        raise RuntimeError("synthetic corrupt connection_config")

    monkeypatch.setattr(api_repository, "connections", failing_connections)

    with pytest.raises(RuntimeError, match="synthetic corrupt connection_config"):
        await repo.catalogs()

    assert fake.closed is True


async def test_catalogs_succeeds_even_when_key_required_and_not_yet_provided(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_fake_dedup(monkeypatch, _FakeDedupRepo(_layout()), encrypted=True)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))
    catalogs = await repo.catalogs()
    assert len(catalogs) == 1


async def test_catalogs_succeeds_even_when_a_previous_key_was_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = _repo_with_fake_dedup(monkeypatch, _FakeDedupRepo(_layout()), keys=keys, key_verification=verification)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))
    catalogs = await repo.catalogs()
    assert len(catalogs) == 1


async def test_catalogs_returns_one_catalog_per_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_fake_dedup(monkeypatch, _FakeDedupRepo(_layout()), encrypted=False)
    first = _make_connection()
    second = dataclasses.replace(first, connection_config_id=ConnectionConfigId(2), display_name="Source 2")
    monkeypatch.setattr(api_repository, "connections", _async_returning([first, second]))

    catalogs = await repo.catalogs()

    assert [c.connection for c in catalogs] == [first, second]
    assert [c.display_name for c in catalogs] == ["Source 1", "Source 2"]


async def test_catalogs_share_the_owning_repositorys_dedup_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    """The vault case: several sibling Catalogs all read through one
    physical dedup pool — Repository.catalogs() doesn't open anything new
    per connection, it just narrows the view onto what's already open."""
    fake_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, encrypted=False)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))

    (catalog,) = await repo.catalogs()

    assert catalog._dedup_repo is _as_dedup_repo(fake_repo)


async def test_catalogs_raises_instead_of_silently_dropping_a_sibling_that_fails_to_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A specific object-storage sibling's own corrupt ``repo_info`` must
    surface, not disappear a catalog from the list -- a caller must never
    mistake "one sibling is broken" for "this bucket only has N-1
    catalogs"."""
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=["good", "bad"])

    async def selective_open(
        cls: object, /, store: object, repo_layout: RepoLayout, keys: object, **kwargs: object
    ) -> DedupRepo:
        if repo_layout.repo_id == "bad":
            raise NotFoundError("synthetic corrupt repo_info", ref="repo_info")
        return _as_dedup_repo(_FakeDedupRepo(repo_layout))

    monkeypatch.setattr(DedupRepo, "open", classmethod(selective_open))
    repo = api.Repository(_as_object_store(_FakeStore()), layout, keys=None, key_verification=None, encrypted=False)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))

    with pytest.raises(NotFoundError, match="synthetic corrupt repo_info"):
        await repo.catalogs()


async def test_catalogs_reraises_cancellederror(monkeypatch: pytest.MonkeyPatch) -> None:
    """``asyncio.gather(..., return_exceptions=True)`` captures a
    cancelled task's own ``CancelledError`` the same way it captures an
    ordinary exception — ``catalogs()`` must re-raise that one too, or a
    caller cancelling this call would have its own cooperative
    cancellation swallowed."""
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=["a"])

    async def cancelled_open(
        cls: object, /, store: object, repo_layout: RepoLayout, keys: object, **kwargs: object
    ) -> DedupRepo:
        raise asyncio.CancelledError

    monkeypatch.setattr(DedupRepo, "open", classmethod(cancelled_open))
    repo = api.Repository(_as_object_store(_FakeStore()), layout, keys=None, key_verification=None, encrypted=False)

    with pytest.raises(asyncio.CancelledError):
        await repo.catalogs()


async def test_catalogs_reraises_a_non_apmrepoerror_too(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transient failure that isn't a recognized ``ApmRepoError`` (e.g.
    a storage backend's own network error) is surfaced the same as any
    other sibling-open failure -- ``catalogs()`` draws no distinction
    between error kinds, only between "opened" and "didn't"."""
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=["a"])

    async def failing_open(
        cls: object, /, store: object, repo_layout: RepoLayout, keys: object, **kwargs: object
    ) -> DedupRepo:
        raise RuntimeError("synthetic transient I/O error")

    monkeypatch.setattr(DedupRepo, "open", classmethod(failing_open))
    repo = api.Repository(_as_object_store(_FakeStore()), layout, keys=None, key_verification=None, encrypted=False)

    with pytest.raises(RuntimeError, match="synthetic transient I/O error"):
        await repo.catalogs()


async def test_catalog_by_id_never_opens_a_sibling_whose_own_repo_id_cannot_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole point of ``catalog_by_id`` over ``catalogs()``: skip a
    sibling by its own ``repo_id`` alone, no I/O at all, rather than
    opening every one just to check."""
    # "other" listed *first* so the loop must actually skip past a
    # mismatched sibling (the branch this test exists to cover), not
    # just happen to find "wanted" on its first iteration.
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=["other", "wanted"])
    opened: list[str | None] = []

    async def recording_open(
        cls: object, /, store: object, repo_layout: RepoLayout, keys: object, **kwargs: object
    ) -> DedupRepo:
        opened.append(repo_layout.repo_id)
        return _as_dedup_repo(_FakeDedupRepo(repo_layout))

    monkeypatch.setattr(DedupRepo, "open", classmethod(recording_open))
    repo = api.Repository(_as_object_store(_FakeStore()), layout, keys=None, key_verification=None, encrypted=False)
    monkeypatch.setattr(api_repository, "connections", _async_returning([_make_connection()]))

    catalog = await repo.catalog_by_id(CatalogId("wanted"))

    assert catalog is not None
    assert catalog.catalog_id == "wanted"
    assert opened == ["wanted"]  # "other" never opened at all


async def test_catalog_by_id_raises_when_a_candidate_it_cannot_rule_out_fails_to_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``catalog_by_id`` only ever opens siblings it can't rule out by
    ``repo_id`` alone -- when the *one* genuinely-ambiguous entry
    (``repo_id`` unset) fails to open, that failure is surfaced, not
    treated as "not it, keep looking" (which would silently report
    "not found" for a catalog that's actually broken, not absent)."""
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=None)

    async def failing_open(
        cls: object, /, store: object, repo_layout: RepoLayout, keys: object, **kwargs: object
    ) -> DedupRepo:
        raise NotFoundError("synthetic corrupt repo_info", ref="repo_info")

    monkeypatch.setattr(DedupRepo, "open", classmethod(failing_open))
    repo = api.Repository(_as_object_store(_FakeStore()), layout, keys=None, key_verification=None, encrypted=False)

    with pytest.raises(NotFoundError, match="synthetic corrupt repo_info"):
        await repo.catalog_by_id(CatalogId("anything"))


# -- verify()'s own key gate ---------------------------------------------
#
# catalog.versions()'s own browsable-status filter silently drops every
# row it can't decrypt version_spec for -- indistinguishable, from inside
# that filter, from a genuinely non-browsable version. Without this gate,
# verify() against an encrypted repository with no/wrong key would walk zero
# versions and report a misleadingly clean result instead of refusing to
# run -- same "workloads()/versions() gate" shape as the tests above.


async def test_repository_verify_raises_key_required_when_no_key_provided() -> None:
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None, encrypted=True
    )
    with pytest.raises(KeyRequiredError):
        await repo.verify()


async def test_repository_verify_raises_key_mismatch_when_key_invalid() -> None:
    keys = _some_key("some-id-0000")
    verification = KeyVerification(gcm_ok=False, vault_key=None)
    repo = api.Repository(
        _as_object_store(_FakeStore()), _repository_layout(), keys=keys, key_verification=verification
    )
    with pytest.raises(KeyMismatchError):
        await repo.verify()


async def test_repository_verify_succeeds_when_not_encrypted(monkeypatch: pytest.MonkeyPatch) -> None:
    """The pass-through branch of ``Repository.verify()``'s own gate --
    proven via the real reachability walk being reached at all (a fake
    ``verify_reachable`` called once per opened ``DedupRepo``), not
    just the absence of a raised exception."""
    fake_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_repo, encrypted=False)
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
    repo = _repo_with_fake_dedup(monkeypatch, _FakeDedupRepo(_layout()))
    sentinel = object()
    monkeypatch.setattr(api_repository, "FileMapTreeProvider", lambda r: sentinel)
    assert await repo.file_map_tree() is sentinel


async def test_invalidate_directory_cache_delegates_to_the_underlying_dedup_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A thin facade delegation to ``DedupRepo.dir_cache.invalidate()``
    — ``DirCache``'s own real re-scan-on-next-listing behavior is tested
    directly (test_storage_dircache.py); this only proves ``Repository``
    actually reaches it rather than being a silent no-op at this layer."""
    fake_dedup_repo = _FakeDedupRepo(_layout())
    calls: list[int] = []

    class _FakeDirCache:
        async def invalidate(self) -> None:
            calls.append(1)

    fake_dedup_repo.dir_cache = _FakeDirCache()  # type: ignore[attr-defined]
    repo = _repo_with_fake_dedup(monkeypatch, fake_dedup_repo)
    await repo._open_catalogs.resolve(0)  # only an already-opened catalog's dir_cache gets invalidated

    await repo.invalidate_directory_cache()

    assert calls == [1]


# -- Repository as context manager / close -----------------------------------


async def test_repository_context_manager_closes_underlying_dedup_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_repo = _FakeDedupRepo(_layout())
    # ``Repository`` only implements ``__aenter__``/``__aexit__``, not the sync pair.
    async with _repo_with_fake_dedup(monkeypatch, fake_repo) as repo:
        assert repo is not None
        await repo._open_catalogs.resolve(0)  # only an already-opened catalog gets closed
    assert fake_repo.closed is True


class _FakeClosableProvider:
    """A minimal ``ClosableUnitProvider`` — the protocol is
    ``@runtime_checkable``, so ``Repository.close()``'s own
    ``isinstance(provider, ClosableUnitProvider)`` check only cares that
    every one of these names (including ``__aenter__``/``__aexit__``) is
    present, not their real behavior."""

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

    async def __aenter__(self) -> _FakeClosableProvider:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


async def test_close_closes_every_tracked_closable_provider_not_just_the_last_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``Repository._track`` records every provider handed out by
    ``provider()``/``file_map_tree()`` so ``close()`` can release each
    one's own sqlite connection (``aiosqlite`` dedicates a non-daemon
    background thread to each connection's whole lifetime, so a leaked
    one blocks interpreter shutdown forever). Two separate
    ``provider()`` calls in one session (e.g. two different versions
    browsed in the same TUI session) must both get closed, not just
    whichever was tracked last."""
    fake_dedup_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_dedup_repo)
    catalog = _make_catalog(repo, (await repo._open_catalogs.resolve(0)).dedup_repo)
    first, second = _FakeClosableProvider(), _FakeClosableProvider()
    remaining = [first, second]

    async def fake_provider_for(r: object, v: object) -> object:
        return remaining.pop(0)

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    version_a = _make_version(workload_id=1, target_type="VM")
    version_b = _make_version(workload_id=2, target_type="VM")
    assert await catalog.provider(version_a) is first
    assert await catalog.provider(version_b) is second

    await repo.close()

    assert first.closed is True
    assert second.closed is True
    assert fake_dedup_repo.closed is True


async def test_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second ``close()`` call is a no-op -- a caller that already
    closed this repository explicitly (to release its resources ahead of
    ``Session.close()``'s own later sweep over every repository it
    yielded, say) must not pay for a second, redundant close of every
    already-closed provider/``DedupRepo``."""
    fake_dedup_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_dedup_repo)
    catalog = _make_catalog(repo, (await repo._open_catalogs.resolve(0)).dedup_repo)
    provider = _FakeClosableProvider()

    async def fake_provider_for(r: object, v: object) -> object:
        return provider

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)
    await catalog.provider(_make_version(workload_id=1, target_type="VM"))

    await repo.close()
    await repo.close()

    assert fake_dedup_repo.close_count == 1


class _FakeRaisingClosableProvider(_FakeClosableProvider):
    """Like ``_FakeClosableProvider``, but ``close()`` itself raises —
    for exercising ``Repository.close()``'s "attempt every item, then
    report" posture."""

    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("synthetic close failure")


async def test_close_still_closes_every_remaining_item_when_an_earlier_one_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A leaked aiosqlite connection blocks interpreter exit forever —
    one tracked provider's ``close()`` raising must not abandon closing
    every later item (including ``self._dedup_repo`` itself), only
    report the failure once everything has been attempted."""
    fake_dedup_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_dedup_repo)
    catalog = _make_catalog(repo, (await repo._open_catalogs.resolve(0)).dedup_repo)
    first, second = _FakeRaisingClosableProvider(), _FakeClosableProvider()
    remaining = [first, second]

    async def fake_provider_for(r: object, v: object) -> object:
        return remaining.pop(0)

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    version_a = _make_version(workload_id=1, target_type="VM")
    version_b = _make_version(workload_id=2, target_type="VM")
    assert await catalog.provider(version_a) is first
    assert await catalog.provider(version_b) is second

    with pytest.raises(ExceptionGroup) as exc_info:
        await repo.close()

    assert first.closed is True
    assert second.closed is True  # still closed despite first's failure
    assert fake_dedup_repo.closed is True  # still closed despite first's failure
    assert len(exc_info.value.exceptions) == 1
    assert isinstance(exc_info.value.exceptions[0], RuntimeError)


class _FakeHangingClosableProvider(_FakeClosableProvider):
    """Like ``_FakeClosableProvider``, but ``close()`` never returns —
    for exercising ``_RESOURCE_CLOSE_TIMEOUT``'s own bound, distinct from
    ``_FakeRaisingClosableProvider``'s "raises promptly" failure mode: a
    stuck connection doesn't raise at all, it just never completes."""

    async def close(self) -> None:
        await asyncio.Event().wait()  # never set -- hangs forever unless bounded


async def test_close_does_not_hang_forever_when_one_providers_close_hangs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tracked provider whose own ``close()`` *hangs* instead of
    raising (a stuck server, a race like ``storage/smb.py``'s own
    ``_drop_slot_session`` one) must not block every other tracked resource's
    own close attempt forever, and the interpreter along with it.
    ``_RESOURCE_CLOSE_TIMEOUT`` is monkeypatched down so this test itself doesn't take the real
    10 seconds to prove it."""
    monkeypatch.setattr(api_repository, "_RESOURCE_CLOSE_TIMEOUT", 0.05)
    fake_dedup_repo = _FakeDedupRepo(_layout())
    repo = _repo_with_fake_dedup(monkeypatch, fake_dedup_repo)
    catalog = _make_catalog(repo, (await repo._open_catalogs.resolve(0)).dedup_repo)
    first, second = _FakeHangingClosableProvider(), _FakeClosableProvider()
    remaining = [first, second]

    async def fake_provider_for(r: object, v: object) -> object:
        return remaining.pop(0)

    monkeypatch.setattr(api_catalog, "provider_for", fake_provider_for)

    version_a = _make_version(workload_id=1, target_type="VM")
    version_b = _make_version(workload_id=2, target_type="VM")
    assert await catalog.provider(version_a) is first
    assert await catalog.provider(version_b) is second

    with pytest.raises(ExceptionGroup) as exc_info:
        await asyncio.wait_for(repo.close(), timeout=5.0)  # the outer bound this test itself enforces

    assert second.closed is True  # still closed despite first hanging
    assert fake_dedup_repo.closed is True  # still closed despite first hanging
    assert len(exc_info.value.exceptions) == 1
    assert isinstance(exc_info.value.exceptions[0], TimeoutError)


async def test_close_also_closes_a_dedup_catalog_that_finishes_opening_after_close_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``catalogs()``/``verify()`` call already in flight (its own open
    not yet settled) when ``close()`` runs must still have its eventual
    ``DedupRepo`` closed once it lands, not leaked -- ``.values()`` alone
    only sees already-settled entries, so ``close()`` uses
    ``known_keys()``'s broader view (settled plus in-flight) to catch a
    fetch that a racing caller already started."""
    fake_repo = _FakeDedupRepo(_layout())
    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_open(cls: object, /, *a: object, **k: object) -> DedupRepo:
        started.set()
        await release.wait()
        return _as_dedup_repo(fake_repo)

    monkeypatch.setattr(DedupRepo, "open", classmethod(_slow_open))
    monkeypatch.setattr(api_repository, "connections", _async_returning([]))
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), None, None)

    resolve_task = asyncio.create_task(repo._open_catalogs.resolve(0))
    await started.wait()  # the open is in flight (owner determined), not yet settled

    close_task = asyncio.create_task(repo.close())
    await asyncio.sleep(0)  # let close() take its known_keys() snapshot while index 0 is still in flight
    release.set()  # let the open finish

    await resolve_task
    await close_task

    assert fake_repo.closed is True


async def test_close_reports_but_does_not_abort_when_an_in_flight_open_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same in-flight race as the test above, but the open itself fails
    once it settles -- ``close()``'s own ``resolve(index)`` re-raises
    that failure (it isn't the owner, just a second waiter on the same
    future) and must report it via the ``ExceptionGroup``, not let it
    escape uncaught or abort closing anything else."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_failing_open(cls: object, /, *a: object, **k: object) -> DedupRepo:
        started.set()
        await release.wait()
        raise RuntimeError("synthetic open failure")

    monkeypatch.setattr(DedupRepo, "open", classmethod(_slow_failing_open))
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), None, None)

    resolve_task = asyncio.create_task(repo._open_catalogs.resolve(0))
    await started.wait()

    close_task = asyncio.create_task(repo.close())
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(RuntimeError, match="synthetic open failure"):
        await resolve_task
    with pytest.raises(ExceptionGroup) as exc_info:
        await close_task
    assert len(exc_info.value.exceptions) == 1
    assert isinstance(exc_info.value.exceptions[0], RuntimeError)


# -- Repository.walk_human_ref() ---------------------------------------------


class _FakeWalkProvider:
    """A minimal ``UnitProvider`` for ``api.Repository.walk_human_ref``:
    ``walk_human_ref`` never calls ``unit()`` (it hands back the raw
    ``Node`` it stopped at; only ``api.Repository.resolve`` converts a
    leaf via ``unit()``), so this fake only needs ``root()``/``children()``."""

    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]]) -> None:
        self._root = root
        self._children_by_ref = children_by_ref

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children_by_ref.get(str(node.ref), [])


def _repo_with_catalog_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    connections: list[Connection] | None = None,
    workloads: list[Workload] | None = None,
    versions: list[Version] | None = None,
    provider: _FakeWalkProvider | None = None,
) -> api.Repository:
    """Fakes every module-level collaborator ``Repository.walk_human_ref``/
    ``Catalog.walk_human_ref`` reach through (``connections()``/
    ``workloads()``/``versions()``/``provider_for()``) — ``Repository`` has
    no equivalent instance methods to monkeypatch directly. Every version
    built by these tests is ``"VM"`` (a device/fs target type), so faking
    ``provider_for`` alone is enough to control what ``Catalog.provider()``
    returns."""
    repo = _repo_with_fake_dedup(monkeypatch, _FakeDedupRepo(_layout()))
    monkeypatch.setattr(api_repository, "connections", _async_returning(connections or []))
    monkeypatch.setattr(api_catalog, "workloads", _async_returning(workloads or []))
    monkeypatch.setattr(api_catalog, "versions", _async_returning(versions or []))
    if provider is not None:
        monkeypatch.setattr(api_catalog, "provider_for", _async_returning(provider))
    return repo


async def test_walk_human_ref_empty_segments_returns_root_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_catalog_stubs(monkeypatch)
    frame = await repo.walk_human_ref(())
    assert frame.level == "root"


async def test_walk_human_ref_one_segment_returns_catalog_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _make_connection()
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[connection])
    frame = await repo.walk_human_ref((connection.display_name,))
    assert frame.level == "catalog"
    assert frame.catalog is not None
    assert frame.catalog.connection is connection


async def test_walk_human_ref_unknown_catalog_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[_make_connection()])
    with pytest.raises(NotFoundError, match="no backup source named"):
        await repo.walk_human_ref(("NoSuchSource",))


async def test_walk_human_ref_two_segments_returns_workload_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _make_connection()
    workload = _make_workload()
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[connection], workloads=[workload])
    frame = await repo.walk_human_ref((connection.display_name, workload.display_name))
    assert frame.level == "workload"
    assert frame.workload is workload


async def test_walk_human_ref_unknown_workload_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _make_connection()
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[connection], workloads=[_make_workload()])
    with pytest.raises(NotFoundError, match="no workload named"):
        await repo.walk_human_ref((connection.display_name, "NoSuchWorkload"))


async def test_walk_human_ref_ambiguous_workload_raises_not_found_with_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Two workloads sharing one raw display_name, same sub_type (so the
    # hint can't resolve the collision either) -- the bare name is a real
    # collision, not a genuine miss, and the message must say so instead
    # of the generic "no workload named" (see _match_or_raise).
    connection = _make_connection()
    dup_a = dataclasses.replace(_make_workload(workload_id=1), workload_uid=WorkloadUid("wl-a"))
    dup_b = dataclasses.replace(_make_workload(workload_id=2), workload_uid=WorkloadUid("wl-b"))
    repo = _repo_with_catalog_stubs(monkeypatch, connections=[connection], workloads=[dup_a, dup_b])
    with pytest.raises(NotFoundError, match="workload named 'Some User' is ambiguous \\(2 matches\\) — use one of:"):
        await repo.walk_human_ref((connection.display_name, dup_a.display_name))


async def test_walk_human_ref_three_segments_returns_node_frame_at_provider_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _make_connection()
    workload = _make_workload()
    version = _make_version(workload_id=1, target_type="VM")
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    provider = _FakeWalkProvider(root_node, {})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    frame = await repo.walk_human_ref((connection.display_name, workload.display_name, version.display_name))
    assert frame.level == "node"
    assert frame.node is root_node
    assert cast(Any, frame.provider) is provider


async def test_walk_human_ref_unknown_version_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, workload = _make_connection(), _make_workload()
    repo = _repo_with_catalog_stubs(
        monkeypatch,
        connections=[connection],
        workloads=[workload],
        versions=[_make_version(workload_id=1, target_type="VM")],
    )
    with pytest.raises(NotFoundError, match="no version named"):
        await repo.walk_human_ref((connection.display_name, workload.display_name, "NoSuchVersion"))


async def test_walk_human_ref_descends_into_provider_tree_by_name(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, workload = _make_connection(), _make_workload()
    version = _make_version(workload_id=1, target_type="VM")
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    child_node = Node(ref=NodeRef("", ("root", "child")), name="Child", is_leaf=True)
    provider = _FakeWalkProvider(root_node, {str(root_node.ref): [child_node]})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    frame = await repo.walk_human_ref((connection.display_name, workload.display_name, version.display_name, "Child"))
    assert frame.level == "node"
    assert frame.node is child_node


async def test_walk_human_ref_unknown_item_name_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, workload = _make_connection(), _make_workload()
    version = _make_version(workload_id=1, target_type="VM")
    root_node = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=False)
    provider = _FakeWalkProvider(root_node, {str(root_node.ref): []})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    with pytest.raises(NotFoundError, match="no item named"):
        await repo.walk_human_ref((connection.display_name, workload.display_name, version.display_name, "NoSuchChild"))


async def test_walk_human_ref_past_a_leaf_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, workload = _make_connection(), _make_workload()
    version = _make_version(workload_id=1, target_type="VM")
    leaf_root = Node(ref=NodeRef("", ("root",)), name="root", is_leaf=True)
    provider = _FakeWalkProvider(leaf_root, {})
    repo = _repo_with_catalog_stubs(
        monkeypatch, connections=[connection], workloads=[workload], versions=[version], provider=provider
    )
    with pytest.raises(NotFoundError, match="more levels than the tree has"):
        await repo.walk_human_ref((connection.display_name, workload.display_name, version.display_name, "too-deep"))


__all__: list[str] = []
