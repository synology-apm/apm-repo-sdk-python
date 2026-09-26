"""Unit tests for ``synology_apm_repo.sdk.api``'s ``Session`` -- the part
of ``test_api.py`` (split three ways, alongside
``test_api_repository.py``/``test_api_catalog.py``, by primary subject
under test) covering discover/open/cancel/progress, close, the
context-manager, ``_resolve_key_verification``, and ``Session.resolve()``
(picking the right already-open ``Repository`` among several, then
delegating into its own ``resolve()``/``walk_human_ref()`` -- exercised
here through the real fake-tree fixtures since that delegation is
exactly what this level adds over ``Repository.resolve()`` alone). These
tests fake every collaborator at the module boundary (monkeypatching the
names ``api.session``/``api.repository``/``api.catalog`` imported them
under) rather than standing up a real repository on disk -- see
``tests/integration/sdk/test_api.py`` for the real end-to-end wiring.
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
from synology_apm_repo.sdk.api import session as api_session
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import ApmRepoError, NotFoundError
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
from synology_apm_repo.sdk.presentation.progress import Progress
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout, catalog_repo_layouts
from synology_apm_repo.sdk.units.base import Node, RestorableUnit, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef


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


class _FakeStoreWithAclose(_FakeStore):
    """``_FakeStore`` plus a real ``aclose()`` — every ``close_repo()``/
    ``close()`` test below that needs to observe whether (and how many
    times) a store actually got closed shares this one definition rather
    than each redeclaring an identical nested class; ``aclose_calls`` is
    harmless overhead for a test that only ever checks ``closed``."""

    def __init__(self) -> None:
        self.closed = False
        self.aclose_calls = 0

    async def aclose(self) -> None:
        self.closed = True
        self.aclose_calls += 1


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
    ``RepoLayout`` it's called with. Construction alone opens nothing —
    only ``Session._confirm_real()`` or an explicit ``catalogs()``/
    ``_open_catalogs.resolve()`` call does; a test checking only
    ``key_status``/``is_encrypted`` right after construction doesn't need
    this helper, a bare ``api.Repository(_as_object_store(_FakeStore()),
    _repository_layout(), ...)`` is enough."""
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


# -- Session.discover / open / cancel / progress -----------------------------


async def test_session_discover_yields_one_repository_per_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    layouts = [_repository_layout("a"), _repository_layout("b")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(api_session, "iter_repository_layouts", _async_iter_repository_layouts(layouts))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    session = api.Session()
    repos = await session.open("/some/path")

    assert len(repos) == 2
    assert [r.layout.repo_root for r in repos] == ["a", "b"]
    assert session._repos == repos


async def test_session_discover_yields_a_vault_whose_catalog_fails_to_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Repository._confirm_real()`` trusts ``iter_repository_layouts``'s
    own marker check for ``VAULT`` — a vault whose one catalog
    would fail to open is still yielded by ``discover()``/``open()``, the
    same as an ``OBJECT_STORE`` bucket whose siblings' own corrupt
    ``repo_info`` isn't caught at discovery either. The failure surfaces
    later instead, scoped to that repository's own ``catalogs()`` call,
    which raises it."""
    layouts = [_repository_layout("looks-fine-but-corrupt")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(api_session, "iter_repository_layouts", _async_iter_repository_layouts(layouts))

    async def failing_open(
        cls: object, /, store: object, layout: RepoLayout, keys: object, **kwargs: object
    ) -> DedupRepo:
        raise ApmRepoError("corrupt repo_info")

    monkeypatch.setattr(DedupRepo, "open", classmethod(failing_open))

    repos = await api.Session().open("/some/path")

    assert len(repos) == 1
    with pytest.raises(ApmRepoError, match="corrupt repo_info"):
        await repos[0].catalogs()


async def test_session_discover_skips_an_object_store_layout_with_no_valid_catalog_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one case ``_confirm_real()`` still actually rejects a layout
    for: a real, listable ``@ActiveProtectData`` with zero valid repo-id
    children (``catalog_ids == []``) — the ``OBJECT_STORE`` half of
    ``_confirm_real()``'s own contract, unaffected by ``VAULT``'s own
    change above."""
    layouts = [
        RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="empty-bucket", catalog_ids=[]),
        _repository_layout("good"),
    ]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(api_session, "iter_repository_layouts", _async_iter_repository_layouts(layouts))

    repos = await api.Session().open("/some/path")

    assert len(repos) == 1
    assert repos[0].layout.repo_root == "good"


async def test_session_discover_skips_a_layout_whose_key_probe_itself_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression test: an ``ApmRepoError`` raised by the key/encryption
    probe (e.g. a half-written vault's corrupt ``db/vault_encryption_key``
    -- see ``dedup.keys``'s own ``DataCorruptError`` wrapping) must be skipped
    the same way a ``DedupRepo.open()`` failure already is, not
    propagate out of ``_open_repository`` and abort the whole scan."""
    layouts = [_repository_layout("bad"), _repository_layout("good")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(api_session, "iter_repository_layouts", _async_iter_repository_layouts(layouts))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    async def fake_probe_encrypted(store: object, layout: RepoLayout) -> bool | None:
        if layout.repo_root == "bad":
            raise ApmRepoError("synthetic corrupt vault_encryption_key")
        return False

    monkeypatch.setattr(api_session, "_probe_encrypted", fake_probe_encrypted)

    repos = await api.Session().open("/some/path")

    assert len(repos) == 1
    assert repos[0].layout.repo_root == "good"


async def test_session_discover_reports_progress_with_found_count(monkeypatch: pytest.MonkeyPatch) -> None:
    layouts = [_repository_layout("a"), _repository_layout("b")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(api_session, "iter_repository_layouts", _async_iter_repository_layouts(layouts))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    seen: list[Progress] = []

    # ``progress`` is ``Callable[[Progress], Awaitable[None]]`` and is
    # awaited, so a bare ``list.append`` doesn't satisfy the contract.
    async def record(p: Progress) -> None:
        seen.append(p)

    [_ async for _ in api.Session().discover("/some/path", progress=record)]

    assert [p.found for p in seen] == [1, 2]
    assert all(p.phase == "discovering" and not p.determinate for p in seen)


async def test_session_discover_cancellation_aborts_the_scan_partway(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cancellation propagates as ``asyncio.CancelledError`` at the next
    ``await`` — a cancelled scan stops partway instead of draining every
    layout. Synchronizes on "a" reaching ``seen`` (set inside ``drain()``),
    not on ``iter_layouts`` merely being asked for its next layout —
    ``_discover_from_store``'s internal ``FIRST_COMPLETED`` race gives no
    guaranteed order between finding "b" and delivering "a"."""
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    async def fake_iter_repository_layouts(store: object, root: str = "") -> AsyncIterator[RepositoryLayout]:
        yield _repository_layout("a")
        await asyncio.sleep(3600)  # the cancellation lands on this await
        yield _repository_layout("b")

    monkeypatch.setattr(api_session, "iter_repository_layouts", fake_iter_repository_layouts)

    seen: list[api.Repository] = []
    got_first = asyncio.Event()

    async def drain() -> None:
        # Deliberately incremental (not a comprehension): the point is that
        # ``seen`` holds what was yielded *before* the cancellation landed.
        async for repo in api.Session().discover("/some/path"):
            seen.append(repo)
            got_first.set()

    task = asyncio.create_task(drain())
    await got_first.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert [r.layout.repo_root for r in seen] == ["a"]


async def test_session_discover_resolves_key_verification_when_key_given(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(verification))

    [repo] = await api.Session().open("/some/path", key=f"some-id-0000@{_VALID_B64_KEY}")

    assert repo.key_verification is verification
    assert repo.key_status is api.KeyStatus.VERIFIED


async def test_session_discover_resolves_encrypted_status_eagerly_when_no_key_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``key_status`` must already be ``NOT_ENCRYPTED`` right out of
    ``discover()``, with zero key given. ``_probe_encrypted`` (called by
    ``_open_repository`` before any ``DedupRepo`` opens) is faked directly
    here rather than via ``DedupRepo.open``/``_FakeDedupRepo``."""
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(api_session, "_probe_encrypted", _async_returning(False))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    [repo] = await api.Session().open("/some/path")

    assert repo.key_status is api.KeyStatus.NOT_ENCRYPTED


async def test_session_discover_reports_no_key_provided_when_probe_confirms_encrypted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(api_session, "_probe_encrypted", _async_returning(True))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    [repo] = await api.Session().open("/some/path")

    assert repo.key_status is api.KeyStatus.NO_KEY_PROVIDED


async def test_session_discover_skips_the_probe_entirely_when_a_key_is_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cost-avoidance half of the same mechanism: once a key is given,
    key_verification alone already fully answers key_status, so the
    eager probe must not run at all — a second, wasted read per repository."""
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", _async_returning(verification))

    probed = False

    async def _fail_if_called(store: object, layout: object) -> bool | None:
        nonlocal probed
        probed = True
        return None

    monkeypatch.setattr(api_session, "_probe_encrypted", _fail_if_called)

    await api.Session().open("/some/path", key=f"some-id-0000@{_VALID_B64_KEY}")

    assert probed is False


async def test_session_discover_remote_uses_the_given_store_directly(monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole point of ``discover_remote`` vs ``discover``: no
    ``LocalFsStore`` is ever built from a path — ``iter_layouts`` runs
    against exactly the store the caller (e.g. the TUI's connect dialog,
    already holding a constructed ``S3Store``/``AzureStore``) handed in."""
    layouts = [_repository_layout("a"), _repository_layout("b")]
    monkeypatch.setattr(api_session, "iter_repository_layouts", _async_iter_repository_layouts(layouts))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    store = _as_object_store(_FakeStore())
    repos = await api.Session().open_remote(store)

    assert len(repos) == 2
    assert [r.layout.repo_root for r in repos] == ["a", "b"]


async def test_session_discover_remote_registers_the_store_for_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """``discover_remote`` takes ownership of ``store`` the same way
    ``discover`` already does its own ``LocalFsStore`` — tracked in
    ``_stores`` so ``close()`` can ``aclose()`` it: ``S3Store``/``AzureStore``
    own an ``aiohttp`` connector that must be released (local stores have
    no ``aclose()`` and are skipped)."""
    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    store = _as_object_store(_FakeStore())
    session = api.Session()
    await session.open_remote(store)

    assert store in session._stores


async def test_session_discover_remote_reports_progress_and_resolves_key_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``discover_remote`` shares its entire body with ``discover`` past
    store construction — one spot check (progress + eager key-status
    resolution) that the shared ``_discover_from_store`` core is really
    being reused, not a parallel reimplementation that could drift."""
    monkeypatch.setattr(
        api_session,
        "iter_repository_layouts",
        _async_iter_repository_layouts([_repository_layout("a"), _repository_layout("b")]),
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    seen: list[Progress] = []

    async def record(p: Progress) -> None:
        seen.append(p)

    repos = [repo async for repo in api.Session().discover_remote(_as_object_store(_FakeStore()), progress=record)]

    assert [p.found for p in seen] == [1, 2]
    # _FakeStore.exists() -> False short-circuits _probe_encrypted() to
    # None, resolved eagerly here (no key given) into
    # KeyStatus.NO_KEY_PROVIDED — that's the correct "confirmed encrypted,
    # or the rare couldn't-tell" state, not a guess.
    assert all(repo.key_status is api.KeyStatus.NO_KEY_PROVIDED for repo in repos)


async def test_session_close_closes_all_repos_and_clears_the_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unlike ``test_session_context_manager_closes_on_exit`` (one repo, via
    ``async with``), this proves ``close()`` itself closes *every* repository a
    single ``open()`` call discovered and empties ``_repos`` — the only test
    covering that plural case."""
    layouts = [_repository_layout("a"), _repository_layout("b")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(api_session, "iter_repository_layouts", _async_iter_repository_layouts(layouts))
    fakes: list[_FakeDedupRepo] = []

    def make_fake(store: object, layout: RepoLayout, keys: object, **kwargs: object) -> _FakeDedupRepo:
        fake = _FakeDedupRepo(layout)
        fakes.append(fake)
        return fake

    monkeypatch.setattr(DedupRepo, "open", _async_open(make_fake))
    monkeypatch.setattr(api_repository, "connections", _async_returning([]))

    session = api.Session()
    repos = await session.open("/some/path")
    for repo in repos:
        await repo._open_catalogs.resolve(0)  # force VAULT open explicitly, mirroring real use
    await session.close()

    assert fakes  # sanity: the resolves above actually opened something
    assert all(f.closed for f in fakes)
    assert session._repos == []


class _FakeRaisingDedupRepo(_FakeDedupRepo):
    """Like ``_FakeDedupRepo``, but ``close()`` itself raises — for
    exercising ``Session.close()``'s "attempt every item, then report"
    posture."""

    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("synthetic close failure")


async def test_session_close_still_closes_every_remaining_repo_when_an_earlier_one_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One repository's ``close()`` raising (surfacing here as
    ``Repository.close()``'s own ``ExceptionGroup``) must not abandon
    closing every other repository the same ``open()`` call discovered."""
    layouts = [_repository_layout("a"), _repository_layout("b")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(api_session, "iter_repository_layouts", _async_iter_repository_layouts(layouts))
    fakes: list[_FakeDedupRepo] = []
    first_repo_root = layouts[0].repo_root

    def make_fake(store: object, layout: RepoLayout, keys: object, **kwargs: object) -> _FakeDedupRepo:
        fake = _FakeRaisingDedupRepo(layout) if layout.repo_root == first_repo_root else _FakeDedupRepo(layout)
        fakes.append(fake)
        return fake

    monkeypatch.setattr(DedupRepo, "open", _async_open(make_fake))
    monkeypatch.setattr(api_repository, "connections", _async_returning([]))

    session = api.Session()
    repos = await session.open("/some/path")
    for repo in repos:
        await repo._open_catalogs.resolve(0)  # force VAULT open explicitly, mirroring real use

    with pytest.raises(ExceptionGroup):
        await session.close()

    assert fakes  # sanity: the resolves above actually opened something
    assert all(f.closed for f in fakes)  # every repository still closed despite the first one's failure
    assert session._repos == []


async def test_session_context_manager_closes_on_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    fake = _FakeDedupRepo(_layout("a"))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: fake))
    monkeypatch.setattr(api_repository, "connections", _async_returning([]))

    # ``Session`` only implements ``__aenter__``/``__aexit__``, not the sync pair.
    async with api.Session() as session:
        (repo,) = await session.open("/some/path")
        await repo._open_catalogs.resolve(0)  # force VAULT open explicitly, mirroring real use
    assert fake.closed is True


async def test_session_close_acloses_stores_that_have_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """The other close() test above uses a plain ``_FakeStore`` (no
    ``aclose`` — the shape a real ``LocalFsStore`` has), so
    ``getattr(store, "aclose", None)``'s "found one" branch never runs.
    S3/AzureStore are the real callers of this branch; a store with an
    ``aclose`` stands in for either without needing a real backend."""

    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    store = _FakeStoreWithAclose()
    session = api.Session()
    await session.open_remote(_as_object_store(store))
    await session.close()

    assert store.closed is True


async def test_session_close_still_acloses_every_remaining_store_when_an_earlier_one_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One store's ``aclose()`` raising must not abandon closing every
    other tracked store — same "attempt every item, then report"
    posture as the repository-closing loop above."""

    class _FakeRaisingStoreWithAclose(_FakeStore):
        def __init__(self) -> None:
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True
            raise RuntimeError("synthetic aclose failure")

    monkeypatch.setattr(
        api_session,
        "iter_repository_layouts",
        _async_iter_repository_layouts([_repository_layout("a"), _repository_layout("b")]),
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    first_store, second_store = _FakeRaisingStoreWithAclose(), _FakeStoreWithAclose()
    session = api.Session()
    await session.open_remote(_as_object_store(first_store))
    await session.open_remote(_as_object_store(second_store))

    with pytest.raises(ExceptionGroup):
        await session.close()

    assert first_store.closed is True
    assert second_store.closed is True  # still aclosed despite the first store's failure


async def test_session_close_repo_closes_and_forgets_it_and_acloses_unshared_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``close_repo()`` on the one repository a store yielded closes both
    the repository and (unlike a bare ``repo.close()``) its own store, and
    removes both from this session's own bookkeeping."""

    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    fake_dedup = _FakeDedupRepo(_layout("a"))
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: fake_dedup))
    monkeypatch.setattr(api_repository, "connections", _async_returning([]))

    store = _FakeStoreWithAclose()
    session = api.Session()
    (repo,) = await session.open_remote(_as_object_store(store))
    await repo._open_catalogs.resolve(0)  # force VAULT open explicitly, mirroring real use

    await session.close_repo(repo)

    assert fake_dedup.closed is True
    assert repo not in session._repos
    assert cast(object, store) not in session._stores
    assert store.closed is True


async def test_session_close_repo_keeps_a_shared_store_open_until_every_sharing_repo_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two repositories yielded by one ``open()`` call share one store --
    closing one must not release it while the other is still tracked."""

    store = _FakeStoreWithAclose()
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: store)
    monkeypatch.setattr(
        api_session,
        "iter_repository_layouts",
        _async_iter_repository_layouts([_repository_layout("a"), _repository_layout("b")]),
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    session = api.Session()
    repo_a, repo_b = await session.open("/some/path")

    await session.close_repo(repo_a)
    assert store.closed is False
    assert cast(object, store) in session._stores
    assert repo_a not in session._repos
    assert repo_b in session._repos

    await session.close_repo(repo_b)
    assert store.closed is True
    assert cast(object, store) not in session._stores


async def test_session_close_repo_is_safe_when_the_caller_already_closed_the_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirrors a caller (``BrowseScreen``, before it switched to
    ``close_repo()``) closing a repository directly before handing it here --
    ``close_repo()`` must still release the store and its own bookkeeping
    even though the repository itself is already closed (``Repository.close()``
    is idempotent, so a second close is a no-op)."""

    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    store = _FakeStoreWithAclose()
    session = api.Session()
    (repo,) = await session.open_remote(_as_object_store(store))
    await repo.close()  # caller already closed it directly

    await session.close_repo(repo)

    assert store.closed is True
    assert repo not in session._repos
    assert cast(object, store) not in session._stores


async def test_session_close_repo_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling ``close_repo()`` twice on the same repository must not raise
    or ``aclose()`` its store a second time."""

    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    store = _FakeStoreWithAclose()
    session = api.Session()
    (repo,) = await session.open_remote(_as_object_store(store))

    await session.close_repo(repo)
    await session.close_repo(repo)  # second call: a no-op, not a double-aclose

    assert store.aclose_calls == 1


async def test_session_close_repo_treats_two_traced_discovers_of_one_backing_store_as_shared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_discover_from_store`` wraps ``store`` in a new ``TracingStore`` on
    every call when ``trace=`` is given — two separate ``discover_remote()``
    calls against the same backing store must still be recognized as
    sharing one backing connector, or ``close_repo()`` would ``aclose()``
    it out from under whichever group closes second."""

    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    def _no_trace(event: api_session.TraceEvent) -> None:
        pass

    backing = _FakeStoreWithAclose()
    session = api.Session()
    (repo_a,) = await session.open_remote(_as_object_store(backing), trace=_no_trace)
    (repo_b,) = await session.open_remote(_as_object_store(backing), trace=_no_trace)

    await session.close_repo(repo_a)
    assert backing.closed is False  # repo_b's group still references the same backing store
    assert len(session._stores) == 2  # neither wrapper removed yet -- still shared

    await session.close_repo(repo_b)
    assert backing.closed is True
    assert session._stores == []  # both wrappers released together, not just repo_b's own


async def test_session_close_repo_keeps_store_tracked_when_cancelled_mid_aclose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation arriving while ``aclose_if_possible`` is still
    in-flight must not orphan the store — neither tracked nor closed.
    ``close_repo()`` only removes a store from ``self._stores`` once its
    own aclose attempt has returned or raised, so a still-in-flight one
    stays trackable for ``Session.close()`` to pick up later."""

    class _FakeStoreThatHangsOnFirstAclose(_FakeStore):
        """Hangs only on its first ``aclose()``; a second caller retrying
        the same close must still succeed normally."""

        def __init__(self) -> None:
            self.closed = False
            self._first_call = True

        async def aclose(self) -> None:
            if self._first_call:
                self._first_call = False
                aclose_started.set()
                await asyncio.Event().wait()  # never resolves - only cancellation ends this
            self.closed = True

    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )
    monkeypatch.setattr(DedupRepo, "open", _async_open(lambda store, layout, keys, **kwargs: _FakeDedupRepo(layout)))

    aclose_started = asyncio.Event()
    store = _FakeStoreThatHangsOnFirstAclose()
    session = api.Session()
    (repo,) = await session.open_remote(_as_object_store(store))

    task = asyncio.create_task(session.close_repo(repo))
    await aclose_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert cast(object, store) in session._stores  # still tracked -- not orphaned
    assert store.closed is False

    await session.close()  # can still reach and close it later
    assert store.closed is True


async def test_session_discover_cancellation_also_cancels_still_pending_open_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unlike the scan-cancellation test above (where every step completes
    instantly, so ``pending_open`` is empty by the time cancellation
    lands), this hangs the encryption probe for the one found layout so
    ``pending_open`` still holds an in-flight task when cancellation
    lands, exercising the ``finally`` block's cancel/await cleanup for
    real."""
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: _FakeStore())
    monkeypatch.setattr(
        api_session, "iter_repository_layouts", _async_iter_repository_layouts([_repository_layout("a")])
    )

    probe_started = asyncio.Event()

    async def hanging_probe(store: object, layout: RepoLayout) -> bool | None:
        probe_started.set()
        await asyncio.Event().wait()  # never resolves - only cancellation ends this
        raise AssertionError("unreachable")  # pragma: no cover

    monkeypatch.setattr(api_session, "_probe_encrypted", hanging_probe)

    async def drain() -> None:
        async for _repo in api.Session().discover("/some/path"):
            raise AssertionError("unreachable")  # pragma: no cover

    task = asyncio.create_task(drain())
    await probe_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_resolve_key_verification_returns_none_without_keys() -> None:
    assert await api_session._resolve_key_verification(None, cast(Any, _FakeStore()), _layout()) is None


async def test_resolve_key_verification_delegates_to_keys_verify(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[Any] = []

    async def fake_verify(self: KeyMaterial, store: object, layout: RepoLayout) -> KeyVerification:
        captured.append((store, layout))
        return KeyVerification(gcm_ok=True, vault_key=None)

    monkeypatch.setattr(KeyMaterial, "verify", fake_verify)
    keys = _no_encryption_keys()
    store = cast(Any, _FakeStore())
    layout = _layout("root")

    result = await api_session._resolve_key_verification(keys, store, layout)

    assert result is not None
    assert result.gcm_ok is True
    assert captured == [(store, layout)]


# -- Session.resolve() -------------------------------------------------------


class _FakeUnitProvider:
    """A tiny fixed tree: root -> folder -> leaf, a growing-prefix ref
    (like Device/FS/Mail/...) — resolved via units/resolve.py's generic
    prefix-guided descent. See ``_FlatIdFakeProvider`` below for the
    Drive-shaped case (a single flat extra segment regardless of depth),
    which that descent can't reach at all."""

    def __init__(
        self, root: Node, children_by_parent_ref: dict[str, list[Node]], units_by_ref: dict[str, RestorableUnit]
    ):
        self._root = root
        self._children_by_parent_ref = children_by_parent_ref
        self._units_by_ref = units_by_ref

    def root(self) -> Node:
        # ``UnitProvider.root()`` stays synchronous; ``children()``/``unit()`` are async.
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        children = self._children_by_parent_ref.get(str(node.ref), [])
        stop = offset + limit if limit is not None else None
        return children[offset:stop]

    async def unit(self, node: Node) -> RestorableUnit:
        return self._units_by_ref[str(node.ref)]


_RESOLVE_CCID = ConnectionConfigId(1)
_RESOLVE_WORKLOAD_ID = WorkloadId(2)
_RESOLVE_VERSION_UID = VersionUid("v-uid")


def _make_tree() -> _FakeUnitProvider:
    root_ref = NodeRef.canonical(
        "", catalog_id=CatalogId(str(_RESOLVE_CCID)), workload_id=_RESOLVE_WORKLOAD_ID, version_uid=_RESOLVE_VERSION_UID
    )
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder_ref = NodeRef(root_ref.repo_path, (*root_ref.segments, "folder"))
    folder = Node(ref=folder_ref, name="Folder", is_leaf=False)
    leaf_ref = NodeRef(root_ref.repo_path, (*folder_ref.segments, "leaf1"))
    leaf = RestorableUnit(ref=leaf_ref, name="leaf1.txt", is_leaf=True)
    return _FakeUnitProvider(
        root=root,
        children_by_parent_ref={
            str(root_ref): [folder],
            str(folder_ref): [leaf],
        },
        units_by_ref={str(leaf_ref): leaf},
    )


class _FlatIdFakeProvider:
    """Drive's shape: every node's ``extra_segments`` is a single flat id
    regardless of depth, so units/resolve.py's generic prefix-guided
    descent can't reach it at all — resolution only works because this
    fake implements ``synology_apm_repo.sdk.units.base.SupportsDirectRefLookup``
    directly (real per-provider coverage of this capability lives in
    ``test_units_saas_drive.py``, against the real ``RecursiveTree``;
    this is just api.repository's own dispatch onto it)."""

    def __init__(self, root: Node, leaf_by_id: dict[str, RestorableUnit]) -> None:
        self._root = root
        self._leaf_by_id = leaf_by_id

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return []  # never reached — resolve_extra() finds a match directly

    async def unit(self, node: Node) -> RestorableUnit:
        return self._leaf_by_id[node.ref.extra_segments[0]]

    async def resolve_extra(self, extra_segments: tuple[str, ...]) -> Node | None:
        if len(extra_segments) != 1:
            return None
        return self._leaf_by_id.get(extra_segments[0])

    async def parent_of(self, node: Node) -> Node | None:
        return None


def _repo_with_tree(monkeypatch: pytest.MonkeyPatch, provider: UnitProvider) -> api.Repository:
    """Same module-boundary faking as ``_repo_with_catalog_stubs`` above,
    but for a single, fixed catalog/workload/version — this repository's own
    ``connection_config_id``/``workload_id``/``version_uid`` are exactly
    ``_RESOLVE_CCID``/``_RESOLVE_WORKLOAD_ID``/``_RESOLVE_VERSION_UID``, so
    a canonical ref built from those constants always resolves to
    ``version``, and ``provider`` is what every resolve test's tree lives
    on. ``version``'s ``M365`` target type routes ``Catalog.provider()``
    through ``workload_by_id``/``saas_provider_for`` (also faked here),
    the same dispatch path a real SaaS version takes."""
    repo = _repo_with_fake_dedup(monkeypatch, _FakeDedupRepo(_layout()))
    connection = _make_connection()
    workload = _make_workload(workload_id=_RESOLVE_WORKLOAD_ID)
    version = _make_version(workload_id=_RESOLVE_WORKLOAD_ID, target_type="M365")
    version = dataclasses.replace(version, version_uid=_RESOLVE_VERSION_UID, connection_config_id=_RESOLVE_CCID)

    async def fake_connections(dedup_repo: object) -> list[Connection]:
        return [connection]

    async def fake_workloads(dedup_repo: object, c: Connection) -> list[Workload]:
        return [workload] if c is connection else []

    async def fake_versions(dedup_repo: object, w: Workload, **k: object) -> list[Version]:
        return [version] if w is workload else []

    async def fake_workload_by_id(dedup_repo: object, workload_id: object) -> Workload | None:
        return workload if workload_id == workload.workload_id else None

    async def fake_saas_provider_for(
        dedup_repo: object, w: object, v: object, saas_streams: object, *, object_db_id: str | None = None
    ) -> UnitProvider:
        assert v is version
        return provider

    monkeypatch.setattr(api_repository, "connections", fake_connections)
    monkeypatch.setattr(api_catalog, "workloads", fake_workloads)
    monkeypatch.setattr(api_catalog, "versions", fake_versions)
    monkeypatch.setattr(api_catalog, "workload_by_id", fake_workload_by_id)
    monkeypatch.setattr(api_catalog, "saas_provider_for", fake_saas_provider_for)
    return repo


def _session_with_repo(repo: api.Repository) -> api.Session:
    session = api.Session()
    session._repos.append(repo)
    return session


async def test_resolve_canonical_ref_growing_prefix_leaf(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)

    leaf_ref = NodeRef.canonical(
        "",
        catalog_id=CatalogId(str(_RESOLVE_CCID)),
        workload_id=WorkloadId(_RESOLVE_WORKLOAD_ID),
        version_uid=VersionUid(_RESOLVE_VERSION_UID),
        extra=("folder", "leaf1"),
    )
    resolved = await session.resolve(str(leaf_ref))
    assert resolved.name == "leaf1.txt"
    assert isinstance(resolved, RestorableUnit)


async def test_resolve_canonical_ref_flat_id_leaf_via_direct_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """The Drive-shaped case: a leaf whose single extra segment is not a
    growing prefix of the version root's segments is resolved via
    ``synology_apm_repo.sdk.units.base.SupportsDirectRefLookup``,
    not units/resolve.py's generic prefix-guided descent (Drive/Team
    Drive's flat, depth-independent node ids never form the growing-prefix
    relationship that descent needs)."""
    root_ref = NodeRef.canonical(
        "", catalog_id=CatalogId(str(_RESOLVE_CCID)), workload_id=_RESOLVE_WORKLOAD_ID, version_uid=_RESOLVE_VERSION_UID
    )
    root = Node(ref=root_ref, name="root", is_leaf=False)
    flat_leaf_ref = NodeRef(root_ref.repo_path, (*root_ref.segments, "flatid"))
    flat_leaf = RestorableUnit(ref=flat_leaf_ref, name="flat.txt", is_leaf=True)
    provider = _FlatIdFakeProvider(root=root, leaf_by_id={"flatid": flat_leaf})
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)

    flat_ref = NodeRef.canonical(
        "",
        catalog_id=CatalogId(str(_RESOLVE_CCID)),
        workload_id=WorkloadId(_RESOLVE_WORKLOAD_ID),
        version_uid=VersionUid(_RESOLVE_VERSION_UID),
        extra=("flatid",),
    )
    resolved = await session.resolve(str(flat_ref))
    assert resolved.name == "flat.txt"


async def test_resolve_canonical_ref_accepts_a_nodeRef_object_directly(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)

    leaf_ref = NodeRef.canonical(
        "",
        catalog_id=CatalogId(str(_RESOLVE_CCID)),
        workload_id=WorkloadId(_RESOLVE_WORKLOAD_ID),
        version_uid=VersionUid(_RESOLVE_VERSION_UID),
        extra=("folder", "leaf1"),
    )
    resolved = await session.resolve(leaf_ref)  # NodeRef, not str
    assert resolved.name == "leaf1.txt"


async def test_resolve_canonical_ref_to_a_non_leaf_returns_the_node_itself(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)

    folder_ref = NodeRef.canonical(
        "",
        catalog_id=CatalogId(str(_RESOLVE_CCID)),
        workload_id=WorkloadId(_RESOLVE_WORKLOAD_ID),
        version_uid=VersionUid(_RESOLVE_VERSION_UID),
        extra=("folder",),
    )
    resolved = await session.resolve(str(folder_ref))
    assert resolved.name == "Folder"
    assert not isinstance(resolved, RestorableUnit)


async def test_resolve_canonical_ref_no_match_in_tree_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)

    missing_ref = NodeRef.canonical(
        "",
        catalog_id=CatalogId(str(_RESOLVE_CCID)),
        workload_id=WorkloadId(_RESOLVE_WORKLOAD_ID),
        version_uid=VersionUid(_RESOLVE_VERSION_UID),
        extra=("does-not-exist",),
    )
    with pytest.raises(NotFoundError):
        await session.resolve(str(missing_ref))


async def test_resolve_malformed_canonical_ref_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    # kind is CANONICAL (starts with "cat:") but missing the wl:/ver: parts.
    malformed = NodeRef("", ("cat:1",))
    with pytest.raises(NotFoundError):
        await session.resolve(str(malformed))


async def test_resolve_canonical_ref_unknown_catalog_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    ref = NodeRef.canonical(
        "", catalog_id=CatalogId("999"), workload_id=_RESOLVE_WORKLOAD_ID, version_uid=VersionUid("x")
    )
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_resolve_canonical_ref_unknown_workload_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    ref = NodeRef.canonical(
        "", catalog_id=CatalogId(str(_RESOLVE_CCID)), workload_id=WorkloadId(999), version_uid=VersionUid("x")
    )
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_resolve_canonical_ref_unknown_version_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    ref = NodeRef.canonical(
        "",
        catalog_id=CatalogId(str(_RESOLVE_CCID)),
        workload_id=_RESOLVE_WORKLOAD_ID,
        version_uid=VersionUid("not-the-real-uid"),
    )
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_resolve_raw_ref_finds_a_leaf_in_file_map_tree(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(_as_object_store(_FakeStore()), _repository_layout(), keys=None, key_verification=None)
    leaf_ref = NodeRef.raw("", "dir/file.bin")
    leaf = RestorableUnit(ref=leaf_ref, name="file.bin", is_leaf=True)
    root_ref = NodeRef.raw("", "")
    root = Node(ref=root_ref, name="/", is_leaf=False)
    dir_ref = NodeRef.raw("", "dir")
    dir_node = Node(ref=dir_ref, name="dir", is_leaf=False)
    provider = _FakeUnitProvider(
        root=root,
        children_by_parent_ref={str(root_ref): [dir_node], str(dir_ref): [leaf]},
        units_by_ref={str(leaf_ref): leaf},
    )
    monkeypatch.setattr(repo, "file_map_tree", _async_returning(provider))
    session = _session_with_repo(repo)

    resolved = await session.resolve(str(leaf_ref))
    assert resolved.name == "file.bin"


async def test_resolve_ref_with_no_matching_open_repo_raises_not_found() -> None:
    session = api.Session()
    ref = NodeRef.canonical(
        "some-other-root",
        catalog_id=CatalogId("1"),
        workload_id=WorkloadId(1),
        version_uid=VersionUid("x"),
    )
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_repo_for_ref_matches_object_store_catalog_level_repo_root() -> None:
    """Regression test: a node produced from an OBJECT_STORE repository
    with enumerable ``catalog_ids`` carries its own catalog-level
    ``RepoLayout.repo_root`` (see ``catalog_repo_layouts()``), never the
    bucket-level ``RepositoryLayout.repo_root`` -- the two diverge by
    construction once ``catalog_ids`` is populated, so a ref's
    ``repo_path`` must be matched against the former, not the latter."""
    bucket_layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="bucket", catalog_ids=["repo-a"])
    repo = api.Repository(_as_object_store(_FakeStore()), bucket_layout, keys=None, key_verification=None)
    catalog_repo_root = catalog_repo_layouts(bucket_layout)[0].repo_root
    assert catalog_repo_root != bucket_layout.repo_root  # the mismatch this bug hinged on

    assert repo.owns_repo_path(catalog_repo_root)
    assert not repo.owns_repo_path(bucket_layout.repo_root)

    session = _session_with_repo(repo)
    assert session._repo_for_ref(NodeRef(catalog_repo_root, ("x",))) is repo


async def test_resolve_human_ref_too_few_segments_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    ref = NodeRef.human("", "OnlyOneSegment")
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_resolve_human_ref_unknown_connection_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    ref = NodeRef.human("", "NoSuchSource", "wl", "ver")
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_resolve_human_ref_unknown_workload_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    connection = _make_connection()
    ref = NodeRef.human("", connection.display_name, "NoSuchWorkload", "ver")
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_resolve_human_ref_unknown_version_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    connection = _make_connection()
    workload = _make_workload(workload_id=_RESOLVE_WORKLOAD_ID)
    ref = NodeRef.human("", connection.display_name, workload.display_name, "NoSuchVersion")
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_resolve_human_ref_down_to_a_leaf_by_display_name(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)
    connection = _make_connection()
    workload = _make_workload(workload_id=_RESOLVE_WORKLOAD_ID)
    version = _make_version(workload_id=_RESOLVE_WORKLOAD_ID, target_type="M365")
    version = dataclasses.replace(version, version_uid=_RESOLVE_VERSION_UID, connection_config_id=_RESOLVE_CCID)

    ref = NodeRef.human("", connection.display_name, workload.display_name, version.display_name, "Folder", "leaf1.txt")
    resolved = await session.resolve(str(ref))
    assert resolved.name == "leaf1.txt"


async def test_resolve_human_ref_names_more_levels_than_tree_has_raises_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)
    connection = _make_connection()
    workload = _make_workload(workload_id=_RESOLVE_WORKLOAD_ID)
    version = _make_version(workload_id=_RESOLVE_WORKLOAD_ID, target_type="M365")
    version = dataclasses.replace(version, version_uid=_RESOLVE_VERSION_UID, connection_config_id=_RESOLVE_CCID)

    ref = NodeRef.human(
        "", connection.display_name, workload.display_name, version.display_name, "Folder", "leaf1.txt", "too-deep"
    )
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


async def test_resolve_human_ref_unknown_item_name_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)
    connection = _make_connection()
    workload = _make_workload(workload_id=_RESOLVE_WORKLOAD_ID)
    version = _make_version(workload_id=_RESOLVE_WORKLOAD_ID, target_type="M365")
    version = dataclasses.replace(version, version_uid=_RESOLVE_VERSION_UID, connection_config_id=_RESOLVE_CCID)

    ref = NodeRef.human("", connection.display_name, workload.display_name, version.display_name, "NoSuchChild")
    with pytest.raises(NotFoundError):
        await session.resolve(str(ref))


__all__: list[str] = []
