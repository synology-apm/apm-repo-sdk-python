"""Unit tests for ``api.Session``: discover/open/cancel/progress, close and
``close_repo()``, the context manager, and ``Session.resolve()`` (picking
the right open ``Repository``, then delegating into its
``resolve()``/``locate()``, exercised through small fake trees).
``Repository`` and ``Catalog`` are covered in
``test_api_repository.py``/``test_api_catalog.py``. Collaborators are faked
at the module boundary (the names ``api.session``/``api.repository``/
``api.catalog`` imported); ``tests/integration/sdk/test_api.py`` covers the
real wiring."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest

from support.fakes import faithful_to
from support.model_factories import make_connection, make_version, make_workload
from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.api import catalog as api_catalog
from synology_apm_repo.sdk.api import repository as api_repository
from synology_apm_repo.sdk.api import session as api_session
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import (
    ApmRepoError,
    KeyMaterialError,
    NotFoundError,
    NotRestorableError,
    StorageBackendError,
)
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
    ConnectionConfigId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.presentation.progress import Progress
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout, catalog_repo_layouts
from synology_apm_repo.sdk.units.base import (
    ClosableUnitProvider,
    Node,
    RestorableUnit,
    SupportsDirectRefLookup,
    UnitProvider,
)
from synology_apm_repo.sdk.units.content.saas_artifact import LazyArtifact
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.sdk.api_fakes import (
    VALID_B64_KEY,
    FakeDedupRepo,
    FakeRaisingDedupRepo,
    FakeStore,
    as_object_store,
    async_returning,
    no_encryption_keys,
    patch_dedup_open,
    patch_layouts,
    repo_with_fake_dedup,
    vault_layout,
    vault_repository_layout,
)


async def _unread_content() -> bytes:
    raise AssertionError("this test never reads a unit's content")


class _FakeStoreWithClose(FakeStore):
    """``FakeStore`` whose ``close()`` records that, and how often, it ran."""

    def __init__(self) -> None:
        self.closed = False
        self.close_calls = 0

    async def close(self) -> None:
        self.closed = True
        self.close_calls += 1


# -- Session.discover / open / cancel / progress -----------------------------


async def test_session_discover_yields_one_repository_per_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    layouts = [vault_repository_layout("a"), vault_repository_layout("b")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, layouts)
    patch_dedup_open(monkeypatch)

    session = api.Session()
    repos = await session.open("/some/path")

    assert len(repos) == 2
    assert [r.layout.repo_root for r in repos] == ["a", "b"]
    assert list(session._repos) == repos


async def test_session_discover_yields_a_vault_whose_catalog_fails_to_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discovery trusts ``iter_repository_layouts``'s
    marker check for ``VAULT``: a vault whose catalog would fail to open is
    still yielded, and the failure surfaces from that repository's
    ``catalogs()`` instead."""
    layouts = [vault_repository_layout("looks-fine-but-corrupt")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, layouts)

    async def failing_open(store: object, layout: RepoLayout, keys: object, **kwargs: object) -> DedupRepo:
        raise ApmRepoError("corrupt repo_info")

    patch_dedup_open(monkeypatch, failing_open)

    repos = await api.Session().open("/some/path")

    assert len(repos) == 1
    with pytest.raises(ApmRepoError, match="corrupt repo_info"):
        await repos[0].catalogs()


async def test_session_discover_skips_an_object_store_layout_with_no_valid_catalog_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one case discovery rejects a found layout for: an
    ``OBJECT_STORE`` ``@ActiveProtectData`` with zero valid repo-id children
    (``catalog_ids == []``)."""
    layouts = [
        RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="empty-bucket", catalog_ids=[]),
        vault_repository_layout("good"),
    ]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, layouts)

    repos = await api.Session().open("/some/path")

    assert len(repos) == 1
    assert repos[0].layout.repo_root == "good"


async def test_session_discover_skips_a_layout_whose_key_probe_itself_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """An ``ApmRepoError`` from the encryption probe (e.g. a half-written
    vault's corrupt ``db/vault_encryption_key``) skips that layout instead of
    aborting the whole scan."""
    layouts = [vault_repository_layout("bad"), vault_repository_layout("good")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, layouts)
    patch_dedup_open(monkeypatch)

    async def fake_probe_encrypted(store: object, layout: RepoLayout) -> bool | None:
        if layout.repo_root == "bad":
            raise ApmRepoError("synthetic corrupt vault_encryption_key")
        return False

    monkeypatch.setattr(api_session, "_probe_encrypted", fake_probe_encrypted)

    repos = await api.Session().open("/some/path")

    assert len(repos) == 1
    assert repos[0].layout.repo_root == "good"


async def test_session_discover_aborts_on_a_storage_backend_failure_in_the_key_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A storage failure says nothing about the layout, so it propagates
    rather than silently dropping a real repository from the scan."""
    layouts = [vault_repository_layout("unreachable"), vault_repository_layout("good")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, layouts)
    patch_dedup_open(monkeypatch)

    async def fake_probe_encrypted(store: object, layout: RepoLayout) -> bool | None:
        if layout.repo_root == "unreachable":
            raise StorageBackendError("synthetic timeout")
        return False

    monkeypatch.setattr(api_session, "_probe_encrypted", fake_probe_encrypted)

    with pytest.raises(StorageBackendError, match="synthetic timeout"):
        await api.Session().open("/some/path")


async def test_session_discover_reports_progress_with_found_count(monkeypatch: pytest.MonkeyPatch) -> None:
    layouts = [vault_repository_layout("a"), vault_repository_layout("b")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, layouts)
    patch_dedup_open(monkeypatch)

    seen: list[Progress] = []

    # ``progress`` is ``Callable[[Progress], Awaitable[None]]`` and is
    # awaited, so a bare ``list.append`` doesn't satisfy the contract.
    async def record(p: Progress) -> None:
        seen.append(p)

    [_ async for _ in api.Session().discover("/some/path", progress=record)]

    assert [p.found for p in seen] == [1, 2]
    assert all(p.phase == "discovering" and not p.determinate for p in seen)


async def test_session_discover_cancellation_aborts_the_scan_partway(monkeypatch: pytest.MonkeyPatch) -> None:
    """A cancelled scan stops partway instead of draining every layout. The
    test cancels once "a" reaches ``seen``: discovery gives no order between
    asking for the next layout and delivering "a"."""
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_dedup_open(monkeypatch)

    async def fake_iter_repository_layouts(store: object, root: str = "") -> AsyncIterator[RepositoryLayout]:
        yield vault_repository_layout("a")
        await asyncio.Event().wait()  # never set: the cancellation lands on this await
        yield vault_repository_layout("b")

    monkeypatch.setattr(api_session, "iter_repository_layouts", fake_iter_repository_layouts)

    seen: list[api.Repository] = []
    got_first = asyncio.Event()

    async def drain() -> None:
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
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(verification))

    [repo] = await api.Session().open("/some/path", key=f"some-id-0000@{VALID_B64_KEY}")

    assert repo.key_verification is verification
    assert repo.key_status is api.KeyStatus.VERIFIED


async def test_session_discover_resolves_encrypted_status_eagerly_when_no_key_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no key given, ``key_status`` is already resolved by the
    encryption probe right out of ``discover()``, before any ``DedupRepo``
    opens."""
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    monkeypatch.setattr(api_session, "_probe_encrypted", async_returning(False))
    patch_dedup_open(monkeypatch)

    [repo] = await api.Session().open("/some/path")

    assert repo.key_status is api.KeyStatus.NOT_ENCRYPTED


async def test_session_discover_reports_no_key_provided_when_probe_confirms_encrypted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    monkeypatch.setattr(api_session, "_probe_encrypted", async_returning(True))
    patch_dedup_open(monkeypatch)

    [repo] = await api.Session().open("/some/path")

    assert repo.key_status is api.KeyStatus.NO_KEY_PROVIDED


async def test_session_discover_skips_the_probe_entirely_when_a_key_is_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once a key is given, ``key_verification`` alone answers
    ``key_status``, so the encryption probe's extra read must not run."""
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)
    verification = KeyVerification(gcm_ok=True, vault_key=b"x" * 32)
    monkeypatch.setattr(KeyMaterial, "verify", async_returning(verification))

    probed = False

    async def _fail_if_called(store: object, layout: object) -> bool | None:
        nonlocal probed
        probed = True
        return None

    monkeypatch.setattr(api_session, "_probe_encrypted", _fail_if_called)

    await api.Session().open("/some/path", key=f"some-id-0000@{VALID_B64_KEY}")

    assert probed is False


async def test_session_discover_from_a_store_uses_it_directly(monkeypatch: pytest.MonkeyPatch) -> None:
    """Given an ``ObjectStore`` rather than a path, no ``LocalFsStore`` is
    built — ``iter_repository_layouts`` runs against exactly the store the
    caller handed in."""
    layouts = [vault_repository_layout("a"), vault_repository_layout("b")]
    patch_layouts(monkeypatch, layouts)
    patch_dedup_open(monkeypatch)

    store = as_object_store(FakeStore())
    repos = await api.Session().open(store)

    assert len(repos) == 2
    assert [r.layout.repo_root for r in repos] == ["a", "b"]


async def test_session_discover_from_a_store_registers_it_for_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """A store handed to ``discover`` is owned the same way the
    ``LocalFsStore`` it builds for a path is — tracked in
    ``_stores`` so ``close()`` can ``close()`` it: ``S3Store``/``AzureStore``
    own an ``aiohttp`` connector that must be released."""
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)

    store = as_object_store(FakeStore())
    session = api.Session()
    await session.open(store)

    assert store in session._stores


async def test_session_discover_from_a_store_reports_progress_and_resolves_key_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A store source goes through the same ``_discover_from_store`` core
    as a path — one spot check (progress + eager key-status resolution)."""
    patch_layouts(monkeypatch, [vault_repository_layout("a"), vault_repository_layout("b")])
    patch_dedup_open(monkeypatch)

    seen: list[Progress] = []

    async def record(p: Progress) -> None:
        seen.append(p)

    repos = [repo async for repo in api.Session().discover(as_object_store(FakeStore()), progress=record)]

    assert [p.found for p in seen] == [1, 2]
    # FakeStore's probe finds no key record; with no key given that is NO_KEY_PROVIDED.
    assert all(repo.key_status is api.KeyStatus.NO_KEY_PROVIDED for repo in repos)


async def test_session_close_closes_all_repos_and_clears_the_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """``close()`` closes every repository a single ``open()`` call
    discovered and empties ``_repos``."""
    layouts = [vault_repository_layout("a"), vault_repository_layout("b")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, layouts)
    fakes: list[FakeDedupRepo] = []

    def make_fake(store: object, layout: RepoLayout, keys: object, **kwargs: object) -> FakeDedupRepo:
        fake = FakeDedupRepo(layout)
        fakes.append(fake)
        return fake

    patch_dedup_open(monkeypatch, make_fake)
    monkeypatch.setattr(api_repository, "connections", async_returning([]))

    session = api.Session()
    repos = await session.open("/some/path")
    for repo in repos:
        await repo._open_catalogs.resolve(0)  # the DedupRepo opens lazily; open it so close() has one to close
    await session.close()

    assert [f.closed for f in fakes] == [True, True]
    assert not session._repos


async def test_session_close_still_closes_every_remaining_repo_when_an_earlier_one_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One repository's close raising its ``ExceptionGroup`` must not
    abandon closing every other repository the same ``open()`` call
    discovered."""
    layouts = [vault_repository_layout("a"), vault_repository_layout("b")]
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, layouts)
    fakes: list[FakeDedupRepo] = []
    first_repo_root = layouts[0].repo_root

    def make_fake(store: object, layout: RepoLayout, keys: object, **kwargs: object) -> FakeDedupRepo:
        fake = FakeRaisingDedupRepo(layout) if layout.repo_root == first_repo_root else FakeDedupRepo(layout)
        fakes.append(fake)
        return fake

    patch_dedup_open(monkeypatch, make_fake)
    monkeypatch.setattr(api_repository, "connections", async_returning([]))

    session = api.Session()
    repos = await session.open("/some/path")
    for repo in repos:
        await repo._open_catalogs.resolve(0)

    with pytest.raises(ExceptionGroup, match=r"Session\.close\(\) failed to close every tracked resource"):
        await session.close()

    assert [f.closed for f in fakes] == [True, True]
    assert not session._repos


async def test_session_context_manager_closes_on_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    fake = FakeDedupRepo(vault_layout("a"))
    patch_dedup_open(monkeypatch, lambda store, layout, keys, **kwargs: fake)
    monkeypatch.setattr(api_repository, "connections", async_returning([]))

    async with api.Session() as session:
        (repo,) = await session.open("/some/path")
        await repo._open_catalogs.resolve(0)
    assert fake.closed is True


async def test_session_close_closes_the_store_it_opened(monkeypatch: pytest.MonkeyPatch) -> None:

    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)

    store = _FakeStoreWithClose()
    session = api.Session()
    await session.open(as_object_store(store))
    await session.close()

    assert store.closed is True


async def test_session_close_still_closes_every_remaining_store_when_an_earlier_one_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One store's ``close()`` raising must not abandon closing every
    other tracked store."""

    class _FakeRaisingStoreWithClose(FakeStore):
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True
            raise RuntimeError("synthetic close failure")

    patch_layouts(monkeypatch, [vault_repository_layout("a"), vault_repository_layout("b")])
    patch_dedup_open(monkeypatch)

    first_store, second_store = _FakeRaisingStoreWithClose(), _FakeStoreWithClose()
    session = api.Session()
    await session.open(as_object_store(first_store))
    await session.open(as_object_store(second_store))

    with pytest.raises(ExceptionGroup, match=r"Session\.close\(\) failed to close every tracked resource"):
        await session.close()

    assert first_store.closed is True
    assert second_store.closed is True  # still closed despite the first store's failure


async def test_session_close_does_not_wait_forever_on_a_hung_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """A store whose ``close()`` never returns (a stuck server) is bounded
    by ``RESOURCE_CLOSE_TIMEOUT``, as each repository's own closes are, and
    the next store is still closed."""

    class _HungStore(FakeStore):
        async def close(self) -> None:
            await asyncio.Event().wait()

    monkeypatch.setattr(api_session, "RESOURCE_CLOSE_TIMEOUT", 0.05)
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)
    second_store = _FakeStoreWithClose()
    session = api.Session()
    await session.open(as_object_store(_HungStore()))
    await session.open(as_object_store(second_store))

    with pytest.raises(ExceptionGroup, match=r"Session\.close\(\) failed to close every tracked resource") as exc_info:
        await session.close()
    assert any(isinstance(e, TimeoutError) for e in exc_info.value.exceptions)
    assert second_store.close_calls == 1


async def test_session_close_repo_closes_and_forgets_it_and_closes_unshared_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``close_repo()`` on the one repository a store yielded closes both
    the repository and its own store, and
    removes both from this session's own bookkeeping."""

    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    fake_dedup = FakeDedupRepo(vault_layout("a"))
    patch_dedup_open(monkeypatch, lambda store, layout, keys, **kwargs: fake_dedup)
    monkeypatch.setattr(api_repository, "connections", async_returning([]))

    store = _FakeStoreWithClose()
    session = api.Session()
    (repo,) = await session.open(as_object_store(store))
    await repo._open_catalogs.resolve(0)

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

    store = _FakeStoreWithClose()
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: store)
    patch_layouts(monkeypatch, [vault_repository_layout("a"), vault_repository_layout("b")])
    patch_dedup_open(monkeypatch)

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


async def test_session_close_repo_mid_discovery_keeps_the_store_for_siblings_yet_to_come(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing the first repository a still-running discovery yielded
    leaves their shared store open for the sibling it yields next."""

    store = _FakeStoreWithClose()
    patch_layouts(monkeypatch, [vault_repository_layout("a"), vault_repository_layout("b")])
    patch_dedup_open(monkeypatch)

    session = api.Session()
    discovery = session.discover(as_object_store(store))
    first = await anext(discovery)
    await session.close_repo(first)
    assert store.closed is False

    second = await anext(discovery)
    await discovery.aclose()
    await session.close_repo(second)
    assert store.closed is True


async def test_a_store_close_repo_deferred_closes_when_its_discovery_ends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The last repository released mid-discovery leaves the store open
    only until that discovery finishes, not until ``Session.close()``."""
    store = _FakeStoreWithClose()
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)

    session = api.Session()
    discovery = session.discover(as_object_store(store))
    only = await anext(discovery)
    await session.close_repo(only)
    assert store.closed is False

    with pytest.raises(StopAsyncIteration):
        await anext(discovery)
    assert store.closed is True
    assert cast(object, store) not in session._stores


async def test_a_discovery_failing_before_it_starts_leaves_no_running_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed key fails the discovery before it runs; a later
    ``close_repo()`` on the same store still closes it."""
    store = _FakeStoreWithClose()
    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)

    session = api.Session()
    with pytest.raises(KeyMaterialError, match="key string is not"):
        await session.open(as_object_store(store), "not-a-key")
    assert session._discovering == {}

    (repo,) = await session.open(as_object_store(store))
    await session.close_repo(repo)
    assert store.closed is True


async def test_session_close_repo_is_safe_when_the_repo_is_already_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A repository whose own close already ran still gets its store and
    bookkeeping released by ``close_repo()`` (that close is idempotent)."""

    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)

    store = _FakeStoreWithClose()
    session = api.Session()
    (repo,) = await session.open(as_object_store(store))
    await repo._close()

    await session.close_repo(repo)

    assert store.closed is True
    assert repo not in session._repos
    assert cast(object, store) not in session._stores


async def test_session_close_repo_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling ``close_repo()`` twice on the same repository must not raise
    or ``close()`` its store a second time."""

    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)

    store = _FakeStoreWithClose()
    session = api.Session()
    (repo,) = await session.open(as_object_store(store))

    await session.close_repo(repo)
    await session.close_repo(repo)

    assert store.close_calls == 1


async def test_session_close_repo_treats_two_traced_discovers_of_one_backing_store_as_shared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With ``trace=``, each ``discover()`` wraps the store in its own
    ``TracingStore``; ``close_repo()`` still treats two wrappers of one
    backing store as shared and closes it only with the last of them."""

    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)

    def _no_trace(event: api_session.TraceEvent) -> None:
        pass

    backing = _FakeStoreWithClose()
    session = api.Session()
    (repo_a,) = await session.open(as_object_store(backing), trace=_no_trace)
    (repo_b,) = await session.open(as_object_store(backing), trace=_no_trace)

    await session.close_repo(repo_a)
    assert backing.closed is False  # repo_b's group still references the same backing store
    assert len(session._stores) == 2  # neither wrapper removed yet -- still shared

    await session.close_repo(repo_b)
    assert backing.closed is True
    assert session._stores == []  # both wrappers released together, not just repo_b's own


async def test_session_close_repo_keeps_store_tracked_when_cancelled_mid_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation arriving while the store's ``close()`` is still
    in flight leaves the store tracked, so ``Session.close()`` can still
    close it — never orphaned (neither tracked nor closed)."""

    class _FakeStoreThatHangsOnFirstClose(FakeStore):
        """Hangs only on its first ``close()``; a second caller retrying
        the same close must still succeed normally."""

        def __init__(self) -> None:
            self.closed = False
            self._first_call = True

        async def close(self) -> None:
            if self._first_call:
                self._first_call = False
                close_started.set()
                await asyncio.Event().wait()  # never resolves - only cancellation ends this
            self.closed = True

    patch_layouts(monkeypatch, [vault_repository_layout("a")])
    patch_dedup_open(monkeypatch)

    close_started = asyncio.Event()
    store = _FakeStoreThatHangsOnFirstClose()
    session = api.Session()
    (repo,) = await session.open(as_object_store(store))

    task = asyncio.create_task(session.close_repo(repo))
    await close_started.wait()
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
    """The encryption probe hangs, so ``pending_open`` still holds an
    in-flight open task when cancellation lands — ``_drain_and_close``
    must cancel and await it."""
    monkeypatch.setattr(api_session, "LocalFsStore", lambda path: FakeStore())
    patch_layouts(monkeypatch, [vault_repository_layout("a")])

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
    assert await api_session._resolve_key_verification(None, cast(Any, FakeStore()), vault_layout()) is None


async def test_resolve_key_verification_delegates_to_keys_verify(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[Any] = []

    async def fake_verify(self: KeyMaterial, store: object, layout: RepoLayout) -> KeyVerification:
        captured.append((store, layout))
        return KeyVerification(gcm_ok=True, vault_key=None)

    monkeypatch.setattr(KeyMaterial, "verify", fake_verify)
    keys = no_encryption_keys()
    store = cast(Any, FakeStore())
    layout = vault_layout("root")

    result = await api_session._resolve_key_verification(keys, store, layout)

    assert result is not None
    assert result.gcm_ok is True
    assert captured == [(store, layout)]


# -- Session.resolve() -------------------------------------------------------


@faithful_to(ClosableUnitProvider)
class _FakeUnitProvider:
    """A tiny fixed tree: root -> folder -> leaf, each node's
    ``extra_segments`` a growing prefix of its child's — the shape
    units/resolve.py's generic prefix-guided descent resolves."""

    def __init__(
        self, root: Node, children_by_parent_ref: dict[str, list[Node]], units_by_ref: dict[str, RestorableUnit]
    ):
        self._root = root
        self._children_by_parent_ref = children_by_parent_ref
        self._units_by_ref = units_by_ref

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        children = self._children_by_parent_ref.get(str(node.ref), [])
        stop = offset + limit if limit is not None else None
        return children[offset:stop]

    async def unit(self, node: Node) -> RestorableUnit:
        return self._units_by_ref[str(node.ref)]

    async def close(self) -> None:
        pass


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
    leaf = RestorableUnit(ref=leaf_ref, name="leaf1.txt", is_leaf=True, content=LazyArtifact(_unread_content))
    return _FakeUnitProvider(
        root=root,
        children_by_parent_ref={
            str(root_ref): [folder],
            str(folder_ref): [leaf],
        },
        units_by_ref={str(leaf_ref): leaf},
    )


@faithful_to(UnitProvider, SupportsDirectRefLookup)
class _FlatIdFakeProvider:
    """Drive's shape: every node's ``extra_segments`` is a single flat id
    regardless of depth, which prefix-guided descent can't reach — only
    ``SupportsDirectRefLookup`` (``resolve_extra``) resolves it. Real
    provider coverage lives in ``test_units_saas_drive.py``."""

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
    """A repository with one fixed catalog/workload/version, faked at the
    module boundary: a canonical ref built from ``_RESOLVE_CCID``/
    ``_RESOLVE_WORKLOAD_ID``/``_RESOLVE_VERSION_UID`` resolves to that
    version, whose tree is ``provider``. The ``M365`` target type routes
    ``Catalog.provider()`` through ``workload_by_id``/``saas_provider_for``
    (also faked), the dispatch path a real SaaS version takes."""
    repo = repo_with_fake_dedup(monkeypatch, FakeDedupRepo(vault_layout()))
    connection = make_connection()
    workload = make_workload(workload_id=_RESOLVE_WORKLOAD_ID, workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=_RESOLVE_WORKLOAD_ID, target_type="M365")
    version = dataclasses.replace(version, version_uid=_RESOLVE_VERSION_UID, connection_config_id=_RESOLVE_CCID)

    async def fake_connections(dedup_repo: object) -> list[Connection]:
        return [connection]

    async def fake_workloads(dedup_repo: object, c: Connection) -> list[Workload]:
        return [workload] if c is connection else []

    async def fake_versions(dedup_repo: object, w: Workload, **k: object) -> list[Version]:
        return [version] if w is workload else []

    async def fake_workload_by_id(dedup_repo: object, workload_id: object) -> Workload | None:
        return workload if workload_id == workload.workload_id else None

    async def fake_version_by_uid(dedup_repo: object, version_uid: object) -> Version | None:
        return version if version_uid == version.version_uid else None

    async def fake_saas_provider_for(dedup_repo: object, w: object, v: object, saas_streams: object) -> UnitProvider:
        assert v is version
        return provider

    monkeypatch.setattr(api_repository, "connections", fake_connections)
    monkeypatch.setattr(api_catalog, "workloads", fake_workloads)
    monkeypatch.setattr(api_catalog, "versions", fake_versions)
    monkeypatch.setattr(api_catalog, "workload_by_id", fake_workload_by_id)
    monkeypatch.setattr(api_catalog, "version_by_uid", fake_version_by_uid)
    monkeypatch.setattr(api_catalog, "saas_provider_for", fake_saas_provider_for)
    return repo


def _session_with_repo(repo: api.Repository) -> api.Session:
    session = api.Session()
    session._repos[repo] = repo._store
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
    frame = await session.resolve(str(leaf_ref))
    assert frame.version is not None and frame.version.version_uid == _RESOLVE_VERSION_UID
    assert frame.workload is not None and frame.workload.workload_id == _RESOLVE_WORKLOAD_ID
    resolved = await frame.unit()
    assert resolved.name == "leaf1.txt"
    assert isinstance(resolved, RestorableUnit)


async def test_node_frame_unit_on_a_folder_raises_not_restorable() -> None:
    folder = Node(ref=NodeRef.raw("", "folder"), name="folder", is_leaf=False)
    with pytest.raises(NotRestorableError, match="is not a single restorable item"):
        await api.NodeFrame(cast(Any, object()), folder).unit()


async def test_resolve_canonical_ref_flat_id_leaf_via_direct_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ``_FlatIdFakeProvider`` leaf resolves via
    ``SupportsDirectRefLookup``."""
    root_ref = NodeRef.canonical(
        "", catalog_id=CatalogId(str(_RESOLVE_CCID)), workload_id=_RESOLVE_WORKLOAD_ID, version_uid=_RESOLVE_VERSION_UID
    )
    root = Node(ref=root_ref, name="root", is_leaf=False)
    flat_leaf_ref = NodeRef(root_ref.repo_path, (*root_ref.segments, "flatid"))
    flat_leaf = RestorableUnit(ref=flat_leaf_ref, name="flat.txt", is_leaf=True, content=LazyArtifact(_unread_content))
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
    resolved = await (await session.resolve(str(flat_ref))).unit()
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
    resolved = await (await session.resolve(leaf_ref)).unit()  # NodeRef, not str
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
    frame = await session.resolve(str(folder_ref))
    assert frame.node is not None
    assert frame.node.name == "Folder"
    assert not frame.node.is_leaf
    with pytest.raises(NotRestorableError, match="is not a single restorable item"):
        await frame.unit()


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
    with pytest.raises(NotFoundError, match="no node in provider tree matches ref"):
        await session.resolve(str(missing_ref))


async def test_resolve_malformed_canonical_ref_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    # kind is CANONICAL (starts with "cat:") but missing the wl:/ver: parts.
    malformed = NodeRef("", ("cat:1",))
    with pytest.raises(NotFoundError, match="malformed canonical ref"):
        await session.resolve(str(malformed))


async def test_resolve_canonical_ref_unknown_catalog_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    ref = NodeRef.canonical(
        "", catalog_id=CatalogId("999"), workload_id=_RESOLVE_WORKLOAD_ID, version_uid=VersionUid("x")
    )
    with pytest.raises(NotFoundError, match="no catalog with catalog_id"):
        await session.resolve(str(ref))


async def test_resolve_canonical_ref_unknown_workload_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    # The version uid exists, but under _RESOLVE_WORKLOAD_ID, not 999.
    ref = NodeRef.canonical(
        "",
        catalog_id=CatalogId(str(_RESOLVE_CCID)),
        workload_id=WorkloadId(999),
        version_uid=VersionUid(_RESOLVE_VERSION_UID),
    )
    with pytest.raises(NotFoundError, match=r"no version with version_uid=.* under workload_id=999"):
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
    with pytest.raises(NotFoundError, match="no version with version_uid"):
        await session.resolve(str(ref))


async def test_resolve_raw_ref_finds_a_leaf_in_file_map_tree(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = api.Repository(as_object_store(FakeStore()), vault_repository_layout(), keys=None, key_verification=None)
    leaf_ref = NodeRef.raw("", "dir/file.bin")
    leaf = RestorableUnit(ref=leaf_ref, name="file.bin", is_leaf=True, content=LazyArtifact(_unread_content))
    root_ref = NodeRef.raw("", "")
    root = Node(ref=root_ref, name="/", is_leaf=False)
    dir_ref = NodeRef.raw("", "dir")
    dir_node = Node(ref=dir_ref, name="dir", is_leaf=False)
    provider = _FakeUnitProvider(
        root=root,
        children_by_parent_ref={str(root_ref): [dir_node], str(dir_ref): [leaf]},
        units_by_ref={str(leaf_ref): leaf},
    )
    monkeypatch.setattr(repo, "file_map_tree", async_returning(provider))
    session = _session_with_repo(repo)

    resolved = await (await session.resolve(str(leaf_ref))).unit()
    assert resolved.name == "file.bin"


async def test_resolve_ref_with_no_matching_open_repo_raises_not_found() -> None:
    session = api.Session()
    ref = NodeRef.canonical(
        "some-other-root",
        catalog_id=CatalogId("1"),
        workload_id=WorkloadId(1),
        version_uid=VersionUid("x"),
    )
    with pytest.raises(NotFoundError, match="no open repository matches ref"):
        await session.resolve(str(ref))


async def test_repo_for_ref_matches_object_store_catalog_level_repo_root() -> None:
    """A node from an OBJECT_STORE repository with enumerable ``catalog_ids``
    carries its catalog-level ``RepoLayout.repo_root`` (see
    ``catalog_repo_layouts()``), which differs from the bucket-level
    ``RepositoryLayout.repo_root``; ``_owns_repo_path`` must match the former."""
    bucket_layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="bucket", catalog_ids=["repo-a"])
    repo = api.Repository(as_object_store(FakeStore()), bucket_layout, keys=None, key_verification=None)
    catalog_repo_root = catalog_repo_layouts(bucket_layout)[0].repo_root
    assert catalog_repo_root != bucket_layout.repo_root  # the two roots really differ

    assert repo._owns_repo_path(catalog_repo_root)
    assert not repo._owns_repo_path(bucket_layout.repo_root)

    session = _session_with_repo(repo)
    assert session._repo_for_ref(NodeRef(catalog_repo_root, ("x",))) is repo


async def test_resolve_human_ref_too_few_segments_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    ref = NodeRef.human("", "OnlyOneSegment")
    with pytest.raises(NotFoundError, match="human ref must name at least a catalog, workload, and version"):
        await session.resolve(str(ref))


async def test_resolve_human_ref_unknown_connection_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    ref = NodeRef.human("", "NoSuchSource", "wl", "ver")
    with pytest.raises(NotFoundError, match="no backup source named"):
        await session.resolve(str(ref))


async def test_resolve_human_ref_unknown_workload_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    connection = make_connection()
    ref = NodeRef.human("", connection.display_name, "NoSuchWorkload", "ver")
    with pytest.raises(NotFoundError, match="no workload named"):
        await session.resolve(str(ref))


async def test_resolve_human_ref_unknown_version_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo_with_tree(monkeypatch, _make_tree())
    session = _session_with_repo(repo)
    connection = make_connection()
    workload = make_workload(workload_id=_RESOLVE_WORKLOAD_ID, workload_type="M365", sub_type="MAIL")
    ref = NodeRef.human("", connection.display_name, workload.display_name, "NoSuchVersion")
    with pytest.raises(NotFoundError, match="no version named"):
        await session.resolve(str(ref))


async def test_resolve_human_ref_down_to_a_leaf_by_display_name(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)
    connection = make_connection()
    workload = make_workload(workload_id=_RESOLVE_WORKLOAD_ID, workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=_RESOLVE_WORKLOAD_ID, target_type="M365")
    version = dataclasses.replace(version, version_uid=_RESOLVE_VERSION_UID, connection_config_id=_RESOLVE_CCID)

    ref = NodeRef.human("", connection.display_name, workload.display_name, version.display_name, "Folder", "leaf1.txt")
    resolved = await (await session.resolve(str(ref))).unit()
    assert resolved.name == "leaf1.txt"


async def test_resolve_human_ref_names_more_levels_than_tree_has_raises_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)
    connection = make_connection()
    workload = make_workload(workload_id=_RESOLVE_WORKLOAD_ID, workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=_RESOLVE_WORKLOAD_ID, target_type="M365")
    version = dataclasses.replace(version, version_uid=_RESOLVE_VERSION_UID, connection_config_id=_RESOLVE_CCID)

    ref = NodeRef.human(
        "", connection.display_name, workload.display_name, version.display_name, "Folder", "leaf1.txt", "too-deep"
    )
    with pytest.raises(NotFoundError, match="ref names more levels than the tree has"):
        await session.resolve(str(ref))


async def test_resolve_human_ref_unknown_item_name_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_tree()
    repo = _repo_with_tree(monkeypatch, provider)
    session = _session_with_repo(repo)
    connection = make_connection()
    workload = make_workload(workload_id=_RESOLVE_WORKLOAD_ID, workload_type="M365", sub_type="MAIL")
    version = make_version(workload_id=_RESOLVE_WORKLOAD_ID, target_type="M365")
    version = dataclasses.replace(version, version_uid=_RESOLVE_VERSION_UID, connection_config_id=_RESOLVE_CCID)

    ref = NodeRef.human("", connection.display_name, workload.display_name, version.display_name, "NoSuchChild")
    with pytest.raises(NotFoundError, match="no item named"):
        await session.resolve(str(ref))
