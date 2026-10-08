"""Fakes and builders the ``api`` unit tests (``test_api_session.py``,
``test_api_catalog.py``, ``test_api_repository.py``) share: duck-typed
``ObjectStore``/``DedupRepo`` stand-ins and a ``Catalog`` over a test's
``Repository``."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable
from typing import Any, cast

import pytest

from support.fakes import faithful_to
from support.model_factories import make_catalog
from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.api import catalog as api_catalog
from synology_apm_repo.sdk.api import repository as api_repository
from synology_apm_repo.sdk.api import session as api_session
from synology_apm_repo.sdk.cachemanager import CacheManager
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout


@faithful_to(ObjectStore)
class FakeStore:
    """A duck-typed store whose ``exists()`` always answers ``False``, so
    ``Session``'s ``_probe_encrypted`` returns ``None`` without reading a key
    record. A test that needs a specific encrypted/not-encrypted outcome
    monkeypatches ``api_session._probe_encrypted`` instead."""

    async def exists(self, path: str) -> bool:
        return False

    async def close(self) -> None:
        pass


def as_object_store(fake: FakeStore) -> ObjectStore:
    return cast(ObjectStore, fake)


@faithful_to(DedupRepo)
class FakeDedupRepo:
    """Stands in for ``DedupRepo`` with just the surface ``Repository`` wires
    through; cast at each call site (``as_dedup_repo``)."""

    def __init__(self, layout: RepoLayout, *, info: object = "fake-info") -> None:
        self.store = FakeStore()
        self.layout = layout
        self.info = info
        self.caches = CacheManager()
        self.closed = False
        self.close_count = 0

    async def close(self) -> None:
        self.closed = True
        self.close_count += 1

    async def open_file(self, path: str, *, fallback_size: int | None = None) -> str:
        return f"opened:{path}"


def as_dedup_repo(fake: FakeDedupRepo) -> DedupRepo:
    return cast(DedupRepo, fake)


def vault_layout(repo_root: str = "") -> RepoLayout:
    return RepoLayout(kind=RepoKind.VAULT, repo_root=repo_root)


def vault_repository_layout(repo_root: str = "") -> RepositoryLayout:
    """The ``RepositoryLayout`` ``api.Repository`` takes. A ``VAULT`` layout
    resolves (``catalog_repo_layouts()``) to exactly one ``RepoLayout``, so
    one ``FakeDedupRepo`` is the whole repository."""
    return RepositoryLayout(kind=RepoKind.VAULT, repo_root=repo_root)


def async_iter_repository_layouts(layouts: list[RepositoryLayout]) -> Any:
    """An async-generator stand-in for ``storage.layout.iter_repository_layouts``."""

    async def _iter(store: object, root: str = "") -> AsyncIterator[RepositoryLayout]:
        for layout in layouts:
            yield layout

    return _iter


def async_returning(value: Any) -> Any:
    """A coroutine function that ignores its arguments and returns
    ``value``."""

    async def _fn(*a: object, **k: object) -> Any:
        return value

    return _fn


def patch_dedup_open(monkeypatch: pytest.MonkeyPatch, opener: Callable[..., Any] | None = None) -> None:
    """Make ``DedupRepo.open(store, layout, keys, **kwargs)`` return
    ``opener(...)``'s result, by default a ``FakeDedupRepo(layout)``.
    ``opener`` may be a plain factory or a coroutine function taking the
    same arguments."""
    if opener is None:
        opener = lambda store, layout, keys, **kwargs: FakeDedupRepo(layout)  # noqa: E731

    async def _open(cls: object, /, *a: object, **k: object) -> DedupRepo:
        result = opener(*a, **k)
        return cast(DedupRepo, await result if inspect.isawaitable(result) else result)

    monkeypatch.setattr(DedupRepo, "open", classmethod(_open))


def patch_layouts(monkeypatch: pytest.MonkeyPatch, layouts: list[RepositoryLayout]) -> None:
    """Make ``Session`` discovery find exactly ``layouts`` in any store."""
    monkeypatch.setattr(api_session, "iter_repository_layouts", async_iter_repository_layouts(layouts))


# parse_key_string requires a 12-char userKeyID and a userKey decoding to
# exactly 32 bytes, even for "NoEncryption".
VALID_B64_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def no_encryption_keys() -> KeyMaterial:
    return KeyMaterial.from_key_string(f"NoEncryption@{VALID_B64_KEY}")


def some_key(user_key_id: str) -> KeyMaterial:
    assert len(user_key_id) == 12
    return KeyMaterial.from_key_string(f"{user_key_id}@{VALID_B64_KEY}")


@faithful_to(DedupRepo)
class FakeRaisingDedupRepo(FakeDedupRepo):
    """Like ``FakeDedupRepo``, but ``close()`` raises, for the
    "attempt every close, then report" paths."""

    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("synthetic close failure")


def repo_catalog(
    repo: api.Repository, dedup_repo: DedupRepo, *, connection: Connection | None = None
) -> api_catalog.Catalog:
    """A ``Catalog`` sharing ``repo``'s provider registry and key manager,
    without the ``repo.catalogs()`` round-trip (which would also need
    ``connections()``/``DedupRepo.open()`` faked)."""
    return make_catalog(connection, dedup_repo=dedup_repo, providers=repo._providers, keys=repo._key_manager)


def repo_with_fake_dedup(
    monkeypatch: pytest.MonkeyPatch,
    fake: FakeDedupRepo,
    *,
    keys: KeyMaterial | None = None,
    key_verification: KeyVerification | None = None,
    encrypted: bool | None = None,
    layout: RepositoryLayout | None = None,
    connections: list[Connection] | None = None,
) -> api.Repository:
    """An ``api.Repository`` whose ``DedupRepo.open`` returns ``fake`` for any
    layout, and whose ``connections`` returns ``connections`` when given.
    Construction opens nothing; a test only reading ``key_status``/
    ``is_encrypted`` can use a bare ``api.Repository(...)``."""
    patch_dedup_open(monkeypatch, lambda *a, **k: fake)
    if connections is not None:
        monkeypatch.setattr(api_repository, "connections", async_returning(connections))
    return api.Repository(
        as_object_store(FakeStore()),
        layout if layout is not None else vault_repository_layout(),
        keys,
        key_verification,
        encrypted=encrypted,
    )
