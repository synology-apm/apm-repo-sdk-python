"""Fakes the listing commands' tests (``ls``, ``tree``, ``browse``) and
``doctor``'s share: a ``Catalog`` with fixed workloads/versions and a
repository serving fixed catalogs."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any, cast

import pytest

from support.fakes import faithful_to
from synology_apm_repo.cli import browse as browse_module
from synology_apm_repo.sdk.api import (
    Catalog,
    Connection,
    Frame,
    KeyStatus,
    KeyVerification,
    Repository,
    Version,
    Workload,
)
from synology_apm_repo.sdk.api.key_manager import KeyManager
from synology_apm_repo.sdk.api.provider_registry import ProviderRegistry
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.repo_info import RepoInfo
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout
from synology_apm_repo.sdk.units.base import Node, UnitProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache


class FakeDedupRepo:
    """A minimal stand-in for ``dedup.repository.DedupRepo``, exposing
    just what ``Catalog.catalog_id``/``Catalog.info`` read (``layout.repo_id``,
    ``info``)."""

    def __init__(self) -> None:
        self.layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        self.info = RepoInfo(
            uuid="repo-uuid-1",
            major=1,
            minor=0,
            repo_type=2,
            repo_flag=None,
            is_global_dedup_supported=None,
            is_worm_supported=None,
            compress_algorithm=None,
            encrypt_algorithm=None,
            raw={},
        )


class FakeCatalog(Catalog):
    """A real ``Catalog`` (so ``isinstance(obj, Catalog)`` checks still
    hold) with its ``workloads()``/``versions()`` overridden to return
    fixed lists instead of reading a ``DedupRepo``."""

    def __init__(
        self,
        connection: Connection,
        *,
        workloads: list[Workload] | None = None,
        versions: list[Version] | None = None,
    ) -> None:
        dedup_repo = cast(DedupRepo, FakeDedupRepo())
        super().__init__(
            dedup_repo,
            connection,
            saas_streams=SaasStreamCache(dedup_repo),
            providers=ProviderRegistry(),
            keys=KeyManager(None, None),
        )
        self._fake_workloads = workloads or []
        self._fake_versions = versions or []

    async def workloads(self) -> list[Workload]:
        return self._fake_workloads

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        return self._fake_versions


@faithful_to(Repository)
class FakeRepo:
    """Serves fixed catalogs, and ``locate()`` lands on ``frame``; a
    workload is supported when its ``workload_type`` is in ``supported_types``."""

    def __init__(
        self,
        *,
        catalogs: list[Catalog] | None = None,
        frame: Frame | None = None,
        layout: RepositoryLayout | None = None,
        key_status: KeyStatus = KeyStatus.NOT_ENCRYPTED,
        is_encrypted: bool | None = False,
        key_verification: KeyVerification | None = None,
        supported_types: Collection[str] = ("VM",),
    ) -> None:
        self._catalogs = catalogs or []
        self.frame = frame
        self.layout = layout if layout is not None else RepositoryLayout(kind=RepoKind.VAULT, repo_root="")
        self.key_status = key_status
        self.is_encrypted = is_encrypted
        self.key_verification = key_verification
        self._supported_types = supported_types

    async def catalogs(self) -> list[Catalog]:
        return self._catalogs

    def workload_is_supported(self, workload: Workload) -> bool:
        return workload.workload_type in self._supported_types

    async def locate(self, ref: object, *, raw: object = None) -> Frame:
        assert self.frame is not None, "this test never set the frame its REF lands on"
        return self.frame


class _FakeRepoCtx:
    """What ``opened_repo`` returns: an async context yielding ``repo``."""

    def __init__(self, repo: object) -> None:
        self._repo = repo

    async def __aenter__(self) -> object:
        return self._repo

    async def __aexit__(self, *exc: object) -> None:
        return None


def patch_walked_frame(monkeypatch: pytest.MonkeyPatch, frame: Frame, repo: FakeRepo | None = None) -> None:
    """Make every listing command's REF open ``repo`` and land on ``frame``."""
    repo = repo if repo is not None else FakeRepo()
    repo.frame = frame
    monkeypatch.setattr(browse_module, "opened_repo", lambda *a, **k: _FakeRepoCtx(repo))


@faithful_to(UnitProvider)
class FlatProvider:
    """``root`` with ``children`` directly below it and nothing deeper;
    ``unit`` is never expected."""

    def __init__(self, root_node: Node, children: list[Node]) -> None:
        self._root_node = root_node
        self._children = children

    def root(self) -> Node:
        return self._root_node

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children if node == self._root_node else []

    async def unit(self, node: Node) -> Any:
        raise NotImplementedError
