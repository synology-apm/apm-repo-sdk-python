"""Fakes the UnitScreen Pilot tests drive the screen with: a configurable provider, a catalog/repository pair, and a minimal app hosting the screen."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from textual.screen import Screen

from support.fakes import faithful_to
from synology_apm_repo.browser.core.app.cmd import AppCmd
from synology_apm_repo.browser.core.app.model import AppModel
from synology_apm_repo.browser.core.app.msg import AppMsg
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.runtime.app_effects import AppEffects
from synology_apm_repo.browser.runtime.load_gate import LoadGate
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Catalog, Finding, Repository, Session, VerifyLevel, Version
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from synology_apm_repo.sdk.units.base import ClosableUnitProvider, ContentSource, Node, RestorableUnit, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.screen_host_fakes import ScreenHostApp


@faithful_to(ContentSource)
class FakeContentSource:
    """``size`` stays ``None`` until the first ``read``, like ``LazyArtifact``."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._read = False

    @property
    def size(self) -> int | None:
        return len(self._data) if self._read else None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        self._read = True
        return self._data[offset:] if length is None else self._data[offset : offset + length]


@faithful_to(UnitProvider)
class ConfigurableProvider:
    """A ``UnitProvider`` over a ``ref -> children`` map; ``children()``
    raises ``ApmRepoError`` for refs in ``raise_children_for``, and
    ``units_by_ref`` backs ``unit()``."""

    def __init__(
        self,
        root: Node,
        children_by_ref: dict[str, list[Node]] | None = None,
        *,
        raise_children_for: set[str] = frozenset(),  # type: ignore[assignment]
        units_by_ref: dict[str, RestorableUnit] | None = None,
    ) -> None:
        self._root = root
        self._children_by_ref = children_by_ref or {}
        self._raise_children_for = raise_children_for
        self._units_by_ref = units_by_ref or {}

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if str(node.ref) in self._raise_children_for:
            raise ApmRepoError(f"boom at {node.ref}")
        items = self._children_by_ref.get(str(node.ref), [])
        return items[offset : offset + limit] if limit is not None else items[offset:]

    async def unit(self, node: Node) -> RestorableUnit:
        unit = self._units_by_ref.get(str(node.ref))
        if unit is None:
            raise NotImplementedError(f"no fake unit registered for {node.ref}")
        return unit


@faithful_to(Catalog)
class _FakeCatalog:
    """Stands in for ``api.Catalog``."""

    def __init__(self, provider: object | None, *, provider_error: ApmRepoError | None = None) -> None:
        self._provider = provider
        self._provider_error = provider_error
        self.provider_calls = 0

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(
        self, version: Version, *, raw: object = None
    ) -> Any:  # any provider-shaped fake a test drives UnitScreen with
        self.provider_calls += 1
        if self._provider_error is not None:
            raise self._provider_error
        assert self._provider is not None
        return self._provider


@faithful_to(Repository)
class FakeRepo:
    def __init__(
        self,
        provider: object | None,
        *,
        provider_error: ApmRepoError | None = None,
        version_for_ref_error: ApmRepoError | None = None,
        version_for_ref_result: Version | None = None,
    ) -> None:
        self.catalog = _FakeCatalog(provider, provider_error=provider_error)
        self._version_for_ref_error = version_for_ref_error
        self._version_for_ref_result = version_for_ref_result
        self.invalidate_caches_calls = 0

    async def version_for_ref(self, ref: NodeRef) -> Any:  # a VersionLocation-shaped namespace
        if self._version_for_ref_error is not None:
            raise self._version_for_ref_error
        assert self._version_for_ref_result is not None
        return SimpleNamespace(catalog=self.catalog, workload=None, version=self._version_for_ref_result)

    async def verify(self, level: VerifyLevel = VerifyLevel.QUICK, *, progress: object = None) -> list[Finding]:
        return []

    async def invalidate_caches(self, *names: str) -> None:
        self.invalidate_caches_calls += 1

    async def release_provider(self, provider: object) -> None:
        if isinstance(provider, ClosableUnitProvider):
            await provider.close()


class FakeApp(ScreenHostApp):
    """Hosts ``UnitScreen`` over ``repo``; ``store`` is a working one because
    ``ExportScreen`` subscribes to it."""

    def __init__(self, version: Version, repo: FakeRepo, *, target_ref: NodeRef | None = None) -> None:
        super().__init__(repo=repo, session=cast(Session, object()))
        self._repo = repo
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, self._perform)
        self.effects = AppEffects(self, self.store, LoadGate())
        self._version = version
        self._target_ref = target_ref

    def _perform(self, cmd: AppCmd) -> None:
        self.effects.perform(cmd)

    def screens(self) -> list[Screen[Any]]:
        return [UnitScreen(self._repo.catalog, self._version, target_ref=self._target_ref)]  # type: ignore[arg-type]


def leaf_node(name: str, ref_segment: str, **kwargs: object) -> Node:
    return Node(ref=NodeRef("repo", ("root", ref_segment)), name=name, is_leaf=True, **kwargs)  # type: ignore[arg-type]


@faithful_to(UnitProvider)
class PagedTreeProvider:
    """A fixed tree: ``children_by_ref`` maps ``str(node.ref)`` to its
    children, paged by ``offset``/``limit``; ``unit`` returns the node."""

    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]]) -> None:
        self._root = root
        self._children_by_ref = children_by_ref

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        all_children = self._children_by_ref.get(str(node.ref), [])
        stop = offset + limit if limit is not None else None
        return all_children[offset:stop]

    async def unit(self, node: Node) -> Node:
        return node
