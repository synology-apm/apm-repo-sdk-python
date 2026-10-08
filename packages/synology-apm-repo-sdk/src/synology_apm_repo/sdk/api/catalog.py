"""``Catalog``: one catalog's workload/version/provider operations, as
returned by ``Repository.catalogs()``/``Repository.catalog_by_id()``; and the
frames a ref lands on."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence

from ..catalog.connection import Connection
from ..catalog.version import Version, version_by_uid, versions
from ..catalog.workload import DEVICE_TARGET_TYPES, Workload, workload_by_id, workloads
from ..dedup.repository import DedupRepo
from ..errors import NotFoundError, NotRestorableError
from ..findings import Finding, VerifyLevel
from ..format.repo_info import RepoInfo
from ..identifiers import CatalogId, VersionUid, WorkloadId, resolve_catalog_id
from ..presentation.progress import ProgressCallback
from ..units.base import ClosableUnitProvider, Node, RestorableUnit
from ..units.dispatch import provider_for, saas_provider_for
from ..units.node_ref import ambiguous_matches, match_display_name, version_pairs, workload_pairs
from ..units.saas.raw_object import RawObjectProvider
from ..units.saas.stream import SaasStreamCache
from ..units.verify_reachable import verify_reachable
from .key_manager import KeyManager
from .provider_registry import ProviderRegistry


@dataclasses.dataclass(frozen=True, slots=True)
class RootFrame:
    """A ref with no segments: the repository's own top level."""


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogFrame:
    """A ref that stopped at a catalog."""

    catalog: Catalog


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadFrame:
    """A ref that stopped at a workload."""

    catalog: Catalog
    workload: Workload


@dataclasses.dataclass(frozen=True, slots=True)
class NodeFrame:
    """A ref that reached a node in a version's tree. A version always
    continues into its tree, so a ref naming one lands on its root node.
    ``catalog``/``workload``/``version`` are kept for breadcrumbs and are
    ``None`` for a raw ref. The owning ``Repository`` tracks ``provider``
    until ``Repository.release_provider`` or the repository's close."""

    provider: ClosableUnitProvider
    node: Node
    catalog: Catalog | None = None
    workload: Workload | None = None
    version: Version | None = None

    async def unit(self) -> RestorableUnit:
        """The restorable item this frame's node names. It reads through
        ``provider``, so release that provider only once done with the item.

        Raises:
            NotRestorableError: The node is a folder.
        """
        if not self.node.is_leaf:
            raise NotRestorableError(f"{self.node.name!r} is not a single restorable item")
        return await self.provider.unit(self.node)


@dataclasses.dataclass(frozen=True, slots=True)
class RawView:
    """Asks for a SaaS version's raw view: its object-name index listed
    as-is by ``RawObjectProvider``, with no application-layer decoding — a
    diagnostic view exposing internal names and object ids. A device
    version has no raw view and ignores it.

    Attributes:
        object_db_id: List this ObjectDB's objects by id
            (``"<streamUuid>_<offset>_<length>"``) instead of the index's
            named entries.
    """

    object_db_id: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class VersionLocation:
    """A version with the catalog and workload it belongs to."""

    catalog: Catalog
    workload: Workload
    version: Version


# Where a ref lands: ``Repository.locate`` returns any of these,
# ``Repository.resolve`` only a ``NodeFrame``.
Frame = RootFrame | CatalogFrame | WorkloadFrame | NodeFrame


class Catalog:
    """One catalog within an opened ``Repository``: a single ``db/
    connection_config`` row (``connection``) plus the workload/version/
    provider operations scoped to it. Never constructed directly.

    A vault's sibling ``Catalog``s share one ``DedupRepo``; on object
    storage each has its own (FORMAT-SPEC.md: Locating the repository root). ``workloads``,
    ``versions``, ``version_by_uid`` and ``verify`` raise
    ``KeyRequiredError``/``KeyMismatchError`` before any I/O while the
    owning repository is encrypted and not key-verified.
    Providers it hands out are tracked and closed by the owning
    ``Repository``; ``Catalog`` itself has no ``close()``.
    """

    def __init__(
        self,
        dedup_repo: DedupRepo,
        connection: Connection,
        *,
        saas_streams: SaasStreamCache,
        providers: ProviderRegistry,
        keys: KeyManager,
    ) -> None:
        self._dedup_repo = dedup_repo
        self._connection = connection
        self._saas_streams = saas_streams
        self._providers = providers
        self._keys = keys

    @property
    def connection(self) -> Connection:
        """The ``db/connection_config`` row this catalog is."""
        return self._connection

    @property
    def display_name(self) -> str:
        """The connection's display name."""
        return self.connection.display_name

    @property
    def catalog_id(self) -> CatalogId:
        """This catalog's repo-id, or ``str(connection_config_id)`` for a
        vault (which has no repo-id), as canonical refs spell it."""
        return resolve_catalog_id(self._dedup_repo.layout.repo_id, self.connection.connection_config_id)

    @property
    def info(self) -> RepoInfo:
        """This catalog's own ``repo_info`` — per-catalog for object
        storage, vault-wide for a vault."""
        return self._dedup_repo.info

    async def workloads(self) -> list[Workload]:
        """This catalog's workloads."""
        self._keys.require_verified()
        return await workloads(self._dedup_repo, self.connection)

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        """``workload``'s browsable versions, newest backup first (ties by
        ``version_id`` descending), deleted ones only with
        ``include_deleted``. A version whose content can't be resolved
        fails only when opened."""
        self._keys.require_verified()
        return await versions(self._dedup_repo, workload, include_deleted=include_deleted)

    async def version_by_uid(self, workload_id: WorkloadId, version_uid: VersionUid) -> VersionLocation | None:
        """The version ``version_uid`` of workload ``workload_id``, deleted
        or not, looked up by id. ``None`` unless that
        version exists, is browsable, and belongs to ``workload_id`` and
        to this catalog."""
        self._keys.require_verified()
        version = await version_by_uid(self._dedup_repo, version_uid)
        if (
            version is None
            or version.workload_id != workload_id
            or version.connection_config_id != self.connection.connection_config_id
        ):
            return None
        workload = await workload_by_id(self._dedup_repo, workload_id)
        return VersionLocation(self, workload, version) if workload is not None else None

    async def provider(self, version: Version, *, raw: RawView | None = None) -> ClosableUnitProvider:
        """Dispatches ``version`` to the right ``UnitProvider`` by
        ``target_type``/``Workload.sub_type``, degrading to
        ``RawObjectProvider`` when nothing recognizes it; ``raw`` asks for
        that raw view directly.

        Raises:
            NotFoundError: ``raw.object_db_id`` is malformed or names
                another stream.
        """
        return await _provider_for_version(
            self._dedup_repo,
            version,
            saas_streams=self._saas_streams,
            raw=raw,
            track=self._providers.track,
        )

    async def verify(
        self,
        level: VerifyLevel = VerifyLevel.QUICK,
        *,
        progress: ProgressCallback | None = None,
    ) -> list[Finding]:
        """Integrity check over this catalog's ``DedupRepo`` — on a vault,
        the same check as every sibling catalog's, not one scoped to this
        catalog's rows.

        Args:
            level: How deep to check.
            progress: Receives a ``Progress`` snapshot as checking advances.

        Returns:
            Every ``Finding`` from the check.

        Raises:
            StorageBackendError: A store call failed mid-check.
        """
        self._keys.require_verified()
        return await verify_reachable(self._dedup_repo, level, progress=progress)

    async def locate(self, segments: Sequence[str], *, raw: RawView | None = None) -> WorkloadFrame | NodeFrame:
        """Walks human-ref display names below this catalog: workload ->
        version -> item, as far as ``segments`` go.

        Args:
            segments: The ref's segments below the catalog.
            raw: Passed to ``provider()``.

        Returns:
            The frame the last segment reached.

        Raises:
            ValueError: ``segments`` is empty.
            NotFoundError: A segment matches nothing, or matches ambiguously.
            KeyRequiredError: The repository is encrypted and no key was
                supplied.
            KeyMismatchError: The last key supplied was rejected.
        """
        if not segments:
            raise ValueError("Catalog.locate needs at least a workload segment")
        workloads_list = await self.workloads()
        pairs, hints = workload_pairs(workloads_list)
        workload = match_or_raise(segments[0], pairs, workloads_list, kind="workload", hints=hints)
        if len(segments) == 1:
            return WorkloadFrame(self, workload)

        versions_list = await self.versions(workload)
        version = match_or_raise(segments[1], version_pairs(versions_list), versions_list, kind="version")
        provider = await self.provider(version, raw=raw)
        try:
            node = provider.root()
            for name in segments[2:]:
                if node.is_leaf:
                    raise NotFoundError(f"ref names more levels than the tree has (stopped at {node.name!r})", ref=name)
                children = await provider.children(node)
                node = match_or_raise(name, [(c.name, str(c.ref)) for c in children], children, kind="item")
        except BaseException as exc:
            # The caller never sees this provider, so it can't release it.
            await self._providers.release_after(exc, provider)
            raise
        return NodeFrame(provider, node, catalog=self, workload=workload, version=version)


async def _provider_for_version(
    dedup_repo: DedupRepo,
    version: Version,
    *,
    saas_streams: SaasStreamCache,
    raw: RawView | None,
    track: Callable[[ClosableUnitProvider], ClosableUnitProvider],
) -> ClosableUnitProvider:
    if version.target_type in DEVICE_TARGET_TYPES:
        return track(await provider_for(dedup_repo, version))
    if raw is not None:
        return track(await RawObjectProvider.create(dedup_repo, version, saas_streams, object_db_id=raw.object_db_id))
    workload = await workload_by_id(dedup_repo, version.workload_id)
    if workload is None:
        return track(await RawObjectProvider.create(dedup_repo, version, saas_streams))
    return track(await saas_provider_for(dedup_repo, workload, version, saas_streams))


def match_or_raise[T](
    segment: str,
    pairs: Sequence[tuple[str, str]],
    objects: Sequence[T],
    *,
    kind: str,
    hints: Sequence[str | None] | None = None,
) -> T:
    """``match_display_name``, raising ``NotFoundError`` on a miss; when
    ``segment`` is the pre-suffix name of several candidates the message
    names them (see ``ambiguous_matches``)."""
    matched = match_display_name(segment, pairs, objects, hints=hints)
    if matched is not None:
        return matched
    candidates = ambiguous_matches(segment, pairs, hints=hints)
    if candidates:
        raise NotFoundError(
            f"{kind} named {segment!r} is ambiguous ({len(candidates)} matches) — "
            f"use one of: {', '.join(repr(c) for c in candidates)}",
            ref=segment,
        )
    raise NotFoundError(f"no {kind} named {segment!r}", ref=segment)
