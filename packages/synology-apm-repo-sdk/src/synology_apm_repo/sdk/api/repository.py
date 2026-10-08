"""``Repository``: one opened repository's catalog/provider facade, and
``locate()``/``resolve()``'s ref dispatch across its catalogs."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterable

from .._util.closing import RESOURCE_CLOSE_TIMEOUT, close_each, close_preserving
from ..asynccache import AsyncKeyedCache, CacheStats
from ..cachemanager import DEFAULT_LIMITS
from ..catalog.connection import Connection, connections
from ..catalog.workload import Workload
from ..dedup.keys import KeyMaterial, KeyVerification
from ..dedup.repository import DedupRepo
from ..dedup.verify_bucket_check import shared_verify_executor
from ..errors import NotFoundError
from ..findings import Finding, VerifyLevel
from ..identifiers import CatalogId
from ..presentation.format import pluralize
from ..presentation.progress import ProgressCallback
from ..storage.base import ObjectStore
from ..storage.layout import RepositoryLayout, catalog_repo_layouts
from ..units.base import ClosableUnitProvider, Node, UnitProvider
from ..units.dispatch import is_supported as _workload_is_supported
from ..units.file_map_tree import FileMapTreeProvider
from ..units.node_ref import NodeRef, RefKind, catalog_pairs
from ..units.resolve import find_node
from ..units.saas.stream import SaasStreamCache
from ..units.verify_reachable import verify_reachable
from .catalog import Catalog, CatalogFrame, Frame, NodeFrame, RawView, RootFrame, VersionLocation, match_or_raise
from .key_manager import KeyManager
from .key_manager import KeyStatus as KeyStatus
from .provider_registry import ProviderRegistry

_INTERACTIVE_LIMITS = dataclasses.replace(DEFAULT_LIMITS, bucket_readers=DEFAULT_LIMITS.bucket_readers_interactive)
"""The limits of the ``DedupRepo`` a ``Repository`` shares among interactive
consumers: a larger bucket-reader cache (see ``CacheLimits.bucket_readers_interactive``)."""


@dataclasses.dataclass(frozen=True, slots=True)
class SetKeyResult:
    """What ``Repository.set_key()`` decided, and what it could not finish.

    Attributes:
        verification: Whether the key verified. The repository's
            ``key_status`` already reflects it.
        reopen_errors: Failures re-opening or closing an already-opened
            catalog while switching to a verified key. The key itself was
            still accepted; empty when nothing went wrong.
    """

    verification: KeyVerification
    reopen_errors: tuple[Exception, ...] = ()

    @property
    def warning(self) -> str | None:
        """A display sentence for ``reopen_errors``, or ``None`` when there
        were none."""
        if not self.reopen_errors:
            return None
        detail = "; ".join(str(error) for error in self.reopen_errors)
        count = len(self.reopen_errors)
        catalogs = f"{count} open {pluralize(count, 'catalog')}"
        return f"the key verified, but {catalogs} could not be switched to it: {detail}"


@dataclasses.dataclass(frozen=True, slots=True)
class _OpenCatalog:
    """One opened catalog layout's resources, kept together so they can't
    drift apart: a ``DedupRepo``, the ``SaasStreamCache`` built on it, and
    its ``connections()`` listing. Held here rather than on ``DedupRepo``
    because ``SaasStreamCache`` is a Unit-layer type."""

    dedup_repo: DedupRepo
    saas_streams: SaasStreamCache
    connections: list[Connection]


class Repository:
    """One opened repository: cheap metadata plus the catalog/provider
    calls that turn it into a browsable tree. Obtained from ``Session``'s
    ``discover``/``open``, never constructed directly; the ``Session``
    closes it (``Session.close_repo`` releases one early).
    """

    def __init__(
        self,
        store: ObjectStore,
        layout: RepositoryLayout,
        keys: KeyMaterial | None,
        key_verification: KeyVerification | None,
        *,
        encrypted: bool | None = None,
    ) -> None:
        self._store = store
        self._layout = layout
        self._key_manager = KeyManager(keys, key_verification, encrypted=encrypted)
        # One RepoLayout per catalog for object storage, always exactly
        # one for a vault. Never exposed outside this class.
        self._catalog_layouts = catalog_repo_layouts(layout)
        # Lazy: an object-storage sibling a caller never touches costs no I/O.
        self._open_catalogs: AsyncKeyedCache[int, _OpenCatalog] = AsyncKeyedCache(self._open_catalog_resources)
        self._providers = ProviderRegistry()
        self._closed = False

    async def _open_catalog_resources(self, index: int) -> _OpenCatalog:
        """The ``_open_catalogs`` cache's factory. Refuses once closed: a
        ``DedupRepo`` opened after ``_close()`` would never be closed."""
        if self._closed:
            raise RuntimeError("Repository is closed; open a new one rather than reusing this instance")
        dedup_repo = await DedupRepo.open(
            self._store,
            self._catalog_layouts[index],
            self._key_manager.keys,
            limits=_INTERACTIVE_LIMITS,
        )
        try:
            conns = await connections(dedup_repo)
        except BaseException as exc:
            # Don't leak the connections dedup_repo opened.
            await close_preserving(exc, [dedup_repo.close])
            raise
        saas_streams = SaasStreamCache(dedup_repo)
        # Registered last, so ``caches.invalidate_all()`` closes the streams
        # before the db sources they read through.
        dedup_repo.caches.register(
            "saas_streams", saas_streams.close, saas_streams.cache_stats, bounded_by="maxsize (LRU)"
        )
        return _OpenCatalog(dedup_repo=dedup_repo, saas_streams=saas_streams, connections=conns)

    @property
    def layout(self) -> RepositoryLayout:
        """Where and what kind of repository this is."""
        return self._layout

    def _owns_repo_path(self, repo_path: str) -> bool:
        """Whether ``repo_path`` (a ``NodeRef.repo_path``) is one of this
        repository's catalog roots, which on object storage differ from
        ``layout.repo_root``."""
        return any(catalog_layout.repo_root == repo_path for catalog_layout in self._catalog_layouts)

    @property
    def is_encrypted(self) -> bool | None:
        """Whether this repository is encrypted, or ``None`` when that
        couldn't be determined."""
        return self._key_manager.is_encrypted

    @property
    def key_status(self) -> KeyStatus:
        """Whether a key was supplied and accepted."""
        return self._key_manager.status

    @property
    def key_verification(self) -> KeyVerification | None:
        """The latest key verification outcome; ``None`` when no key was
        tried."""
        return self._key_manager.verification

    async def set_key(self, key_string: str) -> SetKeyResult:
        """Try a key string against this repository. When it verifies, every
        already-opened catalog is reopened under it, and one that fails to
        switch is reported in ``reopen_errors`` rather than raised. When it
        doesn't, open catalogs are left as they were and ``key_status``
        becomes ``INVALID``.

        Args:
            key_string: ``"<userKeyID>@<base64 userKey>"``.

        Returns:
            The verification outcome plus any catalog reopen failures.

        Raises:
            KeyMaterialError: ``key_string`` is malformed.
            DataCorruptError: The repository's key record is unreadable.
        """
        keys, verification = await self._key_manager.verify(self._store, self._layout, key_string)
        errors: list[Exception] = []
        if verification.ok:
            errors = await self._reopen_catalogs_under_new_key(keys)
        self._key_manager.record(keys, verification)
        return SetKeyResult(verification, tuple(errors))

    async def _reopen_catalogs_under_new_key(self, keys: KeyMaterial) -> list[Exception]:
        """Rebuild every already-opened catalog under the verified ``keys``
        and close the old ones, returning failures instead of raising."""
        errors: list[Exception] = []
        # Settle fetches started under the old key before the snapshot.
        await self._open_catalogs.settle_all()
        already_opened = dict(self._open_catalogs.items())
        # Only a verified key is adopted, so unopened siblings never see a bad one.
        self._key_manager.adopt(keys)
        for index in already_opened:
            self._open_catalogs.invalidate(index)
        # Eager, so a catalog that fails to reopen lands in reopen_errors.
        for index in already_opened:
            try:
                await self._open_catalogs.resolve(index)
            except Exception as exc:  # noqa: BLE001
                # Broad: the close loop below must still run.
                errors.append(exc)
        errors.extend(await _close_opened_catalogs(already_opened.values()))
        return errors

    # -- catalog ----------------------------------------------------------

    def _build_catalog(self, opened: _OpenCatalog, connection: Connection) -> Catalog:
        return Catalog(
            opened.dedup_repo,
            connection,
            saas_streams=opened.saas_streams,
            providers=self._providers,
            keys=self._key_manager,
        )

    async def _open_all_catalogs(self) -> list[_OpenCatalog]:
        """Every catalog layout opened, concurrently. Each open runs to its
        end before the first failure is raised, so none is left running."""
        results = await asyncio.gather(
            *(self._open_catalogs.resolve(i) for i in range(len(self._catalog_layouts))),
            return_exceptions=True,
        )
        for opened_or_error in results:
            if isinstance(opened_or_error, BaseException):
                raise opened_or_error
        return [opened for opened in results if not isinstance(opened, BaseException)]

    async def catalogs(self) -> list[Catalog]:
        """Every catalog this repository holds: a vault's
        ``connection_config`` rows, or those of each object-storage
        sibling, opening the unopened ones concurrently.

        Needs no key: these rows are plaintext, so catalog names can be
        shown before a key prompt. A catalog that fails to open raises
        rather than being skipped, so a short list is never mistaken for a
        repository with fewer catalogs.
        """
        return [
            self._build_catalog(opened, connection)
            for opened in await self._open_all_catalogs()
            for connection in opened.connections
        ]

    async def catalog_by_id(self, catalog_id: CatalogId) -> Catalog | None:
        """The ``Catalog`` matching ``catalog_id``, or ``None``, opening only
        the catalogs that could match. A candidate's open failure is
        raised, as in ``catalogs()``."""
        for index, catalog_layout in enumerate(self._catalog_layouts):
            if catalog_layout.repo_id is not None and catalog_layout.repo_id != catalog_id:
                continue
            opened = await self._open_catalogs.resolve(index)
            for connection in opened.connections:
                candidate = self._build_catalog(opened, connection)
                if candidate.catalog_id == catalog_id:
                    return candidate
        return None

    def workload_is_supported(self, workload: Workload) -> bool:
        """Whether ``workload``'s type has an application-layer provider;
        ``True`` doesn't guarantee every version resolves."""
        return _workload_is_supported(workload)

    async def file_map_tree(self) -> ClosableUnitProvider:
        """The diagnostic ``file_map`` tree, browsable even when catalog
        metadata is missing. Always the first catalog's: a ``RAW`` ref has
        no catalog segment to pick an object-storage sibling by. Tracked
        like any provider."""
        opened = await self._open_catalogs.resolve(0)
        return self._providers.track(FileMapTreeProvider(opened.dedup_repo))

    def cache_names(self) -> list[str]:
        """The names ``invalidate_caches`` accepts: every cache of every
        catalog opened so far, in registration order, without repeats."""
        names: dict[str, None] = {}
        for opened in self._open_catalogs.values():
            names.update(dict.fromkeys(opened.dedup_repo.caches.names()))
        return list(names)

    def cache_stats(self) -> dict[str, CacheStats]:
        """Counters of every cache of every catalog opened so far, keyed
        ``<catalog index>.<cache name>``."""
        return {
            f"{index}.{name}": stats
            for index, opened in self._open_catalogs.items()
            for name, stats in opened.dedup_repo.caches.stats().items()
        }

    async def invalidate_caches(self, *names: str) -> None:
        """Drop cached data so the next call re-reads the store: the named
        caches (see ``cache_names``), or every cache when none is named.

        Call this after the store changed; nothing else revalidates. Catalogs
        stay valid: only their caches are rebuilt, and the connection list is
        re-read when ``db_sources`` is dropped. Providers handed out earlier keep their own connections and
        are not closed; fetch fresh ones to see new data.

        No catalog or provider call may be in flight: db connections and
        SaaS streams are closed and replaced. Does nothing while no catalog
        has been opened, since nothing is cached yet.

        Args:
            names: Cache names to drop; none means all.

        Raises:
            KeyError: A name is not in ``cache_names()``, checked once a catalog
                is open (nothing was dropped).
            RuntimeError: The repository is closed.
            ExceptionGroup: One or more resources failed to close or rebuild.
        """
        if self._closed:
            raise RuntimeError("Repository is closed; open a new one rather than reusing this instance")
        known = self.cache_names()
        if not known:
            return
        unknown = [name for name in names if name not in known]
        if unknown:
            raise KeyError(f"unknown cache name(s): {', '.join(unknown)}")
        opened_catalogs, errors = await self._open_catalogs.settle_all()
        for index, opened in opened_catalogs.items():
            try:
                if names:
                    await opened.dedup_repo.caches.invalidate(*names)
                else:
                    await opened.dedup_repo.caches.invalidate_all()
                if not names or "db_sources" in names:
                    conns = await connections(opened.dedup_repo)
                    self._open_catalogs.put(index, dataclasses.replace(opened, connections=conns))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
        if errors:
            raise ExceptionGroup("Repository.invalidate_caches() failed for one or more resources", errors)

    async def release_provider(self, provider: ClosableUnitProvider) -> None:
        """Close ``provider`` and stop tracking it, so the repository doesn't
        keep it (and what it references) alive until its own close.

        The close runs to completion even if this call is cancelled, and the
        repository's close waits for it.
        """
        await self._providers.release(provider)

    async def locate(self, ref: str | NodeRef, *, raw: RawView | None = None) -> Frame:
        """Where ``ref`` lands. A human ref walks display names (with
        ``disambiguate()`` suffixes) catalog -> workload -> version -> item
        as far as its segments go, so it may stop above a node; a canonical
        or raw ref always names a node. ``ref.repo_path`` is ignored.

        A ``NodeFrame``'s ``provider`` is tracked until the repository
        closes; a caller done with it earlier passes it to
        ``release_provider``.

        Args:
            ref: A ``NodeRef`` or its string form.
            raw: Forwarded to ``Catalog.provider``.

        Returns:
            The frame the ref reached.

        Raises:
            NotFoundError: A segment matches nothing or matches ambiguously,
                or no node matches a canonical/raw ref.
            KeyRequiredError: The ref goes below a catalog of an encrypted
                repository and no key was supplied.
            KeyMismatchError: As above, but the last key supplied was rejected.
            ValueError: The string is not a parseable ref.
        """
        node_ref = NodeRef.coerce(ref)
        # Tree nodes carry layout.repo_root; a user-typed ref may not.
        node_ref = dataclasses.replace(node_ref, repo_path=self.layout.repo_root)
        kind = node_ref.kind
        if kind is RefKind.RAW:
            provider = await self.file_map_tree()
            return NodeFrame(provider, await self._find_or_release(provider, node_ref))
        if kind is RefKind.CANONICAL:
            location = await _locate_canonical_ref(self, node_ref)
            provider = await location.catalog.provider(location.version, raw=raw)
            node = await self._find_or_release(provider, node_ref)
            return NodeFrame(
                provider, node, catalog=location.catalog, workload=location.workload, version=location.version
            )
        segments = node_ref.segments
        if not segments:
            return RootFrame()
        catalogs_list = await self.catalogs()
        catalog = match_or_raise(segments[0], catalog_pairs(catalogs_list), catalogs_list, kind="backup source")
        if len(segments) == 1:
            return CatalogFrame(catalog)
        return await catalog.locate(segments[1:], raw=raw)

    async def _find_or_release(self, provider: ClosableUnitProvider, node_ref: NodeRef) -> Node:
        """``_find_or_raise``, releasing ``provider`` on failure: the caller
        never sees it, so it can't release it."""
        try:
            return await _find_or_raise(provider, node_ref)
        except BaseException as exc:
            await self._providers.release_after(exc, provider)
            raise

    async def resolve(self, ref: str | NodeRef, *, raw: RawView | None = None) -> NodeFrame:
        """``locate``, but ``ref`` must reach a node: a human ref names at
        least a catalog, workload and version. ``NodeFrame.unit()`` turns a
        leaf into its ``RestorableUnit``.

        Args:
            ref: A ``NodeRef`` or its string form.
            raw: Forwarded to ``Catalog.provider``.

        Returns:
            The ``NodeFrame`` the ref reached.

        Raises:
            NotFoundError: No node matches the ref, or a human ref stops
                above a version.
            KeyRequiredError: The ref goes below a catalog of an encrypted
                repository and no key was supplied.
            KeyMismatchError: As above, but the last key supplied was rejected.
            ValueError: The string is not a parseable ref.
        """
        node_ref = NodeRef.coerce(ref)
        if node_ref.kind is RefKind.HUMAN and len(node_ref.segments) < 3:
            raise NotFoundError(
                "human ref must name at least a catalog, workload, and version "
                f"(got {len(node_ref.segments)} {pluralize(len(node_ref.segments), 'segment')}): {node_ref}",
                ref=str(node_ref),
            )
        frame = await self.locate(node_ref, raw=raw)
        assert isinstance(frame, NodeFrame)  # three human segments always reach a version's root
        return frame

    async def version_for_ref(self, ref: str | NodeRef) -> VersionLocation:
        """Resolve a canonical ref's catalog/workload/version prefix, for a
        caller that wants the version's provider rather than one node in it.

        Args:
            ref: A canonical ``NodeRef`` or its string form.

        Returns:
            Where the version is; its ``catalog``'s ``provider()`` opens it.

        Raises:
            NotFoundError: The ref is malformed or names no such catalog or
                version.
            ValueError: The string is not a parseable ref.
            KeyRequiredError: The ref goes below a catalog of an encrypted
                repository and no key was supplied.
            KeyMismatchError: As above, but the last key supplied was rejected.
        """
        return await _locate_canonical_ref(self, NodeRef.coerce(ref))

    async def verify(
        self,
        level: VerifyLevel = VerifyLevel.QUICK,
        *,
        progress: ProgressCallback | None = None,
    ) -> list[Finding]:
        """Integrity check over every catalog, each distinct ``DedupRepo``
        once (a vault's catalogs share one). Opens every catalog first; at
        ``FULL`` level, catalogs over the same pool share one worker pool.

        Args:
            level: How deep to check.
            progress: Receives a ``Progress`` snapshot as checking advances.

        Returns:
            Every ``Finding`` from every catalog.

        Raises:
            KeyRequiredError: The repository is encrypted and no key was
                supplied (an unkeyed walk would read zero versions and look
                clean).
            KeyMismatchError: The last key supplied was rejected.
            StorageBackendError: A store call failed mid-check.
        """
        self._key_manager.require_verified()
        dedup_repos = [opened.dedup_repo for opened in await self._open_all_catalogs()]
        findings: list[Finding] = []
        executor = shared_verify_executor(dedup_repos) if level is VerifyLevel.FULL and len(dedup_repos) > 1 else None
        try:
            for dedup_repo in dedup_repos:
                findings.extend(await verify_reachable(dedup_repo, level, progress=progress, executor=executor))
        finally:
            if executor is not None:
                await executor.close()
        return findings

    async def _close(self) -> None:
        """Close every tracked provider and ``DedupRepo``, on every path
        including errors. Idempotent; ``Session`` is the only caller.

        Leaves the ``ObjectStore`` open, since repositories from one
        discovery can share it; ``Session.close_repo()`` also releases the
        store when no other repository uses it.

        Raises:
            ExceptionGroup: One or more resources failed to close (every
                one was still attempted).
        """
        if self._closed:
            return
        self._closed = True
        errors = await self._providers.close()
        # Settle in-flight fetches so none lands after the close loop.
        opened_catalogs, resolve_errors = await self._open_catalogs.settle_all()
        errors.extend(resolve_errors)
        errors.extend(await _close_opened_catalogs(opened_catalogs.values()))
        # Drop the closed DedupRepos from the cache.
        self._open_catalogs.invalidate()
        if errors:
            raise ExceptionGroup("closing the repository failed to close every tracked resource", errors)


async def _find_or_raise(provider: UnitProvider, node_ref: NodeRef) -> Node:
    node = await find_node(provider, node_ref)
    if node is None:
        raise NotFoundError(f"no node in provider tree matches ref: {node_ref}", ref=str(node_ref))
    return node


async def _locate_canonical_ref(repo: Repository, node_ref: NodeRef) -> VersionLocation:
    """Picks the ``Catalog`` by the ref's ``CatalogId`` first, then the
    version within it: ``connection_config_id`` alone collides across
    object-storage siblings."""
    ids = node_ref.canonical_ids
    if ids is None:
        raise NotFoundError(f"malformed canonical ref: {node_ref}", ref=str(node_ref))
    catalog_id, workload_id, version_uid = ids
    catalog = await repo.catalog_by_id(catalog_id)
    if catalog is None:
        raise NotFoundError(f"no catalog with catalog_id={catalog_id!r}", ref=str(node_ref))
    location = await catalog.version_by_uid(workload_id, version_uid)
    if location is None:
        raise NotFoundError(
            f"no version with version_uid={version_uid!r} under workload_id={workload_id}", ref=str(node_ref)
        )
    return location


async def _close_opened_catalogs(opened: Iterable[_OpenCatalog]) -> list[Exception]:
    """Close each opened catalog's ``saas_streams`` before its ``dedup_repo``
    (the streams read through it), attempting every one."""
    return await close_each(
        (closer for o in opened for closer in (o.saas_streams.close, o.dedup_repo.close)),
        per_close_timeout=RESOURCE_CLOSE_TIMEOUT,
    )
