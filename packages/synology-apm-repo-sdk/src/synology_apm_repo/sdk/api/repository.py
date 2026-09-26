"""``Repository``: one opened repository's catalog/provider facade, part of
the Repository Layer (see ``api/__init__.py``). ``Session`` (discovery/lifetime) lives in the
sibling ``api.session`` module; ``Catalog``/``Frame`` (one catalog's own
workload/version/provider operations) live in the sibling ``api.catalog``
module — this module holds ``Repository`` itself plus the meeting points
that need both (``resolve()``'s canonical/human-ref dispatch, in
particular).
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from typing import Self

from ..asynccache import AsyncKeyedCache
from ..catalog.connection import Connection, connections
from ..catalog.version import Version
from ..catalog.workload import Workload
from ..dedup.keys import KeyMaterial, KeyVerification
from ..dedup.pool import INTERACTIVE_BUCKET_CACHE_SIZE
from ..dedup.pool_descriptor import PoolDescriptor
from ..dedup.repository import DedupRepo
from ..dedup.verify_checks import Finding as Finding
from ..dedup.verify_checks import Stage as Stage
from ..dedup.verify_checks import Symptom as Symptom
from ..dedup.verify_checks import VerifyLevel as VerifyLevel
from ..errors import NotFoundError
from ..identifiers import CatalogId
from ..presentation.progress import Progress
from ..storage.base import ObjectStore
from ..storage.layout import RepoKind, RepositoryLayout, catalog_repo_layouts
from ..units.base import ClosableUnitProvider, Node, RestorableUnit, UnitProvider
from ..units.dispatch import is_supported as _workload_is_supported
from ..units.file_map_tree import FileMapTreeProvider
from ..units.node_ref import NodeRef, RefKind, catalog_pairs
from ..units.resolve import find_node
from ..units.saas.stream import SaasStreamCache
from ..units.verify_bucket_check import build_verify_executor
from ..units.verify_reachable import verify_reachable
from .catalog import Catalog, Frame, _match_or_raise
from .key_manager import KeyManager
from .key_manager import KeyStatus as KeyStatus

_RESOURCE_CLOSE_TIMEOUT = 10.0
"""Bounds one tracked provider's/``DedupRepo``'s own ``close()`` call in
``Repository.close()``'s and ``set_key()``'s "attempt every one, then
report" sweeps — without it, one hung close (a stuck server, say) blocks
every other resource's close and the interpreter's own exit along with it,
since ``aiosqlite`` dedicates a non-daemon thread to each connection."""


@dataclasses.dataclass(frozen=True)
class _OpenCatalog:
    """One opened catalog-layout index's resources, kept together so they
    can never drift out of sync: a ``DedupRepo``, the ``SaasStreamCache``
    built against that exact ``DedupRepo``, and its ``connections()``
    listing. Lives on ``Repository``, not ``DedupRepo`` itself:
    ``SaasStreamCache`` is a Unit-layer type, and ``DedupRepo`` importing
    it back would be an upward/circular import."""

    dedup_repo: DedupRepo
    saas_streams: SaasStreamCache
    connections: list[Connection]


class Repository:
    """One opened repository: cheap metadata plus the catalog/provider
    calls that turn it into a browsable tree. Never constructed directly
    by callers — obtained from ``Session.discover``/``Session.open``.
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
        # Opened lazily: eager opening would waste I/O for an
        # object-storage sibling a caller never touches.
        self._open_catalogs: AsyncKeyedCache[int, _OpenCatalog] = AsyncKeyedCache(self._open_catalog_resources)
        # Every provider this Repository has handed out, so close() can
        # release the sqlite connections they own (DeviceProvider's
        # target.db, FsProvider's version.db, ...) which DedupRepo.close()
        # doesn't know about.
        self._providers: list[UnitProvider] = []
        # A second close() call is a no-op, so Session.close() re-closing
        # a repository a caller already closed early needs no bookkeeping.
        self._closed = False

    async def _open_catalog_resources(self, index: int) -> _OpenCatalog:
        """Build one catalog's ``_OpenCatalog`` bundle — the
        ``_open_catalogs`` cache's own factory.

        Guarded on ``_closed`` because ``close()`` invalidates that cache: a
        resolve arriving afterwards would otherwise open a ``DedupRepo``
        nothing will ever close again. A ``RuntimeError``, not an
        ``ApmRepoError``: using a closed ``Repository`` is a caller bug.
        """
        if self._closed:
            raise RuntimeError("Repository is closed; open a new one rather than reusing this instance")
        dedup_repo = await DedupRepo.open(
            self._store,
            self._catalog_layouts[index],
            self._key_manager.keys,
            bucket_cache_size=INTERACTIVE_BUCKET_CACHE_SIZE,
        )
        try:
            conns = await connections(dedup_repo)
        except Exception:
            # dedup_repo already opened real sqlite connections above --
            # a failure here must not leak them.
            await dedup_repo.close()
            raise
        return _OpenCatalog(dedup_repo=dedup_repo, saas_streams=SaasStreamCache(dedup_repo), connections=conns)

    async def _confirm_real(self) -> bool:
        """``Session``'s own post-construction check: is this actually a
        valid, openable repository, or a layout-detection false positive
        that should be skipped? Trusts the marker check
        ``iter_repository_layouts`` already performed rather than paying
        for a real open — a specific catalog's own corrupt ``repo_info``
        instead surfaces later, scoped to that catalog, the first time
        ``catalogs()`` opens it."""
        if self._layout.kind is RepoKind.VAULT:
            return True
        return self._layout.catalog_ids != []  # None (unenumerable) or non-empty: real; [] means nothing to browse

    @property
    def layout(self) -> RepositoryLayout:
        return self._layout

    def owns_repo_path(self, repo_path: str) -> bool:
        """Whether ``repo_path`` — a ``NodeRef.repo_path`` — names one of
        this repository's own catalogs. Never compared against
        ``self.layout.repo_root``: that's the bucket-level root, which
        diverges from a catalog's own root once ``self.layout.catalog_ids``
        is enumerable."""
        return any(catalog_layout.repo_root == repo_path for catalog_layout in self._catalog_layouts)

    @property
    def is_encrypted(self) -> bool | None:
        """Whether this repository is actually encrypted, or ``None`` when
        that genuinely couldn't be determined."""
        return self._key_manager.is_encrypted

    @property
    def key_status(self) -> KeyStatus:
        return self._key_manager.status

    @property
    def key_verification(self) -> KeyVerification | None:
        return self._key_manager.verification

    async def set_key(self, key_string: str) -> KeyVerification:
        """Try a new key string against this repository. On success, every
        already-opened ``DedupRepo`` is closed and replaced (a catalog not
        yet opened picks up the new key on its own eventual first open).
        On failure, every already-open connection is left as it was —
        ``key_status`` reports ``INVALID``, never reverting to
        ``NO_KEY_PROVIDED``.
        """
        keys, verification = await self._key_manager.verify(self._store, self._layout, key_string)
        errors: list[Exception] = []
        if verification.ok:
            errors = await self._reopen_catalogs_under_new_key(keys)
        # Recorded before any ExceptionGroup below is raised: a caller
        # must see the true key_status even alongside a reopen failure.
        self._key_manager.record(keys, verification)
        if errors:
            raise ExceptionGroup("Repository.set_key() failed to fully switch every open catalog", errors)
        return verification

    async def _reopen_catalogs_under_new_key(self, keys: KeyMaterial) -> list[Exception]:
        """``set_key()``'s own effect on already-opened catalogs, once
        ``keys`` has already verified ``ok`` — every already-opened
        ``_OpenCatalog`` bundle is closed and eagerly rebuilt under
        ``keys``. Returns every reopen/close failure instead of raising —
        ``set_key()`` still records the verification outcome regardless."""
        errors: list[Exception] = []
        # Settle every in-flight fetch (started under the OLD key) before
        # the snapshot below, or it would land pinned to the stale key.
        await self._open_catalogs.settle_all()
        already_opened = dict(self._open_catalogs.items())
        # Adopted only on success: a rejected key must never poison a
        # not-yet-opened sibling with a key already known to be wrong.
        self._key_manager.adopt(keys)
        for index in already_opened:
            self._open_catalogs.invalidate(index)
        # Re-open eagerly so a caller mid-iteration right after set_key()
        # sees the new key's data, not a stale cache entry.
        for index in already_opened:
            try:
                await self._open_catalogs.resolve(index)
            except Exception as exc:
                # Broad: any failure here must still fall through to the
                # close loop below, or the old resources being replaced
                # leak for the rest of this repository's lifetime.
                errors.append(exc)
        for old_opened in already_opened.values():
            # saas_streams closed first: it must never outlive the
            # dedup_repo it was built against.
            try:
                await asyncio.wait_for(old_opened.saas_streams.close(), timeout=_RESOURCE_CLOSE_TIMEOUT)
            except Exception as exc:
                errors.append(exc)
            try:
                await asyncio.wait_for(old_opened.dedup_repo.close(), timeout=_RESOURCE_CLOSE_TIMEOUT)
            except Exception as exc:
                errors.append(exc)
        return errors

    # -- catalog ----------------------------------------------------------

    def _require_key_verified(self) -> None:
        """Raise before any catalog I/O if this repository is *confirmed*
        encrypted and its key hasn't been verified yet — thin delegation to
        ``KeyManager.require_verified``."""
        self._key_manager.require_verified()

    def _build_catalog(self, opened: _OpenCatalog, connection: Connection) -> Catalog:
        """The one shared place ``catalogs()``/``catalog_by_id()`` build a
        ``Catalog`` wrapper around one opened catalog's resources."""
        return Catalog(
            opened.dedup_repo,
            connection,
            saas_streams=opened.saas_streams,
            track=self._track,
            require_key_verified=self._require_key_verified,
        )

    async def catalogs(self) -> list[Catalog]:
        """Every catalog this repository holds — a vault's own
        ``connection_config`` rows, or one per independently-opened
        object-storage sibling repo-id — wrapped uniformly as ``Catalog``.
        Opens every not-yet-opened catalog concurrently via
        ``asyncio.gather``.

        Never gated on the key, unlike ``Catalog.workloads()``/
        ``versions()``: the rows this reads are plaintext regardless of
        encryption, and opening a ``DedupRepo`` itself never requires one
        either — this also preserves the TUI's flow of showing catalog
        names before any key prompt.

        A specific catalog's own open failure is re-raised immediately,
        never treated as "skip it and keep going" — a caller must never
        mistake one broken sibling for a bucket with fewer catalogs than
        it actually has.
        """
        results = await asyncio.gather(
            *(self._open_catalogs.resolve(i) for i in range(len(self._catalog_layouts))),
            return_exceptions=True,
        )
        good_opened: list[_OpenCatalog] = []
        for opened_or_error in results:
            if isinstance(opened_or_error, BaseException):
                raise opened_or_error
            good_opened.append(opened_or_error)
        return [self._build_catalog(opened, connection) for opened in good_opened for connection in opened.connections]

    async def catalog_by_id(self, catalog_id: CatalogId) -> Catalog | None:
        """Resolve exactly the one ``Catalog`` matching ``catalog_id`` —
        opening only the specific ``DedupRepo``(s) that could possibly
        match, never every sibling the way ``catalogs()``'s own full
        listing does (avoidable I/O on an object-storage bucket with
        several). A candidate that fails to open is re-raised immediately,
        same as ``catalogs()``, never treated as "not it, keep looking".
        """
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
        """Whether ``workload`` has a chance at an application-layer
        provider — ``True`` doesn't guarantee every version resolves, only
        that the type is one ``provider_for``/``saas_provider_for``
        recognize at all. Never import ``units.dispatch`` directly from
        CLI/TUI code instead."""
        return _workload_is_supported(workload)

    async def file_map_tree(self) -> UnitProvider:
        """The diagnostic fallback axis of last resort — browsable even
        when catalog metadata is missing or unhelpful. Always the first
        catalog: a ``RAW`` ref's grammar carries no catalog segment, so
        this can't disambiguate between object-storage siblings."""
        opened = await self._open_catalogs.resolve(0)
        return self._track(FileMapTreeProvider(opened.dedup_repo))

    async def invalidate_directory_cache(self) -> None:
        """Drop every cached directory listing for every catalog this
        repository has opened so far, so the next provider/catalog call
        re-scans the store instead of answering from an earlier listing."""
        for opened in self._open_catalogs.values():
            await opened.dedup_repo.dir_cache.invalidate()

    async def resolve(self, ref: str | NodeRef, *, object_db_id: str | None = None) -> Node | RestorableUnit:
        """Turn a ``NodeRef`` (or its string form) into a live node/unit,
        re-derived from cheap catalog/provider-tree calls rather than
        stored anywhere. ``ref.repo_path`` is ignored entirely (canonical/
        human refs never need it).
        """
        node_ref = ref if isinstance(ref, NodeRef) else NodeRef.parse(ref)
        # Normalize repo_path to this repository's own root: tree nodes
        # carry layout.repo_root, but a CLI-supplied ref carries whatever
        # path the user typed.
        node_ref = dataclasses.replace(node_ref, repo_path=self.layout.repo_root)
        kind = node_ref.kind
        if kind is RefKind.RAW:
            return await _resolve_in_provider(await self.file_map_tree(), node_ref)
        if kind is RefKind.CANONICAL:
            catalog, version = await self.version_for_ref(node_ref)
            provider = await catalog.provider(version, object_db_id=object_db_id)
            return await _resolve_in_provider(provider, node_ref)
        # Human ref: walks display names instead -- catalog -> workload ->
        # version -> item, applying the same collision-suffix
        # disambiguate() scheme the CLI/TUI use when displaying names.
        return await _resolve_human_ref(self, node_ref)

    async def version_for_ref(self, node_ref: NodeRef) -> tuple[Catalog, Version]:
        """Resolve a canonical ref's ``cat:``/``wl:``/``ver:`` prefix down
        to its owning ``Catalog`` and ``Version`` — the first half of what
        ``resolve`` does for a canonical ref, exposed for callers that need
        the version's own ``provider`` rather than one specific node inside
        it (e.g. the CLI's ``ls``/``tree``). The ``Catalog`` is part of the
        result too: building a provider needs to know which catalog's
        ``DedupRepo`` to dispatch against."""
        return await _version_for_canonical_ref(self, node_ref)

    async def walk_human_ref(self, segments: tuple[str, ...], *, object_db_id: str | None = None) -> Frame:
        """Walk a human ref's ``segments`` as far as they go, through the
        four levels catalog -> workload -> version -> item tree, raising
        ``NotFoundError`` the moment a segment doesn't match anything at
        its level — a bare catalog or workload name is not itself an
        error, since ``cli/browse.py``'s ``ls``/``tree`` breadcrumbs also
        need to stop at an intermediate depth. ``object_db_id`` is
        forwarded to ``provider`` once ``segments`` reaches a version.

        Only this first level (picking the ``Catalog``) lives here — the
        rest is ``Catalog.walk_human_ref``'s job."""
        if not segments:
            return Frame(level="root")

        catalogs_list = await self.catalogs()
        catalog = _match_or_raise(segments[0], catalog_pairs(catalogs_list), catalogs_list, kind="backup source")
        if len(segments) == 1:
            return Frame(level="catalog", catalog=catalog)
        return await catalog.walk_human_ref(segments[1:], object_db_id=object_db_id)

    async def verify(
        self,
        level: VerifyLevel = VerifyLevel.QUICK,
        *,
        progress: Callable[[Progress], Awaitable[None]] | None = None,
    ) -> list[Finding]:
        """Integrity check over every catalog this repository holds — each
        *distinct* ``DedupRepo`` verified exactly once, not once per
        ``Catalog`` (a vault's sibling catalogs share one physical pool).
        Opens every not-yet-opened catalog first, same concurrent-open and
        fail-fast posture as ``catalogs()``, then runs
        ``units.verify_reachable.verify_reachable()`` once per opened
        instance and concatenates every ``Finding``.

        Gated on ``_require_key_verified()``: an encrypted repository
        verified without a key would otherwise silently walk zero versions
        and report a misleadingly clean result.

        At FULL level with more than one catalog, shares **one**
        multiprocess executor across every ``verify_reachable()`` call
        when every catalog resolves to the identical ``PoolDescriptor``
        (the common vault case) — falls back to each call building its own
        otherwise (object-storage siblings can be genuinely separate
        stores)."""
        self._require_key_verified()
        opened_catalogs = await asyncio.gather(
            *(self._open_catalogs.resolve(i) for i in range(len(self._catalog_layouts)))
        )
        dedup_repos = [opened.dedup_repo for opened in opened_catalogs]
        findings: list[Finding] = []
        executor: ProcessPoolExecutor | None = None
        if level is VerifyLevel.FULL and len(dedup_repos) > 1:
            descriptors = [
                PoolDescriptor.from_repo(repo, verify_fingerprint=True, verify_ciphertext_crc=True)
                for repo in dedup_repos
            ]
            first = descriptors[0]
            if first is not None and all(d == first for d in descriptors):
                executor = build_verify_executor(first)
        try:
            for dedup_repo in dedup_repos:
                findings.extend(await verify_reachable(dedup_repo, level, progress=progress, executor=executor))
        finally:
            if executor is not None:
                # A plain blocking call, routed through to_thread() so it
                # doesn't freeze this whole process's event loop for
                # however long a still-running worker takes.
                await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)
        return findings

    def _track(self, provider: UnitProvider) -> UnitProvider:
        """Remember ``provider`` so ``close`` can release whatever sqlite
        connections it opened — an abandoned one blocks interpreter exit
        forever (see ``_RESOURCE_CLOSE_TIMEOUT``)."""
        self._providers.append(provider)
        return provider

    async def close(self) -> None:
        """Close every tracked provider and ``DedupRepo``, on every path
        including errors. Idempotent.

        Never touches this repository's own ``ObjectStore``: one
        ``discover()``/``discover_remote()`` call's repositories can share
        one store, so only ``Session`` can tell whether it's safe to
        release — call ``Session.close_repo()`` instead of a bare
        ``repo.close()`` to release the store early too.
        """
        if self._closed:
            return
        self._closed = True
        # Every tracked provider gets a close attempt regardless of
        # whether an earlier one raised — a leaked aiosqlite connection
        # blocks interpreter exit forever.
        errors: list[Exception] = []
        for provider in self._providers:
            if isinstance(provider, ClosableUnitProvider):
                try:
                    await asyncio.wait_for(provider.close(), timeout=_RESOURCE_CLOSE_TIMEOUT)
                except Exception as exc:
                    errors.append(exc)
        self._providers.clear()
        # Settle every in-flight fetch (a catalogs()/verify() call racing
        # this close()) before closing each one that lands, or it leaks.
        opened_catalogs, resolve_errors = await self._open_catalogs.settle_all()
        errors.extend(resolve_errors)
        for opened in opened_catalogs.values():
            # saas_streams closed before dedup_repo: its streams hold
            # connections opened against this dedup_repo, so it must
            # never outlive it.
            try:
                await asyncio.wait_for(opened.saas_streams.close(), timeout=_RESOURCE_CLOSE_TIMEOUT)
            except Exception as exc:
                errors.append(exc)
            try:
                await asyncio.wait_for(opened.dedup_repo.close(), timeout=_RESOURCE_CLOSE_TIMEOUT)
            except Exception as exc:
                errors.append(exc)
        # Also forget each closed DedupRepo, not just close it, or the
        # cache keeps it (and everything it references) alive indefinitely.
        self._open_catalogs.invalidate()
        if errors:
            raise ExceptionGroup("Repository.close() failed to close every tracked resource", errors)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


async def _finish(provider: UnitProvider, node: Node) -> Node | RestorableUnit:
    """The common tail of every ``resolve()`` path — a leaf node becomes
    its full ``RestorableUnit`` via ``UnitProvider.unit``; an intermediate
    node is returned as-is."""
    return await provider.unit(node) if node.is_leaf else node


async def _resolve_in_provider(provider: UnitProvider, node_ref: NodeRef) -> Node | RestorableUnit:
    node = await find_node(provider, node_ref)
    if node is None:
        raise NotFoundError(f"no node in provider tree matches ref: {node_ref}", ref=str(node_ref))
    return await _finish(provider, node)


async def _version_for_canonical_ref(repo: Repository, node_ref: NodeRef) -> tuple[Catalog, Version]:
    """Picks the right ``Catalog`` by the ref's own ``CatalogId`` first,
    then searches only that catalog's ``workloads()``/``versions()`` —
    never scanning by ``connection_config_id`` alone, which collides
    across object-storage siblings."""
    ids = node_ref.canonical_ids
    if ids is None:
        raise NotFoundError(f"malformed canonical ref: {node_ref}", ref=str(node_ref))
    catalog_id, workload_id, version_uid = ids
    catalog = await repo.catalog_by_id(catalog_id)
    if catalog is None:
        raise NotFoundError(f"no catalog with catalog_id={catalog_id!r}", ref=str(node_ref))
    workload = next((w for w in await catalog.workloads() if w.workload_id == workload_id), None)
    if workload is None:
        raise NotFoundError(f"no workload with workload_id={workload_id}", ref=str(node_ref))
    versions_list = await catalog.versions(workload, include_deleted=True)
    version = next((v for v in versions_list if v.version_uid == version_uid), None)
    if version is None:
        raise NotFoundError(f"no version with version_uid={version_uid!r}", ref=str(node_ref))
    return catalog, version


async def _resolve_human_ref(repo: Repository, node_ref: NodeRef) -> Node | RestorableUnit:
    """Adds the "at least catalog/workload/version" length check on top of
    ``Repository.walk_human_ref`` (which alone would stop at an
    intermediate ``Frame`` rather than raising, valid for ``ls``/``tree``
    but not for ``resolve()``), then unwraps the final ``Frame`` into the
    leaf/subtree ``resolve()`` promises."""
    segments = node_ref.segments
    if len(segments) < 3:
        raise NotFoundError(
            "human ref must name at least a catalog, workload, and version "
            f"(got {len(segments)} segment(s)): {node_ref}",
            ref=str(node_ref),
        )
    frame = await repo.walk_human_ref(segments)
    assert frame.node is not None and frame.provider is not None  # guaranteed once len(segments) >= 3
    return await _finish(frame.provider, frame.node)
