"""``Repository``: one opened repository's catalog/provider facade, part of
the Repository Layer (the ``Session``/``Repository``/``Catalog`` split
CLI/TUI code imports directly). ``Session`` (discovery/lifetime) lives in the
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
"""Bounds one tracked provider's/``DedupRepo``'s own ``close()`` call
in ``Repository.close()``'s and ``set_key()``'s "attempt every one, then
report" sweeps below. Both already tolerate one ``close()`` *raising*
without abandoning the rest — this covers the different failure mode of
one *hanging* instead (a stuck server, a race like ``storage/smb.py``'s
own ``_drop_slot_session`` one): without a bound, that single call would block
every other tracked resource's own close attempt forever, and the
interpreter along with it — ``aiosqlite`` dedicates a non-daemon background
thread to each connection's whole lifetime, so one leaked connection makes
``threading._shutdown()`` block forever and the interpreter never exits."""


@dataclasses.dataclass(frozen=True)
class _OpenCatalog:
    """One opened catalog-layout index's resources, kept together so
    they can never drift out of sync — a DedupRepo and the SaasStreamCache
    built against that exact DedupRepo, plus that DedupRepo's own
    ``connections()`` listing. Every consumer downstream (Catalog,
    saas_provider_for, ...) assumes saas_streams was built against this
    same dedup_repo; nothing asserts that pairing at the point of use, so
    it must stay sourced from here, never reconstructed independently.

    Lives on ``Repository``, not ``DedupRepo`` itself: ``SaasStreamCache``
    is a Unit-layer type, and a Dedup-layer ``DedupRepo`` importing it back
    would be an illegal upward/circular import (Unit depends on Dedup,
    never the reverse -- see ``ARCHITECTURE.md``'s layer diagram)."""

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
        # Owns keys/key_verification/encrypted and the KeyStatus state
        # machine over them -- kept separate from set_key()'s catalog-reopen
        # orchestration below (see api.key_manager).
        self._key_manager = KeyManager(keys, key_verification, encrypted=encrypted)
        # The individual, directly-openable RepoLayout(s) this bucket/vault
        # resolves to -- one per catalog for object storage, always exactly
        # one for a vault. Never exposed outside this class: the "several
        # sibling directories under @ActiveProtectData" reality this
        # represents is exactly what api/session.py's own Repository/Catalog
        # design keeps hidden from callers.
        self._catalog_layouts = catalog_repo_layouts(layout)
        # Opened lazily, one _OpenCatalog bundle per index into
        # _catalog_layouts: eager opening would waste real I/O for an
        # object-storage bucket with siblings a caller never touches.
        # AsyncKeyedCache also gives concurrent in-flight de-duplication
        # for free, needed once catalogs() opens every index via
        # asyncio.gather.
        self._open_catalogs: AsyncKeyedCache[int, _OpenCatalog] = AsyncKeyedCache(self._open_catalog_resources)
        # Every provider this Repository has handed out, so close() can
        # release the sqlite connections they own. Providers open their own
        # SqliteSources -- DeviceProvider's target.db, FsProvider's
        # version.db, the SaaS providers' streams -- which
        # DedupRepo.close() doesn't know about. Every handed-out
        # provider must be closed, including one whose ref then fails to
        # resolve: an abandoned one hangs interpreter shutdown (see
        # ARCHITECTURE.md's "Async-native, by design").
        self._providers: list[UnitProvider] = []
        # A second close() call is a no-op so a caller that already closed
        # this repository early itself doesn't need its own bookkeeping
        # when Session.close() closes every repository it ever yielded,
        # including this one, again.
        self._closed = False

    async def _open_catalog_resources(self, index: int) -> _OpenCatalog:
        """Build one catalog's ``_OpenCatalog`` bundle (``DedupRepo``,
        the ``SaasStreamCache`` built against it, and its ``connections()``
        listing) — the ``_open_catalogs`` cache's own factory.

        Guarded on ``_closed`` because ``close()`` invalidates that cache: a
        resolve arriving afterwards would otherwise open a brand-new
        ``DedupRepo``, with its own sqlite connections and temp directories,
        that nothing will ever close again. Deliberately a ``RuntimeError``
        rather than an ``ApmRepoError`` — using a Repository after closing it
        is a bug in the caller, not a condition of the repository that callers
        are expected to catch.
        """
        if self._closed:
            raise RuntimeError("Repository is closed; open a new one rather than reusing this instance")
        # Explicit bucket_cache_size, not DEFAULT_BUCKET_CACHE_SIZE's
        # implicit default.
        dedup_repo = await DedupRepo.open(
            self._store,
            self._catalog_layouts[index],
            self._key_manager.keys,
            bucket_cache_size=INTERACTIVE_BUCKET_CACHE_SIZE,
        )
        try:
            # Resolved once here rather than left to catalogs()/catalog_by_id()
            # to fetch per call: connection_config/link-key naming is
            # structural/config-shaped, not live content that could grow
            # mid-session (unlike dir_cache, which invalidate_directory_cache()
            # explicitly does let a caller refresh for exactly that reason) --
            # same "safe for this DedupRepo's whole lifetime" reasoning as
            # DedupRepo's own _probe_cache/_file_meta_table_cache.
            conns = await connections(dedup_repo)
        except Exception:
            # dedup_repo already opened real sqlite connections above --
            # a failure here (a corrupt connection_config table) must not
            # leak them just because this bundle never finishes building.
            await dedup_repo.close()
            raise
        return _OpenCatalog(dedup_repo=dedup_repo, saas_streams=SaasStreamCache(dedup_repo), connections=conns)

    async def _confirm_real(self) -> bool:
        """``Session``'s own post-construction check: is this actually a
        valid, openable repository, or a layout-detection false positive
        that should be skipped rather than yielded broken?

        Neither kind opens a real ``DedupRepo`` here — both trust the
        marker check ``iter_repository_layouts`` already performed when
        this ``RepositoryLayout`` was built (``repo_info``/``link.key``/
        ``.fully_created`` for ``VAULT``; ``db``+``@data`` per catalog id,
        ``storage.layout``'s ``_looks_like_object_store_repo``, for
        ``OBJECT_STORE``) rather than paying for a real open just to
        double-check it — opening eagerly here would defeat lazy
        opening's entire purpose for a bucket with several siblings, and
        this keeps ``VAULT`` on the same posture rather than carving out
        an exception for the one-catalog case. A specific catalog's own
        corrupt ``repo_info`` surfaces later instead, scoped to that one
        catalog, the first time ``catalogs()`` actually opens it and
        raises — a correct, expected ``NotFoundError``/``ApmRepoError`` for a
        genuinely broken catalog, not a reason to hide the whole
        repository from discovery. For ``VAULT`` specifically, whose one
        catalog is also the whole repository, this means a corrupt vault
        is still yielded by discovery, with the failure deferred to its
        own first ``catalogs()`` call — the same deferral an
        ``OBJECT_STORE`` bucket with a broken sibling already gets."""
        if self._layout.kind is RepoKind.VAULT:
            return True
        return self._layout.catalog_ids != []  # None (unenumerable) or non-empty: real; [] means nothing to browse

    @property
    def layout(self) -> RepositoryLayout:
        return self._layout

    def owns_repo_path(self, repo_path: str) -> bool:
        """Whether ``repo_path`` — a ``NodeRef.repo_path``, stamped from
        whichever catalog-level ``RepoLayout`` a node's own provider
        actually opened (see ``catalog_repo_layouts()``) — names one of
        this repository's own catalogs. Never just comparing against
        ``self.layout.repo_root``: that's the bucket-level root, which
        diverges from a catalog's own root once ``self.layout.catalog_ids``
        is enumerable (``layout.repo_root`` stays the bucket root while
        each catalog's own root gains an ``@ActiveProtectData/<repo-id>``
        suffix) — the case ``Session._repo_for_ref`` needs this for.
        """
        return any(catalog_layout.repo_root == repo_path for catalog_layout in self._catalog_layouts)

    @property
    def is_encrypted(self) -> bool | None:
        """Whether this repository is actually encrypted, or ``None`` when
        that genuinely couldn't be determined — thin delegation to
        ``KeyManager.is_encrypted``."""
        return self._key_manager.is_encrypted

    @property
    def key_status(self) -> KeyStatus:
        """The precomputed ``KeyStatus`` — a plain, no-I/O property;
        thin delegation to ``KeyManager.status``."""
        return self._key_manager.status

    @property
    def key_verification(self) -> KeyVerification | None:
        """The GCM-unwrap verification result, or ``None`` when no key
        was ever provided — thin delegation to ``KeyManager.verification``."""
        return self._key_manager.verification

    async def set_key(self, key_string: str) -> KeyVerification:
        """Try a new key string against this repository — the interactive
        "paste a key, see if it's correct" workflow. On success, this
        ``Repository`` starts using the new key for every subsequent
        call: every already-opened ``DedupRepo`` is closed and replaced
        (so a stale ``Catalog`` a caller obtained before this call fails
        cleanly on its next use rather than silently keeping the old
        key's data); a catalog not yet opened simply picks up the new key
        on its own eventual first open. On failure, every already-open
        connection is left exactly as it was — a rejected attempt reports
        ``key_status == INVALID``, never reverting to ``NO_KEY_PROVIDED``
        or corrupting what was already open.

        The one place that legitimately spans both key state
        (``self._key_manager``) and catalog lifecycle
        (``self._open_catalogs``, via ``_reopen_catalogs_under_new_key``)
        — ``KeyManager`` itself owns only the pure key/status state and has
        no state for the catalog-reopen half of this, so that orchestration
        stays here instead.
        """
        keys, verification = await self._key_manager.verify(self._store, self._layout, key_string)
        errors: list[Exception] = []
        if verification.ok:
            errors = await self._reopen_catalogs_under_new_key(keys)
        # Recorded regardless — key_status must distinguish "never tried"
        # (NO_KEY_PROVIDED) from "tried and failed" (INVALID). Done
        # *before* any ExceptionGroup below is raised: verification.ok
        # already settled whether the key itself is correct, independently
        # of whether a specific catalog's own reopen/close also succeeded
        # — a caller must see the true, already-decided key_status even
        # when reporting a partial reopen/close failure alongside it.
        self._key_manager.record(keys, verification)
        if errors:
            raise ExceptionGroup("Repository.set_key() failed to fully switch every open catalog", errors)
        return verification

    async def _reopen_catalogs_under_new_key(self, keys: KeyMaterial) -> list[Exception]:
        """``set_key()``'s own effect on already-opened catalogs, once
        ``keys`` has already verified ``ok`` — every already-opened
        ``_OpenCatalog`` bundle is closed and eagerly rebuilt under
        ``keys``; a catalog not yet opened simply picks up ``keys`` on its
        own eventual first open, and needs nothing done here. Returns every
        reopen/close failure instead of raising — ``set_key()`` itself
        still records the verification outcome and updates
        ``key_status`` regardless of whether every catalog switch
        succeeded."""
        errors: list[Exception] = []
        # Settle every in-flight fetch (started under the OLD key by a
        # concurrent catalogs()/verify() call) before taking the
        # snapshot below -- otherwise such a fetch stays invisible to this
        # method's own cleanup and, once it lands, permanently pins that
        # catalog to the stale key. A fetch that fails here has nothing to
        # invalidate or close either way, so the errors it returns are
        # discarded.
        await self._open_catalogs.settle_all()
        already_opened = dict(self._open_catalogs.items())
        # Adopted *only* on success (the caller only calls this once
        # verification.ok is already known), not unconditionally: this is
        # also what _open_catalog_resources() uses to lazily open any
        # not-yet-opened sibling catalog, so a *rejected* key must never
        # overwrite it — that would poison a sibling that hasn't been
        # touched yet (and might not even be encrypted) with a key already
        # known to be wrong.
        self._key_manager.adopt(keys)
        for index in already_opened:
            self._open_catalogs.invalidate(index)
        # Re-open eagerly, matching set_key()'s own "starts using the new
        # key for every subsequent call" contract — a caller mid-way
        # through iterating an already-fetched Catalog list right
        # after set_key() must see the new key's data, not a stale
        # cache entry with nothing behind it until next access.
        # Sequential, unlike catalogs()/verify()'s own concurrent
        # asyncio.gather() over the same kind of "several independent
        # siblings" loop: this loop's own exception handling needs to
        # stay a plain try/except to remain broad (see below), and
        # set_key() itself never needs to be fast enough to be worth
        # the added complexity of doing that against a list of
        # already-caught `gather(..., return_exceptions=True)` results
        # instead.
        for index in already_opened:
            try:
                await self._open_catalogs.resolve(index)
            except Exception as exc:
                # Broad, not just ApmRepoError: *any* failure here
                # (a transient storage I/O error, not just a corrupt
                # repo_info) must still fall through to the close loop
                # below -- otherwise the old, already-invalidated
                # DedupRepo/SaasStreamCache resources this repository is
                # replacing would leak for its whole remaining
                # lifetime instead of being closed.
                errors.append(exc)
        for old_opened in already_opened.values():
            # saas_streams closed first: its streams hold connections
            # opened against the *old* dedup_repo, so it must never
            # outlive the DedupRepo it was built against.
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
        ``Catalog`` wrapper around one opened catalog's resources plus a
        ``Connection`` row — so a future ``Catalog.__init__`` signature
        change only needs updating here, not at both call sites
        independently."""
        return Catalog(
            opened.dedup_repo,
            connection,
            saas_streams=opened.saas_streams,
            track=self._track,
            require_key_verified=self._require_key_verified,
        )

    async def catalogs(self) -> list[Catalog]:
        """Every catalog this repository holds — a vault's own
        ``connection_config`` rows (sharing one physical dedup pool), or
        one per independently-opened object-storage sibling repo-id —
        wrapped uniformly as ``Catalog``. Opens every not-yet-opened
        catalog concurrently (``asyncio.gather``, the same pattern
        ``units/device_pcps.py`` already uses for concurrent PC/PS fragment
        opens) rather than one at a time; each opened catalog's own
        ``connections()`` listing is already resolved once, by
        ``_open_catalog_resources``, and cached on its ``_OpenCatalog``
        bundle for this ``DedupRepo``'s whole lifetime, so no I/O happens
        here beyond opening a not-yet-opened sibling.

        Never gated on the key, unlike ``Catalog.workloads()``/
        ``versions()`` (each given ``self._require_key_verified`` as a
        callback, so *they* still gate before doing anything a wrong/
        missing key would make meaningless): the rows this reads
        (``connection_config``, via ``connections()``) are genuinely
        unencrypted, plaintext regardless of encryption, and opening a
        ``DedupRepo`` itself never requires a key either
        (``DedupRepo.open()`` only raises for a key that was *given*
        and didn't resolve, never for no key at all) — so nothing here
        actually needs one. This also preserves the TUI's own existing
        flow: catalog names show up before any key prompt, which only
        appears once the user actually opens one.

        A specific catalog's own ``DedupRepo.open()`` failure — a
        corrupt ``repo_info``, a rejected key (``KeyMismatchError``), a
        transient storage I/O error, or a caller cancelling this call —
        is surfaced by re-raising it immediately, the same as any other
        method on this class: a caller must never mistake "one sibling is
        broken" for "this bucket only has N-1 catalogs." This applies
        uniformly, ``asyncio.CancelledError`` included — no exception
        raised while opening a sibling is ever treated as "skip it and
        keep going."""
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
        listing does. For ``OBJECT_STORE``, ``catalog_id`` directly names
        one specific ``RepoLayout``'s own ``repo_id``, so every *other*
        sibling is skipped without ever opening it; for ``VAULT`` (whose
        ``RepoLayout.repo_id`` is always ``None``) there's only ever the
        one index to check anyway. A candidate that can't be ruled out by
        ``repo_id`` alone but then fails to open (a corrupt ``repo_info``,
        a rejected key) is not treated as "not it, keep looking" — the
        failure is re-raised immediately, the same as ``catalogs()``.
        Used by canonical-ref resolution
        (``_version_for_canonical_ref``) and by a caller (e.g. the TUI,
        re-fetching one already-known catalog after ``set_key()``) that
        wants one specific catalog without paying for ``catalogs()``'s own
        full-bucket listing — a hot enough path (every CLI
        ``ls``/``tree``/``cat``/``export`` on a canonical ref, and
        ``Repository.resolve()``'s own ``CANONICAL`` branch) that opening
        every sibling just to find the one already-known id would be
        real, avoidable I/O on an object-storage bucket with several."""
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
        """A plain, no-I/O check for whether ``workload`` has a chance at
        an application-layer provider — ``True`` doesn't guarantee every
        individual version actually resolves, only that the workload type
        is one ``provider_for``/``saas_provider_for`` recognize at all. The
        one place the CLI's ``doctor`` command needs this, so this is the
        one facade method exposing it — never import
        ``units.dispatch`` directly from CLI/TUI code."""
        return _workload_is_supported(workload)

    async def file_map_tree(self) -> UnitProvider:
        """The diagnostic fallback axis of last resort — browsable even
        when catalog metadata is missing or unhelpful (e.g. an empty
        ``copy_meta_file``). Always the *first* catalog
        (``_catalog_layouts[0]``, opened here if not already) — a ``RAW``
        ref's grammar carries no catalog segment at all, so this doesn't
        disambiguate between object-storage siblings; a pre-existing
        limitation of the diagnostic-only raw axis."""
        opened = await self._open_catalogs.resolve(0)
        return self._track(FileMapTreeProvider(opened.dedup_repo))

    async def invalidate_directory_cache(self) -> None:
        """Drop every cached directory listing for every catalog this
        repository has opened so far — for a caller (the TUI's "refresh"
        action) that wants its next provider/catalog call to re-scan the
        store instead of answering from whatever was listed earlier this
        session. A catalog not yet opened has nothing cached to drop."""
        for opened in self._open_catalogs.values():
            await opened.dedup_repo.dir_cache.invalidate()

    async def resolve(self, ref: str | NodeRef, *, object_db_id: str | None = None) -> Node | RestorableUnit:
        """Turn a ``NodeRef`` (or its string form) into a live node/unit,
        re-derived from cheap catalog/provider-tree calls rather than
        stored anywhere. Callable directly here — the common
        one-repository-per-run CLI case; ``Session.resolve`` adds picking the
        right repository among several open ones. ``ref.repo_path`` is ignored
        entirely (canonical/human refs never need it).
        """
        node_ref = ref if isinstance(ref, NodeRef) else NodeRef.parse(ref)
        # Normalize repo_path to this repository's own root before
        # searching: tree nodes carry ``layout.repo_root``, but a
        # CLI-supplied ref carries whatever path the user typed.
        # ``repo_path`` has already done its only job (picking this
        # Repository, in Session.resolve()) by the time we get here.
        node_ref = dataclasses.replace(node_ref, repo_path=self.layout.repo_root)
        kind = node_ref.kind
        if kind is RefKind.RAW:
            # Resolves node_ref against file_map_tree()'s provider tree —
            # for most providers, extra_segments grows by exactly one
            # segment per tree level, so a real prefix-match walk visits
            # only the nodes on the path to the target instead of the whole
            # tree; Drive/Team Drive can't support that and implement
            # SupportsDirectRefLookup instead.
            return await _resolve_in_provider(await self.file_map_tree(), node_ref)
        if kind is RefKind.CANONICAL:
            # Same resolution as the RAW branch above, over the named
            # version's own provider tree instead. ``object_db_id`` only
            # applies here — a raw ref already names a file_map path
            # directly, and a human ref (below) has no specific location
            # yet for it to disambiguate.
            catalog, version = await self.version_for_ref(node_ref)
            provider = await catalog.provider(version, object_db_id=object_db_id)
            return await _resolve_in_provider(provider, node_ref)
        # Human ref: walks display names instead — catalog -> workload ->
        # version via catalogs()/Catalog.workloads()/Catalog.versions(),
        # then into the provider tree by Node.name — applying the same
        # collision-suffix disambiguate() scheme the CLI/TUI use when
        # *displaying* names, so a name copied from a breadcrumb resolves
        # back.
        return await _resolve_human_ref(self, node_ref)

    async def version_for_ref(self, node_ref: NodeRef) -> tuple[Catalog, Version]:
        """Resolve a canonical ref's ``cat:``/``wl:``/``ver:`` prefix down
        to its owning ``Catalog`` and ``Version`` — the first half of what
        ``resolve`` does for a canonical ref, exposed on its own for
        callers that need the version's own ``provider`` rather than one
        specific node inside it (e.g. the CLI's ``ls``/``tree``, which
        list a resolved node's *children*). The ``Catalog`` is part of the
        result (not just the ``Version``) because building a provider
        needs to know *which* catalog's own ``DedupRepo`` to dispatch
        against — no longer implied by "the repository's one dedup
        catalog" now that a repository can hold several."""
        return await _version_for_canonical_ref(self, node_ref)

    async def walk_human_ref(self, segments: tuple[str, ...], *, object_db_id: str | None = None) -> Frame:
        """Walk a human ref's ``segments`` as far as they go, through the
        four levels catalog -> workload -> version -> item tree, raising
        ``NotFoundError`` the moment a segment doesn't match anything at its
        level. ``object_db_id`` is forwarded to ``provider`` once
        ``segments`` reaches a version — it's a no-op until then.

        The shared primitive behind both ``resolve``'s human-ref branch
        (drains straight to a leaf/subtree) and ``cli/browse.py``'s
        ``ls``/``tree`` breadcrumbs (which must also stop at an
        intermediate depth that doesn't yet name a complete ``Node``/
        ``RestorableUnit``, so a bare catalog or workload name is not
        itself an error here).

        Only this first level (picking the right ``Catalog``) lives here
        — the remaining three levels (workload/version/item) are
        ``Catalog.walk_human_ref``'s own job, since they no longer need
        anything from ``Repository`` once the catalog is chosen."""
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
        """Integrity check over every catalog this repository holds —
        each *distinct* ``DedupRepo`` verified exactly once (not once
        per ``Catalog``): a vault's own ``connection_config`` rows all
        share one physical pool, so naively looping ``Catalog.verify()``
        per row would re-run the identical whole-pool check that many
        times. Opens every not-yet-opened catalog first (same
        concurrent-open shape as ``catalogs()``, including the same
        "a specific catalog's own open failure aborts the call" posture
        — silently completing an integrity check that quietly skipped one
        whole catalog would misrepresent what was actually verified),
        then runs ``units.verify_reachable.verify_reachable()`` once per
        opened instance and concatenates every ``Finding`` — the
        top-down, reachability-scoped walk (Catalog -> Workload -> Version
        -> that version's own composition records). ``progress`` is
        optional and forwarded to each call as-is.

        Gated on ``_require_key_verified()`` the same as ``Catalog.
        workloads()``/``versions()`` — ``units.catalog.versions()``'s own
        browsable-status filter silently drops every row it can't decrypt
        ``version_spec`` for (indistinguishable, from inside that filter,
        from a genuinely non-browsable version), so an encrypted repository
        verified without a key would otherwise walk zero versions and
        report a misleadingly clean result instead of refusing to run.

        At FULL level with more than one catalog to verify, shares **one**
        multiprocess executor across every ``verify_reachable()`` call
        instead of letting each spin one up independently — but only when
        every catalog actually resolves to the identical
        ``PoolDescriptor`` (same store, ``pool_root``, vault key): a
        vault's own sibling catalogs always share one physical pool, so
        this is the common case, but an
        object-storage repository's sibling repo-ids can each be a
        genuinely separate store/pool — when they differ, this falls back
        to each ``verify_reachable()`` call building (and tearing down)
        its own executor, exactly as it already does with no shared one
        given."""
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
        including errors.

        Idempotent: a second call is a no-op. ``Session.close()`` closes
        every repository it ever yielded, including one a caller already
        closed early itself to release its resources ahead of the rest of
        a longer-running session -- a caller doing that doesn't need its
        own bookkeeping to avoid a redundant second close.

        This never touches this repository's own ``ObjectStore`` -- by
        design, since one ``discover()``/``discover_remote()`` call's
        repositories can share one store, so only ``Session`` (which tracks
        every store across every repository) can tell whether it's safe to
        release. A caller that wants the store released early too, not just
        this repository's own catalog/provider state, should call
        ``Session.close_repo()`` instead of a bare ``repo.close()``.
        """
        if self._closed:
            return
        self._closed = True
        # Every tracked provider gets a close attempt regardless of
        # whether an earlier one raised — a leaked aiosqlite connection
        # blocks interpreter exit forever, so "attempt all, then report" is
        # the only posture consistent with that invariant. Failures are
        # collected rather than swallowed: they still surface to the
        # caller, just without abandoning every later item in the loop.
        errors: list[Exception] = []
        for provider in self._providers:
            if isinstance(provider, ClosableUnitProvider):
                try:
                    await asyncio.wait_for(provider.close(), timeout=_RESOURCE_CLOSE_TIMEOUT)
                except Exception as exc:
                    errors.append(exc)
        self._providers.clear()
        # Settle every in-flight fetch (a catalogs()/verify() call racing
        # this close()) before closing each one that lands, instead of
        # leaking it -- known_keys() catches a fetch already started by
        # such a racing caller, which a snapshot of only already-settled
        # entries would otherwise miss and leave uncloseable afterward.
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
        # Closing each DedupRepo is not the same as forgetting it: without
        # this the cache keeps every one of them (and everything they in turn
        # reference) alive for as long as anything holds this Repository.
        # ``_closed`` is set before this, so a resolve racing the invalidate
        # raises instead of building a replacement nothing would ever close.
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
    """Picks the right ``Catalog`` by the ref's own ``CatalogId`` first
    (``Repository.catalog_by_id``, opening only what that specific id
    could possibly match), then searches only *that* catalog's
    ``workloads()``/``versions()`` — never scanning across catalogs by
    ``connection_config_id`` alone, which collides across object-storage
    siblings."""
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
    """A human ref names a complete path (never object_db_id — a human ref
    has no specific location yet for it to disambiguate, unlike a canonical
    ref naming a version directly), so this only adds the
    "at least catalog/workload/version" length check on top of
    ``Repository.walk_human_ref``, then unwraps its final ``Frame`` into the
    leaf/subtree ``resolve()`` promises. Below this length,
    ``walk_human_ref`` would stop at an intermediate
    ``catalog``/``workload`` ``Frame`` instead of raising — exactly what
    ``ls``/``tree``'s own breadcrumb walk wants, but not a valid
    ``resolve()`` result."""
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
