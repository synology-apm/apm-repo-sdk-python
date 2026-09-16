"""``verify_reachable()``: the top-down, reachability-scoped integrity
check — what ``Repository.verify()``/``Catalog.verify()`` (the CLI/TUI's
own entry point) actually call. See ``ARCHITECTURE.md``'s Dedup Layer
section for the FULL/QUICK validation-tier table this module implements.

Walks Catalog → Workload → Version → this version's own composition
record(s) (via ``composition_extents_for_version`` below), and only
checks data actually reachable that way — a stale/orphaned ``file_map``
row nothing legitimate still references is never visited.

**Composition records get checked exhaustively; buckets get checked
whole.** No row cap, no bucket-count cap — every touched
``(stream_id, bucket_id)`` gets checked once no matter how many separate
chunk-map entries or versions touch it, and a claimed bucket's
*candidate* chunks are every live (non-``COMPACTED``) physical chunk in
it, not just the specific chunk-map-referenced indices that touched it —
trading a small amount of extra dead-chunk checking for dropping all
per-chunk-index bookkeeping, which is what makes the bucket-level
batching and concurrency below practical.

- **FULL**: every live chunk in a touched bucket gets both its ciphertext
  CRC32 and — once a vault key is available — its decrypt+decompress+
  SHA-256 fingerprint checked, via one merged ``chunk_walk.exec_chunks``
  pass per bucket. As thorough, and as costly, as a real full export of
  the whole repository, not a bounded sample.
- **QUICK**: no chunk content is read or checked at all — only each
  touched bucket's own structural checks (``check_bucket_structure()``:
  file size vs. ``expected_bucket_size()``, ChunkCrcStore trailer
  self-consistency, SizeStore-repair reporting) plus the header-derived
  ``Symptom.KEY_MISSING`` check. To check chunk content, use FULL — there
  is deliberately no sampled middle tier.

This module never checks whether a key is needed before running —
``Repository.verify()``/``Catalog.verify()`` gate on
``_require_key_verified()`` first, the same guard ``workloads()``/
``versions()`` already use. Without that gate this walk would see zero
versions for a confirmed-encrypted repository run without a key and
report a misleadingly clean result; a caller bypassing that gate is on
its own, though the per-bucket ``Symptom.KEY_MISSING`` handling below
still covers it.

Every catalog-listed, non-deleted version is attempted directly, the
same raw list ``Catalog.versions()`` itself now returns — a version this
walk can't resolve reports a ``Finding(Stage.VERSION,
Symptom.DATA_MISSING, ...)`` here and raises the same way when actually
opened for browsing; the two are not two different views of the catalog.
GW/M365's own resolution (``units.saas.stream.SaasStream.open_saas_obj``)
already forward-resolves past routine, backend-side generation rotation
before this walk ever sees a failure, so a GW/M365 ``Finding`` here means
a genuine gap, not the common case an earlier, less complete resolution
once made it look like.

Every ``Finding`` is tagged with the exact catalog/workload/version being
checked when it was found (``Finding.ref`` — see its own docstring for
the one caveat this implies for dedup'd data shared across versions);
``_bucket_claim`` is the bucket-stage tagging mechanism.

**Scope, stated as what's actually checked:**

1. ``link.key`` ↔ ``repo_state.link_key`` (APV) consistency is not
   checked.
2. Generation-selection auditing confirms that each supplemental DB
   (``copy_target_version``, ``file_meta``, ...) this walk queries opens;
   it does not separately audit every other DB in the repository.
3. A bucket physically present in ``Pool/`` but not referenced by any
   live, resolvable version (a "dangling" bucket — the mirror image of a
   referenced-but-missing one, which *does* surface as a ``Finding``) is
   never enumerated or reported: doing so needs a full recursive listing
   of ``Pool/``'s own arbitrary-depth directory tree, a genuinely
   different, unmeasured-cost operation from the catalog-driven walk this
   module performs.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import dataclasses
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor

from ..catalog.connection import connections
from ..catalog.version import Version, versions
from ..catalog.workload import TargetType, Workload, workloads
from ..concurrency import (
    close_worker_loop,
    default_worker_count,
    dispatch_to_pool,
    new_process_pool,
    run_in_worker_loop,
)
from ..dedup.chunk_walk import ChunkPlan, ChunkRun, exec_chunks, plan_chunks_windowed
from ..dedup.composition_reader import CompositionReader, CompositionRecord
from ..dedup.dedup_file import DedupFile
from ..dedup.pool import BucketReader, BucketReaderCache, Pool
from ..dedup.pool_descriptor import PoolDescriptor, aclose_worker_store, build_worker_pool
from ..dedup.repository import DedupRepo
from ..dedup.verify_checks import (
    Finding,
    Stage,
    Symptom,
    VerifyLevel,
    check_bucket_structure,
    check_chunk_ciphertext_crcs,
    check_composition_header,
    check_map_and_attr_crc,
    check_record_head,
    check_repo_info,
)
from ..errors import ApmRepoError, DataCorruptError, FormatError, NotFoundError, PermissionDeniedError
from ..format.addressing import group_start_bucket_id
from ..identifiers import BucketId, SessionId, StreamId
from ..presentation.progress import Progress
from ..storage.base import ObjectStore
from .base import Node
from .content.pcps_disk import VirtualDiskContentSource
from .device import DeviceProvider
from .device_kind import _NodeKind
from .fs import FsProvider
from .node_ref import canonical_ref_for
from .saas.stream import SaasStreamCache

_ProgressCallback = Callable[[Progress], Awaitable[None]]

_BUCKET_BATCH_SIZE = 256
"""Buckets accumulated (claimed but not yet moved into the check phase's
own input list) during discovery before ``verify_reachable``'s discovery
loop calls ``finalize_pending_buckets`` — a starting value, not a
measured one; see this module's own docstring for why batching+
concurrency exists at all."""

_STALE_ROTATED_SUFFIX = "possibly a stale/rotated reference"
"""Appended (behind an em dash) to a ``NotFoundError``'s own message at
every resolution-failure site in this module that produces a
``Symptom.DATA_MISSING`` ``Finding`` — one shared literal instead of three
separately-typed copies that could drift apart. The three sites:
``_unresolvable_finding`` (VM/PC-PS disk/fragment failures),
``discover_version``'s own catch (FS composition-resolution failures —
GW/M365 uses ``_SAAS_GENUINE_GAP_SUFFIX`` instead, see below), and
``_open_bucket_or_finding`` (a Pool-bucket-level case unrelated to any
workload type's own resolution). Editing the wording here changes all
three at once — check each site's own reasoning still applies before
doing so."""

_SAAS_GENUINE_GAP_SUFFIX = "a genuine SaaS resolution gap, not routine backend-side generation rotation"
"""``discover_version``'s GW/M365-specific counterpart to
``_STALE_ROTATED_SUFFIX``. Unlike the shared hedge (calibrated for a case
that could still be either routine rotation or genuine loss), a GW/M365
``NotFoundError`` reaching here is already past every routine-rotation
case ``SaasStream.open_saas_obj`` can resolve — either its own forward
search found nothing live through ``stream_info.
latest_complete_version``, or ``stream_version_for`` found no matching
``version_info`` row at all (a stream this version's own catalog row
claims to belong to never actually recorded it — a different, still
genuine gap, not a rotation case forward-resolution has any bearing on).
Both are a strictly more confident signal than "possibly stale/rotated"
admits, so the wording stays deliberately generic across them rather
than naming just one."""

_MAX_CONCURRENT_BUCKET_CHECKS = 8
"""Concurrent ``_check_one_bucket`` tasks per ``check_all_buckets`` call
(also reused to bound ``finalize_pending_buckets``'s own concurrent
``ObjectStore.size()`` stats at FULL level). Per-chunk decode work is
CPU-bound, so more concurrent tasks add scheduling overhead without more
decode parallelism, and the storage backend itself can also start
queuing under too many outstanding requests."""


@dataclasses.dataclass(frozen=True)
class CompositionExtent:
    """One composition record's byte range a ``Version``'s own content
    lives in — ``[start, end)`` within ``dedup_file``, which already
    exposes its own ``(stream_id, session_id, comp_offset)`` triple
    directly (``DedupFile.stream_id``/``.session_id``/``.comp_offset``),
    so this doesn't duplicate those fields. A VM/FS/SaaS version has
    exactly one of these; a PC/PS version has one per disk fragment.
    """

    dedup_file: DedupFile
    start: int
    end: int
    unit_label: str
    """For a ``Finding.path`` — e.g. ``"VM disk 'disk1.vmdk'"``,
    ``"PC/PS disk fragment fid=42"``."""


@dataclasses.dataclass(frozen=True)
class _RecordCheckState:
    """Once-per-run outcome of checking one composition record's own
    ``RecordHead``/``map_crc`` — ``_ReachabilityWalker._ensure_record_checked``'s
    own memoization payload (``_record_checks``, the one place this is
    written), so a second version/extent sharing the same ``(stream_id,
    session_id, comp_offset)`` never re-runs either check."""

    broken: bool = False
    """``RecordHead`` itself failed to parse — ``plan_chunks_windowed``
    would only re-raise the same corruption, so it's skipped entirely on
    every visit to this record."""
    repaired_map_array: bytes | None = None
    """Non-``None`` exactly when this record's ``map_crc`` mismatch was
    confirmed-repaired via parity — fed into every visiting extent's own
    shared ``CompositionRecord`` (``CompositionRecord.seed_pages_from_array``,
    via ``_ReachabilityWalker._composition_records``) so it never re-derives
    entries from the still-corrupted on-disk bytes."""


async def _all_children(provider: DeviceProvider, node: Node) -> list[Node]:
    """Page through ``provider.children(node)`` fully. Only ever called on
    a VM/PC-PS device or disk-listing node — a handful of entries per
    version, never the FS/SaaS per-file/per-item scale this module
    deliberately never walks (see ``composition_extents_for_version``'s
    own docstring).

    The same "page to exhaustion" shape as ``units/resolve.py``'s own
    private ``_all_children``/``_iter_pages``, duplicated rather than
    imported across that module boundary (a leading-underscore name is
    module-private by convention) — kept independent since a handful of
    real entries per call makes the exact page size moot either way,
    unlike ``resolve.py``'s own tree-walk callers, whose page size trades
    off against a wide, per-node-fetch-bound level."""
    nodes: list[Node] = []
    offset = 0
    page = 1024
    while True:
        batch = await provider.children(node, offset=offset, limit=page)
        nodes.extend(batch)
        if len(batch) < page:
            return nodes
        offset += page


async def composition_extents_for_version(
    repo: DedupRepo, workload: Workload, version: Version, saas_streams: SaasStreamCache
) -> tuple[list[CompositionExtent], list[Finding]]:
    """Every composition record this version's own content lives in — one
    for a VM/FS/SaaS version's single dedup image/stream, one per disk
    fragment for a PC/PS version. Reuses each workload type's own,
    already-implemented resolution (``DeviceProvider``/``FsProvider``/
    ``SaasStream`` via ``saas_streams``, the exact machinery real
    browsing uses) rather than re-deriving any path/SQL logic — this
    function is a thin adapter over each, not a second implementation of
    "how does a version's content resolve." ``saas_streams`` is the
    caller's own run-scoped ``SaasStreamCache`` (``_ReachabilityWalker``
    owns one) — GW/M365's own forward-resolution caching (see
    ``units.saas.stream``) only pays off when every version sharing a
    stream resolves through the same ``SaasStream`` instance, not a
    fresh one per call.

    Deliberately does not recurse into a device version's guest
    filesystem (``DiskFsSibling``) or a SaaS version's individual
    Mail/Drive/Calendar items: both are byte-range/offset views into the
    *same* composition record(s) this function already returns, so
    walking them would only re-touch chunks already covered — a device
    provider's own node tree is shallow (root → children already are the
    leaf disks), confirmed by reading ``DeviceProvider.children()``.

    Returns ``(extents, findings)``: a VM/PC-PS version has more than one
    disk/fragment, and one of them failing to resolve must not discard
    every other disk's already-resolved content the same way every other
    stage in this module already treats a partial failure (see
    ``_discover_extent``'s own chunk-map-walk ``except`` clause) — so a
    per-disk resolution failure is folded into ``findings`` instead of
    raised, and every other disk keeps its own place in ``extents``.
    FS/SaaS always return an empty ``findings`` (a single dedup image/
    stream, nothing to be partial about).

    Raises:
        ApmRepoError: This version's metadata claims it should resolve
            to real content but doesn't resolve *at all* — the caller
            (``verify_reachable``) turns this into a ``Stage.VERSION``
            ``Finding`` rather than silently skipping the version.
    """
    match version.target_type:
        case TargetType.VM:
            return await _vm_extents(repo, version)
        case TargetType.PC | TargetType.PS:
            return await _pcps_extents(repo, version)
        case TargetType.FS:
            return await _fs_extents(repo, version), []
        case _:  # GW, M365
            return await _saas_extents(saas_streams, version), []


def _unresolvable_finding(label: str, exc: ApmRepoError) -> Finding:
    """A ``Stage.VERSION`` ``Finding`` for anything from a catalog/
    workload/version-enumeration failure down to a single disk/fragment's
    own ``provider.unit()`` failure -- see ``Stage.VERSION``'s own
    docstring ("covers failure at either the version level or the
    workload/connection-enumeration level above it") and
    ``composition_extents_for_version``'s docstring for why a disk/
    fragment failure specifically must not abort every other
    already-resolved one alongside it. ``NotFoundError`` gets the "possibly a
    stale/rotated reference" wording every other resolution-failure site
    in this module already uses; any other ``ApmRepoError`` (a genuinely
    corrupt row/table) is ``Symptom.CORRUPTION`` instead."""
    if isinstance(exc, NotFoundError):
        detail = f"{exc} — {_STALE_ROTATED_SUFFIX}"
        return Finding(Stage.VERSION, Symptom.DATA_MISSING, label, detail)
    return Finding(Stage.VERSION, Symptom.CORRUPTION, label, str(exc))


async def _vm_extents(repo: DedupRepo, version: Version) -> tuple[list[CompositionExtent], list[Finding]]:
    extents: list[CompositionExtent] = []
    findings: list[Finding] = []
    async with await DeviceProvider.create(repo, version) as provider:
        for device_node in await _all_children(provider, provider.root()):
            for object_node in await _all_children(provider, device_node):
                attrs = object_node.attrs
                if attrs.get("_kind") != _NodeKind.OBJECT:
                    continue  # a disk-fs "(filesystem)" sibling node, not new dedup content
                if not attrs.get("dedup_object") or attrs.get("unsupported"):
                    continue  # a plain sidecar file (no Pool addressing at all), or a
                    # data_format this SDK deliberately refuses to read (CBT chains) --
                    # nothing this walk can check either way.
                label = f"VM disk {object_node.name!r}"
                try:
                    unit = await provider.unit(object_node)
                except ApmRepoError as exc:
                    findings.append(_unresolvable_finding(label, exc))
                    continue
                content = unit.content
                if not isinstance(content, DedupFile) or content.size is None:
                    continue
                extents.append(CompositionExtent(content, 0, content.size, label))
    return extents, findings


async def _pcps_extents(repo: DedupRepo, version: Version) -> tuple[list[CompositionExtent], list[Finding]]:
    extents: list[CompositionExtent] = []
    findings: list[Finding] = []
    async with await DeviceProvider.create(repo, version) as provider:
        for disk_node in await _all_children(provider, provider.root()):
            if disk_node.attrs.get("_kind") != _NodeKind.PCPS_DISK:
                continue  # a disk-fs sibling node
            try:
                unit = await provider.unit(disk_node)  # may raise NotFoundError -- every fragment unresolvable
            except ApmRepoError as exc:
                findings.append(_unresolvable_finding(f"PC/PS disk {disk_node.name!r}", exc))
                continue
            content = unit.content
            if not isinstance(content, VirtualDiskContentSource):
                continue
            extents.extend(
                CompositionExtent(frag.dedup_file, frag.start, frag.end, f"PC/PS disk fragment fid={frag.fid}")
                for frag in content.fragments
            )
    return extents, findings


async def _fs_extents(repo: DedupRepo, version: Version) -> list[CompositionExtent]:
    provider = FsProvider(repo, version)
    try:
        # FsProvider's own node tree is the guest file tree itself (real
        # per-file entries, potentially huge) -- walking it would only
        # re-touch the one shared dedup.img every file is already a byte
        # range into, so this reaches for the same image directly instead.
        dedup_img = await provider._dedup_img()  # noqa: SLF001 - see this module's own docstring
    finally:
        await provider.close()
    if dedup_img.size is None:
        return []
    return [CompositionExtent(dedup_img, 0, dedup_img.size, "FS dedup.img")]


def _annotate_with_saas_resolution(label: str, saas_streams: SaasStreamCache, version: Version) -> str:
    """Appends "(stream_version R, requested Q)" to ``label`` when
    ``version`` is GW/M365 and its ``saas_obj`` was substituted during
    forward resolution (see ``units.saas.stream``'s own module
    docstring) — unchanged for a plain/non-substituted resolution, or a
    non-SaaS version. Synchronous and free
    (``SaasStreamCache.last_open_resolution`` reads only already-cached
    state, no new I/O) — meaningful only right after this same
    ``saas_streams``'s own ``open_saas_obj(version)`` already succeeded
    for this exact ``version``, otherwise degrades to ``label`` unchanged.

    Used only by ``_saas_extents``, which feeds the result into
    ``CompositionExtent.unit_label`` — reaching a real ``Finding.path``
    only for a ``Stage.COMPOSITION``/``Symptom.CORRUPTION`` finding (a
    chunk-map-walk or parity-repair-reseed failure). A ``Stage.BUCKET``/
    ``Symptom.MISMATCH`` finding (the common per-chunk case) is tagged
    from ``_bucket_claim``'s own ``ref`` only (``_tag_with_claim``) — its
    ``label`` half feeds ``Progress.detail`` during the checking phase,
    never a ``Finding`` field, so this function's own result never
    reaches one that way."""
    if version.target_type not in (TargetType.GW, TargetType.M365):
        return label
    resolution = saas_streams.last_open_resolution(version)
    if resolution is None:
        return label
    requested, resolved = resolution
    if resolved == requested:
        return label
    return f"{label} (stream_version {resolved}, requested {requested})"


async def _saas_extents(saas_streams: SaasStreamCache, version: Version) -> list[CompositionExtent]:
    """``saas_streams`` owns the underlying ``SaasStream`` and its
    lifetime (closed with the cache, not here) — see
    ``composition_extents_for_version``'s own docstring for why this
    must go through the caller's shared cache rather than construct its
    own."""
    dedup_file = await saas_streams.open_saas_obj(version)
    if dedup_file.size is None:
        return []
    label = _annotate_with_saas_resolution("SaaS saas_obj", saas_streams, version)
    return [CompositionExtent(dedup_file, 0, dedup_file.size, label)]


async def _noop_on_run(offset: int, data: bytes | memoryview) -> None:
    """``exec_chunks`` needs an ``on_run`` sink; this walk only cares
    about the checks ``BucketReader.read_chunks``/``Pool.verify_fingerprints``
    already perform as a side effect of decoding, never the decoded bytes
    themselves."""


class _ReachabilityWalker:
    """Per-run state for ``verify_reachable``'s three-phase walk: a
    private ``Pool`` (forcing both ``verify_fingerprint``/
    ``verify_ciphertext_crc`` on, for FULL's exhaustive per-chunk sweep)
    and ``BucketReaderCache`` (never the repository's own shared ones —
    a run-scoped sweep across every touched bucket would otherwise thrash
    the repository's own small, session-wide LRU for every other
    concurrent caller), plus the memoization state that keeps a
    bucket/composition record shared by two versions from being sized or
    checked twice.

    Discovery (``discover_version``/``_discover_extent``, which also
    resolves each claimed bucket's own byte size at FULL level via
    ``finalize_pending_buckets``) and checking (``_check_one_bucket``, run
    from ``check_all_buckets``) are two separate passes, never
    interleaved — see ``finalize_pending_buckets``'s own docstring for why.
    """

    def __init__(
        self,
        repo: DedupRepo,
        level: VerifyLevel,
        *,
        progress: _ProgressCallback | None = None,
        executor: ProcessPoolExecutor | None = None,
    ) -> None:
        self._repo = repo
        self._level = level
        self._progress = progress
        self._pool = Pool(
            repo.store,
            repo.pool_root,
            repo.dir_cache,
            vault_key=repo.vault_key,
            verify_fingerprint=True,
            verify_ciphertext_crc=True,
        )
        self._bucket_cache = BucketReaderCache()
        # Owned for this run's whole lifetime, not per-version -- see
        # composition_extents_for_version's own docstring for why a
        # fresh SaasStream per version would silently defeat its
        # forward-resolution caching. Closed by this walker's own close().
        self._saas_streams = SaasStreamCache(repo)
        # `executor`, when given, is a caller-owned pool this walker never
        # closes (see `Repository.verify()`'s own multi-catalog fan-out,
        # which shares one across several `verify_reachable()` calls
        # instead of each spinning up its own). `None` means this walker
        # builds one itself, lazily, the first time `check_all_buckets()`
        # actually needs it at FULL level -- and then owns closing it.
        self._executor = executor
        self._owns_executor = executor is None
        self._pool_descriptor = PoolDescriptor.from_repo(repo, verify_fingerprint=True, verify_ciphertext_crc=True)
        self._checked_sessions: set[tuple[StreamId, SessionId]] = set()
        self._record_checks: dict[tuple[StreamId, SessionId, int], _RecordCheckState] = {}
        """Every composition record ``_ensure_record_checked`` has settled
        this run — read on every visit to a given key, not just the first,
        so a later version/extent sharing one still gets its own
        ``broken``/``repaired_map_array`` outcome without re-running either
        check. Unconditional growth, no eviction: bounded by how many
        distinct records this run actually visits, not by any one record's
        own ``map_num``."""
        self._composition_records: dict[tuple[StreamId, SessionId, int], CompositionRecord] = {}
        """Every ``CompositionRecord`` (with its own already-fetched page
        cache) this run has resolved, keyed the same as ``_record_checks``
        — ``_discover_extent`` hands a later version/extent sharing one
        this same, already-warm instance instead of letting its own fresh
        ``DedupFile`` cold-refetch and reparse the identical chunk-map
        array again. Incremental-forever backups routinely have hundreds
        of versions whose file content — and so whose composition record —
        never changed; without this, this walk paid that record's full
        page-fetch cost once per version instead of once per run. Same
        unconditional-growth, no-eviction trade-off as ``_record_checks``."""
        self._walked_extent_windows: set[tuple[tuple[StreamId, SessionId, int], int, int]] = set()
        """Every ``(record_key, start, end)`` window ``_discover_extent``
        has already walked via ``plan_chunks_windowed`` to completion this
        run -- a later version/extent sharing that exact window (same
        shared, already-cached ``CompositionRecord`` via
        ``_composition_records``, same declared file size) skips the walk
        entirely rather than re-deriving an identical ``(stream_id,
        bucket_id)`` key set, since that set is a pure function of the
        record's own bytes and this window. A membership set, not a
        key-set-valued cache: every key the walk would ever produce for a
        window already in here was already claimed into ``_bucket_claim``
        by the same walk that added it (the only writer), so there is
        nothing left for a hit to do beyond skipping the walk -- reusing a
        stored key set to re-claim it would be redundant at best, and at
        worst override "first claimer wins" with whichever later version
        happened to hit the cache. Same unconditional-growth, no-eviction
        trade-off as ``_record_checks``/``_composition_records``."""
        self._pending_buckets: list[tuple[StreamId, BucketId]] = []
        """Claimed since the last ``finalize_pending_buckets()`` call --
        drained into ``_buckets_to_check`` (and, at FULL level,
        ``_bucket_sizes``) by that call, not by checking (see that
        method's own docstring for the discover-then-check split)."""
        self._bucket_claim: dict[tuple[StreamId, BucketId], tuple[str, str]] = {}
        """Every claimed ``(stream_id, bucket_id)`` maps to whichever
        version claimed it first, as ``(ref, label)`` -- this dict's own
        keys *are* the claimed set, so no separate "already claimed" set
        is kept alongside it. ``ref`` is the canonical ``NodeRef`` string
        a claimed bucket's own findings get tagged with
        (``_tag_with_claim``); ``label`` is the same claim's human
        ``"workload/version"`` form, for ``Progress.detail`` during the
        check phase once there's no longer a single "current version" the
        way discovery had one. Kept as one dict, not two parallel ones
        keyed identically, since both are written together at the same
        claim site and read independently later -- the same reasoning
        that already keeps this class from also tracking a separate
        "already claimed" set alongside this one."""
        self._buckets_to_check: list[tuple[StreamId, BucketId]] = []
        """Every claimed bucket, in claim order, once
        ``finalize_pending_buckets()`` has moved it over -- the check
        phase's own input, built up entirely during discovery."""
        self._bucket_sizes: dict[tuple[StreamId, BucketId], int] = {}
        self._total_bytes_to_verify = 0
        self._bytes_verified = 0
        self._key_missing_reported = False

    async def close(self) -> None:
        """Closes this walker's own ``SaasStreamCache`` — every
        ``SaasStream`` it opened across the whole run. Nothing else this
        walker holds needs an explicit close (``self._pool``/
        ``self._bucket_cache`` hold no sockets/connections of their own)."""
        await self._saas_streams.close()

    @property
    def pending_bucket_count(self) -> int:
        return len(self._pending_buckets)

    async def discover_version(self, workload: Workload, version: Version) -> list[Finding]:
        """Walk this version's own composition extents, checking each
        record immediately but only *claiming* (not yet checking) the
        buckets it touches — see ``finalize_pending_buckets``/
        ``check_all_buckets`` for when a claimed bucket actually gets
        opened and checked. Returns only the composition-stage findings
        this version's own extents produced directly; a claimed bucket's
        own findings surface later, from ``check_all_buckets``, tagged
        via ``_bucket_claim`` rather than by this method.
        """
        label = f"{workload.display_name}/{version.display_name}"
        # Pure/no-I/O (see canonical_ref_for's own docstring) -- safe to
        # compute even when this version's own content fails to resolve at
        # all, so every Finding this call returns, including the
        # resolution-failure one below, can carry it.
        ref = str(canonical_ref_for(self._repo, version))
        try:
            extents, resolution_findings = await composition_extents_for_version(
                self._repo, workload, version, self._saas_streams
            )
        except ApmRepoError as exc:
            if isinstance(exc, NotFoundError):
                # GW/M365 already forward-resolved past every routine
                # generation rotation before raising (SaasStream.
                # open_saas_obj) -- a NotFoundError reaching here for it is
                # a strictly more confident signal than the shared hedge
                # every other workload type still needs.
                is_saas = version.target_type in (TargetType.GW, TargetType.M365)
                suffix = _SAAS_GENUINE_GAP_SUFFIX if is_saas else _STALE_ROTATED_SUFFIX
                detail = f"{exc} — {suffix}"
                return [Finding(Stage.VERSION, Symptom.DATA_MISSING, label, detail, ref=ref)]
            return [Finding(Stage.VERSION, Symptom.CORRUPTION, label, str(exc), ref=ref)]
        findings: list[Finding] = list(resolution_findings)
        for extent in extents:
            findings.extend(await self._discover_extent(extent, ref, label))
        # One place to tag every composition-stage Finding this version's
        # own checks produced with which version was being checked when it
        # was found -- see Finding.ref's own docstring for the shared-dedup
        # caveat this implies.
        return [dataclasses.replace(f, ref=ref) for f in findings]

    async def _ensure_record_checked(
        self, reader: CompositionReader, record_key: tuple[StreamId, SessionId, int], comp_offset: int, path: str
    ) -> list[Finding]:
        """Check this composition record's own ``RecordHead``/``map_crc``
        exactly once per run, caching the outcome in ``_record_checks``
        (this method is that dict's one writer) — a call for a
        ``record_key`` already in that cache returns ``[]`` immediately,
        its outcome already settled."""
        if record_key in self._record_checks:
            return []
        findings: list[Finding] = []
        head_finding, record_head = await check_record_head(reader, comp_offset, path=path)
        if head_finding is not None:
            findings.append(head_finding)
            self._record_checks[record_key] = _RecordCheckState(broken=True)
            return findings
        repaired_array: bytes | None = None
        if record_head is not None and record_head.map_num > 0:
            crc_findings, repaired_array = await check_map_and_attr_crc(reader, comp_offset, record_head, path=path)
            findings.extend(crc_findings)
        self._record_checks[record_key] = _RecordCheckState(repaired_map_array=repaired_array)
        return findings

    async def _discover_extent(self, extent: CompositionExtent, ref: str, label: str) -> list[Finding]:
        findings: list[Finding] = []
        stream_id = StreamId(extent.dedup_file.stream_id)
        session_id = SessionId(extent.dedup_file.session_id)
        comp_offset = extent.dedup_file.comp_offset
        # Cheap, stateless, no I/O to construct -- built fresh here rather
        # than reaching into DedupFile's own private _comp_reader, exactly
        # the same triple either way. (The *separate* _record crossing
        # below, priming/caching this run's own shared CompositionRecord,
        # is a different, deliberate one -- see self._composition_records's
        # own docstring.)
        reader = CompositionReader(self._repo.store, self._repo.dir_cache, self._repo.comp_root, stream_id, session_id)

        session_key = (stream_id, session_id)
        if session_key not in self._checked_sessions:
            self._checked_sessions.add(session_key)
            header_finding = await check_composition_header(reader, path=extent.unit_label)
            if header_finding is not None:
                findings.append(header_finding)

        record_key = (stream_id, session_id, comp_offset)
        findings.extend(await self._ensure_record_checked(reader, record_key, comp_offset, extent.unit_label))
        check_state = self._record_checks[record_key]
        if check_state.broken:
            # RecordHead itself failed to parse (this call, or an earlier
            # one sharing this record) -- already reported via
            # _ensure_record_checked's own Finding. plan_chunks_windowed()
            # below does its own independent RecordHead read via DedupFile's
            # own reader and would only re-raise the same corruption.
            return findings

        # Only ``chunk_walk``'s lower primitives are reused here, never
        # ``export_to()``/``Repository.export()``'s whole orchestration:
        # ``exec_chunks`` aborts its whole call on a corrupt chunk, correct
        # for a real export (a restore should stop, not hand back silently
        # wrong bytes) but wrong for verify, which must keep checking every
        # other bucket/version after one failure to report everything
        # wrong, not just the first thing. The cache lookup/fetch below is
        # inside this same try -- a cache-miss ``_get_record()`` can raise
        # exactly these same exceptions (e.g. the composition sub-file went
        # missing between ``_ensure_record_checked``'s own RecordHead read
        # moments ago and this one), and must follow the same catch-and-
        # continue contract as every other failure in this block, not abort
        # the whole run.
        try:
            cached_record = self._composition_records.get(record_key)
            if cached_record is not None:
                # A later version/extent sharing this record_key: hand this
                # DedupFile the same, already page-cached CompositionRecord an
                # earlier visit this run already resolved (dedup means its
                # chunk-map bytes are provably identical) instead of letting it
                # cold-refetch and reparse the whole array again -- see
                # self._composition_records's own docstring.
                extent.dedup_file._record = cached_record  # noqa: SLF001 - DedupFile/CompositionRecord are one cohesive unit (see chunk_walk.py, device_pcps.py's own precedent)
            else:
                cached_record = await extent.dedup_file._get_record()  # noqa: SLF001 - see comment above
                self._composition_records[record_key] = cached_record

            if check_state.repaired_map_array is not None:
                # This record_key's own shared CompositionRecord (cached/
                # primed just above) would otherwise carry entries re-derived
                # from the still-corrupted on-disk bytes this walker's own
                # check_map_and_attr_crc already proved wrong and fixed via
                # parity -- see CompositionRecord.seed_pages_from_array's own
                # docstring. Reseeded unconditionally (via check_state,
                # above), not just on the visit that first cached this
                # record -- idempotent and I/O-free (it reparses already-
                # in-memory repaired bytes), so a later version/extent
                # sharing this record_key still gets a correctly-seeded
                # record with no extra network cost.
                record = cached_record
                try:
                    await record.seed_pages_from_array(check_state.repaired_map_array)
                except ValueError as exc:
                    # seed_pages_from_array's own defensive length-mismatch
                    # check (a concurrent writer changed this record between
                    # this method's own RecordHead read and extent.dedup_file's
                    # independent one) -- caught here, at its own call site,
                    # rather than folded into the except below, which is
                    # scoped to plan_chunks_windowed's own corruption/
                    # not-found/format failures and must not also absorb an
                    # unrelated ValueError raised deeper in that call chain
                    # (chunk_walk.py's _validate_window_start guards a caller
                    # invariant, not corruption, and should crash loudly if
                    # ever violated).
                    findings.append(
                        Finding(
                            Stage.COMPOSITION,
                            Symptom.CORRUPTION,
                            extent.unit_label,
                            f"parity-repair reseed failed: {exc}",
                        )
                    )
                    return findings
            plan_key = (record_key, extent.start, extent.end)
            if plan_key not in self._walked_extent_windows:
                # A later version/extent sharing this exact (record_key,
                # start, end) would otherwise re-walk every one of this
                # record's chunk-map entries all over again just to
                # re-derive an identical (stream_id, bucket_id) key set --
                # deterministic, since it's a pure function of the record's
                # own (now shared/cached) bytes and this window. Claimed
                # incrementally as each window is yielded -- a later
                # window's own corrupt entry must still leave an earlier,
                # already-yielded window's buckets claimed rather than
                # discarding them too, the same catch-and-continue contract
                # this whole method follows. Only marked walked (below the
                # loop) once every window has been walked without error --
                # a walk that raises partway through must not mark this
                # window done, so it's simply re-walked (and re-claimed the
                # same way, a no-op for whatever it already claimed) the
                # next time a version shares this window.
                async for plan in plan_chunks_windowed(
                    extent.dedup_file, extent.start, extent.end, extent.start, write_zero_fill=None
                ):
                    for key in plan.groups:
                        if key not in self._bucket_claim:
                            self._bucket_claim[key] = (ref, label)
                            self._pending_buckets.append(key)
                self._walked_extent_windows.add(plan_key)
            # else: this window's own key set was already claimed in full
            # by whichever earlier version's walk first completed it (see
            # self._walked_extent_windows's own docstring) -- nothing left
            # to do.
        except (NotFoundError, DataCorruptError, FormatError) as exc:
            # A corrupt individual chunk-map entry deeper in the array (past
            # whatever check_map_and_attr_crc's own whole-array mapCrc
            # already validated) -- reported here rather than aborting this
            # version's remaining extents, or the whole run, the same
            # catch-and-continue contract every other stage in this module
            # already follows.
            findings.append(
                Finding(Stage.COMPOSITION, Symptom.CORRUPTION, extent.unit_label, f"chunk-map walk failed: {exc}")
            )
        return findings

    async def finalize_pending_buckets(self) -> None:
        """Move every currently-pending claimed bucket into
        ``_buckets_to_check`` — the check phase's own input, built up
        entirely during discovery, never interleaved with checking. This
        keeps the claim-before-check invariant ``_bucket_claim`` relies on
        race-free (no ``await`` between checking and setting membership,
        so two versions' discovery can never race to claim the same
        bucket), and is why ``check_all_buckets()`` can report progress
        against a real, stable total instead of one that would otherwise
        keep growing while checking is already under way.

        Called once ``_BUCKET_BATCH_SIZE`` buckets have accumulated, or
        discovery runs out of versions — a version's own extents can
        claim far more than ``_BUCKET_BATCH_SIZE`` buckets before this
        threshold check ever runs (it happens once per version, not once
        per claim), so the batch size is a trigger *floor*, not a hard cap
        on how many buckets one call moves over.

        At FULL level, also resolves each one's own on-disk byte size
        (concurrently, bounded by ``_MAX_CONCURRENT_BUCKET_CHECKS`` the
        same as the check phase below — a cheap ``ObjectStore.size()``
        stat, never a content read) and adds it to
        ``_total_bytes_to_verify``, since FULL's own per-bucket cost
        scales with how much of it actually gets decrypted+hashed. QUICK
        skips this sizing pass entirely: it reads no chunk content at all,
        so its own per-bucket cost is independent of bucket size regardless
        — a *bucket count*, not bytes, is the honest metric there, and
        needs no size lookup at all (crediting a whole multi-MB bucket's
        size to ``done`` the moment its own structural check finishes would
        inflate QUICK's own reported rate/ETA well past what it's actually
        doing).
        """
        if not self._pending_buckets:
            return
        batch, self._pending_buckets = self._pending_buckets, []
        if self._level is not VerifyLevel.FULL:
            self._buckets_to_check.extend(batch)
            return
        sem = asyncio.Semaphore(_MAX_CONCURRENT_BUCKET_CHECKS)

        async def _bounded(key: tuple[StreamId, BucketId]) -> None:
            async with sem:
                size = await self._size_one_bucket(key)
            self._bucket_sizes[key] = size
            self._total_bytes_to_verify += size
            self._buckets_to_check.append(key)

        async with asyncio.TaskGroup() as tg:
            for key in batch:
                tg.create_task(_bounded(key))

    async def _size_one_bucket(self, key: tuple[StreamId, BucketId]) -> int:
        """Called from inside ``finalize_pending_buckets``'s own
        ``asyncio.TaskGroup``, so — like ``_check_one_bucket`` — this must
        never let an exception escape: doing so would cancel every other
        in-flight sizing task in the same batch and abort the whole
        ``verify_reachable()`` call over what should only ever cost this
        one bucket 0 bytes toward the total."""
        stream_id, bucket_id = key
        try:
            path = await self._pool.bucket_path(stream_id, bucket_id)
            return await self._repo.store.size(path)
        except (NotFoundError, PermissionDeniedError):
            # Missing or inaccessible -- check_all_buckets() will surface
            # this properly (DATA_MISSING or, for PermissionDeniedError, its
            # own generic-catch-all CORRUPTION) once it actually tries to
            # open this key for real; sizing just treats a bucket it can't
            # even stat as contributing nothing to the total rather than
            # duplicating that failure here.
            return 0
        except Exception:
            # Anything else (an unexpected backend error) -- see this
            # method's own docstring for why it must not propagate.
            return 0

    async def check_all_buckets(self) -> list[Finding]:
        """Check every bucket ``finalize_pending_buckets()`` has already
        queued — see that method's own docstring for why every bucket is
        discovered (and sized) fully before any of them are checked this
        way.

        At FULL level, when this repository's store can be reconstructed in a
        fresh process (``self._pool_descriptor is not None``), dispatches
        through a real ``ProcessPoolExecutor`` instead of in-process
        ``asyncio`` concurrency — real multi-core parallelism for the
        CPU-bound decompress+ciphertext-CRC+SHA-256 work involved, which
        an in-process ``asyncio.TaskGroup``/``asyncio.to_thread()`` never
        actually gets past CPython's GIL (see ``ARCHITECTURE.md``'s
        "Async-native, by design" section for the measurement this is
        based on). QUICK, and any repository whose store isn't describable
        (a ``TracingStore``/``RecordingStore`` wrapper, or one built from
        an injected client), always uses the original in-process
        ``asyncio.TaskGroup`` path, bounded by
        ``_MAX_CONCURRENT_BUCKET_CHECKS``.

        Reports progress once *per completed bucket* either way, both
        ``done`` and ``total`` cumulative across this whole call — a real,
        meaningful percentage/rate/ETA, which needs discovery finished
        first: a total that kept growing while checking was already under
        way would make every percentage/rate/ETA reported before the end
        meaningless. At FULL level this is in bytes
        (``_total_bytes_to_verify``, already a stable figure by now); at
        QUICK it's a plain bucket count (``len(self._buckets_to_check)``)
        instead — see ``finalize_pending_buckets``'s own docstring for why
        bytes would be a *dishonest* metric at QUICK specifically, not
        just an unavailable one.
        """
        findings: list[Finding] = []
        full = self._level is VerifyLevel.FULL
        total_buckets = len(self._buckets_to_check)
        buckets_checked = 0

        async def _tick(key: tuple[StreamId, BucketId]) -> None:
            nonlocal buckets_checked
            # No await between the increment(s) and reading them into the
            # Progress below -- safe under cooperative concurrency the
            # same way _dedup_key_missing's own check-then-set is -- so
            # concurrent siblings finishing in the same event-loop tick
            # still each report their own, distinct running total.
            buckets_checked += 1
            if self._progress is not None:
                if full:
                    self._bytes_verified += self._bucket_sizes.get(key, 0)
                    done, total, unit = self._bytes_verified, self._total_bytes_to_verify, "bytes"
                else:
                    done, total, unit = buckets_checked, total_buckets, "buckets"
                await self._progress(
                    Progress(
                        phase="verifying",
                        determinate=True,
                        done=done,
                        total=total,
                        unit=unit,
                        detail=self._bucket_claim.get(key, ("", ""))[1],
                    )
                )

        if full and self._pool_descriptor is not None and self._buckets_to_check:
            return await self._check_all_buckets_multiprocess(_tick)

        sem = asyncio.Semaphore(_MAX_CONCURRENT_BUCKET_CHECKS)

        async def _bounded(key: tuple[StreamId, BucketId]) -> None:
            async with sem:
                findings.extend(await self._check_one_bucket(key))
            await _tick(key)

        async with asyncio.TaskGroup() as tg:
            for key in self._buckets_to_check:
                tg.create_task(_bounded(key))
        return findings

    async def _check_all_buckets_multiprocess(
        self, tick: Callable[[tuple[StreamId, BucketId]], Awaitable[None]]
    ) -> list[Finding]:
        """``check_all_buckets``'s FULL-level, store-describable path —
        split out only because it needs its own executor lifetime
        management, not because the dispatch shape itself is complex (see
        ``concurrency.dispatch_to_pool``, which owns that). Buckets are
        submitted sorted by ``group_start_bucket_id`` — not a rebalance,
        just a free ordering win: a worker's own ``AllocationTableCache``
        (built once per worker process, per ``_verify_worker_init``, kept
        for that worker's whole lifetime) is more likely to already be
        warm for the next bucket this way, since buckets sharing one
        ``.inf`` group land on the same worker back-to-back more often
        than a claim-order submission would."""
        assert self._pool_descriptor is not None
        findings: list[Finding] = []
        ordered = sorted(self._buckets_to_check, key=lambda key: group_start_bucket_id(key[1]))

        executor = self._executor
        if executor is None:
            executor = new_process_pool(initializer=_verify_worker_init, initargs=(self._pool_descriptor,))
            self._executor = executor

        async def _on_result(key: tuple[StreamId, BucketId], result: tuple[list[Finding], bool]) -> None:
            worker_findings, key_missing = result
            findings.extend(self._tag_with_claim(key, self._dedup_key_missing(worker_findings, key_missing)))
            await tick(key)

        try:
            await dispatch_to_pool(
                executor,
                _verify_bucket_worker,
                ordered,
                max_concurrent=default_worker_count(),
                on_result=_on_result,
            )
        finally:
            if self._owns_executor:
                # to_thread() hop for the same reason as dedup/export_scheduler.py's
                # own shutdown() call: a plain blocking shutdown(wait=True) here would
                # freeze this whole process's event loop until the slowest still-
                # running worker finishes its current bucket.
                await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)
        return findings

    def _tag_with_claim(self, key: tuple[StreamId, BucketId], findings: list[Finding]) -> list[Finding]:
        claim = self._bucket_claim.get(key)
        ref = claim[0] if claim is not None else None
        return [dataclasses.replace(f, ref=ref) for f in findings]

    async def _check_one_bucket(self, key: tuple[StreamId, BucketId]) -> list[Finding]:
        """Thin, ``self``-bound wrapper around the shared, module-level
        ``_check_one_bucket_core`` — see that function's own docstring for
        why the actual open→check→branch sequence lives there instead of
        here: this is the in-process path (called by ``check_all_buckets``
        from inside a concurrent ``asyncio.TaskGroup`` task at QUICK, or as
        the fallback when this repository's store can't cross a process
        boundary), and it must reuse the identical implementation
        ``_verify_bucket_worker`` (the multiprocess path) calls, not a
        hand-written copy of it."""
        findings, key_missing = await _check_one_bucket_core(
            self._pool, self._bucket_cache, self._repo.store, self._repo.vault_key, key, self._level
        )
        return self._tag_with_claim(key, self._dedup_key_missing(findings, key_missing))

    def _dedup_key_missing(self, findings: list[Finding], key_missing: bool) -> list[Finding]:
        """``_check_one_bucket_core`` always includes its own
        ``Symptom.KEY_MISSING`` finding when ``key_missing`` — the "report
        this only once per whole run" bookkeeping is a parent-only concern
        (only this walker instance has a notion of "this run"), applied
        here so both ``_check_one_bucket`` (in-process) and
        ``check_all_buckets``'s own multiprocess ``on_result`` callback
        share the identical dedup logic instead of each reimplementing
        it."""
        if not key_missing:
            return findings
        if self._key_missing_reported:
            return [f for f in findings if f.symptom is not Symptom.KEY_MISSING]
        self._key_missing_reported = True
        return findings


# -- Shared, `self`-free bucket-check core -----------------------------
#
# Everything below is module-level and takes every dependency as an
# explicit parameter, on purpose: `_check_one_bucket_core` is the one
# implementation both `_ReachabilityWalker._check_one_bucket` (in-process)
# and `_verify_bucket_worker` (multiprocess, below) call — a worker process
# has no `self` (no `_ReachabilityWalker` instance at all, just its own
# freshly-rebuilt `Pool`/`BucketReaderCache`/`ObjectStore`), so the only way
# to guarantee both paths run the identical open→check→branch sequence is
# to give that sequence no `self` to begin with.


async def _open_bucket_or_finding(
    pool: Pool, bucket_cache: BucketReaderCache, key: tuple[StreamId, BucketId]
) -> tuple[BucketReader, None] | tuple[None, list[Finding]]:
    """Opens the bucket at ``key``, converting
    ``NotFoundError``/``DataCorruptError``/``FormatError``/anything else
    into a ``Finding`` instead of raising — same broad-catch posture
    ``_check_one_bucket_core``'s own docstring explains (its caller, and
    the only reason this must never raise either), scoped here to just the
    open call's own failure modes. Returns ``(reader, None)`` on success or
    ``(None, findings)`` on failure — never both non-``None``. Findings
    returned here are untagged (no ``Finding.ref``) — tagging is a
    parent-only concern (``_ReachabilityWalker._tag_with_claim``)."""
    stream_id, bucket_id = key
    try:
        reader = await bucket_cache.buckets.resolve(key, pool.open_bucket_uncached_by_key)
    except NotFoundError as exc:
        return None, [
            Finding(
                Stage.BUCKET,
                Symptom.DATA_MISSING,
                f"bucket {stream_id}/{bucket_id}",
                f"{exc} — {_STALE_ROTATED_SUFFIX}",
            )
        ]
    except (DataCorruptError, FormatError) as exc:
        return None, [Finding(Stage.BUCKET, Symptom.CORRUPTION, f"bucket {stream_id}/{bucket_id}", str(exc))]
    except Exception as exc:
        # Same broad safety net _check_one_bucket_core's own docstring
        # explains, needed here too: a real ObjectStore can raise
        # something past NotFoundError/DataCorruptError/FormatError on
        # open (PermissionDeniedError, for one) that must still become a
        # Finding rather than escape into the caller's own concurrent
        # dispatch (an asyncio.TaskGroup, or a worker process boundary).
        return None, [
            Finding(Stage.BUCKET, Symptom.CORRUPTION, f"bucket {stream_id}/{bucket_id}", f"unexpected error: {exc}")
        ]
    return reader, None


async def _check_chunks_full(
    pool: Pool,
    bucket_cache: BucketReaderCache,
    key: tuple[StreamId, BucketId],
    reader: BucketReader,
    indices: list[int],
) -> list[Finding]:
    """Every one of ``indices``, ciphertext CRC *and*
    decrypt+decompress+fingerprint together, in one merged ``exec_chunks``
    pass — one read per chunk, not two. Runs against a throwaway ``Pool``
    constructed with both ``verify_fingerprint``/``verify_ciphertext_crc``
    forced on, reusing the same bucket-major batched reader and read
    merging every real export uses (see ``dedup/pool.py``'s own
    ``BucketReader``/``Pool``)."""
    # dest_offset_start is meaningless here (verify never writes
    # anything) -- size=0 makes every _flush_run() call a no-op
    # (write_len <= 0 unconditionally), so on_run is never actually
    # invoked; only the decode-time checks (ciphertext CRC inside
    # read_chunks, fingerprint via Pool.verify_fingerprints) matter.
    plan = ChunkPlan(groups={key: [ChunkRun(idx, 1, 0) for idx in indices]}, holes=0, zeros=0)
    try:
        await exec_chunks(plan, pool=pool, on_run=_noop_on_run, size=0, export_cache=bucket_cache)
    except (NotFoundError, DataCorruptError, FormatError) as exc:
        return [Finding(Stage.BUCKET, Symptom.MISMATCH, reader.path, f"chunk decode/fingerprint check failed: {exc}")]
    return []


async def _check_chunks_ciphertext_only(reader: BucketReader, indices: list[int]) -> list[Finding]:
    """FULL level's own path for a vault-encrypted bucket opened without a
    key: every one of ``indices`` still gets its ciphertext CRC32 checked
    (no decrypt attempt, so no key needed), but decode+fingerprint is
    skipped entirely for all of them rather than falling back to QUICK's
    small sample — the ``KEY_MISSING`` finding already reported once per
    run explains why fingerprinting can't run at all here.
    ``check_chunk_ciphertext_crcs`` batches the underlying reads (see its
    own docstring) — real here too, since a bucket opened at FULL without
    a key can still mean *every* live chunk in it gets this check."""
    return await check_chunk_ciphertext_crcs(reader, indices)


async def _check_one_bucket_core(
    pool: Pool,
    bucket_cache: BucketReaderCache,
    store: ObjectStore,
    vault_key: bytes | None,
    key: tuple[StreamId, BucketId],
    level: VerifyLevel,
) -> tuple[list[Finding], bool]:
    """Open (via ``_open_bucket_or_finding``), structurally check, and
    content-check one bucket — the ``self``-free core both
    ``_ReachabilityWalker._check_one_bucket`` (in-process) and
    ``_verify_bucket_worker`` (multiprocess, below) call identically, so a
    future change to this sequence can never land in one copy and not the
    other (see this module's own "Shared, self-free bucket-check core"
    section comment above).

    Called from inside a concurrent dispatch (an ``asyncio.TaskGroup``
    task, or a separate worker process), so this must never let an
    exception escape: past the specific
    ``NotFoundError``/``DataCorruptError``/``FormatError`` branches below,
    a final broad ``except Exception`` converts anything unexpected into a
    ``Finding`` too. Skipping that net would let a bug in one bucket's
    check propagate into the caller's own concurrent dispatch, which
    cancels/aborts every other in-flight sibling — a strictly worse blast
    radius than one bucket's own failure. ``asyncio.CancelledError`` is a
    ``BaseException``, not an ``Exception``, so real task cancellation
    still propagates through this net untouched.

    Returns ``(findings, key_missing)``: findings are **untagged** (no
    ``Finding.ref``) and, when ``key_missing``, *always* include their own
    ``Symptom.KEY_MISSING`` finding — the "report this only once per run"
    bookkeeping, like tagging, is a parent-only concern this core function
    has no state for; its caller (``_ReachabilityWalker._dedup_key_missing``)
    applies both.
    """
    reader, open_findings = await _open_bucket_or_finding(pool, bucket_cache, key)
    if reader is None:
        assert open_findings is not None
        return open_findings, False

    findings: list[Finding] = []
    key_missing = False
    try:
        findings.extend(await check_bucket_structure(store, reader))

        if not reader.header.is_compressed:
            return findings, False

        key_missing = reader.header.is_vault_encrypted and vault_key is None
        if key_missing:
            findings.append(
                Finding(
                    Stage.ENCRYPT_KEY,
                    Symptom.KEY_MISSING,
                    reader.path,
                    "bucket is vault-encrypted but this repository was opened without a key",
                )
            )

        # QUICK never reaches this: `indices` stays empty, so no chunk is
        # ever read -- only FULL enumerates the bucket's own live chunks.
        indices = reader.non_compacted_chunk_indices() if level is VerifyLevel.FULL else []
        if indices:
            if key_missing:
                # Ciphertext CRC needs no key and stays exhaustive even
                # here (see _check_chunks_ciphertext_only's own docstring)
                # -- only decode+fingerprint is what actually needs one,
                # so that's the only half skipped. Routing this through
                # _check_chunks_full instead would attempt to decrypt with
                # no key and raise KeyRequiredError.
                findings.extend(await _check_chunks_ciphertext_only(reader, indices))
            else:
                findings.extend(await _check_chunks_full(pool, bucket_cache, key, reader, indices))
    except (NotFoundError, DataCorruptError, FormatError) as exc:
        findings.append(Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, str(exc)))
    except Exception as exc:
        findings.append(Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, f"unexpected error: {exc}"))
    return findings, key_missing


# -- Multiprocess worker functions ---------------------------------------
#
# Module-level (never a method or closure — required for `spawn`
# picklability) and process-global rather than passed as arguments: each
# runs inside its own freshly-spawned worker process, one `_verify_worker_init`
# call per worker's whole lifetime, so the `Pool` it builds (and that
# `Pool`'s own `AllocationTableCache`, when fingerprinting) persists across
# every bucket this worker ever checks — not rebuilt per task.

_worker_pool: Pool | None = None
_worker_bucket_cache: BucketReaderCache | None = None
_worker_store: ObjectStore | None = None
_worker_vault_key: bytes | None = None


def _verify_worker_init(descriptor: PoolDescriptor) -> None:
    """``ProcessPoolExecutor(initializer=...)`` target — see this
    module's own "Multiprocess worker functions" section comment above for
    why this runs once per worker process, not once per task."""
    global _worker_pool, _worker_bucket_cache, _worker_store, _worker_vault_key
    _worker_store, _worker_pool = build_worker_pool(descriptor)
    _worker_bucket_cache = BucketReaderCache()
    _worker_vault_key = descriptor.vault_key
    # Registered last, only once every worker-global above is actually set,
    # so a shutdown hook never runs against half-initialized state.
    atexit.register(_verify_worker_shutdown)


def _verify_worker_shutdown() -> None:
    """Runs once, at this worker process's normal exit (registered by
    ``_verify_worker_init``) — releases ``_worker_store`` (see
    ``aclose_worker_store()``'s own docstring) before closing this
    worker's persistent event loop."""
    run_in_worker_loop(aclose_worker_store(_worker_store))
    close_worker_loop()


def build_verify_executor(descriptor: PoolDescriptor) -> ProcessPoolExecutor:
    """The one public factory for an executor this module's own worker
    functions (above) know how to serve — for a caller (``Repository.verify()``'s
    own multi-catalog fan-out) that wants to share **one** executor across
    several ``verify_reachable()`` calls instead of letting each build (and
    tear down) its own. Kept here, not re-derived by that caller, so
    ``_verify_worker_init`` itself stays module-private: every executor
    this module's workers can serve is built through this one function."""
    return new_process_pool(initializer=_verify_worker_init, initargs=(descriptor,))


def _verify_bucket_worker(key: tuple[StreamId, BucketId]) -> tuple[list[Finding], bool]:
    """The multiprocess path's per-bucket work item — a thin wrapper
    around ``_check_one_bucket_core``, the same function the in-process
    path calls (see that function's own docstring for why this matters).
    Always called at FULL level: ``check_all_buckets`` never dispatches
    here for QUICK."""
    assert _worker_pool is not None
    assert _worker_bucket_cache is not None
    assert _worker_store is not None
    # see concurrency.run_in_worker_loop's own docstring for why this isn't asyncio.run().
    return run_in_worker_loop(
        _check_one_bucket_core(
            _worker_pool, _worker_bucket_cache, _worker_store, _worker_vault_key, key, VerifyLevel.FULL
        )
    )


async def verify_reachable(
    repo: DedupRepo,
    level: VerifyLevel = VerifyLevel.QUICK,
    *,
    progress: _ProgressCallback | None = None,
    executor: ProcessPoolExecutor | None = None,
) -> list[Finding]:
    """Run the top-down, reachability-scoped check and return every
    ``Finding`` found — see this module's own docstring for exactly what
    each level covers.

    ``progress`` is reported in two stages. While discovering (walking
    every version's composition extents and claiming the buckets they
    touch), ``phase="discovering"``, ``determinate=True``,
    ``unit="items"`` — one tick per ``(workload, version)`` pair, a stable
    denominator known up front. While checking (once every version has
    been discovered), ``phase="verifying"``, ``determinate=True`` — one
    tick per bucket actually checked, ``done``/``total`` both real,
    cumulative counts already fixed by the time checking starts. The unit
    (and so what ``done``/``total`` actually count) depends on
    ``level``: ``"bytes"`` at FULL (``_total_bytes_to_verify``, from
    each claimed bucket's own real on-disk size), ``"buckets"`` at QUICK
    (a plain count) — see ``finalize_pending_buckets``'s own docstring for
    why bytes would overstate QUICK's own real throughput rather than
    just being unavailable. ``Progress.detail`` names whichever ``(workload,
    version)`` pair the bucket just finished was claimed while
    discovering.

    A ``connections``/``workloads``/``versions`` call failing (a corrupt or
    unreadable ``connection_config``/``workload_config``/
    ``copy_target_version`` row or table) is caught at whichever level it
    happened and folded into a ``Finding`` via ``_unresolvable_finding``,
    same as every other resolution failure this module reports — not left
    to propagate out of this function entirely, per ``Stage.VERSION``'s
    own docstring ("covers failure ... at the workload/connection-
    enumeration level"). A connection/workload whose own listing fails is
    simply skipped for the rest of this walk — its siblings still get
    checked.

    ``executor`` (default ``None``: this call builds its own, iff FULL's
    own bucket sweep actually needs one, and closes it before returning —
    given: the caller already built one and owns its lifetime) is FULL
    level's real multi-core parallelism for its per-bucket decode sweep —
    see ``_ReachabilityWalker.check_all_buckets``'s own docstring. Pass a
    shared one when calling this more than once for one logical operation
    (``Repository.verify()``'s own multi-catalog fan-out does exactly
    this) rather than let each call spin up its own pool independently.
    QUICK never uses it regardless of what's passed.
    """
    findings: list[Finding] = []
    findings += await check_repo_info(repo)

    pairs: list[tuple[Workload, Version]] = []
    try:
        conns = await connections(repo)
    except ApmRepoError as exc:
        findings.append(_unresolvable_finding(repo.layout.repo_root, exc))
        conns = []
    for connection in conns:
        try:
            wls = await workloads(repo, connection)
        except ApmRepoError as exc:
            findings.append(_unresolvable_finding(connection.display_name, exc))
            continue
        for workload in wls:
            try:
                vers = await versions(repo, workload, include_deleted=False)
            except ApmRepoError as exc:
                findings.append(_unresolvable_finding(workload.display_name, exc))
                continue
            pairs.extend([(workload, version) for version in vers])

    walker = _ReachabilityWalker(repo, level, progress=progress, executor=executor)
    try:
        for index, (workload, version) in enumerate(pairs):
            if progress is not None:
                await progress(
                    Progress(
                        phase="discovering",
                        determinate=True,
                        done=index,
                        total=len(pairs),
                        unit="items",
                        detail=f"{workload.display_name}/{version.display_name}",
                    )
                )
            findings += await walker.discover_version(workload, version)
            if walker.pending_bucket_count >= _BUCKET_BATCH_SIZE:
                await walker.finalize_pending_buckets()
        await walker.finalize_pending_buckets()
        findings += await walker.check_all_buckets()
    except BaseException:
        # A genuine failure from the walk itself takes priority --
        # walker.close()'s own failure (SaasStreamCache.close() failing to
        # close every tracked stream) must never replace it, the same
        # cleanup-priority shape export_scheduler.py's own sink-close uses.
        with contextlib.suppress(BaseException):
            await walker.close()
        raise
    else:
        await walker.close()
    return findings
