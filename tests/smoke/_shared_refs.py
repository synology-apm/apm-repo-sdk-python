"""Cross-distribution real-sample discovery and ref-selection helpers,
shared by ``sdk/``, ``cli/``, and ``browser/``'s own ``__main__.py``
bootstraps.

``discover_repos()`` is the one "turn ``smoke_samples.toml`` into a list of
open, real ``Repository`` instances" step every distribution's bootstrap
starts from -- dispatching on which of ``_samples.py``'s three
``SampleEntry`` kinds (``LocalSample``/``ProfileSample``/
``RemoteStorageSample``) each configured entry is.
``list_representative_refs()`` builds on it for ``cli/`` and ``browser/``,
which both only need one real, representative leaf per ``(sample,
workload type)`` pair to exercise their own commands/screens against --
neither cares about the deeper per-workload version lists
``sdk/__main__.py``'s own per-repo enumeration keeps around for its
correctness checks (``phases/_catalog.py`` and friends), so this is the
one call both of them make instead of reimplementing that walk.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from synology_apm_repo.sdk import (
    Catalog,
    ChunkCompactedError,
    DataCorruptError,
    KeyStatus,
    Node,
    NodeRef,
    NotFoundError,
    Repository,
    Session,
    TraceEvent,
    UnitKind,
    UnitProvider,
    UnsupportedDataFormatError,
    Version,
    Workload,
    build_store,
    store_from_config,
)
from synology_apm_repo.sdk.identifiers import CatalogId
from synology_apm_repo.sdk.units.base import ClosableUnitProvider

from ._samples import LocalSample, ProfileSample, RemoteStorageSample, SampleEntry

#: A leaf/version this bootstrap tries that turns out to be a known,
#: sample-specific data gap (metadata-only fragment, unsupported format,
#: ...) rather than a real bug -- same set ``sdk/``'s own per-domain
#: phases treat as DEGRADED via ``ctx.call(degrade_on=...)``. Bootstrap
#: has no report to record a DEGRADED step into, so it just moves on to
#: the next candidate version/workload instead.
_DATA_GAP = (NotFoundError, DataCorruptError, ChunkCompactedError, UnsupportedDataFormatError)

#: Bounds for the leaf-picking breadth-first search -- same values as
#: ``sdk/phases/_shared.py``'s own (this module doesn't need a second,
#: independently-tuned set of constants for the same kind of walk).
_MAX_CHILDREN_PER_LEVEL = 5
_MAX_DEPTH = 6
_MAX_VISITS = 200

# No mitigation needed here for aiohttp's "Unclosed client session"/
# "Unclosed connector" warnings: TracingStore/RecordingStore (storage/
# recording.py) forward aclose() to whatever they wrap, so Session.close()
# always reaches a real S3Store/AzureStore's own aclose() -- regardless of
# whether trace= is passed (every smoke tool here always does) -- and
# closes its aiohttp connector correctly.


@dataclass
class RepoInfo:
    """One repository this run's bootstrap step discovered, labeled for
    report traceability.

    ``sample_name`` is index-suffixed (``"sample-1[0]"``) because
    ``Session.discover``/``discover_remote`` are ``AsyncIterator``s that
    can yield more than one ``Repository`` for a single configured entry
    -- a bucket/vault holding "two repositories sharing one bucket" (see
    ``tests/CLAUDE.md``'s sample coverage table).
    """

    sample_name: str
    repo: Repository
    repo_root: str
    """``repo.layout.repo_root`` -- ``ObjectStore``-relative (relative to
    the store's own root: ``entry.path``'s ``LocalFsStore`` for a
    ``LocalSample``, the whole bucket/container for a
    ``ProfileSample``/``RemoteStorageSample``), *not* a real filesystem
    path a fresh process could open directly. See ``narrow_repo_ref``."""
    key_status: KeyStatus
    readable: bool
    """``not repo.is_encrypted or key_status is KeyStatus.VERIFIED`` --
    gates every content-level check (a repository that isn't readable is
    skipped rather than attempting a read that would only fail on a key
    error already known in advance)."""
    key: str
    """The key string bootstrap opened this repository with (``""`` if none) --
    kept so a caller that deliberately calls ``repo.set_key()`` with a
    different key (``sdk/phases/_catalog.py``'s own negative-key check)
    can restore this repository's verified state afterward, since it's the same
    shared ``Repository`` instance every later reader also reads from."""
    entry: SampleEntry
    """The ``_samples.py`` entry this repository was discovered from -- carries
    which of the three kinds it is, plus that kind's own config (the
    profile name, the direct S3/Azure/SMB credentials, ...). See
    ``narrow_repo_ref``/``broad_repo_ref``."""
    sole_repo_for_entry: bool
    """``False`` for ``sample-1``'s own "two repositories sharing one bucket"
    case -- when a single sample entry's own config is applied to a
    *narrower*, single-repository rescan of just one sibling (what ``cli/``'s
    subprocess does), the object-store layout's own key-verification
    probe reports "no repository found" instead of gracefully ignoring an
    unneeded key, even for the sibling that doesn't actually need it.
    ``cli/`` only ever passes ``--key`` when this is ``True``,
    sidestepping the whole ambiguity rather than guessing which sibling
    the configured key really belongs to.

    This is a real, structural limitation for a *local* sample
    specifically: ``Session.open()`` builds a ``LocalFsStore`` rooted
    exactly at whatever path it's given, so a narrowed local path
    structurally cannot see a sibling ``@ActiveProtectKey`` two levels
    up, even with ``storage/layout.py``'s narrowed-root key-tree fix
    (which only helps a *remote* narrowed rescan sharing the same,
    already-broad ``store`` instance)."""

    @property
    def broad_repo_ref(self) -> str:
        """The broad, un-narrowed root this repository was discovered under --
        ``entry.path`` for every kind alike: a real filesystem directory
        for a ``LocalSample``, a store-relative prefix (possibly ``""``,
        meaning the whole bucket/container) for a ``ProfileSample``/
        ``RemoteStorageSample``. See ``narrow_repo_ref``; this is the ref
        used whenever ``--key`` is passed, since ``object_store``'s layout
        detector doesn't recognize a ``--key``-narrowed single-repository
        rescan the way the vault layout does."""
        return self.entry.path

    @property
    def narrow_repo_ref(self) -> str:
        """The narrowest addressable location for just this one repository, as
        a fresh CLI subprocess's ``REPO`` argument -- a real, absolute
        filesystem path (``entry.path`` joined with ``repo_root``) for a
        ``LocalSample``, or ``repo_root`` alone for a ``ProfileSample``/
        ``RemoteStorageSample`` (already store-root-relative regardless
        of any narrower ``root=`` the discovery scan itself used --
        ``root=`` only narrows which store-relative sub-path
        ``discover_remote`` scans, since an S3/Azure store is scoped to a
        whole bucket/container with no "sub-root" constructor argument of
        its own; it doesn't change what a found repository's own
        ``repo_root`` is reported relative to)."""
        if isinstance(self.entry, LocalSample):
            return str(Path(self.entry.path) / self.repo_root) if self.repo_root else self.entry.path
        return self.repo_root


async def discover_repos(
    session: Session,
    entries: list[SampleEntry],
    *,
    trace: Callable[[str, TraceEvent], None] | None = None,
) -> list[RepoInfo]:
    """Discover every configured sample's repositories -- the piece every
    distribution's bootstrap starts from. ``trace``, when given, is called
    once per underlying ``ObjectStore`` call with the sample name it came
    from (mirrors ``Session.discover``'s own ``trace=`` callback, see
    ``api/session.py``)."""
    repo_infos: list[RepoInfo] = []
    for entry in entries:
        entry_repos: list[RepoInfo] = []
        index = 0

        def _trace(event: TraceEvent, name: str = entry.name) -> None:
            if trace is not None:
                trace(name, event)

        match entry:
            case LocalSample(path=path, key=key):
                repos = session.discover(Path(path), key=key or None, trace=_trace)
            case ProfileSample(profile=profile_name, path=root, key=key):
                store = await build_store(profile_name)
                repos = session.discover_remote(store, key=key or None, root=root, trace=_trace)
            case RemoteStorageSample(kind=kind, config=config, secrets=secrets, path=root, key=key):
                store = await store_from_config(kind, config, secrets)
                repos = session.discover_remote(store, key=key or None, root=root, trace=_trace)

        async for repo in repos:
            key_status = repo.key_status
            readable = not repo.is_encrypted or key_status is KeyStatus.VERIFIED
            entry_repos.append(
                RepoInfo(
                    f"{entry.name}[{index}]",
                    repo,
                    repo.layout.repo_root,
                    key_status,
                    readable,
                    entry.key,
                    entry,
                    sole_repo_for_entry=True,  # corrected below once the full count is known
                )
            )
            index += 1
        if len(entry_repos) != 1:
            for ri in entry_repos:
                ri.sole_repo_for_entry = False
        repo_infos.extend(entry_repos)
    return repo_infos


async def close_if_closable(provider: UnitProvider | None) -> None:
    """Closes ``provider`` if it implements ``ClosableUnitProvider``, a
    no-op otherwise (or if ``provider`` is ``None``, e.g. a candidate that
    raised before ``catalog.provider()`` ever returned)."""
    if isinstance(provider, ClosableUnitProvider):
        await provider.close()


def prefer_adversarial_name(node: Node) -> bool:
    """A ``find_leaf``/``pick_workload_with_retry`` ``prefer=`` predicate:
    ``True`` when ``node.name`` contains a non-printable-ASCII character
    or an HTML/markup-special one (``<``, ``>``, ``[``, ``]``, ``&``).

    Real sample data's own adversarial names (emoji, HTML-escaped text,
    path-traversal-looking segments) are exactly the class of name most
    likely to break Rich-markup escaping in the CLI's ``ls``/``tree``/
    ``doctor``/``verify`` commands -- this seeks that class of name out on
    purpose rather than leaving it to chance."""
    return any(not (" " <= ch <= "~") or ch in "<>[]&" for ch in node.name)


async def pick_workload_with_retry(
    entries: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]],
    kinds: frozenset[UnitKind],
    *,
    prefer: Callable[[Node], bool] | None = None,
) -> tuple[tuple[RepoInfo, Workload, Version, UnitProvider, Node] | None, bool]:
    """Try every ``(workload, version)`` pair in ``entries``, not just the
    first with a non-empty version list, since a single version's known
    data gap must not doom the whole group when a sibling version might
    still work.

    Each entry carries the ``CatalogId`` its ``workload``/``versions``
    came from, resolved back to a live ``Catalog`` fresh every time via
    ``resolve_catalog`` (see that function's own docstring for why).

    ``Catalog.versions()`` never pre-filters by resolvability, so this
    doesn't re-filter by ``version.meta is not None`` either -- that would
    silently exclude every GW/M365 version, which carries no
    ``VersionMeta`` at all.

    Returns ``(result, any_truncated)``: ``result`` carries the winning
    candidate's already-fetched ``provider``/``leaf`` so the caller's own
    ``ctx.call`` records that work instead of repeating it; ``any_truncated``
    is ``True`` if ``find_leaf``'s search bound cut short at least one
    tried candidate, even when a match was still found.

    Closes every rejected candidate's own provider immediately, per
    ``tests/CLAUDE.md``'s "Closing a provider built directly against a
    repository" -- a search that tries many candidates can't wait for
    ``Repository.close()`` to eventually reach each one."""
    any_truncated = False
    for ri, catalog_id, workload, versions in entries:
        for version in versions:
            provider: UnitProvider | None = None
            try:
                catalog = await resolve_catalog(ri.repo, catalog_id)
                provider = await catalog.provider(version)
                leaf, truncated = await find_leaf(provider, provider.root(), kinds, prefer=prefer)
            except _DATA_GAP:
                await close_if_closable(provider)
                continue
            any_truncated = any_truncated or truncated
            if leaf is not None:
                return (ri, workload, version, provider, leaf), any_truncated
            await close_if_closable(provider)
    return None, any_truncated


async def find_leaf(
    provider: UnitProvider,
    root: Node,
    kinds: frozenset[UnitKind],
    *,
    prefer: Callable[[Node], bool] | None = None,
) -> tuple[Node | None, bool]:
    """Bounded breadth-first search for a leaf ``Node`` whose ``kind`` is
    in ``kinds``, starting from ``root``.

    Without ``prefer``, returns the first matching leaf found. With
    ``prefer``, keeps searching -- within the same bound -- for a match
    satisfying it too, falling back to the first match found if none
    does.

    Returns ``(leaf, truncated)``. ``truncated`` is ``True`` only when the
    search bound (depth/children-per-level/total-visits cap) actually cut
    the walk short -- some node was never explored because of it -- and no
    match was found anyway; that's a real dispatch/heuristic gap worth a
    human's eyes (the bound may simply need raising, or something upstream
    may be wrong). A tree that was walked *to completion* within the bound
    and genuinely has no matching leaf (an empty mailbox's folders with
    zero messages, say) returns ``(None, False)`` -- as unremarkable as an
    empty tree at the root.
    """
    if root.is_leaf:
        return (root if root.kind in kinds else None), False
    queue: list[tuple[Node, int]] = [(root, 0)]
    visited = 0
    truncated = False
    first_match: Node | None = None
    while queue:
        if visited >= _MAX_VISITS:
            truncated = True  # nodes remain in queue, unexplored
            break
        node, depth = queue.pop(0)
        if depth >= _MAX_DEPTH:
            truncated = True  # this node's own children were never fetched
            continue
        # Peek one past the cap so a level with more children than
        # _MAX_CHILDREN_PER_LEVEL is itself recognized as truncated,
        # rather than silently mistaken for "every child was seen."
        children = await provider.children(node, limit=_MAX_CHILDREN_PER_LEVEL + 1)
        if len(children) > _MAX_CHILDREN_PER_LEVEL:
            truncated = True
            children = children[:_MAX_CHILDREN_PER_LEVEL]
        for child in children:
            visited += 1
            if child.is_leaf and child.kind in kinds:
                if prefer is None or prefer(child):
                    return child, False
                if first_match is None:
                    first_match = child
            elif not child.is_leaf:
                queue.append((child, depth + 1))
            if visited >= _MAX_VISITS:
                truncated = True
                break
    if first_match is not None:
        return first_match, False
    return None, truncated


@dataclass(frozen=True)
class RepresentativeRef:
    """One real, representative ``(sample, workload type)`` pair's picked
    leaf -- a plain data snapshot: the repository it came from is already
    closed by the time any caller sees this, so ``cli/``/``browser/``
    always reconnect fresh (via ``repo_path``/``ref`` below) rather than
    reuse a live ``Repository``."""

    sample_name: str
    type_key: str
    """``workload.type_hint`` -- the other half of this ref's grouping
    key (``sample_name`` alone collides across a sample's several
    workload types); use ``f"{sample_name}.{type_key}"`` for a unique
    step-name prefix."""
    repo_path: str
    """A ``REPO``-argument value a fresh process can re-open this
    repository from -- ``RepoInfo.broad_repo_ref``/``.narrow_repo_ref``,
    *not* ``repo.layout.repo_root`` (store-relative, meaningless outside
    the in-process ``Session`` that discovered it). For a ``local`` ref,
    a real filesystem path; for a profile/``remote_storage`` ref, a
    store-relative sub-path -- pair with ``profile`` (see below)."""
    workload: Workload
    version: Version
    node: Node
    ref: str
    """``str(NodeRef(repo_path, node.ref.segments))`` -- ``node.ref``
    itself carries the same store-relative ``repo_root`` as
    ``RepoInfo.repo_root`` (wrong for a fresh subprocess), so this
    rebuilds it with ``repo_path`` instead of reusing ``node.ref`` as-is."""
    key: str
    """The key string bootstrap opened this repository with (``""`` if
    none) -- ``cli/`` passes this as ``--key`` when non-empty, since the
    subprocess has no access to the in-process ``Repository``'s
    already-verified state."""
    local: bool
    """``True`` only for a ``LocalSample``-derived ref. ``browser/``'s own
    bootstrap picks its ``main_ref``/``encrypted_ref`` from local refs
    only, since every domain but the dedicated remote-connect one drives
    ``ConnectDialog`` via ``connect_local()``, which takes a filesystem
    path."""
    profile: str
    """The saved profile name, for a ``ProfileSample``-derived ref --
    ``""`` otherwise. ``cli/`` prepends ``--profile <name>`` when this is
    set; a ``RemoteStorageSample``-derived ref (``local`` and ``profile``
    both falsy) has no CLI-reopenable form at all -- see
    ``list_representative_refs``'s ``exclude_unreopenable_by_cli``."""


async def list_representative_refs(
    session: Session,
    entries: list[SampleEntry],
    *,
    leaf_kinds: frozenset[UnitKind] | None = None,
    exclude_unreopenable_by_cli: bool = False,
) -> tuple[list[RepresentativeRef], list[str]]:
    """One representative, real leaf per ``(sample, workload type)`` pair
    -- ``cli/__main__.py`` and ``browser/__main__.py``'s entire bootstrap
    step. Flattened across every workload type in one pass (unlike
    ``sdk/``'s per-domain phases), since neither needs per-type-specific
    leaf kinds, just *some* meaningful leaf to exercise a command/screen
    against. A repository that isn't readable (unkeyed/wrong-keyed) is
    skipped, same as ``sdk/``'s own bootstrap. Processes and closes one
    sample entry's repositories at a time, same shape as
    ``sdk/__main__.py``'s ``_process_entry``, keeping only one picked leaf
    per group rather than every version list.

    ``exclude_unreopenable_by_cli``, when set, also skips any
    ``RemoteStorageSample``-derived repository: the real CLI has no
    raw-credential flag, only ``--profile <name>`` for a saved profile.
    ``cli/__main__.py`` sets this; ``sdk/``'s own bootstrap (which
    doesn't call this function) and ``browser/__main__.py`` (which drives
    ``ConnectDialog`` directly, profile or direct alike) don't.

    Returns ``(refs, skip_reasons)`` -- one skip-reason line per
    ``(sample, workload type)`` this bootstrap couldn't pick a ref for, so
    a caller with its own ``SmokeContext`` (which doesn't exist yet at
    this point) can record each as a real ``ctx.skip(...)``.
    """
    kinds = leaf_kinds if leaf_kinds is not None else frozenset(UnitKind)
    refs: list[RepresentativeRef] = []
    skip_reasons: list[str] = []

    for entry in entries:
        for ri in await discover_repos(session, [entry]):
            if not ri.readable:
                await session.close_repo(ri.repo)
                continue
            if exclude_unreopenable_by_cli and isinstance(ri.entry, RemoteStorageSample):
                reason = f"{ri.sample_name}: remote_storage sample, no --profile flag to reopen via cli subprocess"
                print(f"[smoke] {reason} -- skipped")
                skip_reasons.append(reason)
                await session.close_repo(ri.repo)
                continue
            if ri.repo.is_encrypted and not ri.sole_repo_for_entry:
                # A shared-bucket, encrypted sibling: real, verified in this
                # in-process session (its key already resolved via
                # Session.discover/discover_remote), but structurally
                # unaddressable by cli/browser's own fresh, per-repository re-open
                # -- the sample's one configured key can't be safely applied
                # at that narrower scope (see RepoInfo.sole_repo_for_entry),
                # so a canonical ref built here would only fail to resolve
                # there. sdk/ smoke still exercises this repository fully
                # in-process; this is a cli/browser-specific gap, not a data
                # gap.
                reason = f"{ri.sample_name}: shared-bucket encrypted sibling, not addressable by cli/browser"
                print(f"[smoke] {reason} -- skipped")
                skip_reasons.append(reason)
                await session.close_repo(ri.repo)
                continue

            by_type: dict[str, list[tuple[RepoInfo, CatalogId, Workload, list[Version]]]] = {}
            for catalog in await ri.repo.catalogs():
                for workload in await catalog.workloads():
                    type_key = workload.type_hint
                    versions = await catalog.versions(workload)
                    by_type.setdefault(type_key, []).append((ri, catalog.catalog_id, workload, versions))

            for type_key, entries_for_type in by_type.items():
                ref = await _first_working_ref(ri.sample_name, type_key, entries_for_type, kinds)
                if ref is None:
                    reason = f"{ri.sample_name}/{type_key}: no readable leaf found across any version"
                    print(f"[smoke] {reason} -- skipped")
                    skip_reasons.append(reason)
                    continue
                refs.append(ref)
            await session.close_repo(ri.repo)

    return refs, skip_reasons


async def resolve_catalog(repo: Repository, catalog_id: CatalogId) -> Catalog:
    """``repo.catalog_by_id(catalog_id)``, raising instead of returning
    ``None`` -- every caller here just enumerated ``catalog_id`` from a
    live listing moments ago, so a miss means the repository's own catalog set
    changed out from under this run, worth a loud failure rather than a
    silent skip.

    Grouping helpers in this module key on ``CatalogId`` rather than a
    cached ``Catalog`` object, since ``Repository.set_key()`` closes and
    replaces every already-opened ``DedupRepo`` -- a ``Catalog`` cached
    from earlier in the run would surface a raw ``aiosqlite`` "no active
    connection" error on its next use instead of raising cleanly."""
    catalog = await repo.catalog_by_id(catalog_id)
    if catalog is None:
        raise LookupError(f"catalog {catalog_id!r} no longer present on {repo!r}")
    return catalog


async def _first_working_ref(
    sample_name: str,
    type_key: str,
    entries_for_type: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]],
    kinds: frozenset[UnitKind],
) -> RepresentativeRef | None:
    """Tries every ``(workload, version)`` pair in turn, since a single
    version's known data gap shouldn't doom the whole group when a sibling
    version might still work -- the same retry shape
    ``pick_workload_with_retry`` gives ``sdk/``'s own device/fs/saas
    domains.

    Each entry carries the ``CatalogId`` its ``workload``/``versions``
    came from, resolved back to a live ``Catalog`` via ``resolve_catalog``
    rather than re-derived from ``ri.repo.catalogs()[0]`` -- a repository
    can hold more than one ``Catalog`` with the same ``type_hint``, and
    ``Catalog.provider()`` doesn't validate that a ``Version`` belongs to
    it.

    Closes every rejected candidate's own provider immediately, same as
    ``pick_workload_with_retry``. Unlike that function, the *winning*
    candidate's provider is closed here too: ``RepresentativeRef`` carries
    only plain data, and nothing downstream reuses the provider object."""
    for ri, catalog_id, workload, versions in entries_for_type:
        for version in versions:
            provider: UnitProvider | None = None
            try:
                catalog = await resolve_catalog(ri.repo, catalog_id)
                provider = await catalog.provider(version)
                leaf, _truncated = await find_leaf(provider, provider.root(), kinds)
                if leaf is None:
                    await close_if_closable(provider)
                    continue
                # find_leaf only walks provider.children() -- a tree
                # structure that can browse cleanly even when the leaf's
                # actual chunk bytes hit the same kind of data gap
                # (composition sub-file missing, ...). A real, tiny probe
                # read here is what sdk/phases/_shared.py's own
                # bounded_read_and_export checks for each domain's own
                # picked leaf too -- cheap, and it's exactly what cli/'s
                # own `cat`/`ls` would otherwise discover the hard way,
                # as an unexplained subprocess failure instead of a clean
                # bootstrap skip.
                unit = await provider.unit(leaf)
                content = unit.open()
                await content.read(0, min(64, content.size or 64))
            except _DATA_GAP:
                await close_if_closable(provider)
                continue
            await close_if_closable(provider)
            # Only the unambiguous, single-repository-per-entry case gets
            # --key passed at all. When it is passed, the *broad* ref (not
            # the narrowed one) is the one path accepted by --key for both
            # vault and object_store layouts alike -- object_store's own
            # layout detector doesn't recognize a --key-narrowed
            # single-repository rescan the vault layout's does, but
            # broad_repo_ref is only ever used here when sole_repo_for_
            # entry already rules out the ambiguity a broader ref would
            # otherwise risk.
            key = ri.key if ri.sole_repo_for_entry else ""
            real_path = ri.broad_repo_ref if key else ri.narrow_repo_ref
            ref_str = str(NodeRef(real_path, leaf.ref.segments))
            profile_name = ri.entry.profile if isinstance(ri.entry, ProfileSample) else ""
            return RepresentativeRef(
                sample_name,
                type_key,
                real_path,
                workload,
                version,
                leaf,
                ref_str,
                key,
                local=isinstance(ri.entry, LocalSample),
                profile=profile_name,
            )
    return None
