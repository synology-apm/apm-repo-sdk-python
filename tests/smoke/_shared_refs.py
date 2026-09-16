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
# closes its aiohttp connector correctly. See storage/recording.py's own
# _InstrumentedStore.aclose() docstring for the contract this relies on,
# and git log for the investigation that found and fixed the gap.


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
    unneeded key, even for the sibling that doesn't actually need it
    (empirically verified against ``sample-1``/``s3-sample-2-encrypted``'s
    real bytes -- both siblings fail identically with the shared key,
    both succeed identically without it). ``cli/`` only ever passes
    ``--key`` when this is ``True``, sidestepping the whole ambiguity
    rather than guessing which sibling the configured key really belongs
    to.

    This is a real, structural limitation for a *local* sample
    specifically (``Session.open()`` builds a ``LocalFsStore`` rooted
    exactly at whatever path it's given, so a narrowed local path
    structurally cannot see a sibling ``@ActiveProtectKey`` two levels
    up -- confirmed empirically, `--key` still fails there even with
    ``storage/layout.py``'s narrowed-root key-tree fix, which only helps
    a *remote* narrowed rescan sharing the same, already-broad ``store``
    instance). See the project's plan file for the proper fix (a
    Repository/Catalog hierarchy rename) this sidesteps for now."""

    @property
    def broad_repo_ref(self) -> str:
        """The broad, un-narrowed root this repository was discovered under --
        ``entry.path`` for every kind alike: a real filesystem directory
        for a ``LocalSample``, a store-relative prefix (possibly ``""``,
        meaning the whole bucket/container) for a ``ProfileSample``/
        ``RemoteStorageSample``. See ``narrow_repo_ref`` and
        ``_first_working_ref``'s own comment on which of the two a
        ``--key`` reopen needs."""
        return self.entry.path

    @property
    def narrow_repo_ref(self) -> str:
        """The narrowest addressable location for just this one repository, as
        a fresh CLI subprocess's ``REPO`` argument -- a real, absolute
        filesystem path (``entry.path`` joined with ``repo_root``) for a
        ``LocalSample``, or ``repo_root`` alone for a ``ProfileSample``/
        ``RemoteStorageSample`` (already store-root-relative regardless
        of any narrower ``root=`` the discovery scan itself used -- see
        ``Session.discover_remote``'s own docstring)."""
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
    path-traversal-looking segments) are exactly the class of name that
    exposed a real Rich-markup-escaping bug in the CLI's ``ls``/``tree``/
    ``doctor``/``verify`` commands by accident, via manual review -- this
    seeks that class of name out on purpose instead."""
    return any(not (" " <= ch <= "~") or ch in "<>[]&" for ch in node.name)


async def pick_workload_with_retry(
    entries: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]],
    kinds: frozenset[UnitKind],
    *,
    prefer: Callable[[Node], bool] | None = None,
) -> tuple[tuple[RepoInfo, Workload, Version, UnitProvider, Node] | None, bool]:
    """Try every ``(workload, version)`` pair in ``entries`` -- not just
    the first with a non-empty version list -- giving up only once every
    combination's own ``provider()``/``find_leaf()`` has failed or found
    nothing. Mirrors ``_first_working_ref``'s own retry, for the identical
    "a sibling version might still work" reason its docstring gives: a
    single version's known data gap (a metadata-only fragment, an
    unsupported format, ...) must not doom the whole group when another
    version might still work, the way the old, first-version-only
    ``pick_workload`` this replaces did.

    Each entry carries the ``CatalogId`` of the specific ``Catalog`` its
    ``workload``/``versions`` came from -- see ``resolve_catalog``'s own
    docstring for why this must be resolved back to a live ``Catalog``
    fresh, every time, rather than re-deriving "some catalog" from
    ``ri.repo.catalogs()[0]`` (wrong the moment a repository holds more than
    one ``Catalog``) or caching the ``Catalog`` object itself (stale the
    moment any ``set_key()`` call touches the same repository later in the
    same run).

    ``Repository.versions()`` (see its own docstring) already filters
    each workload's version list down to what's confirmed openable for
    its own ``workload_type`` (VM/FS's own ``meta`` fields, PC/PS's disk-
    fragment resolution, GW/M365's ``saas_obj`` resolution) -- so this
    doesn't re-filter by ``version.meta is not None`` on top of that,
    which would silently exclude *every* GW/M365 version (SaaS versions
    carry no ``VersionMeta`` at all) and second-guess PC/PS's own,
    different resolution check.

    Returns ``(result, any_truncated)``. ``result`` carries the winning
    candidate's already-fetched ``provider``/``leaf`` alongside it, so the
    caller's own ``ctx.call`` records that real work instead of repeating
    it -- a candidate's own data-gap failure is swallowed here exactly
    like ``_first_working_ref``'s, invisible to the per-domain report the
    same way a rejected sibling already is there. ``any_truncated`` is
    ``True`` if ``find_leaf``'s own search bound cut short at least one
    tried candidate (see its own docstring on what that means) --
    reported even when ``result`` isn't ``None``, and worth surfacing to
    the caller's report either way, distinct from every candidate's tree
    genuinely, fully walked and found empty.

    Closes every rejected candidate's own provider immediately (device/PC-
    PS's own ``target.db`` SqliteSource, most notably), same "whoever
    builds one outside ``Repository.provider()``'s own tracking owns
    closing it" contract as ``tests/CLAUDE.md``'s "Closing a provider
    built directly against a repository" -- this walk can try many
    candidates before finding a winner (or none at all), and leaving a
    rejected one open until ``Repository.close()`` eventually gets to it
    isn't good enough for a search that can touch many candidates in one
    repository's own turn."""
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
    leaf -- carries both the live objects (for ``browser/``'s
    screen-driven navigation) and the built canonical ref string (for
    ``cli/``'s subprocess argv); one bootstrap, two consumption shapes."""

    sample_name: str
    type_key: str
    """``workload.type_hint`` -- the other half of this ref's grouping
    key (``sample_name`` alone collides across a sample's several
    workload types); use ``f"{sample_name}.{type_key}"`` for a unique
    step-name prefix."""
    repo: Repository
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
    """The key string bootstrap opened ``repo`` with (``""`` if none) --
    ``cli/`` passes this as ``--key`` when non-empty, since the subprocess
    has no access to the in-process ``Repository``'s already-verified
    state."""
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
    step. Flattened across every workload type in one pass rather than
    split by domain the way ``sdk/``'s phases are: neither ``cli/`` nor
    ``browser/`` smoke needs per-type-specific leaf kinds (a disk image
    vs. a plain file) the way ``sdk/``'s device/fs domains do, just *some*
    meaningful leaf to exercise a command/screen against. A repository that
    isn't readable (unkeyed/wrong-keyed) is skipped, same as ``sdk/``'s
    own bootstrap.

    ``exclude_unreopenable_by_cli``, when set, also skips any
    ``RemoteStorageSample``-derived repository: the real CLI has no
    raw-credential flag, only ``--profile <name>`` for a saved profile, so
    a direct-credential sample can't be reopened by a fresh subprocess at
    all. ``cli/__main__.py`` sets this; ``sdk/``'s own bootstrap (which
    doesn't call this function) and ``browser/__main__.py`` (which drives
    ``ConnectDialog`` directly, profile or direct alike) don't.

    Returns ``(refs, skip_reasons)`` -- ``skip_reasons`` is one line per
    ``(sample, workload type)`` this bootstrap couldn't pick a ref for
    (printed live as it happens, same as before), handed back so a caller
    that has its own ``SmokeContext`` by the time this returns can record
    each one as a real ``ctx.skip(...)`` -- this function runs *before*
    either ``cli/``'s or ``browser/``'s own ``SmokeContext`` exists, so it
    can't record them itself, and a bare ``print()`` alone would leave
    real, cli/browser-specific coverage gaps invisible in ``index.md``,
    visible only in whatever terminal happened to run the tool.
    """
    kinds = leaf_kinds if leaf_kinds is not None else frozenset(UnitKind)
    repos = await discover_repos(session, entries)
    skip_reasons: list[str] = []

    grouped: dict[tuple[str, str], list[tuple[RepoInfo, CatalogId, Workload, list[Version]]]] = {}
    for ri in repos:
        if not ri.readable:
            continue
        if exclude_unreopenable_by_cli and isinstance(ri.entry, RemoteStorageSample):
            reason = f"{ri.sample_name}: remote_storage sample, no --profile flag to reopen via cli subprocess"
            print(f"[smoke] {reason} -- skipped")
            skip_reasons.append(reason)
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
            continue
        for catalog in await ri.repo.catalogs():
            for workload in await catalog.workloads():
                type_key = workload.type_hint
                versions = await catalog.versions(workload)
                grouped.setdefault((ri.sample_name, type_key), []).append((ri, catalog.catalog_id, workload, versions))

    refs: list[RepresentativeRef] = []
    for (sample_name, type_key), entries_for_type in grouped.items():
        ref = await _first_working_ref(sample_name, type_key, entries_for_type, kinds)
        if ref is None:
            reason = f"{sample_name}/{type_key}: no readable leaf found across any version"
            print(f"[smoke] {reason} -- skipped")
            skip_reasons.append(reason)
            continue
        refs.append(ref)
    return refs, skip_reasons


async def resolve_catalog(repo: Repository, catalog_id: CatalogId) -> Catalog:
    """``repo.catalog_by_id(catalog_id)``, raising instead of returning
    ``None`` -- every caller here just enumerated ``catalog_id`` from a
    live listing moments ago, so a miss means the repository's own catalog set
    changed out from under this run, worth a loud failure rather than a
    silent skip.

    Deliberately the real ``Repository.catalog_by_id()`` (see its own
    docstring), not a fresh ``repo.catalogs()`` listing filtered down to
    one match: that method already skips opening every sibling it can
    rule out by ``repo_id`` alone, and — its own docstring's own words —
    exists precisely for "re-fetching one already-known catalog after
    ``set_key()``." ``Repository.set_key()`` closes and replaces every
    already-opened ``DedupRepo`` (see its own docstring), which
    silently turns a ``Catalog`` reference cached here from an earlier
    point in a run stale the moment ``catalog.py``'s own wrong-key/restore
    round trip (or any other ``set_key()`` call) runs against the same
    repository later in the same process -- a stale reference's own next use
    doesn't raise cleanly the way that docstring promises; it surfaces
    several layers down as a raw ``aiosqlite`` "no active connection"
    error instead. Grouping helpers in this module key on ``CatalogId``
    rather than a ``Catalog`` object for exactly this reason."""
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
    version's known data gap (a metadata-only fragment, an unsupported
    format, ...) shouldn't doom the whole group when a sibling version
    might still work -- the same retry shape ``pick_workload_with_retry``
    gives ``sdk/``'s own device/fs/saas domains.

    Each entry carries the ``CatalogId`` of the specific ``Catalog`` its
    ``workload``/``versions`` actually came from -- a repository can hold more
    than one ``Catalog``, and the same ``type_hint`` can appear in more
    than one of them (a real, non-empty workload in one, an empty one of
    the same type in another); ``Catalog.provider()`` doesn't validate
    that a ``Version`` belongs to it, so re-deriving "some catalog" from
    ``ri.repo.catalogs()[0]`` here instead of resolving this entry's own
    ``catalog_id`` (see ``resolve_catalog``) would silently build a
    provider against the wrong catalog for any entry beyond the first.

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
            # See RepoInfo.sole_repo_for_entry's own docstring: only the
            # unambiguous, single-repository-per-entry case gets --key passed at
            # all. When it is passed, the *broad* ref (not the narrowed
            # one) is the one path empirically verified to accept --key
            # for both vault and object_store layouts alike -- object_
            # store's own layout detector doesn't recognize a --key-
            # narrowed single-repository rescan the vault layout's does, but
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
                ri.repo,
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
