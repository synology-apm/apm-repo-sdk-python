"""Cross-distribution real-sample discovery and ref-selection helpers,
shared by ``sdk/``, ``cli/``, and ``browser/``'s own ``__main__.py``
bootstraps.

``discover_repos()`` turns ``smoke_samples.toml`` into open, real
``Repository`` instances, dispatching on each entry's ``SampleEntry`` kind;
every distribution's bootstrap starts from it. ``list_representative_refs()``
builds on it for ``cli/`` and ``browser/``, which need only one real,
representative leaf per ``(sample, workload type)`` pair.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import assert_never

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
    TargetType,
    TraceEvent,
    UnitKind,
    UnitProvider,
    UnsupportedDataFormatError,
    Version,
    Workload,
)
from synology_apm_repo.sdk.identifiers import CatalogId
from synology_apm_repo.sdk.profiles import store_from_config, store_from_profile
from synology_apm_repo.sdk.units.base import ClosableUnitProvider

from ._samples import LocalSample, ProfileSample, RemoteStorageSample, SampleEntry

#: Known, sample-specific data gaps (metadata-only fragment, unsupported
#: format, ...) rather than real bugs -- what ``sdk/``'s per-domain phases
#: record as DEGRADED via ``ctx.call(degrade_on=...)``. Bootstrap has no
#: report to record into, so it moves on to the next candidate instead.
_DATA_GAP = (NotFoundError, DataCorruptError, ChunkCompactedError, UnsupportedDataFormatError)

#: Bounds for ``find_leaf``'s breadth-first search.
_MAX_CHILDREN_PER_LEVEL = 5
_MAX_DEPTH = 6
_MAX_VISITS = 200


@dataclass
class RepoInfo:
    """One repository this run's bootstrap step discovered, labeled for
    report traceability.

    ``sample_name`` is index-suffixed (``"sample-plain[0]"``) because
    ``Session.discover`` is an ``AsyncIterator`` that
    can yield more than one ``Repository`` for a single configured entry
    (two repositories sharing one bucket).
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
    """The key string bootstrap opened this repository with (``""`` if none),
    so a phase that calls ``repo.set_key()`` with another key
    (``sdk/phases/_catalog.py``'s wrong-key check) can restore the shared
    ``Repository``'s verified state afterward."""
    entry: SampleEntry
    """The ``_samples.py`` entry this repository was discovered from."""
    sole_repo_for_entry: bool
    """``False`` when one sample entry yielded several repositories sharing
    one bucket. Applying the entry's key to a narrower, single-repository
    rescan of one sibling (what ``cli/``'s subprocess does) makes the
    object-store key probe report "no repository found", so ``cli/`` passes
    ``--key`` only when this is ``True``. For a local sample this is
    structural: ``Session.open()`` roots its ``LocalFsStore`` at the path
    given, so a narrowed path cannot see a sibling ``@ActiveProtectKey``
    two levels up."""

    @property
    def broad_repo_ref(self) -> str:
        """The root this repository was discovered under, ``entry.path``: a
        filesystem directory for a ``LocalSample``, a store-relative prefix
        (``""`` for the whole bucket/container) otherwise. The ref used
        whenever ``--key`` is passed (see ``sole_repo_for_entry``)."""
        return self.entry.path

    @property
    def narrow_repo_ref(self) -> str:
        """The narrowest addressable location for just this one repository, as
        a fresh CLI subprocess's ``REPO`` argument: ``entry.path`` joined with
        ``repo_root`` (an absolute filesystem path) for a ``LocalSample``, or
        ``repo_root`` alone for a ``ProfileSample``/``RemoteStorageSample``,
        which is store-root-relative whatever ``root=`` the discovery scan
        used."""
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
    distribution's bootstrap starts from. ``trace``, when given, is
    ``Session.discover``'s ``trace=`` callback with the sample name added."""
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
                store = await store_from_profile(profile_name)
                repos = session.discover(store, key=key or None, root=root, trace=_trace)
            case RemoteStorageSample(config=config, secrets=secrets, path=root, key=key):
                store = await store_from_config(config, secrets)
                repos = session.discover(store, key=key or None, root=root, trace=_trace)
            case _:
                assert_never(entry)

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
    or a markup-special one (``<``, ``>``, ``[``, ``]``, ``&``) -- the names
    most likely to break Rich-markup escaping in CLI output."""
    return any(not (" " <= ch <= "~") or ch in "<>[]&" for ch in node.name)


async def pick_workload_with_retry(
    entries: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]],
    kinds: frozenset[UnitKind],
    *,
    prefer: Callable[[Node], bool] | None = None,
) -> tuple[tuple[RepoInfo, Workload, Version, UnitProvider, Node] | None, bool]:
    """Try every ``(workload, version)`` pair in ``entries`` until one yields
    a matching leaf, skipping a version whose known data gap
    (``_DATA_GAP``) breaks it.

    Returns ``(result, any_truncated)``: ``result`` carries the winning
    candidate's still-open ``provider`` and ``leaf``; ``any_truncated`` is
    ``True`` if ``find_leaf``'s bound cut short at least one tried
    candidate. Every rejected candidate's provider is closed here."""
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

    Returns ``(leaf, truncated)``. ``truncated`` is ``True`` only when no
    match was found and the bound (depth/children-per-level/total-visits)
    left some node unexplored; a tree walked to completion with no match
    returns ``(None, False)``.
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
    """The leaf picked for one ``(sample, workload type)`` pair, as plain
    data: its repository is already closed, so a caller re-opens it from
    ``repo_path``/``ref``."""

    sample_name: str
    type_key: str
    """``workload.type_hint`` -- the other half of this ref's grouping
    key (``sample_name`` alone collides across a sample's several
    workload types); use ``f"{sample_name}.{type_key}"`` for a unique
    step-name prefix."""
    repo_path: str
    """A filesystem path a fresh process can re-open this repository from
    (``RepoInfo.broad_repo_ref`` or ``.narrow_repo_ref``)."""
    workload: Workload
    version: Version
    node: Node
    ref: str
    """``node.ref``'s segments under ``repo_path``: ``node.ref`` itself is
    rooted at the store-relative ``RepoInfo.repo_root``."""
    key: str
    """The key a fresh process passes as ``--key`` (``""`` for none)."""


async def list_representative_refs(
    entries: list[SampleEntry],
    *,
    leaf_kinds: frozenset[UnitKind] | None = None,
) -> tuple[list[RepresentativeRef], list[str]]:
    """One representative leaf per ``(sample, workload type)`` pair of every
    ``[[local]]`` sample in ``entries`` -- ``cli/`` and ``browser/``'s
    bootstrap. Local only: both re-open a ref by filesystem path, and reach
    profile/``remote_storage`` samples through their ``remote_connect``
    domain instead. An unreadable (unkeyed/wrong-keyed) repository is
    skipped.

    Returns ``(refs, skip_reasons)``, one reason per pair no ref could be
    picked for, for the caller to record with ``ctx.skip``.
    """
    kinds = leaf_kinds if leaf_kinds is not None else frozenset(UnitKind)
    refs: list[RepresentativeRef] = []
    skip_reasons: list[str] = []
    local = [entry for entry in entries if isinstance(entry, LocalSample)]
    if not local:
        return refs, skip_reasons

    async with Session() as session:
        for entry in local:
            for ri in await discover_repos(session, [entry]):
                if not ri.readable:
                    await session.close_repo(ri.repo)
                    continue
                if ri.repo.is_encrypted and not ri.sole_repo_for_entry:
                    # A shared-bucket, encrypted sibling: verified in this
                    # session, but cli/browser's fresh per-repository re-open
                    # can't apply the sample's key at that narrower scope (see
                    # RepoInfo.sole_repo_for_entry). sdk/ smoke still covers it.
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
    ``None``: every caller just listed ``catalog_id``, so a miss is a bug.

    This module's helpers key on ``CatalogId`` rather than a cached
    ``Catalog`` because ``Repository.set_key()`` closes and reopens every
    opened catalog, leaving a cached one on a closed connection."""
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
    """``pick_workload_with_retry``'s search, plus a small content probe read,
    returning plain data: every candidate's provider is closed, the
    winner's included."""
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
                # A tree can browse cleanly while the leaf's chunk bytes
                # hit a data gap; a tiny probe read turns that into a clean
                # bootstrap skip instead of an unexplained cli/ subprocess
                # failure later.
                unit = await provider.unit(leaf)
                content = unit.content
                await content.read(0, min(64, content.size or 64))
            except _DATA_GAP:
                await close_if_closable(provider)
                continue
            await close_if_closable(provider)
            # --key only for a sole repository (see sole_repo_for_entry),
            # and then with the broad ref: the object-store layout doesn't
            # recognize a --key-narrowed single-repository rescan.
            key = ri.key if ri.sole_repo_for_entry else ""
            real_path = ri.broad_repo_ref if key else ri.narrow_repo_ref
            ref_str = str(NodeRef(real_path, leaf.ref.segments))
            return RepresentativeRef(
                sample_name,
                type_key,
                real_path,
                workload,
                version,
                leaf,
                ref_str,
                key,
            )
    return None


def pick_session_refs(refs: list[RepresentativeRef]) -> tuple[RepresentativeRef | None, RepresentativeRef | None]:
    """``(main_ref, encrypted_ref)`` from ``list_representative_refs``'s
    refs, shared by ``cli/`` and ``browser/``. ``main_ref`` prefers an
    unencrypted, non-SaaS ref (``UnitScreen`` reloads its whole tree,
    dropping the cursor, when verbose mode flips on a SaaS version); ``encrypted_ref`` is the first ref opened
    with a key. Either can be ``None``, and both can be the same ref."""
    unencrypted = [r for r in refs if not r.key]
    main = (
        next((r for r in unencrypted if r.version.target_type not in (TargetType.M365, TargetType.GWS)), None)
        or (unencrypted[0] if unencrypted else None)
        or (refs[0] if refs else None)
    )
    return main, next((r for r in refs if r.key), None)
