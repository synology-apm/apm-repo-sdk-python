"""``Catalog``/``Frame``: one catalog's own workload/version/provider
operations — see ``synology_apm_repo.sdk.api``'s own module docstring for
the whole Repository Layer's scope. ``Catalog`` is obtained from
``Repository.catalogs()``/``Repository.catalog_by_id()`` (the sibling
``api.repository`` module), never constructed directly; the two modules
meet at ``Repository.resolve()``'s canonical/human-ref dispatch, which
``api.repository`` itself owns.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable, Sequence
from typing import Literal, TypeVar

from ..catalog.connection import Connection
from ..catalog.version import Version, versions
from ..catalog.workload import Workload, workload_by_id, workloads
from ..dedup.repository import DedupRepo
from ..dedup.verify_checks import Finding, VerifyLevel
from ..errors import NotFoundError
from ..format.repo_info import RepoInfo
from ..identifiers import CatalogId, resolve_catalog_id
from ..presentation.progress import Progress
from ..units.base import Node, UnitProvider
from ..units.dispatch import SUPPORTED_TARGET_TYPES as _DEVICE_FS_TARGET_TYPES
from ..units.dispatch import provider_for, raw_fallback_provider_for, saas_provider_for
from ..units.node_ref import ambiguous_matches, match_display_name, version_pairs, workload_pairs
from ..units.verify_reachable import verify_reachable


@dataclasses.dataclass(frozen=True)
class Frame:
    """Where a human ref's segments landed after ``Repository.walk_human_ref``
    — exactly as deep as they went, no deeper. ``level`` says which of the
    fields below (if any) is meaningful; the others are carried along for
    breadcrumb rendering and for continuing the walk one level further.
    Resolving a version always continues straight into its node tree (at
    least ``provider.root()``), so ``level`` is never ``"version"`` on
    its own — a version and its ``provider`` only ever appear together
    with ``level="node"``."""

    level: Literal["root", "catalog", "workload", "node"]
    catalog: Catalog | None = None
    workload: Workload | None = None
    version: Version | None = None
    provider: UnitProvider | None = None
    node: Node | None = None


class Catalog:
    """One catalog within an opened ``Repository``: a single ``db/
    connection_config`` row (``connection``) plus the workload/version/
    provider operations scoped to it. Obtained from
    ``Repository.catalogs()``, never constructed directly.

    For a vault, several sibling ``Catalog``s share one underlying
    ``DedupRepo`` (one physical dedup pool) — this mirrors one opened
    ``Pool``/``db`` set queried for several ``connection_config`` rows.
    For object storage, each ``Catalog`` owns its own
    independently-opened ``DedupRepo`` — a distinct physical pool per
    sibling repo-id (FORMAT-SPEC.md: no cross-repo-id dedup).

    A provider this hands out is tracked by the owning ``Repository`` —
    ``Repository.close()`` closes it, not this object; ``Catalog`` itself
    has no ``close()``.
    """

    def __init__(
        self,
        dedup_repo: DedupRepo,
        connection: Connection,
        *,
        track: Callable[[UnitProvider], UnitProvider],
        require_key_verified: Callable[[], None],
    ) -> None:
        self._dedup_repo = dedup_repo
        self.connection = connection
        self._track = track
        # The owning Repository's own gate (KeyRequiredError/KeyMismatchError before
        # any I/O a wrong/missing key would make meaningless) — bound in
        # rather than duplicated here, since key/encryption status is
        # Repository-wide state this class has no copy of. Repository.
        # catalogs() itself is deliberately *not* gated (see its own
        # docstring) — this is the first point that actually is.
        self._require_key_verified = require_key_verified

    @property
    def display_name(self) -> str:
        return self.connection.display_name

    @property
    def catalog_id(self) -> CatalogId:
        """See ``identifiers.resolve_catalog_id``'s own docstring for the
        shared formula: ``self._dedup_repo.layout.repo_id`` (this
        catalog's own repo-id, set on every ``RepoLayout``
        ``catalog_repo_layouts()`` derives for ``OBJECT_STORE``) is used
        when available, falling back to ``str(connection_config_id)`` for
        a vault (whose ``RepoLayout.repo_id`` is always ``None``) and for
        the one object-storage edge case with no derivable repo-id (see
        ``storage.layout``'s ``_data_dir_ancestor``) — safe there too,
        since that case only arises when this is the sole catalog
        reachable from its ``Repository`` anyway."""
        return resolve_catalog_id(self._dedup_repo.layout.repo_id, self.connection.connection_config_id)

    @property
    def info(self) -> RepoInfo:
        """This catalog's own ``repo_info`` — genuinely per-catalog for
        object storage (each repo-id has its own marker file), and the
        vault-wide one shared by every sibling for a vault."""
        return self._dedup_repo.info

    async def workloads(self) -> list[Workload]:
        """Raises ``KeyRequiredError``/``KeyMismatchError`` before any I/O if the
        owning repository is encrypted and not yet key-verified — see
        ``Repository._require_key_verified``. Without this, a locked
        repository's workload list would otherwise come back silently empty or
        wrong, indistinguishable from "this catalog genuinely has no
        workloads"."""
        self._require_key_verified()
        return await workloads(self._dedup_repo, self.connection)

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        """The raw catalog read (``catalog.version.versions``'s own
        docstring covers sort order) — nothing is filtered out. A version
        whose content later turns out unresolvable (a genuine gap, or
        routine backend-side generation rotation the resolving Unit already
        absorbs — see ``units.saas.stream``) raises when actually opened
        (VM/FS, GW/M365) or surfaces a diagnostic node in place of the
        missing disk (PC/PS — see ``units.device_pcps``), the same as
        ``verify()`` already treats it; this method never hides a real
        catalog row in advance of that. Same key gate as ``workloads()`` —
        see its own docstring."""
        self._require_key_verified()
        return await versions(self._dedup_repo, workload, include_deleted=include_deleted)

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> UnitProvider:
        """See ``_provider_for_version``'s own docstring for exactly how
        ``version`` gets dispatched."""
        return await _provider_for_version(
            self._dedup_repo, version, object_db_id=object_db_id, force_raw=force_raw, track=self._track
        )

    async def verify(
        self,
        level: VerifyLevel = VerifyLevel.QUICK,
        *,
        progress: Callable[[Progress], Awaitable[None]] | None = None,
    ) -> list[Finding]:
        """Integrity check over this catalog's own ``DedupRepo`` — for
        a vault, where several sibling ``Catalog``s share one
        ``DedupRepo``, this is the same check as every sibling's own
        ``verify()``, not one scoped to this catalog's own rows alone
        (the top-down walk checks the shared pool via *every* catalog's
        own workloads/versions, not one connection's data alone — see
        ``units.verify_reachable``'s own docstring). Same key gate as
        ``workloads()``/``versions()`` — see its own docstring: without
        it, a locked repository would walk zero versions and report a
        misleadingly clean result instead of refusing to run."""
        self._require_key_verified()
        return await verify_reachable(self._dedup_repo, level, progress=progress)

    async def walk_human_ref(self, segments: tuple[str, ...], *, object_db_id: str | None = None) -> Frame:
        """The three levels below a chosen catalog — workload -> version
        -> item — raising ``NotFoundError`` the moment a segment doesn't match.
        ``segments`` is never empty here: ``Repository.walk_human_ref``
        (this method's only caller) already returns its own ``"catalog"``
        ``Frame`` directly once a human ref names nothing past the
        catalog itself."""
        assert segments, "Repository.walk_human_ref returns before delegating to an empty segments tuple"
        workloads_list = await self.workloads()
        pairs, hints = workload_pairs(workloads_list)
        workload = _match_or_raise(segments[0], pairs, workloads_list, kind="workload", hints=hints)
        if len(segments) == 1:
            return Frame(level="workload", catalog=self, workload=workload)

        versions_list = await self.versions(workload)
        version = _match_or_raise(segments[1], version_pairs(versions_list), versions_list, kind="version")
        provider = await self.provider(version, object_db_id=object_db_id)
        node = provider.root()
        for name in segments[2:]:
            if node.is_leaf:
                raise NotFoundError(f"ref names more levels than the tree has (stopped at {node.name!r})", ref=name)
            children = await provider.children(node)
            node = _match_or_raise(name, [(c.name, str(c.ref)) for c in children], children, kind="item")
        return Frame(level="node", catalog=self, workload=workload, version=version, provider=provider, node=node)


async def _provider_for_version(
    dedup_repo: DedupRepo,
    version: Version,
    *,
    object_db_id: str | None = None,
    force_raw: bool = False,
    track: Callable[[UnitProvider], UnitProvider],
) -> UnitProvider:
    """The shared body behind ``Catalog.provider`` — dispatches ``version``
    to the right ``UnitProvider``:
    ``DeviceProvider``/``FsProvider`` for VM/PC/PS/FS, an
    application-layer SaaS provider chosen by ``Workload.sub_type`` for
    M365/GW, degrading to ``RawObjectProvider`` when nothing recognizes
    it — callers never see ``UnsupportedDataFormatError`` here. Every
    constructed provider is handed to ``track`` before being returned, so
    its owning ``Repository`` can close it later.

    ``object_db_id`` (manual disambiguation override) is ignored for
    VM/PC/PS/FS (a SaaS-only concept) and only reaches a constructed
    provider on the ``RawObjectProvider`` fallback path.

    ``force_raw`` (also SaaS-only, also ignored for VM/PC/PS/FS): skips
    the application-layer candidate loop and goes straight to
    ``RawObjectProvider`` even when an application-layer provider would
    otherwise recognize the version — the TUI's diagnostic (``d``) mode
    uses this to show raw index entries on demand.
    """
    if version.target_type in _DEVICE_FS_TARGET_TYPES:
        return track(await provider_for(dedup_repo, version))
    if force_raw:
        return track(await raw_fallback_provider_for(dedup_repo, version, object_db_id=object_db_id))
    workload = await workload_by_id(dedup_repo, version.workload_id)
    if workload is None:
        return track(await raw_fallback_provider_for(dedup_repo, version, object_db_id=object_db_id))
    return track(await saas_provider_for(dedup_repo, workload, version, object_db_id=object_db_id))


_T = TypeVar("_T")


def _match_or_raise(
    segment: str,
    pairs: Sequence[tuple[str, str]],
    objects: Sequence[_T],
    *,
    kind: str,
    hints: Sequence[str | None] | None = None,
) -> _T:
    """``match_display_name``, raising ``NotFoundError`` on a miss instead of
    returning ``None`` — the "match this segment against candidates, or
    fail with `no {kind} named ...`" shape ``walk_human_ref`` repeats for
    each of its four levels (both ``Catalog.walk_human_ref`` here and
    ``Repository.walk_human_ref``'s own first level). A miss that's
    actually a collision (``segment`` is the pre-suffix name of two or
    more candidates, so none of their disambiguated forms equal it
    exactly) raises a distinct "ambiguous" message naming the real,
    disambiguated candidates instead of the generic "no X named" — see
    ``ambiguous_matches``."""
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
