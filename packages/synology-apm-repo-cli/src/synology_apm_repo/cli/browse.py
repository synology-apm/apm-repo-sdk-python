"""Ref parsing and tree walking shared by ``ls``/``tree``/``cat``/``export``.

A CLI ``<ref>`` argument is ``<filesystem-path>[#<fragment>]`` — the part
before ``#`` tells this module which directory to ``Session.open``, the
part after (if any) says where to navigate once there. ``walk_ref`` drives
``Repository.walk_human_ref`` for a human ref and ``Repository.resolve`` for
a canonical/raw one, wrapping either into the same ``Frame`` shape so
``ls``/``tree`` never need to branch on ref kind.
"""

from __future__ import annotations

import dataclasses

from synology_apm_repo.cli.errors import fail
from synology_apm_repo.sdk.api import Frame as Frame
from synology_apm_repo.sdk.api import Repository
from synology_apm_repo.sdk.units.base import RestorableUnit, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef, RefKind


@dataclasses.dataclass(frozen=True)
class ParsedRef:
    """A CLI ``<ref>`` argument split into the filesystem path (``Session.open``
    target) and the parsed navigation fragment."""

    fs_path: str
    node_ref: NodeRef


def parse_ref_argument(value: str) -> ParsedRef:
    """A bare path (no ``#``) is a human ref with zero segments — "browse
    from the top of whatever's discovered there"."""
    if "#" not in value:
        return ParsedRef(fs_path=value, node_ref=NodeRef.human(value))
    node_ref = NodeRef.parse(value)
    return ParsedRef(fs_path=node_ref.repo_path, node_ref=node_ref)


async def walk_ref(repo: Repository, node_ref: NodeRef, *, object_db_id: str | None = None) -> Frame:
    """``Repository.walk_human_ref`` for a human ref; canonical/raw refs
    (already fully-qualified) resolve straight to a node via
    ``Repository.resolve`` instead, wrapped in the same ``Frame`` shape so
    callers don't need to branch on ref kind."""
    if node_ref.kind is RefKind.HUMAN:
        return await repo.walk_human_ref(node_ref.segments, object_db_id=object_db_id)
    resolved = await repo.resolve(node_ref, object_db_id=object_db_id)
    return Frame(level="node", node=resolved, provider=await _provider_for(repo, node_ref, object_db_id=object_db_id))


async def _provider_for(repo: Repository, node_ref: NodeRef, *, object_db_id: str | None = None) -> UnitProvider | None:
    """The children-listing provider for a resolved canonical/raw node —
    needed by ``walk_ref``'s callers (``ls``/``tree``) to list a resolved
    node's own children, something ``Repository.resolve()`` itself has no
    reason to return (``cat``/``export`` never need it)."""
    if node_ref.kind is RefKind.RAW:
        return await repo.file_map_tree()
    if node_ref.kind is RefKind.CANONICAL:
        catalog, version = await repo.version_for_ref(node_ref)
        return await catalog.provider(version, object_db_id=object_db_id)
    return None  # pragma: no cover - defensive: walk_ref() only reaches here for RAW/CANONICAL kinds


async def resolve_restorable(
    repo: Repository, node_ref: NodeRef, *, ref: str, hint: str, object_db_id: str | None = None
) -> RestorableUnit:
    """``Repository.resolve`` narrowed to a single restorable item —
    ``fail()``s with ``hint`` appended when REF instead names a folder.
    Shared by ``cat``/``export``, whose entire output *is* one item's
    content and so have nothing meaningful to do with a folder ref."""
    resolved = await repo.resolve(node_ref, object_db_id=object_db_id)
    if not isinstance(resolved, RestorableUnit):
        fail(f"{ref!r} names a folder, not a single item — {hint}")
    return resolved
