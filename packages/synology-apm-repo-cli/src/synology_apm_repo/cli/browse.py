"""Ref parsing and tree walking shared by ``ls``/``tree``/``cat``/``export``.

A CLI ``<ref>`` argument is ``<path>[#<fragment>]``: ``<path>`` is what to
open (a local directory, or with ``--profile`` a root in that profile's
store) and the fragment where to navigate once there, via
``Repository.locate``, so ``ls``/``tree`` never branch on ref kind.
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import AsyncIterator

from synology_apm_repo.cli.errors import fail
from synology_apm_repo.cli.options import raw_view
from synology_apm_repo.cli.repo_session import opened_repo
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.sdk import Frame, NodeRef, Repository, RestorableUnit


@dataclasses.dataclass(frozen=True, slots=True)
class ParsedRef:
    """A CLI ``<ref>`` argument split into the path to open and the parsed
    navigation fragment."""

    fs_path: str
    node_ref: NodeRef


def parse_ref_argument(value: str) -> ParsedRef:
    """A bare path (no ``#``) is a human ref with zero segments — "browse
    from the top of whatever's discovered there"."""
    if "#" not in value:
        return ParsedRef(fs_path=value, node_ref=NodeRef.human(value))
    node_ref = NodeRef.parse(value)
    return ParsedRef(fs_path=node_ref.repo_path, node_ref=node_ref)


@dataclasses.dataclass(frozen=True, slots=True)
class WalkedRef:
    """Where a listing command's REF landed: the open repository, the frame
    it walked to, the REF's filesystem path and whether to show refs."""

    repo: Repository
    frame: Frame
    fs_path: str
    show_ref: bool


@contextlib.asynccontextmanager
async def walked_ref(
    state: CliState, ref: str, key: str | None, *, profile: str | None, object_db_id: str | None, show_ref: bool
) -> AsyncIterator[WalkedRef]:
    """Parse REF, open its repository (``opened_repo``) and walk to it, for
    ``ls``/``tree``. ``--verbose`` implies ``--ref``."""
    parsed = parse_ref_argument(ref)
    async with opened_repo(parsed.fs_path, key, profile=profile, state=state) as repo:
        frame = await repo.locate(parsed.node_ref, raw=raw_view(object_db_id))
        yield WalkedRef(repo, frame, parsed.fs_path, state.verbose or show_ref)


async def resolve_restorable(
    repo: Repository, node_ref: NodeRef, *, ref: str, hint: str, object_db_id: str | None = None
) -> RestorableUnit:
    """``Repository.resolve`` narrowed to a single restorable item —
    ``fail()``s with ``hint`` appended when REF instead names a folder.
    Shared by ``cat``/``export``, whose entire output *is* one item's
    content and so have nothing meaningful to do with a folder ref."""
    frame = await repo.resolve(node_ref, raw=raw_view(object_db_id))
    if not frame.node.is_leaf:
        fail(f"{ref!r} names a folder, not a single item — {hint}")
    return await frame.unit()
