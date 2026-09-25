"""Shared canonical-ref goto (``g``) parsing/resolution."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from synology_apm_repo.browser.strings import GOTO_REF_NOT_CANONICAL_WARNING, GOTO_REF_PARSE_ERROR_WARNING
from synology_apm_repo.browser.widgets.progress_hint import DebouncedProgress
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.units.node_ref import NodeRef, RefKind

if TYPE_CHECKING:
    from synology_apm_repo.sdk.api import Catalog, Repository, Version

    from .navigable_screen import NavigableScreen


def parse_canonical_ref(text: str, *, notify: Callable[..., None]) -> NodeRef | None:
    """Shared by ``BrowseScreen``/``UnitScreen``'s own ``g`` handling:
    parses ``text`` and validates it's a *canonical* ref (the only
    shape ``g`` accepts — the same shape ``y`` copies), notifying and
    returning ``None`` on either failure so both call sites get identical
    error messages for identical mistakes."""
    try:
        node_ref = NodeRef.parse(text.strip())
    except ValueError:
        notify(GOTO_REF_PARSE_ERROR_WARNING, severity="warning")
        return None
    if node_ref.kind is not RefKind.CANONICAL or node_ref.canonical_ids is None:
        notify(GOTO_REF_NOT_CANONICAL_WARNING, severity="warning")
        return None
    return node_ref


async def resolve_goto_version(
    notify: Callable[..., None], repo: Repository, node_ref: NodeRef
) -> tuple[Catalog, Version] | None:
    """Shared by ``BrowseScreen``/``UnitScreen``'s own ``_submit_goto``:
    resolves an already-``parse_canonical_ref``-validated ``node_ref`` to
    its owning ``Catalog`` and ``Version``, notifying and returning
    ``None`` on an ``ApmRepoError`` so both call sites report an
    unresolvable ref identically. Each caller keeps its own
    short-circuit/push behavior around this — ``UnitScreen``'s
    same-version fast path in particular has no shared equivalent here."""
    try:
        return await repo.version_for_ref(node_ref)
    except ApmRepoError as exc:
        notify(str(exc), severity="warning")
        return None


async def resolve_and_open_goto_target(screen: NavigableScreen, repo: Repository, node_ref: NodeRef) -> None:
    """Shared by ``BrowseScreen``'s and ``UnitScreen``'s own cross-catalog/
    cross-version ``g`` dispatch: resolves ``node_ref`` via
    ``resolve_goto_version`` and pushes a fresh ``UnitScreen`` onto the
    result, wrapped in ``DebouncedProgress`` — identical on both call
    sites. Each caller's own same-catalog/same-version fast path (only
    ``UnitScreen`` has one) stays local, since this covers only the
    branch that actually needs a real fetch."""
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen

    with DebouncedProgress(screen):
        resolved = await resolve_goto_version(screen.notify, repo, node_ref)
    if resolved is None:
        return
    catalog, version = resolved
    screen.app.push_screen(UnitScreen(catalog, version, target_ref=node_ref))
