"""Shared canonical-ref goto (``g``) parsing/resolution."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from synology_apm_repo.browser.strings import GOTO_REF_NOT_CANONICAL_WARNING, GOTO_REF_PARSE_ERROR_WARNING
from synology_apm_repo.browser.widgets.progress_hint import DebouncedProgress
from synology_apm_repo.sdk import ApmRepoError, NodeRef, RefKind

if TYPE_CHECKING:
    from synology_apm_repo.sdk import Repository, VersionLocation

    from .navigable_screen import NavigableScreen


def parse_canonical_ref(text: str, *, notify: Callable[..., None]) -> NodeRef | None:
    """``text`` parsed as a canonical ref (the form ``y`` copies, the only
    one ``g`` accepts), or ``None`` after notifying why not."""
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
) -> VersionLocation | None:
    """The version a canonical ``node_ref`` names, or ``None`` after
    notifying an ``ApmRepoError``."""
    try:
        return await repo.version_for_ref(node_ref)
    except ApmRepoError as exc:
        notify(str(exc), severity="warning")
        return None


async def resolve_and_open_goto_target(screen: NavigableScreen, repo: Repository, node_ref: NodeRef) -> None:
    """Resolves ``node_ref`` (``resolve_goto_version``, with the
    breadcrumb's loading indicator) and pushes a ``UnitScreen`` on its
    version."""
    from synology_apm_repo.browser.screens.unit_screen import UnitScreen

    with DebouncedProgress(screen):
        resolved = await resolve_goto_version(screen.notify, repo, node_ref)
    if resolved is None:
        return
    screen.app.push_screen(UnitScreen(resolved.catalog, resolved.version, target_ref=node_ref))
