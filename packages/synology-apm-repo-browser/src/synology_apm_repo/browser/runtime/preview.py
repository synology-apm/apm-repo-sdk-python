"""``load_preview``: reads a leaf's content and renders its detail-pane
preview."""

from __future__ import annotations

import asyncio
import dataclasses

from synology_apm_repo.browser.core.unit.select import prefers_recent_content, preview_renderer_for
from synology_apm_repo.sdk import ContentUnavailableError, Node, UnitProvider


@dataclasses.dataclass(frozen=True, slots=True)
class PreviewText:
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class PreviewNote:
    """The content is deliberately unreadable (a cloud-sync placeholder, an
    EFS-encrypted file), not a failure."""

    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class PreviewError:
    message: str


#: ``None``: the renderer had nothing to show.
PreviewResult = PreviewText | PreviewNote | PreviewError | None


async def load_preview(provider: UnitProvider, node: Node, *, read_limit: int) -> PreviewResult:
    """Reads up to ``read_limit`` bytes of ``node``'s content and renders
    them. A ``prefers_recent_content`` node reads the tail instead, so the
    newest messages show. Rendering runs in ``asyncio.to_thread()``
    (CPU-bound). Every ``Exception`` becomes a result; cancellation
    propagates."""
    try:
        unit = await provider.unit(node)
        content = unit.content
        offset = 0
        if prefers_recent_content(node):
            # A zero-length read builds LazyArtifact content, whose size
            # is None until then.
            await content.read(0, 0)
            if content.size is not None and content.size > read_limit:
                offset = content.size - read_limit
        data = await content.read(offset, read_limit)
        # Renderers take bytes; a preview read is capped at read_limit, so the copy is small.
        preview = await asyncio.to_thread(preview_renderer_for(node), bytes(data))
    except ContentUnavailableError as exc:
        return PreviewNote(str(exc))
    except Exception as exc:  # noqa: BLE001
        return PreviewError(str(exc))
    return PreviewText(preview) if preview else None
