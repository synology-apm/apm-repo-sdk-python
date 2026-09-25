"""``LazyArtifact``: the ``ContentSource`` implementation shared by
every application-layer artifact this project builds (Calendar's
``.ics``, Mail's ``.eml``, Contact's CSV) — each cheap to construct a
*node* for, expensive to actually assemble, and once assembled just a
small, fully in-memory blob of bytes. ``CalendarProvider``/
``MailProvider``/``ContactProvider`` each supply only the one thing
that's actually different: the ``build`` callback itself.

Teams chat/channel content is the one known exception to "small":
``render_channel_html`` (``saas_teams_chat.py``) holds an entire
rendered channel/chat history in memory as one string.

``parse_meta_json`` is a second, smaller shared piece those same
``build`` callbacks (and ``units/saas/site.py``'s own item-``_assemble``,
one layer up) each need: every one of them starts by parsing a fetched
META object's raw bytes as JSON before touching its fields.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any, cast

from ...dedup.dedup_file import DEFAULT_STREAM_BLOCK, ExportResult
from ...errors import DataCorruptError


def parse_meta_json(meta_bytes: bytes, label: str, *, ref: str | None = None) -> dict[str, Any]:
    """Parses ``meta_bytes`` as JSON, raising
    ``DataCorruptError(f"{label} did not parse as JSON: ...", ref=ref)``
    instead of letting a bare ``json.JSONDecodeError`` propagate — the
    identical try/except every META-object JSON parse in this project
    needs (Mail, Contact, Calendar, SharePoint Site). ``label`` is the
    caller's own already-formatted "<item kind> <id!r> META"-shaped
    prefix, so each keeps its own established wording/ordering rather
    than this function imposing one.

    Returns ``dict[str, Any]`` — real META JSON is always an object at
    the top level, but individual field values are still ``Any``: every
    caller immediately narrows them with its own ``.get()``/``isinstance``
    checks (the data is untrusted on-disk content, never assumed-shaped),
    the same way it already would against a bare ``json.loads()`` result.
    """
    try:
        return cast("dict[str, Any]", json.loads(meta_bytes))
    except json.JSONDecodeError as exc:
        raise DataCorruptError(f"{label} did not parse as JSON: {exc}", ref=ref) from exc


class LazyArtifact:
    """Implements ``ContentSource`` around an ``async build() -> bytes``
    callback, awaited at most once and cached on the first
    ``read``/``stream``. Assembly failures (e.g. a META object shaped in
    a way the caller doesn't recognize) surface as whatever exception
    ``build`` itself raises, the first time any of
    ``read``/``stream``/``export_to`` is actually awaited — constructing
    a ``RestorableUnit`` around a ``LazyArtifact`` never does I/O or
    raises on its own.
    """

    def __init__(self, build: Callable[[], Awaitable[bytes]]) -> None:
        self._build = build
        self._bytes: bytes | None = None

    async def _ensure_built(self) -> bytes:
        if self._bytes is None:
            self._bytes = await self._build()
        return self._bytes

    @property
    def size(self) -> int | None:
        """``None`` until the artifact has actually been assembled — a
        synchronous property, but assembling requires ``await``, so a
        caller that needs the real length must first read/stream/export
        the content."""
        return None if self._bytes is None else len(self._bytes)

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``False`` — an assembled artifact is a single in-memory
        blob with no bucket concept to spread reads across."""
        return False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if offset < 0 or (length is not None and length < 0):
            raise ValueError(f"read(offset={offset}, length={length}): offset/length must be non-negative")
        data = await self._ensure_built()
        if length is None:
            length = len(data) - offset
        return data[offset : offset + length]

    async def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        # NOT stream_via_read() (dedup/dedup_file.py): that helper reads
        # via self.size, which stays None here until _ensure_built() has
        # actually run -- a real, legitimate difference from every other
        # ContentSource implementer, not an oversight. Slices the
        # already-materialized buffer directly instead.
        data = await self._ensure_built()
        pos = 0
        while pos < len(data):
            n = min(block, len(data) - pos)
            yield pos, data[pos : pos + n]
            pos += n

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> ExportResult:
        data = await self._ensure_built()
        # Local file writes have no native async form in CPython (no
        # asyncio equivalent of os.open/os.write) — the blocking call
        # goes through to_thread, same as dedup/dedup_file.py's own
        # export path.
        await asyncio.to_thread(dst.write_bytes, data)
        if progress is not None:
            await progress(len(data), len(data))
        return ExportResult(bytes_written=len(data), logical_size=len(data), holes=0, zeros=0)
