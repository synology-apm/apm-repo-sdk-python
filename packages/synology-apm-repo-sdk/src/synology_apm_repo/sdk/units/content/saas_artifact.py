"""``LazyArtifact``: the shared ``ContentSource`` implementation for
in-memory application-layer artifacts (Calendar's ``.ics``, Mail's
``.eml``, Contact's CSV) — cheap to construct a node for, expensive to
assemble, then a small in-memory blob; each provider supplies only its
own ``build`` callback. Teams chat/channel content
(``saas_teams_chat.py``) is the one exception to "small": its
``render_channel_html`` holds an entire rendered history in memory as
one string.

``parse_meta_json`` is a shared JSON-parsing helper every ``build``
callback (and ``units/saas/site.py``'s own item assembly) uses before
touching a fetched META object's fields.
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
    instead of a bare ``json.JSONDecodeError`` — the shared parse used by
    Mail, Contact, Calendar, and SharePoint Site. ``label`` is the
    caller's own pre-formatted "<item kind> <id!r> META" prefix.

    Returns ``dict[str, Any]``: field values stay ``Any`` since every
    caller narrows them with its own ``.get()``/``isinstance`` checks
    against untrusted on-disk content.
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
        # Not stream_via_read() (dedup/dedup_file.py): that reads via
        # self.size, which stays None until _ensure_built() runs. Slices
        # the already-materialized buffer directly instead.
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
