"""``LazyArtifact``: the ``ContentSource`` implementation shared by
every application-layer artifact this project builds (Calendar's
``.ics``, Mail's ``.eml``, Contact's CSV) — each cheap to construct a
*node* for, expensive to actually assemble, and once assembled just a
small, fully in-memory blob of bytes. ``CalendarProvider``/
``MailProvider``/``ContactProvider`` each supply only the one thing
that's actually different: the ``build`` callback itself.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

from ...dedup.dedup_file import DEFAULT_STREAM_BLOCK, ExportResult


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
        """``None`` until the artifact has actually been assembled. This
        is a synchronous property, but assembling requires ``await``, so
        it reports the real length only after a ``read``/``stream``/
        ``export_to`` call has built it. Callers that need the number
        must read the content."""
        return None if self._bytes is None else len(self._bytes)

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``False`` — an assembled artifact is a single in-memory
        blob with no bucket concept to spread reads across."""
        return False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
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
        # Local file writes have no native async form (storage/local.py's
        # own docstring) — the blocking call goes through to_thread, same
        # as dedup/dedup_file.py's own export path.
        await asyncio.to_thread(dst.write_bytes, data)
        if progress is not None:
            await progress(len(data), len(data))
        return ExportResult(bytes_written=len(data), logical_size=len(data), holes=0, zeros=0)
