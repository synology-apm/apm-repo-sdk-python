"""``LazyArtifact``: the shared ``ContentSource`` for application-layer
artifacts assembled in memory (Mail's ``.eml``, Calendar's ``.ics``,
Contact's CSV, a Site List's values, a Teams transcript's HTML); each
provider supplies only a ``build`` callback. The whole artifact is held in
memory, which for a Teams transcript is the entire rendered history.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

from ...dedup.dedup_file import DEFAULT_STREAM_BLOCK, validate_export_range, validate_read_args
from ...dedup.export_scheduler import ExportTuning
from ...dedup.export_sink import ExportWriter, WrittenBytesCallback
from ...dedup.extent import ExportResult


class LazyArtifact:
    """Implements ``ContentSource`` around an ``async build() -> bytes``
    callback, run once on the first ``read``/``stream``/``export_range``/
    ``planned_bytes`` and cached; whatever ``build`` raises surfaces
    there. Construction does no I/O.
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
        """``None`` until the artifact has been assembled by a read, stream,
        export or ``planned_bytes`` call."""
        return None if self._bytes is None else len(self._bytes)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        validate_read_args(offset, length)
        data = await self._ensure_built()
        if length is None:
            length = len(data) - offset
        return data[offset : offset + length]

    async def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        # Not stream_via_read(): it needs self.size, None until built.
        data = await self._ensure_built()
        pos = 0
        while pos < len(data):
            n = min(block, len(data) - pos)
            yield pos, data[pos : pos + n]
            pos += n

    async def export_range(
        self,
        writer: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: WrittenBytesCallback | None = None,
        tuning: ExportTuning | None = None,
    ) -> ExportResult:
        """Writes the assembled artifact's ``[start, end)`` into ``writer``.
        The artifact is assembled first, so ``size`` is known afterwards.

        Raises:
            ValueError: The range is not inside the artifact."""
        data = await self._ensure_built()
        validate_export_range(start, end, len(data))
        if end > start:
            await writer.write_at(0, memoryview(data)[start:end])
        if progress is not None:
            await progress(end - start)
        return ExportResult(bytes_written=end - start, logical_size=end - start, holes=0, zeros=0)

    async def planned_bytes(self, start: int, end: int) -> int:
        validate_export_range(start, end, len(await self._ensure_built()))
        return end - start
