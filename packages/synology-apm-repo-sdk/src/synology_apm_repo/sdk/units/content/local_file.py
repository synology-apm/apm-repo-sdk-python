"""``LocalFileContentSource`` — a ``ContentSource`` over one plain store
file, outside the dedup layer."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Self

from ...dedup.dedup_file import (
    DEFAULT_STREAM_BLOCK,
    read_blocks,
    stream_via_read,
    validate_export_range,
    validate_read_args,
)
from ...dedup.export_scheduler import ExportTuning
from ...dedup.export_sink import ExportWriter, WrittenBytesCallback
from ...dedup.extent import ExportResult
from ...errors import FormatError
from ...storage.base import ObjectStore


class LocalFileContentSource:
    """A ``ContentSource`` reading a file directly from the store, not
    through the dedup layer — for the non-dedup descriptor files
    (``.vmx``/``.vmdk`` headers/``.delta``) under ``copy_meta_file/<dir>/``.

    Build one with ``create``: a caller-unknown size is resolved via
    ``store.size()`` there, since ``size`` must stay a sync property."""

    def __init__(self, store: ObjectStore, path: str, size: int | None) -> None:
        self._store = store
        self._path = path
        self._size = size

    @classmethod
    async def create(cls, store: ObjectStore, path: str, size: int | None) -> Self:
        return cls(store, path, size if size is not None else await store.size(path))

    @property
    def size(self) -> int | None:
        return self._size

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        validate_read_args(offset, length)
        return await self._store.read(self._path, offset, length)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return stream_via_read(self, block)

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
        """Copies the range block by block: always a full, non-sparse write
        regardless of ``sparse``.

        Raises:
            ValueError: The range is not inside the file.
            FormatError: The store file is shorter than ``size``.
        """
        assert self.size is not None
        validate_export_range(start, end, self.size)
        written = 0
        async for offset, block in read_blocks(self, start, end):
            await writer.write_at(offset, block)
            written += len(block)
            if progress is not None:
                await progress(len(block))
        if written < end - start:
            raise FormatError(f"store file holds {start + written} bytes, expected {end}", ref=self._path)
        return ExportResult(bytes_written=written, logical_size=end - start, holes=0, zeros=0)

    async def planned_bytes(self, start: int, end: int) -> int:
        assert self.size is not None
        validate_export_range(start, end, self.size)
        return end - start
