"""``ContentSource`` for one already-resolved Dissect filesystem entry,
built by ``_DissectEntry.open_file`` (``_disk_filesystem.py``).
``DissectFileContentSource`` reads the entry through a lazily-opened,
lock-serialized stream; ``_BlockingReadContentSource`` holds the shared
blocking read/export logic.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Callable

from ....dedup.dedup_file import (
    DEFAULT_STREAM_BLOCK,
    clamp_read_length,
    stream_via_read,
    validate_export_range,
    validate_read_args,
)
from ....dedup.export_scheduler import ExportTuning
from ....dedup.export_sink import ExportWriter, WrittenBytesCallback
from ....dedup.extent import ExportResult
from ....errors import ContentUnavailableError, DataCorruptError
from ._apfs import _apfs_is_dataless
from ._base import _CLOUD_ONLY_REASON


class _BlockingReadContentSource:
    """``ContentSource`` over a synchronous ``(offset, length) -> bytes``
    reader for one guest file, run off the event loop. Held privately by
    ``DissectFileContentSource``.
    """

    def __init__(self, size: int | None, read_blocking: Callable[[int, int], bytes]) -> None:
        """
        Args:
            size: The file's size, or ``None`` if unknown (treated as ``0``).
            read_blocking: Synchronous ``(offset, length) -> bytes`` reader,
                run via ``asyncio.to_thread``.
        """
        self._size = size if size is not None else 0
        self._read_blocking = read_blocking

    @property
    def size(self) -> int | None:
        return self._size

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        """``ContentSource.read``; a request past ``size`` is clamped.

        Raises:
            DataCorruptError: A short read at the declared end, or
                ``read_blocking`` failed for any reason other than a
                cloud-sync placeholder.
            ContentUnavailableError: ``read_blocking`` failed on a
                cloud-sync placeholder with no local data (currently
                only APFS's ``_apfs._apfs_is_dataless`` check).
        """
        validate_read_args(offset, length)
        n = clamp_read_length(offset, length, self._size)
        if n <= 0:
            return b""
        data = await asyncio.to_thread(self._read_blocking, offset, n)
        if len(data) < n and offset + n >= self._size:
            raise DataCorruptError(
                f"read {len(data)} bytes at offset {offset}, expected {n} (declared size={self._size})"
            )
        return data

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
        # Always a full, non-sparse write: Dissect exposes no holes.
        validate_export_range(start, end, self._size)

        def _read_validated(offset: int, n: int) -> bytes:
            # read()'s short-read-at-declared-end check, which this bypasses.
            data = self._read_blocking(offset, n)
            if len(data) < n and offset + n >= self._size:
                raise DataCorruptError(
                    f"read {len(data)} bytes at offset {offset}, expected {n} (declared size={self._size})"
                )
            return data

        written = 0
        offset = start
        while offset < end:
            # Advance by the requested n, not len(data): an interior short
            # read would otherwise stall the loop below ``end``.
            n = min(DEFAULT_STREAM_BLOCK, end - offset)
            data = await asyncio.to_thread(_read_validated, offset, n)
            await writer.write_at(offset - start, data)
            written += len(data)
            offset += n
            if progress is not None:
                await progress(len(data))
        return ExportResult(bytes_written=written, logical_size=end - start, holes=0, zeros=0)

    async def planned_bytes(self, start: int, end: int) -> int:
        validate_export_range(start, end, self._size)
        return end - start


class DissectFileContentSource:
    """``ContentSource`` over one resolved Dissect filesystem entry, read
    through its ``.open()`` stream (the read API every supported format
    shares)."""

    def __init__(self, entry: object, size: int | None) -> None:
        self._entry = entry
        #: Opened lazily and reused, since ``.open()`` re-resolves the
        #: file's layout. ``_fh_lock`` is a ``threading.Lock`` (reads run in
        #: executor threads) serializing every ``seek``/``read`` pair.
        self._fh: object | None = None
        self._fh_lock = threading.Lock()
        self._blocking = _BlockingReadContentSource(size, self._read_blocking)

    def _read_blocking(self, offset: int, length: int) -> bytes:
        with self._fh_lock:
            try:
                if self._fh is None:
                    self._fh = self._entry.open()  # type: ignore[attr-defined]
                self._fh.seek(offset)  # type: ignore[union-attr]
                return self._fh.read(length)  # type: ignore[union-attr,no-any-return]
            except Exception as exc:
                # Stream state is unknown after a failure: reopen next time.
                self._fh = None
                # Backstop for the check made at open_file() time.
                if _apfs_is_dataless(self._entry):
                    raise ContentUnavailableError(_CLOUD_ONLY_REASON) from exc
                raise DataCorruptError(
                    f"dissect filesystem parser failed reading offset={offset}, length={length}: {exc}"
                ) from exc
            except BaseException:
                self._fh = None
                raise

    @property
    def size(self) -> int | None:
        return self._blocking.size

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return await self._blocking.read(offset, length)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return self._blocking.stream(block)

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
        return await self._blocking.export_range(writer, start, end, sparse=sparse, progress=progress, tuning=tuning)

    async def planned_bytes(self, start: int, end: int) -> int:
        return await self._blocking.planned_bytes(start, end)
