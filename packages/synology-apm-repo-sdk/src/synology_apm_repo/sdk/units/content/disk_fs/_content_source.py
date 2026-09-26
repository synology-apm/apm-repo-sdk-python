"""``ContentSource`` implementations for one already-resolved Dissect
filesystem entry — the "read this file's bytes" half of this package's
design. A ``DiskFilesystem`` reaches one of these entries via
``_DissectEntry.open_file`` (``_disk_filesystem.py``), which resolves the
path through the format's own ``resolve()`` before constructing one.
``_BlockingReadContentSource`` is the shared blocking-
read/export plumbing; ``DissectFileContentSource`` adapts one already-
resolved entry (NTFS/ext/FAT/APFS) to it via a lazily-opened,
lock-serialized stream.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import BinaryIO

from ....dedup.dedup_file import DEFAULT_STREAM_BLOCK, ExportResult, clamp_read_length, stream_via_read
from ....errors import ContentUnavailableError, DataCorruptError
from ._apfs import _apfs_is_dataless
from ._base import _CLOUD_ONLY_REASON


class _BlockingReadContentSource:
    """Shared ``ContentSource`` for a single already-resolved guest-OS
    file: deliberately thin, since a Dissect entry's own ``.open()``
    already resolves runlist/fragmentation/sparse regions internally (no
    extent bookkeeping here, unlike ``VirtualDiskContentSource``).
    Streamed in blocks rather than materialized in memory like
    ``LazyArtifact`` — a guest-OS file can be arbitrarily large.

    Held privately by ``DissectFileContentSource`` (composition, not
    inheritance) rather than exposed on its own public surface.
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

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``False`` — a single guest-OS file read through Dissect
        has no bucket concept to spread reads across."""
        return False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        """See ``units.base.ContentSource.read``'s EOF contract — a
        request extending past this file's own ``size`` is clamped, never
        an error.

        Unlike a chunk-map-backed ``ContentSource`` (which never returns a
        silently-short result), the underlying Dissect stream's
        ``.read(n)`` can legitimately return fewer than ``n`` bytes if the
        guest file's real data is truncated relative to what the
        filesystem declared. A short read at this file's declared end is
        raised as ``DataCorruptError`` rather than silently handed back as
        a shorter file.

        Raises:
            DataCorruptError: the underlying stream returned fewer bytes than
                requested while reading up to this file's declared end, or
                ``read_blocking`` itself raised for a reason unrelated to a
                confirmed cloud-sync placeholder (see ``ContentUnavailableError``
                below).
            ContentUnavailableError: ``read_blocking`` failed in a way this
                SDK attributes to a cloud-sync placeholder with no local
                data (currently only APFS's own confirmed flag/xattr
                check — see ``_apfs._apfs_is_dataless``).
        """
        if offset < 0 or (length is not None and length < 0):
            raise ValueError(f"read(offset={offset}, length={length}): offset/length must be non-negative")
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

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> ExportResult:
        # No sparse/hole concept applies to a single guest-OS file read
        # through Dissect (unlike a dedup composition's own ZERO chunks)
        # — always a full, non-sparse write, same posture
        # device.py::_LocalFileContentSource takes for its own non-dedup
        # sidecar files.
        def _open_dst() -> BinaryIO:
            dst.parent.mkdir(parents=True, exist_ok=True)
            return dst.open("wb")

        def _read_validated(offset: int, n: int) -> bytes:
            # Replicates read()'s own short-read-at-declared-end
            # contract, since export_to() bypasses read() itself below.
            data = self._read_blocking(offset, n)
            if len(data) < n and offset + n >= self._size:
                raise DataCorruptError(
                    f"read {len(data)} bytes at offset {offset}, expected {n} (declared size={self._size})"
                )
            return data

        def _read_and_write(handle: BinaryIO, offset: int, n: int) -> int:
            # Fused read+write in one asyncio.to_thread() call rather than
            # a separate read-then-write (two thread-pool round trips per
            # block) -- only the first block pays that extra round trip
            # (below).
            data = _read_validated(offset, n)
            handle.write(data)
            return len(data)

        handle: BinaryIO | None = None
        try:
            written = 0
            offset = 0
            while offset < self._size:
                # offset (not written) drives the loop and the next read's
                # position, advancing by the full block n regardless of how
                # many bytes this call actually returned — matching
                # stream_via_read()'s own identical "advance by n" contract.
                # An interior short read (legitimate per read()'s
                # docstring: the guest file's real data can be truncated
                # relative to what the filesystem declared, at any offset,
                # not just the final block) would otherwise leave `written`
                # permanently stuck below self._size, spinning forever
                # instead of completing.
                n = min(DEFAULT_STREAM_BLOCK, self._size - offset)
                if handle is None:
                    # The destination file is deliberately not created
                    # until this first block actually reads successfully
                    # -- a first-block failure (a cloud-sync placeholder's
                    # ContentUnavailableError, or a genuinely corrupt
                    # file's DataCorruptError) then never leaves a stray,
                    # empty .part file on disk.
                    data = await asyncio.to_thread(_read_validated, offset, n)
                    handle = await asyncio.to_thread(_open_dst)
                    await asyncio.to_thread(handle.write, data)
                    written += len(data)
                else:
                    written += await asyncio.to_thread(_read_and_write, handle, offset, n)
                offset += n
                if progress is not None:
                    await progress(written, self._size)
            if handle is None:
                # self._size == 0: the loop above never ran, but a
                # genuinely empty file's correct export result is still
                # an empty destination file.
                handle = await asyncio.to_thread(_open_dst)
        finally:
            if handle is not None:
                await asyncio.to_thread(handle.close)
        return ExportResult(bytes_written=written, logical_size=self._size, holes=0, zeros=0)


class DissectFileContentSource:
    """Backed by one already-resolved Dissect filesystem entry (an NTFS
    ``MftRecord``, an ext ``INode``, a FAT ``DirectoryEntry``, or an APFS
    ``DirectoryEntry``) — reads through that entry's own ``.open()`` stream
    rather than a format-specific random-access method, since that's the
    one thing all four formats' entries agree on (unlike read-at-offset
    method names/arg orders, which differ per format)."""

    def __init__(self, entry: object, size: int | None) -> None:
        self._entry = entry
        #: Lazily opened on first read, reused for every later block —
        #: ``.open()`` re-resolves the file's runlist/fragmentation
        #: internally, so a multi-block read/export must not pay that
        #: cost again per block. ``_fh_lock`` is a real ``threading.Lock``
        #: (not ``asyncio.Lock``: ``asyncio.to_thread`` dispatches to a
        #: real executor thread pool, so two OS threads can race into
        #: ``_read_blocking`` concurrently — same reasoning as
        #: ``storage/local.py``'s ``_fd_lock``); it serializes every
        #: ``seek``/``read`` pair so an overlapping ``read()``/``stream()``
        #: call on the same instance never races another's ``seek``.
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
                # A failure mid-open/seek/read leaves this stream's state
                # unknown -- drop the cached handle so the next call opens
                # a fresh one.
                self._fh = None
                # Defense-in-depth: _apfs._apfs_content_unavailable's own
                # proactive check (at open_file() time) already catches
                # every confirmed cloud-sync placeholder case ahead of
                # this point; any other failure still becomes
                # DataCorruptError.
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

    @property
    def supports_concurrent_export(self) -> bool:
        return self._blocking.supports_concurrent_export

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return await self._blocking.read(offset, length)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return self._blocking.stream(block)

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> ExportResult:
        return await self._blocking.export_to(dst, sparse=sparse, progress=progress)
