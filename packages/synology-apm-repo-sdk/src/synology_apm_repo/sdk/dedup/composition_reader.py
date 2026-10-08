"""Composition sub-file access and record decoding.

Two collaborating classes:

- ``CompositionReader`` — resolves ``(stream_id, session_id)`` to the
  right sequence of composition sub-files (via ``composition_path``
  plus sequence-id resolution), and reads any byte range of the
  session's global offset space, transparently crossing 16 MiB
  sub-file boundaries.
- ``CompositionRecord`` — one backup version's ``RecordHead`` plus
  page-cached, binary-searched access to its ``ChunkMapRecord`` array.
"""

from __future__ import annotations

import asyncio
import bisect
import dataclasses
from collections.abc import AsyncIterator, Awaitable, Callable

from ..asynccache import AsyncKeyedCache
from ..cachemanager import DEFAULT_LIMITS
from ..errors import FormatError
from ..format.addressing import composition_path, split_composition_offset, split_layer_leaf
from ..format.chunkmap import ChunkMapEntry, chunk_map_end_offset, iter_chunk_map_page, parse_chunk_map_record
from ..format.composition import (
    CompositionHeader,
    CompositionStatus,
    RecordHead,
    chunk_map_array_offset,
    parse_composition_header,
    parse_record_head,
)
from ..format.const import CHUNK_MAP_RECORD_LENGTH, RECORD_HEAD_LENGTH, SUB_FILE_SIZE
from ..format.headers import HEADER_LEN
from ..identifiers import SessionId, StreamId
from ..storage.base import ObjectStore, join_path
from ..storage.dircache import DirCache
from ..storage.seqid import resolve_seq_path


class CompositionReader:
    """Read access to one ``(stream_id, session_id)``'s composition
    sub-files. Does **not** validate the ``subID=0`` header automatically
    (a failure there would already surface as consistently wrong data) —
    call ``verify_header`` explicitly for that stricter, opt-in check.

    ``composition_cache``, when given, is consulted by ``record()`` —
    see that method for the sharing contract.
    """

    def __init__(
        self,
        store: ObjectStore,
        dir_cache: DirCache,
        comp_root: str,
        stream_id: StreamId,
        session_id: SessionId,
        *,
        composition_cache: AsyncKeyedCache[tuple[StreamId, SessionId, int], CompositionRecord] | None = None,
    ) -> None:
        self._store = store
        self._dir_cache = dir_cache
        self._comp_root = comp_root
        self.stream_id = stream_id
        self.session_id = session_id
        self._composition_cache = composition_cache

    async def _subfile_path(self, sub_id: int) -> str:
        full_logical = composition_path(self.stream_id, self.session_id, sub_id)
        dir_part, leaf = split_layer_leaf(full_logical)
        full_dir = join_path(self._comp_root, dir_part)
        return await resolve_seq_path(self._dir_cache, full_dir, leaf)

    async def read_at(self, global_offset: int, n: int) -> bytes:
        """Read ``n`` bytes starting at the session-global ``global_offset``,
        transparently crossing 16 MiB sub-file boundaries
        (FORMAT-SPEC.md: Composition file splitting).

        Raises:
            NotFoundError: A sub-file the range touches is missing.
            FormatError: A sub-file ends before the range does.
        """
        if n == 0:
            return b""
        out = bytearray()
        offset = global_offset
        remaining = n
        while remaining > 0:
            sub_id, sub_off = split_composition_offset(offset)
            path = await self._subfile_path(sub_id)
            take = min(remaining, SUB_FILE_SIZE - sub_off)
            chunk = await self._store.read(path, sub_off, take)
            if len(chunk) < take:
                raise FormatError(
                    f"composition sub-file {path!r} truncated: expected {take} bytes at offset "
                    f"{sub_off}, got {len(chunk)}",
                    ref=path,
                )
            out += chunk
            offset += len(chunk)
            remaining -= len(chunk)
        return bytes(out)

    async def verify_header(self) -> CompositionHeader:
        """Explicitly validate the ``subID=0`` sub-file's 64-byte header
        (major version, ``subFileSize`` constant match).

        Raises:
            UnsupportedVersionError: the header's major version is unsupported.
            DataCorruptError: A bad magic or header CRC, or ``subFileSize``
                is not the fixed 16 MiB.
            NotFoundError: the ``subID=0`` sub-file is missing.
            FormatError: the sub-file is truncated.
        """
        raw = await self.read_at(0, HEADER_LEN)
        return parse_composition_header(raw)

    async def record(self, head_off: int) -> CompositionRecord:
        """Open the composition record at global offset ``head_off``
        (``db/file_map.comp_offset``) — reads only the 32-byte
        ``RecordHead``, not the (potentially huge) chunk-map array that
        follows it.

        With a ``composition_cache``, every caller sharing the same
        ``(stream_id, session_id, head_off)`` gets the identical
        ``CompositionRecord`` (page cache warm) until the entry is
        LRU-evicted; the next call then builds a new one.

        Raises:
            NotFoundError: the sub-file is missing.
            FormatError: the sub-file is truncated.
            DataCorruptError: bad ``RecordHead`` magic or head CRC, or an
                unknown status.
            UnsupportedVersionError: the record lacks the ``Redundancy`` mode bit.
        """
        if self._composition_cache is None:
            return await self._build_record(head_off)
        key = (self.stream_id, self.session_id, head_off)
        return await self._composition_cache.resolve(key, lambda _k: self._build_record(head_off))

    async def _build_record(self, head_off: int) -> CompositionRecord:
        raw = await self.read_at(head_off, RECORD_HEAD_LENGTH)
        record_head = parse_record_head(raw)
        return CompositionRecord(head_off=head_off, record_head=record_head, reader=self)


_PAGE_SIZE = 2048
"""Chunk-map entries per page: the array is read and cached in ~40 KiB pages
(2048 * 20 bytes) rather than one ``read_at()`` per entry."""

_DEFAULT_PAGE_CACHE_MAXSIZE = DEFAULT_LIMITS.composition_pages
"""Default ``_pages`` bound, in pages; a cached page holds raw bytes, not
decoded ``ChunkMapEntry`` values."""


# Not frozen: a stateful page cache, not a value model — _pages/
# _page_end_offsets/_contiguous_scanned are mutated in place as pages are
# fetched.
@dataclasses.dataclass
class CompositionRecord:
    """One backup version's composition record: a ``RecordHead`` plus
    lazy, page-cached, binary-search-capable access to its
    ``ChunkMapRecord`` array.

    Pages (``_PAGE_SIZE`` entries each) are fetched with one read each
    and cached in ``_pages`` as raw bytes, bounded to
    ``_DEFAULT_PAGE_CACHE_MAXSIZE`` pages (LRU beyond that); a page's
    ``ChunkMapEntry`` values are decoded only as a caller consumes them.
    ``seed_pages_from_array`` switches every page, cached or not, to
    already-repaired bytes.
    """

    head_off: int
    record_head: RecordHead
    reader: CompositionReader

    _pages: AsyncKeyedCache[int, bytes] = dataclasses.field(
        default_factory=lambda: AsyncKeyedCache[int, bytes](maxsize=_DEFAULT_PAGE_CACHE_MAXSIZE),
        repr=False,
        compare=False,
    )
    _page_end_offsets: dict[int, int] = dataclasses.field(default_factory=dict, repr=False, compare=False)
    """Each fetched page's last ``end_offset``, by ``page_idx``. Never
    evicted, unlike ``_pages``, so ``_locate``'s search over it needs no
    I/O."""
    _contiguous_scanned: int = dataclasses.field(default=0, repr=False, compare=False)
    """How many pages from page 0 have a recorded end-offset with no gap:
    the prefix ``_locate`` can binary-search."""
    _repaired_array: bytes | None = dataclasses.field(default=None, repr=False, compare=False)
    """The parity-repaired chunk-map array ``seed_pages_from_array`` was
    given; a page missing from ``_pages`` is cut from it, never re-read from
    the corrupted on-disk copy."""
    _page_locks: dict[int, asyncio.Lock] = dataclasses.field(default_factory=dict, repr=False, compare=False)
    """One lock per page index, held by ``_get_page`` and
    ``seed_pages_from_array`` so a fetch and a reseed of the same page never
    interleave (the record is shared repo-wide). Bounded by
    ``_page_count``."""

    def _lock_for_page(self, page_idx: int) -> asyncio.Lock:
        lock = self._page_locks.get(page_idx)
        if lock is None:
            lock = asyncio.Lock()
            self._page_locks[page_idx] = lock
        return lock

    @property
    def status(self) -> CompositionStatus:
        return self.record_head.status

    @property
    def map_num(self) -> int:
        return self.record_head.map_num

    @property
    def attr_leng(self) -> int:
        return self.record_head.attr_leng

    @property
    def _page_count(self) -> int:
        return (self.map_num + _PAGE_SIZE - 1) // _PAGE_SIZE

    async def entries(self, start: int = 0, end: int | None = None) -> AsyncIterator[ChunkMapEntry]:
        """Stream ``ChunkMapEntry`` values whose range overlaps
        ``[start, end)`` (file offsets, **not** record indices; ``end=None``
        runs to the last entry). The first entry is found by binary search
        (``_locate``), then entries are walked forward page by page.
        """
        if self.map_num == 0:
            return
        page_idx, idx_in_page = await self._locate(start)
        while page_idx < self._page_count:
            page_bytes = await self._get_page(page_idx)
            _, count = self._page_bounds(page_idx)
            remaining = page_bytes[idx_in_page * CHUNK_MAP_RECORD_LENGTH :]
            for entry in iter_chunk_map_page(remaining, count - idx_in_page):
                if end is not None and entry.file_offset >= end:
                    return
                yield entry
            page_idx += 1
            idx_in_page = 0

    async def _locate(self, start: int) -> tuple[int, int]:
        """Return ``(page_idx, idx_in_page)`` of the first entry whose
        ``end_offset > start``.

        Binary-searches the known contiguous page prefix's end-offsets (no
        I/O); past it, fetches forward page by page until one covers
        ``start`` (file offsets are not derivable from array position: entry
        lengths and holes vary), then binary-searches within that page.
        """
        if start <= 0:
            return 0, 0

        # Bisects the dict through range(): no O(pages) copy per call.
        page_idx = bisect.bisect_right(range(self._contiguous_scanned), start, key=self._page_end_offsets.__getitem__)

        page_bytes = b""
        while page_idx < self._page_count:
            page_bytes = await self._get_page(page_idx)
            if self._page_end_offsets[page_idx] > start:
                break
            page_idx += 1
        else:
            return self._page_count, 0

        _, count = self._page_bounds(page_idx)
        # Only the ~log2(count) records the search probes are decoded.
        idx_in_page = bisect.bisect_right(range(count), start, key=lambda i: chunk_map_end_offset(page_bytes, i))
        return page_idx, idx_in_page

    async def _resolve_page(self, page_idx: int, fetch: Callable[[int], Awaitable[bytes]]) -> bytes:
        """Resolve page ``page_idx``'s raw bytes via ``fetch``, record its
        last ``end_offset`` and extend ``_contiguous_scanned``. The caller
        holds ``_lock_for_page(page_idx)``.
        """
        page_bytes = await self._pages.resolve(page_idx, fetch)
        count = len(page_bytes) // CHUNK_MAP_RECORD_LENGTH
        last_entry_off = (count - 1) * CHUNK_MAP_RECORD_LENGTH
        last_entry = parse_chunk_map_record(page_bytes[last_entry_off : last_entry_off + CHUNK_MAP_RECORD_LENGTH])
        # Unconditional: a seed_pages_from_array() reseed must replace a pre-repair value.
        self._page_end_offsets[page_idx] = last_entry.end_offset
        if page_idx == self._contiguous_scanned:
            # Also absorb pages already fetched out of order just past this one.
            self._contiguous_scanned += 1
            while self._contiguous_scanned in self._page_end_offsets:
                self._contiguous_scanned += 1
        return page_bytes

    async def _get_page(self, page_idx: int) -> bytes:
        """Page ``page_idx``'s raw bytes: one ``read_at()`` on a miss,
        cached in ``_pages`` thereafter."""
        async with self._lock_for_page(page_idx):
            return await self._resolve_page(page_idx, self._fetch_page)

    async def seed_pages_from_array(self, array_raw: bytes) -> None:
        """Serve every page from ``array_raw``, chunk-map array bytes a
        parity repair already confirmed, so no later read sees the
        still-corrupted on-disk copy, including after a page's eviction.

        Raises:
            ValueError: ``array_raw`` is not ``map_num *
                CHUNK_MAP_RECORD_LENGTH`` bytes.
        """
        expected_len = self.map_num * CHUNK_MAP_RECORD_LENGTH
        if len(array_raw) != expected_len:
            raise ValueError(
                f"seed_pages_from_array: array_raw is {len(array_raw)} bytes, expected "
                f"{expected_len} (map_num={self.map_num})"
            )

        self._repaired_array = array_raw

        async def _repaired_page(page_idx: int) -> bytes:
            return self._page_of_array(array_raw, page_idx)

        for page_idx in range(self._page_count):  # ascending order keeps _contiguous_scanned advancing correctly
            async with self._lock_for_page(page_idx):
                # resolve() keeps an already-settled key, so drop the pre-repair page first.
                self._pages.invalidate(page_idx)
                await self._resolve_page(page_idx, _repaired_page)

    async def extent(self) -> tuple[int, int]:
        """``(start, end)``: the file-offset range this record covers, from
        its first entry's ``file_offset`` to its last entry's
        ``end_offset``; reads only the first and last pages.

        ``units/device_pcps.py`` uses it for a PC/PS disk fragment, whose
        ``file_meta.file_size`` is the whole disk's capacity
        (FORMAT-SPEC.md: PC/PS disk fragments).
        """
        if self.map_num == 0:
            return (0, 0)
        first_page_bytes = await self._get_page(0)
        await self._get_page(self._page_count - 1)
        first_entry = parse_chunk_map_record(first_page_bytes[:CHUNK_MAP_RECORD_LENGTH])
        return first_entry.file_offset, self._page_end_offsets[self._page_count - 1]

    def _page_of_array(self, array_raw: bytes, page_idx: int) -> bytes:
        start_entry, count = self._page_bounds(page_idx)
        return array_raw[start_entry * CHUNK_MAP_RECORD_LENGTH : (start_entry + count) * CHUNK_MAP_RECORD_LENGTH]

    async def _fetch_page(self, page_idx: int) -> bytes:
        if self._repaired_array is not None:
            return self._page_of_array(self._repaired_array, page_idx)
        start_entry, count = self._page_bounds(page_idx)
        map_array_off = chunk_map_array_offset(self.head_off)
        return await self.reader.read_at(
            map_array_off + start_entry * CHUNK_MAP_RECORD_LENGTH,
            count * CHUNK_MAP_RECORD_LENGTH,
        )

    def _page_bounds(self, page_idx: int) -> tuple[int, int]:
        """``(start_entry, count)``: which entries page ``page_idx`` covers."""
        start_entry = page_idx * _PAGE_SIZE
        count = min(_PAGE_SIZE, self.map_num - start_entry)
        return start_entry, count
