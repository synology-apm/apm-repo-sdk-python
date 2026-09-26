"""Composition sub-file access and record decoding.

Two collaborating classes:

- ``CompositionReader`` — resolves ``(stream_id, session_id)`` to the
  right sequence of composition sub-files (via ``composition_path``
  plus sequence-id resolution), and reads any byte range of the
  session's global offset space, transparently crossing 16 MiB
  sub-file boundaries.
- ``CompositionRecord`` — one backup version's ``RecordHead`` plus
  binary-search-capable access to its ``ChunkMapRecord`` array — see
  ``CompositionRecord.entries`` for why it never linear-scans.
"""

from __future__ import annotations

import bisect
import dataclasses
from collections.abc import AsyncIterator, Awaitable, Callable

from ..asynccache import AsyncKeyedCache
from ..errors import FormatError
from ..format.addressing import composition_path, split_composition_offset, split_layer_leaf
from ..format.chunkmap import ChunkMapEntry, iter_chunk_map_page, parse_chunk_map_record
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
    """

    def __init__(
        self,
        store: ObjectStore,
        dir_cache: DirCache,
        comp_root: str,
        stream_id: StreamId,
        session_id: SessionId,
    ) -> None:
        self._store = store
        self._dir_cache = dir_cache
        self._comp_root = comp_root
        self.stream_id = stream_id
        self.session_id = session_id

    async def _subfile_path(self, sub_id: int) -> str:
        full_logical = composition_path(self.stream_id, self.session_id, sub_id)
        dir_part, leaf = split_layer_leaf(full_logical)
        full_dir = join_path(self._comp_root, dir_part)
        return await resolve_seq_path(self._dir_cache, full_dir, leaf)

    async def read_at(self, global_offset: int, n: int) -> bytes:
        """Read ``n`` bytes starting at the session-global ``global_offset``,
        transparently crossing 16 MiB sub-file boundaries
        (FORMAT-SPEC.md: composition-splitting)."""
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
        (major version, ``subFileSize`` constant match)."""
        raw = await self.read_at(0, HEADER_LEN)
        return parse_composition_header(raw)

    async def record(self, head_off: int) -> CompositionRecord:
        """Open the composition record at global offset ``head_off``
        (``db/file_map.comp_offset``) — reads only the 32-byte
        ``RecordHead``, not the (potentially huge) chunk-map array that
        follows it."""
        raw = await self.read_at(head_off, RECORD_HEAD_LENGTH)
        record_head = parse_record_head(raw)
        return CompositionRecord(head_off=head_off, record_head=record_head, reader=self)


_PAGE_SIZE = 2048
"""Chunk-map entries per page — read/cache the array in ~40 KiB pages
(2048 * 20 bytes) instead of one 20-byte ``read_at()`` per entry. Real
workloads' ``map_num`` stays within 1-2 pages at this size."""

_DEFAULT_PAGE_CACHE_MAXSIZE = 128
"""Default ``_pages`` bound, in pages: ``128 * 2048 = 262,144`` entries,
generous since a cached page holds raw ~40 KiB, not a decoded
``ChunkMapEntry`` list."""


# Not frozen: a stateful page cache, not a value model — _pages/
# _page_end_offsets/_contiguous_scanned are mutated in place as pages are
# fetched.
@dataclasses.dataclass
class CompositionRecord:
    """One backup version's composition record: a ``RecordHead`` plus
    lazy, page-cached, binary-search-capable access to its
    ``ChunkMapRecord`` array.

    Pages (``_PAGE_SIZE`` entries each) are fetched with one merged read
    and cached in ``_pages`` as raw bytes, bounded to
    ``_DEFAULT_PAGE_CACHE_MAXSIZE`` pages (LRU beyond that) — see
    ``_page_end_offsets`` for how ``_locate`` stays free of that bound. A
    page's ``ChunkMapEntry`` values are materialized lazily, one at a
    time, only where a caller actually consumes them.

    A caller that already has this record's own chunk-map array bytes in
    hand and already knows they're correct (``seed_pages_from_array``) can
    pre-populate the whole cache from them directly, instead of only ever
    filling it cold, page by page, off disk.
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
    """Every page ever fetched's own last entry's ``end_offset``, keyed by
    ``page_idx`` — never evicted, unlike ``_pages``. The only thing
    ``_locate``'s binary search needs to find which page an offset falls
    in, so that search stays free of I/O regardless of what's still
    resident in ``_pages``."""
    _contiguous_scanned: int = dataclasses.field(default=0, repr=False, compare=False)
    """How many pages, starting from page 0, are known with no gaps —
    the prefix ``_locate`` can binary-search over for free. Only ever
    advances past a page index that was fetched *in order* (see
    ``_get_page``); a page fetched out of order still gets its end-offset
    recorded for exact reuse, it just doesn't yet extend this contiguous
    prefix."""

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
        ``[start, end)`` (file offsets within the described file, **not**
        record indices). Locates the starting entry via ``_locate``
        (binary search over known page boundaries, page-sized reads for
        anything not yet cached), then walks forward sequentially — never
        a linear, one-entry-at-a-time scan.

        ``start=0, end=None`` (the default) walks every entry, for a full
        sequential export/verify.
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

        Two-level binary search: first over the already-known contiguous
        page prefix's end-offsets (free, no I/O), then, if ``start`` falls
        beyond every page scanned so far, fetching forward page by page —
        a not-yet-visited page's coverage is data-dependent (``repeat``/
        ``INHERIT`` mean file offsets aren't derivable from array
        position alone) — until one page's ``end_offset`` covers
        ``start``, and only then binary-searching within that one page's
        real bytes.
        """
        if start <= 0:
            return 0, 0

        known_end_offsets = [self._page_end_offsets[i] for i in range(self._contiguous_scanned)]
        page_idx = bisect.bisect_right(known_end_offsets, start)

        # Each page fetched here along the way has its end-offset recorded
        # for good — this loop only ever advances forward.
        page_bytes = b""
        while page_idx < self._page_count:
            page_bytes = await self._get_page(page_idx)
            if self._page_end_offsets[page_idx] > start:
                break
            page_idx += 1
        else:
            return self._page_count, 0

        _, count = self._page_bounds(page_idx)
        end_offsets = [e.end_offset for e in iter_chunk_map_page(page_bytes, count)]
        idx_in_page = bisect.bisect_right(end_offsets, start)
        return page_idx, idx_in_page

    async def _resolve_page(self, page_idx: int, fetch: Callable[[int], Awaitable[bytes]]) -> bytes:
        """The shared half of ``_get_page``/``seed_pages_from_array``: resolve
        page ``page_idx``'s raw bytes via ``fetch`` (cold-fetched off disk
        for the former, sliced from already-in-memory bytes for the
        latter), record its own last entry's ``end_offset`` into
        ``_page_end_offsets``, and advance ``_contiguous_scanned`` the
        same way either caller needs.
        """
        page_bytes = await self._pages.resolve(page_idx, fetch)
        count = len(page_bytes) // CHUNK_MAP_RECORD_LENGTH
        last_entry_off = (count - 1) * CHUNK_MAP_RECORD_LENGTH
        last_entry = parse_chunk_map_record(page_bytes[last_entry_off : last_entry_off + CHUNK_MAP_RECORD_LENGTH])
        # Unconditional, not just-if-unset: a seed_pages_from_array()
        # reseed must overwrite a stale, pre-repair value here too, not
        # just in _pages.
        self._page_end_offsets[page_idx] = last_entry.end_offset
        if page_idx == self._contiguous_scanned:
            # Runs after every call, hit or miss alike — harmless on a
            # hit, since the ``while`` loop below is itself the guard that
            # keeps this from advancing past a page index already passed.
            self._contiguous_scanned += 1
            while self._contiguous_scanned in self._page_end_offsets:
                self._contiguous_scanned += 1
        return page_bytes

    async def _get_page(self, page_idx: int) -> bytes:
        """Return page ``page_idx``'s raw bytes — one merged ``read_at()``
        for up to ``_PAGE_SIZE`` entries on first access, cached
        thereafter up to ``_DEFAULT_PAGE_CACHE_MAXSIZE`` pages (LRU
        beyond that — see the class docstring). A cold ``page_idx`` is
        fetched once even under concurrent callers, via
        ``AsyncKeyedCache``'s own in-flight-dedup contract.
        """
        return await self._resolve_page(page_idx, self._fetch_page)

    async def seed_pages_from_array(self, array_raw: bytes) -> None:
        """Pre-populate every page of this record's cache from
        ``array_raw`` — chunk-map array bytes already confirmed correct
        via redundancy-blob parity repair — instead of leaving
        ``_get_page`` to cold-fetch the still-corrupted on-disk copy.
        ``array_raw`` must be exactly ``map_num * CHUNK_MAP_RECORD_LENGTH``
        bytes. Force-refreshes any page already resolved by an earlier
        call, not just cold ones.

        Raises:
            ValueError: ``array_raw``'s length doesn't match this
                record's own ``map_num``.
        """
        expected_len = self.map_num * CHUNK_MAP_RECORD_LENGTH
        if len(array_raw) != expected_len:
            raise ValueError(
                f"seed_pages_from_array: array_raw is {len(array_raw)} bytes, expected "
                f"{expected_len} (map_num={self.map_num})"
            )

        async def _page_from_array(page_idx: int) -> bytes:
            start_entry, count = self._page_bounds(page_idx)
            return array_raw[start_entry * CHUNK_MAP_RECORD_LENGTH : (start_entry + count) * CHUNK_MAP_RECORD_LENGTH]

        for page_idx in range(self._page_count):  # ascending order keeps _contiguous_scanned advancing correctly
            # invalidate() first: AsyncKeyedCache.resolve() is a no-op for
            # an already-settled key, so skipping this would leave a
            # pre-repair page's stale content in place.
            self._pages.invalidate(page_idx)
            await self._resolve_page(page_idx, _page_from_array)

    async def extent(self) -> tuple[int, int]:
        """``(start, end)``: the real file-offset range this record
        actually covers — its first entry's ``file_offset`` to its last
        entry's ``end_offset``. Cheap regardless of ``map_num`` (two page
        reads, first and last, never a full scan).

        Needed by PC/PS's per-region disk fragments
        (``units/content/pcps_disk.py``): each fragment's registered
        ``file_meta.file_size`` is the whole disk's total capacity,
        identical across every sibling fragment — asking its own
        composition record directly is the only way to learn a
        fragment's actual coverage (FORMAT-SPEC.md: pcps-fragments). VM's
        model doesn't need this: one composition record already covers
        the whole disk.
        """
        if self.map_num == 0:
            return (0, 0)
        first_page_bytes = await self._get_page(0)
        await self._get_page(self._page_count - 1)
        first_entry = parse_chunk_map_record(first_page_bytes[:CHUNK_MAP_RECORD_LENGTH])
        return first_entry.file_offset, self._page_end_offsets[self._page_count - 1]

    async def _fetch_page(self, page_idx: int) -> bytes:
        map_array_off = chunk_map_array_offset(self.head_off)
        start_entry, count = self._page_bounds(page_idx)
        return await self.reader.read_at(
            map_array_off + start_entry * CHUNK_MAP_RECORD_LENGTH,
            count * CHUNK_MAP_RECORD_LENGTH,
        )

    def _page_bounds(self, page_idx: int) -> tuple[int, int]:
        """``(start_entry, count)``: which entries page ``page_idx`` covers,
        shared by ``_fetch_page`` and ``seed_pages_from_array``'s
        ``_page_from_array`` — only how each then gets those entries' raw
        bytes (an on-disk read vs. a slice of an already-in-memory array)
        differs between the two."""
        start_entry = page_idx * _PAGE_SIZE
        count = min(_PAGE_SIZE, self.map_num - start_entry)
        return start_entry, count
