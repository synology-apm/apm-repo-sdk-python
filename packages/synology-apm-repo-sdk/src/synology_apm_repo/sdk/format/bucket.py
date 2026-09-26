"""Bucket file (``.buk``) format (FORMAT-SPEC.md §4).

Pure ``bytes -> dataclass`` decode, zero I/O — this module never opens a
file. The Dedup Layer's ``BucketReader`` is what actually fetches bytes
from an ``ObjectStore`` at the offsets this module computes, then hands
ciphertext through ``format.crypto`` and ``compression``.

ABP builds always write ``mode = COMPRESS|CHUNK_CRC`` (``0x03``), optionally
with ``VAULT_ENCRYPT`` (``0x83``) — but every mode bit is checked explicitly
here rather than assumed: FORMAT-SPEC.md: bucket-header warns that a bit always
being 1 today is an observation, not a guarantee the check can be skipped.
"""

from __future__ import annotations

import array
import dataclasses
import struct
from collections.abc import Sequence
from typing import NamedTuple, TypeVar, overload

from ..errors import DataCorruptError, FormatError
from .compression import CompressType
from .const import (
    CHUNK_CRC_SIZE,
    COMPRESS_RESERVED_LENG,
    FIXED_CHUNK_LENGTH,
    REDUNDANCY_COVERAGE_BUCKET,
    RESERVED_LENG,
    SIZE_STORE_REC_BIT_NUM,
)
from .headers import MAGIC, parse_index_header, verify_crc32
from .redundancy import redundancy_size

_SPEC = "FORMAT-SPEC.md §4"

MODE_COMPRESS = 0x01
MODE_CHUNK_CRC = 0x02
MODE_BUCKET_PARITY = 0x04  # never set on any real data
MODE_ENCRYPT = 0x08  # the DATA_KEY encryption path — never set by any current writer
MODE_LOGIC_LOCALITY = 0x10  # never granted by any production code path
MODE_EXTENT_PARITY = 0x20  # never set on any real data
MODE_INPLACE_PARITY = 0x40  # never produced by any current writer
MODE_VAULT_ENCRYPT = 0x80

_MAJOR_VAULT = 3
"""Newest bucket major version (``MAJOR_VAULT``), what every ABP build
writes; reads accept anything ``<= 3``."""

_OFF_MODE = 8  # mode/chunk_num/chunk_size_crc are contiguous uint32s at [8, 20)
_OFF_CRC_OF_CHUNK_CRC = 29  # not contiguous with the [8, 20) group above

COMPRESS_TYPE_BY_VALUE = {member.value: member for member in CompressType}
"""Plain-dict stand-in for ``CompressType(value)``, used in
``parse_size_store``'s per-chunk hot loop (up to 8192 calls per bucket) to
avoid ``Enum.__call__``'s lookup overhead."""

_COMPRESS_TYPE_NONE_VALUE = CompressType.NONE.value
COMPRESS_TYPE_COMPACTED_VALUE = CompressType.COMPACTED.value
"""Raw value compared directly in ``_SizeStoreArray.effective_lens``'s own
hot loop, same reasoning as ``COMPRESS_TYPE_BY_VALUE`` above, one level
further — that loop never needs the ``CompressType`` member itself, only to
compare against these two particular values."""


@dataclasses.dataclass(frozen=True)
class BucketFileHeader:
    """Parsed ``.buk`` header (FORMAT-SPEC.md: bucket-header)."""

    major: int
    minor: int
    mode: int
    chunk_num: int
    chunk_size_crc: int
    crc_of_chunk_crc: int
    """CRC32 over the whole ChunkCrcStore trailer (FORMAT-SPEC.md:
    ChunkCrcStore) — not needed to read a chunk's content; a ``verify``
    caller checks it against ``chunk_crc_store_region``'s bytes."""

    @property
    def is_compressed(self) -> bool:
        return bool(self.mode & MODE_COMPRESS)

    @property
    def is_vault_encrypted(self) -> bool:
        """The only reliable signal that this bucket's chunks are
        encrypted — never ``repo_info``'s ``encrypt_algorithm``, which is a
        compile-time constant on ABP builds and unrelated to any individual
        bucket's actual state."""
        return bool(self.mode & MODE_VAULT_ENCRYPT)


def parse_bucket_header(data: bytes) -> BucketFileHeader:
    """Parse a ``.buk`` file's 64-byte header from the start of ``data``."""
    header = parse_index_header(data, expect_magic=MAGIC["bucket"], max_major=_MAJOR_VAULT, spec=_SPEC)
    mode, chunk_num, chunk_size_crc = struct.unpack(">III", data[_OFF_MODE : _OFF_MODE + 12])
    (crc_of_chunk_crc,) = struct.unpack(">I", data[_OFF_CRC_OF_CHUNK_CRC : _OFF_CRC_OF_CHUNK_CRC + 4])
    return BucketFileHeader(
        major=header.major,
        minor=header.minor,
        mode=mode,
        chunk_num=chunk_num,
        chunk_size_crc=chunk_size_crc,
        crc_of_chunk_crc=crc_of_chunk_crc,
    )


class SizeStoreEntry(NamedTuple):
    """One chunk's compression type and *stored* (compressed) length, as
    recorded in ``SizeStore`` (FORMAT-SPEC.md: SizeStore).

    ``NamedTuple``, not this project's usual ``@dataclass(frozen=True)`` —
    a deliberate exception: constructed up to 8192 times per bucket, cheaper
    than dataclass construction at that volume. ``parse_size_store`` hands
    these out lazily via ``_SizeStoreArray`` rather than building every
    entry up front.
    """

    compress_type: CompressType
    stored_len: int
    """The raw 12-bit size field. Not the same as ``effective_len``."""

    @property
    def effective_len(self) -> int:
        """Actual bytes this chunk occupies in the data region. ``4096``
        for ``CompressType.NONE`` (``stored_len`` is meaningless there),
        ``0`` for ``CompressType.COMPACTED``, otherwise ``stored_len``
        verbatim."""
        if self.compress_type is CompressType.NONE:
            return FIXED_CHUNK_LENGTH
        if self.compress_type is CompressType.COMPACTED:
            return 0
        return self.stored_len


_E = TypeVar("_E")


class _ArrayBackedSequence(Sequence[_E]):
    """Shared ``Sequence`` boilerplate for ``_SizeStoreArray``/
    ``_ChunkLocatorArray``: both lazily build a real element only for the
    index actually asked for, over a handful of parallel ``array.array``
    columns — identical ``__len__``/slice-handling either way, differing
    only in what one index actually builds. Subclasses supply ``_length``
    (each has its own differently-named backing columns) and ``_build``.
    """

    __slots__ = ()

    def __len__(self) -> int:
        return self._length()

    @overload
    def __getitem__(self, index: int) -> _E: ...
    @overload
    def __getitem__(self, index: slice) -> list[_E]: ...

    def __getitem__(self, index: int | slice) -> _E | list[_E]:
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        return self._build(index)

    def _length(self) -> int:
        raise NotImplementedError  # pragma: no cover - every real subclass overrides this

    def _build(self, index: int) -> _E:
        raise NotImplementedError  # pragma: no cover - every real subclass overrides this


class _SizeStoreArray(_ArrayBackedSequence[SizeStoreEntry]):
    """``array.array``-backed substitute for ``list[SizeStoreEntry]``,
    ``parse_size_store``'s actual return type. ``__getitem__`` builds a
    real ``SizeStoreEntry`` only for the index actually asked for, so a
    caller touching only a handful of a bucket's up-to-8192 chunks pays
    construction cost only for what it reads, not the whole bucket up
    front. ``effective_lens`` gives ``chunk_locators``/
    ``expected_bucket_size`` the same array-native fast path without
    constructing any ``SizeStoreEntry`` at all.
    """

    __slots__ = ("_compress_types", "_stored_lens")

    def __init__(self, compress_types: array.array[int], stored_lens: array.array[int]) -> None:
        self._compress_types = compress_types
        self._stored_lens = stored_lens

    def _length(self) -> int:
        return len(self._compress_types)

    def _build(self, index: int) -> SizeStoreEntry:
        return SizeStoreEntry(COMPRESS_TYPE_BY_VALUE[self._compress_types[index]], self._stored_lens[index])

    def effective_lens(self) -> array.array[int]:
        """Every chunk's ``SizeStoreEntry.effective_len``, computed
        straight off the flat arrays — the array-native fast path
        ``chunk_locators``/``expected_bucket_size`` use.

        Returns:
            An unsigned-short (``"H"``) array — every value is at most
            ``FIXED_CHUNK_LENGTH`` (4096), well inside its 65535 ceiling.
        """
        n = len(self._compress_types)
        out = array.array("H", bytes(2 * n))
        compress_types, stored_lens = self._compress_types, self._stored_lens
        for i in range(n):
            ctype = compress_types[i]
            if ctype == _COMPRESS_TYPE_NONE_VALUE:
                out[i] = FIXED_CHUNK_LENGTH
            elif ctype == COMPRESS_TYPE_COMPACTED_VALUE:
                out[i] = 0
            else:
                out[i] = stored_lens[i]
        return out

    def raw_compress_types(self) -> array.array[int]:
        """The raw per-chunk ``CompressType`` values, zero-copy — for a
        caller that needs O(1) access without constructing a single
        ``SizeStoreEntry``. See ``raw_chunk_arrays``, the intended way to
        reach this."""
        return self._compress_types


def chunk_size_store_tight_length(chunk_num: int) -> int:
    """``ceil(chunk_num * 15 / 8)``, the tightly-packed SizeStore length in
    bytes, before zero-padding to the fixed 16320-byte on-disk allocation."""
    return (chunk_num * SIZE_STORE_REC_BIT_NUM + 7) >> 3


_SIZE_STORE_GROUP_RECORDS = 8
"""8 consecutive 15-bit records pack into exactly 120 bits, 15 whole bytes,
no overlap into the next group of 8, since
``lcm(SIZE_STORE_REC_BIT_NUM, 8) / SIZE_STORE_REC_BIT_NUM == 8`` — tied to
the current ``SIZE_STORE_REC_BIT_NUM == 15`` by the assertion right below,
so a future spec change to that constant fails loudly here instead of
silently decoding wrong."""
_SIZE_STORE_GROUP_BYTES = 15
_SIZE_STORE_GROUP_BITS = _SIZE_STORE_GROUP_BYTES * 8
assert _SIZE_STORE_GROUP_RECORDS * SIZE_STORE_REC_BIT_NUM == _SIZE_STORE_GROUP_BITS, (
    "_SIZE_STORE_GROUP_RECORDS/_SIZE_STORE_GROUP_BYTES assume SIZE_STORE_REC_BIT_NUM == 15 exactly"
)


def _decode_size_store_group(
    padded: bytes,
    byte_pos: int,
    count: int,
    chunk_idx: int,
    compress_types: array.array[int],
    stored_lens: array.array[int],
) -> int:
    """Decode ``count`` (``<= _SIZE_STORE_GROUP_RECORDS``) SizeStore records
    packed into the 15-byte group at ``padded[byte_pos:]``, writing into
    ``compress_types``/``stored_lens`` starting at ``chunk_idx`` (see
    ``_SIZE_STORE_GROUP_RECORDS`` for the bit-packing this relies on).
    Shared by ``parse_size_store``'s full-group and
    remainder-group loops, which differ only in ``count``.

    Returns:
        The next ``chunk_idx``.
    """
    group = int.from_bytes(padded[byte_pos : byte_pos + _SIZE_STORE_GROUP_BYTES], "big")
    for i in range(count):
        val = (group >> (_SIZE_STORE_GROUP_BITS - (i + 1) * SIZE_STORE_REC_BIT_NUM)) & 0x7FFF
        type_value = (val >> 12) & 0x7
        if type_value not in COMPRESS_TYPE_BY_VALUE:
            raise DataCorruptError(f"unknown CompressType {type_value} at chunk {chunk_idx}", spec=_SPEC)
        compress_types[chunk_idx] = type_value
        stored_lens[chunk_idx] = val & 0xFFF
        chunk_idx += 1
    return chunk_idx


def parse_size_store(data: bytes, chunk_num: int, *, verify_crc: int | None = None) -> _SizeStoreArray:
    """Decode ``chunk_num`` 15-bit packed SizeStore records starting at the
    beginning of ``data`` (FORMAT-SPEC.md: SizeStore, the bucket's SizeStore
    region, bytes ``[64, 16384)``); only the first
    ``chunk_size_store_tight_length`` bytes plus a little slack are
    actually read.

    Decodes ``_SIZE_STORE_GROUP_RECORDS`` (8) records at a time (see that
    constant for the bit-packing this relies on). Each record's decoded
    ``compress_type``/``stored_len`` still gets eager
    validation (raising ``DataCorruptError`` immediately for an unknown
    ``CompressType``) — only building a ``SizeStoreEntry`` object is
    deferred, to ``_SizeStoreArray``'s own ``__getitem__``.

    Args:
        data: Bytes starting at the SizeStore region.
        chunk_num: Number of records to decode, from the bucket header.
        verify_crc: The header's ``chunkSizeCrc`` field. When given, the
            tightly-packed bytes' CRC32 is checked against it — every
            open validates this.

    Raises:
        FormatError: ``data`` is shorter than the tightly-packed region
            ``chunk_num`` implies.
        DataCorruptError: ``verify_crc`` was given and does not match, or a
            decoded record's ``CompressType`` value is unknown.
    """
    tight_len = chunk_size_store_tight_length(chunk_num)
    if len(data) < tight_len:
        raise FormatError(f"SizeStore data too short: {len(data)} bytes < {tight_len}", spec=_SPEC)
    tight = data[:tight_len]

    if verify_crc is not None:
        verify_crc32(tight, verify_crc, label="SizeStore", spec=_SPEC)

    # Pad a full group's worth so the last (possibly partial) group's own
    # int.from_bytes() read never runs past the tight region.
    padded = tight + bytes(_SIZE_STORE_GROUP_BYTES)

    compress_types = array.array("B", bytes(chunk_num))
    stored_lens = array.array("H", bytes(2 * chunk_num))

    full_groups, remainder = divmod(chunk_num, _SIZE_STORE_GROUP_RECORDS)
    chunk_idx = 0
    byte_pos = 0
    for _ in range(full_groups):
        chunk_idx = _decode_size_store_group(
            padded, byte_pos, _SIZE_STORE_GROUP_RECORDS, chunk_idx, compress_types, stored_lens
        )
        byte_pos += _SIZE_STORE_GROUP_BYTES
    if remainder:
        chunk_idx = _decode_size_store_group(padded, byte_pos, remainder, chunk_idx, compress_types, stored_lens)
    return _SizeStoreArray(compress_types, stored_lens)


class ChunkLocator(NamedTuple):
    """Absolute file byte-range for one chunk's (still compressed and/or
    encrypted) data.

    Same deliberate ``NamedTuple``-not-``@dataclass(frozen=True)`` exception
    as ``SizeStoreEntry`` above — see there for the reasoning.

    ``chunk_locators`` hands these out lazily too, via
    ``_ChunkLocatorArray``, when it's fed a ``_SizeStoreArray``: a real
    ``ChunkLocator`` is built only for the index actually asked for.
    """

    offset: int
    length: int


class _ChunkLocatorArray(_ArrayBackedSequence[ChunkLocator]):
    """``array.array``-backed substitute for ``list[ChunkLocator]``, the
    other half of ``_SizeStoreArray``'s construction-cost saving:
    ``chunk_locators`` returns one of
    these instead of a ``list[ChunkLocator]`` when its ``entries`` argument
    is a ``_SizeStoreArray``, so the cumulative ``offset`` pass never
    constructs a ``ChunkLocator`` for a chunk nobody ends up asking for
    either."""

    __slots__ = ("_offsets", "_lengths")

    def __init__(self, offsets: array.array[int], lengths: array.array[int]) -> None:
        self._offsets = offsets
        self._lengths = lengths

    def _length(self) -> int:
        return len(self._offsets)

    def _build(self, index: int) -> ChunkLocator:
        return ChunkLocator(offset=self._offsets[index], length=self._lengths[index])

    def raw_offsets(self) -> array.array[int]:
        """The raw per-chunk ``offset`` values, zero-copy — see
        ``_SizeStoreArray.raw_compress_types`` for why, and
        ``raw_chunk_arrays`` for the intended way to reach this."""
        return self._offsets

    def raw_lengths(self) -> array.array[int]:
        """The raw per-chunk ``length`` values, zero-copy — same as
        ``raw_offsets``."""
        return self._lengths


def chunk_locators(header: BucketFileHeader, entries: Sequence[SizeStoreEntry]) -> Sequence[ChunkLocator]:
    """Absolute file byte-range for every chunk described by ``entries``
    (FORMAT-SPEC.md: bucket-physical-layout).

    ABP builds always use the compressed layout (data starts at
    ``COMPRESS_RESERVED_LENG``, 16384); the uncompressed layout (fixed 4096
    bytes/chunk starting at ``RESERVED_LENG``, 4096) is handled too, but is
    never produced by any current writer.
    """
    if not header.is_compressed:
        return [
            ChunkLocator(offset=RESERVED_LENG + i * FIXED_CHUNK_LENGTH, length=FIXED_CHUNK_LENGTH)
            for i in range(len(entries))
        ]

    if isinstance(entries, _SizeStoreArray):
        # "I": a bucket's compressed region tops out at ~33.6 MB
        # (COMPRESS_RESERVED_LENG + BUCKET_MAX_CHUNK_NUM * FIXED_CHUNK_LENGTH).
        effective_lens = entries.effective_lens()
        offsets = array.array("I", bytes(4 * len(effective_lens)))
        running = COMPRESS_RESERVED_LENG
        for i, length in enumerate(effective_lens):
            offsets[i] = running
            running += length
        return _ChunkLocatorArray(offsets, effective_lens)

    locators: list[ChunkLocator] = []
    running = 0
    for entry in entries:
        length = entry.effective_len
        locators.append(ChunkLocator(offset=COMPRESS_RESERVED_LENG + running, length=length))
        running += length
    return locators


def raw_chunk_arrays(
    header: BucketFileHeader, entries: Sequence[SizeStoreEntry], locators: Sequence[ChunkLocator]
) -> tuple[array.array[int], array.array[int], array.array[int]]:
    """``(compress_type_values, offsets, lengths)`` as three flat
    ``array.array`` buffers — O(1) per-chunk access to every field
    ``SizeStoreEntry``/``ChunkLocator`` carry without constructing either
    object.

    Zero-copy whenever ``header.is_compressed`` — every current writer's
    layout — returning the exact arrays ``parse_size_store``/
    ``chunk_locators`` already built; the ``assert`` below is a fail-fast
    check that ``entries``/``locators`` actually came from that branch. The
    uncompressed layout instead gets a formulaic fill (every chunk is
    ``CompressType.NONE`` at a fixed ``FIXED_CHUNK_LENGTH`` stride from
    ``RESERVED_LENG``), with nothing to read from ``entries``/``locators``
    at all.
    """
    if not header.is_compressed:
        chunk_num = len(entries)
        return (
            array.array("B", bytes(chunk_num)),
            array.array("I", (RESERVED_LENG + i * FIXED_CHUNK_LENGTH for i in range(chunk_num))),
            array.array("H", (FIXED_CHUNK_LENGTH for _ in range(chunk_num))),
        )
    assert isinstance(entries, _SizeStoreArray) and isinstance(locators, _ChunkLocatorArray), (
        "a compressed-layout header's entries/locators always come from parse_size_store()/chunk_locators()'s "
        "own array fast path"
    )
    return entries.raw_compress_types(), locators.raw_offsets(), locators.raw_lengths()


def _effective_totals(entries: Sequence[SizeStoreEntry]) -> tuple[int, int]:
    """``(total_effective_len, non_empty_chunk_count)`` over ``entries`` —
    shared by ``expected_bucket_size`` and ``chunk_crc_store_region``, both
    of which need the same cumulative-length/non-empty-count pass.

    The exact definition of "non-empty" for the ``CompressType.COMPACTED``
    case is ambiguous in the spec — ``chunk_num`` verbatim vs.
    ``effective_len > 0`` are indistinguishable without a real Compacted
    chunk to test against. This takes the spec-literal reading: one entry
    per chunk whose ``effective_len > 0``. Callers (``dedup/verify_checks.py``) treat a
    mismatch stemming from this ambiguity as a warning, not a hard
    failure, until real data exercising compaction settles it.
    """
    if isinstance(entries, _SizeStoreArray):
        effective_lens = entries.effective_lens()
        return sum(effective_lens), sum(1 for length in effective_lens if length > 0)
    return (
        sum(entry.effective_len for entry in entries),
        sum(1 for entry in entries if entry.effective_len > 0),
    )


def expected_bucket_size(header: BucketFileHeader, entries: Sequence[SizeStoreEntry]) -> int:
    """Self-check total on-disk file size.

    ``COMPRESS_RESERVED_LENG + Σ effective_len + 4×non-empty-chunks +
    redundancy_size(tight_sizestore_len, 256)``.

    Raises:
        ValueError: ``header`` is not the compressed (ABP) layout.
    """
    if not header.is_compressed:
        raise ValueError("expected_bucket_size only applies to the compressed (ABP) layout")
    total_effective, non_empty = _effective_totals(entries)
    tight_len = chunk_size_store_tight_length(header.chunk_num)
    trailer = CHUNK_CRC_SIZE * non_empty + redundancy_size(tight_len, REDUNDANCY_COVERAGE_BUCKET)
    return COMPRESS_RESERVED_LENG + total_effective + trailer


def chunk_crc_store_region(header: BucketFileHeader, entries: Sequence[SizeStoreEntry]) -> tuple[int, int]:
    """Byte ``(offset, length)`` of the ChunkCrcStore trailer (FORMAT-SPEC.md:
    ChunkCrcStore) within the bucket file — immediately after the chunk-data
    region, one 4-byte entry per non-empty chunk.

    Raises:
        ValueError: ``header`` is not the compressed (ABP) layout.
    """
    if not header.is_compressed:
        raise ValueError("chunk_crc_store_region only applies to the compressed (ABP) layout")
    total_effective, non_empty = _effective_totals(entries)
    return COMPRESS_RESERVED_LENG + total_effective, CHUNK_CRC_SIZE * non_empty


def chunk_crc_store_index(entries: Sequence[SizeStoreEntry], chunk_idx: int) -> int:
    """Position of chunk ``chunk_idx`` within the ChunkCrcStore trailer —
    the count of non-empty (``effective_len > 0``) chunks at indices below
    it, since a COMPACTED chunk has no trailer entry of its own at all.

    O(``chunk_idx``) per call — correct but quadratic if called once per
    chunk for every chunk in a bucket, since a real bucket can hold
    thousands of chunks. A one-off single-chunk lookup (the only current
    use, ``verify``'s spot-checks) is fine; a caller resolving *every*
    chunk in a bucket wants ``chunk_crc_store_positions`` instead.
    """
    if isinstance(entries, _SizeStoreArray):
        return sum(1 for length in entries.effective_lens()[:chunk_idx] if length > 0)
    return sum(1 for entry in entries[:chunk_idx] if entry.effective_len > 0)


def chunk_crc_store_positions(entries: Sequence[SizeStoreEntry]) -> array.array[int]:
    """Every chunk's own ChunkCrcStore position, computed in one O(n)
    cumulative-count pass — the batch counterpart to
    ``chunk_crc_store_index``'s O(1)-but-called-once-per-chunk-is-O(n²)
    single lookup. A COMPACTED chunk's own slot holds whatever position
    the *next* non-empty chunk would get (it has no real entry of its
    own; never read for one)."""
    if isinstance(entries, _SizeStoreArray):
        effective_lens: Sequence[int] = entries.effective_lens()
    else:
        effective_lens = [entry.effective_len for entry in entries]
    positions = array.array("I", bytes(4 * len(effective_lens)))
    running = 0
    for i, length in enumerate(effective_lens):
        positions[i] = running
        if length > 0:
            running += 1
    return positions


def parse_chunk_crc_store(data: bytes, non_empty: int, *, verify_crc: int | None = None) -> tuple[int, ...]:
    """Decode the ChunkCrcStore trailer: one big-endian 4-byte ciphertext
    CRC32 per non-empty chunk (FORMAT-SPEC.md: ChunkCrcStore), computed at
    write time over each chunk's *stored* bytes (after compression and
    encryption, if any) — not a plaintext check.

    Args:
        data: Bytes starting at ``chunk_crc_store_region``'s offset; only
            the first ``CHUNK_CRC_SIZE * non_empty`` bytes are read.
        non_empty: Number of entries, from ``chunk_crc_store_region``.
        verify_crc: The header's ``crc_of_chunk_crc`` field. When given,
            the whole trailer's own CRC32 is checked against it — this is
            a self-consistency check of the trailer bytes, not a check of
            any individual chunk's stored data against its own entry here.

    Raises:
        FormatError: ``data`` is shorter than the trailer needs.
        DataCorruptError: ``verify_crc`` was given and does not match.
    """
    length = CHUNK_CRC_SIZE * non_empty
    if len(data) < length:
        raise FormatError(f"ChunkCrcStore trailer too short: {len(data)} bytes < {length}", spec=_SPEC)
    trailer = data[:length]
    if verify_crc is not None:
        verify_crc32(trailer, verify_crc, label="ChunkCrcStore", spec=_SPEC)
    return struct.unpack(f">{non_empty}I", trailer) if non_empty else ()
