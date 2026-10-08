"""Chunk decompression dispatch (FORMAT-SPEC.md: SizeStore).

**Decrypt before decompress**: ciphertext length equals compressed length.
This module only decompresses; ``crypto`` only decrypts; callers sequence
the two.
"""

from __future__ import annotations

import enum
import io
import struct
import threading
from collections.abc import Iterator, Sequence

import lz4.block
import zstandard

from ..errors import ChunkCompactedError, DataCorruptError
from .const import FIXED_CHUNK_LENGTH

_SPEC = "FORMAT-SPEC.md: SizeStore"

ZSTD_FRAME_MAGIC = b"\x28\xb5\x2f\xfd"
"""Standard ZSTD frame magic, the first 4 bytes of every ZSTD frame (LZ4
blocks carry none)."""

_ZSTD_STREAM_READ_SIZE = 1 << 20
"""Bytes read per streaming-reader iteration in ``iter_decompressed_zstd``;
bounds one iteration's work, not the total output (``max_output_size``)."""


class CompressType(enum.Enum):
    """Per-chunk compression type, as recorded in ``SizeStore``
    (FORMAT-SPEC.md: SizeStore). Values match the on-disk encoding; 3 is
    skipped."""

    NONE = 0
    LZ4 = 1
    ZSTD = 2
    COMPACTED = 4


_thread_local = threading.local()


def _zstd_decompressor() -> zstandard.ZstdDecompressor:
    """One ``ZstdDecompressor`` per OS thread; its decode context can't be
    shared across threads."""
    dctx = getattr(_thread_local, "zstd_decompressor", None)
    if dctx is None:
        dctx = zstandard.ZstdDecompressor()
        _thread_local.zstd_decompressor = dctx
    return dctx


def _lz4_decompress(data: bytes | memoryview) -> bytes:
    try:
        plain: bytes = lz4.block.decompress(data, uncompressed_size=FIXED_CHUNK_LENGTH)
        return plain
    except lz4.block.LZ4BlockError as exc:
        raise DataCorruptError(f"lz4 decompress failed: {exc}", spec=_SPEC) from exc


def decompress(ctype: CompressType, data: bytes | memoryview) -> bytes:
    """Decompress one chunk's decrypted stored bytes back to exactly
    ``FIXED_CHUNK_LENGTH`` (4096) bytes of plaintext.

    Single-chunk path; see ``decompress_many`` for the batched form. ``data``
    may be a ``memoryview`` (copied only for ``NONE``).

    Raises:
        ChunkCompactedError: For ``CompressType.COMPACTED`` (unrecoverable,
            reclaimed by compaction).
        DataCorruptError: The chunk doesn't decompress, or not to exactly
            4096 bytes.
    """
    match ctype:
        case CompressType.COMPACTED:
            raise ChunkCompactedError("chunk is COMPACTED — reclaimed by compaction, cannot be recovered", spec=_SPEC)
        case CompressType.NONE:
            plain = bytes(data)
        case CompressType.LZ4:
            plain = _lz4_decompress(data)
        case CompressType.ZSTD:
            # Bounded streaming read: the one-shot API ignores its cap when the
            # frame declares a size (see iter_decompressed_zstd).
            try:
                with _zstd_decompressor().stream_reader(io.BytesIO(data)) as reader:
                    plain = reader.read(FIXED_CHUNK_LENGTH + 1)
            except zstandard.ZstdError as exc:
                raise DataCorruptError(f"zstd decompress failed: {exc}", spec=_SPEC) from exc
        case _:  # pragma: no cover - CompressType is exhaustive above
            raise DataCorruptError(f"unknown CompressType {ctype}", spec=_SPEC)

    if len(plain) != FIXED_CHUNK_LENGTH:
        raise DataCorruptError(
            f"decompressed chunk is {len(plain)} bytes, expected {FIXED_CHUNK_LENGTH}",
            spec=_SPEC,
        )
    return plain


_ZSTD_SIZE_ENTRY = struct.pack("<Q", FIXED_CHUNK_LENGTH)
"""One ``decompressed_sizes`` entry for ``multi_decompress_to_buffer``; an
n-chunk batch repeats it n times."""

_ZSTD_BATCH_THREADS = 4
_ZSTD_BATCH_THREADS_MIN_ENTRIES = 500
"""Below this many ZSTD frames in one ``multi_decompress_to_buffer`` call,
``_ZSTD_BATCH_THREADS`` threads cost more than they save, so ``threads=1``
is used."""


def decompress_many(items: Sequence[tuple[CompressType, bytes | memoryview]]) -> list[bytes | memoryview]:
    """Batch form of ``decompress``: decompresses ``(compress_type, data)``
    pairs in one pass, preserving order. ``data`` may be a ``memoryview``.

    **A ZSTD entry's result is a ``memoryview`` into the call's shared decode
    buffer, not a copy** (copying every frame is the most expensive decode
    step). Keeping one alive keeps the whole batch buffer alive, so copy via
    ``bytes(x)`` if a chunk must outlive the batch. ``NONE`` entries return
    the input as given.

    Raises:
        ChunkCompactedError: An entry is ``COMPACTED``.
        DataCorruptError: A chunk fails to decompress to 4096 bytes.
    """
    result: list[bytes | memoryview] = [b""] * len(items)
    zstd_positions: list[int] = []
    zstd_data: list[bytes | bytearray | memoryview] = []

    for i, (ctype, data) in enumerate(items):
        if ctype is CompressType.ZSTD:
            # The common case; the batch call's decompressed_sizes enforces its length.
            zstd_positions.append(i)
            zstd_data.append(data)
            continue
        match ctype:
            case CompressType.COMPACTED:
                raise ChunkCompactedError(
                    "chunk is COMPACTED — reclaimed by compaction, cannot be recovered", spec=_SPEC
                )
            case CompressType.NONE:
                # Already plaintext (bytes or the caller's view).
                plain = data
            case CompressType.LZ4:
                # The lz4 binding has no batch call, and LZ4 chunks are rare.
                plain = _lz4_decompress(data)
            case _:  # pragma: no cover - CompressType is exhaustive above
                raise DataCorruptError(f"unknown CompressType {ctype}", spec=_SPEC)
        if len(plain) != FIXED_CHUNK_LENGTH:
            raise DataCorruptError(
                f"decompressed chunk at position {i} is {len(plain)} bytes, expected {FIXED_CHUNK_LENGTH}",
                spec=_SPEC,
            )
        result[i] = plain

    if zstd_data:
        threads = _ZSTD_BATCH_THREADS if len(zstd_data) >= _ZSTD_BATCH_THREADS_MIN_ENTRIES else 1
        try:
            decoded = _zstd_decompressor().multi_decompress_to_buffer(
                zstd_data, decompressed_sizes=_ZSTD_SIZE_ENTRY * len(zstd_data), threads=threads
            )
        except (zstandard.ZstdError, ValueError) as exc:
            # decompressed_sizes is an exact expectation, so a mismatch raises
            # here; translated so both paths give callers DataCorruptError.
            # ValueError: every ZSTD entry is empty.
            raise DataCorruptError(f"batch zstd decompress failed: {exc}", spec=_SPEC) from exc
        for pos, i in enumerate(zstd_positions):
            # memoryview, not a bare BufferSegment: BufferSegment.__eq__ is identity-based.
            result[i] = memoryview(decoded[pos])  # type: ignore[arg-type]
    return result


def _declared_content_size(data: bytes | bytearray) -> int | None:
    """The decompressed size ``data``'s zstd frame header declares, or
    ``None`` if it declares none (legitimate). Parses only the header."""
    content_size = zstandard.get_frame_parameters(data).content_size
    return None if content_size == zstandard.CONTENTSIZE_UNKNOWN else content_size


def is_zstd_frame(head: bytes | bytearray) -> bool:
    """Whether ``head`` opens with a standard ZSTD frame's magic number —
    only its first 4 bytes are inspected."""
    return head[:4] == ZSTD_FRAME_MAGIC


def zstd_content_size(data: bytes | bytearray) -> int | None:
    """The decompressed size ``data``'s zstd frame declares, or ``None`` if
    ``data`` lacks the frame magic or the frame declares none. For callers
    that need the ceiling before decompressing.

    Raises:
        zstandard.ZstdError: The frame header is malformed.
    """
    if not is_zstd_frame(data):
        return None
    return _declared_content_size(data)


def decompress_zstd_stream(data: bytes | bytearray, *, max_output_size: int | None) -> bytes:
    """Decompress a whole ZSTD frame (e.g. ``version.db.zst`` or a service-DB
    snapshot) into one ``bytes``. A wrapper around ``iter_decompressed_zstd``
    (see it for ``max_output_size``); a caller writing to a file should stream
    that iterator instead of holding the payload in memory.

    Raises:
        DataCorruptError: As ``iter_decompressed_zstd``.
        zstandard.ZstdError: As ``iter_decompressed_zstd``.
    """
    return b"".join(iter_decompressed_zstd(data, max_output_size=max_output_size))


def iter_decompressed_zstd(data: bytes | bytearray, *, max_output_size: int | None) -> Iterator[bytes]:
    """Decompress a whole ZSTD frame incrementally, yielding plaintext pieces.
    Does no I/O; the caller decides where the pieces go.

    ``max_output_size`` is mandatory, with no default, as a
    decompression-bomb guard: pass a byte ceiling, or ``None`` only for a
    source already trusted not to be hostile-sized (see
    ``units/saas/services.py``'s ``open_service_db``). It is the
    fallback ceiling for a frame that declares no size; a declared size
    becomes the ceiling instead. ``None`` with no declared size bounds
    nothing.

    Reads through the streaming reader and counts the total itself, because
    ``ZstdDecompressor.decompress(max_output_size=...)`` ignores its cap when
    the frame declares a size. A declared size is also verified afterward,
    since a truncated frame can decode with fewer bytes than declared.

    Raises:
        DataCorruptError: The frame declares a content size that doesn't
            match what decompression produced.
        zstandard.ZstdError: The output exceeds the effective ceiling, or
            the frame is malformed.
    """
    declared = _declared_content_size(data)
    effective_max = declared if declared is not None else max_output_size
    decompressor = zstandard.ZstdDecompressor()
    total = 0
    with decompressor.stream_reader(io.BytesIO(data)) as reader:
        while True:
            piece = reader.read(_ZSTD_STREAM_READ_SIZE)
            if not piece:
                break
            total += len(piece)
            if effective_max is not None and total > effective_max:
                raise zstandard.ZstdError(f"decompressed output exceeds max_output_size ({effective_max} bytes)")
            yield piece
    if declared is not None and total != declared:
        # No spec= citation: this function serves several sources
        # (version.db.zst, SaaS service-DB snapshots, target.db), and _SPEC names SizeStore.
        raise DataCorruptError(
            f"zstd frame declared a decompressed size of {declared} bytes, actually produced {total}"
        )
