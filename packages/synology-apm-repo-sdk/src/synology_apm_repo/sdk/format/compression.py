"""Chunk decompression dispatch (FORMAT-SPEC.md: SizeStore).

**Decrypt before decompress** — ciphertext length equals compressed length;
decompression happens on the plaintext side. This module
only decompresses; ``crypto`` only decrypts; callers sequence the two.
"""

from __future__ import annotations

import enum
import io
import struct
import threading
from collections.abc import Sequence

import lz4.block
import zstandard

from ..errors import ChunkCompactedError, DataCorruptError
from .const import FIXED_CHUNK_LENGTH

_SPEC = "FORMAT-SPEC.md: SizeStore"

ZSTD_FRAME_MAGIC = b"\x28\xb5\x2f\xfd"
"""Standard ZSTD frame magic — the strongest offline signal that a
candidate vault key is correct for a ZSTD chunk (FORMAT-SPEC.md: chunk-pool-encryption).
Unlike LZ4's unmagicked block format, decrypting with the wrong key and
getting something that both matches this magic and decompresses cleanly
is negligibly unlikely."""

_ZSTD_STREAM_READ_SIZE = 1 << 20
"""Bytes read per streaming-reader iteration in ``decompress_zstd_stream``'s
bounded path — bounds one iteration's own work while decompressing an
oversized/hostile frame, not a limit on total output (that's
``max_output_size`` itself)."""


class CompressType(enum.Enum):
    """Per-chunk compression type, as recorded in ``SizeStore`` (FORMAT-SPEC.md: SizeStore).
    Values match the on-disk encoding exactly — note 3 is skipped."""

    NONE = 0
    LZ4 = 1
    ZSTD = 2
    COMPACTED = 4


_thread_local = threading.local()


def _zstd_decompressor() -> zstandard.ZstdDecompressor:
    """One ``ZstdDecompressor`` per OS thread, reused for that thread's whole
    lifetime — a ``ZstdDecompressor`` holds a live decode context that
    can't be shared across the several real OS threads decode may run on.
    """
    dctx = getattr(_thread_local, "zstd_decompressor", None)
    if dctx is None:
        dctx = zstandard.ZstdDecompressor()
        _thread_local.zstd_decompressor = dctx
    return dctx


def decompress(ctype: CompressType, data: bytes | memoryview) -> bytes:
    """Decompress one chunk's ciphertext-removed bytes back to exactly
    ``FIXED_CHUNK_LENGTH`` (4096) bytes of plaintext.

    Single-chunk path — see ``decompress_many`` for the batched form.
    ``data`` accepts a ``memoryview`` as well as ``bytes`` — like
    ``decompress_many``, so a caller slicing a chunk out of its own larger
    read buffer can pass a view through without copying it first; only the
    ``NONE`` case below (no real decompression happening at all) needs an
    explicit copy to still return real ``bytes`` as promised.

    Raises:
        ChunkCompactedError: For ``CompressType.COMPACTED`` (unrecoverable,
            reclaimed by compaction).
        DataCorruptError: Decompression succeeds but yields anything other
            than exactly 4096 bytes.
    """
    match ctype:
        case CompressType.COMPACTED:
            raise ChunkCompactedError("chunk is COMPACTED — reclaimed by compaction, cannot be recovered", spec=_SPEC)
        case CompressType.NONE:
            plain = bytes(data)
        case CompressType.LZ4:
            plain = lz4.block.decompress(data, uncompressed_size=FIXED_CHUNK_LENGTH)
        case CompressType.ZSTD:
            # Bounded streaming read, not the one-shot decompress(...): see
            # decompress_zstd_stream for why the one-shot API isn't safe here.
            with _zstd_decompressor().stream_reader(io.BytesIO(data)) as reader:
                plain = reader.read(FIXED_CHUNK_LENGTH + 1)
        case _:  # pragma: no cover - CompressType is exhaustive above
            raise DataCorruptError(f"unknown CompressType {ctype}", spec=_SPEC)

    if len(plain) != FIXED_CHUNK_LENGTH:
        raise DataCorruptError(
            f"decompressed chunk is {len(plain)} bytes, expected {FIXED_CHUNK_LENGTH}",
            spec=_SPEC,
        )
    return plain


_ZSTD_SIZE_ENTRY = struct.pack("<Q", FIXED_CHUNK_LENGTH)
"""One ``decompressed_sizes`` entry for ``multi_decompress_to_buffer`` —
every chunk decompresses to exactly ``FIXED_CHUNK_LENGTH``, so the whole
array for an n-chunk batch is just this repeated n times."""

_ZSTD_BATCH_THREADS = 4
_ZSTD_BATCH_THREADS_MIN_ENTRIES = 500
"""Below this many ZSTD frames in one ``multi_decompress_to_buffer`` call,
spinning up ``_ZSTD_BATCH_THREADS`` real threads costs more than it
saves; ``threads=1`` alone already beats one-chunk-at-a-time decompression
purely from batching the call overhead. A merged run can reach a whole
bucket's worth of chunks (``BUCKET_MAX_CHUNK_NUM``, 8192), well past this
threshold — ``threads=4`` above it extrapolates the same trend to larger
batches."""


def decompress_many(items: Sequence[tuple[CompressType, bytes | memoryview]]) -> list[bytes | memoryview]:
    """Batch form of ``decompress``: decompresses a whole sequence of
    ``(compress_type, ciphertext-removed data)`` pairs in one pass,
    preserving input order. ``data`` accepts a ``memoryview`` as well as
    ``bytes`` so a caller slicing chunks out of its own larger read buffer
    can pass a view through without copying it first.

    **A ZSTD entry's returned ``memoryview`` is a view into that call's own
    shared decode buffer, not an independent copy** — deliberately: copying
    every frame out into its own ``bytes`` object is the single most
    expensive step in the whole decode path. **Tradeoff**: keeping a
    returned ``memoryview`` alive keeps the whole batch's shared decode
    buffer alive too — up to
    one merged run's worth of chunks, not just that one chunk's 4096 bytes
    — so copy out via ``bytes(x)`` at any point one chunk needs to outlive
    this call.

    This is the batch decode entry point ``BucketReader._decode_run`` uses.
    """
    result: list[bytes | memoryview] = [b""] * len(items)
    zstd_positions: list[int] = []
    zstd_data: list[bytes | bytearray | memoryview] = []

    for i, (ctype, data) in enumerate(items):
        match ctype:
            case CompressType.COMPACTED:
                raise ChunkCompactedError(
                    "chunk is COMPACTED — reclaimed by compaction, cannot be recovered", spec=_SPEC
                )
            case CompressType.NONE:
                # No compression happened — data (bytes or a caller's own view)
                # is already the plaintext.
                result[i] = data
            case CompressType.LZ4:
                # No batch primitive exists in the lz4 binding this project
                # uses, and LZ4 chunks are rare enough in real data that a
                # hand-rolled batch path isn't worth it.
                result[i] = lz4.block.decompress(data, uncompressed_size=FIXED_CHUNK_LENGTH)
            case CompressType.ZSTD:
                zstd_positions.append(i)
                zstd_data.append(data)
            case _:  # pragma: no cover - CompressType is exhaustive above
                raise DataCorruptError(f"unknown CompressType {ctype}", spec=_SPEC)

    if zstd_data:
        threads = _ZSTD_BATCH_THREADS if len(zstd_data) >= _ZSTD_BATCH_THREADS_MIN_ENTRIES else 1
        try:
            decoded = _zstd_decompressor().multi_decompress_to_buffer(
                zstd_data, decompressed_sizes=_ZSTD_SIZE_ENTRY * len(zstd_data), threads=threads
            )
        except zstandard.ZstdError as exc:
            # Unlike decompress()'s single-chunk max_output_size (a cap —
            # short output comes back silently and the length check below
            # catches it), multi_decompress_to_buffer's decompressed_sizes
            # is an exact expectation: any mismatch raises here instead of
            # returning short data. Translated to DataCorruptError for the same
            # exception type either path gives a caller.
            raise DataCorruptError(f"batch zstd decompress failed: {exc}", spec=_SPEC) from exc
        for pos, i in enumerate(zstd_positions):
            # zstandard's stub doesn't declare BufferSegment as a Buffer,
            # but it genuinely supports the protocol. memoryview, not a
            # bare BufferSegment, so equality is content-correct —
            # BufferSegment.__eq__ falls back to identity and silently
            # returns False even for matching content.
            result[i] = memoryview(decoded[pos])  # type: ignore[arg-type]

    for i, plain in enumerate(result):
        if len(plain) != FIXED_CHUNK_LENGTH:
            raise DataCorruptError(
                f"decompressed chunk at position {i} is {len(plain)} bytes, expected {FIXED_CHUNK_LENGTH}",
                spec=_SPEC,
            )
    return result


def decompress_zstd_stream(data: bytes, *, max_output_size: int | None = None) -> bytes:
    """Decompress a whole ZSTD frame (e.g. ``version.db.zst`` or a
    service-level-DB snapshot object), as opposed to a single
    fixed-4096-byte chunk.

    ``max_output_size``, when given, bounds how much the decompressor
    produces before giving up (``zstandard.ZstdError``) — for content of an
    *unconfirmed* type, a cap turns "this wasn't really zstd-framed" into a
    fast failure instead of decompressing a hostile oversized frame in
    full. Omit it for a source already known to be a trusted db snapshot.

    Always reads incrementally through the streaming reader, checking the
    running total ourselves, rather than ``zstandard``'s one-shot
    ``ZstdDecompressor.decompress(data, max_output_size=N)``: that API only
    enforces the cap when the frame does *not* declare its own decompressed
    size in its header — a frame that does (the common case) is decompressed
    in full regardless, silently ignoring it. The frame's own declared size
    is never trusted either way.
    """
    decompressor = zstandard.ZstdDecompressor()
    with decompressor.stream_reader(io.BytesIO(data)) as reader:
        if max_output_size is None:
            result: bytes = reader.read()
            return result
        chunks: list[bytes] = []
        total = 0
        while True:
            piece = reader.read(_ZSTD_STREAM_READ_SIZE)
            if not piece:
                break
            total += len(piece)
            if total > max_output_size:
                raise zstandard.ZstdError(f"decompressed output exceeds max_output_size ({max_output_size} bytes)")
            chunks.append(piece)
        return b"".join(chunks)
