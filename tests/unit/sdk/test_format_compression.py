"""Unit tests for ``synology_apm_repo.sdk.format.compression``."""

from __future__ import annotations

import io

import lz4.block
import pytest
import zstandard

from synology_apm_repo.sdk.errors import ChunkCompactedError, DataCorruptError
from synology_apm_repo.sdk.format.compression import (
    _ZSTD_BATCH_THREADS_MIN_ENTRIES,
    ZSTD_FRAME_MAGIC,
    CompressType,
    decompress,
    decompress_many,
    decompress_zstd_stream,
)

_PLAINTEXT = (b"hello dedup world! " * 250)[:4096]
assert len(_PLAINTEXT) == 4096


def test_none_is_identity() -> None:
    assert decompress(CompressType.NONE, _PLAINTEXT) == _PLAINTEXT


def test_none_wrong_length_raises_data_corrupt() -> None:
    with pytest.raises(DataCorruptError):
        decompress(CompressType.NONE, _PLAINTEXT[:100])


def test_lz4_round_trip() -> None:
    compressed = lz4.block.compress(_PLAINTEXT, store_size=False)
    assert decompress(CompressType.LZ4, compressed) == _PLAINTEXT


def test_zstd_round_trip() -> None:
    compressed = zstandard.ZstdCompressor().compress(_PLAINTEXT)
    assert compressed[:4] == ZSTD_FRAME_MAGIC
    assert decompress(CompressType.ZSTD, compressed) == _PLAINTEXT


def test_compacted_raises_chunk_compacted() -> None:
    with pytest.raises(ChunkCompactedError):
        decompress(CompressType.COMPACTED, b"")


def test_lz4_corrupt_input_raises_lz4_block_error() -> None:
    # No try/except wraps the LZ4 branch in decompress() -- confirm
    # empirically what lz4.block.decompress() itself raises for input it
    # can't parse as a valid LZ4 block, rather than assuming it's
    # translated into one of this project's own exception types.
    with pytest.raises(lz4.block.LZ4BlockError):
        decompress(CompressType.LZ4, b"\xff\xff\xff\xff\xff\xff\xff\xff")


def test_zstd_wrong_length_raises_data_corrupt() -> None:
    short_plaintext = b"x" * 100
    compressed = zstandard.ZstdCompressor().compress(short_plaintext)
    with pytest.raises(DataCorruptError):
        decompress(CompressType.ZSTD, compressed)


def test_zstd_oversized_chunk_raises_data_corrupt_without_fully_materializing_it() -> None:
    """A corrupt or hostile bucket chunk whose zstd frame declares far
    more than ``FIXED_CHUNK_LENGTH`` (4096) must be rejected without
    ``decompress()`` first decompressing the whole declared size into
    memory."""
    oversized_plaintext = b"x" * (2 * 1024 * 1024)
    compressed = zstandard.ZstdCompressor().compress(oversized_plaintext)
    with pytest.raises(DataCorruptError):
        decompress(CompressType.ZSTD, compressed)


def test_zstd_branch_never_calls_the_one_shot_decompress_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """``zstandard``'s one-shot ``ZstdDecompressor.decompress(data,
    max_output_size=N)`` silently ignores ``max_output_size`` when the
    frame declares its own content size (the common case), so a hostile
    chunk claiming a huge declared size would decompress in full before
    any length check runs. ``decompress()``'s ZSTD branch must never
    call that one-shot method at all -- only the streaming reader,
    bounded to one ``FIXED_CHUNK_LENGTH + 1``-byte read."""

    def _must_not_be_called(self: zstandard.ZstdDecompressor, data: bytes, *, max_output_size: int = 0) -> bytes:
        raise AssertionError("one-shot decompress() must not be called from the ZSTD branch")

    monkeypatch.setattr(zstandard.ZstdDecompressor, "decompress", _must_not_be_called)

    oversized_plaintext = b"x" * (2 * 1024 * 1024)
    compressed = zstandard.ZstdCompressor().compress(oversized_plaintext)
    with pytest.raises(DataCorruptError):
        decompress(CompressType.ZSTD, compressed)


def test_decompress_zstd_stream_unbounded() -> None:
    big_plaintext = b"y" * (10 * 1024 * 1024)  # far larger than one chunk
    compressed = zstandard.ZstdCompressor().compress(big_plaintext)
    assert decompress_zstd_stream(compressed) == big_plaintext


def test_decompress_zstd_stream_unbounded_handles_a_frame_with_no_declared_content_size() -> None:
    """A zstd frame with no declared content size must still decompress
    correctly via the streaming reader -- the one-shot ``zstandard``
    API's own unbounded mode needs the frame to declare a content size
    and raises otherwise, so ``test_decompress_zstd_stream_unbounded``
    above never exercises this path."""
    plaintext = b"y" * (10 * 1024 * 1024)
    compressed = _compress_without_content_size(plaintext)
    assert zstandard.get_frame_parameters(compressed).content_size == zstandard.CONTENTSIZE_UNKNOWN
    assert decompress_zstd_stream(compressed) == plaintext


def test_decompress_zstd_stream_max_output_size_allows_content_within_the_cap() -> None:
    plaintext = b"z" * 1024
    compressed = zstandard.ZstdCompressor().compress(plaintext)
    assert decompress_zstd_stream(compressed, max_output_size=1024) == plaintext


def test_decompress_zstd_stream_max_output_size_rejects_a_declared_size_frame_over_the_cap() -> None:
    """The realistic case: a normal ``ZstdCompressor().compress()`` frame
    -- the shape every real frame this project reads has -- whose
    declared size exceeds the cap. ``zstandard``'s one-shot
    ``decompress()`` ignores ``max_output_size`` for a frame that
    declares its own content size, so ``decompress_zstd_stream`` never
    calls that API when ``max_output_size`` is given either; it reads
    through the streaming reader and checks the running total itself,
    enforced independent of what the frame's header claims."""
    plaintext = b"z" * (2 * 1024 * 1024)
    compressed = zstandard.ZstdCompressor().compress(plaintext)
    assert zstandard.get_frame_parameters(compressed).content_size == len(plaintext)
    with pytest.raises(zstandard.ZstdError):
        decompress_zstd_stream(compressed, max_output_size=1024)


def test_decompress_zstd_stream_bounded_path_never_calls_the_one_shot_decompress_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verifies the same regression as the declared-size-over-the-cap
    test above by mocking rather than observing the outcome: the bounded
    path must never call ``zstandard``'s own one-shot ``decompress()``
    at all, only the streaming reader -- not merely "it happens to still
    raise correctly"."""

    def _must_not_be_called(self: zstandard.ZstdDecompressor, data: bytes, *, max_output_size: int = 0) -> bytes:
        raise AssertionError("one-shot decompress() must not be called when max_output_size is given")

    monkeypatch.setattr(zstandard.ZstdDecompressor, "decompress", _must_not_be_called)

    plaintext = b"z" * (2 * 1024 * 1024)
    compressed = zstandard.ZstdCompressor().compress(plaintext)
    with pytest.raises(zstandard.ZstdError):
        decompress_zstd_stream(compressed, max_output_size=1024)


def _compress_without_content_size(data: bytes) -> bytes:
    # No declared content size in the frame header — the other real
    # frame shape a caller might see (deliberately, or just differently,
    # constructed), used by both the unbounded and bounded-cap tests
    # below.
    buf = io.BytesIO()
    writer = zstandard.ZstdCompressor(write_content_size=False).stream_writer(buf, closefd=False)
    writer.write(data)
    writer.flush(zstandard.FLUSH_FRAME)
    return buf.getvalue()


def test_decompress_zstd_stream_max_output_size_rejects_an_undeclared_size_frame_over_the_cap() -> None:
    plaintext = b"z" * (2 * 1024 * 1024)
    compressed = _compress_without_content_size(plaintext)
    assert zstandard.get_frame_parameters(compressed).content_size == zstandard.CONTENTSIZE_UNKNOWN
    with pytest.raises(zstandard.ZstdError):
        decompress_zstd_stream(compressed, max_output_size=1024)


def test_compress_type_values_match_spec() -> None:
    # note 3 is deliberately skipped on the wire
    assert CompressType.NONE.value == 0
    assert CompressType.LZ4.value == 1
    assert CompressType.ZSTD.value == 2
    assert CompressType.COMPACTED.value == 4


def _distinct_plaintext(i: int) -> bytes:
    return ((b"plaintext-%d-" % i) * 400)[:4096]


class TestDecompressMany:
    """Batch form of ``decompress`` — one call over a whole sequence
    of ``(compress_type, data)`` pairs instead of one call per chunk. Every
    case is checked against ``decompress`` itself as the oracle: the
    single-chunk function is trusted (covered by the tests above), so
    these only need to prove batching doesn't change *what* comes out,
    just how many calls it takes to get there."""

    def test_all_zstd_matches_decompress_per_chunk(self) -> None:
        plaintexts = [_distinct_plaintext(i) for i in range(5)]
        compressor = zstandard.ZstdCompressor()
        items = [(CompressType.ZSTD, compressor.compress(p)) for p in plaintexts]

        result = decompress_many(items)

        assert result == plaintexts
        assert result == [decompress(ctype, data) for ctype, data in items]

    def test_mixed_types_preserve_order_and_correspondence(self) -> None:
        # NONE, ZSTD, LZ4, ZSTD, NONE — deliberately not grouped by type in
        # the input, so a batching bug that reorders by type instead of
        # reassembling into the original positions would be caught here.
        plaintexts = [_distinct_plaintext(i) for i in range(5)]
        compressor = zstandard.ZstdCompressor()
        items: list[tuple[CompressType, bytes]] = [
            (CompressType.NONE, plaintexts[0]),
            (CompressType.ZSTD, compressor.compress(plaintexts[1])),
            (CompressType.LZ4, lz4.block.compress(plaintexts[2], store_size=False)),
            (CompressType.ZSTD, compressor.compress(plaintexts[3])),
            (CompressType.NONE, plaintexts[4]),
        ]

        result = decompress_many(items)

        assert result == plaintexts
        assert result == [decompress(ctype, data) for ctype, data in items]

    def test_empty_input_returns_empty_list(self) -> None:
        assert decompress_many([]) == []

    def test_compacted_raises_chunk_compacted(self) -> None:
        with pytest.raises(ChunkCompactedError):
            decompress_many(
                [
                    (CompressType.ZSTD, zstandard.ZstdCompressor().compress(_distinct_plaintext(0))),
                    (CompressType.COMPACTED, b""),
                ]
            )

    def test_wrong_length_zstd_raises_data_corrupt(self) -> None:
        short_plaintext = b"x" * 100
        compressed = zstandard.ZstdCompressor().compress(short_plaintext)
        with pytest.raises(DataCorruptError):
            decompress_many([(CompressType.ZSTD, compressed)])

    def test_wrong_length_none_raises_data_corrupt(self) -> None:
        with pytest.raises(DataCorruptError):
            decompress_many(
                [
                    (CompressType.ZSTD, zstandard.ZstdCompressor().compress(_distinct_plaintext(0))),
                    (CompressType.NONE, b"too short"),
                ]
            )

    def test_lz4_corrupt_input_raises_lz4_block_error(self) -> None:
        with pytest.raises(lz4.block.LZ4BlockError):
            decompress_many(
                [
                    (CompressType.ZSTD, zstandard.ZstdCompressor().compress(_distinct_plaintext(0))),
                    (CompressType.LZ4, b"\xff\xff\xff\xff\xff\xff\xff\xff"),
                ]
            )

    def test_accepts_memoryview_input_zero_copy_for_none(self) -> None:
        # A caller slicing chunks straight out of its own larger read
        # buffer can pass memoryview slices through without copying first
        # -- for CompressType.NONE the result is the exact same
        # memoryview object handed in, not a copy.
        plaintexts = [_distinct_plaintext(i) for i in range(2)]
        lz4_compressed = lz4.block.compress(plaintexts[1], store_size=False)
        buf = plaintexts[0] + lz4_compressed
        none_view = memoryview(buf)[: len(plaintexts[0])]
        lz4_view = memoryview(buf)[len(plaintexts[0]) :]

        result = decompress_many([(CompressType.NONE, none_view), (CompressType.LZ4, lz4_view)])

        assert result[0] is none_view
        assert bytes(result[0]) == plaintexts[0]
        assert bytes(result[1]) == plaintexts[1]

    def test_large_all_zstd_batch_crosses_the_multithreaded_threshold(self) -> None:
        """Above ``_ZSTD_BATCH_THREADS_MIN_ENTRIES``,
        ``multi_decompress_to_buffer`` runs with more than one thread —
        correctness must hold there too, not just for the small batches
        the other cases above use."""
        n = _ZSTD_BATCH_THREADS_MIN_ENTRIES + 50
        plaintexts = [_distinct_plaintext(i) for i in range(n)]
        compressor = zstandard.ZstdCompressor()
        items = [(CompressType.ZSTD, compressor.compress(p)) for p in plaintexts]

        assert decompress_many(items) == plaintexts
