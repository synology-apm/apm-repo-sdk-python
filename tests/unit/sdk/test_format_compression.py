"""Unit tests for ``synology_apm_repo.sdk.format.compression``."""

from __future__ import annotations

import io
from typing import Any

import lz4.block
import pytest
import zstandard

from support.fakes import unchecked_fake
from support.format_builders import zstd_frame_without_content_size
from synology_apm_repo.sdk.errors import ChunkCompactedError, DataCorruptError
from synology_apm_repo.sdk.format import compression
from synology_apm_repo.sdk.format.compression import (
    _ZSTD_BATCH_THREADS_MIN_ENTRIES,
    ZSTD_FRAME_MAGIC,
    CompressType,
    decompress,
    decompress_many,
    decompress_zstd_stream,
    is_zstd_frame,
    iter_decompressed_zstd,
    zstd_content_size,
)
from synology_apm_repo.sdk.format.const import FIXED_CHUNK_LENGTH

_PLAINTEXT = (b"hello dedup world! " * 250)[:4096]
assert len(_PLAINTEXT) == 4096


def test_none_is_identity() -> None:
    assert decompress(CompressType.NONE, _PLAINTEXT) == _PLAINTEXT


def test_none_wrong_length_raises_data_corrupt() -> None:
    with pytest.raises(DataCorruptError, match="decompressed chunk is"):
        decompress(CompressType.NONE, _PLAINTEXT[:100])


def test_lz4_round_trip() -> None:
    compressed = lz4.block.compress(_PLAINTEXT, store_size=False)
    assert decompress(CompressType.LZ4, compressed) == _PLAINTEXT


def test_zstd_round_trip() -> None:
    compressed = zstandard.ZstdCompressor().compress(_PLAINTEXT)
    assert compressed[:4] == ZSTD_FRAME_MAGIC
    assert decompress(CompressType.ZSTD, compressed) == _PLAINTEXT


def test_compacted_raises_chunk_compacted() -> None:
    with pytest.raises(ChunkCompactedError, match="chunk is COMPACTED"):
        decompress(CompressType.COMPACTED, b"")


def test_lz4_corrupt_input_raises_data_corrupt() -> None:
    with pytest.raises(DataCorruptError, match="lz4 decompress failed"):
        decompress(CompressType.LZ4, b"\xff\xff\xff\xff\xff\xff\xff\xff")


def test_zstd_corrupt_input_raises_data_corrupt() -> None:
    with pytest.raises(DataCorruptError, match="zstd decompress failed"):
        decompress(CompressType.ZSTD, ZSTD_FRAME_MAGIC + b"\xff" * 16)


def test_zstd_wrong_length_raises_data_corrupt() -> None:
    short_plaintext = b"x" * 100
    compressed = zstandard.ZstdCompressor().compress(short_plaintext)
    with pytest.raises(DataCorruptError, match="decompressed chunk is"):
        decompress(CompressType.ZSTD, compressed)


def test_zstd_oversized_chunk_raises_data_corrupt_without_fully_materializing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A chunk whose zstd frame declares far more than
    ``FIXED_CHUNK_LENGTH`` is rejected after reading at most one byte past
    it, not the whole declared size."""
    produced: list[int] = []

    @unchecked_fake("zstandard.ZstdDecompressor's stream_reader")
    class _CountingDecompressor:
        def stream_reader(self, source: io.BytesIO) -> _CountingReader:
            return _CountingReader(zstandard.ZstdDecompressor().stream_reader(source))

    @unchecked_fake("a zstandard ZstdDecompressionReader")
    class _CountingReader:
        def __init__(self, reader: Any) -> None:  # zstandard types close() and __exit__ untyped
            self._reader = reader

        def __enter__(self) -> _CountingReader:
            self._reader.__enter__()
            return self

        def __exit__(self, *exc: object) -> None:
            self._reader.__exit__(*exc)

        def read(self, size: int = -1) -> bytes:
            piece: bytes = self._reader.read(size)
            produced.append(len(piece))
            return piece

    monkeypatch.setattr(compression, "_zstd_decompressor", _CountingDecompressor)
    oversized_plaintext = b"x" * (2 * 1024 * 1024)
    compressed = zstandard.ZstdCompressor().compress(oversized_plaintext)
    with pytest.raises(DataCorruptError, match="decompressed chunk is"):
        decompress(CompressType.ZSTD, compressed)
    assert produced and sum(produced) <= FIXED_CHUNK_LENGTH + 1


def test_zstd_branch_never_calls_the_one_shot_decompress_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """``zstandard``'s one-shot ``decompress(max_output_size=N)`` ignores
    its cap when the frame declares a content size, so the ZSTD branch uses
    only the bounded streaming reader."""

    def _must_not_be_called(self: zstandard.ZstdDecompressor, data: bytes, *, max_output_size: int = 0) -> bytes:
        raise AssertionError("one-shot decompress() must not be called from the ZSTD branch")

    monkeypatch.setattr(zstandard.ZstdDecompressor, "decompress", _must_not_be_called)

    oversized_plaintext = b"x" * (2 * 1024 * 1024)
    compressed = zstandard.ZstdCompressor().compress(oversized_plaintext)
    with pytest.raises(DataCorruptError, match="decompressed chunk is"):
        decompress(CompressType.ZSTD, compressed)


def test_decompress_zstd_stream_unbounded() -> None:
    big_plaintext = b"y" * (10 * 1024 * 1024)  # far larger than one chunk
    compressed = zstandard.ZstdCompressor().compress(big_plaintext)
    assert decompress_zstd_stream(compressed, max_output_size=None) == big_plaintext


def test_decompress_zstd_stream_unbounded_handles_a_frame_with_no_declared_content_size() -> None:
    """``zstandard``'s one-shot API can't decode a frame with no declared
    content size unbounded; the streaming reader can."""
    plaintext = b"y" * (10 * 1024 * 1024)
    compressed = zstd_frame_without_content_size(plaintext)
    assert zstandard.get_frame_parameters(compressed).content_size == zstandard.CONTENTSIZE_UNKNOWN
    assert decompress_zstd_stream(compressed, max_output_size=None) == plaintext


def test_decompress_zstd_stream_max_output_size_allows_content_within_the_cap() -> None:
    plaintext = b"z" * 1024
    compressed = zstandard.ZstdCompressor().compress(plaintext)
    assert decompress_zstd_stream(compressed, max_output_size=1024) == plaintext


def test_decompress_zstd_stream_max_output_size_rejects_an_undeclared_oversized_frame_mid_stream() -> None:
    """With no declared size, only the running-total check during the
    streaming read can catch it."""
    plaintext = b"z" * (2 * 1024 * 1024)
    compressed = zstd_frame_without_content_size(plaintext)
    assert zstandard.get_frame_parameters(compressed).content_size == zstandard.CONTENTSIZE_UNKNOWN
    with pytest.raises(zstandard.ZstdError, match="decompressed output exceeds max_output_size"):
        decompress_zstd_stream(compressed, max_output_size=1024)


def test_iter_decompressed_zstd_never_yields_a_piece_past_the_cap() -> None:
    # The total is checked before each piece is yielded, so the piece that
    # trips the cap is never handed to the caller.
    plaintext = b"z" * (2 * 1024 * 1024)
    compressed = zstd_frame_without_content_size(plaintext)
    consumed = 0
    with pytest.raises(zstandard.ZstdError, match="decompressed output exceeds max_output_size"):
        for piece in iter_decompressed_zstd(compressed, max_output_size=1024):
            consumed += len(piece)
    assert consumed <= 1024


class TestDeclaredContentSizeHint:
    """A frame's declared content size replaces ``max_output_size`` (the
    fallback for a frame declaring none) as the enforced ceiling, and is
    checked afterward against what decompression produced."""

    def test_a_declared_size_over_the_fallback_cap_is_still_honored_in_full(self) -> None:
        plaintext = b"z" * (2 * 1024 * 1024)
        compressed = zstandard.ZstdCompressor().compress(plaintext)
        assert zstandard.get_frame_parameters(compressed).content_size == len(plaintext)
        assert decompress_zstd_stream(compressed, max_output_size=1024) == plaintext

    def test_a_declared_size_within_the_fallback_cap_still_succeeds(self) -> None:
        plaintext = b"z" * 1024
        compressed = zstandard.ZstdCompressor().compress(plaintext)
        assert zstandard.get_frame_parameters(compressed).content_size == len(plaintext)
        assert decompress_zstd_stream(compressed, max_output_size=2048) == plaintext

    def test_declared_size_mismatch_raises_data_corrupt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The declared size is monkeypatched: ``zstandard`` itself refuses
        a single-segment frame whose declared size doesn't match its body,
        so no real frame decodes and mismatches."""
        import synology_apm_repo.sdk.format.compression as compression_mod

        plaintext = b"z" * 1024
        compressed = zstandard.ZstdCompressor().compress(plaintext)
        monkeypatch.setattr(compression_mod, "_declared_content_size", lambda data: len(plaintext) + 1)
        with pytest.raises(DataCorruptError, match=r"declared a decompressed size of 1025.*actually produced 1024"):
            decompress_zstd_stream(compressed, max_output_size=None)

    def test_iter_decompressed_zstd_yields_the_full_body_before_raising_on_a_declared_size_mismatch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The declared size is checked only once the whole body was yielded.
        import synology_apm_repo.sdk.format.compression as compression_mod

        plaintext = b"z" * 1024
        compressed = zstandard.ZstdCompressor().compress(plaintext)
        monkeypatch.setattr(compression_mod, "_declared_content_size", lambda data: len(plaintext) + 1)
        pieces = []
        with pytest.raises(DataCorruptError, match=r"declared a decompressed size of 1025.*actually produced 1024"):
            for piece in iter_decompressed_zstd(compressed, max_output_size=None):
                pieces.append(piece)  # noqa: PERF402 - list(...) would lose the pieces collected before the raise
        assert b"".join(pieces) == plaintext


def test_decompress_zstd_stream_bounded_path_never_calls_the_one_shot_decompress_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An undeclared-size frame, so ``max_output_size`` is what's enforced
    (see ``TestDeclaredContentSizeHint``)."""

    def _must_not_be_called(self: zstandard.ZstdDecompressor, data: bytes, *, max_output_size: int = 0) -> bytes:
        raise AssertionError("one-shot decompress() must not be called when max_output_size is given")

    monkeypatch.setattr(zstandard.ZstdDecompressor, "decompress", _must_not_be_called)

    plaintext = b"z" * (2 * 1024 * 1024)
    compressed = zstd_frame_without_content_size(plaintext)
    with pytest.raises(zstandard.ZstdError, match="decompressed output exceeds max_output_size"):
        decompress_zstd_stream(compressed, max_output_size=1024)


class TestZstdContentSize:
    """``zstd_content_size`` — the public form of ``_declared_content_size``
    for a caller that hasn't already confirmed its input is zstd-framed."""

    def test_not_zstd_framed_at_all_returns_none(self) -> None:
        assert zstd_content_size(b"not a zstd frame") is None

    def test_declared_size_is_returned(self) -> None:
        plaintext = b"z" * 1024
        compressed = zstandard.ZstdCompressor().compress(plaintext)
        assert zstd_content_size(compressed) == len(plaintext)

    def test_zstd_framed_but_no_declared_size_returns_none(self) -> None:
        compressed = zstd_frame_without_content_size(b"z" * 1024)
        assert zstd_content_size(compressed) is None


def test_compress_type_values_match_spec() -> None:
    # 3 is skipped.
    assert CompressType.NONE.value == 0
    assert CompressType.LZ4.value == 1
    assert CompressType.ZSTD.value == 2
    assert CompressType.COMPACTED.value == 4


def _distinct_plaintext(i: int) -> bytes:
    return ((b"plaintext-%d-" % i) * 400)[:4096]


class TestDecompressMany:
    """``decompress_many``, the batch form of ``decompress``, checked
    against ``decompress`` itself as the oracle."""

    def test_all_zstd_matches_decompress_per_chunk(self) -> None:
        plaintexts = [_distinct_plaintext(i) for i in range(5)]
        compressor = zstandard.ZstdCompressor()
        items = [(CompressType.ZSTD, compressor.compress(p)) for p in plaintexts]

        result = decompress_many(items)

        assert result == plaintexts
        assert result == [decompress(ctype, data) for ctype, data in items]

    def test_mixed_types_preserve_order_and_correspondence(self) -> None:
        # Interleaved types: batching by type must restore input order.
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
        with pytest.raises(ChunkCompactedError, match="chunk is COMPACTED"):
            decompress_many(
                [
                    (CompressType.ZSTD, zstandard.ZstdCompressor().compress(_distinct_plaintext(0))),
                    (CompressType.COMPACTED, b""),
                ]
            )

    def test_wrong_length_zstd_raises_data_corrupt(self) -> None:
        short_plaintext = b"x" * 100
        compressed = zstandard.ZstdCompressor().compress(short_plaintext)
        with pytest.raises(DataCorruptError, match="batch zstd decompress failed"):
            decompress_many([(CompressType.ZSTD, compressed)])

    @pytest.mark.parametrize("empty", [b"", memoryview(b"")])
    def test_only_empty_zstd_entries_raise_data_corrupt(self, empty: bytes | memoryview) -> None:
        with pytest.raises(DataCorruptError, match="batch zstd decompress failed"):
            decompress_many([(CompressType.ZSTD, empty), (CompressType.ZSTD, empty)])

    def test_wrong_length_none_raises_data_corrupt(self) -> None:
        with pytest.raises(DataCorruptError, match="decompressed chunk at position"):
            decompress_many(
                [
                    (CompressType.ZSTD, zstandard.ZstdCompressor().compress(_distinct_plaintext(0))),
                    (CompressType.NONE, b"too short"),
                ]
            )

    def test_lz4_corrupt_input_raises_data_corrupt(self) -> None:
        with pytest.raises(DataCorruptError, match="lz4 decompress failed"):
            decompress_many(
                [
                    (CompressType.ZSTD, zstandard.ZstdCompressor().compress(_distinct_plaintext(0))),
                    (CompressType.LZ4, b"\xff\xff\xff\xff\xff\xff\xff\xff"),
                ]
            )

    def test_accepts_memoryview_input_zero_copy_for_none(self) -> None:
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
        """At ``_ZSTD_BATCH_THREADS_MIN_ENTRIES`` and above,
        ``multi_decompress_to_buffer`` runs multithreaded."""
        n = _ZSTD_BATCH_THREADS_MIN_ENTRIES + 50
        plaintexts = [_distinct_plaintext(i) for i in range(n)]
        compressor = zstandard.ZstdCompressor()
        items = [(CompressType.ZSTD, compressor.compress(p)) for p in plaintexts]

        assert decompress_many(items) == plaintexts


def test_is_zstd_frame_checks_only_the_leading_magic() -> None:
    assert is_zstd_frame(ZSTD_FRAME_MAGIC + b"anything")
    assert not is_zstd_frame(b"aHlT" + ZSTD_FRAME_MAGIC)
    assert not is_zstd_frame(b"")
