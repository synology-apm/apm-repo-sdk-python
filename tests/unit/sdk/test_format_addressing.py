"""Unit tests for ``synology_apm_repo.sdk.format.addressing``."""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.format.addressing import (
    ChunkAddress,
    composition_path,
    composition_session_dir,
    group_start_bucket_id,
    pool_layer_path,
    split_composition_offset,
    split_layer_leaf,
)
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, SessionId, StreamId


def _addr(stream_id: int, bucket_id: int, chunk_idx: int) -> ChunkAddress:
    """Cast plain-int test literals into ``ChunkAddress``'s ``NewType`` fields
    once, here, instead of at every call site below."""
    return ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx))


class TestChunkAddress:
    def test_pack_unpack_round_trip(self) -> None:
        addr = _addr(132, 330, 17)
        packed = addr.to_int()
        assert ChunkAddress.from_int(packed) == addr

    def test_bit_layout_matches_spec(self) -> None:
        # streamID(8) | bucketID(40) | chunkIdx(16), big fields packed MSB-first
        addr = _addr(0x7F, 0x1234567890, 0x00FF)
        packed = addr.to_int()
        assert packed == (0x7F << 56) | (0x1234567890 << 16) | 0x00FF

    def test_from_int_does_not_validate_chunk_idx_past_bucket_capacity(self) -> None:
        # chunk_idx field is 16 bits wide; [8192, 65536) is out of any real
        # bucket's capacity, but ``from_int()`` trusts its input.
        bad = (1 << 56) | (0 << 16) | 8192
        addr = ChunkAddress.from_int(bad)
        assert addr.chunk_idx == 8192

    def test_from_int_accepts_chunk_idx_at_max_valid_value(self) -> None:
        ok = (1 << 56) | (0 << 16) | 8191
        addr = ChunkAddress.from_int(ok)
        assert addr.chunk_idx == 8191

    def test_construction_does_not_validate_out_of_range_bucket_id(self) -> None:
        addr = _addr(0, 1 << 40, 0)
        assert addr.bucket_id == 1 << 40

    def test_construction_does_not_validate_out_of_range_stream_id(self) -> None:
        addr = _addr(256, 0, 0)
        assert addr.stream_id == 256

    def test_advance_within_bucket_no_carry(self) -> None:
        addr = _addr(1, 5, 10)
        result = addr.advance(20)
        assert result == _addr(1, 5, 30)

    def test_advance_carries_at_8192_not_65536(self) -> None:
        addr = _addr(1, 5, 8190)
        result = addr.advance(5)
        assert result.bucket_id == 6
        assert result.chunk_idx == 3  # 8190 + 5 = 8195 = 1*8192 + 3

    def test_advance_can_carry_across_multiple_buckets(self) -> None:
        addr = _addr(1, 0, 0)
        result = addr.advance(8192 * 3 + 7)
        assert result.bucket_id == 3
        assert result.chunk_idx == 7

    def test_advance_exactly_to_boundary_does_not_carry(self) -> None:
        addr = _addr(1, 5, 0)
        result = addr.advance(8191)
        assert result.bucket_id == 5
        assert result.chunk_idx == 8191

    def test_advance_rejects_negative(self) -> None:
        addr = _addr(1, 0, 0)
        with pytest.raises(ValueError):
            addr.advance(-1)


class TestPoolLayerPath:
    def test_small_bucket_id_has_no_ancestor_layers(self) -> None:
        # apv-sample-1: ``Pool/132/330.buk.*`` sits directly under
        # ``Pool/132``, no intermediate directories.
        assert pool_layer_path(StreamId(132), BucketId(330)) == "132/330"

    def test_zero_bucket_id(self) -> None:
        assert pool_layer_path(StreamId(132), BucketId(0)) == "132/0"

    def test_bucket_id_past_1024_gets_one_ancestor_layer(self) -> None:
        assert pool_layer_path(StreamId(5), BucketId(1025)) == "5/1/1025"

    def test_bucket_id_past_1024_squared_gets_two_ancestor_layers(self) -> None:
        big = (1024 * 1024) + 5  # >> 10 == 1024, >> 20 == 1
        assert pool_layer_path(StreamId(5), BucketId(big)) == f"5/1/1024/{big}"


class TestSplitLayerLeaf:
    def test_no_ancestor_layers(self) -> None:
        assert split_layer_leaf("132/330") == ("132", "330")

    def test_one_ancestor_layer(self) -> None:
        assert split_layer_leaf("5/1/1025") == ("5/1", "1025")

    def test_bare_leaf_has_empty_dir_part(self) -> None:
        assert split_layer_leaf("1025") == ("", "1025")


class TestGroupStartBucketId:
    def test_group_start_matches_1024_alignment(self) -> None:
        assert group_start_bucket_id(BucketId(0)) == 0
        assert group_start_bucket_id(BucketId(1023)) == 0
        assert group_start_bucket_id(BucketId(1024)) == 1024
        assert group_start_bucket_id(BucketId(1025)) == 1024
        assert group_start_bucket_id(BucketId(2047)) == 1024
        assert group_start_bucket_id(BucketId(2048)) == 2048


class TestCompositionPaths:
    def test_session_dir_matches_real_sample_shape(self) -> None:
        # real shape: ``Composition/132/4.com/c0.39`` on apv-sample-1 (".39"
        # is a sequence-id suffix resolved separately by ``storage/seqid.py``).
        assert composition_session_dir(StreamId(132), SessionId(4)) == "132/4.com"
        assert composition_path(StreamId(132), SessionId(4), 0) == "132/4.com/c0"

    def test_session_id_past_1024_gets_ancestor_layer(self) -> None:
        assert composition_session_dir(StreamId(7), SessionId(1025)) == "7/1/1025.com"

    def test_sub_id_past_1024_gets_ancestor_layer(self) -> None:
        assert composition_path(StreamId(7), SessionId(4), 1025) == "7/4.com/1/c1025"


class TestCompositionOffsetSplit:
    def test_split_at_zero(self) -> None:
        assert split_composition_offset(0) == (0, 0)

    def test_split_within_first_subfile(self) -> None:
        assert split_composition_offset(64) == (0, 64)

    def test_split_at_subfile_boundary(self) -> None:
        sixteen_mib = 1 << 24
        assert split_composition_offset(sixteen_mib) == (1, 0)
        assert split_composition_offset(sixteen_mib - 1) == (0, sixteen_mib - 1)
        assert split_composition_offset(sixteen_mib + 100) == (1, 100)

    def test_split_round_trips_via_the_documented_shift(self) -> None:
        # (sub_id << 24) | sub_off is split_composition_offset()'s inverse —
        # inlined here rather than kept as a public ``join_*`` function,
        # since this read-only SDK only ever decodes existing offsets,
        # never constructs new ones.
        for global_offset in (0, 64, (1 << 24) - 1, (1 << 24) + 100, (1 << 26) + 12345):
            sub_id, sub_off = split_composition_offset(global_offset)
            assert (sub_id << 24) | sub_off == global_offset
