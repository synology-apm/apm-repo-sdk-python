"""Unit tests for ``synology_apm_repo.sdk.dedup.fingerprint`` —
synthetic ``.inf``/``.fgp`` files written to real files."""

from __future__ import annotations

import hashlib
import random
from pathlib import Path

import pytest

from support.format_builders import (
    inf_header,
)
from support.repo_builders import (
    write_inf,
)
from synology_apm_repo.sdk.cachemanager import DEFAULT_LIMITS
from synology_apm_repo.sdk.dedup.fingerprint import (
    _FGP_RECORD_LENGTH,
    _FGP_SEGMENT_SIZE,
    FingerprintIndex,
    _group_contiguous_runs,
)
from synology_apm_repo.sdk.errors import DataCorruptError, FormatError
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.pool_fakes import write_fgp

_ALLOC_TABLE_OFFSET = 12288


def _index(store: LocalFsStore) -> FingerprintIndex:
    return FingerprintIndex(store, DirCache(store), "Pool")


async def _one(store: LocalFsStore, stream_id: StreamId, bucket_id: BucketId, chunk_idx: int) -> bytes:
    """The one digest of ``chunk_idx`` through a fresh ``FingerprintIndex``."""
    return (await _index(store).digests(stream_id, bucket_id, [chunk_idx]))[chunk_idx]


class TestFingerprintLookup:
    async def test_finds_fingerprint_for_bucket_zero_chunk_zero(self, tmp_path: Path) -> None:
        digest = hashlib.sha256(b"chunk0").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", digest)
        store = LocalFsStore(tmp_path)
        assert await _one(store, StreamId(5), BucketId(0), 0) == digest

    async def test_second_chunk_at_the_correct_32_byte_offset(self, tmp_path: Path) -> None:
        d0 = hashlib.sha256(b"c0").digest()
        d1 = hashlib.sha256(b"c1").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 2)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", d0 + d1)
        store = LocalFsStore(tmp_path)
        assert await _one(store, StreamId(5), BucketId(0), 1) == d1

    async def test_second_bucket_in_the_same_group_uses_its_own_byte_offset(self, tmp_path: Path) -> None:
        # Bucket 0's 128 32-byte records fill exactly one 4 KiB page, so
        # bucket 1's byte_off is the next page.
        d_b1 = hashlib.sha256(b"bucket1chunk0").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 128), 1: (4096, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 4096 + d_b1)
        store = LocalFsStore(tmp_path)
        assert await _one(store, StreamId(5), BucketId(1), 0) == d_b1

    async def test_crosses_the_4mib_fgp_segment_boundary(self, tmp_path: Path) -> None:
        # last 4096-aligned page before the 4 MiB boundary; chunk_idx=127
        # lands on its last 32-byte record (ending exactly at the boundary),
        # chunk_idx=128 lands on the first record of the next segment.
        byte_off = _FGP_SEGMENT_SIZE - 4096
        last_record_off = _FGP_SEGMENT_SIZE - 32
        d_last = hashlib.sha256(b"last-in-segment-0").digest()
        d_first_next = hashlib.sha256(b"first-in-segment-1").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (byte_off, 129)})
        seg0 = bytearray(_FGP_SEGMENT_SIZE)
        seg0[last_record_off : last_record_off + 32] = d_last
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", bytes(seg0))
        write_fgp(tmp_path / "Pool" / "5" / "0_1.fgp", d_first_next)
        store = LocalFsStore(tmp_path)
        assert await _one(store, StreamId(5), BucketId(0), 127) == d_last
        assert await _one(store, StreamId(5), BucketId(0), 128) == d_first_next

    async def test_high_bucket_id_uses_its_groups_starting_bucket_id(self, tmp_path: Path) -> None:
        # bucket_id 1025 belongs to the group starting at 1024;
        # group path layering nests under Pool/5/1/1024.*, and its
        # allocation-table entry index is 1025 & 1023 = 1.
        digest = hashlib.sha256(b"high-group").digest()
        write_inf(tmp_path / "Pool" / "5" / "1" / "1024.inf", {1: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "1" / "1024_0.fgp", digest)
        store = LocalFsStore(tmp_path)
        assert await _one(store, StreamId(5), BucketId(1025), 0) == digest

    async def test_sequence_suffixed_inf_and_fgp_resolve(self, tmp_path: Path) -> None:
        digest = hashlib.sha256(b"seq").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf.3", {0: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp.9", digest)
        store = LocalFsStore(tmp_path)
        assert await _one(store, StreamId(5), BucketId(0), 0) == digest

    async def test_chunk_idx_beyond_recnum_raises_data_corrupt(self, tmp_path: Path) -> None:
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 32)
        store = LocalFsStore(tmp_path)
        with pytest.raises(DataCorruptError, match="has no fingerprint recorded for bucket"):
            await _one(store, StreamId(5), BucketId(0), 1)

    async def test_bad_inf_magic_raises_data_corrupt(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.inf"
        path.parent.mkdir(parents=True)
        buf = bytearray(_ALLOC_TABLE_OFFSET + 1024 * 8)
        buf[0:4] = b"XXXX"
        path.write_bytes(bytes(buf))
        store = LocalFsStore(tmp_path)
        with pytest.raises(DataCorruptError, match="bad magic"):
            await _one(store, StreamId(5), BucketId(0), 0)

    async def test_inf_truncated_before_its_own_allocation_entry_raises_format_error(self, tmp_path: Path) -> None:
        # Header only: the file ends before _ALLOC_TABLE_OFFSET.
        path = tmp_path / "Pool" / "5" / "0.inf"
        path.parent.mkdir(parents=True)
        path.write_bytes(inf_header())
        store = LocalFsStore(tmp_path)
        with pytest.raises(FormatError, match="no allocation entry"):
            await _one(store, StreamId(5), BucketId(0), 0)

    async def test_truncated_fgp_raises_format_error(self, tmp_path: Path) -> None:
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 10)  # too short for a 32-byte record
        store = LocalFsStore(tmp_path)
        with pytest.raises(FormatError, match="truncated: expected"):
            await _one(store, StreamId(5), BucketId(0), 0)


class TestBatchedFingerprintReads:
    """``digests()``'s digest-read batching: consecutive, same-segment
    ``chunk_idx``\\ s cost one merged ``store.read()`` instead of one per
    chunk."""

    @staticmethod
    def _count_fgp_reads(monkeypatch: pytest.MonkeyPatch) -> list[int]:
        """A one-element counter of ``store.read()`` calls against any ``.fgp`` file."""
        counter = [0]
        real_read = LocalFsStore.read

        async def counting_read(self: LocalFsStore, path: str, offset: int = 0, length: int | None = None) -> bytes:
            if ".fgp" in path:
                counter[0] += 1
            return await real_read(self, path, offset, length)

        monkeypatch.setattr(LocalFsStore, "read", counting_read)
        return counter

    async def test_consecutive_chunk_indices_cost_one_merged_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        digests = [hashlib.sha256(f"c{i}".encode()).digest() for i in range(5)]
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 5)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"".join(digests))
        store = LocalFsStore(tmp_path)
        fgp_reads = self._count_fgp_reads(monkeypatch)

        result = await _index(store).digests(StreamId(5), BucketId(0), [ChunkIdx(i) for i in range(5)])

        assert result == {ChunkIdx(i): digests[i] for i in range(5)}
        assert fgp_reads[0] == 1

    async def test_scattered_chunk_indices_cost_one_read_each(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        digests = [hashlib.sha256(f"c{i}".encode()).digest() for i in range(10)]
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 10)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"".join(digests))
        store = LocalFsStore(tmp_path)
        fgp_reads = self._count_fgp_reads(monkeypatch)

        result = await _index(store).digests(StreamId(5), BucketId(0), [ChunkIdx(0), ChunkIdx(4), ChunkIdx(9)])

        assert result == {ChunkIdx(0): digests[0], ChunkIdx(4): digests[4], ChunkIdx(9): digests[9]}
        assert fgp_reads[0] == 3

    async def test_a_run_crossing_the_segment_boundary_splits_into_two_merged_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Same layout as test_crosses_the_4mib_fgp_segment_boundary, plus
        # chunks 125 and 126 just before the boundary, so the requested run
        # (125, 126, 127, 128) spans both segments.
        byte_off = _FGP_SEGMENT_SIZE - 4096
        d125 = hashlib.sha256(b"c125").digest()
        d126 = hashlib.sha256(b"c126").digest()
        d127 = hashlib.sha256(b"last-in-segment-0").digest()
        d128 = hashlib.sha256(b"first-in-segment-1").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (byte_off, 129)})
        seg0 = bytearray(_FGP_SEGMENT_SIZE)
        seg0[_FGP_SEGMENT_SIZE - 96 : _FGP_SEGMENT_SIZE - 64] = d125
        seg0[_FGP_SEGMENT_SIZE - 64 : _FGP_SEGMENT_SIZE - 32] = d126
        seg0[_FGP_SEGMENT_SIZE - 32 :] = d127
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", bytes(seg0))
        write_fgp(tmp_path / "Pool" / "5" / "0_1.fgp", d128)
        store = LocalFsStore(tmp_path)
        fgp_reads = self._count_fgp_reads(monkeypatch)

        result = await _index(store).digests(
            StreamId(5), BucketId(0), [ChunkIdx(125), ChunkIdx(126), ChunkIdx(127), ChunkIdx(128)]
        )

        assert result == {
            ChunkIdx(125): d125,
            ChunkIdx(126): d126,
            ChunkIdx(127): d127,
            ChunkIdx(128): d128,
        }
        # (125, 126, 127) merge into one read of segment 0; 128 reads segment 1.
        assert fgp_reads[0] == 2

    async def test_batched_call_raises_data_corrupt_for_a_chunk_beyond_recnum(self, tmp_path: Path) -> None:
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 2)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 64)
        store = LocalFsStore(tmp_path)
        with pytest.raises(DataCorruptError, match="has no fingerprint recorded for bucket"):
            await _index(store).digests(StreamId(5), BucketId(0), [ChunkIdx(0), ChunkIdx(5)])

    async def test_batched_call_raises_format_error_for_a_truncated_fgp(self, tmp_path: Path) -> None:
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 2)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 10)  # too short for even one record
        store = LocalFsStore(tmp_path)
        with pytest.raises(FormatError, match="truncated: expected"):
            await _index(store).digests(StreamId(5), BucketId(0), [ChunkIdx(0), ChunkIdx(1)])


def _reference_runs(chunk_indices: list[int], byte_off: int) -> list[tuple[int, int]]:
    """Per-index oracle: extend a run only by the next integer in the same segment."""

    def segment(idx: int) -> int:
        return (byte_off + idx * _FGP_RECORD_LENGTH) // _FGP_SEGMENT_SIZE

    runs: list[list[int]] = []
    for idx in sorted(set(chunk_indices)):
        if runs and runs[-1][-1] == idx - 1 and segment(runs[-1][-1]) == segment(idx):
            runs[-1].append(idx)
        else:
            runs.append([idx])
    return [(run[0], len(run)) for run in runs]


class TestGroupContiguousRuns:
    def test_splits_at_gaps_and_at_a_segment_boundary(self) -> None:
        per_segment = _FGP_SEGMENT_SIZE // _FGP_RECORD_LENGTH
        indices = [0, 1, 2, 5, 6, per_segment - 1, per_segment, per_segment + 1]
        assert _group_contiguous_runs(indices, 0) == [(0, 3), (5, 2), (per_segment - 1, 1), (per_segment, 2)]

    def test_empty(self) -> None:
        assert _group_contiguous_runs([], 0) == []

    def test_matches_the_per_index_oracle(self) -> None:
        rng = random.Random(7)
        per_segment = _FGP_SEGMENT_SIZE // _FGP_RECORD_LENGTH
        for _ in range(200):
            byte_off = rng.randrange(0, 4 * _FGP_SEGMENT_SIZE, _FGP_RECORD_LENGTH)
            base = rng.randrange(0, 3 * per_segment)
            indices = [base + rng.randrange(0, 40) for _ in range(rng.randrange(0, 30))]
            assert _group_contiguous_runs(indices, byte_off) == _reference_runs(indices, byte_off)


class TestAllocationTableSharing:
    """One ``FingerprintIndex`` reads a group's whole allocation table once
    and shares it across every bucket in that group."""

    async def test_a_second_bucket_in_the_same_group_costs_no_further_inf_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        d0 = hashlib.sha256(b"bucket0chunk0").digest()
        d1 = hashlib.sha256(b"bucket1chunk0").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1), 1: (4096, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", d0 + b"\x00" * (4096 - 32) + d1)
        store = LocalFsStore(tmp_path)
        index = _index(store)

        inf_reads = 0
        real_read = LocalFsStore.read

        async def counting_read(self: LocalFsStore, path: str, offset: int = 0, length: int | None = None) -> bytes:
            nonlocal inf_reads
            if path.endswith(".inf"):
                inf_reads += 1
            return await real_read(self, path, offset, length)

        monkeypatch.setattr(LocalFsStore, "read", counting_read)

        assert await index.digests(StreamId(5), BucketId(0), [0]) == {0: d0}
        assert await index.digests(StreamId(5), BucketId(1), [0]) == {0: d1}
        # Header + whole allocation table, once for both buckets.
        assert inf_reads == 2


class TestAllocationTableBound:
    async def test_the_cache_is_lru_bounded_by_its_maxsize(self, tmp_path: Path) -> None:
        for stream in (1, 2, 3):
            write_inf(tmp_path / "Pool" / str(stream) / "0.inf", {0: (0, 1)})
            write_fgp(tmp_path / "Pool" / str(stream) / "0_0.fgp", b"\x00" * 32)
        store = LocalFsStore(tmp_path)
        index = FingerprintIndex(store, DirCache(store), "Pool", maxsize=2)

        for stream in (1, 2, 3):
            await index.digests(StreamId(stream), BucketId(0), [0])

        stats = index.stats()
        assert (stats.size, stats.maxsize, stats.evictions) == (2, 2, 1)

    def test_the_default_bound_is_the_cache_limit(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)
        assert _index(store).stats().maxsize == DEFAULT_LIMITS.allocation_tables
