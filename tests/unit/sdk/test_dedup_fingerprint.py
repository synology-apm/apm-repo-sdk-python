"""Unit tests for ``synology_apm_repo.sdk.dedup.fingerprint`` —
synthetic ``.inf``/``.fgp`` files written to real files."""

from __future__ import annotations

import hashlib
import zlib
from pathlib import Path

import pytest

from synology_apm_repo.sdk.dedup.fingerprint import AllocationTableCache, fingerprint, fingerprints
from synology_apm_repo.sdk.errors import DataCorruptError, FormatError
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore

_ALLOC_TABLE_OFFSET = 12288
_FGP_SEGMENT_SIZE = 4 << 20


def _inf_header() -> bytes:
    header = bytearray(64)
    header[0:4] = b"GMet"
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _write_inf(path: Path, entries: dict[int, tuple[int, int]]) -> None:
    """``entries``: bucket-index-within-group -> (byte_off, rec_num)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = bytearray(_ALLOC_TABLE_OFFSET + 1024 * 8)
    buf[0:64] = _inf_header()
    for idx, (byte_off, rec_num) in entries.items():
        raw_pos = ((byte_off // 4096) << 15) | rec_num
        off = _ALLOC_TABLE_OFFSET + idx * 8
        buf[off : off + 4] = raw_pos.to_bytes(4, "big")
        buf[off + 4 : off + 8] = (0).to_bytes(4, "big")  # crc field, not decoded by this module
    path.write_bytes(bytes(buf))


def _write_fgp(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


class TestFingerprintLookup:
    async def test_finds_fingerprint_for_bucket_zero_chunk_zero(self, tmp_path: Path) -> None:
        digest = hashlib.sha256(b"chunk0").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", digest)
        store = LocalFsStore(tmp_path)
        assert await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(0)) == digest

    async def test_second_chunk_at_the_correct_32_byte_offset(self, tmp_path: Path) -> None:
        d0 = hashlib.sha256(b"c0").digest()
        d1 = hashlib.sha256(b"c1").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 2)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", d0 + d1)
        store = LocalFsStore(tmp_path)
        assert await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(1)) == d1

    async def test_second_bucket_in_the_same_group_uses_its_own_byte_offset(self, tmp_path: Path) -> None:
        # byte_off is only ever a multiple of 4096 on real data (Offset4K is
        # a 4K-granularity field) — bucket 0 here fills exactly one 4K page
        # (128 records * 32 bytes = 4096) so bucket 1's offset lands cleanly
        # on the next page, exactly like real (always-full-except-the-last)
        # buckets do.
        d_b1 = hashlib.sha256(b"bucket1chunk0").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 128), 1: (4096, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 4096 + d_b1)
        store = LocalFsStore(tmp_path)
        assert await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(1), ChunkIdx(0)) == d_b1

    async def test_crosses_the_4mib_fgp_segment_boundary(self, tmp_path: Path) -> None:
        # last 4096-aligned page before the 4 MiB boundary; chunk_idx=127
        # lands on its last 32-byte record (ending exactly at the boundary),
        # chunk_idx=128 lands on the first record of the next segment.
        byte_off = _FGP_SEGMENT_SIZE - 4096
        last_record_off = _FGP_SEGMENT_SIZE - 32
        d_last = hashlib.sha256(b"last-in-segment-0").digest()
        d_first_next = hashlib.sha256(b"first-in-segment-1").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (byte_off, 129)})
        seg0 = bytearray(_FGP_SEGMENT_SIZE)
        seg0[last_record_off : last_record_off + 32] = d_last
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", bytes(seg0))
        _write_fgp(tmp_path / "Pool" / "5" / "0_1.fgp", d_first_next)
        store = LocalFsStore(tmp_path)
        assert await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(127)) == d_last
        assert (
            await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(128)) == d_first_next
        )

    async def test_high_bucket_id_uses_its_groups_starting_bucket_id(self, tmp_path: Path) -> None:
        # bucket_id 1025 belongs to the group starting at 1024;
        # group path layering nests under Pool/5/1/1024.*, and its
        # allocation-table entry index is 1025 & 1023 = 1.
        digest = hashlib.sha256(b"high-group").digest()
        _write_inf(tmp_path / "Pool" / "5" / "1" / "1024.inf", {1: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "1" / "1024_0.fgp", digest)
        store = LocalFsStore(tmp_path)
        assert await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(1025), ChunkIdx(0)) == digest

    async def test_sequence_suffixed_inf_and_fgp_resolve(self, tmp_path: Path) -> None:
        digest = hashlib.sha256(b"seq").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf.3", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp.9", digest)
        store = LocalFsStore(tmp_path)
        assert await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(0)) == digest

    async def test_chunk_idx_beyond_recnum_raises_data_corrupt(self, tmp_path: Path) -> None:
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 32)
        store = LocalFsStore(tmp_path)
        with pytest.raises(DataCorruptError):
            await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(1))

    async def test_bad_inf_magic_raises_data_corrupt(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.inf"
        path.parent.mkdir(parents=True)
        buf = bytearray(_ALLOC_TABLE_OFFSET + 1024 * 8)
        buf[0:4] = b"XXXX"
        path.write_bytes(bytes(buf))
        store = LocalFsStore(tmp_path)
        with pytest.raises(DataCorruptError):
            await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(0))

    async def test_inf_truncated_before_its_own_allocation_entry_raises_format_error(self, tmp_path: Path) -> None:
        # Header only -- nothing at all at _ALLOC_TABLE_OFFSET, unlike
        # test_bad_inf_magic_raises_data_corrupt's full-length buffer.
        path = tmp_path / "Pool" / "5" / "0.inf"
        path.parent.mkdir(parents=True)
        path.write_bytes(_inf_header())
        store = LocalFsStore(tmp_path)
        with pytest.raises(FormatError, match="no allocation entry"):
            await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(0))

    async def test_truncated_fgp_raises_format_error(self, tmp_path: Path) -> None:
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 10)  # too short for a 32-byte record
        store = LocalFsStore(tmp_path)
        with pytest.raises(FormatError):
            await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(0))


class TestBatchedFingerprintReads:
    """``fingerprints()``'s own digest-read batching — consecutive,
    same-segment ``chunk_idx``\\ s cost one merged ``store.read()`` instead
    of one per chunk. Separate from ``TestAllocationTableCache``, which
    covers the ``.inf`` allocation-table half of batching, not this."""

    @staticmethod
    def _count_fgp_reads(monkeypatch: pytest.MonkeyPatch) -> list[int]:
        """Returns a single-element list acting as a mutable counter of
        ``store.read()`` calls against any ``.fgp`` file."""
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
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 5)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"".join(digests))
        store = LocalFsStore(tmp_path)
        fgp_reads = self._count_fgp_reads(monkeypatch)

        result = await fingerprints(
            store, DirCache(store), "Pool", StreamId(5), BucketId(0), [ChunkIdx(i) for i in range(5)]
        )

        assert result == {ChunkIdx(i): digests[i] for i in range(5)}
        assert fgp_reads[0] == 1

    async def test_scattered_chunk_indices_cost_one_read_each(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        digests = [hashlib.sha256(f"c{i}".encode()).digest() for i in range(10)]
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 10)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"".join(digests))
        store = LocalFsStore(tmp_path)
        fgp_reads = self._count_fgp_reads(monkeypatch)

        result = await fingerprints(
            store, DirCache(store), "Pool", StreamId(5), BucketId(0), [ChunkIdx(0), ChunkIdx(4), ChunkIdx(9)]
        )

        assert result == {ChunkIdx(0): digests[0], ChunkIdx(4): digests[4], ChunkIdx(9): digests[9]}
        # No two requested indices are adjacent -- grouping can't merge
        # any of them, so this costs exactly one read per index, same as
        # calling fingerprint() in a loop would.
        assert fgp_reads[0] == 3

    async def test_a_run_crossing_the_segment_boundary_splits_into_two_merged_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Same layout as test_crosses_the_4mib_fgp_segment_boundary, plus
        # one more chunk (125) just before the boundary so the requested
        # run (125, 126, 127, 128) spans both segments.
        byte_off = _FGP_SEGMENT_SIZE - 4096
        d125 = hashlib.sha256(b"c125").digest()
        d126 = hashlib.sha256(b"c126").digest()
        d127 = hashlib.sha256(b"last-in-segment-0").digest()
        d128 = hashlib.sha256(b"first-in-segment-1").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (byte_off, 129)})
        seg0 = bytearray(_FGP_SEGMENT_SIZE)
        seg0[_FGP_SEGMENT_SIZE - 96 : _FGP_SEGMENT_SIZE - 64] = d125
        seg0[_FGP_SEGMENT_SIZE - 64 : _FGP_SEGMENT_SIZE - 32] = d126
        seg0[_FGP_SEGMENT_SIZE - 32 :] = d127
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", bytes(seg0))
        _write_fgp(tmp_path / "Pool" / "5" / "0_1.fgp", d128)
        store = LocalFsStore(tmp_path)
        fgp_reads = self._count_fgp_reads(monkeypatch)

        result = await fingerprints(
            store,
            DirCache(store),
            "Pool",
            StreamId(5),
            BucketId(0),
            [ChunkIdx(125), ChunkIdx(126), ChunkIdx(127), ChunkIdx(128)],
        )

        assert result == {
            ChunkIdx(125): d125,
            ChunkIdx(126): d126,
            ChunkIdx(127): d127,
            ChunkIdx(128): d128,
        }
        # (125, 126, 127) merge into one read of segment 0; 128 is its own
        # segment's own read -- 2 reads total, not 4 and not 1.
        assert fgp_reads[0] == 2

    async def test_batched_call_raises_data_corrupt_for_a_chunk_beyond_recnum(self, tmp_path: Path) -> None:
        """The merged-run path's own bound check -- distinct from
        ``TestFingerprintLookup.test_chunk_idx_beyond_recnum_raises_data_corrupt``,
        which only exercises the unbatched, single-chunk ``fingerprint()``."""
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 2)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 64)
        store = LocalFsStore(tmp_path)
        with pytest.raises(DataCorruptError):
            await fingerprints(store, DirCache(store), "Pool", StreamId(5), BucketId(0), [ChunkIdx(0), ChunkIdx(5)])

    async def test_batched_call_raises_format_error_for_a_truncated_fgp(self, tmp_path: Path) -> None:
        """The merged-run path's own truncation check -- distinct from
        ``TestFingerprintLookup.test_truncated_fgp_raises_format_error``,
        which only exercises the unbatched, single-chunk ``fingerprint()``."""
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 2)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"\x00" * 10)  # too short for even one record
        store = LocalFsStore(tmp_path)
        with pytest.raises(FormatError):
            await fingerprints(store, DirCache(store), "Pool", StreamId(5), BucketId(0), [ChunkIdx(0), ChunkIdx(1)])


class TestAllocationTableCache:
    """``fingerprint()``'s optional ``cache=`` — shares one group's whole
    allocation table across separate calls/buckets instead of resolving it
    fresh every time, since every bucket in a group shares the same
    allocation-table bytes."""

    async def test_a_second_bucket_in_the_same_group_costs_no_further_inf_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        d0 = hashlib.sha256(b"bucket0chunk0").digest()
        d1 = hashlib.sha256(b"bucket1chunk0").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1), 1: (4096, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", d0 + b"\x00" * (4096 - 32) + d1)
        store = LocalFsStore(tmp_path)
        cache = AllocationTableCache()

        inf_reads = 0
        real_read = LocalFsStore.read

        async def counting_read(self: LocalFsStore, path: str, offset: int = 0, length: int | None = None) -> bytes:
            nonlocal inf_reads
            if path.endswith(".inf"):
                inf_reads += 1
            return await real_read(self, path, offset, length)

        monkeypatch.setattr(LocalFsStore, "read", counting_read)

        assert (
            await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(0), cache=cache) == d0
        )
        assert (
            await fingerprint(store, DirCache(store), "Pool", StreamId(5), BucketId(1), ChunkIdx(0), cache=cache) == d1
        )
        # Exactly 2 reads against the .inf file total (header + the whole
        # allocation table), for *two* buckets sharing one group -- not 4
        # (2 per bucket), the way two calls with no cache at all would cost.
        assert inf_reads == 2

    async def test_inf_truncated_before_its_own_allocation_entry_raises_format_error(self, tmp_path: Path) -> None:
        """The cached path's own truncated-table guard — distinct from
        ``TestFingerprintLookup``'s identically-named test, which exercises
        the uncached (``cache=None``) resolution instead."""
        path = tmp_path / "Pool" / "5" / "0.inf"
        path.parent.mkdir(parents=True)
        path.write_bytes(_inf_header())  # header only -- nothing at the allocation table offset at all
        store = LocalFsStore(tmp_path)
        with pytest.raises(FormatError, match="no allocation entry"):
            await fingerprint(
                store, DirCache(store), "Pool", StreamId(5), BucketId(0), ChunkIdx(0), cache=AllocationTableCache()
            )
