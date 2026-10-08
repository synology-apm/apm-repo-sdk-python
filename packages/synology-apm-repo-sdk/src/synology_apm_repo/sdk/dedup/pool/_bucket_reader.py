"""``BucketReader``: one ``.buk`` file's header, ``BucketIndex`` (SizeStore
entries and byte ranges), and the decrypt+decompress read path. Reads only
through its ``ObjectStore`` (``read_sync`` when it is ``SyncReadable``).
"""

from __future__ import annotations

import array
import asyncio
import dataclasses
import re
from collections.abc import Callable, Sequence

from cryptography.hazmat.primitives.ciphers import algorithms

from ...errors import ChunkCompactedError, DataCorruptError, FormatError, KeyRequiredError, NotFoundError
from ...format.addressing import ChunkAddress
from ...format.bucket import (
    COMPRESS_TYPE_BY_VALUE,
    COMPRESS_TYPE_COMPACTED_VALUE,
    BucketFileHeader,
    BucketIndex,
    chunk_crc_store_region,
    chunk_size_store_tight_length,
    parse_bucket_header,
    parse_chunk_crc_store,
    parse_size_store,
)
from ...format.compression import CompressType, decompress, decompress_many
from ...format.const import CHUNK_CRC_SIZE, COMPRESS_RESERVED_LENG, REDUNDANCY_COVERAGE_BUCKET
from ...format.crypto import ChunkDecryptor, decrypt_chunk
from ...format.headers import HEADER_LEN, verify_crc32
from ...format.redundancy import redundancy_size
from ...identifiers import BucketId, ChunkIdx, StreamId
from ...storage.base import ObjectStore, SyncReadable
from ..redundancy_repair import repair_via_trailer

_SPEC = "FORMAT-SPEC.md: ChunkCrcStore & Redundancy"

#: Locator byte-ranges within this many bytes of each other merge into one
#: ``ObjectStore.read``, trading read amplification for fewer reads. A merged
#: run has no size cap; the bucket's chunk count bounds it.
_GAP_TOLERANCE = 1 << 20  # 1 MiB


async def _attempt_size_store_repair(
    store: ObjectStore, path: str, header: BucketFileHeader, sizestore_region: bytes
) -> tuple[bytes, int] | None:
    """On a ``chunk_size_crc`` mismatch, fetch this bucket's trailing
    Redundancy blob (FORMAT-SPEC.md: ChunkCrcStore & Redundancy; the last
    thing in the file) and attempt in-memory self-repair
    (``dedup.redundancy_repair.repair_via_trailer``).

    The trailer offset is the on-disk file size minus the trailer size, which
    depends only on ``header.chunk_num``; the SizeStore's own chunk lengths
    are what may be wrong.

    Returns:
        ``(repaired tight SizeStore bytes, file_size)`` on a confirmed
        repair, or ``None`` if the size or trailer can't be fetched or the
        repair doesn't validate.
    """
    tight_len = chunk_size_store_tight_length(header.chunk_num)
    tight = sizestore_region[:tight_len]
    trailer_len = redundancy_size(tight_len, REDUNDANCY_COVERAGE_BUCKET)
    try:
        file_size = await store.size(path)
    except (NotFoundError, FormatError):
        return None
    if file_size < trailer_len:
        return None
    repaired = await repair_via_trailer(
        tight,
        coverage=REDUNDANCY_COVERAGE_BUCKET,
        expected_crc=header.chunk_size_crc,
        fetch_trailer=lambda: store.read(path, file_size - trailer_len, trailer_len),
    )
    if repaired is None:
        return None
    return repaired, file_size


_NON_COMPACTED_RUN = re.compile(b"[^" + re.escape(bytes([COMPRESS_TYPE_COMPACTED_VALUE])) + b"]+")
"""A maximal run of raw compress-type bytes other than ``COMPACTED``."""


@dataclasses.dataclass(frozen=True, slots=True)
class _IndexRun:
    """One merged read of ``read_chunks``/``read_raw_chunks``: the stored byte span
    ``[start, end)`` and the chunk-index subranges ``[a, b)`` decoded from it."""

    start: int
    end: int
    chunks: list[tuple[int, int]]


class BucketReader:
    """One opened ``.buk`` file. Build with ``open``.

    The read paths use ``index``'s raw columns directly, since a
    bucket-major export touches nearly every chunk.

    Attributes:
        path: The ``.buk`` path in the store.
        header: The parsed bucket header.
        index: Every chunk's SizeStore entry and byte range.
        sizestore_repaired: ``open`` fixed a SizeStore CRC mismatch via
            Redundancy parity (``check_bucket_structure`` reports it).
        known_file_size: The on-disk size, when that repair had to fetch it.
    """

    def __init__(
        self,
        store: ObjectStore,
        path: str,
        header: BucketFileHeader,
        index: BucketIndex,
        vault_key: bytes | None,
        *,
        algorithm: algorithms.AES | None = None,
        verify_ciphertext_crc: bool = False,
        sizestore_repaired: bool = False,
        known_file_size: int | None = None,
    ) -> None:
        self._store = store
        self.path = path
        self.header = header
        self.index = index
        self.sizestore_repaired = sizestore_repaired
        self.known_file_size = known_file_size
        self._vault_key = vault_key
        # Pool's cached key schedule, reused by every decrypt_chunk() call.
        self._algorithm = algorithm
        # Whether every chunk read checks its ChunkCrcStore entry first.
        self._verify_ciphertext_crc = verify_ciphertext_crc
        # Read lazily by ensure_chunk_crc_store(): the trailer needs its own fetch.
        self._chunk_crc_values: tuple[int, ...] | None = None
        # Cumulative lookup array computed alongside _chunk_crc_values: O(1) per chunk.
        self._chunk_crc_positions: array.array[int] | None = None

    @classmethod
    async def open(
        cls,
        store: ObjectStore,
        path: str,
        *,
        vault_key: bytes | None = None,
        algorithm: algorithms.AES | None = None,
        verify_ciphertext_crc: bool = False,
    ) -> BucketReader:
        """Open ``path`` and parse its header + SizeStore in one read of up
        to ``COMPRESS_RESERVED_LENG`` (16384) bytes.

        The SizeStore CRC is always verified. A mismatch first tries the
        bucket's trailing Redundancy blob (``_attempt_size_store_repair``),
        so parity repair is transparent on every open; a successful repair
        sets ``sizestore_repaired``.

        Args:
            store: The object store holding ``path``.
            path: Physical ``.buk`` path.
            vault_key: Required to read chunks of a vault-encrypted bucket,
                even when a prebuilt AES ``algorithm`` is passed.
            algorithm: A key schedule built from ``vault_key``, reused by
                every chunk decrypt instead of rebuilding it.
            verify_ciphertext_crc: Make every chunk read check its stored
                bytes against the ChunkCrcStore first; opening does not read
                the trailer itself.

        Raises:
            NotFoundError: ``path`` does not exist.
            FormatError: The file is shorter than its header.
            DataCorruptError: The header or SizeStore is corrupt and not
                repairable.
            UnsupportedVersionError: The bucket's major version is too new.
        """
        head = await store.read(path, 0, COMPRESS_RESERVED_LENG)
        header = parse_bucket_header(head)
        index: BucketIndex
        sizestore_repaired = False
        known_file_size: int | None = None
        if header.is_compressed:
            sizestore_region = head[HEADER_LEN:COMPRESS_RESERVED_LENG]
            try:
                index = parse_size_store(sizestore_region, header.chunk_num, verify_crc=header.chunk_size_crc)
            except DataCorruptError:
                repair_result = await _attempt_size_store_repair(store, path, header, sizestore_region)
                if repair_result is None:
                    raise
                repaired, known_file_size = repair_result
                sizestore_repaired = True
                # verify_crc=None: the repair already confirmed the CRC.
                index = parse_size_store(repaired, header.chunk_num, verify_crc=None)
        else:
            # Uncompressed layout: no SizeStore at all, every chunk is
            # implicitly CompressType.NONE (FORMAT-SPEC.md: Header & mode bits).
            index = BucketIndex.uncompressed(header.chunk_num)
        return cls(
            store,
            path,
            header,
            index,
            vault_key,
            algorithm=algorithm,
            verify_ciphertext_crc=verify_ciphertext_crc,
            sizestore_repaired=sizestore_repaired,
            known_file_size=known_file_size,
        )

    async def ensure_chunk_crc_store(self) -> tuple[int, ...]:
        """Lazily read and self-validate this bucket's ChunkCrcStore trailer
        (FORMAT-SPEC.md: ChunkCrcStore & Redundancy), caching the per-chunk
        ciphertext CRC32 values.

        Returns:
            The CRC32 values, one per stored chunk.

        Raises:
            FormatError: The trailer is shorter than declared.
            DataCorruptError: The trailer's bytes don't match the
                header's ``crcOfChunkCrc`` self-consistency field.
        """
        if self._chunk_crc_values is None:
            offset, length = chunk_crc_store_region(self.header, self.index)
            if length == 0:
                self._chunk_crc_values = ()
            else:
                trailer_raw = await self._store.read(self.path, offset, length)
                self._chunk_crc_values = parse_chunk_crc_store(
                    trailer_raw, length // CHUNK_CRC_SIZE, verify_crc=self.header.crc_of_chunk_crc
                )
            # One O(chunk_num) pass, not per-chunk-per-call.
            self._chunk_crc_positions = self.index.crc_store_positions()
        return self._chunk_crc_values

    def _check_ciphertext_crc(self, chunk_idx: int, raw: bytes | memoryview) -> None:
        """``raw`` against chunk ``chunk_idx``'s ChunkCrcStore entry; the
        caller has awaited ``ensure_chunk_crc_store()`` (sync, so a worker
        thread's ``_decode_run`` can call it)."""
        assert self._chunk_crc_values is not None and self._chunk_crc_positions is not None, (
            "ensure_chunk_crc_store() must run before a ciphertext CRC check"
        )
        expected = self._chunk_crc_values[self._chunk_crc_positions[chunk_idx]]
        verify_crc32(raw, expected, label="chunk ciphertext", spec=_SPEC)

    def _chunk_decryptor(self) -> ChunkDecryptor:
        """A ``ChunkDecryptor`` on this bucket's key, for an encrypted bucket.

        Raises:
            KeyRequiredError: No vault key was given.
        """
        if self._vault_key is None:
            raise KeyRequiredError(f"{self.path!r} is encrypted but no vault key was provided", ref=self.path)
        return ChunkDecryptor(self._vault_key, algorithm=self._algorithm)

    def _decrypt(self, chunk_idx: int, addr: ChunkAddress | None, raw: bytes | memoryview) -> bytes | memoryview:
        """``raw`` decrypted with its own IV when the bucket is
        vault-encrypted, else unchanged.

        Raises:
            KeyRequiredError: The bucket is encrypted and no vault key was given.
        """
        if not self.header.is_vault_encrypted:
            return raw
        if self._vault_key is None:
            raise KeyRequiredError(f"{self.path!r} is encrypted but no vault key was provided", ref=self.path)
        assert addr is not None, (
            f"chunk {chunk_idx} of {self.path!r} is vault-encrypted but was read with no "
            "ChunkAddress — a caller may only omit addr when the bucket isn't encrypted"
        )
        return decrypt_chunk(self._vault_key, addr, raw, algorithm=self._algorithm)

    async def verify_chunk_ciphertext_crc(self, chunk_idx: ChunkIdx) -> None:
        """Check chunk ``chunk_idx``'s stored bytes against its
        ``ChunkCrcStore`` entry without decrypting, so no vault key is needed.

        Raises:
            DataCorruptError: The ciphertext doesn't match its entry.
            ChunkCompactedError: ``chunk_idx`` is ``COMPACTED``.
        """
        raw = await self.read_raw_chunk(chunk_idx)
        await self.verify_raw_chunk_ciphertext_crc(chunk_idx, raw)

    async def verify_raw_chunk_ciphertext_crc(self, chunk_idx: int, raw: bytes | memoryview) -> None:
        """``verify_chunk_ciphertext_crc`` for a caller that already holds
        ``chunk_idx``'s stored bytes (e.g. from ``read_raw_chunks``).

        Raises:
            DataCorruptError: ``raw`` doesn't match ``chunk_idx``'s own
                ``ChunkCrcStore`` entry.
        """
        await self.ensure_chunk_crc_store()
        self._check_ciphertext_crc(chunk_idx, raw)

    def non_compacted_chunk_ranges(self) -> list[tuple[int, int]]:
        """Every chunk with readable data, as ascending ``(chunk_idx_start,
        length)`` runs that skip ``COMPACTED`` slots (reclaimed, not
        corruption). Scans the raw compress-type bytes in C, not per chunk."""
        return [
            (match.start(), match.end() - match.start())
            for match in _NON_COMPACTED_RUN.finditer(self.index.compress_types.tobytes())
        ]

    def _resolve_compress_type(self, chunk_idx: int) -> CompressType:
        """Chunk ``chunk_idx``'s ``CompressType`` from the raw arrays.

        Raises:
            ChunkCompactedError: The chunk is ``COMPACTED``.
        """
        ctype_value = self.index.compress_types[chunk_idx]
        if ctype_value == COMPRESS_TYPE_COMPACTED_VALUE:
            raise ChunkCompactedError(
                f"chunk {chunk_idx} of {self.path!r} is COMPACTED — reclaimed, cannot be recovered",
                ref=self.path,
            )
        return COMPRESS_TYPE_BY_VALUE[ctype_value]

    async def read_chunk(self, chunk_idx: ChunkIdx, addr: ChunkAddress) -> bytes:
        """Decrypt (if vault-encrypted) and decompress chunk ``chunk_idx``,
        first checking its stored bytes against its ``ChunkCrcStore`` entry
        (FORMAT-SPEC.md: ChunkCrcStore & Redundancy) when opened with
        ``verify_ciphertext_crc`` (one trailer read per bucket).

        Args:
            chunk_idx: The chunk within this bucket.
            addr: The chunk's own ``ChunkAddress``, supplying the decryption IV.

        Returns:
            Exactly 4096 bytes of plaintext.

        Raises:
            ChunkCompactedError: The chunk is ``COMPACTED``.
            KeyRequiredError: The bucket is encrypted and no vault key was given.
            DataCorruptError: The CRC check fails or the chunk doesn't
                decompress to 4096 bytes.
        """
        self._resolve_compress_type(chunk_idx)  # fail fast on COMPACTED, before spending a real read
        offset = self.index.offsets[chunk_idx]
        length = self.index.effective_lens[chunk_idx]
        raw = await self._store.read(self.path, offset, length)
        return await self.decode_raw_chunk(chunk_idx, addr, raw)

    async def decode_raw_chunk(self, chunk_idx: int, addr: ChunkAddress, raw: bytes | memoryview) -> bytes:
        """``read_chunk`` for a caller that already holds ``chunk_idx``'s stored
        bytes (e.g. from ``read_raw_chunks``)."""
        compress_type = self._resolve_compress_type(chunk_idx)
        if self._verify_ciphertext_crc:
            await self.ensure_chunk_crc_store()
            self._check_ciphertext_crc(chunk_idx, raw)
        return decompress(compress_type, self._decrypt(chunk_idx, addr, raw))

    async def read_raw_chunk(self, chunk_idx: ChunkIdx) -> bytes:
        """Chunk ``chunk_idx``'s stored bytes exactly as written (still
        compressed and/or encrypted), for ChunkCrcStore checks
        (FORMAT-SPEC.md: ChunkCrcStore & Redundancy).

        Raises:
            ChunkCompactedError: ``chunk_idx`` is ``COMPACTED``.
        """
        self._resolve_compress_type(chunk_idx)
        offset = self.index.offsets[chunk_idx]
        length = self.index.effective_lens[chunk_idx]
        return await self._store.read(self.path, offset, length)

    async def read_raw_chunks(self, chunk_indices: Sequence[int]) -> dict[int, bytes | memoryview]:
        """Batch form of ``read_raw_chunk``: locator byte-ranges within
        ``_GAP_TOLERANCE`` of each other share one ``ObjectStore.read``.

        Args:
            chunk_indices: Sorted ascending and deduplicated.

        Raises:
            ChunkCompactedError: A chunk is ``COMPACTED``; raised before any read.
        """
        result: dict[int, bytes | memoryview] = {}
        for chunk_idx in chunk_indices:
            self._resolve_compress_type(chunk_idx)  # raises on COMPACTED; the type is unused
        offsets, lengths = self.index.offsets, self.index.effective_lens
        for run in self._plan_index_runs([(chunk_idx, 1) for chunk_idx in chunk_indices]):
            merged = await self._store.read(self.path, run.start, run.end - run.start)
            for sub_start, sub_end in run.chunks:
                for chunk_idx in range(sub_start, sub_end):
                    local_off = offsets[chunk_idx] - run.start
                    result[chunk_idx] = merged[local_off : local_off + lengths[chunk_idx]]
        return result

    async def read_chunks(
        self,
        stream_id: StreamId,
        bucket_id: BucketId,
        ranges: Sequence[tuple[int, int]],
        *,
        semaphore: asyncio.Semaphore | None = None,
    ) -> dict[int, bytes | memoryview]:
        """Batch form of ``read_chunk``: locator byte-ranges within
        ``_GAP_TOLERANCE`` of each other share one ``ObjectStore.read``;
        decrypt is per chunk and decompress is batched per merged run
        (``_decode_run``/``decompress_many``).

        Args:
            stream_id: This bucket's stream, for an encrypted chunk's IV.
            bucket_id: This bucket's id, likewise.
            ranges: ``(chunk_idx_start, length)`` runs, sorted ascending and
                non-overlapping (not re-sorted here).
            semaphore: Shared read-concurrency bound (``None``: serial), the
                one ``exec_chunks()`` draws from. The caller has already
                acquired one permit for this call, which the first merged run
                spends; each further physically separate run acquires its own.

        Returns:
            Plaintext by ``chunk_idx``. A value may be a ``memoryview`` into
            a buffer its whole run shares; holding it keeps that buffer
            alive (copy with ``bytes()`` to keep one chunk).

        Raises:
            IndexError: A range runs past this bucket's chunks.
            ChunkCompactedError: A requested chunk is ``COMPACTED``; raised
                before any read.
            KeyRequiredError: The bucket is encrypted and no vault key was given.
            DataCorruptError: A CRC check fails or a chunk doesn't decompress
                to 4096 bytes.
        """
        result: dict[int, bytes | memoryview] = {}
        if not ranges:
            return result

        if self._verify_ciphertext_crc:
            # _decode_run() runs on a worker thread and cannot await this.
            await self.ensure_chunk_crc_store()

        compress_types = memoryview(self.index.compress_types)
        for start, length in ranges:
            if start < 0 or start + length > len(compress_types):
                raise IndexError(
                    f"chunks [{start}, {start + length}) are outside {self.path!r}'s {len(compress_types)}"
                )
            compacted = compress_types[start : start + length].tobytes().find(COMPRESS_TYPE_COMPACTED_VALUE)
            if compacted >= 0:
                self._resolve_compress_type(start + compacted)  # raises ChunkCompactedError

        runs = self._plan_index_runs(ranges)
        address = (stream_id, bucket_id)

        if semaphore is None or len(runs) <= 1:
            # Fast path, no TaskGroup: len(runs) <= 1 means the one permit
            # the caller already acquired covers this single run.
            for run in runs:
                await self._read_run(address, run, result)
        else:
            # Several physically separate regions need several reads anyway;
            # fan the extra runs out to overlap their latency.
            async def _read_extra(run: _IndexRun) -> None:
                async with semaphore:
                    await self._read_run(address, run, result)

            async with asyncio.TaskGroup() as tg:
                # runs[0] spends the caller's permit. Runs write disjoint keys
                # of ``result``, so no lock is needed.
                tg.create_task(self._read_run(address, runs[0], result))
                for run in runs[1:]:
                    tg.create_task(_read_extra(run))
        return result

    def _plan_index_runs(self, ranges: Sequence[tuple[int, int]]) -> list[_IndexRun]:
        """Merges chunk-index ranges into runs in one pass, without I/O, so
        every run is known before the first read: chunks whose bytes start
        within ``_GAP_TOLERANCE`` after the previous chunk's end share a run,
        read straight off the raw offset/length arrays."""
        offsets, lengths = self.index.offsets, self.index.effective_lens
        runs: list[_IndexRun] = []
        current: list[tuple[int, int]] | None = None
        run_start = run_end = 0
        for start, length in ranges:
            sub_start = start
            for chunk_idx in range(start, start + length):
                offset = offsets[chunk_idx]
                if current is not None and 0 <= offset - run_end <= _GAP_TOLERANCE:
                    run_end = offset + lengths[chunk_idx]
                    continue
                if current is not None:
                    if chunk_idx > sub_start:
                        current.append((sub_start, chunk_idx))
                    runs.append(_IndexRun(run_start, run_end, current))
                current, sub_start = [], chunk_idx
                run_start, run_end = offset, offset + lengths[chunk_idx]
            assert current is not None
            current.append((sub_start, start + length))
        if current is not None:
            runs.append(_IndexRun(run_start, run_end, current))
        return runs

    async def _read_run(
        self, address: tuple[StreamId, BucketId], run: _IndexRun, result: dict[int, bytes | memoryview]
    ) -> None:
        """Fetch one merged byte range, then decode its chunks on a worker
        thread so a multi-MB decode doesn't stall the event loop; a
        ``SyncReadable`` store's read happens on that same thread, one hop
        per run instead of two. ``read_chunk`` does not hop: one 4096-byte
        decode costs less than the hop. ``read_chunks`` has already loaded
        the ChunkCrcStore.
        """
        if isinstance(self._store, SyncReadable):
            await asyncio.to_thread(self._read_and_decode_run, self._store.read_sync, address, run, result)
            return
        merged = await self._store.read(self.path, run.start, run.end - run.start)
        # A memoryview makes every per-chunk slice a zero-copy view of this buffer.
        await asyncio.to_thread(self._decode_run, address, run, memoryview(merged), result)

    def _read_and_decode_run(
        self,
        read_sync: Callable[[str, int, int], bytes],
        address: tuple[StreamId, BucketId],
        run: _IndexRun,
        result: dict[int, bytes | memoryview],
    ) -> None:
        """``_read_run``'s body on one worker thread, for a ``SyncReadable`` store."""
        merged = read_sync(self.path, run.start, run.end - run.start)
        self._decode_run(address, run, memoryview(merged), result)

    def _decode_run(
        self,
        address: tuple[StreamId, BucketId],
        run: _IndexRun,
        merged: memoryview,
        result: dict[int, bytes | memoryview],
    ) -> None:
        """Check (when opened with ``verify_ciphertext_crc``) and decrypt each
        chunk in ``run``, own IV per chunk, then decompress the whole run with
        one ``decompress_many`` call."""
        offsets, lengths, compress_types = self.index.offsets, self.index.effective_lens, self.index.compress_types
        verify = self._verify_ciphertext_crc
        encrypted = self.header.is_vault_encrypted
        # Built per call (_decode_run runs on worker threads, and a context
        # isn't thread-safe), at the first decrypt: a chunk's ciphertext CRC
        # is checked before a missing key is reported, as for a single chunk.
        decryptor: ChunkDecryptor | None = None
        stream_id, bucket_id = address
        chunk_idxs: list[int] = []
        items: list[tuple[CompressType, bytes | memoryview]] = []
        for sub_start, sub_end in run.chunks:
            for chunk_idx in range(sub_start, sub_end):
                local_off = offsets[chunk_idx] - run.start
                raw: bytes | memoryview = merged[local_off : local_off + lengths[chunk_idx]]
                if verify:
                    self._check_ciphertext_crc(chunk_idx, raw)
                if encrypted:
                    if decryptor is None:
                        decryptor = self._chunk_decryptor()
                    raw = decryptor.decrypt(ChunkAddress(stream_id, bucket_id, ChunkIdx(chunk_idx)), raw)
                chunk_idxs.append(chunk_idx)
                items.append((COMPRESS_TYPE_BY_VALUE[compress_types[chunk_idx]], raw))
        result.update(zip(chunk_idxs, decompress_many(items), strict=True))
