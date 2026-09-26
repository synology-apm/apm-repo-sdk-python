"""``BucketReader``: one ``.buk`` file's header, SizeStore entries,
per-chunk locators, and the decrypt+decompress read path itself. Only
ever calls ``ObjectStore.read`` — never mmaps directly, never assumes
anything about the backend.
"""

from __future__ import annotations

import array
import asyncio
from collections.abc import Sequence

from ...errors import ChunkCompactedError, DataCorruptError, FormatError, KeyRequiredError, NotFoundError
from ...format.addressing import ChunkAddress
from ...format.bucket import (
    COMPRESS_TYPE_BY_VALUE,
    COMPRESS_TYPE_COMPACTED_VALUE,
    BucketFileHeader,
    SizeStoreEntry,
    chunk_crc_store_positions,
    chunk_crc_store_region,
    chunk_locators,
    chunk_size_store_tight_length,
    parse_bucket_header,
    parse_chunk_crc_store,
    parse_size_store,
    raw_chunk_arrays,
)
from ...format.compression import CompressType, decompress, decompress_many
from ...format.const import CHUNK_CRC_SIZE, COMPRESS_RESERVED_LENG, REDUNDANCY_COVERAGE_BUCKET
from ...format.crypto import decrypt_chunk
from ...format.headers import HEADER_LEN, verify_crc32
from ...format.redundancy import redundancy_size, repair_via_trailer
from ...identifiers import ChunkIdx
from ...storage.base import ObjectStore

_SPEC = "FORMAT-SPEC.md: sidecar-files/chunk-pool-encryption"

#: Merged multi-chunk read: locator byte-ranges within this many bytes of
#: each other are merged into one ``ObjectStore.read`` call instead of
#: one per chunk, trading read amplification for fewer seeks.
#:
#: No size cap on a merged run — a bucket's own chunk index space already
#: bounds it, and a fixed byte cap tends to split genuinely contiguous
#: ranges into extra reads more often than it protects against a real gap.
_GAP_TOLERANCE = 1 << 20  # 1 MiB


async def _attempt_size_store_repair(
    store: ObjectStore, path: str, header: BucketFileHeader, sizestore_region: bytes
) -> tuple[bytes, int] | None:
    """On a ``chunk_size_crc`` mismatch, lazily fetch this bucket's
    trailing Redundancy blob (FORMAT-SPEC.md: ChunkCrcStore & Redundancy —
    the last thing in the file, after the chunk-data region and the
    ChunkCrcStore trailer) and attempt in-memory self-repair
    (``format.redundancy.repair_via_trailer``).

    The trailer's file offset is derived from the actual on-disk file
    size (one extra ``store.size()`` call, only on this failure path)
    minus the trailer's own size, which depends only on
    ``header.chunk_num`` — deliberately not from the SizeStore's own
    decoded chunk lengths, which would be circular: those lengths are
    exactly what may be wrong here.

    Returns ``(repaired, file_size)`` — the repaired tight SizeStore
    bytes plus the file size already fetched, so ``BucketReader.open``
    can hand it forward for ``check_bucket_structure`` to reuse — on a
    confirmed-correct reconstruction, or ``None`` if the file
    size/trailer can't be fetched or the reconstruction still doesn't
    validate.
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


class BucketReader:
    """``entries``/``locators`` (lazy ``Sequence``\\ s) are for external,
    sparse-access callers that only touch a handful of a bucket's chunks
    (``verify_checks.py``'s spot-checks, the ``dump`` CLI command). This
    class's own per-chunk reads (``read_chunk``, ``read_chunks``) go
    straight to the raw ``array.array`` values ``raw_chunk_arrays``
    returns instead, skipping per-chunk construction entirely — a
    bucket-major export touches nearly every chunk in nearly every
    bucket it opens, dense enough that even lazy construction adds up.
    """

    def __init__(
        self,
        store: ObjectStore,
        path: str,
        header: BucketFileHeader,
        entries: Sequence[SizeStoreEntry],
        vault_key: bytes | None,
        *,
        default_verify_ciphertext_crc: bool = False,
        sizestore_repaired: bool = False,
        known_file_size: int | None = None,
    ) -> None:
        self._store = store
        self.path = path
        self.header = header
        self.entries = entries
        self.locators = chunk_locators(header, entries)
        # Set by open() when this bucket's SizeStore CRC mismatch was
        # already fixed via Redundancy-blob parity. check_bucket_structure
        # turns this into a Symptom.REPAIRED_VIA_PARITY finding.
        self.sizestore_repaired = sizestore_repaired
        # Set alongside sizestore_repaired when repair already had to fetch
        # this file's actual size to locate its trailer -- check_bucket_
        # structure reuses it instead of a second store.size() round-trip.
        self.known_file_size = known_file_size
        self._vault_key = vault_key
        # Session-wide default for read_chunk()/read_chunks()'s own
        # verify_ciphertext_crc= — same None-defers-to-this shape as
        # Pool.read_chunk's verify_fingerprint, one layer down (ChunkCrcStore
        # lives inside this bucket file, unlike fingerprints' separate
        # .inf/.fgp sidecars).
        self._default_verify_ciphertext_crc = default_verify_ciphertext_crc
        # Lazily read+self-validated by ensure_chunk_crc_store() on first
        # use — unlike SizeStore's CRC, the trailer needs its own extra
        # fetch past what open() already has in hand, so it's never paid
        # for unless a caller asks for a ciphertext-CRC check.
        self._chunk_crc_values: tuple[int, ...] | None = None
        # Computed alongside _chunk_crc_values — chunk_crc_store_index() is
        # O(chunk_idx) per call, quadratic if called once per chunk the way
        # read_chunks()'s per-chunk verify pass does; this cumulative array
        # makes each lookup O(1) instead.
        self._chunk_crc_positions: array.array[int] | None = None
        # read_chunks()/_decode_run()'s own raw-array fast path — they
        # bypass entries/locators entirely instead of also using (even
        # lazy) SizeStoreEntry/ChunkLocator construction.
        self._raw_compress_types, self._raw_offsets, self._raw_lengths = raw_chunk_arrays(
            header, entries, self.locators
        )

    @classmethod
    async def open(
        cls, store: ObjectStore, path: str, *, vault_key: bytes | None = None, verify_ciphertext_crc: bool = False
    ) -> BucketReader:
        """Open ``path`` and parse its header + SizeStore in one read of up
        to ``COMPRESS_RESERVED_LENG`` (16384) bytes.

        SizeStore's CRC is **always** verified here (not configurable to
        skip — computing chunk locators already requires reading the
        SizeStore bytes, so validating them is free). A CRC mismatch
        first tries the bucket's own trailing Redundancy blob (see
        ``_attempt_size_store_repair``) — every current-format bucket
        carries one — making this the one place in the SDK where parity
        self-repair is transparent on every open, not just an explicit
        ``verify``/``diagnostics`` check. A successful repair sets the
        returned reader's ``sizestore_repaired``; ``verify_checks.
        check_bucket_structure`` is what turns that into a ``Finding``.

        ``verify_ciphertext_crc`` becomes this reader's
        ``read_chunk``/``read_chunks`` default — it does not itself
        trigger reading the ChunkCrcStore trailer here.
        """
        head = await store.read(path, 0, COMPRESS_RESERVED_LENG)
        header = parse_bucket_header(head)
        entries: Sequence[SizeStoreEntry]
        sizestore_repaired = False
        known_file_size: int | None = None
        if header.is_compressed:
            sizestore_region = head[HEADER_LEN:COMPRESS_RESERVED_LENG]
            try:
                entries = parse_size_store(sizestore_region, header.chunk_num, verify_crc=header.chunk_size_crc)
            except DataCorruptError:
                repair_result = await _attempt_size_store_repair(store, path, header, sizestore_region)
                if repair_result is None:
                    raise
                repaired, known_file_size = repair_result
                sizestore_repaired = True
                # verify_crc=None: attempt_repair() already confirmed this
                # candidate's own CRC32 matches header.chunk_size_crc byte
                # for byte -- re-checking it here would be redundant.
                entries = parse_size_store(repaired, header.chunk_num, verify_crc=None)
        else:
            # Uncompressed layout: no SizeStore at all, every chunk is
            # implicitly CompressType.NONE (FORMAT-SPEC.md: bucket-header).
            entries = [SizeStoreEntry(CompressType.NONE, 0) for _ in range(header.chunk_num)]
        return cls(
            store,
            path,
            header,
            entries,
            vault_key,
            default_verify_ciphertext_crc=verify_ciphertext_crc,
            sizestore_repaired=sizestore_repaired,
            known_file_size=known_file_size,
        )

    async def ensure_chunk_crc_store(self) -> tuple[int, ...]:
        """Lazily read and self-validate this bucket's ChunkCrcStore
        trailer (FORMAT-SPEC.md: ChunkCrcStore), caching the per-chunk
        ciphertext CRC32 values for ``read_chunk``/``read_chunks``'s own
        ``verify_ciphertext_crc`` option, and for a caller doing its own
        one-off structural check, without a second read of the same
        bytes.

        Raises:
            FormatError: The trailer is shorter than declared.
            DataCorruptError: The trailer's bytes don't match the
                header's ``crcOfChunkCrc`` self-consistency field.
        """
        if self._chunk_crc_values is None:
            offset, length = chunk_crc_store_region(self.header, self.entries)
            if length == 0:
                self._chunk_crc_values = ()
            else:
                trailer_raw = await self._store.read(self.path, offset, length)
                self._chunk_crc_values = parse_chunk_crc_store(
                    trailer_raw, length // CHUNK_CRC_SIZE, verify_crc=self.header.crc_of_chunk_crc
                )
            # One O(chunk_num) pass, not per-chunk-per-call.
            self._chunk_crc_positions = chunk_crc_store_positions(self.entries)
        return self._chunk_crc_values

    async def _verify_chunk_ciphertext_crc(self, chunk_idx: int, raw: bytes | memoryview) -> None:
        values = await self.ensure_chunk_crc_store()
        assert self._chunk_crc_positions is not None  # populated by ensure_chunk_crc_store() just above
        verify_crc32(raw, values[self._chunk_crc_positions[chunk_idx]], label="chunk ciphertext", spec=_SPEC)

    async def verify_chunk_ciphertext_crc(self, chunk_idx: ChunkIdx) -> None:
        """Check chunk ``chunk_idx``'s stored bytes against its
        ``ChunkCrcStore`` entry, with no decrypt/decompress — unlike
        ``read_chunk``'s own ``verify_ciphertext_crc=`` option (which
        still decrypts afterward, needing a vault key), this works
        whether or not a key is available. For a caller (``verify``)
        that wants this check in isolation, independent of
        ``KeyRequiredError``.

        Raises:
            DataCorruptError: The chunk's ciphertext doesn't match its
                ``ChunkCrcStore`` entry.
        """
        raw = await self.read_raw_chunk(chunk_idx)
        await self.verify_raw_chunk_ciphertext_crc(chunk_idx, raw)

    async def verify_raw_chunk_ciphertext_crc(self, chunk_idx: int, raw: bytes | memoryview) -> None:
        """The second half of ``verify_chunk_ciphertext_crc``, split out
        for a caller that already has ``chunk_idx``'s stored bytes in
        hand (e.g. from a batched ``read_raw_chunks`` call).

        Raises:
            DataCorruptError: ``raw`` doesn't match ``chunk_idx``'s own
                ``ChunkCrcStore`` entry.
        """
        await self._verify_chunk_ciphertext_crc(chunk_idx, raw)

    def non_compacted_chunk_indices(self) -> list[int]:
        """Every chunk index with real, readable data — skips ``COMPACTED``
        slots (reclaimed, not corruption). Built from the raw
        compress-type array, so a whole-bucket sweep costs one
        O(chunk_num) pass with no per-chunk ``SizeStoreEntry``
        construction — a caller wanting just one representative chunk
        should index ``entries`` directly instead."""
        return [i for i, ctype in enumerate(self._raw_compress_types) if ctype != COMPRESS_TYPE_COMPACTED_VALUE]

    def _resolve_compress_type(self, chunk_idx: int) -> CompressType:
        """Look up chunk ``chunk_idx``'s ``CompressType`` off the raw
        arrays, raising ``ChunkCompactedError`` for the COMPACTED sentinel —
        shared by ``read_chunk``'s and ``read_chunks``'s identical
        per-chunk resolution."""
        ctype_value = self._raw_compress_types[chunk_idx]
        if ctype_value == COMPRESS_TYPE_COMPACTED_VALUE:
            raise ChunkCompactedError(
                f"chunk {chunk_idx} of {self.path!r} is COMPACTED — reclaimed, cannot be recovered",
                ref=self.path,
            )
        return COMPRESS_TYPE_BY_VALUE[ctype_value]

    async def read_chunk(
        self, chunk_idx: ChunkIdx, addr: ChunkAddress, *, verify_ciphertext_crc: bool | None = None
    ) -> bytes:
        """Decrypt (if the bucket is vault-encrypted) and decompress chunk
        ``chunk_idx``, returning exactly 4096 bytes of plaintext.

        ``addr`` supplies the IV for decryption — pass the chunk's own
        ``ChunkAddress``.

        Reads straight off the raw arrays rather than
        ``entries``/``locators`` — no ``SizeStoreEntry``/``ChunkLocator``
        construction needed once the raw arrays already exist.

        ``verify_ciphertext_crc`` (``None``: defer to this reader's
        ``open()`` default) compares this chunk's stored bytes against
        its ``ChunkCrcStore`` entry (FORMAT-SPEC.md: ChunkCrcStore)
        before decrypting — off by default since it costs an extra
        trailer read the first time it's used per bucket.
        """
        self._resolve_compress_type(chunk_idx)  # fail fast on COMPACTED, before spending a real read
        offset = self._raw_offsets[chunk_idx]
        length = self._raw_lengths[chunk_idx]
        raw = await self._store.read(self.path, offset, length)
        return await self.decode_raw_chunk(chunk_idx, addr, raw, verify_ciphertext_crc=verify_ciphertext_crc)

    async def decode_raw_chunk(
        self,
        chunk_idx: int,
        addr: ChunkAddress,
        raw: bytes | memoryview,
        *,
        verify_ciphertext_crc: bool | None = None,
    ) -> bytes:
        """The decrypt/decompress half of ``read_chunk``, split out for a
        caller that already has ``chunk_idx``'s stored bytes in hand
        (e.g. from a batched ``read_raw_chunks`` call).

        ``verify_ciphertext_crc``: same meaning as ``read_chunk``'s own
        parameter. A caller that already ran
        ``verify_raw_chunk_ciphertext_crc`` on ``raw`` should pass
        ``False`` here, or this redoes (and raises past) that check.
        """
        compress_type = self._resolve_compress_type(chunk_idx)
        should_verify = self._default_verify_ciphertext_crc if verify_ciphertext_crc is None else verify_ciphertext_crc
        if should_verify:
            await self._verify_chunk_ciphertext_crc(chunk_idx, raw)
        if self.header.is_vault_encrypted:
            if self._vault_key is None:
                raise KeyRequiredError(f"{self.path!r} is encrypted but no vault key was provided", ref=self.path)
            raw = decrypt_chunk(self._vault_key, addr, raw)
        return decompress(compress_type, raw)

    async def read_raw_chunk(self, chunk_idx: ChunkIdx) -> bytes:
        """Fetch chunk ``chunk_idx``'s stored bytes exactly as written —
        compressed and/or encrypted per ``mode``, no decrypt/decompress.
        For ``verify_checks.py``'s ChunkCrcStore spot-check
        (FORMAT-SPEC.md: ChunkCrcStore), which checks the ciphertext
        CRC32 directly.

        Raises:
            ChunkCompactedError: ``chunk_idx`` is ``COMPACTED`` — checked
                first, since a compacted chunk has no ChunkCrcStore entry
                of its own.
        """
        self._resolve_compress_type(chunk_idx)
        offset = self._raw_offsets[chunk_idx]
        length = self._raw_lengths[chunk_idx]
        return await self._store.read(self.path, offset, length)

    async def read_raw_chunks(self, chunk_indices: Sequence[int]) -> dict[int, bytes | memoryview]:
        """Batch form of ``read_raw_chunk``: merges ``chunk_indices``' own
        locator byte-ranges within ``_GAP_TOLERANCE`` of each other into
        a single ``ObjectStore.read`` call instead of one read per
        chunk, minus the decrypt/decompress step neither this nor
        ``read_raw_chunk`` performs. Matters most against a
        remote-object-storage backend, where every separate read is its
        own round trip.

        Args:
            chunk_indices: Must already be sorted ascending and
                deduplicated — same contract as ``read_chunks``'
                ``requests``.

        Raises:
            ChunkCompactedError: For the first ``COMPACTED`` chunk
                found, raised up front before any read happens.
        """
        result: dict[int, bytes | memoryview] = {}
        if not chunk_indices:
            return result

        located: list[tuple[int, int, int]] = []
        for chunk_idx in chunk_indices:
            self._resolve_compress_type(
                chunk_idx
            )  # validates, raises ChunkCompactedError -- the type itself is unused here
            located.append((chunk_idx, self._raw_offsets[chunk_idx], self._raw_lengths[chunk_idx]))

        runs: list[list[tuple[int, int, int]]] = []
        for item in located:
            _, offset, _ = item
            if runs and self._fits_in_run(runs[-1][-1][1] + runs[-1][-1][2], offset):
                runs[-1].append(item)
            else:
                runs.append([item])

        for run in runs:
            run_start = run[0][1]
            run_end = run[-1][1] + run[-1][2]
            merged = await self._store.read(self.path, run_start, run_end - run_start)
            for chunk_idx, offset, length in run:
                local_off = offset - run_start
                result[chunk_idx] = merged[local_off : local_off + length]
        return result

    async def read_chunks(
        self,
        requests: Sequence[tuple[int, ChunkAddress | None]],
        *,
        semaphore: asyncio.Semaphore | None = None,
        verify_ciphertext_crc: bool | None = None,
    ) -> dict[int, bytes | memoryview]:
        """Batch form of ``read_chunk``: merges ``requests``' locator
        byte-ranges within ``_GAP_TOLERANCE`` of each other into a single
        ``ObjectStore.read`` call instead of one per chunk, then
        decrypts/decompresses each chunk out of its own slice of
        whichever merged buffer it landed in. Decrypt stays per-chunk;
        decompress is batched across a whole merged run via
        ``_decode_run``/``decompress_many``.

        Args:
            requests: Must already be sorted by ``chunk_idx`` ascending
                and deduplicated — not re-sorted here, so a caller bug
                surfaces as a wrong/inefficient merge rather than being
                hidden. Each ``addr`` may be ``None`` when the caller
                already knows this bucket isn't vault-encrypted.
            semaphore: The shared concurrency pool for this whole export
                (default ``None``: serial) — the same
                ``asyncio.Semaphore`` ``exec_chunks()``'s cross-bucket
                dispatch loop draws from, so total in-flight reads across
                both levels never exceed one shared bound. A caller
                passing a non-``None`` semaphore has already acquired one
                permit on this call's behalf; this method's first merged
                run spends that permit, and only a further run (chunks
                landing in more than one physically-separate region)
                acquires its own additional permit from the same pool.
            verify_ciphertext_crc: Same meaning as ``read_chunk``'s own
                parameter, checked per chunk before decrypting.

        Returns:
            A chunk's value may be a ``memoryview`` into either
            ``decompress_many``'s shared per-run decode buffer or this
            method's own merged read buffer, never copied to satisfy
            this method's own contract — every consumer today only reads
            it forward into another buffer, never holding it past its
            own call.

        Raises:
            ChunkCompactedError: For the first COMPACTED chunk found in
                ``requests``, raised up front before any read happens.
        """
        result: dict[int, bytes | memoryview] = {}
        if not requests:
            return result

        should_verify = self._default_verify_ciphertext_crc if verify_ciphertext_crc is None else verify_ciphertext_crc
        if should_verify:
            # Resolved once, up front — _decode_run() runs on a worker
            # thread and can't await this itself, so the cache must
            # already be warm.
            await self.ensure_chunk_crc_store()

        # Off the raw arrays, not entries/locators: located/run carry the
        # already-resolved CompressType alongside plain offset/length ints,
        # so _decode_run() never looks a chunk's compress_type up again.
        located: list[tuple[int, ChunkAddress | None, int, int, CompressType]] = []
        for chunk_idx, addr in requests:
            compress_type = self._resolve_compress_type(chunk_idx)
            located.append((chunk_idx, addr, self._raw_offsets[chunk_idx], self._raw_lengths[chunk_idx], compress_type))

        runs = self._plan_runs(located)

        if semaphore is None or len(runs) <= 1:
            # Fast path, no TaskGroup: len(runs) <= 1 means the one permit
            # the caller already acquired covers this single run.
            for run in runs:
                await self._read_run(run, result, verify_ciphertext_crc=should_verify)
        else:
            # A bucket whose requested chunks span more than one
            # physically-separate region already needs more than one
            # real GET regardless — fanning the "extra" runs out here
            # hides each one's own request latency behind the others
            # instead of paying for them one at a time.
            async def _read_extra(run: list[tuple[int, ChunkAddress | None, int, int, CompressType]]) -> None:
                # An "extra" run — beyond the first — acquires its own
                # fresh permit from the shared pool and releases it as
                # soon as this one run finishes, independent of how long
                # the caller's own pre-acquired permit (spent on runs[0]
                # below) stays held.
                async with semaphore:
                    await self._read_run(run, result, verify_ciphertext_crc=should_verify)

            async with asyncio.TaskGroup() as tg:
                # runs[0] spends the permit the caller already acquired.
                # Concurrent runs write disjoint keys into ``result`` (each
                # run's own chunk_idx set never overlaps another's, by
                # construction), so no lock is needed around it.
                tg.create_task(self._read_run(runs[0], result, verify_ciphertext_crc=should_verify))
                for run in runs[1:]:
                    tg.create_task(_read_extra(run))
        return result

    @staticmethod
    def _fits_in_run(prev_end: int, offset: int) -> bool:
        """Whether the next locator range (starting at ``offset``) is
        close enough to a run's own last entry (ending at ``prev_end``)
        to merge — gap-only, no size cap: a bucket's chunk-index space
        already bounds how large a merged run can get. Shared by
        ``read_chunks`` and ``read_raw_chunks``, each passing its own run
        shape's offset+length in."""
        gap = offset - prev_end
        return 0 <= gap <= _GAP_TOLERANCE

    @staticmethod
    def _plan_runs(
        located: list[tuple[int, ChunkAddress | None, int, int, CompressType]],
    ) -> list[list[tuple[int, ChunkAddress | None, int, int, CompressType]]]:
        """Pure planning, no I/O — merges ``located``'s locator byte-ranges
        within ``_GAP_TOLERANCE`` of each other (``_fits_in_run``) into
        runs, in one pass (already offset-ordered). Every run
        ``read_chunks`` will need is known before the first byte is
        fetched, the same reason ``chunk_walk.py``'s bucket-major plan
        can decide concurrency ahead of time rather than mid-walk."""
        runs: list[list[tuple[int, ChunkAddress | None, int, int, CompressType]]] = []
        for item in located:
            _, _, offset, _, _ = item
            if runs and BucketReader._fits_in_run(runs[-1][-1][2] + runs[-1][-1][3], offset):
                runs[-1].append(item)
            else:
                runs.append([item])
        return runs

    async def _read_run(
        self,
        run: list[tuple[int, ChunkAddress | None, int, int, CompressType]],
        result: dict[int, bytes | memoryview],
        *,
        verify_ciphertext_crc: bool = False,
    ) -> None:
        """Fetch one merged byte range, then decode its chunks on a real
        OS thread — one hop per merged run, not per chunk, so its cost
        amortizes away.

        The ``asyncio.to_thread()`` hop is about responsiveness, not
        throughput: a multi-MB decrypt+decompress left on the event loop
        would stall every other Task for its duration. ``read_chunk``,
        the single-chunk interactive path, deliberately does not hop —
        one 4096-byte decode is far cheaper than a thread round-trip.

        ``verify_ciphertext_crc`` is passed down from ``read_chunks``
        already resolved (never ``None`` here) — ``ensure_chunk_crc_store``
        has already been awaited by the caller, so ``_decode_run`` below
        can read the cached values synchronously.
        """
        run_start = run[0][2]
        run_end = run[-1][2] + run[-1][3]
        merged = await self._store.read(self.path, run_start, run_end - run_start)
        # Wrapped once, here, rather than left as bytes for _decode_run to
        # slice — every per-chunk slice becomes a zero-copy view into this
        # one buffer. Nothing holds one of these views past this merged
        # run's decode: result only lives until _exec_one_bucket_group's
        # run-assembly loop copies each chunk forward.
        await asyncio.to_thread(
            self._decode_run, run, run_start, memoryview(merged), result, verify_ciphertext_crc=verify_ciphertext_crc
        )

    def _decode_run(
        self,
        run: list[tuple[int, ChunkAddress | None, int, int, CompressType]],
        run_start: int,
        merged: memoryview,
        result: dict[int, bytes | memoryview],
        *,
        verify_ciphertext_crc: bool = False,
    ) -> None:
        """Decrypt each chunk in ``run`` individually (own IV per chunk,
        so this loop can't be collapsed), then hand the whole run's
        ciphertext-removed bytes to ``decompress_many`` in one batched
        call instead of decompressing chunk by chunk.

        Takes each chunk's ``compress_type``/``offset``/``length``
        straight from ``run``, already resolved by ``read_chunks()``.
        ``addr`` is only dereferenced inside the ``is_vault_encrypted``
        branch — a caller that already knows this bucket isn't
        vault-encrypted may pass ``None`` for it.

        ``verify_ciphertext_crc``, when set, checks each chunk's raw
        slice against ``self._chunk_crc_values`` (already resolved by
        ``read_chunks`` before this runs — this method can't await that
        resolution itself, running on a worker thread) before
        decrypting."""
        chunk_idxs: list[int] = []
        items: list[tuple[CompressType, bytes | memoryview]] = []
        for chunk_idx, addr, offset, length, compress_type in run:
            local_off = offset - run_start
            raw: bytes | memoryview = merged[local_off : local_off + length]
            if verify_ciphertext_crc:
                assert self._chunk_crc_values is not None, "read_chunks() must resolve this before calling _decode_run"
                assert self._chunk_crc_positions is not None
                position = self._chunk_crc_positions[chunk_idx]
                verify_crc32(raw, self._chunk_crc_values[position], label="chunk ciphertext", spec=_SPEC)
            if self.header.is_vault_encrypted:
                if self._vault_key is None:
                    raise KeyRequiredError(f"{self.path!r} is encrypted but no vault key was provided", ref=self.path)
                assert addr is not None, (
                    f"chunk {chunk_idx} of {self.path!r} is vault-encrypted but read_chunks() was handed no "
                    "ChunkAddress for it — a caller may only omit addr when the bucket isn't encrypted"
                )
                raw = decrypt_chunk(self._vault_key, addr, raw)
            chunk_idxs.append(chunk_idx)
            items.append((compress_type, raw))
        result.update(zip(chunk_idxs, decompress_many(items), strict=True))
