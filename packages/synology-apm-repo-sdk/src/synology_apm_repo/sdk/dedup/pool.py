"""Chunk pool: resolves a ``ChunkAddress`` to plaintext bytes.

The only place chunk decrypt+decompress happens. Three collaborating
classes: ``BucketReader`` (one ``.buk`` file's header/SizeStore/
locators plus the actual per-chunk read), ``Pool`` (repository-wide
entry point — resolves ``(streamID, bucketID)`` to the right ``.buk``
path and caches both ``BucketReader``\\ s and decoded plaintext chunks),
and ``BucketReaderCache`` (the private, unbounded cache shape a bulk
sweep needs instead of ``Pool``'s own bounded one). See each class's own
docstring for its actual contract.
"""

from __future__ import annotations

import array
import asyncio
import dataclasses
import hashlib
from collections.abc import Mapping, Sequence

from ..asynccache import AsyncKeyedCache
from ..errors import ChunkCompactedError, DataCorruptError, FormatError, KeyRequiredError, NotFoundError
from ..format.addressing import ChunkAddress, pool_layer_path, split_layer_leaf
from ..format.bucket import (
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
from ..format.compression import CompressType, decompress, decompress_many
from ..format.const import CHUNK_CRC_SIZE, COMPRESS_RESERVED_LENG, REDUNDANCY_COVERAGE_BUCKET
from ..format.crypto import decrypt_chunk
from ..format.headers import HEADER_LEN, verify_crc32
from ..format.redundancy import redundancy_size, repair_via_trailer
from ..identifiers import BucketId, ChunkIdx, StreamId
from ..storage.base import ObjectStore, join_path
from ..storage.dircache import DirCache
from ..storage.seqid import resolve_seq_path
from .fingerprint import AllocationTableCache, fingerprints

_SPEC = "FORMAT-SPEC.md: sidecar-files/chunk-pool-encryption"

#: Merged multi-chunk read: locator byte-ranges within this many bytes of
#: each other are merged into one ``ObjectStore.read`` call instead of
#: one per chunk, trading a little read amplification for far fewer seeks.
#:
#: No size cap on a merged run — a bucket's own chunk index space already
#: bounds it (``BUCKET_MAX_CHUNK_NUM`` caps how large one bucket's file can
#: be), and a fixed byte cap tends to split genuinely contiguous locator
#: ranges into extra reads more often than it protects against a real gap.
_GAP_TOLERANCE = 1 << 20  # 1 MiB


async def _attempt_size_store_repair(
    store: ObjectStore, path: str, header: BucketFileHeader, sizestore_region: bytes
) -> tuple[bytes, int] | None:
    """On a ``chunk_size_crc`` mismatch, lazily fetch this bucket's
    trailing Redundancy blob (FORMAT-SPEC.md: ChunkCrcStore & Redundancy —
    the very last thing in the file, after the chunk-data region and the
    ChunkCrcStore trailer) and attempt in-memory self-repair
    (``format.redundancy.repair_via_trailer``).

    The trailer's own file offset is derived from the *actual* on-disk
    file size (one extra ``store.size()`` call, only on this failure
    path) minus the trailer's own size — which depends only on
    ``header.chunk_num`` (always trustworthy, from the header) —
    deliberately **not** from the SizeStore's own decoded chunk lengths,
    which would be circular: those lengths are exactly what may be wrong
    when this is called, so trusting them to locate the very trailer that
    would fix them could point at the wrong bytes entirely.

    Returns ``(repaired, file_size)`` — the repaired *tight* SizeStore
    bytes (exactly ``chunk_size_store_tight_length(header.chunk_num)``
    long) plus the file size already fetched to locate the trailer, so
    ``BucketReader.open`` can hand it forward for ``check_bucket_structure``
    to reuse instead of re-fetching it — on a confirmed-correct
    reconstruction, or ``None`` if the file size/trailer can't be fetched
    or the reconstruction still doesn't validate — either way, the caller
    falls back to raising the original ``DataCorruptError``.
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
    """One ``.buk`` file: header, SizeStore entries, per-chunk locators, and
    the decrypt+decompress read path itself. Only ever calls
    ``ObjectStore.read`` — never mmaps directly, never assumes
    anything about the backend.

    ``entries``/``locators`` (lazy ``Sequence``\\ s) are for *external*,
    sparse-access callers that only ever touch a handful of a bucket's
    chunks (``verify.py``'s spot-checks, the ``dump`` CLI command). This
    class's *own* per-chunk reads (``read_chunk``, ``read_chunks``)
    go straight to the raw ``array.array`` values
    ``raw_chunk_arrays`` returns
    instead, skipping per-chunk ``SizeStoreEntry``/``ChunkLocator``
    construction entirely — a bucket-major export touches nearly every
    chunk in nearly every bucket it opens, dense enough that even lazy
    construction adds up.
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
        # already fixed via its own Redundancy-blob parity before this
        # reader was ever constructed. open() itself has no
        # Finding-returning contract, so check_bucket_structure is what
        # actually turns this into a Symptom.REPAIRED_VIA_PARITY finding.
        self.sizestore_repaired = sizestore_repaired
        # Set alongside sizestore_repaired when repair already had to fetch
        # this file's actual size to locate its trailer -- check_bucket_
        # structure reuses it instead of paying for a second store.size()
        # round-trip on the same path.
        self.known_file_size = known_file_size
        self._vault_key = vault_key
        # Session-wide default for read_chunk()/read_chunks()'s own
        # verify_ciphertext_crc= parameter — same None-defers-to-this
        # shape as Pool.read_chunk's verify_fingerprint, one layer down
        # (ChunkCrcStore lives inside this bucket file itself, unlike
        # fingerprints' separate .inf/.fgp sidecars, which is why this
        # check is owned here rather than by Pool).
        self._default_verify_ciphertext_crc = default_verify_ciphertext_crc
        # Lazily read+self-validated by ensure_chunk_crc_store() on first
        # actual use — unlike SizeStore's CRC, the trailer needs its own
        # extra fetch past the header/SizeStore region already in hand
        # from open(), so it's never paid for until a caller actually asks
        # for a ciphertext-CRC check.
        self._chunk_crc_values: tuple[int, ...] | None = None
        # Computed alongside _chunk_crc_values (same lazy-once trigger) —
        # chunk_crc_store_index() is O(chunk_idx) per call, fine for a
        # one-off single-chunk lookup but quadratic if called once per
        # chunk for every chunk in a bucket, which read_chunks()'s own
        # per-chunk verify_ciphertext_crc pass does; this cumulative array
        # makes each chunk's own lookup O(1) instead.
        self._chunk_crc_positions: array.array[int] | None = None
        # read_chunks()/_decode_run()'s own raw-array fast path — see this
        # class's own docstring for why they bypass entries/locators
        # entirely instead of also using (even lazy) SizeStoreEntry/
        # ChunkLocator construction.
        self._raw_compress_types, self._raw_offsets, self._raw_lengths = raw_chunk_arrays(
            header, entries, self.locators
        )

    @classmethod
    async def open(
        cls, store: ObjectStore, path: str, *, vault_key: bytes | None = None, verify_ciphertext_crc: bool = False
    ) -> BucketReader:
        """Open ``path`` and parse its header + SizeStore in one read of up
        to ``COMPRESS_RESERVED_LENG`` (16384) bytes.

        SizeStore's CRC is **always** verified here (this is
        not configurable to skip — you cannot compute chunk locators
        without first reading the SizeStore bytes, so validating them is
        free at that point, unlike the composition layer's much more
        expensive full-map-array ``mapCrc``). A CRC mismatch is not
        immediately fatal, though: this first tries the bucket's own
        trailing Redundancy blob (see ``_attempt_size_store_repair``) —
        every current-format bucket carries one (FORMAT-SPEC.md:
        ChunkCrcStore & Redundancy) — since this check *does* run on every
        open, unconditionally, this is the one place in the whole SDK
        where parity self-repair is genuinely transparent for every
        caller, not just an explicit ``verify``/``diagnostics`` check. A
        successful repair sets the returned reader's own
        ``sizestore_repaired`` — this method itself has no
        ``Finding``-returning contract, so ``verify_checks.
        check_bucket_structure`` is what actually reports it.

        ``verify_ciphertext_crc`` becomes this reader's own
        ``read_chunk``/``read_chunks`` default (see their own docstrings) —
        it does not itself trigger reading the ChunkCrcStore trailer here.
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
        ``verify_ciphertext_crc`` option — and for a caller doing its own
        one-off structural self-consistency check — to share without a
        second read of the same bytes.

        Raises:
            FormatError: The trailer is shorter than declared.
            DataCorruptError: The trailer's own bytes don't match the header's
                ``crcOfChunkCrc`` self-consistency field.
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
            # One O(chunk_num) pass, not per-chunk-per-call — see this
            # field's own comment in __init__.
            self._chunk_crc_positions = chunk_crc_store_positions(self.entries)
        return self._chunk_crc_values

    async def _verify_chunk_ciphertext_crc(self, chunk_idx: int, raw: bytes | memoryview) -> None:
        values = await self.ensure_chunk_crc_store()
        assert self._chunk_crc_positions is not None  # populated by ensure_chunk_crc_store() just above
        verify_crc32(raw, values[self._chunk_crc_positions[chunk_idx]], label="chunk ciphertext", spec=_SPEC)

    async def verify_chunk_ciphertext_crc(self, chunk_idx: ChunkIdx) -> None:
        """Check chunk ``chunk_idx``'s *stored* bytes against its
        ``ChunkCrcStore`` entry, with no decrypt/decompress attempt at all
        — unlike ``read_chunk``'s own ``verify_ciphertext_crc=`` option
        (which still goes on to decrypt afterward, and so still needs a
        vault key for an encrypted bucket), this works whether or not a
        key is even available. For a caller (``verify``) that wants this
        one check in isolation, independent of ``KeyRequiredError``.

        Raises:
            DataCorruptError: The chunk's ciphertext doesn't match its
                ``ChunkCrcStore`` entry.
        """
        raw = await self.read_raw_chunk(chunk_idx)
        await self.verify_raw_chunk_ciphertext_crc(chunk_idx, raw)

    async def verify_raw_chunk_ciphertext_crc(self, chunk_idx: int, raw: bytes | memoryview) -> None:
        """The second half of ``verify_chunk_ciphertext_crc``, split out
        for a caller that already has ``chunk_idx``'s stored bytes in
        hand — e.g. from a batched ``read_raw_chunks`` call — instead of
        fetching them itself.

        Raises:
            DataCorruptError: ``raw`` doesn't match ``chunk_idx``'s own
                ``ChunkCrcStore`` entry.
        """
        await self._verify_chunk_ciphertext_crc(chunk_idx, raw)

    def non_compacted_chunk_indices(self) -> list[int]:
        """Every chunk index with real, readable data — skips ``COMPACTED``
        slots (reclaimed, not corruption). Built from the raw compress-type
        array, the same source ``read_chunk``/``read_chunks`` use for their
        own per-chunk decoding, so a whole-bucket sweep costs one
        O(chunk_num) pass over plain ints with no per-chunk
        ``SizeStoreEntry`` construction — unlike a caller wanting just one
        representative chunk instead, which should index ``entries``
        directly rather than call this for a single lookup."""
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

        ``addr`` supplies the IV for decryption — pass the chunk's *own* ``ChunkAddress``.

        Reads straight off the raw arrays (see this class's own docstring)
        rather than ``entries``/``locators`` — no reason for even a single
        cold-path ``SizeStoreEntry``/``ChunkLocator`` construction once the
        raw arrays already exist from ``__init__``.

        ``verify_ciphertext_crc`` (``None``: defer to whatever this reader
        was ``open()``\\ ed with) compares this chunk's *stored* bytes
        against its ``ChunkCrcStore`` entry (FORMAT-SPEC.md: ChunkCrcStore)
        before decrypting — a general data-integrity check independent of
        encryption and needing no vault key, off by default since it costs
        an extra trailer read the first time it's used per bucket.
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
        caller that already has ``chunk_idx``'s stored bytes in hand — e.g.
        from a batched ``read_raw_chunks`` call — instead of fetching them
        itself, the same split ``verify_raw_chunk_ciphertext_crc`` already
        has from ``verify_chunk_ciphertext_crc``.

        ``verify_ciphertext_crc``: same meaning as ``read_chunk``'s own
        parameter. A caller that already ran
        ``verify_raw_chunk_ciphertext_crc`` on ``raw`` itself should pass
        ``False`` here — this would otherwise redo (and raise past) that
        same check.
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
        compressed and/or encrypted per ``mode``, no decrypt/decompress
        applied. For ``verify.py``'s ChunkCrcStore spot-check
        (FORMAT-SPEC.md: ChunkCrcStore), which checks the *ciphertext*
        CRC32 directly and has no need for the plaintext ``read_chunk``
        produces.

        Raises:
            ChunkCompactedError: ``chunk_idx`` is ``COMPACTED`` — checked
                first (like ``read_chunk``'s own ``_resolve_compress_type``
                call), since a compacted chunk has no ChunkCrcStore entry
                of its own; a caller resolving one anyway would otherwise
                index one past the end of that trailer's own array.
        """
        self._resolve_compress_type(chunk_idx)
        offset = self._raw_offsets[chunk_idx]
        length = self._raw_lengths[chunk_idx]
        return await self._store.read(self.path, offset, length)

    async def read_raw_chunks(self, chunk_indices: Sequence[int]) -> dict[int, bytes | memoryview]:
        """Batch form of ``read_raw_chunk``: merges ``chunk_indices``'
        own locator byte-ranges that are within ``_GAP_TOLERANCE`` of
        each other into a single ``ObjectStore.read`` call instead of
        issuing one read per chunk — the same run-merging
        ``read_chunks`` does, minus the decrypt/decompress step neither
        this method nor the single-chunk ``read_raw_chunk`` performs.
        Collapsing the read count like this matters most on a
        remote-object-storage backend, where every separate read is its
        own network round trip.

        Args:
            chunk_indices: Must already be sorted ascending and
                deduplicated — same contract as ``read_chunks``'
                ``requests``.

        Raises:
            ChunkCompactedError: For the first ``COMPACTED`` chunk found in
                ``chunk_indices``, raised up front before any read
                happens — same as ``read_chunks``.
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
        byte-ranges that are within ``_GAP_TOLERANCE`` of each other into a
        single ``ObjectStore.read`` call instead of issuing one read per
        chunk, then decrypts/decompresses each chunk out of its own slice of
        whichever merged buffer it landed in. Decrypt stays strictly
        per-chunk; decompress is batched across a whole merged run instead,
        via ``_decode_run``/``decompress_many``.

        Args:
            requests: Must already be sorted by ``chunk_idx`` ascending and
                deduplicated — not re-sorted or re-deduplicated here, so a
                caller bug in either regard surfaces as a wrong/inefficient
                merge rather than being silently hidden. Each ``addr`` may
                be ``None`` when the caller already knows this bucket isn't
                vault-encrypted, since ``_decode_run`` never dereferences it
                unless it is; a caller that doesn't know ahead of time may
                still pass a real one.
            semaphore: The shared concurrency pool for this whole export
                (default ``None``: serial) — see ``exec_chunks``'s own
                docstring for the cross-bucket/in-bucket accounting. A
                caller passing a non-``None`` semaphore has already
                acquired one permit on this call's behalf; this method's
                first merged run spends that permit directly, and only a
                second/third/... run (a bucket whose requested chunks land
                in more than one physically-separate region of its ``.buk``
                file) acquires its own additional permit from the same
                pool.
            verify_ciphertext_crc: Same meaning as ``read_chunk``'s own
                parameter (``None`` defers to whatever this reader was
                ``open()``\\ ed with) — checked per chunk, before decrypting.

        Returns:
            A chunk's value may be a ``memoryview`` rather than an
            independent ``bytes`` object — it can view into either
            ``decompress_many``'s shared per-run decode buffer (ZSTD) or
            this method's own merged I/O-read buffer (unencrypted ``NONE``/
            ``LZ4``), never copied just to satisfy this method's own
            contract. Every consumer today only reads the bytes forward
            into another buffer, never holding the value past its own call.

        Raises:
            ChunkCompactedError: For the first COMPACTED chunk found in
                ``requests``, raised up front before any read happens.
        """
        result: dict[int, bytes | memoryview] = {}
        if not requests:
            return result

        should_verify = self._default_verify_ciphertext_crc if verify_ciphertext_crc is None else verify_ciphertext_crc
        if should_verify:
            # Resolved (and self-validated) once, up front — _decode_run()
            # runs on a worker thread via asyncio.to_thread and can't await
            # this itself, so the cache must already be warm by the time it
            # runs.
            await self.ensure_chunk_crc_store()

        # Off the raw arrays (see this class's own docstring), not
        # entries/locators: no SizeStoreEntry/ChunkLocator is constructed
        # anywhere in this method or in _fits_in_run()/_read_run()/
        # _decode_run() below, not even lazily — ``located``/``run`` carry the
        # already-resolved CompressType alongside plain offset/length ints
        # instead, so _decode_run() never needs to look a chunk's
        # compress_type up a second time either.
        located: list[tuple[int, ChunkAddress | None, int, int, CompressType]] = []
        for chunk_idx, addr in requests:
            compress_type = self._resolve_compress_type(chunk_idx)
            located.append((chunk_idx, addr, self._raw_offsets[chunk_idx], self._raw_lengths[chunk_idx], compress_type))

        runs = self._plan_runs(located)

        if semaphore is None or len(runs) <= 1:
            # Fast path, no TaskGroup at all — even when a semaphore was
            # passed, len(runs) <= 1 means the one permit the caller
            # already acquired on this call's behalf covers this single
            # run, so there is nothing left to fan out.
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
                # runs[0] spends the permit the caller already acquired —
                # see this method's own docstring's ``semaphore`` contract.
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
        to merge — gap-only, no size cap (see ``_GAP_TOLERANCE``'s own
        docstring for why). The next range's own length plays no part in
        this decision, only where it starts. Shared by ``read_chunks``
        and ``read_raw_chunks`` — each passes its own run shape's
        offset+length in, rather than this method assuming one."""
        gap = offset - prev_end
        return 0 <= gap <= _GAP_TOLERANCE

    @staticmethod
    def _plan_runs(
        located: list[tuple[int, ChunkAddress | None, int, int, CompressType]],
    ) -> list[list[tuple[int, ChunkAddress | None, int, int, CompressType]]]:
        """Pure planning, no I/O — merges ``located``'s locator byte-ranges
        that are within ``_GAP_TOLERANCE`` of each other (``_fits_in_run``)
        into runs, in one pass over ``located`` (already offset-ordered by
        ``read_chunks``'s own construction of it). Every run
        ``read_chunks`` will need is already known before the first byte is
        fetched, the same reason ``chunk_walk.py``'s own bucket-major plan
        can decide ``max_concurrent_reads``/``max_concurrent_opens``
        concurrency ahead of time rather than discovering it mid-walk."""
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
        OS thread — one hop per merged run (up to one whole bucket's
        worth of chunks), not per chunk, so its cost amortizes away.

        The ``asyncio.to_thread()`` hop is about responsiveness, not
        throughput: asyncio is single-threaded cooperative scheduling, so
        a multi-MB decrypt+decompress left on the event loop would stall
        every other Task (TUI redraws, a progress callback, a concurrent
        metadata read) for its duration. ``read_chunk``, the
        single-chunk interactive path, deliberately does not hop — one
        4096-byte decode is far cheaper than a thread round-trip.

        ``verify_ciphertext_crc`` is passed down from ``read_chunks``
        already resolved (never ``None`` here) — ``ensure_chunk_crc_store``
        has already been awaited by the caller, so ``_decode_run`` below
        can read the cached values synchronously.
        """
        run_start = run[0][2]
        run_end = run[-1][2] + run[-1][3]
        merged = await self._store.read(self.path, run_start, run_end - run_start)
        # Wrapped once, here, rather than left as bytes for _decode_run to
        # slice — every per-chunk slice below becomes a zero-copy view into
        # this one buffer instead of an independent copy. Unlike
        # BucketReaderCache, which caches whole BucketReaders but not
        # decoded chunk plaintext (see its own docstring), nothing here
        # holds one of these views past this one merged run's own decode:
        # ``result`` only lives until
        # _exec_one_bucket_group's run-assembly loop copies each chunk
        # forward into its own dest_offset run_buf, never longer.
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
        so this loop can't be collapsed — see this class's own
        ``read_chunks`` docstring), then hand the whole run's
        ciphertext-removed bytes to ``decompress_many`` in one batched
        call instead of decompressing chunk by chunk.

        Takes each chunk's ``compress_type``/``offset``/``length`` straight
        from ``run`` (see ``read_chunks``'s own comment) — no
        ``SizeStoreEntry``/``ChunkLocator`` lookup anywhere in this
        method. ``addr`` is only actually dereferenced inside the
        ``is_vault_encrypted`` branch below — see ``read_chunks``'s
        own docstring for why a caller may have skipped constructing a
        real one when it already knows that branch never runs.

        ``verify_ciphertext_crc``, when set, checks each chunk's raw slice
        against ``self._chunk_crc_values`` (already resolved by
        ``read_chunks`` before this ever runs — see that method's own
        docstring) before decrypting it."""
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


# Not frozen: a stateful cache object, not a value model — buckets is
# grown in place as new BucketReaders are opened.
@dataclasses.dataclass
class BucketReaderCache:
    """A private, unbounded ``BucketReader`` cache for one bulk sweep
    through many buckets — export's bucket-major path and ``verify``'s
    Bucket-and-key stage each build one instead of going through
    ``Pool``'s own bounded ``_buckets``, so a sweep touching every bucket
    once doesn't evict genuinely-hot interactive entries from that shared
    cache. Reads/writes never cross over with ``Pool``'s own cache — the
    two stay fully independent.

    Grow-only, no eviction — bounded by however many distinct buckets one
    sweep actually touches. Built on ``AsyncKeyedCache`` like every other
    cache in this SDK, with ``fetch`` supplied *per call* (typically
    ``Pool.open_bucket_uncached``) since this class is constructed at
    layers with no ``Pool`` reference of their own.

    One instance is shared across every fragment of a
    ``VirtualDiskContentSource`` export, so a bucket one fragment opens
    stays open for the next; a standalone ``export_to`` call or a
    ``verify`` run instead each get their own separate instance.

    Note:
        Deliberately does not also cache decoded chunk plaintext across
        calls — same-call repeats are already deduplicated by
        ``exec_chunks`` itself, and unconditionally remembering every
        decoded chunk would cost memory roughly equal to the whole
        export's unique DATA content for little cross-call reuse against
        real VM-image exports. Chunk decode stays scoped to one
        bucket-group call, which is also what lets ``decompress_many``
        hand back zero-copy ``memoryview`` values instead of an
        independent ``bytes`` copy per chunk.
    """

    buckets: AsyncKeyedCache[tuple[StreamId, BucketId], BucketReader] = dataclasses.field(
        default_factory=AsyncKeyedCache
    )


class Pool:
    """Repository-wide chunk pool entry point.

    Resolves any ``ChunkAddress`` to its 4096-byte plaintext, with
    two-level LRU caching: ``BucketReader``\\ s (header + locators, cheap,
    kept many) and decoded plaintext chunks (more expensive, kept fewer).
    Both are ``AsyncKeyedCache`` instances — session-wide, shared across
    every interactive caller —
    which already gives concurrency-safety (an internal lock, held only
    around bookkeeping, never around the I/O of opening a bucket or
    reading/decrypting/decompressing a chunk) and in-flight fetch
    de-duplication (two concurrent callers missing the same key pay for
    one fetch, not two) for free; see that module's own docstring for the
    exact contract.
    """

    def __init__(
        self,
        store: ObjectStore,
        pool_root: str,
        dir_cache: DirCache,
        *,
        vault_key: bytes | None = None,
        bucket_cache_size: int = 16,
        chunk_cache_size: int = 4096,
        verify_fingerprint: bool = False,
        verify_ciphertext_crc: bool = False,
    ) -> None:
        self._store = store
        self._pool_root = pool_root
        self._dir_cache = dir_cache
        self._vault_key = vault_key
        # See read_chunk()'s own docstring — this is the session-wide
        # default; a per-call ``verify_fingerprint=`` argument overrides it.
        self._verify_fingerprint = verify_fingerprint
        # Threaded into every BucketReader this Pool opens (open_bucket_uncached)
        # as that reader's own default — see BucketReader.open()'s docstring.
        # Unlike verify_fingerprint, there's no separate Pool-level
        # verify_fingerprints()-style method for this: ChunkCrcStore lives
        # inside the bucket file BucketReader already owns, not a separate
        # sidecar Pool has to locate.
        self._verify_ciphertext_crc = verify_ciphertext_crc
        self._buckets: AsyncKeyedCache[tuple[StreamId, BucketId], BucketReader] = AsyncKeyedCache(
            self.open_bucket_uncached_by_key, maxsize=bucket_cache_size
        )
        self._chunks: AsyncKeyedCache[tuple[StreamId, BucketId, ChunkIdx], bytes] = AsyncKeyedCache(
            maxsize=chunk_cache_size
        )
        # Shared by every fingerprint lookup this Pool ever does (below,
        # and via verify_fingerprints()) — up to GROUP_BUCKET_NUM buckets
        # sharing one .inf group only ever pay for its own header
        # validation/allocation-table read once, not once per bucket. See
        # AllocationTableCache's own docstring.
        self._allocation_cache = AllocationTableCache()

    def release_caches(self) -> None:
        """Drop everything this pool holds in memory: decoded chunks, open
        bucket readers, and allocation tables.

        Closing a repository does not, on its own, free any of this — the
        caches live on the ``Pool``, which a caller can still be holding a
        reference to. Anything walking several repositories in one process
        (a smoke run, a TUI session browsing one after another) would
        otherwise keep every pool it ever opened fully populated. The
        ``DirCache`` is deliberately untouched: it belongs to the store,
        not to this pool.
        """
        self._buckets.invalidate()
        self._chunks.invalidate()
        self._allocation_cache.clear()

    @property
    def store(self) -> ObjectStore:
        """Needed by ``dedup.pool_descriptor.PoolDescriptor.from_pool`` to
        describe an equivalent ``Pool`` for a multiprocess worker to
        rebuild — the same reason ``dedup.repository.DedupRepo`` already
        exposes its own ``store``."""
        return self._store

    @property
    def pool_root(self) -> str:
        return self._pool_root

    @property
    def vault_key(self) -> bytes | None:
        return self._vault_key

    @property
    def verify_fingerprint(self) -> bool:
        """This ``Pool``'s own session-wide default — needed alongside
        ``store``/``pool_root``/``vault_key`` by
        ``dedup.pool_descriptor.PoolDescriptor.from_pool`` so a
        multiprocess worker's own rebuilt ``Pool`` mirrors this one's
        actual configuration instead of silently resetting it."""
        return self._verify_fingerprint

    @property
    def verify_ciphertext_crc(self) -> bool:
        return self._verify_ciphertext_crc

    async def bucket_path(self, stream_id: StreamId, bucket_id: BucketId) -> str:
        """Resolve ``(stream_id, bucket_id)`` to its physical ``.buk`` path
        (including any ``.<seqId>`` generation suffix), relative to the
        repository root."""
        layer_path = pool_layer_path(stream_id, bucket_id)
        dir_part, leaf = split_layer_leaf(layer_path)
        logical_name = f"{leaf}.buk"
        full_dir = join_path(self._pool_root, dir_part)
        return await resolve_seq_path(self._dir_cache, full_dir, logical_name)

    async def bucket(self, stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
        """Return the (cached) ``BucketReader`` for ``(stream_id,
        bucket_id)``, opening and caching it on first access."""
        return await self._buckets.resolve((stream_id, bucket_id))

    async def open_bucket_uncached(self, stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
        """Open ``(stream_id, bucket_id)`` fresh, **without** touching
        ``_buckets`` at all — no lookup, no insert, no eviction.

        Exists for callers that need their own private, separately-scoped
        ``BucketReader`` cache instead of this shared one (a bulk sweep like
        ``export_scheduler.export_to``'s bucket-major path, or ``verify``'s
        Bucket-and-key stage — see ``BucketReaderCache``'s own
        docstring). ``_buckets`` itself is bound to call this for its
        own miss path (see ``Pool.__init__``); the two never diverge in
        how a bucket actually gets opened, only in whether the result is
        remembered in the shared cache afterward.
        """
        path = await self.bucket_path(stream_id, bucket_id)
        return await BucketReader.open(
            self._store, path, vault_key=self._vault_key, verify_ciphertext_crc=self._verify_ciphertext_crc
        )

    async def open_bucket_uncached_by_key(self, key: tuple[StreamId, BucketId]) -> BucketReader:
        """``open_bucket_uncached``, taking its ``(stream_id, bucket_id)``
        as one tuple — the single-arg shape ``AsyncKeyedCache.resolve``'s
        ``fetch`` callback needs, so every caller building a
        ``BucketReader`` cache keyed this way (this class's own
        ``_buckets``, ``chunk_walk.py``'s bucket-major export, ``verify``'s
        Bucket stage) can pass this bound method directly instead of each
        writing its own ``lambda key: pool.open_bucket_uncached(*key)``."""
        return await self.open_bucket_uncached(*key)

    async def read_chunk(
        self,
        addr: ChunkAddress,
        *,
        cache: bool = True,
        verify_fingerprint: bool | None = None,
        verify_ciphertext_crc: bool | None = None,
    ) -> bytes:
        """Resolve ``addr`` — using *its own* embedded ``stream_id``/
        ``bucket_id``,
        not any caller-assumed values — to 4096 bytes of plaintext. Not
        used by the bucket-major export scheduler
        (``chunk_walk.py``'s ``_exec_one_bucket_group``), which calls
        ``BucketReader.read_chunks`` directly, bypassing this class
        entirely.

        ``cache=False`` bypasses the plaintext chunk cache entirely — for
        a one-off integrity check of a single chunk per bucket, which has
        nothing to gain from caching it and would only evict genuinely-hot
        interactive data for no benefit.

        ``verify_fingerprint`` (``None``: defer to whatever this
        ``Pool`` was constructed with; an explicit ``True``/``False``
        overrides it for this one call) compares this chunk's plaintext
        SHA-256 against its stored ``.fgp`` fingerprint and raises
        ``DataCorruptError`` on a mismatch —
        a general data-integrity check independent of encryption, off by
        default since it costs one extra ``.inf``/``.fgp`` read per
        chunk. A plaintext-cache hit is checked too, not skipped, since
        the point is verifying the bytes about to be handed back.

        ``verify_ciphertext_crc`` — forwarded to
        ``BucketReader.read_chunk`` as-is (``None`` defers to whatever
        that reader was opened with); see its own docstring. Checked
        before decrypting, so it still requires a vault key for an
        encrypted bucket to get as far as returning plaintext — a caller
        that wants the ChunkCrcStore check in isolation, independent of
        whether a key is even available, wants
        ``BucketReader.verify_chunk_ciphertext_crc`` instead.
        """
        key = (addr.stream_id, addr.bucket_id, addr.chunk_idx)

        async def fetch_chunk(_key: tuple[StreamId, BucketId, ChunkIdx]) -> bytes:
            reader = await self.bucket(addr.stream_id, addr.bucket_id)
            return await reader.read_chunk(addr.chunk_idx, addr, verify_ciphertext_crc=verify_ciphertext_crc)

        plain = await self._chunks.resolve(key, fetch_chunk) if cache else await fetch_chunk(key)
        await self.verify_fingerprints(
            addr.stream_id, addr.bucket_id, {int(addr.chunk_idx): plain}, verify_fingerprint=verify_fingerprint
        )
        return plain

    async def verify_fingerprints(
        self,
        stream_id: StreamId,
        bucket_id: BucketId,
        chunks: Mapping[int, bytes | memoryview],
        *,
        verify_fingerprint: bool | None = None,
    ) -> None:
        """Check every entry in ``chunks`` (already-decoded plaintext, keyed
        by its own ``chunk_idx`` within ``(stream_id, bucket_id)``) against
        its stored ``.fgp`` digest — the same policy/lookup ``read_chunk``
        applies to its own single chunk, factored out here so
        ``BucketReader.read_chunks``'s multi-chunk batch result (fetched
        by ``dedup_file.py``'s ``_fill_data_extent`` and
        ``chunk_walk.py``'s ``_exec_one_bucket_group``, both of which call
        ``read_chunks`` directly and so bypass ``read_chunk`` entirely) can
        honor it too, instead of silently skipping verification whenever a
        read/export spans more than one distinct chunk.

        ``verify_fingerprint`` — same meaning as ``read_chunk``'s own
        parameter: ``None`` defers to this ``Pool``'s session-wide default.

        Raises:
            DataCorruptError: Any chunk's plaintext SHA-256 doesn't match its
                stored fingerprint.
        """
        should_verify = self._verify_fingerprint if verify_fingerprint is None else verify_fingerprint
        if not should_verify:
            return
        # One batched fingerprints() call resolves the .inf header/
        # allocation-table entry shared by every chunk in this bucket
        # exactly once, instead of once per chunk (see fingerprints()'s
        # own docstring) -- real for a multi-chunk read/export with
        # verify_fingerprint enabled. self._allocation_cache additionally
        # shares that resolution across separate calls/buckets in the same
        # .inf group (see AllocationTableCache's own docstring).
        chunk_indices = [ChunkIdx(raw_chunk_idx) for raw_chunk_idx in chunks]
        expected_by_chunk = await self.fingerprints_for(stream_id, bucket_id, chunk_indices)
        for raw_chunk_idx, plain in chunks.items():
            chunk_idx = ChunkIdx(raw_chunk_idx)
            if hashlib.sha256(plain).digest() != expected_by_chunk[chunk_idx]:
                addr = ChunkAddress(stream_id, bucket_id, chunk_idx)
                raise DataCorruptError(f"chunk fingerprint mismatch at {addr}", ref=self._pool_root, spec=_SPEC)

    async def fingerprints_for(
        self, stream_id: StreamId, bucket_id: BucketId, chunk_indices: Sequence[ChunkIdx]
    ) -> dict[ChunkIdx, bytes]:
        """Batched ``dedup.fingerprint.fingerprints()``, using this Pool's
        own ``AllocationTableCache`` — ``verify_fingerprints``'s own
        lookup half (resolves the stored digests only, no plaintext
        comparison of its own), factored out so its own ``.inf``/``.fgp``
        resolution shares this ``Pool``'s cache the same way every other
        lookup here does."""
        return await fingerprints(
            self._store,
            self._dir_cache,
            self._pool_root,
            stream_id,
            bucket_id,
            chunk_indices,
            cache=self._allocation_cache,
        )
