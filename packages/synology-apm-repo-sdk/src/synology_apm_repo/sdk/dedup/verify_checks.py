"""Byte-level integrity check primitives for ``units/verify_reachable.py``'s
top-down, reachability-scoped walk — what ``Repository.verify()``/
``Catalog.verify()`` actually use.

Every function here is a pure "given this already-open reader/triple,
check it, return the ``Finding``(s)" primitive — no scanning of
``db/file_map``, no bucket sampling, no notion of which triples/buckets
are worth looking at in the first place. That's the walker's own job;
this module only owns *how* to check one thing once it's been decided
the thing is worth checking.

Also owns the ``Finding``/``Stage``/``Symptom``/``VerifyLevel`` value
types the walker (and its callers, ``api.catalog``/``api.repository``)
share — defined here, at the bottom of the dependency, so nothing above
this module ever needs to import back into ``units/verify_reachable.py``
just to reference one of these value types.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
from collections.abc import Sequence
from typing import TYPE_CHECKING

from ..errors import DataCorruptError, FormatError, NotFoundError
from ..format.bucket import expected_bucket_size
from ..format.composition import (
    RecordHead,
    chunk_map_array_offset,
    parse_record_head,
    should_thread_chunk_map_crc,
    verify_attr_crc,
    verify_chunk_map_crc,
)
from ..format.const import CHUNK_MAP_RECORD_LENGTH, RECORD_HEAD_LENGTH, REDUNDANCY_COVERAGE_COMPOSITION
from ..format.redundancy import redundancy_size, repair_via_trailer
from ..format.repo_info import parse_repo_info
from ..identifiers import ChunkIdx
from ..storage.base import ObjectStore, join_path
from ..storage.seqid import resolve_seq_path
from .composition_reader import CompositionReader
from .pool import BucketReader

if TYPE_CHECKING:
    # Type-only: check_repo_info's signature needs DedupRepo, but this
    # module has no runtime reason to depend on .repository. This
    # module's own ``from __future__ import annotations`` means the name
    # is never evaluated at runtime, so no real import happens here.
    from .repository import DedupRepo

_REPO_INFO_NAME = "repo_info"


class Symptom(enum.Enum):
    """The symptom categories a finding can carry."""

    CORRUPTION = "Corruption"
    FILE_MISSING = "FileMissing"
    MISMATCH = "Mismatch"
    DATA_MISSING = "DataMissing"
    KEY_MISSING = "KeyMissing"
    REPAIRED_VIA_PARITY = "RepairedViaParity"
    """A CRC mismatch that this SDK's own Redundancy-blob self-repair
    (``format.redundancy.attempt_repair``) was able to reconstruct and
    byte-for-byte confirm correct. Still reported, not silently dropped —
    repeated repairs against the same bucket group over time is itself a
    signal of failing underlying storage, even though the check the
    caller actually cared about (is this data readable) passed."""


class VerifyLevel(enum.Enum):
    """How thorough a ``verify`` run is. ``FULL`` reads and checks every
    live chunk in a touched bucket — both its ciphertext CRC32 and, once a
    vault key is available, its decrypt+decompress+SHA-256 fingerprint —
    as costly as a real full export of the whole repository. ``QUICK``
    reads no chunk content at all, only each touched bucket's own
    structural checks; there is deliberately no sampled middle tier."""

    QUICK = "quick"
    FULL = "full"


class Stage(enum.StrEnum):
    """The stages a ``Finding`` can come from. A ``str`` subclass: every
    existing site rendering/serializing ``Finding.stage`` directly as a
    string keeps working unchanged."""

    REPO_INFO = "RepoInfo"
    FILE_MAP = "FileMap"
    COMPOSITION = "Composition"
    BUCKET = "Bucket"
    ENCRYPT_KEY = "EncryptKey"
    VERSION = "Version"
    """A catalog/workload/version that should resolve to real content,
    per its own metadata, but doesn't — covers failure at either the
    version level or the workload/connection-enumeration level above it,
    one stage rather than one per layer of a top-down walk."""


@dataclasses.dataclass(frozen=True)
class Finding:
    """One integrity-check result. ``path`` is whatever store-relative
    path (or ``file_map`` path, or workload/version display info, for a
    ``Stage.VERSION`` finding) the finding is about.

    ``ref`` (``None`` unless set) is a canonical
    ``repo_path#cat:<id>/wl:<id>/ver:<uid>`` ref (``units.node_ref.NodeRef``)
    naming the exact catalog/workload/version this finding was found while
    checking — set by ``units/verify_reachable.py``'s top-down walk, the
    only place with a ``Version`` in hand to name. When a bucket/chunk is
    shared by more than one version through dedup, ``ref`` names whichever
    version's own check actually triggered this finding first —
    memoization means a later version sharing the same already-checked
    data produces no finding of its own, not a second one with a
    different ``ref``."""

    stage: Stage
    symptom: Symptom
    path: str
    detail: str
    ref: str | None = None


async def check_repo_info(repo: DedupRepo) -> list[Finding]:
    """``repo_info``'s own header/CRC/JSON-parse check — repository-wide, not
    scoped to any one catalog reference, so ``verify_reachable()`` runs
    this exactly once regardless of what else it covers."""
    try:
        path = await resolve_seq_path(repo.dir_cache, repo.layout.repo_root, _REPO_INFO_NAME)
    except NotFoundError:
        path = join_path(repo.layout.repo_root, _REPO_INFO_NAME)
        return [Finding(Stage.REPO_INFO, Symptom.FILE_MISSING, path, "repo_info not found")]
    try:
        raw = await repo.store.read(path)
    except NotFoundError:
        return [Finding(Stage.REPO_INFO, Symptom.FILE_MISSING, path, "repo_info not found")]
    try:
        parse_repo_info(raw)
    except (DataCorruptError, FormatError) as exc:
        return [Finding(Stage.REPO_INFO, Symptom.CORRUPTION, path, str(exc))]
    return []


async def check_composition_header(reader: CompositionReader, *, path: str) -> Finding | None:
    """The session's own ``subID=0`` sub-file header
    (``CompositionReader.verify_header``) — the stricter, opt-in check
    ``CompositionReader`` itself normally skips. Meant to be called once
    per distinct ``(stream_id, session_id)``, not once per row/extent
    that happens to share it."""
    try:
        await reader.verify_header()
    except NotFoundError as exc:
        return Finding(Stage.COMPOSITION, Symptom.DATA_MISSING, path, str(exc))
    except (DataCorruptError, FormatError) as exc:
        return Finding(Stage.COMPOSITION, Symptom.CORRUPTION, path, str(exc))
    return None


async def check_record_head(
    reader: CompositionReader, comp_offset: int, *, path: str
) -> tuple[Finding | None, RecordHead | None]:
    """Read and parse the 32-byte ``RecordHead`` at ``comp_offset`` —
    magic + ``head_crc`` are checked unconditionally by
    ``parse_record_head`` itself. Returns ``(finding, None)`` on failure,
    ``(None, record_head)`` on success."""
    try:
        raw = await reader.read_at(comp_offset, RECORD_HEAD_LENGTH)
        return None, parse_record_head(raw)
    except NotFoundError as exc:
        return Finding(Stage.FILE_MAP, Symptom.DATA_MISSING, path, str(exc)), None
    except (DataCorruptError, FormatError) as exc:
        return Finding(Stage.FILE_MAP, Symptom.CORRUPTION, path, str(exc)), None


async def verify_chunk_map_crc_threaded(map_array_bytes: bytes, expected_crc: int) -> None:
    """``format.composition.verify_chunk_map_crc``, with the thread-hop
    decision folded in — ``map_array_bytes`` can be a multi-hundred-MB
    array, so this keeps the rare large case's ``zlib.crc32`` pass off
    the event loop. ``should_thread_chunk_map_crc`` skips the hop for the
    common small-array case, where the hop would cost more than the
    ``crc32`` pass it saves. Shared by ``check_map_and_attr_crc`` and
    ``diagnostics.py``'s ``_verify_map_crc``.

    Raises:
        DataCorruptError: ``map_array_bytes`` doesn't match ``expected_crc``.
    """
    if should_thread_chunk_map_crc(map_array_bytes):
        await asyncio.to_thread(verify_chunk_map_crc, map_array_bytes, expected_crc)
    else:
        verify_chunk_map_crc(map_array_bytes, expected_crc)


async def _attempt_map_crc_repair(
    reader: CompositionReader, map_array_off: int, map_array_len: int, attr_leng: int, array_raw: bytes, map_crc: int
) -> bytes | None:
    """On a ``map_crc`` mismatch, lazily fetch the record's trailing
    Redundancy blob (FORMAT-SPEC.md: ChunkCrcStore/RecordHead — stored
    after the attribute blob, at ``map_array_off + map_array_len +
    attr_leng``, coverage 8192) and attempt in-memory self-repair
    (``format.redundancy.repair_via_trailer``).

    Never read on the healthy path — only called once
    ``verify_chunk_map_crc_threaded`` has already raised.

    Returns the repaired array bytes on a confirmed-correct
    reconstruction, or ``None`` if the trailer can't be fetched/parsed or
    the reconstruction still doesn't validate — either way, the caller
    falls back to reporting the original mismatch unchanged.
    """
    trailer_len = redundancy_size(map_array_len, REDUNDANCY_COVERAGE_COMPOSITION)
    trailer_off = map_array_off + map_array_len + attr_leng
    return await repair_via_trailer(
        array_raw,
        coverage=REDUNDANCY_COVERAGE_COMPOSITION,
        expected_crc=map_crc,
        fetch_trailer=lambda: reader.read_at(trailer_off, trailer_len),
    )


async def check_map_and_attr_crc(
    reader: CompositionReader, comp_offset: int, record_head: RecordHead, *, path: str
) -> tuple[list[Finding], bytes | None]:
    """Full ``mapCrc`` over the whole chunk-map array plus (when present)
    ``attrCrc`` over the trailing JSON attribute blob — one merged read
    for both, since they're contiguous.

    A ``map_crc`` mismatch first tries the record's own Redundancy-blob
    self-repair (``_attempt_map_crc_repair``), since every current-format
    record carries one. A confirmed-correct repair is reported as
    ``Symptom.REPAIRED_VIA_PARITY`` rather than dropped silently.

    Nothing is checked when ``record_head.map_num == 0``.

    Returns:
        ``(findings, repaired_array)``. ``repaired_array`` is the
        confirmed-correct chunk-map array bytes exactly when a
        ``map_crc`` mismatch was just repaired via parity, ``None``
        otherwise. A caller with its own separate re-read of this same
        array must feed a non-``None`` result into
        ``CompositionRecord.seed_pages_from_array`` itself, or it
        re-derives entries from the still-corrupted on-disk bytes.
    """
    if record_head.map_num == 0:
        return [], None
    findings: list[Finding] = []
    map_array_off = chunk_map_array_offset(comp_offset)
    map_array_len = record_head.map_num * CHUNK_MAP_RECORD_LENGTH
    try:
        combined_raw = await reader.read_at(map_array_off, map_array_len + record_head.attr_leng)
    except (NotFoundError, FormatError) as exc:
        return [Finding(Stage.COMPOSITION, Symptom.DATA_MISSING, path, str(exc))], None
    array_raw = combined_raw[:map_array_len]
    attr_raw = combined_raw[map_array_len:]

    repaired_array: bytes | None = None
    try:
        await verify_chunk_map_crc_threaded(array_raw, record_head.map_crc)
    except DataCorruptError as exc:
        repaired_array = await _attempt_map_crc_repair(
            reader, map_array_off, map_array_len, record_head.attr_leng, array_raw, record_head.map_crc
        )
        if repaired_array is None:
            findings.append(Finding(Stage.COMPOSITION, Symptom.MISMATCH, path, str(exc)))
        else:
            findings.append(
                Finding(Stage.COMPOSITION, Symptom.REPAIRED_VIA_PARITY, path, "map CRC mismatch repaired via parity")
            )

    if record_head.attr_leng > 0:
        try:
            verify_attr_crc(attr_raw, record_head.attr_crc)
        except DataCorruptError as exc:
            findings.append(Finding(Stage.COMPOSITION, Symptom.MISMATCH, path, str(exc)))

    return findings, repaired_array


async def check_bucket_structure(store: ObjectStore, reader: BucketReader) -> list[Finding]:
    """Everything about this bucket that doesn't depend on which chunk(s)
    a caller cares about: ``expected_bucket_size`` against the real
    on-disk size, and the ChunkCrcStore trailer's own self-consistency
    (via ``BucketReader.ensure_chunk_crc_store``, which also warms the
    cache ``check_chunk_ciphertext_crc`` below reuses). Header/SizeStore
    CRC are already checked by ``BucketReader.open`` itself; this adds a
    ``Symptom.REPAIRED_VIA_PARITY`` finding when ``open()``'s own
    SizeStore self-repair succeeded, since ``open()`` has no
    ``Finding``-returning contract of its own.

    A legacy uncompressed-layout bucket has neither an
    ``expected_bucket_size`` formula nor a ChunkCrcStore trailer — both
    are skipped, not treated as findings.

    Takes a bare ``store``, not a full ``DedupRepo``, so a caller with
    just an ``ObjectStore`` in hand (a multiprocess worker rebuilding its
    own state via ``dedup.pool_descriptor``) can call this directly.
    """
    findings: list[Finding] = []
    if not reader.header.is_compressed:
        return findings
    if reader.sizestore_repaired:
        findings.append(
            Finding(
                Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, reader.path, "SizeStore CRC mismatch repaired via parity"
            )
        )
    expected = expected_bucket_size(reader.header, reader.entries)
    # reader.known_file_size is already the actual on-disk size when
    # SizeStore repair fetched it moments earlier — reuse it instead of a
    # second store.size() round-trip.
    actual = reader.known_file_size if reader.known_file_size is not None else await store.size(reader.path)
    if actual != expected:
        findings.append(
            Finding(
                Stage.BUCKET,
                Symptom.MISMATCH,
                reader.path,
                f"expected_bucket_size {expected} != actual file size {actual}",
            )
        )
    try:
        await reader.ensure_chunk_crc_store()
    except NotFoundError as exc:
        findings.append(Finding(Stage.BUCKET, Symptom.DATA_MISSING, reader.path, f"ChunkCrcStore trailer: {exc}"))
    except (DataCorruptError, FormatError) as exc:
        findings.append(Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, str(exc)))
    return findings


async def check_chunk_ciphertext_crc(reader: BucketReader, chunk_idx: int) -> Finding | None:
    """One chunk's *stored* bytes against its own ChunkCrcStore entry
    (FORMAT-SPEC.md: ChunkCrcStore) — no decrypt/decompress attempt, so
    this needs no vault key and runs the same whether or not the bucket
    turns out to be encrypted."""
    try:
        await reader.verify_chunk_ciphertext_crc(ChunkIdx(chunk_idx))
    except NotFoundError as exc:
        return Finding(Stage.BUCKET, Symptom.DATA_MISSING, reader.path, f"chunk {chunk_idx} ciphertext: {exc}")
    except DataCorruptError:
        return Finding(Stage.BUCKET, Symptom.MISMATCH, reader.path, f"chunk {chunk_idx} ciphertext CRC mismatch")
    except FormatError as exc:
        return Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, str(exc))
    return None


async def check_raw_chunk_ciphertext_crc(
    reader: BucketReader, chunk_idx: int, raw: bytes | memoryview
) -> Finding | None:
    """``check_chunk_ciphertext_crc``'s counterpart for a caller that
    already has ``chunk_idx``'s stored bytes in hand (e.g. from a batched
    ``read_raw_chunks`` call). Same exception mapping as the singular
    form above; kept separate from ``check_chunk_ciphertext_crcs`` below
    (its only caller) so that caller's batched-fetch-then-check shape
    stays readable."""
    try:
        await reader.verify_raw_chunk_ciphertext_crc(chunk_idx, raw)
    except NotFoundError as exc:
        return Finding(Stage.BUCKET, Symptom.DATA_MISSING, reader.path, f"chunk {chunk_idx} ciphertext: {exc}")
    except DataCorruptError:
        return Finding(Stage.BUCKET, Symptom.MISMATCH, reader.path, f"chunk {chunk_idx} ciphertext CRC mismatch")
    except FormatError as exc:
        return Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, str(exc))
    return None


async def check_chunk_ciphertext_crcs(reader: BucketReader, chunk_indices: Sequence[int]) -> list[Finding]:
    """Batched ``check_chunk_ciphertext_crc``: every one of
    ``chunk_indices``'s stored bytes is fetched via
    ``reader.read_raw_chunks`` — one merged ``store.read()`` per
    contiguous run instead of one per chunk — before checking each
    against its own ``ChunkCrcStore`` entry.

    ``read_raw_chunks`` has no per-run isolation: a later run's failure
    discards every earlier run's already-fetched (but never checked)
    bytes. So any failure at all from the batched read falls back to one
    ``check_chunk_ciphertext_crc`` call per chunk instead — reserved for
    the failure path only; the common case never pays for it.
    """
    try:
        raw_by_chunk = await reader.read_raw_chunks(list(chunk_indices))
    except Exception:
        return await _check_chunk_ciphertext_crcs_one_by_one(reader, chunk_indices)

    findings: list[Finding] = []
    for chunk_idx in chunk_indices:
        finding = await check_raw_chunk_ciphertext_crc(reader, chunk_idx, raw_by_chunk[chunk_idx])
        if finding is not None:
            findings.append(finding)
    return findings


async def _check_chunk_ciphertext_crcs_one_by_one(reader: BucketReader, chunk_indices: Sequence[int]) -> list[Finding]:
    """``check_chunk_ciphertext_crcs``'s fallback when its own batched
    ``read_raw_chunks`` call fails: one ``check_chunk_ciphertext_crc``
    call per chunk, each with its own independent error handling, so one
    chunk's failure can never hide another's already-fetched result."""
    findings: list[Finding] = []
    for chunk_idx in chunk_indices:
        finding = await check_chunk_ciphertext_crc(reader, chunk_idx)
        if finding is not None:
            findings.append(finding)
    return findings
