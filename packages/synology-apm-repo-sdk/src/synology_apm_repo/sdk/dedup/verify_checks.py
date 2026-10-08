"""Byte-level integrity check primitives for the reachability-scoped walk
(``dedup.verify_walk``, driven by ``units/verify_reachable.py``) behind
``Repository.verify()``/``Catalog.verify()``.

Each function checks what it is handed (an open reader, or for
``check_repo_info`` the repository) and returns ``Finding``\\ s; choosing what to check is the walker's job.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING

from ..errors import DataCorruptError, FormatError, NotFoundError, StorageBackendError
from ..findings import Finding, Stage, Symptom
from ..format.bucket import expected_bucket_size
from ..format.composition import (
    RecordHead,
    chunk_map_array_offset,
    parse_record_head,
    verify_attr_crc,
    verify_chunk_map_crc,
)
from ..format.const import CHUNK_MAP_RECORD_LENGTH, RECORD_HEAD_LENGTH, REDUNDANCY_COVERAGE_COMPOSITION
from ..format.redundancy import redundancy_size
from ..format.repo_info import parse_repo_info
from ..identifiers import ChunkIdx
from ..storage.base import ObjectStore, join_path
from ..storage.layout import REPO_INFO_NAME
from .composition_reader import CompositionReader
from .pool import BucketReader
from .redundancy_repair import repair_via_trailer

if TYPE_CHECKING:
    from .repository import DedupRepo


STALE_ROTATED_SUFFIX = "possibly a stale/rotated reference"
"""Appended (behind an em dash) to a ``NotFoundError``'s message in a
``Symptom.DATA_MISSING`` ``Finding``: ``unresolvable_finding``'s default
``missing_suffix``."""


async def check_repo_info(repo: DedupRepo) -> list[Finding]:
    """``repo_info``'s header/CRC/JSON-parse check; repository-wide, so
    ``verify_reachable()`` runs it once."""
    try:
        path = await repo.repo_info_path()
    except NotFoundError:
        path = join_path(repo.layout.repo_root, REPO_INFO_NAME)
        return [Finding(Stage.REPO_INFO, Symptom.FILE_MISSING, path, "repo_info not found")]
    try:
        raw = await repo.store.read(path)
    except NotFoundError:
        return [Finding(Stage.REPO_INFO, Symptom.FILE_MISSING, path, "repo_info not found")]
    try:
        parse_repo_info(raw)
    except FormatError as exc:
        return [Finding(Stage.REPO_INFO, Symptom.CORRUPTION, path, str(exc))]
    return []


async def check_composition_header(reader: CompositionReader, *, path: str) -> Finding | None:
    """The session's ``subID=0`` sub-file header
    (``CompositionReader.verify_header``), which ``CompositionReader`` itself
    skips. Call once per distinct ``(stream_id, session_id)``."""
    try:
        await reader.verify_header()
    except NotFoundError as exc:
        return Finding(Stage.COMPOSITION, Symptom.DATA_MISSING, path, str(exc))
    except FormatError as exc:
        return Finding(Stage.COMPOSITION, Symptom.CORRUPTION, path, str(exc))
    return None


async def check_record_head(
    reader: CompositionReader, comp_offset: int, *, path: str
) -> tuple[Finding | None, RecordHead | None]:
    """Read and parse the 32-byte ``RecordHead`` at ``comp_offset``
    (``parse_record_head`` checks magic and ``head_crc``). Returns
    ``(finding, None)`` on failure, ``(None, record_head)`` on success."""
    try:
        raw = await reader.read_at(comp_offset, RECORD_HEAD_LENGTH)
        return None, parse_record_head(raw)
    except NotFoundError as exc:
        return Finding(Stage.FILE_MAP, Symptom.DATA_MISSING, path, str(exc)), None
    except FormatError as exc:
        return Finding(Stage.FILE_MAP, Symptom.CORRUPTION, path, str(exc)), None


_CRC_THREAD_HOP_MIN_BYTES = 1 << 18  # 256 KiB
"""Below this size, calling ``verify_chunk_map_crc`` directly is cheaper
than an ``asyncio.to_thread()`` hop. Most chunk-map arrays are well under it."""


def should_thread_chunk_map_crc(map_array_bytes: bytes) -> bool:
    """Whether ``verify_chunk_map_crc(map_array_bytes, ...)`` is worth
    running via ``asyncio.to_thread()`` (see ``_CRC_THREAD_HOP_MIN_BYTES``)."""
    return len(map_array_bytes) >= _CRC_THREAD_HOP_MIN_BYTES


async def verify_chunk_map_crc_threaded(map_array_bytes: bytes, expected_crc: int) -> None:
    """``format.composition.verify_chunk_map_crc``, moved to a thread when
    ``should_thread_chunk_map_crc`` says the array is large enough to block
    the event loop.

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
    """On a ``map_crc`` mismatch, fetch the record's trailing Redundancy blob
    (FORMAT-SPEC.md: RecordHead; ChunkCrcStore & Redundancy; stored after the
    attribute blob, coverage 8192) and try in-memory self-repair
    (``redundancy_repair.repair_via_trailer``). Not read on the healthy path.

    Returns:
        The confirmed-correct array bytes, or ``None`` if the trailer can't
        be fetched/parsed or the repair doesn't validate.
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
    """``mapCrc`` over the whole chunk-map array plus (when present)
    ``attrCrc`` over the trailing JSON attribute blob, in one merged read.

    A ``map_crc`` mismatch first tries Redundancy-blob self-repair
    (``_attempt_map_crc_repair``); a confirmed repair is reported as
    ``Symptom.REPAIRED_VIA_PARITY``. Nothing is checked when
    ``record_head.map_num == 0``.

    Returns:
        ``(findings, repaired_array)``. ``repaired_array`` is the
        confirmed-correct chunk-map array bytes exactly when a
        ``map_crc`` mismatch was repaired via parity, ``None`` otherwise.
        A caller that re-reads this array must pass a non-``None`` result to
        ``CompositionRecord.seed_pages_from_array``, or it reads the
        still-corrupted on-disk bytes.
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


async def check_bucket_structure(
    store: ObjectStore, reader: BucketReader, *, known_file_size: int | None = None
) -> list[Finding]:
    """The chunk-independent checks of one bucket: ``expected_bucket_size``
    against the on-disk size, and the ChunkCrcStore trailer's
    self-consistency (``BucketReader.ensure_chunk_crc_store``, which also
    warms the cache ``check_chunk_ciphertext_crc`` reuses). Header/SizeStore
    CRC are checked by ``BucketReader.open``; this adds a
    ``Symptom.REPAIRED_VIA_PARITY`` finding when its SizeStore self-repair
    succeeded.

    A legacy uncompressed-layout bucket has neither an
    ``expected_bucket_size`` formula nor a ChunkCrcStore trailer; both checks
    are skipped.

    ``known_file_size`` is the on-disk size when the caller already has it
    (``Pool.bucket_size``, from the directory listing); without it, and
    unless a SizeStore repair fetched it, this asks ``store.size``.

    Takes a bare ``store`` so a worker process holding only an
    ``ObjectStore`` can call it.
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
    expected = expected_bucket_size(reader.header, reader.index)
    actual = reader.known_file_size if reader.known_file_size is not None else known_file_size
    if actual is None:
        actual = await store.size(reader.path)
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
    except FormatError as exc:
        findings.append(Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, str(exc)))
    return findings


async def check_chunk_ciphertext_crc(
    reader: BucketReader, chunk_idx: int, raw: bytes | memoryview | None = None
) -> Finding | None:
    """One chunk's *stored* bytes against its ChunkCrcStore entry
    (FORMAT-SPEC.md: ChunkCrcStore & Redundancy): ``raw`` when the caller
    already holds them (e.g. from ``read_raw_chunks``), else read here.
    Nothing is decrypted, so no vault key is needed."""
    try:
        if raw is None:
            await reader.verify_chunk_ciphertext_crc(ChunkIdx(chunk_idx))
        else:
            await reader.verify_raw_chunk_ciphertext_crc(chunk_idx, raw)
    except NotFoundError as exc:
        return Finding(Stage.BUCKET, Symptom.DATA_MISSING, reader.path, f"chunk {chunk_idx} ciphertext: {exc}")
    except DataCorruptError:
        return Finding(Stage.BUCKET, Symptom.MISMATCH, reader.path, f"chunk {chunk_idx} ciphertext CRC mismatch")
    except FormatError as exc:
        return Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, str(exc))
    return None


async def check_chunk_ciphertext_crcs(reader: BucketReader, chunk_indices: Sequence[int]) -> list[Finding]:
    """Batched ``check_chunk_ciphertext_crc``: fetches every chunk via
    ``reader.read_raw_chunks`` (one merged read per contiguous run), then
    checks each against its ``ChunkCrcStore`` entry.

    ``read_raw_chunks`` discards earlier runs when a later one fails, so any
    failure but a ``StorageBackendError`` (raised) falls back to one
    ``check_chunk_ciphertext_crc`` call per chunk.
    """
    try:
        raw_by_chunk = await reader.read_raw_chunks(list(chunk_indices))
    except StorageBackendError:
        raise
    except Exception:  # noqa: BLE001
        return await _check_chunk_ciphertext_crcs_one_by_one(reader, chunk_indices)

    findings: list[Finding] = []
    for chunk_idx in chunk_indices:
        finding = await check_chunk_ciphertext_crc(reader, chunk_idx, raw_by_chunk[chunk_idx])
        if finding is not None:
            findings.append(finding)
    return findings


async def _check_chunk_ciphertext_crcs_one_by_one(reader: BucketReader, chunk_indices: Sequence[int]) -> list[Finding]:
    """``check_chunk_ciphertext_crcs``'s fallback: one independent
    ``check_chunk_ciphertext_crc`` per chunk, so one failure hides no other."""
    findings: list[Finding] = []
    for chunk_idx in chunk_indices:
        finding = await check_chunk_ciphertext_crc(reader, chunk_idx)
        if finding is not None:
            findings.append(finding)
    return findings
