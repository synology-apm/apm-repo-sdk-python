"""Raw inspection of one physical ``.buk``/composition/chunk-map file,
addressed by a store-relative path instead of a ``NodeRef`` and bypassing
the catalog, for forensics tooling such as the CLI's ``dump`` commands. It
uses the same parsers as the read path, so callers need no
``format``/``storage``/``dedup`` internals.

Every function takes an already-resolved ``ObjectStore`` (``open_local()``
builds one for a local path) and the literal on-disk filename: a ``.<N>``
sequence-id suffix (FORMAT-SPEC.md: Sequence-id suffix mechanism) is not
resolved from a logical name.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from pathlib import Path

from .dedup.pool import BucketReader
from .dedup.verify_checks import verify_chunk_map_crc_threaded
from .errors import ApmRepoError, NotFoundError, StorageBackendError
from .format.bucket import (
    MODE_BUCKET_PARITY,
    MODE_CHUNK_CRC,
    MODE_COMPRESS,
    MODE_ENCRYPT,
    MODE_EXTENT_PARITY,
    MODE_INPLACE_PARITY,
    MODE_LOGIC_LOCALITY,
    MODE_VAULT_ENCRYPT,
    expected_bucket_size,
)
from .format.chunkmap import ChunkMapKind, parse_chunk_map_record
from .format.composition import (
    RecordHead,
    chunk_map_array_offset,
    parse_composition_header,
    parse_record_head,
    record_total_length,
)
from .format.const import CHUNK_MAP_RECORD_LENGTH, RECORD_HEAD_LENGTH
from .format.headers import MAGIC
from .storage.base import ObjectStore
from .storage.local import LocalFsStore

_MODE_BIT_NAMES = [
    (MODE_COMPRESS, "compress"),
    (MODE_CHUNK_CRC, "chunk_crc"),
    (MODE_BUCKET_PARITY, "bucket_parity"),
    (MODE_ENCRYPT, "encrypt"),
    (MODE_LOGIC_LOCALITY, "logic_locality"),
    (MODE_EXTENT_PARITY, "extent_parity"),
    (MODE_INPLACE_PARITY, "inplace_parity"),
    (MODE_VAULT_ENCRYPT, "vault_encrypt"),
]


def mode_flags(mode: int) -> list[str]:
    """Bucket-header ``mode`` bits as their names."""
    return [name for bit, name in _MODE_BIT_NAMES if mode & bit]


def open_local(path: Path) -> tuple[ObjectStore, str]:
    """A local file path as an ``ObjectStore`` rooted at its parent
    directory plus the bare filename, for the functions here."""
    return LocalFsStore(path.parent), path.name


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkEntryInspection:
    """One bucket entry's fields, as ``inspect_bucket``'s ``chunk``
    selected it."""

    index: int
    compress_type: str
    stored_len: int
    effective_len: int
    offset: int
    length: int


@dataclasses.dataclass(frozen=True, slots=True)
class BucketInspection:
    """A ``.buk`` file's header, SizeStore summary, and the
    ``expected_bucket_size`` self-check."""

    path: str
    major: int
    minor: int
    mode: int
    mode_flags: list[str]
    chunk_num: int
    chunk_size_crc: int
    compress_type_counts: dict[str, int]
    #: ``None`` for the legacy uncompressed layout, which has no
    #: ``expected_bucket_size`` formula; ``size_check_ok`` is ``None`` then too.
    expected_size: int | None
    actual_size: int
    size_check_ok: bool | None
    chunk: ChunkEntryInspection | None = None


async def inspect_bucket(store: ObjectStore, rel: str, *, chunk: int | None = None) -> BucketInspection:
    """Inspect a ``.buk`` file's header, SizeStore summary, and
    ``expected_bucket_size`` self-check.

    Raises:
        NotFoundError: ``rel`` does not exist, or ``chunk`` is out of range
            for this bucket's entry count.
        DataCorruptError: The header or SizeStore is corrupt and not
            repairable.
    """
    reader = await BucketReader.open(store, rel)
    actual_size = await store.size(rel)

    header = reader.header
    counts = dict(Counter(entry.compress_type.name for entry in reader.index))
    expected = expected_bucket_size(header, reader.index) if header.is_compressed else None

    chunk_info: ChunkEntryInspection | None = None
    if chunk is not None:
        if not 0 <= chunk < len(reader.index):
            raise NotFoundError(f"chunk {chunk} out of range [0, {len(reader.index)})", ref=rel)
        entry = reader.index[chunk]
        locator = reader.index.locator(chunk)
        chunk_info = ChunkEntryInspection(
            index=chunk,
            compress_type=entry.compress_type.name,
            stored_len=entry.stored_len,
            effective_len=entry.effective_len,
            offset=locator.offset,
            length=locator.length,
        )

    return BucketInspection(
        path=rel,
        major=header.major,
        minor=header.minor,
        mode=header.mode,
        mode_flags=mode_flags(header.mode),
        chunk_num=header.chunk_num,
        chunk_size_crc=header.chunk_size_crc,
        compress_type_counts=counts,
        expected_size=expected,
        actual_size=actual_size,
        size_check_ok=expected == actual_size if expected is not None else None,
        chunk=chunk_info,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class CompositionRecordInspection:
    """One composition record's fields; ``map_crc_ok`` is ``None`` unless
    ``walk_composition`` was asked to ``verify_map``."""

    head_off: int
    status: str
    map_num: int
    mode: int
    has_redundancy: bool
    attr_leng: int
    map_crc_ok: bool | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class CompositionWalk:
    """``walk_composition``'s result.

    Attributes:
        path: The walked file, as ``rel`` named it.
        header: The composition header's ``(major, minor)``; ``None`` when
            the file has none (only ``subID=0`` does).
        records: The records walked, at most ``limit``.
        next_offset: Where walking stopped.
        file_size: The file's size, which bounds the walk.
        stopped_with_error: Why a record failed to parse, when that ended
            the walk before ``limit`` or the end of the file.
    """

    path: str
    header: tuple[int, int] | None
    records: list[CompositionRecordInspection]
    next_offset: int
    file_size: int
    stopped_with_error: str | None = None


async def _verify_map_crc(store: ObjectStore, rel: str, map_off: int, record: RecordHead) -> bool:
    """Whether ``record``'s chunk-map array matches ``record.map_crc``; a
    storage failure still raises."""
    map_bytes = await store.read(rel, map_off, record.map_num * CHUNK_MAP_RECORD_LENGTH)
    try:
        await verify_chunk_map_crc_threaded(map_bytes, record.map_crc)
        return True
    except StorageBackendError:
        raise
    except ApmRepoError:
        return False


async def _walk_composition_records(
    store: ObjectStore, rel: str, *, start: int, file_size: int, limit: int, verify_map: bool
) -> tuple[list[CompositionRecordInspection], int, ApmRepoError | None]:
    """Walks up to ``limit`` composition records from ``start``, never
    past ``file_size``, as ``(records, next_offset, stopping_error)``. A
    record that fails to parse ends the walk without raising (it may just
    be the end of the data); ``walk_composition`` decides whether it is
    fatal."""
    pos = start
    records: list[CompositionRecordInspection] = []
    while pos < file_size and len(records) < limit:
        try:
            head_bytes = await store.read(rel, pos, RECORD_HEAD_LENGTH)
            record = parse_record_head(head_bytes)
        except StorageBackendError:
            raise
        except ApmRepoError as exc:
            return records, pos, exc
        map_crc_ok: bool | None = None
        if verify_map:
            map_off = chunk_map_array_offset(pos)
            map_crc_ok = await _verify_map_crc(store, rel, map_off, record)
        records.append(
            CompositionRecordInspection(
                head_off=pos,
                status=record.status.name,
                map_num=record.map_num,
                mode=record.mode,
                has_redundancy=record.has_redundancy,
                attr_leng=record.attr_leng,
                map_crc_ok=map_crc_ok,
            )
        )
        pos += record_total_length(record.map_num, record.attr_leng)
    return records, pos, None


async def walk_composition(
    store: ObjectStore, rel: str, *, offset: int | None = None, limit: int = 10, verify_map: bool = False
) -> CompositionWalk:
    """The composition header of ``rel``, if present, plus up to ``limit``
    records from ``offset`` (default: right after the header, else byte 0).
    A parse failure after something was parsed is reported in
    ``stopped_with_error``.

    Raises:
        ApmRepoError: Nothing could be parsed: no header, and not even one
            record.
    """
    head_probe = await store.read(rel, 0, 64)

    header: tuple[int, int] | None = None
    pos = 0
    if head_probe[:4] == MAGIC["composition"]:
        comp_header = parse_composition_header(head_probe)
        header = (comp_header.major, comp_header.minor)
        pos = 64
    if offset is not None:
        pos = offset

    file_size = await store.size(rel)
    records, next_offset, stop_exc = await _walk_composition_records(
        store, rel, start=pos, file_size=file_size, limit=limit, verify_map=verify_map
    )
    stopped_with_error: str | None = None
    if stop_exc is not None:
        if not records and header is None:
            raise stop_exc
        stopped_with_error = str(stop_exc)

    return CompositionWalk(
        path=rel,
        header=header,
        records=records,
        next_offset=next_offset,
        file_size=file_size,
        stopped_with_error=stopped_with_error,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkMapAddrInspection:
    """A ``MAPPING`` chunk-map entry's chunk address."""

    stream_id: int
    bucket_id: int
    chunk_idx: int


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkMapEntryInspection:
    """One ``ChunkMapRecord`` entry; ``addr`` is set only for a
    ``ChunkMapKind.MAPPING`` entry."""

    index: int
    kind: str
    file_offset: int
    end_offset: int
    is_inherit: bool
    map_num: int
    repeat: int
    addr: ChunkMapAddrInspection | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkMapInspection:
    """``inspect_chunk_map``'s result: the record's entry count, the
    decoded entries, and the map-CRC verdict when checked."""

    path: str
    head_off: int
    map_num: int
    map_crc_ok: bool | None
    entries: list[ChunkMapEntryInspection]


async def inspect_chunk_map(
    store: ObjectStore, rel: str, offset: int, *, limit: int = 50, verify: bool = False
) -> ChunkMapInspection:
    """The first ``limit`` entries of the ``ChunkMapRecord`` array of the
    record at ``offset``, plus the chunk-map CRC verdict when ``verify``
    is set."""
    record = parse_record_head(await store.read(rel, offset, RECORD_HEAD_LENGTH))

    map_crc_ok: bool | None = None
    map_off = chunk_map_array_offset(offset)
    if verify:
        map_crc_ok = await _verify_map_crc(store, rel, map_off, record)

    shown = min(record.map_num, limit)
    raw = b"" if shown == 0 else await store.read(rel, map_off, shown * CHUNK_MAP_RECORD_LENGTH)
    entries: list[ChunkMapEntryInspection] = []
    for i in range(shown):
        parsed = parse_chunk_map_record(raw[i * CHUNK_MAP_RECORD_LENGTH : (i + 1) * CHUNK_MAP_RECORD_LENGTH])
        addr: ChunkMapAddrInspection | None = None
        if parsed.kind is ChunkMapKind.MAPPING:
            assert parsed.addr is not None
            addr = ChunkMapAddrInspection(
                stream_id=int(parsed.addr.stream_id),
                bucket_id=int(parsed.addr.bucket_id),
                chunk_idx=int(parsed.addr.chunk_idx),
            )
        entries.append(
            ChunkMapEntryInspection(
                index=i,
                kind=parsed.kind.name,
                file_offset=parsed.file_offset,
                end_offset=parsed.end_offset,
                is_inherit=parsed.is_inherit,
                map_num=parsed.map_num,
                repeat=parsed.repeat,
                addr=addr,
            )
        )

    return ChunkMapInspection(path=rel, head_off=offset, map_num=record.map_num, map_crc_ok=map_crc_ok, entries=entries)
