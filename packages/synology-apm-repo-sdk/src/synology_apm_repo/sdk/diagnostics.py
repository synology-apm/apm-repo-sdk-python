"""Repository Layer sibling — raw, catalog-bypassing inspection of one
physical file, addressed by a bare store-relative path rather than a
``NodeRef``. For forensics tooling (the CLI's ``dump`` command family) that
needs "what does this byte layout actually say" for one
``.buk``/composition/chunk-map file outside any discovered repository —
the same Codec/Dedup Layer parsers the real read path uses, wrapped here
so CLI/TUI code never imports ``format``/``storage``/``dedup`` internals
directly for this. Every function here takes an already-resolved
``ObjectStore`` rather than opening one itself — the CLI decides local vs.
``--profile`` and hands in whichever store applies; ``open_local()`` is
only the local-path adapter it uses for its own local case.

Deliberately out of scope: resolving a ``.<N>`` sequence-id suffix
(FORMAT-SPEC.md: sequence-id-suffix) from a logical name — callers pass
the literal on-disk filename, local or remote, the same way they always
have.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from pathlib import Path

from .dedup.pool import BucketReader
from .dedup.verify_checks import verify_chunk_map_crc_threaded
from .errors import ApmRepoError
from .format.bucket import expected_bucket_size
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
    (0x01, "compress"),
    (0x02, "chunk_crc"),
    (0x04, "bucket_parity"),
    (0x08, "encrypt"),
    (0x10, "logic_locality"),
    (0x20, "extent_parity"),
    (0x40, "inplace_parity"),
    (0x80, "vault_encrypt"),
]


def mode_flags(mode: int) -> list[str]:
    """Bucket-header ``mode`` bits as their names — bit→string is format
    knowledge, not a CLI rendering concern."""
    return [name for bit, name in _MODE_BIT_NAMES if mode & bit]


def open_local(path: Path) -> tuple[ObjectStore, str]:
    """Split a literal local file path into an ``ObjectStore`` rooted at
    the parent directory plus the bare filename — the CLI's own adapter
    for its local (non-``--profile``) case, so the actual reads still go
    through the storage layer, the same as every ``--profile``-resolved
    remote store, rather than a second, ad hoc ``open()`` call here."""
    return LocalFsStore(path.parent), path.name


@dataclasses.dataclass(frozen=True)
class ChunkEntryInspection:
    """One ``--chunk``-selected entry's own fields, nested under
    ``BucketInspection`` when a specific chunk was asked for."""

    index: int
    compress_type: str
    stored_len: int
    effective_len: int
    offset: int
    length: int


@dataclasses.dataclass(frozen=True)
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
    expected_size: int
    actual_size: int
    size_check_ok: bool
    chunk: ChunkEntryInspection | None = None


async def inspect_bucket(store: ObjectStore, rel: str, *, chunk: int | None = None) -> BucketInspection:
    """Inspect a ``.buk`` file's header, SizeStore summary, and
    ``expected_bucket_size`` self-check.

    Raises:
        IndexError: ``chunk`` is out of range for this bucket's own
            entry count.
    """
    reader = await BucketReader.open(store, rel)
    actual_size = await store.size(rel)

    header = reader.header
    counts = dict(Counter(entry.compress_type.name for entry in reader.entries))
    expected = expected_bucket_size(header, reader.entries)

    chunk_info: ChunkEntryInspection | None = None
    if chunk is not None:
        if not 0 <= chunk < len(reader.entries):
            raise IndexError(f"--chunk {chunk} out of range [0, {len(reader.entries)})")
        entry = reader.entries[chunk]
        locator = reader.locators[chunk]
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
        size_check_ok=expected == actual_size,
        chunk=chunk_info,
    )


@dataclasses.dataclass(frozen=True)
class CompositionRecordInspection:
    """One composition record's own fields, mirroring
    ``ChunkMapEntryInspection``'s shape one section down: ``map_crc_ok`` is
    only present when ``verify_map`` was given (see ``walk_composition``'s
    own body), never a placeholder ``None`` otherwise."""

    head_off: int
    status: str
    map_num: int
    mode: int
    has_redundancy: bool
    attr_leng: int
    map_crc_ok: bool | None = None


@dataclasses.dataclass(frozen=True)
class CompositionWalk:
    """``walk_composition``'s result: the header (if the file starts with
    one — ``subID=0`` only), every record walked up to ``limit``, the
    offset walking stopped at, and — only when the walk stopped because a
    record failed to parse partway through, not because it reached
    ``limit``/the file's end — that failure's message."""

    path: str
    header: tuple[int, int] | None
    records: list[CompositionRecordInspection]
    next_offset: int
    file_size: int
    stopped_with_error: str | None = None


async def _verify_map_crc(store: ObjectStore, rel: str, map_off: int, record: RecordHead) -> bool:
    """Read ``record``'s chunk-map array bytes and check them against
    ``record.map_crc`` via ``verify_chunk_map_crc_threaded``, collapsing
    any mismatch to ``False`` rather than propagating — shared by
    ``_walk_composition_records``'s and ``inspect_chunk_map``'s identical
    read-then-verify-then-collapse shape."""
    map_bytes = await store.read(rel, map_off, record.map_num * CHUNK_MAP_RECORD_LENGTH)
    try:
        await verify_chunk_map_crc_threaded(map_bytes, record.map_crc)
        return True
    except ApmRepoError:
        return False


async def _walk_composition_records(
    store: ObjectStore, rel: str, *, start: int, file_size: int, limit: int, verify_map: bool
) -> tuple[list[CompositionRecordInspection], int, ApmRepoError | None]:
    """Walks up to ``limit`` composition records starting at ``start``,
    never past ``file_size``. Stops cleanly (not raising) the moment a
    record fails to parse, since that's the normal end-of-walk boundary
    for a file with no trailing padding, not necessarily corruption — see
    ``walk_composition``'s own docstring for how the caller decides whether
    that failure is fatal.

    Returns ``(records, next_offset, stopping_error)`` — ``stopping_error``
    is ``None`` only when the walk stopped because it reached ``limit``
    or ``file_size``, never because a record failed to parse.
    """
    pos = start
    records: list[CompositionRecordInspection] = []
    while pos < file_size and len(records) < limit:
        try:
            head_bytes = await store.read(rel, pos, RECORD_HEAD_LENGTH)
            record = parse_record_head(head_bytes)
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
    """Walk composition records in ``rel`` — the header, if present
    (``subID=0`` only), plus up to ``limit`` records starting at
    ``offset`` (default: right after the header, or byte 0 for a
    non-``subID=0`` file).

    Raises the underlying ``ApmRepoError`` when nothing at all could be
    parsed (no header, and the record walk failed before it collected
    even one record); a failure partway through an otherwise-successful
    walk is reported in the returned ``CompositionWalk``'s
    ``stopped_with_error`` instead.
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


@dataclasses.dataclass(frozen=True)
class ChunkMapAddrInspection:
    """A ``MAPPING`` chunk-map entry's resolved chunk address — which
    stream, bucket, and chunk index it points at."""

    stream_id: int
    bucket_id: int
    chunk_idx: int


@dataclasses.dataclass(frozen=True)
class ChunkMapEntryInspection:
    """One ``ChunkMapRecord`` entry — ``addr`` only present for a
    ``ChunkMapKind.MAPPING`` record, every other kind omits it entirely
    rather than carrying a placeholder ``None``."""

    index: int
    kind: str
    file_offset: int
    end_offset: int
    is_inherit: bool
    map_num: int
    repeat: int
    addr: ChunkMapAddrInspection | None = None


@dataclasses.dataclass(frozen=True)
class ChunkMapInspection:
    """The result of dumping one record's ``ChunkMapRecord`` array — its
    own map-entry count, decoded entries, and (when checked) the mapCrc
    verdict."""

    path: str
    head_off: int
    map_num: int
    map_crc_ok: bool | None
    entries: list[ChunkMapEntryInspection]


async def inspect_chunk_map(
    store: ObjectStore, rel: str, offset: int, *, limit: int = 50, verify: bool = False
) -> ChunkMapInspection:
    """Dump the ``ChunkMapRecord`` array — the core read structure — of
    the record at ``offset``: each entry's kind, file/end offsets,
    inherit flag, map number and repeat count, plus (with ``verify=True``)
    the chunk-map CRC check result."""
    record = parse_record_head(await store.read(rel, offset, RECORD_HEAD_LENGTH))

    map_crc_ok: bool | None = None
    map_off = chunk_map_array_offset(offset)
    if verify:
        map_crc_ok = await _verify_map_crc(store, rel, map_off, record)

    shown = min(record.map_num, limit)
    # One read covering every shown entry, not one read per entry — the
    # same "read once, parse many" shape composition_reader.py's own
    # page-fetching uses, and this function's own sibling _verify_map_crc
    # already does for the identical array. shown == 0 (record.map_num == 0,
    # or --limit 0) skips the read entirely rather than issuing a
    # zero-length one — a length of 0 has no real meaning to ask a backend
    # for, and nothing below needs its result anyway.
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
