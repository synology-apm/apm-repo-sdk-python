"""Regression tests for ``dedup.composition_reader`` against one real
composition record: header verify, record head, a full sequential
``entries()`` walk, a binary-search start partway through the map array,
and a bounded ``[start, end)`` walk.

Fixture: ``dedup_composition_reader_c0_8_vault_plain.json.gz``, recorded against
``vault-plain``. No single test makes every call, so recording needs the
whole file run together.
"""

from __future__ import annotations

import itertools
import zlib
from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.format.composition import CompositionStatus
from synology_apm_repo.sdk.format.const import CHUNK_MAP_RECORD_LENGTH
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage import DirCache
from synology_apm_repo.sdk.storage.base import ObjectStore

_COMP_ROOT = "@ActiveProtectVault/@data/Composition"
_STREAM_ID = StreamId(132)
_SESSION_ID = SessionId(0)
_HEAD_OFF = 64
_MAP_NUM = 37
_MAP_CRC = 0xC9D61ADB
_MAP_ARRAY_OFF = _HEAD_OFF + 32  # chunk_map_array_offset(64)


async def _reader(record_target: Callable[[str], Awaitable[ObjectStore]]) -> CompositionReader:
    store = await record_target("dedup_composition_reader_c0_8_vault_plain.json.gz")
    return CompositionReader(store, DirCache(store), _COMP_ROOT, _STREAM_ID, _SESSION_ID)


async def test_replayed_verify_header_matches_real_subfile_size_constant(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    # verify_header() raises unless subFileSize is the fixed 16 MiB.
    header = await (await _reader(record_target)).verify_header()
    assert header.major == 1


async def test_replayed_record_head_matches_previously_confirmed_raw_values(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    record = await (await _reader(record_target)).record(_HEAD_OFF)
    assert record.status is CompositionStatus.COMPLETE
    assert record.map_num == _MAP_NUM
    assert record.attr_leng == 60


async def test_replayed_full_walk_reproduces_the_real_recorded_mapcrc(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    # entries() doesn't verify mapCrc; recomputing it over the raw array
    # checks that the walk below covers the real records.
    reader = await _reader(record_target)
    raw = await reader.read_at(_MAP_ARRAY_OFF, _MAP_NUM * CHUNK_MAP_RECORD_LENGTH)
    assert zlib.crc32(raw) & 0xFFFFFFFF == _MAP_CRC

    record = await reader.record(_HEAD_OFF)
    entries = [e async for e in record.entries()]
    assert len(entries) == _MAP_NUM
    assert all(a.file_offset < b.file_offset for a, b in itertools.pairwise(entries))


async def test_replayed_binary_search_lands_on_the_correct_entry_partway_through(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    reader = await _reader(record_target)
    record = await reader.record(_HEAD_OFF)
    all_entries = [e async for e in record.entries()]
    mid = all_entries[len(all_entries) // 2]

    # Starting exactly at an entry's file_offset yields that entry first.
    from_binary_search = [e async for e in record.entries(start=mid.file_offset)]
    assert from_binary_search[0].file_offset == mid.file_offset
    assert from_binary_search[0].kind == mid.kind
    assert len(from_binary_search) == len(all_entries) - all_entries.index(mid)


async def test_replayed_start_one_byte_into_an_entry_still_returns_that_entry(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    reader = await _reader(record_target)
    record = await reader.record(_HEAD_OFF)
    all_entries = [e async for e in record.entries()]
    target = next(e for e in all_entries if e.length > 1)
    result = [e async for e in record.entries(start=target.file_offset + 1, end=target.end_offset)]
    assert len(result) == 1
    assert result[0].file_offset == target.file_offset
