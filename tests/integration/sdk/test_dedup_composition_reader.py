"""Regression test for ``dedup.composition_reader`` — replayed from a
committed fixture recorded against a real composition sub-file
(``apv-sample-1``'s ``Composition/132/0.com/c0.8``) — including binary
search landing on an entry partway through the real 37-entry map array,
not just a full sequential walk — with **no external dependency**: this
always runs, on CI or anywhere else, because it goes through
``ReplayStore`` instead of a real ``LocalFsStore``.

The fixture (``tests/fixtures/dedup_composition_reader_c0_8_apv1.json.gz``)
was produced against a real store rooted at ``apv-sample-1`` —
recording every ``ObjectStore`` call a header-verify, a record-head
read, a full sequential ``entries()`` walk, a binary-search walk, and a
bounded ``[start, end)`` walk make against the same real record. No
single test below reproduces all five call shapes, so re-recording needs
all five run together against one real backend, not just the widest one
— see ``tests/CLAUDE.md``'s "Recording a fixture" section.
"""

from __future__ import annotations

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
    store = await record_target("dedup_composition_reader_c0_8_apv1.json.gz")
    return CompositionReader(store, DirCache(store), _COMP_ROOT, _STREAM_ID, _SESSION_ID)


async def test_replayed_verify_header_matches_real_subfile_size_constant(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
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
    # entries() never auto-verifies mapCrc (interactive reads skip it)
    # but a full sequential walk touches every raw 20-byte record,
    # so recomputing the CRC over those same bytes here is an independent
    # end-to-end check that entries() is walking the *real* array, not just
    # that individual fields decode.
    reader = await _reader(record_target)
    raw = await reader.read_at(_MAP_ARRAY_OFF, _MAP_NUM * CHUNK_MAP_RECORD_LENGTH)
    assert zlib.crc32(raw) & 0xFFFFFFFF == _MAP_CRC

    record = await reader.record(_HEAD_OFF)
    entries = [e async for e in record.entries()]
    assert len(entries) == _MAP_NUM
    # entries are strictly ordered by file_offset (on-disk-format.md §8)
    # (deliberately strict=False: entries[1:] is one shorter by construction)
    assert all(a.file_offset < b.file_offset for a, b in zip(entries, entries[1:], strict=False))


async def test_replayed_binary_search_lands_on_the_correct_entry_partway_through(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    reader = await _reader(record_target)
    record = await reader.record(_HEAD_OFF)
    all_entries = [e async for e in record.entries()]
    mid = all_entries[len(all_entries) // 2]

    # asking to start exactly at a real, non-trivial entry's file_offset
    # must resolve (via binary search, not a sequential walk) to that same
    # entry as the first result.
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


__all__: list[str] = []
