"""Regression test for ``format.composition`` — replayed from a committed
fixture recorded against a real composition sub-file (``apv-sample-1``'s
``Composition/132/0.com/c0.8``, corresponding to ``db/file_map``'s
``stream_id=132, session_id=0, comp_offset=64`` row), with **no external
dependency**: this always runs, on CI or anywhere else, because it goes
through ``ReplayStore`` instead of reading the real file directly off
disk.

The strongest check here is ``record_total_length``: it must land exactly
on the *next* record's ``"Mu"`` magic — proving the entire record size
formula (RecordHead + chunk-map array + extAttr + Redundancy trailer) end
to end, not just that individual fields parse.

The fixture (``tests/fixtures/format_composition_c0_8_header_apv1.json.gz``,
recorded once by ``RecordingStore`` via each test's own
``record_target()`` call — see ``tests/conftest.py`` and
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...`` workflow that (re-)records it) was produced
against a real store rooted at ``apv-sample-1``, recording the first 96
bytes of ``c0.8`` plus the 2-byte magic at its next record's computed
offset (1656).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.format.composition import parse_composition_header, parse_record_head, record_total_length
from synology_apm_repo.sdk.storage.base import ObjectStore

_REL_PATH = "@ActiveProtectVault/@data/Composition/132/0.com/c0.8"
_COMP_OFFSET = 64  # db/file_map row: stream_id=132, session_id=0, comp_offset=64


async def test_replayed_composition_header_and_first_record(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("format_composition_c0_8_header_apv1.json.gz")
    data = await store.read(_REL_PATH, 0, 96)

    header = parse_composition_header(data[:64])
    assert header.major == 1
    assert header.minor == 1

    record = parse_record_head(data[_COMP_OFFSET : _COMP_OFFSET + 32])
    assert record.map_num == 37
    assert record.map_crc == 0xC9D61ADB
    assert record.mode == 0x0001
    assert record.attr_leng == 60
    assert record.attr_crc == 0x3487C5DE
    assert record.has_redundancy is True


async def test_replayed_record_total_length_lands_exactly_on_next_record(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("format_composition_c0_8_header_apv1.json.gz")
    data = await store.read(_REL_PATH, 0, 96)
    record = parse_record_head(data[_COMP_OFFSET : _COMP_OFFSET + 32])

    next_off = _COMP_OFFSET + record_total_length(record.map_num, record.attr_leng)
    assert next_off == 1656

    next_magic = await store.read(_REL_PATH, next_off, 2)
    assert next_magic == b"Mu"


__all__: list[str] = []
