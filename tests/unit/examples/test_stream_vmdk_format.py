"""Tests for the VMDK that examples/hypervisor-restore/stream_vmdk.py's ``StreamOptimizedVmdkSink`` writes: a
streamOptimized reader here decodes it, through the footer's grain directory, back to the exported disk."""

from __future__ import annotations

import struct
import zlib
from types import ModuleType
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from support.fakes import unchecked_fake
from synology_apm_repo.sdk.export import run_export
from unit.examples.hypervisor_restore_fakes import ExtentContent

_SECTOR = 512
_GRAIN = 64 << 10
_HEADER = struct.Struct("<IIIQQQQIQQQB4sH433x")


@unchecked_fake("examples/hypervisor-restore/stream_vmdk.py's VmdkOutput, loaded at test time")
class _MemoryOutput:
    def __init__(self) -> None:
        self.data = bytearray()
        self.finished = False
        self.aborted = False

    async def write(self, data: bytes) -> None:
        self.data += data

    async def finish(self) -> None:
        self.finished = True

    async def abort(self) -> None:
        self.aborted = True


def _flat(size: int, extents: dict[int, bytes]) -> bytes:
    disk = bytearray(size)
    for offset, data in extents.items():
        disk[offset : offset + len(data)] = data
    return bytes(disk)


def _decode(vmdk: bytes) -> tuple[dict[str, Any], bytes]:
    """The footer's header fields and the disk the VMDK holds, read through its grain directory and tables."""
    names = ("magic", "version", "flags", "capacity", "grain", "desc_offset", "desc_size", "gtes", "rgd", "gd")
    footer = dict(zip(names, _HEADER.unpack(vmdk[-2 * _SECTOR : -_SECTOR])[:10], strict=True))
    grain_sectors, gtes = footer["grain"], footer["gtes"]
    grains = -(-footer["capacity"] // grain_sectors)
    tables = -(-grains // gtes)
    directory = struct.unpack_from(f"<{tables}I", vmdk, footer["gd"] * _SECTOR)
    disk = bytearray(grains * grain_sectors * _SECTOR)
    for table_index, table_sector in enumerate(directory):
        if table_sector == 0:
            continue
        for entry, sector in enumerate(struct.unpack_from(f"<{gtes}I", vmdk, table_sector * _SECTOR)):
            if sector == 0:
                continue
            lba, size = struct.unpack_from("<QI", vmdk, sector * _SECTOR)
            grain = table_index * gtes + entry
            assert lba == grain * grain_sectors
            data = zlib.decompress(vmdk[sector * _SECTOR + 12 : sector * _SECTOR + 12 + size])
            disk[lba * _SECTOR : lba * _SECTOR + len(data)] = data
    return footer, bytes(disk[: footer["capacity"] * _SECTOR])


async def _export(ex: ModuleType, content: ExtentContent, *, cid: int = 1) -> _MemoryOutput:
    output = _MemoryOutput()
    await run_export(content, ex.StreamOptimizedVmdkSink(output, cid=cid, file_name="d.vmdk"))
    return output


# -- round trip ------------------------------------------------------------------


@st.composite
def _sparse_disks(draw: st.DrawFn) -> tuple[int, dict[int, bytes]]:
    size = draw(st.integers(1, 40)) * 4096
    extents: dict[int, bytes] = {}
    for _ in range(draw(st.integers(0, 4))):
        offset = draw(st.integers(0, size - 1))
        extents[offset] = draw(st.binary(min_size=1, max_size=min(size - offset, 3 * _GRAIN)))
    return size, extents


@given(_sparse_disks())
async def test_vmdk_decodes_back_to_the_disk(ex: ModuleType, disk: tuple[int, dict[int, bytes]]) -> None:
    size, extents = disk

    output = await _export(ex, ExtentContent(size, extents))

    _, decoded = _decode(bytes(output.data))
    assert decoded == _flat(size, extents)
    assert output.finished and not output.aborted


async def test_vmdk_spans_segments_and_grain_tables(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    """A disk over several segments and past one grain table's 32 MiB still decodes."""
    monkeypatch.setattr(ex, "_SEGMENT_SIZE", 4 * _GRAIN)
    size = 33 << 20
    extents = {0: b"boot" * 1000, 5 * _GRAIN - 3: b"straddles a segment end", (32 << 20) + 7: b"second table"}

    output = await _export(ex, ExtentContent(size, extents))

    footer, decoded = _decode(bytes(output.data))
    assert decoded == _flat(size, extents)
    directory = struct.unpack_from("<2I", output.data, footer["gd"] * _SECTOR)
    assert all(directory)


# -- layout ----------------------------------------------------------------------


async def test_vmdk_header_descriptor_and_footer(ex: ModuleType) -> None:
    size = 10 * _GRAIN + 1000  # not a whole sector: the capacity rounds up
    output = await _export(ex, ExtentContent(size, {0: b"x"}), cid=0xABCDEF)
    vmdk = bytes(output.data)

    header = _HEADER.unpack(vmdk[:_SECTOR])
    footer, _ = _decode(vmdk)
    capacity = -(-size // _SECTOR)
    assert header[:8] == (0x564D444B, 3, 0x30001, capacity, 128, 1, header[6], 512)
    assert header[9] == 0xFFFFFFFFFFFFFFFF  # the directory comes at the end
    assert header[10:] == (128, 0, b"\n \r\n", 1)
    assert footer["capacity"] == capacity
    assert footer["gd"] != header[9]
    text = vmdk[_SECTOR : (1 + header[6]) * _SECTOR].rstrip(b"\0").decode()
    assert 'createType="streamOptimized"' in text
    assert f'RW {capacity} SPARSE "d.vmdk"' in text
    assert "CID=00abcdef" in text
    assert vmdk[-_SECTOR:] == bytes(_SECTOR)  # the end-of-stream marker
    assert len(vmdk) % _SECTOR == 0


def _stored_grains(vmdk: bytes) -> list[int]:
    """The grains the VMDK's tables point at."""
    footer, _ = _decode(vmdk)
    tables = -(-footer["capacity"] // (footer["grain"] * footer["gtes"]))
    stored = []
    for index, table in enumerate(struct.unpack_from(f"<{tables}I", vmdk, footer["gd"] * _SECTOR)):
        if table:
            entries = struct.unpack_from(f"<{footer['gtes']}I", vmdk, table * _SECTOR)
            stored += [index * footer["gtes"] + i for i, sector in enumerate(entries) if sector]
    return stored


async def test_vmdk_leaves_out_zero_grains(ex: ModuleType) -> None:
    size = 64 * _GRAIN
    sparse = await _export(ex, ExtentContent(size, {3 * _GRAIN: b"\1", 9 * _GRAIN: bytes(_GRAIN)}))
    empty = await _export(ex, ExtentContent(size, {}))

    assert _stored_grains(bytes(sparse.data)) == [3]
    assert _stored_grains(bytes(empty.data)) == []
    assert _decode(bytes(empty.data))[1] == bytes(size)


async def test_vmdk_size_is_the_same_on_every_run(ex: ModuleType) -> None:
    """What a two-pass upload relies on: the second export comes out the size the first counted."""
    content = ExtentContent(20 * _GRAIN, {100: bytes(range(256)) * 400, 9 * _GRAIN: b"tail" * 99})

    first = await _export(ex, content, cid=7)
    second = await _export(ex, content, cid=7)
    counter = ex.CountingOutput()
    await run_export(content, ex.StreamOptimizedVmdkSink(counter, cid=7))

    assert first.data == second.data
    assert counter.size == len(first.data)


async def test_vmdk_aborts_the_output_when_the_export_fails(ex: ModuleType) -> None:
    output = _MemoryOutput()
    sink = ex.StreamOptimizedVmdkSink(output, cid=1)

    with pytest.raises(RuntimeError, match="synthetic failure"):
        await run_export(ExtentContent(4 * _GRAIN, {0: b"x"}, fail_at=0), sink)

    assert output.aborted and not output.finished
