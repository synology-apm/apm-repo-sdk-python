"""Tests for the zip flow of examples/export_disk_to_zip.py: ``ZipSink`` (the ``BufferedExportSink`` that owns one
entry), ``export_disk``, ``write_zip`` and ``collect_disks``."""

from __future__ import annotations

import asyncio
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk import Node, NodeRef, RestorableUnit, UnitKind
from synology_apm_repo.sdk.export import ExportResult, run_export
from synology_apm_repo.sdk.units.base import ContentSource, UnitProvider

_SEG = 4096


@faithful_to(ContentSource)
class _Content:
    """A ``ContentSource`` over bytes whose export writes them, optionally failing at one offset."""

    def __init__(self, data: bytes, *, fail_at: int | None = None) -> None:
        self._data = data
        self.size = len(data)
        self._fail_at = fail_at

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    def stream(self, block: int = 0) -> Any:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self, writer: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: object = None
    ) -> ExportResult:
        if start == self._fail_at:
            raise RuntimeError(f"synthetic failure at {start}")
        await writer.write_at(0, self._data[start:end])
        if progress is not None:
            await progress(end - start)
        return ExportResult(bytes_written=end - start, logical_size=end - start, holes=0, zeros=0)


def _disk(ex: ModuleType, label: str, content: _Content) -> Any:
    unit = RestorableUnit(
        ref=NodeRef("repo", (label,)),
        name=label,
        is_leaf=True,
        kind=UnitKind.DISK_IMAGE,
        size=content.size,
        content=content,
    )
    return ex.SourceDisk(label, unit, content.size)


def _settings(ex: ModuleType, output: Path, *, segment_size: int = _SEG) -> Any:
    return ex.ZipSettings(output=output, segment_size=segment_size, buffered_segments=2, level=6)


def _payload(size: int, seed: int) -> bytes:
    """Bytes that neither compress to nothing nor repeat across segments."""
    return bytes((seed + i * 7 + (i >> 8)) % 251 for i in range(size))


# -- ZipSink ---------------------------------------------------------------------


async def test_zip_sink_writes_a_disk_into_one_compressed_entry_in_order(ex: ModuleType, tmp_path: Path) -> None:
    data = _payload(3 * _SEG + 100, seed=1)
    path = tmp_path / "out.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        sink = ex.ZipSink(archive, "disk.img", segment_size=_SEG, buffered_segments=2)

        result = await run_export(_Content(data), sink)

    assert result.bytes_written == len(data)
    with zipfile.ZipFile(path) as archive:
        info = archive.getinfo("disk.img")
        assert info.compress_type == zipfile.ZIP_DEFLATED
        assert info.file_size == len(data)
        assert archive.read("disk.img") == data
        assert archive.testzip() is None


async def test_zip_sink_compresses_at_the_archives_level(ex: ModuleType, tmp_path: Path) -> None:
    data = b"".join(bytes([(i * 31) % 251]) * (1 + i % 5) + bytes(range(i % 200)) for i in range(4000))
    sizes = {}
    for level in (1, 9):
        path = tmp_path / f"level{level}.zip"
        with zipfile.ZipFile(
            path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=level, allowZip64=True
        ) as archive:
            sink = ex.ZipSink(archive, "disk.img", segment_size=_SEG * 64, buffered_segments=2)
            await run_export(_Content(data), sink)
            sizes[level] = archive.getinfo("disk.img").compress_size
        with zipfile.ZipFile(path) as archive:
            assert archive.read("disk.img") == data
    assert sizes[9] < sizes[1]


async def test_zip_sink_keeps_the_archive_usable_after_a_failed_export(ex: ModuleType, tmp_path: Path) -> None:
    """The failed entry is closed, so the archive can still be closed and the caller can delete it."""
    path = tmp_path / "out.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        sink = ex.ZipSink(archive, "disk.img", segment_size=_SEG, buffered_segments=2)

        with pytest.raises(RuntimeError, match="synthetic failure at 4096"):
            await run_export(_Content(_payload(3 * _SEG, 2), fail_at=_SEG), sink)

    assert path.exists()  # closing the archive did not raise "open writing handle"


async def test_zip_sink_uses_shared_memory_so_workers_can_write_into_it(ex: ModuleType, tmp_path: Path) -> None:
    with zipfile.ZipFile(tmp_path / "out.zip", "w") as archive:
        sink = ex.ZipSink(archive, "disk.img", segment_size=_SEG, buffered_segments=2)
        await sink.open(_SEG, sparse=True)
        try:
            writer = await sink.begin_segment(0, 0, _SEG)
            assert writer.worker_target() is not None
        finally:
            await sink.abort()


# -- export_disk and write_zip ---------------------------------------------------


async def test_export_disk_prints_the_stored_size_and_the_wait(
    ex: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data = bytes(40 * _SEG)  # compresses to almost nothing
    with zipfile.ZipFile(tmp_path / "out.zip", "w", compression=zipfile.ZIP_DEFLATED) as archive:
        await ex.export_disk(archive, _disk(ex, "d", _Content(data)), "d.img", 0, 1, _settings(ex, tmp_path / "x"))

    err = capsys.readouterr().err
    assert "disk 1/1:" in err and "waited for the zip" in err
    assert "%" in err


async def test_write_zip_puts_every_disk_in_its_own_entry_and_publishes_the_zip_last(
    ex: ModuleType, tmp_path: Path
) -> None:
    first, second = _payload(2 * _SEG + 5, 3), _payload(_SEG, 4)
    output = tmp_path / "out.zip"
    disks = [_disk(ex, "a", _Content(first)), _disk(ex, "b", _Content(second))]

    await ex.write_zip(disks, ["01-a.img", "02-b.img"], _settings(ex, output))

    with zipfile.ZipFile(output) as archive:
        assert archive.namelist() == ["01-a.img", "02-b.img"]
        assert archive.read("01-a.img") == first
        assert archive.read("02-b.img") == second
    assert not (tmp_path / "out.zip.part").exists()


async def test_a_failing_disk_leaves_neither_a_part_file_nor_a_zip(ex: ModuleType, tmp_path: Path) -> None:
    output = tmp_path / "out.zip"
    disks = [_disk(ex, "a", _Content(_payload(_SEG, 5))), _disk(ex, "b", _Content(_payload(2 * _SEG, 6), fail_at=_SEG))]

    with pytest.raises(RuntimeError, match="synthetic failure"):
        await ex.write_zip(disks, ["a.img", "b.img"], _settings(ex, output))

    assert sorted(path.name for path in tmp_path.iterdir()) == []


async def test_a_failed_export_does_not_touch_an_existing_zip(ex: ModuleType, tmp_path: Path) -> None:
    output = tmp_path / "out.zip"
    output.write_bytes(b"the previous export")
    disks = [_disk(ex, "a", _Content(_payload(2 * _SEG, 7), fail_at=0))]

    with pytest.raises(RuntimeError, match="synthetic failure"):
        await ex.write_zip(disks, ["a.img"], _settings(ex, output))

    assert output.read_bytes() == b"the previous export"
    assert not (tmp_path / "out.zip.part").exists()


async def test_a_cancelled_export_leaves_no_part_file(ex: ModuleType, tmp_path: Path) -> None:
    output = tmp_path / "out.zip"

    started = asyncio.Event()

    class _Stuck(_Content):
        async def export_range(
            self, writer: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: Any = None
        ) -> ExportResult:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    task = asyncio.ensure_future(ex.write_zip([_disk(ex, "a", _Stuck(bytes(_SEG)))], ["a.img"], _settings(ex, output)))
    await asyncio.wait_for(started.wait(), 10)  # cancelled mid-export
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert list(tmp_path.iterdir()) == []


# -- collect_disks ---------------------------------------------------------------


@faithful_to(UnitProvider)
class _Provider:
    def __init__(self, children: dict[str, list[Node]]) -> None:
        self._children = children

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children.get(node.name, [])

    async def unit(self, node: Node) -> RestorableUnit:
        return RestorableUnit(
            ref=node.ref, name=node.name, is_leaf=True, kind=node.kind, size=node.size, content=_Content(b"")
        )


def _node(name: str, *, leaf: bool, kind: UnitKind | None = None, size: int | None = None) -> Node:
    return Node(ref=NodeRef("repo", (name,)), name=name, is_leaf=leaf, kind=kind, size=size)


async def test_collect_disks_lists_every_disk_image_and_skips_the_filesystem_view(ex: ModuleType) -> None:
    root = _node("Devices", leaf=False)
    provider = _Provider(
        {
            "Devices": [_node("host-1", leaf=False)],
            "host-1": [
                _node("disk.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=30),
                _node("(filesystem)", leaf=False, kind=UnitKind.DISK_FILESYSTEM),
            ],
            "(filesystem)": [_node("boot.ini", leaf=True, kind=UnitKind.FILE, size=1)],
        }
    )

    disks = await ex.collect_disks(provider, root)

    assert [(disk.label, disk.size) for disk in disks] == [("host-1/disk.img", 30)]


async def test_collect_disks_refuses_a_ref_without_disks(ex: ModuleType) -> None:
    with pytest.raises(SystemExit, match="no restorable disk images"):
        await ex.collect_disks(_Provider({}), _node("Devices", leaf=False))
