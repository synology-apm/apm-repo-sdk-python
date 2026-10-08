"""Unit tests for ``synology_apm_repo.sdk.units.content.local_file``."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk.dedup.export_sink import ExportWriter
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.export import LocalFileSink, run_export
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.content.local_file import LocalFileContentSource
from synology_apm_repo.sdk.units.device import DeviceProvider
from unit.sdk.dedup_export_fakes import SegmentCollector
from unit.sdk.device_fakes import build_vm_repo, vm_version

_DATA = b"0123456789abcdef"


pytestmark = pytest.mark.usefixtures("no_disk_fs_sibling")


@pytest.fixture
def store(tmp_path: Path) -> LocalFsStore:
    (tmp_path / "meta").mkdir()
    (tmp_path / "meta" / "disk.vmdk").write_bytes(_DATA)
    return LocalFsStore(tmp_path)


class TestCreate:
    async def test_resolves_an_unknown_size_from_the_store(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        assert source.size == len(_DATA)

    async def test_keeps_a_caller_supplied_size(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", 4)
        assert source.size == 4


class TestRead:
    async def test_reads_a_range_and_the_whole_file(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        assert await source.read(4, 4) == b"4567"
        assert await source.read() == _DATA

    async def test_negative_offset_or_length_raises(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        with pytest.raises(ValueError, match="non-negative"):
            await source.read(-1)
        with pytest.raises(ValueError, match="non-negative"):
            await source.read(0, -1)

    async def test_stream_yields_offset_ordered_blocks(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        blocks = [(offset, block) async for offset, block in source.stream(block=6)]
        assert blocks == [(0, b"012345"), (6, b"6789ab"), (12, b"cdef")]


class TestExportTo:
    async def test_copies_the_whole_file_and_reports_it(self, store: LocalFsStore, tmp_path: Path) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        dst = tmp_path / "out.bin"
        result = await run_export(source, LocalFileSink(dst, staged=False))
        assert dst.read_bytes() == _DATA
        assert (result.bytes_written, result.logical_size, result.holes, result.zeros) == (16, 16, 0, 0)


class TestExportSubRange:
    async def test_a_sub_range_is_written_at_offsets_relative_to_its_start(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        sink = _CollectingSink()
        progress: list[int] = []

        async def _progress(written: int) -> None:
            progress.append(written)

        result = await source.export_range(sink, 4, 12, progress=_progress)  # type: ignore[arg-type]

        assert sink.writes == [(0, _DATA[4:12])]
        assert progress == [8]
        assert (result.bytes_written, result.logical_size) == (8, 8)

    async def test_a_range_outside_the_file_is_rejected(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        with pytest.raises(ValueError, match="is not inside"):
            await source.export_range(_CollectingSink(), 8, 17)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="is not inside"):
            await source.planned_bytes(8, 17)

    async def test_planned_bytes_is_the_range_length(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        assert await source.planned_bytes(4, 12) == 8


@faithful_to(ExportWriter)
class _CollectingSink:
    """Records each ``write_at`` call, the one ``ExportWriter`` method
    ``export_range`` uses."""

    def __init__(self) -> None:
        self.writes: list[tuple[int, bytes]] = []

    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self.writes.append((offset, bytes(data)))


class TestExportRange:
    async def test_streams_the_file_into_the_sink_and_reports_progress(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", None)
        sink = _CollectingSink()
        progress: list[int] = []

        async def _progress(written: int) -> None:
            progress.append(written)

        result = await source.export_range(sink, 0, 16, progress=_progress)  # type: ignore[arg-type]

        assert sink.writes == [(0, _DATA)]
        assert progress == [16]
        assert (result.bytes_written, result.logical_size, result.holes, result.zeros) == (16, 16, 0, 0)


class TestSegmentedExport:
    async def test_the_file_comes_out_whole_across_segments(self, tmp_path: Path) -> None:
        (tmp_path / "meta").mkdir()
        data = bytes(range(256)) * 40
        (tmp_path / "meta" / "big.bin").write_bytes(data)
        source = await LocalFileContentSource.create(LocalFsStore(tmp_path), "meta/big.bin", None)

        sink = SegmentCollector(4096)
        result = await run_export(source, sink)

        assert bytes(sink.output) == data
        assert result.bytes_written == len(data)


class TestShortStoreFile:
    async def test_export_range_raises_when_the_file_is_shorter_than_size(self, store: LocalFsStore) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", len(_DATA) + 8)
        with pytest.raises(FormatError, match=r"holds 16 bytes, expected 24"):
            await source.export_range(_CollectingSink(), 0, 24)  # type: ignore[arg-type]

    async def test_export_to_raises_instead_of_reporting_a_zero_padded_success(
        self, store: LocalFsStore, tmp_path: Path
    ) -> None:
        source = await LocalFileContentSource.create(store, "meta/disk.vmdk", len(_DATA) + 8)
        dst = tmp_path / "out.bin"
        with pytest.raises(FormatError, match="store file holds"):
            await run_export(source, LocalFileSink(dst, staged=False))
        assert dst.read_bytes() == _DATA + bytes(8)  # the unstaged sink keeps its pre-sized partial


class TestLocalFileContentSource:
    """Reached through ``DeviceProvider``, as a VM device's non-dedup
    ``.delta`` sidecar under ``copy_meta_file``."""

    async def test_stream_reassembles_to_the_same_bytes_as_read(self, tmp_path: Path) -> None:
        build_vm_repo(
            tmp_path,
            extra_objects=[(2, 0, "ActiveBackup_2026-01-01/my-vm/disk.img.delta", "", "", 0, 64)],
        )
        delta_dir = tmp_path / "copy_meta_file" / "VM_uid1" / "ActiveBackup_2026-01-01" / "my-vm"
        delta_dir.mkdir(parents=True)
        delta_bytes = b"CbTT" + os.urandom(60)
        (delta_dir / "disk.img.delta").write_bytes(delta_bytes)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            delta = next(o for o in await provider.children(device) if o.kind is UnitKind.FILE)
            content = (await provider.unit(delta)).content
            reassembled = b"".join([chunk async for _offset, chunk in content.stream(block=10)])
            assert reassembled == delta_bytes

    async def test_export_to_writes_the_full_content(self, tmp_path: Path) -> None:
        build_vm_repo(
            tmp_path,
            extra_objects=[(2, 0, "ActiveBackup_2026-01-01/my-vm/disk.img.delta", "", "", 0, 64)],
        )
        delta_dir = tmp_path / "copy_meta_file" / "VM_uid1" / "ActiveBackup_2026-01-01" / "my-vm"
        delta_dir.mkdir(parents=True)
        delta_bytes = b"CbTT" + os.urandom(60)
        (delta_dir / "disk.img.delta").write_bytes(delta_bytes)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            delta = next(o for o in await provider.children(device) if o.kind is UnitKind.FILE)
            content = (await provider.unit(delta)).content
            dst = tmp_path / "out.bin"
            await run_export(content, LocalFileSink(dst, staged=False))
            assert dst.read_bytes() == delta_bytes
