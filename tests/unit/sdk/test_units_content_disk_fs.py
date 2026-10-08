"""Unit tests for ``synology_apm_repo.sdk.units.content.disk_fs``'s FAT,
ext2/3/4, XFS, Btrfs, and NTFS backends (``dissect.*``), all on offline,
committed or hand-built fixtures. The APFS backend (``_apfs.py``) is in
``test_units_content_disk_fs_apfs.py``;
``tests/integration/sdk/test_units_content_disk_fs.py`` covers real-disk listings.

Fixtures:

- FAT12: hand-built byte-exact in pure Python.
- ``tests/fixtures/tiny_ext4.raw.tar.gz``: 8MiB ext4 from ``mke2fs -t ext4``
  (journal disabled), holding ``hello.txt``/``subdir/nested.txt``.
- ``tiny_xfs.raw.tar.gz``: 320MiB XFS from ``mkfs.xfs`` (its ~300MiB minimum),
  built in a throwaway Debian Docker container, same payload.
- ``tiny_btrfs.raw.tar.gz``: 115MiB flat Btrfs from ``mkfs.btrfs -r <dir>``
  (its ~109MiB minimum), same payload.
- ``tiny_btrfs_subvols.raw.tar.gz``: 115MiB Btrfs with real ``root``/``home``
  subvolumes, created by ``btrfs subvolume create`` on a loop mount inside a
  ``--privileged`` container; proves listing and reading through subvolume
  roots, which ``listdir()`` crosses as ordinary directories.
- ``tiny_ntfs.raw.tar.gz``: 16MiB NTFS from ``mkntfs``, populated through an
  ``ntfs-3g`` loop mount; its root carries a literal ``"."`` entry that
  ``_ntfs_iterdir`` filters.

The ``.raw.tar.gz`` fixtures are single-member ``tar --sparse`` archives:
each image is mostly unallocated, and ``tarfile`` restores the holes as real
holes, so ``disk_fs_fakes.extracted_image`` never materializes the full logical size.
"""

from __future__ import annotations

import asyncio
import importlib.util
import struct
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, cast

import pytest

from support.fakes import faithful_to, unchecked_fake
from synology_apm_repo.sdk.dedup.dedup_file import DEFAULT_STREAM_BLOCK
from synology_apm_repo.sdk.errors import ContentUnavailableError, DataCorruptError
from synology_apm_repo.sdk.export import ExportWriter, LocalFileSink, run_export
from synology_apm_repo.sdk.units.base import FileState
from synology_apm_repo.sdk.units.content.disk_fs import (
    DiskFilesystem,
    DissectFileContentSource,
    disk_fs_available,
)
from synology_apm_repo.sdk.units.content.disk_fs._base import (
    _CLOUD_ONLY_REASON,
    _ENCRYPTED_REASON,
    _default_size,
    _DirEntry,
    _Format,
    _try_import,
)
from synology_apm_repo.sdk.units.content.disk_fs._disk_filesystem import _DissectEntry, _partition_table_label
from synology_apm_repo.sdk.units.content.disk_fs._ntfs import (
    _is_cloud_file,
    _ntfs_content_unavailable,
    _ntfs_entry_mtime,
    _ntfs_is_encrypted,
    _ntfs_is_encrypted_attr,
    _ntfs_iterdir,
    _ntfs_size,
)
from synology_apm_repo.sdk.units.content.disk_fs._posix_formats import (
    _btrfs_iterdir,
    _btrfs_volume_label,
    _extfs_volume_label,
    _fat_entry_mtime,
    _fat_volume_label,
    _xfs_volume_label,
)
from unit.sdk.disk_fs_fakes import FileBackedContent, MemoryContent, extracted_image

_SECTOR = 512
_RESERVED_SECTORS = 1
_NUM_FATS = 2
_ROOT_ENTRIES = 16
_SECTORS_PER_FAT = 1
_TOTAL_SECTORS = 2880  # ~1.44 MiB — plenty of room in a 12-bit FAT
_FIRST_DATA_SECTOR = _RESERVED_SECTORS + _NUM_FATS * _SECTORS_PER_FAT + (_ROOT_ENTRIES * 32 + _SECTOR - 1) // _SECTOR

_FAT12_FILE_CONTENT = b"hello from a hand-built FAT12 image\n"
_EXT4_HELLO_CONTENT = b"hello from a real ext4 test fixture\n"
_EXT4_NESTED_CONTENT = b"nested file\n"
_XFS_HELLO_CONTENT = b"hello from a real xfs test fixture\n"
_XFS_NESTED_CONTENT = b"nested file\n"
_BTRFS_HELLO_CONTENT = b"hello from a real btrfs test fixture\n"
_BTRFS_NESTED_CONTENT = b"nested file\n"
_BTRFS_SUBVOL_ROOT_HELLO_CONTENT = b"hello from the real root subvolume\n"
_BTRFS_SUBVOL_HOME_HELLO_CONTENT = b"hello from the real home subvolume\n"
_BTRFS_SUBVOL_NESTED_CONTENT = b"nested file\n"
_NTFS_HELLO_CONTENT = b"hello from a real ntfs test fixture\n"
_NTFS_NESTED_CONTENT = b"nested file\n"


def _set_fat12_entry(fat: bytearray, index: int, value: int) -> None:
    offset = index + index // 2
    if index % 2 == 0:
        fat[offset] = value & 0xFF
        fat[offset + 1] = (fat[offset + 1] & 0xF0) | ((value >> 8) & 0x0F)
    else:
        fat[offset] = (fat[offset] & 0x0F) | ((value & 0x0F) << 4)
        fat[offset + 1] = (value >> 4) & 0xFF


def _dir_entry(name: str, ext: str, cluster: int, size: int, attr: int) -> bytes:
    e = bytearray(32)
    e[0:8] = name.ljust(8).encode("ascii")[:8]
    e[8:11] = ext.ljust(3).encode("ascii")[:3]
    e[11] = attr  # 0x20 = archive (file), 0x10 = directory
    struct.pack_into("<H", e, 26, cluster)
    struct.pack_into("<I", e, 28, size)
    return bytes(e)


def _build_fat12_image() -> bytes:
    """A valid FAT12 floppy image with ``HELLO.TXT`` and an empty ``SUBDIR`` in the root."""
    boot = bytearray(_SECTOR)
    boot[0:3] = b"\xeb\x3c\x90"
    boot[3:11] = b"MSDOS5.0"
    struct.pack_into("<H", boot, 11, _SECTOR)
    boot[13] = 1  # sectors per cluster
    struct.pack_into("<H", boot, 14, _RESERVED_SECTORS)
    boot[16] = _NUM_FATS
    struct.pack_into("<H", boot, 17, _ROOT_ENTRIES)
    struct.pack_into("<H", boot, 19, _TOTAL_SECTORS)
    boot[21] = 0xF8  # media descriptor
    struct.pack_into("<H", boot, 22, _SECTORS_PER_FAT)
    struct.pack_into("<H", boot, 24, 18)
    struct.pack_into("<H", boot, 26, 2)
    boot[38] = 0x29  # extended boot signature
    struct.pack_into("<I", boot, 39, 0x12345678)
    boot[43:54] = b"TESTVOL    "[:11]
    boot[54:62] = b"FAT12   "
    boot[510] = 0x55
    boot[511] = 0xAA

    fat = bytearray(_SECTORS_PER_FAT * _SECTOR)
    fat[0] = 0xF8
    fat[1] = 0xFF
    fat[2] = 0xFF
    _set_fat12_entry(fat, 2, 0xFFF)  # cluster 2 (HELLO.TXT), EOF
    _set_fat12_entry(fat, 3, 0xFFF)  # cluster 3 (SUBDIR), EOF

    root = bytearray(_ROOT_ENTRIES * 32)
    root[0:32] = _dir_entry("HELLO", "TXT", 2, len(_FAT12_FILE_CONTENT), 0x20)
    root[32:64] = _dir_entry("SUBDIR", "", 3, 0, 0x10)

    image = bytearray(_TOTAL_SECTORS * _SECTOR)
    image[0:_SECTOR] = boot
    image[_RESERVED_SECTORS * _SECTOR : (_RESERVED_SECTORS + _SECTORS_PER_FAT) * _SECTOR] = fat
    image[(_RESERVED_SECTORS + _SECTORS_PER_FAT) * _SECTOR : (_RESERVED_SECTORS + 2 * _SECTORS_PER_FAT) * _SECTOR] = fat
    root_off = (_RESERVED_SECTORS + _NUM_FATS * _SECTORS_PER_FAT) * _SECTOR
    image[root_off : root_off + _ROOT_ENTRIES * 32] = root

    cluster2_off = _FIRST_DATA_SECTOR * _SECTOR
    image[cluster2_off : cluster2_off + len(_FAT12_FILE_CONTENT)] = _FAT12_FILE_CONTENT

    cluster3_off = (_FIRST_DATA_SECTOR + 1) * _SECTOR
    sub_entries = bytearray(_SECTOR)
    sub_entries[0:32] = _dir_entry(".", "", 3, 0, 0x10)
    sub_entries[32:64] = _dir_entry("..", "", 0, 0, 0x10)
    image[cluster3_off : cluster3_off + _SECTOR] = bytes(sub_entries)

    return bytes(image)


# -- FAT12 -------------------------------------------------------------


async def test_fat12_recognized_as_a_bare_unpartitioned_filesystem() -> None:
    disk_fs = await DiskFilesystem.open(MemoryContent(_build_fat12_image()))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert "FAT" in label


async def _open_fat12() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(MemoryContent(_build_fat12_image()))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_fat12_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_fat12()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    assert names == {"HELLO.TXT", "SUBDIR"}

    hello = next(e for e in entries if e.name == "HELLO.TXT")
    assert hello.is_dir is False
    assert hello.size == len(_FAT12_FILE_CONTENT)
    # DIR_WrtDate/DIR_WrtTime are zero; _fat_entry_mtime reinterprets
    # dostimestamp(0), FAT's epoch sentinel, as UTC.
    assert hello.mtime == datetime(1980, 1, 1, tzinfo=UTC)

    subdir = next(e for e in entries if e.name == "SUBDIR")
    assert subdir.is_dir is True
    assert subdir.mtime == datetime(1980, 1, 1, tzinfo=UTC)


async def test_fat12_open_file_reads_the_real_file_content() -> None:
    disk_fs, addr = await _open_fat12()
    content = await disk_fs.open_file(addr, "/HELLO.TXT")
    assert content.size == len(_FAT12_FILE_CONTENT)
    assert await content.read(0, content.size) == _FAT12_FILE_CONTENT
    assert await content.read(6, 4) == b"from"


async def test_fat12_over_length_read_clamps_instead_of_raising() -> None:
    disk_fs, addr = await _open_fat12()
    content = await disk_fs.open_file(addr, "/HELLO.TXT")
    assert content.size is not None
    result = await content.read(0, content.size + 100)
    assert result == _FAT12_FILE_CONTENT


async def test_fat12_read_starting_at_or_past_end_returns_empty() -> None:
    disk_fs, addr = await _open_fat12()
    content = await disk_fs.open_file(addr, "/HELLO.TXT")
    assert content.size is not None
    assert await content.read(content.size, 10) == b""
    assert await content.read(content.size + 100, 10) == b""


async def test_fat12_negative_offset_or_length_raises() -> None:
    disk_fs, addr = await _open_fat12()
    content = await disk_fs.open_file(addr, "/HELLO.TXT")
    with pytest.raises(ValueError, match="non-negative"):
        await content.read(-1)
    with pytest.raises(ValueError, match="non-negative"):
        await content.read(0, -1)


async def test_fat12_empty_subdir_lists_no_real_entries() -> None:
    disk_fs, addr = await _open_fat12()
    entries = await disk_fs.list_dir(addr, "/SUBDIR")
    assert entries == []


async def test_fat12_export_to_writes_the_real_file_content(tmp_path: Path) -> None:
    disk_fs, addr = await _open_fat12()
    content = await disk_fs.open_file(addr, "/HELLO.TXT")
    dst = tmp_path / "exported_hello.txt"
    result = await run_export(content, LocalFileSink(dst, staged=False))
    assert result.bytes_written == len(_FAT12_FILE_CONTENT)
    assert dst.read_bytes() == _FAT12_FILE_CONTENT


# -- ext2/3/4 ------------------------------------------------------------


async def test_ext4_recognized_as_a_bare_unpartitioned_filesystem() -> None:
    disk_fs = await DiskFilesystem.open(FileBackedContent(*extracted_image("tiny_ext4.raw.tar.gz")))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    # The volume label ("TinyExt4Test") takes precedence over ``last_mount``.
    assert label == "TinyExt4Test (ext2/3/4)"


def test_extfs_volume_label_falls_back_to_last_mount_when_unset() -> None:
    """With no ``volume_name``, the label falls back to ``last_mount`` (the
    superblock's ``last_mounted`` field, populated on every real mount)."""

    class _FakeVolume:
        volume_name = ""
        last_mount = "/boot"

    assert _extfs_volume_label(_FakeVolume()) == "/boot"


def test_extfs_volume_label_prefers_a_real_volume_name_over_last_mount() -> None:
    class _FakeVolume:
        volume_name = "MyLabel"
        last_mount = "/boot"

    assert _extfs_volume_label(_FakeVolume()) == "MyLabel"


def test_extfs_volume_label_is_none_when_neither_is_set() -> None:
    class _FakeVolume:
        volume_name = ""
        last_mount = ""

    assert _extfs_volume_label(_FakeVolume()) is None


def test_xfs_volume_label_reads_the_real_name_attribute() -> None:
    class _FakeVolume:
        name = "MyXfsLabel"

    assert _xfs_volume_label(_FakeVolume()) == "MyXfsLabel"


def test_xfs_volume_label_is_none_when_unset() -> None:
    class _FakeVolume:
        name = ""

    assert _xfs_volume_label(_FakeVolume()) is None


def test_btrfs_volume_label_reads_the_real_label_attribute() -> None:
    class _FakeVolume:
        label = "MyBtrfsLabel"

    assert _btrfs_volume_label(_FakeVolume()) == "MyBtrfsLabel"


def test_btrfs_volume_label_is_none_when_unset() -> None:
    class _FakeVolume:
        label = ""

    assert _btrfs_volume_label(_FakeVolume()) is None


async def _open_ext4() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(FileBackedContent(*extracted_image("tiny_ext4.raw.tar.gz")))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_ext4_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_ext4()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    # "lost+found" is a real ext-family reserved directory, kept, not filtered.
    assert {"hello.txt", "subdir", "lost+found"} <= names

    hello = next(e for e in entries if e.name == "hello.txt")
    assert hello.is_dir is False
    assert hello.size == len(_EXT4_HELLO_CONTENT)
    # The build-time mtime isn't known in advance; only its presence is checked.
    assert isinstance(hello.mtime, datetime)
    assert hello.mtime.tzinfo is not None

    subdir = next(e for e in entries if e.name == "subdir")
    assert subdir.is_dir is True
    assert isinstance(subdir.mtime, datetime)


async def test_ext4_open_file_reads_the_real_file_content() -> None:
    disk_fs, addr = await _open_ext4()
    content = await disk_fs.open_file(addr, "/hello.txt")
    assert content.size == len(_EXT4_HELLO_CONTENT)
    assert await content.read(0, content.size) == _EXT4_HELLO_CONTENT
    assert await content.read(6, 4) == b"from"


async def test_ext4_subdirectory_listing_and_read_of_a_nested_file() -> None:
    disk_fs, addr = await _open_ext4()
    entries = await disk_fs.list_dir(addr, "/subdir")
    assert {e.name for e in entries} == {"nested.txt"}

    content = await disk_fs.open_file(addr, "/subdir/nested.txt")
    assert await content.read(0, content.size) == _EXT4_NESTED_CONTENT


async def test_ext4_content_source_stream_reassembles_to_the_same_bytes() -> None:
    disk_fs, addr = await _open_ext4()
    content = await disk_fs.open_file(addr, "/hello.txt")
    chunks = [chunk async for _pos, chunk in content.stream(block=4)]
    assert b"".join(chunks) == _EXT4_HELLO_CONTENT


async def test_ext4_export_to_invokes_the_progress_callback(tmp_path: Path) -> None:
    disk_fs, addr = await _open_ext4()
    content = await disk_fs.open_file(addr, "/hello.txt")

    calls: list[tuple[int, int]] = []

    async def _progress(written: int, total: int) -> None:
        calls.append((written, total))

    dst = tmp_path / "exported_hello.txt"
    result = await run_export(content, LocalFileSink(dst, staged=False), progress=_progress)
    assert result.bytes_written == len(_EXT4_HELLO_CONTENT)
    assert calls == [(len(_EXT4_HELLO_CONTENT), len(_EXT4_HELLO_CONTENT))]


# -- XFS ------------------------------------------------------------------


async def test_xfs_recognized_as_a_bare_unpartitioned_filesystem() -> None:
    disk_fs = await DiskFilesystem.open(FileBackedContent(*extracted_image("tiny_xfs.raw.tar.gz")))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert "XFS" in label


async def _open_xfs() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(FileBackedContent(*extracted_image("tiny_xfs.raw.tar.gz")))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_xfs_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_xfs()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    assert {"hello.txt", "subdir"} <= names

    hello = next(e for e in entries if e.name == "hello.txt")
    assert hello.is_dir is False
    assert hello.size == len(_XFS_HELLO_CONTENT)
    assert isinstance(hello.mtime, datetime)
    assert hello.mtime.tzinfo is not None

    subdir = next(e for e in entries if e.name == "subdir")
    assert subdir.is_dir is True
    assert isinstance(subdir.mtime, datetime)


async def test_xfs_open_file_reads_the_real_file_content() -> None:
    disk_fs, addr = await _open_xfs()
    content = await disk_fs.open_file(addr, "/hello.txt")
    assert content.size == len(_XFS_HELLO_CONTENT)
    assert await content.read(0, content.size) == _XFS_HELLO_CONTENT
    assert await content.read(6, 4) == b"from"


async def test_xfs_subdirectory_listing_and_read_of_a_nested_file() -> None:
    disk_fs, addr = await _open_xfs()
    entries = await disk_fs.list_dir(addr, "/subdir")
    assert {e.name for e in entries} == {"nested.txt"}

    content = await disk_fs.open_file(addr, "/subdir/nested.txt")
    assert await content.read(0, content.size) == _XFS_NESTED_CONTENT


async def test_xfs_export_to_writes_the_real_file_content(tmp_path: Path) -> None:
    disk_fs, addr = await _open_xfs()
    content = await disk_fs.open_file(addr, "/hello.txt")
    dst = tmp_path / "exported_hello.txt"
    result = await run_export(content, LocalFileSink(dst, staged=False))
    assert result.bytes_written == len(_XFS_HELLO_CONTENT)
    assert dst.read_bytes() == _XFS_HELLO_CONTENT


# -- Btrfs ------------------------------------------------------------------


async def test_btrfs_recognized_as_a_bare_unpartitioned_filesystem() -> None:
    disk_fs = await DiskFilesystem.open(FileBackedContent(*extracted_image("tiny_btrfs.raw.tar.gz")))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert "Btrfs" in label


async def _open_btrfs() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(FileBackedContent(*extracted_image("tiny_btrfs.raw.tar.gz")))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_btrfs_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_btrfs()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    assert {"hello.txt", "subdir"} <= names

    hello = next(e for e in entries if e.name == "hello.txt")
    assert hello.is_dir is False
    assert hello.size == len(_BTRFS_HELLO_CONTENT)
    assert isinstance(hello.mtime, datetime)
    assert hello.mtime.tzinfo is not None

    subdir = next(e for e in entries if e.name == "subdir")
    assert subdir.is_dir is True
    assert isinstance(subdir.mtime, datetime)


async def test_btrfs_open_file_reads_the_real_file_content() -> None:
    disk_fs, addr = await _open_btrfs()
    content = await disk_fs.open_file(addr, "/hello.txt")
    assert content.size == len(_BTRFS_HELLO_CONTENT)
    assert await content.read(0, content.size) == _BTRFS_HELLO_CONTENT
    assert await content.read(6, 4) == b"from"


async def test_btrfs_subdirectory_listing_and_read_of_a_nested_file() -> None:
    disk_fs, addr = await _open_btrfs()
    entries = await disk_fs.list_dir(addr, "/subdir")
    assert {e.name for e in entries} == {"nested.txt"}

    content = await disk_fs.open_file(addr, "/subdir/nested.txt")
    assert await content.read(0, content.size) == _BTRFS_NESTED_CONTENT


def test_btrfs_dot_dot_can_be_a_bare_subvolume_object_not_a_real_inode() -> None:
    """Pins the ``dissect.btrfs`` quirk ``_btrfs_iterdir`` guards against: an ``INode``
    from ``Subvolume.get(path)`` has the ``Subvolume`` as ``.parent``, so ``listdir()[".."]``
    has no ``.is_dir()``/``.size``."""
    import dissect.btrfs.btrfs as btrfs_mod

    path, _size = extracted_image("tiny_btrfs.raw.tar.gz")
    with path.open("rb") as fh:
        volume = btrfs_mod.Btrfs(fh)
        parent = volume.get("/subdir").listdir()[".."]
        assert isinstance(parent, btrfs_mod.Subvolume)
        assert not hasattr(parent, "is_dir")
        assert not hasattr(parent, "size")


def test_btrfs_iterdir_skips_a_child_missing_is_dir_even_under_a_non_dot_name() -> None:
    @unchecked_fake("a dissect filesystem object")
    class _FakeChild:
        def is_dir(self) -> bool:
            return False

        size = 3

    class _NotARealInode:
        pass  # deliberately no is_dir()/size, like a bare Subvolume

    @unchecked_fake("a dissect filesystem object")
    class _FakeEntry:
        def listdir(self) -> dict[str, object]:
            return {"real_file.txt": _FakeChild(), "weird": _NotARealInode()}

    assert _btrfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="real_file.txt", is_dir=False, size=3, file_state=FileState.NORMAL, mtime=None)
    ]


def test_dissect_entry_list_dir_sorts_directories_before_files() -> None:
    unsorted = [
        _DirEntry(name="aaa.txt", is_dir=False, size=10, file_state=FileState.NORMAL, mtime=None),
        _DirEntry(name="zzz_dir", is_dir=True, size=None, file_state=FileState.NORMAL, mtime=None),
        _DirEntry(name="mid.txt", is_dir=False, size=5, file_state=FileState.NORMAL, mtime=None),
    ]
    fmt = _Format(
        label="fake",
        open=lambda fh: object(),
        resolve=lambda volume, path: None,
        iterdir=lambda entry: unsorted,
        size=_default_size,
        volume_label=lambda volume: None,
        content_unavailable=lambda entry: None,
    )
    entry = _DissectEntry(fmt, object())
    assert entry.list_dir("/") == [
        _DirEntry(name="zzz_dir", is_dir=True, size=None, file_state=FileState.NORMAL, mtime=None),
        _DirEntry(name="aaa.txt", is_dir=False, size=10, file_state=FileState.NORMAL, mtime=None),
        _DirEntry(name="mid.txt", is_dir=False, size=5, file_state=FileState.NORMAL, mtime=None),
    ]


async def test_btrfs_export_to_writes_the_real_file_content(tmp_path: Path) -> None:
    disk_fs, addr = await _open_btrfs()
    content = await disk_fs.open_file(addr, "/hello.txt")
    dst = tmp_path / "exported_hello.txt"
    result = await run_export(content, LocalFileSink(dst, staged=False))
    assert result.bytes_written == len(_BTRFS_HELLO_CONTENT)
    assert dst.read_bytes() == _BTRFS_HELLO_CONTENT


async def _open_btrfs_subvols() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(FileBackedContent(*extracted_image("tiny_btrfs_subvols.raw.tar.gz")))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_btrfs_top_level_lists_both_real_subvolumes() -> None:
    disk_fs, addr = await _open_btrfs_subvols()
    entries = await disk_fs.list_dir(addr, "/")
    assert {e.name for e in entries} == {"root", "home"}
    assert all(e.is_dir for e in entries)


async def test_btrfs_each_subvolume_lists_its_own_distinct_content() -> None:
    disk_fs, addr = await _open_btrfs_subvols()
    root_entries = await disk_fs.list_dir(addr, "/root")
    assert {e.name for e in root_entries} == {"hello.txt", "subdir"}
    home_entries = await disk_fs.list_dir(addr, "/home")
    assert {e.name for e in home_entries} == {"hello.txt"}


async def test_btrfs_reads_real_content_across_two_different_subvolumes() -> None:
    disk_fs, addr = await _open_btrfs_subvols()
    root_content = await disk_fs.open_file(addr, "/root/hello.txt")
    assert await root_content.read(0, root_content.size) == _BTRFS_SUBVOL_ROOT_HELLO_CONTENT
    home_content = await disk_fs.open_file(addr, "/home/hello.txt")
    assert await home_content.read(0, home_content.size) == _BTRFS_SUBVOL_HOME_HELLO_CONTENT
    nested_content = await disk_fs.open_file(addr, "/root/subdir/nested.txt")
    assert await nested_content.read(0, nested_content.size) == _BTRFS_SUBVOL_NESTED_CONTENT


# -- NTFS -------------------------------------------------------------------


async def _open_ntfs() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(FileBackedContent(*extracted_image("tiny_ntfs.raw.tar.gz")))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_ntfs_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_ntfs()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    # $MFT/$LogFile/etc. are real NTFS system metadata files, kept like
    # ext4's "lost+found"; "." is the self-referential entry _ntfs_iterdir
    # filters.
    assert {"hello.txt", "subdir", "$MFT"} <= names
    assert "." not in names

    hello = next(e for e in entries if e.name == "hello.txt")
    assert hello.is_dir is False
    assert hello.size == len(_NTFS_HELLO_CONTENT)
    assert isinstance(hello.mtime, datetime)
    assert hello.mtime.tzinfo is not None

    subdir = next(e for e in entries if e.name == "subdir")
    assert subdir.is_dir is True
    assert isinstance(subdir.mtime, datetime)


async def test_ntfs_open_file_reads_the_real_file_content() -> None:
    disk_fs, addr = await _open_ntfs()
    content = await disk_fs.open_file(addr, "/hello.txt")
    assert content.size == len(_NTFS_HELLO_CONTENT)
    assert await content.read(0, content.size) == _NTFS_HELLO_CONTENT
    assert await content.read(6, 4) == b"from"


async def test_ntfs_subdirectory_listing_and_read_of_a_nested_file() -> None:
    disk_fs, addr = await _open_ntfs()
    entries = await disk_fs.list_dir(addr, "/subdir")
    assert {e.name for e in entries} == {"nested.txt"}

    content = await disk_fs.open_file(addr, "/subdir/nested.txt")
    assert await content.read(0, content.size) == _NTFS_NESTED_CONTENT


async def test_ntfs_content_source_stream_reassembles_to_the_same_bytes() -> None:
    disk_fs, addr = await _open_ntfs()
    content = await disk_fs.open_file(addr, "/hello.txt")
    chunks = [chunk async for _pos, chunk in content.stream(block=4)]
    assert b"".join(chunks) == _NTFS_HELLO_CONTENT


async def test_ntfs_export_to_writes_the_real_file_content(tmp_path: Path) -> None:
    disk_fs, addr = await _open_ntfs()
    content = await disk_fs.open_file(addr, "/hello.txt")
    dst = tmp_path / "exported_hello.txt"
    result = await run_export(content, LocalFileSink(dst, staged=False))
    assert result.bytes_written == len(_NTFS_HELLO_CONTENT)
    assert dst.read_bytes() == _NTFS_HELLO_CONTENT


# -- shared / cross-cutting ------------------------------------------------


def test_disk_fs_available_reflects_real_import_system_state() -> None:
    # The whole dissect.* stack is a required dependency, so a normal
    # environment has every backend present. test_units_content_disk_fs_apfs.py
    # covers the "only dissect.apfs present" case via monkeypatch.
    assert disk_fs_available() is True


def test_disk_fs_available_false_when_every_backend_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a, **kw: None)
    assert disk_fs_available() is False


async def test_open_returns_none_when_nothing_on_the_disk_is_recognized() -> None:
    disk_fs = await DiskFilesystem.open(MemoryContent(b"\x00" * (2 * 1024 * 1024)))
    assert disk_fs is None


def test_try_import_returns_none_for_a_module_that_genuinely_does_not_exist() -> None:
    assert _try_import("this.module.does.not.exist.anywhere") is None


def test_default_size_falls_back_to_none_when_reading_size_raises() -> None:
    @unchecked_fake("a dissect filesystem object")
    class _FakeEntry:
        @property
        def size(self) -> int:
            raise RuntimeError("no size for this kind of entry")

    assert _default_size(_FakeEntry()) is None


def test_ntfs_size_falls_back_to_none_when_size_raises() -> None:
    # A real NTFS system metadata file (e.g. $Secure, MFT record 9) can
    # have no unnamed $DATA stream at all -- MftRecord.size() looks for
    # exactly that stream and raises when it's missing.
    @unchecked_fake("a dissect filesystem object")
    class _FakeEntry:
        def size(self) -> int:
            raise FileNotFoundError("no unnamed $DATA stream")

    assert _ntfs_size(_FakeEntry()) is None


def test_ntfs_iterdir_filters_the_roots_self_referential_dot_entry() -> None:
    @unchecked_fake("a dissect filesystem object")
    class _FakeAttr:
        def __init__(self, name: str, is_dir: bool, size: int | None) -> None:
            self.file_name = name
            self.file_size = size
            self._is_dir = is_dir

        def is_dir(self) -> bool:
            return self._is_dir

    class _FakeChild:
        def __init__(self, attribute: _FakeAttr) -> None:
            self.attribute = attribute

    @unchecked_fake("a dissect filesystem object")
    class _FakeEntry:
        def iterdir(self, *, dereference: bool, ignore_dos: bool) -> list[_FakeChild]:
            return [
                _FakeChild(_FakeAttr(".", True, None)),  # the root's own self-reference
                _FakeChild(_FakeAttr("real_file.txt", False, 3)),
            ]

    assert _ntfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="real_file.txt", is_dir=False, size=3, file_state=FileState.NORMAL, mtime=None)
    ]


def test_is_cloud_file_returns_false_when_the_method_is_absent_or_raises() -> None:
    class _NoMethod:
        pass

    class _Raises:
        def is_cloud_file(self) -> bool:
            raise RuntimeError("dissect.ntfs internal failure")

    class _True:
        def is_cloud_file(self) -> bool:
            return True

    assert _is_cloud_file(_NoMethod()) is False
    assert _is_cloud_file(_Raises()) is False
    assert _is_cloud_file(_True()) is True


def test_ntfs_content_unavailable_returns_a_reason_only_for_a_cloud_file_entry() -> None:
    class _CloudEntry:
        def is_cloud_file(self) -> bool:
            return True

    class _NormalEntry:
        def is_cloud_file(self) -> bool:
            return False

    assert _ntfs_content_unavailable(_CloudEntry()) == _CLOUD_ONLY_REASON
    assert _ntfs_content_unavailable(_NormalEntry()) is None


def test_ntfs_iterdir_flags_a_cloud_file_child_as_cloud_only() -> None:
    """The cloud check reads the ``$FILE_NAME`` attribute ``_ntfs_iterdir`` already reads."""

    class _CloudAttr:
        file_name = "cloud.txt"
        file_size = 5

        def is_dir(self) -> bool:
            return False

        def is_cloud_file(self) -> bool:
            return True

    class _CloudChild:
        attribute = _CloudAttr()

    @unchecked_fake("a dissect filesystem object")
    class _FakeEntry:
        def iterdir(self, *, dereference: bool, ignore_dos: bool) -> list[_CloudChild]:
            return [_CloudChild()]

    assert _ntfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="cloud.txt", is_dir=False, size=5, file_state=FileState.CLOUD_ONLY, mtime=None)
    ]


#: ``FILE_ATTRIBUTE_ENCRYPTED`` (winnt.h), as in dissect.ntfs's
#: ``c_ntfs.FILE_ATTRIBUTE.ENCRYPTED``; duplicated so the fakes below need
#: no dissect.ntfs import.
_FILE_ATTRIBUTE_ENCRYPTED = 0x4000


class _FakeStandardInformation:
    def __init__(self, file_attributes: int) -> None:
        self.file_attributes = file_attributes


@unchecked_fake("a dissect filesystem object")
class _FakeNtfsAttributes:
    """Fakes just enough of ``AttributeMap`` for ``_ntfs_is_encrypted``:
    ``.STANDARD_INFORMATION.file_attributes`` and ``.find(name, type)``
    for a ``$EFS``-named ``$LOGGED_UTILITY_STREAM``."""

    def __init__(self, *, file_attributes: int = 0, has_efs: bool = False) -> None:
        self.STANDARD_INFORMATION = _FakeStandardInformation(file_attributes)
        self._has_efs = has_efs

    def find(self, name: str, attr_type: object) -> list[object]:
        return [object()] if self._has_efs and name == "$EFS" else []


class _FakeMftRecord:
    def __init__(self, *, file_attributes: int = 0, has_efs: bool = False) -> None:
        self.attributes = _FakeNtfsAttributes(file_attributes=file_attributes, has_efs=has_efs)


def test_ntfs_is_encrypted_attr_reads_the_file_names_own_cached_flag() -> None:
    class _EncryptedAttr:
        file_attributes = _FILE_ATTRIBUTE_ENCRYPTED

    class _NormalAttr:
        file_attributes = 0

    class _NoAttr:
        pass

    assert _ntfs_is_encrypted_attr(_EncryptedAttr()) is True
    assert _ntfs_is_encrypted_attr(_NormalAttr()) is False
    assert _ntfs_is_encrypted_attr(_NoAttr()) is False


def test_ntfs_is_encrypted_matches_the_standard_information_flag_or_a_real_efs_attribute() -> None:
    flagged = _FakeMftRecord(file_attributes=_FILE_ATTRIBUTE_ENCRYPTED)
    efs_attribute_only = _FakeMftRecord(file_attributes=0, has_efs=True)
    neither = _FakeMftRecord(file_attributes=0, has_efs=False)

    assert _ntfs_is_encrypted(flagged) is True
    assert _ntfs_is_encrypted(efs_attribute_only) is True
    assert _ntfs_is_encrypted(neither) is False


def test_ntfs_content_unavailable_returns_the_efs_reason_for_an_encrypted_entry() -> None:
    class _EncryptedEntry:
        def is_cloud_file(self) -> bool:
            return False

        attributes = _FakeNtfsAttributes(file_attributes=_FILE_ATTRIBUTE_ENCRYPTED)

    assert _ntfs_content_unavailable(_EncryptedEntry()) == _ENCRYPTED_REASON


def test_ntfs_content_unavailable_prioritizes_cloud_only_over_encrypted() -> None:
    """A file that is both a cloud placeholder and EFS-encrypted is reported as the placeholder."""

    class _CloudAndEncryptedEntry:
        def is_cloud_file(self) -> bool:
            return True

        attributes = _FakeNtfsAttributes(file_attributes=_FILE_ATTRIBUTE_ENCRYPTED)

    assert _ntfs_content_unavailable(_CloudAndEncryptedEntry()) == _CLOUD_ONLY_REASON


def test_ntfs_iterdir_flags_an_encrypted_child_as_encrypted() -> None:
    class _EncryptedFileNameAttr:
        file_name = "secret.docx"
        file_size = 42
        file_attributes = _FILE_ATTRIBUTE_ENCRYPTED

        def is_dir(self) -> bool:
            return False

        def is_cloud_file(self) -> bool:
            return False

    class _EncryptedChild:
        attribute = _EncryptedFileNameAttr()

    @unchecked_fake("a dissect filesystem object")
    class _FakeEntry:
        def iterdir(self, *, dereference: bool, ignore_dos: bool) -> list[_EncryptedChild]:
            return [_EncryptedChild()]

    assert _ntfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="secret.docx", is_dir=False, size=42, file_state=FileState.ENCRYPTED, mtime=None)
    ]


def test_ntfs_iterdir_reads_a_real_last_modification_time_off_the_file_name_attribute() -> None:
    expected = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

    class _TimedAttr:
        file_name = "timed.txt"
        file_size = 7
        last_modification_time = expected

        def is_dir(self) -> bool:
            return False

    class _TimedChild:
        attribute = _TimedAttr()

    @unchecked_fake("a dissect filesystem object")
    class _FakeEntry:
        def iterdir(self, *, dereference: bool, ignore_dos: bool) -> list[_TimedChild]:
            return [_TimedChild()]

    (entry,) = _ntfs_iterdir(_FakeEntry())
    assert entry.mtime == expected


def test_ntfs_entry_mtime_degrades_to_none_when_the_underlying_read_raises() -> None:
    class _RaisingAttr:
        @property
        def last_modification_time(self) -> datetime:
            raise RuntimeError("malformed $FILE_NAME timestamp")

    assert _ntfs_entry_mtime(_RaisingAttr()) is None


def test_open_file_raises_content_unavailable_without_opening_or_sizing_the_entry() -> None:
    """A truthy ``_Format.content_unavailable(entry)`` makes ``_DissectEntry.open_file``
    raise before sizing or opening the entry."""

    class _PlaceholderEntry:
        def open(self) -> object:
            raise AssertionError("must not be called for a content-unavailable entry")

    size_calls: list[object] = []

    def _unreachable_open(fh: object) -> object:
        raise NotImplementedError("unused by this test")

    def _size(entry: object) -> int | None:
        size_calls.append(entry)
        return 123

    fmt = _Format(
        label="fake",
        open=_unreachable_open,
        resolve=lambda volume, path: _PlaceholderEntry(),
        iterdir=lambda entry: [],
        size=_size,
        volume_label=lambda volume: None,
        content_unavailable=lambda entry: "synthetic placeholder reason",
    )
    dissect_entry = _DissectEntry(fmt, volume=object())
    with pytest.raises(ContentUnavailableError, match="synthetic placeholder reason"):
        dissect_entry.open_file("/some/path")
    assert size_calls == []


def test_fat_volume_label_falls_back_to_none_when_reading_it_raises() -> None:
    @unchecked_fake("a dissect filesystem object")
    class _FakeVolume:
        @property
        def volume_label(self) -> str:
            raise RuntimeError("dissect.fat doesn't handle this variant")

    assert _fat_volume_label(_FakeVolume()) is None


def test_fat_entry_mtime_attaches_utc_to_the_naive_dostimestamp_value() -> None:
    class _FakeEntry:
        mtime = datetime(2023, 6, 15, 12, 30, 0)  # naive, as dostimestamp() returns

    result = _fat_entry_mtime(_FakeEntry())
    assert result == datetime(2023, 6, 15, 12, 30, 0, tzinfo=UTC)


def test_fat_entry_mtime_degrades_to_none_when_the_underlying_read_raises() -> None:
    @unchecked_fake("a dissect filesystem object")
    class _FakeEntry:
        @property
        def mtime(self) -> datetime:
            raise RuntimeError("malformed FAT directory-entry timestamp")

    assert _fat_entry_mtime(_FakeEntry()) is None


class TestPartitionTableLabel:
    def test_prefers_a_real_partition_name_when_set(self) -> None:
        class _FakePart:
            name = "EFI System Partition"
            type_name = "Unknown"
            type = 0xEF

        assert _partition_table_label(_FakePart()) == "EFI System Partition"

    def test_falls_back_to_a_recognized_type_name_when_no_name_is_set(self) -> None:
        class _FakePart:
            name = ""
            type_name = "Linux filesystem"
            type = 0x8300

        assert _partition_table_label(_FakePart()) == "Linux filesystem"

    def test_falls_back_to_the_raw_type_value_when_neither_name_nor_type_name_is_useful(self) -> None:
        class _FakePart:
            name = ""
            type_name = "Unknown"
            type = 0x8300

        assert _partition_table_label(_FakePart()) == str(0x8300)

    def test_a_partition_with_no_type_attribute_at_all_still_yields_a_label(self) -> None:
        # A partition with no readable .type must still yield a label
        # rather than raising out of DiskFilesystem.open().
        class _FakePart:
            name = ""
            type_name = "Unknown"

        assert _partition_table_label(_FakePart()) == "Unknown"


def test_try_open_apfs_skips_a_volume_whose_root_is_undecodable() -> None:
    """A volume whose ``volume.get("/")`` raises (sealed system volume,
    FileVault-locked) is skipped alone; a decodable sibling still resolves."""

    class _LockedVolume:
        name = "Locked"

        def get(self, path: str) -> object:
            raise RuntimeError("locked")

    class _OpenVolume:
        name = "Data"

        def get(self, path: str) -> object:
            return object()

    class _FakeApfs:
        def __init__(self, fh: object) -> None:
            self.volumes = [_LockedVolume(), _OpenVolume()]

    class _FakeApfsModule:
        APFS = _FakeApfs

    def fh_factory() -> BinaryIO:
        return cast("BinaryIO", object())

    disk_fs = DiskFilesystem(object(), object())  # type: ignore[arg-type]
    claimed, minted = disk_fs._try_open_apfs(_FakeApfsModule(), fh_factory, "whole image", 0)

    assert claimed is True
    assert minted == 1  # only the decodable volume was actually browsable
    assert 0 in disk_fs._filesystems  # took the first slot -- the locked one never claimed one


@unchecked_fake("a dissect filesystem object")
class _FakeSeekableStream:
    """In-memory stand-in for a Dissect entry's ``.open()`` stream: just
    the ``seek``/``read`` ``DissectFileContentSource._read_blocking`` uses."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    def seek(self, offset: int) -> None:
        self._pos = offset

    def read(self, length: int) -> bytes:
        chunk = self._data[self._pos : self._pos + length]
        self._pos += len(chunk)
        return chunk


@unchecked_fake("a dissect filesystem object")
class _CountingFakeEntry:
    """Counts ``.open()`` calls, returning a fresh ``_FakeSeekableStream``
    over ``data`` each time."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.open_calls = 0

    def open(self) -> _FakeSeekableStream:
        self.open_calls += 1
        return _FakeSeekableStream(self._data)


class TestDissectFileContentSourceExportRange:
    async def test_a_sub_range_is_written_at_offsets_relative_to_its_start(self) -> None:
        content = bytes(range(100))
        source = DissectFileContentSource(_CountingFakeEntry(content), size=len(content))
        writes: list[tuple[int, bytes]] = []

        @faithful_to(ExportWriter)
        class _Writer:
            async def write_at(self, offset: int, data: bytes | memoryview) -> None:
                writes.append((offset, bytes(data)))

        result = await source.export_range(_Writer(), 10, 30)  # type: ignore[arg-type]

        assert writes == [(0, content[10:30])]
        assert (result.bytes_written, result.logical_size) == (20, 20)

    async def test_a_range_outside_the_file_is_rejected(self) -> None:
        source = DissectFileContentSource(_CountingFakeEntry(b"abc"), size=3)
        with pytest.raises(ValueError, match="is not inside"):
            await source.export_range(None, 0, 4)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="is not inside"):
            await source.planned_bytes(0, 4)

    async def test_planned_bytes_is_the_range_length(self) -> None:
        source = DissectFileContentSource(_CountingFakeEntry(b"abcdef"), size=6)
        assert await source.planned_bytes(2, 5) == 3


class TestDissectFileContentSourceHandleReuse:
    """``DissectFileContentSource`` opens the guest file's stream
    (``entry.open()``, which re-parses the file's runlist) at most once per
    instance and reuses it across every block."""

    async def test_two_separate_reads_share_one_open_call(self) -> None:
        content = b"hello world"
        entry = _CountingFakeEntry(content)
        source = DissectFileContentSource(entry, size=len(content))
        assert await source.read(0, 5) == b"hello"
        assert await source.read(6, 5) == b"world"
        assert entry.open_calls == 1

    async def test_streamed_export_across_many_blocks_shares_one_open_call(self, tmp_path: Path) -> None:
        content = b"0123456789" * 100  # 1000 bytes, well past one block at block=64
        entry = _CountingFakeEntry(content)
        source = DissectFileContentSource(entry, size=len(content))
        blocks = [chunk async for _pos, chunk in source.stream(block=64)]
        assert b"".join(blocks) == content
        assert entry.open_calls == 1

        result = await run_export(source, LocalFileSink(tmp_path / "out.bin", staged=False))
        assert (tmp_path / "out.bin").read_bytes() == content
        assert result.bytes_written == len(content)
        assert entry.open_calls == 1  # the export's own full pass reuses the same cached handle too

    async def test_a_failed_read_drops_the_cached_handle_so_the_next_call_gets_a_fresh_open(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stream that raises partway is dropped so the next call opens a fresh one; the
        failure surfaces as ``DataCorruptError``."""
        content = b"hello world"

        class _FailFirstOpenEntry:
            def __init__(self) -> None:
                self.open_calls = 0

            def open(self) -> _FakeSeekableStream:
                self.open_calls += 1
                stream = _FakeSeekableStream(content)
                if self.open_calls == 1:

                    def _fail(length: int) -> bytes:
                        raise OSError("synthetic read failure")

                    monkeypatch.setattr(stream, "read", _fail)
                return stream

        entry = _FailFirstOpenEntry()
        source = DissectFileContentSource(entry, size=len(content))
        with pytest.raises(DataCorruptError, match="synthetic read failure") as exc_info:
            await source.read(0, 5)
        assert isinstance(exc_info.value.__cause__, OSError)
        assert await source.read(0, 5) == b"hello"
        assert entry.open_calls == 2  # the failed handle was dropped; the retry opened a fresh one

    async def test_short_read_reaching_declared_end_raises_data_corrupt(self) -> None:
        # A short read for a request reaching the declared end means the
        # guest file is truncated relative to its metadata.
        entry = _CountingFakeEntry(b"ab")
        source = DissectFileContentSource(entry, size=100)
        with pytest.raises(DataCorruptError, match="declared size=100"):
            await source.read(0, 100)

    async def test_short_read_not_reaching_declared_end_does_not_raise(self) -> None:
        # Same short stream, but the window doesn't reach the declared end.
        entry = _CountingFakeEntry(b"ab")
        source = DissectFileContentSource(entry, size=100)
        assert await source.read(0, 50) == b"ab"

    async def test_export_to_short_read_reaching_declared_end_raises_data_corrupt(self, tmp_path: Path) -> None:
        # export_range replicates read()'s short-read-at-declared-end check
        # independently (it bypasses read()), so it needs its own proof.
        dst = tmp_path / "out.bin"
        entry = _CountingFakeEntry(b"ab")
        source = DissectFileContentSource(entry, size=100)
        with pytest.raises(DataCorruptError, match="declared size=100"):
            await run_export(source, LocalFileSink(dst, staged=False))
        # The only block failed, so no bytes were written and the sink's
        # abort() removed the destination.
        assert not dst.exists()

    async def test_export_to_never_creates_the_destination_file_when_the_first_block_fails(
        self, tmp_path: Path
    ) -> None:
        class _AlwaysFailsToOpenEntry:
            def open(self) -> object:
                raise EOFError("not enough bytes to read struct")

        dst = tmp_path / "nested" / "out.bin"
        source = DissectFileContentSource(_AlwaysFailsToOpenEntry(), size=1000)
        with pytest.raises(DataCorruptError, match="dissect filesystem parser failed reading offset"):
            await run_export(source, LocalFileSink(dst, staged=False))
        assert not dst.exists()

    async def test_export_to_still_creates_an_empty_destination_file_for_a_genuinely_empty_source(
        self, tmp_path: Path
    ) -> None:
        dst = tmp_path / "out.bin"
        entry = _CountingFakeEntry(b"")
        source = DissectFileContentSource(entry, size=0)
        result = await run_export(source, LocalFileSink(dst, staged=False))
        assert dst.exists()
        assert dst.read_bytes() == b""
        assert result.bytes_written == 0

    async def test_export_to_keeps_the_partial_destination_file_when_a_later_block_fails(self, tmp_path: Path) -> None:
        """A later-block failure keeps the file at its full logical size with earlier bytes
        written; the unwritten tail reads as zero."""

        @unchecked_fake("a dissect filesystem object")
        class _FailsOnSecondBlockStream:
            def __init__(self) -> None:
                self._reads = 0

            def seek(self, offset: int) -> None:
                pass

            def read(self, length: int) -> bytes:
                self._reads += 1
                if self._reads == 1:
                    return b"x" * length
                raise OSError("synthetic failure on the second block")

        class _FailsOnSecondBlockEntry:
            def open(self) -> _FailsOnSecondBlockStream:
                return _FailsOnSecondBlockStream()

        dst = tmp_path / "out.bin"
        source = DissectFileContentSource(_FailsOnSecondBlockEntry(), size=DEFAULT_STREAM_BLOCK * 2)
        with pytest.raises(DataCorruptError, match="synthetic failure on the second block"):
            await run_export(source, LocalFileSink(dst, staged=False))
        assert dst.exists()
        assert dst.read_bytes() == b"x" * DEFAULT_STREAM_BLOCK + bytes(DEFAULT_STREAM_BLOCK)

    async def test_export_to_terminates_on_a_short_read_before_the_declared_end(self, tmp_path: Path) -> None:
        """An interior short read ends ``export_range`` in ``DataCorruptError`` at the declared
        end instead of looping forever (``wait_for`` guards)."""

        @unchecked_fake("a dissect filesystem object")
        class _AlwaysShortStream:
            def seek(self, offset: int) -> None:
                pass

            def read(self, length: int) -> bytes:
                return b"x" * min(length, 4)  # short regardless of position requested

        class _AlwaysShortEntry:
            def open(self) -> _AlwaysShortStream:
                return _AlwaysShortStream()

        source = DissectFileContentSource(_AlwaysShortEntry(), size=DEFAULT_STREAM_BLOCK * 2)
        with pytest.raises(DataCorruptError, match=r"read \d+ bytes at offset \d+, expected"):
            await asyncio.wait_for(run_export(source, LocalFileSink(tmp_path / "out.bin", staged=False)), timeout=5.0)

    async def test_concurrent_reads_do_not_race_the_shared_seek_position(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``_fh_lock`` serializes concurrent reads sharing one cached handle:
        the first read parks until the second has reached the lock, or,
        without one, has already moved the shared position."""
        content = b"AAAAABBBBB"  # offset [0,5) is 'A's, [5,10) is 'B's
        second_arrived = threading.Event()

        class _ParkingStream(_FakeSeekableStream):
            def __init__(self, data: bytes) -> None:
                super().__init__(data)
                self._seeks = self._reads = 0

            def seek(self, offset: int) -> None:
                self._seeks += 1
                if self._seeks == 2:
                    second_arrived.set()
                super().seek(offset)

            def read(self, length: int) -> bytes:
                self._reads += 1
                if self._reads == 1:
                    second_arrived.wait(10)
                return super().read(length)

        @unchecked_fake("a dissect filesystem object")
        class _FakeEntry:
            def open(self) -> _ParkingStream:
                return _ParkingStream(content)

        @unchecked_fake("threading.Lock")
        class _ArrivalLock:
            """Sets ``second_arrived`` when a second caller arrives."""

            def __init__(self) -> None:
                self._lock = threading.Lock()
                self._count_lock = threading.Lock()
                self._arrivals = 0

            def __enter__(self) -> None:
                with self._count_lock:
                    self._arrivals += 1
                    if self._arrivals == 2:
                        second_arrived.set()
                self._lock.acquire()

            def __exit__(self, *exc_info: object) -> None:
                self._lock.release()

        source = DissectFileContentSource(_FakeEntry(), size=len(content))
        monkeypatch.setattr(source, "_fh_lock", _ArrivalLock())
        a, b = await asyncio.gather(source.read(0, 5), source.read(5, 5))
        assert a == b"AAAAA"
        assert b == b"BBBBB"
