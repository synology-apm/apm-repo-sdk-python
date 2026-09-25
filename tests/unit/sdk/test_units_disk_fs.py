"""Unit tests for ``synology_apm_repo.sdk.units.content.disk_fs``'s FAT,
ext2/3/4, XFS, Btrfs, and NTFS backends (``dissect.fat``/``dissect.extfs``/
``dissect.xfs``/``dissect.btrfs``/``dissect.ntfs``) — kept together in one
file since all five use offline, committed/hand-built fixtures with no
external sample dependency (see ``tests/unit/sdk/test_units_disk_fs_apfs.py``
for APFS's own fixture, built differently again). This is a real,
mkfs-built NTFS volume, not a fixture the real-sample-only
``tests/integration/sdk/test_units_disk_fs.py`` coverage replaces — that
file separately proves a specific optimization (the ``dereference=False``
listing shape) against a real Windows VM sample at a scale/complexity this
tiny fixture doesn't attempt to reproduce.

FAT12 fixture: hand-built byte-exact in pure Python (no external mkfs
tool needed — macOS's own ``newfs_msdos`` refuses to format a plain file
rather than a real block device).

ext4 fixture: ``tests/fixtures/tiny_ext4.raw.tar.gz`` — a real, minimal,
committed binary fixture: an 8MiB ext4 filesystem
built with the real ``mke2fs -t ext4`` (journal disabled to keep it
small/deterministic), given a known ``hello.txt``/``subdir/nested.txt``
payload. Not hand-assembled like FAT12 — ext4's own on-disk structures
(checksums, extent trees) aren't practical to construct by hand, so
this is a real binary blob instead, the same tradeoff
``tiny_apfs_gpt.raw.gz`` already made for APFS.

XFS fixture: ``tests/fixtures/tiny_xfs.raw.tar.gz`` — a real 320MiB XFS
filesystem (``mkfs.xfs``'s own hard minimum is 300MiB; there is no
smaller valid XFS image) built inside a throwaway Debian Docker
container (macOS has no native XFS tooling at all, unlike ext4's
Homebrew-installable ``e2fsprogs``), given the same
``hello.txt``/``subdir/nested.txt`` payload as ext4's fixture.

Btrfs fixture: ``tests/fixtures/tiny_btrfs.raw.tar.gz`` — a real 115MiB
Btrfs filesystem (``mkfs.btrfs``'s own hard minimum is ~109MiB) built
the same way, via ``mkfs.btrfs -r <payload dir>`` (which builds the
image directly from a directory tree — no mount/loop device needed
inside the container at all). This one is deliberately flat (no nested
subvolumes) — see ``tiny_btrfs_subvols.raw.tar.gz`` below for the
multi-subvolume case.

Btrfs multi-subvolume fixture: ``tests/fixtures/tiny_btrfs_subvols.raw.tar.gz``
— a second, real 115MiB Btrfs filesystem, this time with two real
subvolumes (``btrfs subvolume create root``/``home``, matching real-world
Fedora's own default layout: the root filesystem lives under a ``root``
subvolume, sibling to a ``home`` subvolume). Unlike the flat fixture above,
``mkfs.btrfs -r <dir>`` can't produce pre-existing subvolumes on its own,
so this one needs an actually-mounted filesystem to run ``btrfs subvolume
create`` against — done via a plain ``mount -o loop`` inside the same
``--privileged`` container (the Docker Desktop Linux VM's kernel already
has Btrfs built in — no separate kernel module to load). Subvolume-crossing
is a real, non-trivial thing for
``_btrfs_iterdir`` to get right (its own ``Subvolume``-as-``.parent``
guard, characterized in ``test_btrfs_dot_dot_can_be_a_bare_subvolume_
object_not_a_real_inode`` against the flat fixture above) — this fixture
proves that guard against an actually-crossed real subvolume boundary,
not just a characterization of the underlying library quirk.
``tests/integration/sdk/test_units_disk_fs.py``'s own Btrfs coverage
against a real Fedora 40 disk covers the one concern this fixture alone
can't reach: real ext4 ``/boot`` partition content.

NTFS fixture: ``tests/fixtures/tiny_ntfs.raw.tar.gz`` — a real 16MiB NTFS
volume built with ``ntfs-3g``'s own ``mkntfs`` inside a throwaway Debian
Docker container (macOS has no native NTFS-formatting tool either),
given the same ``hello.txt``/``subdir/nested.txt`` payload. Unlike
ext4/XFS/Btrfs's directory-tree-only ``mkfs -r``/``-d`` build, ``mkntfs``
formats an empty volume that then needs mounting to write real files
into — done via ``mount -t ntfs-3g -o loop`` inside the same
``--privileged`` container (``ntfs-3g``'s FUSE driver, not an in-kernel
NTFS write driver). This real volume also reproduces the on-disk
convention that its root directory really does carry a
literal ``"."`` self-referential entry, which ``_ntfs_iterdir`` filters —
this fixture's own tests below prove that filtering against a real
volume, on top of
``test_ntfs_iterdir_filters_the_roots_self_referential_dot_entry``'s
hand-built-fake proof of the same behavior.

All five of the above are ``.raw.tar.gz``, not plain ``.raw.gz``: a
single-member ``tar --sparse`` archive, gzipped. Every one of these
images is mostly unallocated (their own filesystem's real content is a
small fraction of the image's own logical size — ``mkfs.xfs``/
``mkfs.btrfs``'s own hard size floors, and NTFS's own system metadata
files, are much larger than anything this fixture's own payload needs),
so ``tarfile``'s native GNU-sparse support — reconstructing the archived
holes as real holes on extraction — is what ``_load_tar_gz_fixture``
relies on to materialize any of these to a temp file without that temp
file's own disk usage approaching the image's full logical size."""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import importlib.util
import os
import shutil
import struct
import sys
import tarfile
import tempfile
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import BinaryIO, cast

import pytest

from synology_apm_repo.sdk.dedup.dedup_file import DEFAULT_STREAM_BLOCK
from synology_apm_repo.sdk.errors import ContentUnavailableError, DataCorruptError
from synology_apm_repo.sdk.units.base import FileState
from synology_apm_repo.sdk.units.content.disk_fs import (
    DiskFilesystem,
    DissectFileContentSource,
    _DissectEntry,
    _partition_table_label,
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

_O_BINARY = getattr(os, "O_BINARY", 0)

_PREAD_LOCK = threading.Lock()


def _pread(fd: int, length: int, offset: int) -> bytes:
    """Same shape as ``storage/local.py``'s own helper — ``os.pread()``
    where available (POSIX), ``lseek``+``read`` under a lock on Windows,
    which has no positional read and whose fd here is shared across
    concurrent ``to_thread`` reads."""
    if sys.platform != "win32":
        return os.pread(fd, length, offset)
    with _PREAD_LOCK:
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)


_FIXTURES = Path(__file__).parent.parent.parent / "fixtures"

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


class _FakeAsyncContent:
    """Mimics ``ContentSource``
    (sync ``size``, async ``read``) over an in-memory buffer, with a real
    ``await`` point in ``read()`` proving the async/sync bridge
    (``dissect.util.stream.AlignedStream``'s ``_read`` override in
    ``disk_fs.py``) genuinely round-trips through the event loop rather
    than merely working by accident on an already-resolved coroutine."""

    supports_concurrent_export = False

    def __init__(self, data: bytes) -> None:
        self._data = data

    @property
    def size(self) -> int | None:
        return len(self._data)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        import asyncio

        await asyncio.sleep(0)
        n = length if length is not None else len(self._data) - offset
        return self._data[offset : offset + n]

    def stream(self, block: int = 8 << 20) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")

    async def export_to(
        self, dst: Path, *, sparse: bool = True, progress: Callable[[int, int], Awaitable[None]] | None = None
    ) -> object:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")


class _FileBackedAsyncContent:
    """Same ``ContentSource``
    surface as ``_FakeAsyncContent``, but backed by a real file
    (``os.pread`` off a real fd, via ``asyncio.to_thread`` — the same
    pattern ``storage/local.py``'s ``LocalFsStore`` uses) instead of an
    in-memory buffer, so a large fixture's own bytes live in the OS page
    cache rather than pinned in this process's heap. Pairs with
    ``_load_tar_gz_fixture``, whose whole point is to avoid ever
    holding one of these fixtures as a single in-memory ``bytes``."""

    supports_concurrent_export = False

    def __init__(self, path: Path, size: int) -> None:
        self._fd = os.open(path, os.O_RDONLY | _O_BINARY)
        self._size = size

    def __del__(self) -> None:
        # Best-effort: this fd's own file outlives every test (removed
        # only at interpreter exit, see _load_tar_gz_fixture), so
        # leaving this one fd for GC to reclaim is harmless either way —
        # closing it promptly here just avoids piling up open fds across
        # this file's several tests per filesystem.
        with contextlib.suppress(OSError):
            os.close(self._fd)

    @property
    def size(self) -> int | None:
        return self._size

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        n = length if length is not None else self._size - offset
        return await asyncio.to_thread(_pread, self._fd, n, offset)

    def stream(self, block: int = 8 << 20) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")

    async def export_to(
        self, dst: Path, *, sparse: bool = True, progress: Callable[[int, int], Awaitable[None]] | None = None
    ) -> object:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")


@cache
def _load_tar_gz_fixture(name: str) -> tuple[Path, int]:
    """Extracts ``tests/fixtures/<name>``'s one member to a real temp
    file and returns ``(path, size)`` — the single shared loader for
    every ``.raw.tar.gz`` fixture in this file (ext4/XFS/Btrfs), keyed
    and cached by filename so each one is only ever extracted once per
    worker process.

    ``.tar.gz``, not plain ``.gz``: each of these images is mostly
    unallocated relative to its own logical size (XFS/Btrfs's real
    format tooling forces a large minimum image size -- 300MiB/~109MiB --
    even though the actual payload is a couple of tiny files), and
    ``tarfile``'s ``extract()`` reconstructs a
    ``tar --sparse``-recorded hole as a real hole on the destination
    filesystem (seeking past it rather than writing real zero bytes) —
    the same outcome a hand-rolled zero-detecting write loop would need
    to reimplement, already correct in the standard library. The
    resulting temp file is what ``_FileBackedAsyncContent`` reads,
    so none of these fixtures is ever held as a single in-memory
    ``bytes`` regardless of its own logical size. Removed at interpreter
    exit via ``atexit`` — nothing in a single test run's own lifetime
    needs it cleaned up sooner."""
    tmp_dir = Path(tempfile.mkdtemp())
    atexit.register(shutil.rmtree, tmp_dir, ignore_errors=True)
    with tarfile.open(_FIXTURES / name, "r:gz") as tf:
        (member,) = tf.getmembers()
        tf.extract(member, path=tmp_dir, filter="data")
    return tmp_dir / member.name, member.size


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
    """A minimal, real, valid FAT12 floppy image: one file (``HELLO.TXT``)
    and one subdirectory (``SUBDIR``, itself empty beyond ``.``/``..``)
    in the root directory."""
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
    disk_fs = await DiskFilesystem.open(_FakeAsyncContent(_build_fat12_image()))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert "FAT" in label


async def _open_fat12() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(_FakeAsyncContent(_build_fat12_image()))
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
    # The hand-built image never sets DIR_WrtDate/DIR_WrtTime (left
    # zeroed) -- dostimestamp(0) is FAT's own epoch sentinel, not a
    # missing value, and _fat_entry_mtime reinterprets it as UTC.
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
    # DissectFileContentSource.supports_concurrent_export delegates to
    # _BlockingReadContentSource's own property -- covers both.
    assert content.supports_concurrent_export is False


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
    result = await content.export_to(dst)
    assert result.bytes_written == len(_FAT12_FILE_CONTENT)
    assert dst.read_bytes() == _FAT12_FILE_CONTENT


# -- ext2/3/4 ------------------------------------------------------------


async def test_ext4_recognized_as_a_bare_unpartitioned_filesystem() -> None:
    disk_fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture("tiny_ext4.raw.tar.gz")))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    # The fixture's own real volume label ("TinyExt4Test", set at mkfs
    # time) takes precedence over ``last_mount`` (see
    # ``_extfs_volume_label``'s own fallback-order tests below).
    assert label == "TinyExt4Test (ext2/3/4)"


def test_extfs_volume_label_falls_back_to_last_mount_when_unset() -> None:
    """Pure fallback-chain test, no real fixture/dependency needed --
    real-data confirmation lives in
    tests/integration/sdk/test_units_disk_fs.py's own Fedora /boot coverage
    (a real ext4 partition with an empty volume_name but
    last_mount="/boot"). ``last_mount`` is the kernel's own
    ``last_mounted`` superblock field, updated on every real mount, so it's
    a populated fallback in the common case a label was never set."""

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
    """Pure fallback-chain test, no real fixture involved -- mirrors
    ``_extfs_volume_label``'s own tests above."""

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
    disk_fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture("tiny_ext4.raw.tar.gz")))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_ext4_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_ext4()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    # "lost+found" is a real ext-family reserved directory (kept, not
    # filtered: a real directory a real filesystem creates, the same
    # posture already taken for NTFS's own $MFT/$LogFile in the previous
    # pytsk3-based implementation).
    assert {"hello.txt", "subdir", "lost+found"} <= names

    hello = next(e for e in entries if e.name == "hello.txt")
    assert hello.is_dir is False
    assert hello.size == len(_EXT4_HELLO_CONTENT)
    # A real mtime the real mke2fs build set at fixture-creation time --
    # not guessable in advance, just confirmed present and real.
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
    result = await content.export_to(dst, progress=_progress)
    assert result.bytes_written == len(_EXT4_HELLO_CONTENT)
    assert calls == [(len(_EXT4_HELLO_CONTENT), len(_EXT4_HELLO_CONTENT))]


# -- XFS ------------------------------------------------------------------


async def test_xfs_recognized_as_a_bare_unpartitioned_filesystem() -> None:
    disk_fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture("tiny_xfs.raw.tar.gz")))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert "XFS" in label


async def _open_xfs() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture("tiny_xfs.raw.tar.gz")))
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
    result = await content.export_to(dst)
    assert result.bytes_written == len(_XFS_HELLO_CONTENT)
    assert dst.read_bytes() == _XFS_HELLO_CONTENT


# -- Btrfs ------------------------------------------------------------------


async def test_btrfs_recognized_as_a_bare_unpartitioned_filesystem() -> None:
    disk_fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture("tiny_btrfs.raw.tar.gz")))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert "Btrfs" in label


async def _open_btrfs() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture("tiny_btrfs.raw.tar.gz")))
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
    """Characterization test for a ``dissect.btrfs`` quirk:
    ``Subvolume.get(path)`` -- the resolve every Btrfs listing in
    ``disk_fs.py`` goes through -- sets a resolved ``INode``'s own
    ``.parent`` to the ``Subvolume`` object itself, not a real
    ``INode``, unlike ``INode.iterdir()``'s own internal directory walk
    (which sets it correctly). Reproduced here even on this fixture's
    deliberately flat, single-subvolume layout (``/subdir``'s own
    ``".."``) -- not just a multi-subvolume real sample -- so this needs
    no real multi-subvolume disk to catch a future ``dissect.btrfs``
    upgrade that changes this: ``entry.listdir()[".."]`` bypassed here
    has no ``.is_dir()``/``.size`` at all. This test talks to the raw
    ``dissect.btrfs`` object directly instead of going through
    ``DiskFilesystem``, to pin down the underlying library
    behavior ``_btrfs_iterdir``'s own defenses rely on."""
    import dissect.btrfs.btrfs as btrfs_mod

    path, _size = _load_tar_gz_fixture("tiny_btrfs.raw.tar.gz")
    with path.open("rb") as fh:
        volume = btrfs_mod.Btrfs(fh)
        parent = volume.get("/subdir").listdir()[".."]
        assert isinstance(parent, btrfs_mod.Subvolume)
        assert not hasattr(parent, "is_dir")
        assert not hasattr(parent, "size")


def test_btrfs_iterdir_skips_a_child_missing_is_dir_even_under_a_non_dot_name() -> None:
    """Regression test for ``_btrfs_iterdir``'s own *second*, independent
    defense against the real quirk the characterization test above
    pins down (a bare ``Subvolume`` masquerading as a directory child
    with no ``.is_dir()``/``.size``): the ``hasattr(child, "is_dir")``
    guard must skip such an object even when it doesn't happen to be
    named "." or ".." -- proving it is load-bearing on its own, not
    just defensive commentary sitting next to the name check. Uses a
    hand-built fake entry rather than a real ``dissect.btrfs`` object
    (no ``dissect.btrfs`` install needed at all): the point is
    ``_btrfs_iterdir``'s own behavior, not reproducing the library
    quirk again (that's what the characterization test is for)."""

    class _FakeChild:
        def is_dir(self) -> bool:
            return False

        size = 3

    class _NotARealInode:
        pass  # deliberately no is_dir()/size, like a bare Subvolume

    class _FakeEntry:
        def listdir(self) -> dict[str, object]:
            return {"real_file.txt": _FakeChild(), "weird": _NotARealInode()}

    assert _btrfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="real_file.txt", is_dir=False, size=3, file_state=FileState.NORMAL, mtime=None)
    ]


def test_dissect_entry_list_dir_sorts_directories_before_files() -> None:
    """``_DissectEntry.list_dir`` sorts directories before files, then by
    name within each group -- a hand-built fake ``_Format`` rather than
    any real fixture, since real fixtures above don't control naming
    precisely enough to prove the container-vs-leaf rank is applied (as
    opposed to just the name tiebreaker). "zzz_dir" would sort after
    "aaa.txt" by name alone."""
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
    result = await content.export_to(dst)
    assert result.bytes_written == len(_BTRFS_HELLO_CONTENT)
    assert dst.read_bytes() == _BTRFS_HELLO_CONTENT


async def _open_btrfs_subvols() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture("tiny_btrfs_subvols.raw.tar.gz")))
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
    """Real, end-to-end proof that crossing into different Btrfs
    subvolumes reads each one's own distinct real bytes, never a
    mixed-up or cached-from-the-other-subvolume result — the scenario
    ``_btrfs_iterdir``'s own ``Subvolume``-as-``.parent`` guard
    (characterized against the flat, single-subvolume fixture by
    ``test_btrfs_dot_dot_can_be_a_bare_subvolume_object_not_a_real_inode``)
    exists for, now exercised against an actually-crossed real subvolume
    boundary instead of just the underlying library quirk in isolation."""
    disk_fs, addr = await _open_btrfs_subvols()
    root_content = await disk_fs.open_file(addr, "/root/hello.txt")
    assert await root_content.read(0, root_content.size) == _BTRFS_SUBVOL_ROOT_HELLO_CONTENT
    home_content = await disk_fs.open_file(addr, "/home/hello.txt")
    assert await home_content.read(0, home_content.size) == _BTRFS_SUBVOL_HOME_HELLO_CONTENT
    nested_content = await disk_fs.open_file(addr, "/root/subdir/nested.txt")
    assert await nested_content.read(0, nested_content.size) == _BTRFS_SUBVOL_NESTED_CONTENT


# -- NTFS -------------------------------------------------------------------


async def _open_ntfs() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture("tiny_ntfs.raw.tar.gz")))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_ntfs_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_ntfs()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    # $MFT/$LogFile/etc. are real NTFS system metadata files every real
    # volume has (kept, not filtered — the same posture ext4's own
    # "lost+found" test above takes); "." is the real self-referential
    # entry _ntfs_iterdir filters -- left unfiltered it would loop into
    # itself forever (see
    # test_ntfs_iterdir_filters_the_roots_self_referential_dot_entry).
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
    result = await content.export_to(dst)
    assert result.bytes_written == len(_NTFS_HELLO_CONTENT)
    assert dst.read_bytes() == _NTFS_HELLO_CONTENT


# -- shared / cross-cutting ------------------------------------------------


def test_disk_fs_available_reflects_real_import_system_state() -> None:
    # The whole dissect.* stack is a required dependency, not split per
    # format, so a normal dev environment always has every backend
    # present. See test_units_disk_fs_apfs.py for the "only one backend
    # present" cases, exercised via monkeypatch instead.
    assert disk_fs_available() is True


def test_disk_fs_available_false_when_every_backend_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    # Every existing disk_fs_available() test (here and in
    # test_units_disk_fs_apfs.py) only ever covers "at least one
    # present"; the all-absent False branch had no coverage of its own.
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a, **kw: None)
    assert disk_fs_available() is False


async def test_open_returns_none_when_nothing_on_the_disk_is_recognized() -> None:
    """``DiskFilesystem.open()`` returns ``None``, never raises, when
    nothing on the disk is recognized -- every existing test elsewhere in
    this file/``test_units_disk_fs_apfs.py``/the replay suite feeds real,
    openable bytes, leaving this path with no coverage of its own until
    now."""
    disk_fs = await DiskFilesystem.open(_FakeAsyncContent(b"\x00" * (2 * 1024 * 1024)))
    assert disk_fs is None


def test_try_import_returns_none_for_a_module_that_genuinely_does_not_exist() -> None:
    assert _try_import("this.module.does.not.exist.anywhere") is None


def test_default_size_falls_back_to_none_when_reading_size_raises() -> None:
    class _FakeEntry:
        @property
        def size(self) -> int:
            raise RuntimeError("no size for this kind of entry")

    assert _default_size(_FakeEntry()) is None


def test_ntfs_size_falls_back_to_none_when_size_raises() -> None:
    # A real NTFS system metadata file (e.g. $Secure, MFT record 9) can
    # have no unnamed $DATA stream at all -- MftRecord.size() looks for
    # exactly that stream and raises when it's missing.
    class _FakeEntry:
        def size(self) -> int:
            raise FileNotFoundError("no unnamed $DATA stream")

    assert _ntfs_size(_FakeEntry()) is None


def test_ntfs_iterdir_filters_the_roots_self_referential_dot_entry() -> None:
    """``_ntfs_iterdir``'s own filtering behavior: left unfiltered, the
    root directory's genuine, structural ``$FILE_NAME`` entry literally
    named ``"."`` (MFT record 5, whose ``ParentDirectory`` points back at
    the root itself) would make any resolution reaching past the root
    level recurse into ``"."`` forever, since its own listing contains
    another ``"."`` pointing at the same root, with no base case. Hand-built
    fake entry, no real ``dissect.ntfs`` install or disk image needed --
    ``test_ntfs_list_dir_lists_real_entries`` above proves the same
    filtering against the real fixture."""

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
    """Uses the same lightweight, non-dereferenced ``$FILE_NAME``
    attribute shape ``_ntfs_iterdir`` already reads for
    ``is_dir``/``file_size`` — ``_is_cloud_file`` costs nothing extra
    here."""

    class _CloudAttr:
        file_name = "cloud.txt"
        file_size = 5

        def is_dir(self) -> bool:
            return False

        def is_cloud_file(self) -> bool:
            return True

    class _CloudChild:
        attribute = _CloudAttr()

    class _FakeEntry:
        def iterdir(self, *, dereference: bool, ignore_dos: bool) -> list[_CloudChild]:
            return [_CloudChild()]

    assert _ntfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="cloud.txt", is_dir=False, size=5, file_state=FileState.CLOUD_ONLY, mtime=None)
    ]


#: Microsoft's own FILE_ATTRIBUTE_ENCRYPTED value (winnt.h), matching
#: dissect.ntfs's own ``c_ntfs.FILE_ATTRIBUTE.ENCRYPTED`` (0x4000) -- used
#: to build fakes below without importing dissect.ntfs into this test
#: module.
_FILE_ATTRIBUTE_ENCRYPTED = 0x4000


class _FakeStandardInformation:
    def __init__(self, file_attributes: int) -> None:
        self.file_attributes = file_attributes


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
    """A file that's both an evicted cloud placeholder *and* EFS-encrypted
    is reported as the cloud placeholder — it has no local bytes to even
    attempt reading, encrypted or not."""

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
    """Generic proof of ``_DissectEntry.open_file``'s own short-circuit:
    a truthy ``_Format.content_unavailable(entry)`` result raises before
    ``size()``/``DissectFileContentSource`` are ever touched. Built
    against a synthetic ``_Format``, not ``_NTFS_FORMAT`` itself — the
    point is this shared mechanism, not NTFS's own detection (already
    proven above)."""

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
        # A partition kind with no readable .type at all (a future
        # dissect.volume shape this project hasn't seen yet) must still
        # produce a label -- getattr-guarded like every other access in
        # this function -- rather than raising out of
        # DiskFilesystem.open()'s to_thread call.
        class _FakePart:
            name = ""
            type_name = "Unknown"

        assert _partition_table_label(_FakePart()) == "Unknown"


def test_try_open_apfs_skips_a_volume_whose_root_is_undecodable() -> None:
    """A sealed/signed system volume without a decodable root, or a
    FileVault-locked one without a key -- ``volume.get("/")`` raises,
    and only that one volume is skipped, not the whole container: a
    second, decodable volume in the same container still resolves."""

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


class _FakeSeekableStream:
    """Minimal in-memory stand-in for what a real Dissect filesystem
    entry's own ``.open()`` returns -- just enough ``seek``/``read`` for
    ``DissectFileContentSource._read_blocking`` to drive."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    def seek(self, offset: int) -> None:
        self._pos = offset

    def read(self, length: int) -> bytes:
        chunk = self._data[self._pos : self._pos + length]
        self._pos += len(chunk)
        return chunk


class _CountingFakeEntry:
    """Counts every real ``.open()`` call, handing back a fresh
    ``_FakeSeekableStream`` over ``data`` each time -- shared by every
    ``TestDissectFileContentSourceHandleReuse`` test that just needs to
    know how many times ``.open()`` was actually called."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.open_calls = 0

    def open(self) -> _FakeSeekableStream:
        self.open_calls += 1
        return _FakeSeekableStream(self._data)


class TestDissectFileContentSourceHandleReuse:
    """``DissectFileContentSource._read_blocking`` must resolve the guest
    file's own stream (``entry.open()`` — re-parses the whole file's
    runlist/fragmentation internally, work identical across every block
    of the same file) at most once per instance, reusing it across every
    block a streamed read/export issues rather than reopening per block."""

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

        result = await source.export_to(tmp_path / "out.bin")
        assert (tmp_path / "out.bin").read_bytes() == content
        assert result.bytes_written == len(content)
        assert entry.open_calls == 1  # export_to's own full pass reuses the same cached handle too

    async def test_a_failed_read_drops_the_cached_handle_so_the_next_call_gets_a_fresh_open(self) -> None:
        """A stream that raises partway through is left in an unknown
        state -- the cached handle must be dropped so a later call opens
        a fresh stream instead of reusing a possibly-broken one, the same
        self-healing a fresh per-call ``entry.open()`` always had. The
        raw failure itself is converted to ``DataCorruptError`` (this
        fake has no ``bsd_flags``/decmpfs xattr, so it never matches the
        cloud-placeholder check) -- see
        ``test_read_converts_a_dataless_apfs_open_failure_to_content_unavailable``
        for the ``ContentUnavailableError`` counterpart."""
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

                    stream.read = _fail  # type: ignore[method-assign]
                return stream

        entry = _FailFirstOpenEntry()
        source = DissectFileContentSource(entry, size=len(content))
        with pytest.raises(DataCorruptError, match="synthetic read failure") as exc_info:
            await source.read(0, 5)
        assert isinstance(exc_info.value.__cause__, OSError)
        assert await source.read(0, 5) == b"hello"
        assert entry.open_calls == 2  # the failed handle was dropped; the retry opened a fresh one

    async def test_short_read_reaching_declared_end_raises_data_corrupt(self) -> None:
        # A Dissect stream's own .read() can legitimately return fewer
        # bytes than requested (ordinary file-object semantics) -- when
        # that happens for a request reaching the file's declared end,
        # it's evidence the guest file's real data is truncated relative
        # to what the filesystem's own metadata declared.
        entry = _CountingFakeEntry(b"ab")
        source = DissectFileContentSource(entry, size=100)
        with pytest.raises(DataCorruptError, match="declared size=100"):
            await source.read(0, 100)

    async def test_short_read_not_reaching_declared_end_does_not_raise(self) -> None:
        # The same short underlying stream, but the caller only asked for
        # a window that doesn't reach the declared end -- not evidence of
        # anything missing, just a partial request.
        entry = _CountingFakeEntry(b"ab")
        source = DissectFileContentSource(entry, size=100)
        assert await source.read(0, 50) == b"ab"

    async def test_export_to_short_read_reaching_declared_end_raises_data_corrupt(self, tmp_path: Path) -> None:
        # export_to()'s own read-and-write helper replicates read()'s
        # short-read-at-declared-end check independently, rather than
        # calling read() and reusing it, because it fuses one block's read
        # and write into a single asyncio.to_thread() call for throughput
        # -- same fixture
        # shape as test_short_read_reaching_declared_end_raises_data_corrupt
        # above, proving that duplicated check independently.
        dst = tmp_path / "out.bin"
        entry = _CountingFakeEntry(b"ab")
        source = DissectFileContentSource(entry, size=100)
        with pytest.raises(DataCorruptError, match="declared size=100"):
            await source.export_to(dst)
        # This is the file's *only* block (100 bytes < DEFAULT_STREAM_BLOCK)
        # failing -- no destination file (or its parent directory) is ever
        # created for a doomed export that never got a single real byte.
        assert not dst.exists()

    async def test_export_to_never_creates_the_destination_file_when_the_first_block_fails(
        self, tmp_path: Path
    ) -> None:
        """Regression test: the destination file (and its parent
        directory) must not exist at all after a first-block failure --
        this is what previously let a cloud-sync placeholder's failed
        export leave a stray, empty ``.part`` file on disk (the ``.part``
        file itself is ``_run_export``'s/the CLI's own naming, not
        something this class knows about -- proven here at the plain
        ``dst`` path it's given)."""

        class _AlwaysFailsToOpenEntry:
            def open(self) -> object:
                raise EOFError("not enough bytes to read struct")

        dst = tmp_path / "nested" / "out.bin"
        source = DissectFileContentSource(_AlwaysFailsToOpenEntry(), size=1000)
        with pytest.raises(DataCorruptError):
            await source.export_to(dst)
        assert not dst.exists()
        assert not dst.parent.exists()  # mkdir(parents=True) is deferred right alongside dst.open()

    async def test_export_to_still_creates_an_empty_destination_file_for_a_genuinely_empty_source(
        self, tmp_path: Path
    ) -> None:
        """``size == 0`` means the read/write loop never runs at all (no
        "first block" to defer opening the destination past) -- a
        genuinely empty guest file's correct export result is still an
        empty destination file, not a missing one."""
        dst = tmp_path / "out.bin"
        entry = _CountingFakeEntry(b"")
        source = DissectFileContentSource(entry, size=0)
        result = await source.export_to(dst)
        assert dst.exists()
        assert dst.read_bytes() == b""
        assert result.bytes_written == 0

    async def test_export_to_keeps_the_partial_destination_file_when_a_later_block_fails(self, tmp_path: Path) -> None:
        """Deferring the destination file's creation only changes
        behavior for a *first*-block failure -- a failure on a later
        block still leaves the earlier, real bytes already written on
        disk (unchanged from before), matching ``_run_export``'s own
        Ctrl-C/``--keep-partial`` posture of keeping real partial
        progress rather than discarding it."""

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
            await source.export_to(dst)
        assert dst.exists()
        assert dst.read_bytes() == b"x" * DEFAULT_STREAM_BLOCK

    async def test_export_to_terminates_on_a_short_read_before_the_declared_end(self, tmp_path: Path) -> None:
        """Regression test: an *interior* short read (the guest file's
        real data genuinely truncated partway through, not just at the
        final block -- read() already treats this as legitimate) must not
        spin export_to()'s own read/write loop forever. ``size`` spans
        more than one ``DEFAULT_STREAM_BLOCK`` so
        the short read lands on a non-final block first; ``asyncio.
        wait_for``'s timeout is the actual regression guard -- a version
        of this loop that advances by actual bytes written, not by the
        full block requested, gets stuck re-requesting the same
        never-growing amount of progress and would time out here instead
        of completing (with ``DataCorruptError`` once the short read finally
        reaches the declared end, same as the single-block case above)."""

        class _AlwaysShortStream:
            def seek(self, offset: int) -> None:
                pass

            def read(self, length: int) -> bytes:
                return b"x" * min(length, 4)  # short regardless of position requested

        class _AlwaysShortEntry:
            def open(self) -> _AlwaysShortStream:
                return _AlwaysShortStream()

        source = DissectFileContentSource(_AlwaysShortEntry(), size=DEFAULT_STREAM_BLOCK * 2)
        with pytest.raises(DataCorruptError):
            await asyncio.wait_for(source.export_to(tmp_path / "out.bin"), timeout=5.0)

    async def test_concurrent_reads_do_not_race_the_shared_seek_position(self) -> None:
        """Without ``_fh_lock``, two concurrent ``asyncio.to_thread()``
        hops sharing one cached handle could interleave: A's ``seek(0)``,
        B's ``seek(5)`` (both fast, no delay), then A's delayed ``read()``
        picks up whichever ``seek`` landed last (B's) instead of its own.
        The race needs the delay inside ``read()`` specifically — a delay
        placed inside ``seek()`` instead would not reproduce it, since
        each thread's own ``read()`` already runs immediately after its
        ``seek()`` with nothing yielding the GIL in between. The lock
        must still serialize the two regardless of thread-pool timing."""
        content = b"AAAAABBBBB"  # offset [0,5) is 'A's, [5,10) is 'B's

        class _SlowReadStream(_FakeSeekableStream):
            def read(self, length: int) -> bytes:
                time.sleep(0.05)
                return super().read(length)

        class _FakeEntry:
            def open(self) -> _SlowReadStream:
                return _SlowReadStream(content)

        source = DissectFileContentSource(_FakeEntry(), size=len(content))
        a, b = await asyncio.gather(source.read(0, 5), source.read(5, 5))
        assert a == b"AAAAA"
        assert b == b"BBBBB"


__all__: list[str] = []
