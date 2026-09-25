"""Unit tests for ``synology_apm_repo.sdk.units.content.disk_fs``'s APFS
backend (``dissect.apfs``) — kept in its own file, separate from
``test_units_disk_fs.py``'s FAT/ext4 tests, matching this project's
established convention of splitting test files along the same lines the
underlying dissect backend does.

Fixture: ``tests/fixtures/tiny_apfs_gpt.raw.gz`` — a real, minimal raw
disk image, a 16MiB GPT-partitioned disk with one real ``Apple_APFS``
partition, built with macOS's own
``hdiutil`` (mounted, given a known ``hello.txt``/``subdir/nested.txt``
payload, unmounted, then ``hdiutil convert ... -format UDTO`` for raw
bytes).

The fixture's own ``Apple_APFS`` partition starts at byte offset 20480
(sector 40, 512-byte sectors).
"""

from __future__ import annotations

import asyncio
import gzip
import importlib.util
import struct
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import pytest

import synology_apm_repo.sdk.units.content.disk_fs._disk_filesystem as disk_fs_module
from synology_apm_repo.sdk.errors import ContentUnavailableError
from synology_apm_repo.sdk.units.base import FileState
from synology_apm_repo.sdk.units.content.disk_fs import (
    DiskFilesystem,
    DissectFileContentSource,
    _DissectEntry,
    disk_fs_available,
)
from synology_apm_repo.sdk.units.content.disk_fs._apfs import (
    _APFS_FORMAT,
    _apfs_content_unavailable,
    _apfs_entry_mtime,
    _apfs_is_dataless,
    _apfs_iterdir,
    _apfs_size,
)
from synology_apm_repo.sdk.units.content.disk_fs._base import _CLOUD_ONLY_REASON, _DirEntry

#: IS_PURGEABLE, per dissect.apfs's own c_apfs.py -- unrelated to the
#: dataless checks below, used only where a test needs to prove a
#: purgeable-but-not-dataless inode is left alone.
_IS_PURGEABLE_FLAG = 0x00080000

#: SF_DATALESS, per dissect.apfs's own c_apfs.py.
_SF_DATALESS_FLAG = 0x40000000

#: A real HFS+/APFS compression algorithm (LZFSE) -- disjoint from the
#: dataless-marker sentinel values, used to prove a genuinely compressed,
#: locally-resident file is never flagged.
_REAL_LZFSE_ALGORITHM = 11

#: One of Apple's own dataless-marker decmpfs algorithm sentinels.
_DATALESS_ALGORITHM = 0x80000001


def _decmpfs_header_bytes(algorithm: int, uncompressed_size: int) -> bytes:
    """Byte-for-byte what a real ``com.apple.decmpfs`` xattr's header
    looks like on disk, matching the ``dissect.apfs`` cstruct type
    ``_apfs_decmpfs_header`` decodes with."""
    magic_int = int.from_bytes(b"cmpf", "big")
    return struct.pack("<IIQ", magic_int, algorithm, uncompressed_size)


class _FakeXAttr:
    def __init__(self, algorithm: int, uncompressed_size: int) -> None:
        self._raw = _decmpfs_header_bytes(algorithm, uncompressed_size)

    def open(self) -> BytesIO:
        return BytesIO(self._raw)


class _FakeApfsInode:
    """A fake APFS ``INode``, exposing exactly what ``_apfs_is_dataless``/
    ``_apfs_size`` need: ``bsd_flags``, ``is_compressed()``, ``xattr``,
    and ``size``."""

    def __init__(
        self,
        *,
        bsd_flags: int = 0,
        decmpfs_algorithm: int | None = None,
        decmpfs_uncompressed_size: int = 100,
        size: int | None = 0,
        size_raises: BaseException | None = None,
        mtime: datetime | None = None,
    ) -> None:
        self.bsd_flags = bsd_flags
        self._size = size
        self._size_raises = size_raises
        self.mtime = mtime
        self._xattr: dict[str, object] = {}
        if decmpfs_algorithm is not None:
            self._xattr["com.apple.decmpfs"] = _FakeXAttr(decmpfs_algorithm, decmpfs_uncompressed_size)

    @property
    def xattr(self) -> dict[str, object]:
        return self._xattr

    def is_compressed(self) -> bool:
        return bool(self.bsd_flags & 0x00000020)  # UF_COMPRESSED

    @property
    def size(self) -> int | None:
        if self._size_raises is not None:
            raise self._size_raises
        return self._size


_FIXTURE = Path(__file__).parent.parent.parent / "fixtures" / "tiny_apfs_gpt.raw.gz"
_APFS_PARTITION_OFFSET = 20480

_HELLO_CONTENT = b"hello from a real APFS test fixture\n"
_NESTED_CONTENT = b"nested file\n"


def _load_fixture() -> bytes:
    return gzip.decompress(_FIXTURE.read_bytes())


class _FakeAsyncContent:
    """Same shape as ``test_units_disk_fs.py``'s own helper of the same
    name (this project's convention is one test file never imports
    another) — mimics ``ContentSource``
    over an in-memory buffer, with a real ``await`` point in ``read()``
    proving the async/sync bridge genuinely round-trips through the
    event loop."""

    supports_concurrent_export = False

    def __init__(self, data: bytes) -> None:
        self._data = data

    @property
    def size(self) -> int | None:
        return len(self._data)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        await asyncio.sleep(0)
        n = length if length is not None else len(self._data) - offset
        return self._data[offset : offset + n]

    def stream(self, block: int = 8 << 20) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")

    async def export_to(
        self, dst: Path, *, sparse: bool = True, progress: Callable[[int, int], Awaitable[None]] | None = None
    ) -> object:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")


async def test_open_recognizes_a_gpt_wrapped_apfs_container() -> None:
    """The fixture's own real shape: a GPT partition table
    ``dissect.volume`` has to walk first, with a real Apple_APFS
    partition inside it ``dissect.apfs`` then decodes at the byte offset
    ``dissect.volume`` computed."""
    disk_fs = await DiskFilesystem.open(_FakeAsyncContent(_load_fixture()))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert "APFS" in label
    assert "TinyAPFSTest" in label


async def test_open_recognizes_a_bare_unpartitioned_apfs_container() -> None:
    """The real-world shape a genuine macOS PC/PS backup disk actually
    has: no partition table at all, just the APFS container's own
    superblock starting at byte offset 0. Slicing the GPT wrapper off this same
    fixture reproduces that shape directly — this path is exercised
    whether or not dissect.volume is installed (no partition table
    means a single "(whole image)" candidate either way)."""
    bare = _load_fixture()[_APFS_PARTITION_OFFSET:]
    disk_fs = await DiskFilesystem.open(_FakeAsyncContent(bare))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    # The "(whole image)" placeholder is dropped entirely rather than kept
    # as a redundant "(whole image) - " prefix when a bare APFS container
    # has no partition table: the container's own per-volume name already
    # says more.
    assert label == "TinyAPFSTest (APFS)"


async def test_open_recognizes_apfs_container_without_dissect_volume_at_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without dissect.volume there is no partition-table engine at all,
    so no partition candidates are ever produced -- ``open()`` then falls
    back to treating the whole image as one bare, unpartitioned candidate
    at offset 0. Forces that path via monkeypatch rather than actually
    uninstalling dissect.volume."""
    real_try_import = disk_fs_module._try_import

    def _fake_try_import(module_path: str) -> object | None:
        if module_path == "dissect.volume.disk.disk":
            return None
        return real_try_import(module_path)

    monkeypatch.setattr(disk_fs_module, "_try_import", _fake_try_import)
    bare = _load_fixture()[_APFS_PARTITION_OFFSET:]
    disk_fs = await DiskFilesystem.open(_FakeAsyncContent(bare))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert label == "TinyAPFSTest (APFS)"


async def _open_gpt_fixture() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(_FakeAsyncContent(_load_fixture()))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_gpt_fixture()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    # Real entries from a real, if minimal, macOS-created APFS volume --
    # ".fseventsd" is macOS's own real bookkeeping directory (kept, not
    # filtered: a real directory a real filesystem creates).
    assert {"hello.txt", "subdir", ".fseventsd"} <= names

    hello = next(e for e in entries if e.name == "hello.txt")
    assert hello.is_dir is False
    assert hello.size == len(_HELLO_CONTENT)
    assert isinstance(hello.mtime, datetime)
    assert hello.mtime.tzinfo is not None

    subdir = next(e for e in entries if e.name == "subdir")
    assert subdir.is_dir is True
    assert isinstance(subdir.mtime, datetime)


def test_apfs_iterdir_skips_a_self_reference_and_a_partial_object() -> None:
    """Regression-shaped test for ``_apfs_iterdir``'s defensive filter/
    guard, mirroring ``test_units_disk_fs.py``'s own Btrfs equivalent —
    there is no real macOS sample confirming ``dissect.apfs`` actually
    emits either hazard (unlike NTFS's self-referencing "." entry and
    Btrfs's bare-``Subvolume`` child), so this only proves
    ``_apfs_iterdir`` itself would handle them if it did. Uses a
    hand-built fake entry, no ``dissect.apfs`` install needed."""

    class _FakeInode:
        size = 3

    class _FakeChild:
        name = "real_file.txt"
        inode = _FakeInode()

        def is_dir(self) -> bool:
            return False

    class _SelfReference:
        name = "."

        def is_dir(self) -> bool:
            return True

    class _PartialObject:
        name = "weird"  # deliberately no is_dir()/inode, unlike a real DirectoryEntry

    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FakeChild(), _SelfReference(), _PartialObject()]

    assert _apfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="real_file.txt", is_dir=False, size=3, file_state=FileState.NORMAL, mtime=None)
    ]


def test_apfs_iterdir_degrades_size_to_none_when_the_underlying_parser_fails() -> None:
    """Regression test for a real hazard: a macOS ".DS_Store" file's own
    inode can carry a DIR_STATS_KEY extended field (normally only
    meaningful on a directory's inode) whose raw bytes are shorter than
    dissect.apfs's own parser for that field type expects, raising a
    bare EOFError while decoding ``child.inode.size`` -- unrelated to
    this format's own logic. One such file anywhere in a directory must
    not fail the *entire* listing, only that one file's size."""

    class _FailingInode:
        @property
        def size(self) -> int:
            raise EOFError("not enough bytes to read struct")

    class _FailingChild:
        name = "real_file.txt"
        inode = _FailingInode()

        def is_dir(self) -> bool:
            return False

    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FailingChild()]

    assert _apfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="real_file.txt", is_dir=False, size=None, file_state=FileState.NORMAL, mtime=None)
    ]


def test_apfs_iterdir_flags_a_dataless_file_child_as_cloud_only() -> None:
    """A file whose inode has both a real, cleanly-read ``.size`` *and*
    ``SF_DATALESS`` set still gets flagged — the flag doesn't require an
    actual read failure to have happened first."""

    class _FakeChild:
        name = "dataless_file.txt"
        inode = _FakeApfsInode(bsd_flags=_SF_DATALESS_FLAG, size=3)

        def is_dir(self) -> bool:
            return False

    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FakeChild()]

    assert _apfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="dataless_file.txt", is_dir=False, size=3, file_state=FileState.CLOUD_ONLY, mtime=None)
    ]


def test_apfs_iterdir_size_fix_reaches_the_listing_too() -> None:
    """A dataless file whose ``.size`` is wrongly ``0`` (``dissect.apfs``'s
    own zeroed-on-eviction bug) shows its real, pre-eviction size in the
    listing, not ``0`` — proving ``_apfs_size``'s fix reaches
    ``_apfs_iterdir``, not just the single-file-open path."""

    class _FakeChild:
        name = "C2Password_Export_20221014.zip"
        inode = _FakeApfsInode(
            bsd_flags=_SF_DATALESS_FLAG | 0x00000020,  # SF_DATALESS | UF_COMPRESSED
            decmpfs_algorithm=_DATALESS_ALGORITHM,
            decmpfs_uncompressed_size=13016,
            size=0,
        )

        def is_dir(self) -> bool:
            return False

    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FakeChild()]

    assert _apfs_iterdir(_FakeEntry()) == [
        _DirEntry(
            name="C2Password_Export_20221014.zip", is_dir=False, size=13016, file_state=FileState.CLOUD_ONLY, mtime=None
        )
    ]


def test_apfs_iterdir_reads_a_real_mtime_for_both_a_file_and_a_directory_child() -> None:
    """``_apfs_iterdir`` reads a distinct, real ``mtime`` for both a file
    and a directory child -- a directory's own inode is dereferenced the
    same as a file's, not skipped."""
    file_mtime = datetime(2024, 3, 4, 5, 6, 7, tzinfo=UTC)
    dir_mtime = datetime(2024, 8, 9, 10, 11, 12, tzinfo=UTC)

    class _FileChild:
        name = "timed_file.txt"
        inode = _FakeApfsInode(size=3, mtime=file_mtime)

        def is_dir(self) -> bool:
            return False

    class _DirChild:
        name = "timed_dir"
        inode = _FakeApfsInode(mtime=dir_mtime)

        def is_dir(self) -> bool:
            return True

    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FileChild(), _DirChild()]

    file_entry, dir_entry = _apfs_iterdir(_FakeEntry())
    assert file_entry.mtime == file_mtime
    assert dir_entry.mtime == dir_mtime


def test_apfs_entry_mtime_degrades_to_none_when_the_underlying_read_raises() -> None:
    class _RaisingInode:
        @property
        def mtime(self) -> datetime:
            raise RuntimeError("malformed APFS inode timestamp")

    assert _apfs_entry_mtime(_RaisingInode()) is None


def test_apfs_is_dataless_matches_sf_dataless_or_a_dataless_decmpfs_algorithm() -> None:
    # SF_DATALESS alone is enough even without is_compressed()/a decmpfs xattr at all.
    assert _apfs_is_dataless(_FakeApfsInode(bsd_flags=_SF_DATALESS_FLAG)) is True
    assert _apfs_is_dataless(_FakeApfsInode(decmpfs_algorithm=_DATALESS_ALGORITHM)) is True
    # The decmpfs algorithm check fires unconditionally -- not gated behind
    # is_compressed() -- since a real dataless file's own is_compressed()
    # bit is commonly unset.
    assert _apfs_is_dataless(_FakeApfsInode(bsd_flags=0, decmpfs_algorithm=_DATALESS_ALGORITHM)) is True
    # IS_PURGEABLE (the old, superseded heuristic bit) is unrelated now.
    assert _apfs_is_dataless(_FakeApfsInode(bsd_flags=_IS_PURGEABLE_FLAG)) is False
    # A real compression algorithm (LZFSE) must never be flagged.
    assert _apfs_is_dataless(_FakeApfsInode(bsd_flags=0x00000020, decmpfs_algorithm=_REAL_LZFSE_ALGORITHM)) is False
    assert _apfs_is_dataless(_FakeApfsInode()) is False


def test_apfs_content_unavailable_is_a_thin_wrapper_around_is_dataless() -> None:
    assert _apfs_content_unavailable(_FakeApfsInode(bsd_flags=_SF_DATALESS_FLAG)) == _CLOUD_ONLY_REASON
    assert _apfs_content_unavailable(_FakeApfsInode()) is None


def test_open_file_raises_content_unavailable_for_a_dataless_apfs_entry_before_sizing_it() -> None:
    """End-to-end proof that ``_APFS_FORMAT.content_unavailable`` (wired
    to ``_apfs_content_unavailable``) runs at ``_DissectEntry.open_file``
    time, before ``_Format.size``/``DissectFileContentSource`` is ever
    constructed -- so a placeholder export never gets as far as creating
    a destination file at all."""

    class _CountingFakeApfsInode(_FakeApfsInode):
        size_calls = 0

        @property
        def size(self) -> int | None:
            type(self).size_calls += 1
            return super().size

    entry = _CountingFakeApfsInode(bsd_flags=_SF_DATALESS_FLAG, size=123)

    class _FakeVolume:
        def get(self, path: str) -> _CountingFakeApfsInode:
            return entry

    dissect_entry = _DissectEntry(_APFS_FORMAT, _FakeVolume())
    with pytest.raises(ContentUnavailableError, match="cloud-sync placeholder"):
        dissect_entry.open_file("/dataless_file.txt")
    assert _CountingFakeApfsInode.size_calls == 0


async def test_read_converts_a_dataless_apfs_open_failure_to_content_unavailable() -> None:
    """A dataless APFS inode whose ``.open()`` raises a bare ``EOFError``
    (dissect.apfs's own ``DIR_STATS_KEY``/``XF_MAP`` limitation) surfaces as
    ``ContentUnavailableError``, not the raw ``EOFError`` nor the generic
    ``DataCorruptError`` a non-dataless failure would still get (see
    ``test_a_failed_read_drops_the_cached_handle_so_the_next_call_gets_a_fresh_open``
    in ``test_units_disk_fs.py`` for that counterpart). This exercises
    the reactive path (``_read_blocking``) directly, constructing
    ``DissectFileContentSource`` by hand -- ``_apfs_content_unavailable``
    (tested above) is the proactive counterpart that now catches this
    same case earlier, at ``open_file()`` time, before this class is
    ever constructed."""

    class _DatalessEntry(_FakeApfsInode):
        def open(self) -> object:
            raise EOFError("not enough bytes to read struct")

    source = DissectFileContentSource(_DatalessEntry(bsd_flags=_SF_DATALESS_FLAG), size=10)
    with pytest.raises(ContentUnavailableError, match="cloud-sync placeholder") as exc_info:
        await source.read(0, 5)
    assert str(exc_info.value) == _CLOUD_ONLY_REASON
    assert isinstance(exc_info.value.__cause__, EOFError)


def test_apfs_size_prefers_the_decmpfs_headers_uncompressed_size_when_zeroed_on_eviction() -> None:
    """A compressed entry reporting ``.size == 0`` (dissect.apfs's own
    zeroed-on-eviction bug) gets the correct, nonzero size back from its
    decmpfs xattr's own header instead."""
    entry = _FakeApfsInode(
        bsd_flags=0x00000020,  # UF_COMPRESSED
        decmpfs_algorithm=_DATALESS_ALGORITHM,
        decmpfs_uncompressed_size=13016,
        size=0,
    )
    assert _apfs_size(entry) == 13016


def test_apfs_size_never_overrides_a_genuinely_empty_non_compressed_files_zero() -> None:
    entry = _FakeApfsInode(bsd_flags=0, size=0)
    assert _apfs_size(entry) == 0


def test_apfs_size_returns_zero_when_compressed_but_no_decmpfs_xattr_at_all() -> None:
    """A distinct code path from the "real compression algorithm" case
    above -- here there's no decmpfs xattr to fall back to at all."""
    entry = _FakeApfsInode(bsd_flags=0x00000020, size=0)  # UF_COMPRESSED, no decmpfs xattr
    assert _apfs_size(entry) == 0


async def test_open_file_reads_the_real_file_content() -> None:
    disk_fs, addr = await _open_gpt_fixture()
    content = await disk_fs.open_file(addr, "/hello.txt")
    assert content.size == len(_HELLO_CONTENT)
    assert await content.read(0, content.size) == _HELLO_CONTENT
    assert await content.read(6, 4) == b"from"


async def test_subdirectory_listing_and_read_of_a_nested_file() -> None:
    disk_fs, addr = await _open_gpt_fixture()
    entries = await disk_fs.list_dir(addr, "/subdir")
    assert {e.name for e in entries} == {"nested.txt"}

    content = await disk_fs.open_file(addr, "/subdir/nested.txt")
    assert await content.read(0, content.size) == _NESTED_CONTENT


async def test_content_source_stream_reassembles_to_the_same_bytes() -> None:
    disk_fs, addr = await _open_gpt_fixture()
    content = await disk_fs.open_file(addr, "/hello.txt")
    chunks = [chunk async for _pos, chunk in content.stream(block=4)]
    assert b"".join(chunks) == _HELLO_CONTENT


async def test_export_to_writes_the_real_file_content(tmp_path: Path) -> None:
    disk_fs, addr = await _open_gpt_fixture()
    content = await disk_fs.open_file(addr, "/hello.txt")

    dst = tmp_path / "exported_hello.txt"
    result = await content.export_to(dst)
    assert result.bytes_written == len(_HELLO_CONTENT)
    assert result.logical_size == len(_HELLO_CONTENT)
    assert dst.read_bytes() == _HELLO_CONTENT


async def test_open_raises_disk_filesystem_unavailable_when_no_backend_importable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers ``open()``'s own defensive check directly, via
    monkeypatching ``disk_fs_available`` rather than actually
    uninstalling every real ``dissect.*`` package (this dev environment
    always has the whole extra installed)."""
    monkeypatch.setattr(disk_fs_module, "disk_fs_available", lambda: False)
    with pytest.raises(disk_fs_module.DiskFilesystemUnavailableError):
        await DiskFilesystem.open(_FakeAsyncContent(_load_fixture()))


async def test_read_with_zero_length_returns_empty_bytes_without_a_real_read() -> None:
    # The n <= 0 short-circuit lives in the shared, format-independent
    # _BlockingReadContentSource.read() (disk_fs.py) every format's own
    # content source goes through -- one test here is enough, no need
    # for a FAT-backed twin of this same case.
    disk_fs, addr = await _open_gpt_fixture()
    content = await disk_fs.open_file(addr, "/hello.txt")
    assert await content.read(0, 0) == b""


async def test_export_to_invokes_the_progress_callback(tmp_path: Path) -> None:
    disk_fs, addr = await _open_gpt_fixture()
    content = await disk_fs.open_file(addr, "/hello.txt")

    calls: list[tuple[int, int]] = []

    async def _progress(written: int, total: int) -> None:
        calls.append((written, total))

    dst = tmp_path / "exported_with_progress.txt"
    result = await content.export_to(dst, progress=_progress)
    assert result.bytes_written == len(_HELLO_CONTENT)
    assert calls == [(len(_HELLO_CONTENT), len(_HELLO_CONTENT))]


def test_disk_fs_available_true_with_only_dissect_apfs(monkeypatch: pytest.MonkeyPatch) -> None:
    real_find_spec = importlib.util.find_spec

    def _fake_find_spec(name: str, *args: object, **kwargs: object) -> object | None:
        # Excludes every _DISSECT_PACKAGES name but "dissect.apfs" itself,
        # so this genuinely isolates "only apfs" as available rather than
        # leaving a sibling package real too.
        if name in ("dissect.volume", "dissect.ntfs", "dissect.extfs", "dissect.xfs", "dissect.btrfs", "dissect.fat"):
            return None
        return real_find_spec(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(importlib.util, "find_spec", _fake_find_spec)
    assert disk_fs_available() is True


__all__: list[str] = []
