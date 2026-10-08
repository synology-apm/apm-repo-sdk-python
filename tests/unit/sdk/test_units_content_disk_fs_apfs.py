"""Unit tests for ``synology_apm_repo.sdk.units.content.disk_fs``'s APFS
backend (``dissect.apfs``) — kept apart from ``test_units_content_disk_fs.py``,
which covers the other formats.

Fixture: ``tests/fixtures/tiny_apfs_gpt.raw.gz``, a 16MiB GPT disk with one
``Apple_APFS`` partition at byte offset 20480, built with macOS ``hdiutil`` and
holding a ``hello.txt``/``subdir/nested.txt`` payload.
"""

from __future__ import annotations

import importlib.util
import struct
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import pytest

import synology_apm_repo.sdk.units.content.disk_fs._disk_filesystem as disk_fs_module
from support.fakes import unchecked_fake
from synology_apm_repo.sdk.errors import ContentUnavailableError
from synology_apm_repo.sdk.export import LocalFileSink, run_export
from synology_apm_repo.sdk.units.base import FileState
from synology_apm_repo.sdk.units.content.disk_fs import (
    DiskFilesystem,
    DissectFileContentSource,
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
from synology_apm_repo.sdk.units.content.disk_fs._disk_filesystem import _DissectEntry
from unit.sdk.disk_fs_fakes import MemoryContent, raw_image

#: IS_PURGEABLE, per dissect.apfs's c_apfs.py -- for proving a
#: purgeable-but-not-dataless inode is left alone.
_IS_PURGEABLE_FLAG = 0x00080000

#: SF_DATALESS, per dissect.apfs's own c_apfs.py.
_SF_DATALESS_FLAG = 0x40000000

#: A real compression algorithm (LZFSE), disjoint from the dataless
#: sentinels -- for proving a compressed, locally-resident file isn't flagged.
_REAL_LZFSE_ALGORITHM = 11

#: One of Apple's own dataless-marker decmpfs algorithm sentinels.
_DATALESS_ALGORITHM = 0x80000001


def _decmpfs_header_bytes(algorithm: int, uncompressed_size: int) -> bytes:
    """A ``com.apple.decmpfs`` xattr header, in the layout
    ``_apfs_decmpfs_header`` decodes."""
    magic_int = int.from_bytes(b"cmpf", "big")
    return struct.pack("<IIQ", magic_int, algorithm, uncompressed_size)


@unchecked_fake("a dissect.apfs object")
class _FakeXAttr:
    def __init__(self, algorithm: int, uncompressed_size: int) -> None:
        self._raw = _decmpfs_header_bytes(algorithm, uncompressed_size)

    def open(self) -> BytesIO:
        return BytesIO(self._raw)


@unchecked_fake("a dissect.apfs object")
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


_APFS_PARTITION_OFFSET = 20480

_HELLO_CONTENT = b"hello from a real APFS test fixture\n"
_NESTED_CONTENT = b"nested file\n"


async def test_open_recognizes_a_gpt_wrapped_apfs_container() -> None:
    """A GPT partition table is walked first; the Apple_APFS partition
    inside it is then decoded at the offset ``dissect.volume`` computed."""
    disk_fs = await DiskFilesystem.open(MemoryContent(raw_image("tiny_apfs_gpt.raw.gz")))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert "APFS" in label
    assert "TinyAPFSTest" in label


async def test_open_recognizes_a_bare_unpartitioned_apfs_container() -> None:
    bare = raw_image("tiny_apfs_gpt.raw.gz")[_APFS_PARTITION_OFFSET:]
    disk_fs = await DiskFilesystem.open(MemoryContent(bare))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    # No redundant "(whole image) - " prefix: the volume name says more.
    assert label == "TinyAPFSTest (APFS)"


async def test_open_recognizes_apfs_container_without_dissect_volume_at_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without dissect.volume, ``open()`` treats the whole image as one
    unpartitioned candidate at offset 0 (forced here via monkeypatch)."""
    real_try_import = disk_fs_module._try_import

    def _fake_try_import(module_path: str) -> object | None:
        if module_path == "dissect.volume.disk.disk":
            return None
        return real_try_import(module_path)

    monkeypatch.setattr(disk_fs_module, "_try_import", _fake_try_import)
    bare = raw_image("tiny_apfs_gpt.raw.gz")[_APFS_PARTITION_OFFSET:]
    disk_fs = await DiskFilesystem.open(MemoryContent(bare))
    assert disk_fs is not None
    partitions = disk_fs.partitions()
    assert len(partitions) == 1
    _addr, label = partitions[0]
    assert label == "TinyAPFSTest (APFS)"


async def _open_gpt_fixture() -> tuple[DiskFilesystem, int]:
    disk_fs = await DiskFilesystem.open(MemoryContent(raw_image("tiny_apfs_gpt.raw.gz")))
    assert disk_fs is not None
    ((addr, _label),) = disk_fs.partitions()
    return disk_fs, addr


async def test_list_dir_lists_real_entries() -> None:
    disk_fs, addr = await _open_gpt_fixture()
    entries = await disk_fs.list_dir(addr, "/")
    names = {e.name for e in entries}
    # ".fseventsd" is macOS's own bookkeeping directory, kept, not filtered.
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
    """``_apfs_iterdir`` skips a ``"."`` self-reference and a child lacking
    ``is_dir()``/``inode``."""

    class _FakeInode:
        size = 3

    @unchecked_fake("a dissect.apfs object")
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

    @unchecked_fake("a dissect.apfs object")
    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FakeChild(), _SelfReference(), _PartialObject()]

    assert _apfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="real_file.txt", is_dir=False, size=3, file_state=FileState.NORMAL, mtime=None)
    ]


def test_apfs_iterdir_degrades_size_to_none_when_the_underlying_parser_fails() -> None:
    """A child whose ``inode.size`` raises (e.g. a ".DS_Store" with a truncated
    DIR_STATS_KEY field) gets ``size=None`` without failing the listing."""

    class _FailingInode:
        @property
        def size(self) -> int:
            raise EOFError("not enough bytes to read struct")

    class _FailingChild:
        name = "real_file.txt"
        inode = _FailingInode()

        def is_dir(self) -> bool:
            return False

    @unchecked_fake("a dissect.apfs object")
    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FailingChild()]

    assert _apfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="real_file.txt", is_dir=False, size=None, file_state=FileState.NORMAL, mtime=None)
    ]


def test_apfs_iterdir_flags_a_dataless_file_child_as_cloud_only() -> None:
    """``SF_DATALESS`` flags a file as cloud-only even when its ``.size``
    reads cleanly."""

    @unchecked_fake("a dissect.apfs object")
    class _FakeChild:
        name = "dataless_file.txt"
        inode = _FakeApfsInode(bsd_flags=_SF_DATALESS_FLAG, size=3)

        def is_dir(self) -> bool:
            return False

    @unchecked_fake("a dissect.apfs object")
    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FakeChild()]

    assert _apfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="dataless_file.txt", is_dir=False, size=3, file_state=FileState.CLOUD_ONLY, mtime=None)
    ]


def test_apfs_iterdir_lists_a_dataless_files_pre_eviction_size() -> None:
    """A dataless file whose ``.size`` is wrongly ``0`` (zeroed on
    eviction) lists its pre-eviction size via ``_apfs_size``."""

    @unchecked_fake("a dissect.apfs object")
    class _FakeChild:
        name = "alice-notes.zip"
        inode = _FakeApfsInode(
            bsd_flags=_SF_DATALESS_FLAG | 0x00000020,  # SF_DATALESS | UF_COMPRESSED
            decmpfs_algorithm=_DATALESS_ALGORITHM,
            decmpfs_uncompressed_size=13016,
            size=0,
        )

        def is_dir(self) -> bool:
            return False

    @unchecked_fake("a dissect.apfs object")
    class _FakeEntry:
        def iterdir(self) -> list[object]:
            return [_FakeChild()]

    assert _apfs_iterdir(_FakeEntry()) == [
        _DirEntry(name="alice-notes.zip", is_dir=False, size=13016, file_state=FileState.CLOUD_ONLY, mtime=None)
    ]


def test_apfs_iterdir_reads_a_real_mtime_for_both_a_file_and_a_directory_child() -> None:
    """``_apfs_iterdir`` reads ``mtime`` for a directory child as well as
    a file child."""
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

    @unchecked_fake("a dissect.apfs object")
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
    # SF_DATALESS alone is enough, without is_compressed() or a decmpfs xattr.
    assert _apfs_is_dataless(_FakeApfsInode(bsd_flags=_SF_DATALESS_FLAG)) is True
    assert _apfs_is_dataless(_FakeApfsInode(decmpfs_algorithm=_DATALESS_ALGORITHM)) is True
    # The decmpfs check isn't gated on is_compressed(), which a real
    # dataless file commonly leaves unset.
    assert _apfs_is_dataless(_FakeApfsInode(bsd_flags=0, decmpfs_algorithm=_DATALESS_ALGORITHM)) is True
    # IS_PURGEABLE is unrelated.
    assert _apfs_is_dataless(_FakeApfsInode(bsd_flags=_IS_PURGEABLE_FLAG)) is False
    # A real compression algorithm (LZFSE) must never be flagged.
    assert _apfs_is_dataless(_FakeApfsInode(bsd_flags=0x00000020, decmpfs_algorithm=_REAL_LZFSE_ALGORITHM)) is False
    assert _apfs_is_dataless(_FakeApfsInode()) is False


def test_apfs_content_unavailable_is_a_thin_wrapper_around_is_dataless() -> None:
    assert _apfs_content_unavailable(_FakeApfsInode(bsd_flags=_SF_DATALESS_FLAG)) == _CLOUD_ONLY_REASON
    assert _apfs_content_unavailable(_FakeApfsInode()) is None


def test_open_file_raises_content_unavailable_for_a_dataless_apfs_entry_before_sizing_it() -> None:
    """``_DissectEntry.open_file`` raises for a dataless entry, which
    ``_APFS_FORMAT.content_unavailable`` reports, before sizing it."""

    class _CountingFakeApfsInode(_FakeApfsInode):
        size_calls = 0

        @property
        def size(self) -> int | None:
            type(self).size_calls += 1
            return super().size

    entry = _CountingFakeApfsInode(bsd_flags=_SF_DATALESS_FLAG, size=123)

    @unchecked_fake("a dissect.apfs object")
    class _FakeVolume:
        def get(self, path: str) -> _CountingFakeApfsInode:
            return entry

    dissect_entry = _DissectEntry(_APFS_FORMAT, _FakeVolume())
    with pytest.raises(ContentUnavailableError, match="cloud-sync placeholder"):
        dissect_entry.open_file("/dataless_file.txt")
    assert _CountingFakeApfsInode.size_calls == 0


async def test_read_converts_a_dataless_apfs_open_failure_to_content_unavailable() -> None:
    """A dataless inode whose ``.open()`` raises a bare ``EOFError`` surfaces from
    ``_read_blocking`` as ``ContentUnavailableError``, not ``DataCorruptError``."""

    class _DatalessEntry(_FakeApfsInode):
        def open(self) -> object:
            raise EOFError("not enough bytes to read struct")

    source = DissectFileContentSource(_DatalessEntry(bsd_flags=_SF_DATALESS_FLAG), size=10)
    with pytest.raises(ContentUnavailableError, match="cloud-sync placeholder") as exc_info:
        await source.read(0, 5)
    assert str(exc_info.value) == _CLOUD_ONLY_REASON
    assert isinstance(exc_info.value.__cause__, EOFError)


def test_apfs_size_prefers_the_decmpfs_headers_uncompressed_size_when_zeroed_on_eviction() -> None:
    """A compressed entry reporting ``.size == 0`` (zeroed on eviction)
    gets its size from the decmpfs xattr header instead."""
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
    """With no decmpfs xattr to fall back to, the size stays ``0``."""
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
    result = await run_export(content, LocalFileSink(dst, staged=False))
    assert result.bytes_written == len(_HELLO_CONTENT)
    assert result.logical_size == len(_HELLO_CONTENT)
    assert dst.read_bytes() == _HELLO_CONTENT


async def test_open_raises_disk_filesystem_unavailable_when_no_backend_importable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``open()`` raises ``DiskFilesystemUnavailableError`` when
    ``disk_fs_available()`` is false (monkeypatched)."""
    monkeypatch.setattr(disk_fs_module, "disk_fs_available", lambda: False)
    with pytest.raises(
        disk_fs_module.DiskFilesystemUnavailableError, match=r"no dissect\.\* filesystem-parsing package is installed"
    ):
        await DiskFilesystem.open(MemoryContent(raw_image("tiny_apfs_gpt.raw.gz")))


async def test_read_with_zero_length_returns_empty_bytes_without_a_real_read() -> None:
    # The n <= 0 short-circuit lives in the format-independent
    # _BlockingReadContentSource.read() (_content_source.py), so one
    # format suffices.
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
    result = await run_export(content, LocalFileSink(dst, staged=False), progress=_progress)
    assert result.bytes_written == len(_HELLO_CONTENT)
    assert calls == [(len(_HELLO_CONTENT), len(_HELLO_CONTENT))]


def test_disk_fs_available_true_with_only_dissect_apfs(monkeypatch: pytest.MonkeyPatch) -> None:
    real_find_spec = importlib.util.find_spec

    def _fake_find_spec(name: str, *args: object, **kwargs: object) -> object | None:
        # Hides every _DISSECT_PACKAGES name except "dissect.apfs".
        if name in ("dissect.volume", "dissect.ntfs", "dissect.extfs", "dissect.xfs", "dissect.btrfs", "dissect.fat"):
            return None
        return real_find_spec(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(importlib.util, "find_spec", _fake_find_spec)
    assert disk_fs_available() is True
