"""Unit tests for ``synology_apm_repo.sdk.units.content.disk_fs``'s APFS
backend (``dissect.apfs``) — kept in its own file, separate from
``test_units_disk_fs.py``'s FAT/ext4 tests, matching this project's
established convention of splitting test files along the same lines the
underlying dissect backend does.

Fixture: ``tests/fixtures/tiny_apfs_gpt.raw.gz`` — a real, minimal
(~20KB gzip-compressed) raw disk image, a 16MiB GPT-partitioned disk
with one real ``Apple_APFS`` partition, built with macOS's own
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
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest

import synology_apm_repo.sdk.units.content.disk_fs as disk_fs_module
from synology_apm_repo.sdk.units.content.disk_fs import DiskFilesystem, _apfs_iterdir, disk_fs_available

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
    # The volume's own real name says more than the "(whole image)"
    # placeholder every bare/unpartitioned candidate is otherwise
    # constructed with -- shown here instead of alongside it (see
    # ``_try_open_apfs``'s own comment on this).
    assert label == "TinyAPFSTest (APFS)"


async def test_open_recognizes_apfs_container_without_dissect_volume_at_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without dissect.volume there is no partition-table engine at all
    -- the one shape still reachable is a bare, unpartitioned container
    at offset 0 (this module's own open() docstring). Forces that path
    via monkeypatch rather than actually uninstalling dissect.volume."""
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
    names = {name for name, _is_dir, _size in entries}
    # Real entries from a real, if minimal, macOS-created APFS volume --
    # ".fseventsd" is macOS's own real bookkeeping directory (kept, not
    # filtered: a real directory a real filesystem creates).
    assert {"hello.txt", "subdir", ".fseventsd"} <= names

    hello = next(e for e in entries if e[0] == "hello.txt")
    assert hello[1] is False  # is_dir
    assert hello[2] == len(_HELLO_CONTENT)

    subdir = next(e for e in entries if e[0] == "subdir")
    assert subdir[1] is True


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

    assert _apfs_iterdir(_FakeEntry()) == [("real_file.txt", False, 3)]


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

    assert _apfs_iterdir(_FakeEntry()) == [("real_file.txt", False, None)]


async def test_open_file_reads_the_real_file_content() -> None:
    disk_fs, addr = await _open_gpt_fixture()
    content = await disk_fs.open_file(addr, "/hello.txt")
    assert content.size == len(_HELLO_CONTENT)
    assert await content.read(0, content.size) == _HELLO_CONTENT
    assert await content.read(6, 4) == b"from"


async def test_subdirectory_listing_and_read_of_a_nested_file() -> None:
    disk_fs, addr = await _open_gpt_fixture()
    entries = await disk_fs.list_dir(addr, "/subdir")
    assert {name for name, _is_dir, _size in entries} == {"nested.txt"}

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
