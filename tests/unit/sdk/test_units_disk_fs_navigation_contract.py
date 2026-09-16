"""Cross-format navigation contract for ``DiskFilesystem``: root's own
listing never contains a self-referential/bookkeeping entry
(``.``/``..``), run identically against ext4, XFS, Btrfs, and NTFS's
real, committed fixtures (the four formats sharing ``tests/unit/sdk/
test_units_disk_fs.py``'s own ``.raw.tar.gz`` + ``_FileBackedAsyncContent``
mechanism — see that file's module docstring for where each fixture came
from and why FAT12 isn't included here: it's hand-built byte-exact rather
than a real filesystem image, a different construction entirely).

Complements, rather than replaces, the dedicated regression tests for
NTFS root's literal ``"."`` self-reference
(``test_ntfs_iterdir_filters_the_roots_self_referential_dot_entry``) and
Btrfs's ``Subvolume``-as-``.parent`` quirk
(``test_btrfs_dot_dot_can_be_a_bare_subvolume_object_not_a_real_inode``/
``test_btrfs_iterdir_skips_a_child_missing_is_dir_even_under_a_non_dot_name``)
— both in ``test_units_disk_fs.py``, both against hand-built fake objects
calling the private ``_ntfs_iterdir``/``_btrfs_iterdir`` functions
directly, no real sample or even a real ``dissect.*`` disk image needed
(the Btrfs one is additionally characterized against this project's own
flat, single-subvolume fixture, proving the underlying library quirk
itself doesn't need real multi-subvolume data to reproduce). What this
file adds on top is the *general* shape both bugs took (a directory
listing that includes a self-referential/bookkeeping entry), checked
through the real, public ``DiskFilesystem.open()``/``list_dir()`` path
against every format this project can exercise offline — so a future
format, or a regression reachable only through that public path rather
than the private per-format functions above, is still caught here.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import os
import shutil
import tarfile
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable
from functools import cache
from pathlib import Path

import pytest

from synology_apm_repo.sdk.units.content.disk_fs import DiskFilesystem

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures"

#: Every not-hand-built format's ``.raw.tar.gz`` fixture — see this
#: file's own module docstring for why FAT12 (hand-built, not a real
#: image) isn't part of this contract.
_TAR_GZ_FIXTURES = [
    "tiny_ext4.raw.tar.gz",
    "tiny_xfs.raw.tar.gz",
    "tiny_btrfs.raw.tar.gz",
    "tiny_ntfs.raw.tar.gz",
]


class _FileBackedAsyncContent:
    """Same shape as ``test_units_disk_fs.py``'s own class of the same
    name — duplicated per this project's "no test module imports from
    another" convention (``tests/CLAUDE.md``), not a divergent copy."""

    supports_concurrent_export = False

    def __init__(self, path: Path, size: int) -> None:
        self._fd = os.open(path, os.O_RDONLY)
        self._size = size

    def __del__(self) -> None:
        with contextlib.suppress(OSError):
            os.close(self._fd)

    @property
    def size(self) -> int | None:
        return self._size

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        n = length if length is not None else self._size - offset
        return await asyncio.to_thread(os.pread, self._fd, n, offset)

    def stream(self, block: int = 8 << 20) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")

    async def export_to(
        self, dst: Path, *, sparse: bool = True, progress: Callable[[int, int], Awaitable[None]] | None = None
    ) -> object:
        raise NotImplementedError("unused by DiskFilesystem.open() — only satisfies the ContentSource Protocol")


@cache
def _load_tar_gz_fixture(name: str) -> tuple[Path, int]:
    """Same mechanism as ``test_units_disk_fs.py``'s own helper of the
    same name — see that file's docstring for why ``.raw.tar.gz``
    (sparse-aware extraction) rather than plain ``.raw.gz``."""
    tmp_dir = Path(tempfile.mkdtemp())
    atexit.register(shutil.rmtree, tmp_dir, ignore_errors=True)
    with tarfile.open(_FIXTURES / name, "r:gz") as tf:
        (member,) = tf.getmembers()
        tf.extract(member, path=tmp_dir, filter="data")
    return tmp_dir / member.name, member.size


@pytest.fixture(params=_TAR_GZ_FIXTURES)
async def disk_fs(request: pytest.FixtureRequest) -> DiskFilesystem:
    fs = await DiskFilesystem.open(_FileBackedAsyncContent(*_load_tar_gz_fixture(request.param)))
    assert fs is not None, f"{request.param} did not open as a recognized filesystem"
    return fs


async def _all_names(fs: DiskFilesystem, partition_addr: int, path: str, *, depth: int) -> list[str]:
    """Every entry name reachable from ``path`` within ``depth`` levels —
    a bounded walk, not a real recursive-listing feature: exists purely so
    this test can check every name it can cheaply reach, not just root's
    own immediate children."""
    entries = await fs.list_dir(partition_addr, path)
    names = [name for name, _is_dir, _size in entries]
    if depth <= 0:
        return names
    for name, is_dir, _size in entries:
        if is_dir:
            child_path = f"{path.rstrip('/')}/{name}"
            names.extend(await _all_names(fs, partition_addr, child_path, depth=depth - 1))
    return names


async def test_root_listing_has_no_self_referential_or_bookkeeping_entry(disk_fs: DiskFilesystem) -> None:
    for partition_addr, _label in disk_fs.partitions():
        entries = await disk_fs.list_dir(partition_addr, "/")
        names = {name for name, _is_dir, _size in entries}
        assert "." not in names, f"partition {partition_addr}: root listed itself ('.') as a child"
        assert ".." not in names, f"partition {partition_addr}: root listed its own parent ('..') as a child"


async def test_no_bookkeeping_entry_at_any_reachable_depth(disk_fs: DiskFilesystem) -> None:
    for partition_addr, _label in disk_fs.partitions():
        names = await _all_names(disk_fs, partition_addr, "/", depth=2)
        assert "." not in names
        assert ".." not in names
