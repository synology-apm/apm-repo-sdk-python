"""Cross-format navigation contract for ``DiskFilesystem``: a listing through
the public ``open()``/``list_dir()`` path never contains ``.``/``..``, checked
against the ext4, XFS, Btrfs, and NTFS image fixtures (built as described in
``test_units_content_disk_fs.py``).
"""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.units.content.disk_fs import DiskFilesystem
from unit.sdk.disk_fs_fakes import FileBackedContent, extracted_image

#: One image per filesystem; FAT12 has no image (it is hand-built).
_TAR_GZ_FIXTURES = [
    "tiny_ext4.raw.tar.gz",
    "tiny_xfs.raw.tar.gz",
    "tiny_btrfs.raw.tar.gz",
    "tiny_ntfs.raw.tar.gz",
]


@pytest.fixture(params=_TAR_GZ_FIXTURES)
async def disk_fs(request: pytest.FixtureRequest) -> DiskFilesystem:
    fs = await DiskFilesystem.open(FileBackedContent(*extracted_image(request.param)))
    assert fs is not None, f"{request.param} did not open as a recognized filesystem"
    return fs


async def _all_names(fs: DiskFilesystem, partition_addr: int, path: str, *, depth: int) -> list[str]:
    """Every entry name reachable from ``path`` within ``depth`` levels."""
    entries = await fs.list_dir(partition_addr, path)
    names = [e.name for e in entries]
    if depth <= 0:
        return names
    for entry in entries:
        if entry.is_dir:
            child_path = f"{path.rstrip('/')}/{entry.name}"
            names.extend(await _all_names(fs, partition_addr, child_path, depth=depth - 1))
    return names


async def test_root_listing_has_no_self_referential_or_bookkeeping_entry(disk_fs: DiskFilesystem) -> None:
    for partition_addr, _label in disk_fs.partitions():
        entries = await disk_fs.list_dir(partition_addr, "/")
        names = {e.name for e in entries}
        assert "." not in names, f"partition {partition_addr}: root listed itself ('.') as a child"
        assert ".." not in names, f"partition {partition_addr}: root listed its own parent ('..') as a child"


async def test_no_bookkeeping_entry_at_any_reachable_depth(disk_fs: DiskFilesystem) -> None:
    for partition_addr, _label in disk_fs.partitions():
        names = await _all_names(disk_fs, partition_addr, "/", depth=2)
        assert "." not in names
        assert ".." not in names
