"""``DiskFilesystem`` parses the partition table and filesystem(s)
inside a VM/PC/PS disk image via the Dissect framework (``dissect.*``
PyPI packages), giving ``units/device_disk_fs.py`` a read-only, per-file
browsing/export view as a sibling node of the whole-image leaf.

Covers partition tables (auto-detected MBR/GPT/Apple Partition Map/BSD
disklabel) plus NTFS/ext2-4/XFS/Btrfs/FAT/APFS content (HFS+/ISO9660
show the same "no filesystem recognized" diagnostic as any other
unsupported format). A disk with no recognized partition table (the
common case for a bare, unpartitioned APFS container) falls back to
treating the whole image as one filesystem candidate. Each format's
detection/listing/sizing lives in its own sibling module (``_ntfs``,
``_apfs``, ``_posix_formats``); ``_disk_filesystem.py`` holds the shared
partition-table/APFS-container wiring, ``_content_source.py`` the two
``ContentSource`` classes every format reads through.

A node's stable identifier is its absolute path string: every Dissect
filesystem object resolves ``get(path)`` from scratch, so a canonical ref
works in a new process.

``dissect.*`` is a required dependency, imported lazily so a caller who
never browses into a disk's filesystem doesn't pay for importing seven
packages. See ``_base._Format.content_unavailable`` and
``_disk_filesystem._build_bridge`` for this package's content-availability
and async-threading contracts.
"""

from __future__ import annotations

from ._content_source import DissectFileContentSource
from ._disk_filesystem import DiskFilesystem, DiskFilesystemUnavailableError, disk_fs_available

__all__ = [
    "DiskFilesystem",
    "DiskFilesystemUnavailableError",
    "DissectFileContentSource",
    "disk_fs_available",
]
