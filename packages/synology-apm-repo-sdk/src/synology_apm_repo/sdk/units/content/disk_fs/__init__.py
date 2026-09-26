"""``DiskFilesystem`` parses the partition table and filesystem(s)
inside a VM/PC/PS disk image via the Dissect framework (``dissect.*``
PyPI packages), giving ``units/device.py`` a read-only, per-file
browsing/export view alongside the whole-image view it already
supports — wired in as an additional sibling node; existing disk-image
refs are unaffected.

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

A node's stable identifier is the resolved absolute path string itself —
every Dissect filesystem object re-resolves ``get(path)`` from scratch,
so a canonical ref works even pasted into a brand new process.

``dissect.*`` is a required dependency, imported lazily so importing
``units/device.py`` doesn't pay the cost of importing seven packages for
a caller who never browses into a disk's filesystem. See ``_base.py``'s
``content_unavailable`` field and ``_disk_filesystem._build_bridge`` for
this package's content-availability and async-threading contracts.
"""

from __future__ import annotations

from ._content_source import DissectFileContentSource
from ._disk_filesystem import DiskFilesystem, DiskFilesystemUnavailableError, disk_fs_available
from ._disk_filesystem import _DissectEntry as _DissectEntry
from ._disk_filesystem import _partition_table_label as _partition_table_label

__all__ = [
    "DiskFilesystem",
    "DiskFilesystemUnavailableError",
    "DissectFileContentSource",
    "disk_fs_available",
]
