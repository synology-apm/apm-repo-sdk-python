"""``DiskFilesystem`` parses the partition table and filesystem(s)
*inside* a VM/PC/PS disk image via the Dissect framework (Fox-IT/NCC
Group's DFIR toolkit, ``dissect.*`` PyPI packages), so ``units/device.py``
can offer a read-only, per-file browsing/export view alongside the
whole-image ``cat``/``export`` it already supports, wired in as a new,
additional sibling node next to each disk-image leaf — existing
disk-image refs are completely unaffected.

Dissect's packages are pure Python (``py3-none-any`` wheels, no
per-platform native build) and cover partition tables (``dissect.volume``,
auto-detecting MBR/GPT/Apple Partition Map/BSD disklabel) plus
NTFS/ext2-4/XFS/Btrfs/FAT/APFS content (HFS+ and ISO9660 disks show the
same "no filesystem recognized" diagnostic as any other unsupported
format). A disk with no recognized partition table (``Disk(...)`` raises)
falls back to treating the whole image as one filesystem candidate — a
bare, unpartitioned APFS container is the common real-world case. Each
format's own detection/listing/sizing lives in its own sibling module
(``_ntfs``, ``_apfs``, ``_posix_formats`` for ext2/3/4+XFS+Btrfs+FAT);
``_disk_filesystem.py`` holds the shared partition-table/APFS-container
wiring (``DiskFilesystem``), and ``_content_source.py`` holds the two
``ContentSource`` classes every format's own opened file reads through.

Every Dissect filesystem object exposes ``get(path)`` (NTFS via its root
``MftRecord``; ext/FAT/APFS directly on the opened volume object) that
re-resolves an absolute, forward-slash path from scratch — so a node's
own stable identifier is simply that path string, resolved fresh
whenever ``DiskFilesystem.list_dir``/``open_file`` needs it, including a
canonical ref pasted into a brand new process with no prior listing in
this instance.

The whole ``dissect.*`` stack is always installed (a required dependency of
this SDK, not split per format since every package involved is equally
well-packaged — pure-Python universal wheels; ``dissect.apfs``'s one native
dependency, ``pycryptodome`` for FileVault decryption, ships broad prebuilt
wheels of its own) but imported lazily, so importing ``units/device.py``
doesn't pay the real cost of importing seven packages for a caller who
never browses into a disk's filesystem.

Each format also has its own ``content_unavailable`` check for a file
whose real content this SDK cannot produce, rather than genuinely
corrupt: a cloud-sync placeholder with no local data (e.g. an evicted
OneDrive/iCloud file), or (NTFS only) an EFS-encrypted file this SDK
has no key material for.

Every ``dissect.*`` call is synchronous while this SDK is async-native
throughout, so every call here runs inside ``asyncio.to_thread`` — except
``_disk_filesystem._build_bridge``'s stream adapter, whose worker thread
briefly reads back across the loop boundary to fetch real bytes.
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
