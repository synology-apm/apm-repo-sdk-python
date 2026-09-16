"""Shared ``Node.attrs["_kind"]`` vocabulary for the VM/PC/PS device axis
— ``units/device.py`` (VM/FS), ``units/device_pcps.py`` (PC/PS listing),
and ``units/device_disk_fs.py`` (the "(filesystem)" sibling axis) each
tag their own nodes with one of these values, and ``DeviceProvider``'s
``children()``/``unit()`` dispatch on them across all three. One shared
enum here, rather than each module defining its own private string
constants, means an unrecognized tag is a ``mypy``-checked ``match`` case
instead of a silently-swallowed ``if``/``elif`` fallthrough.
"""

from __future__ import annotations

import enum


class _NodeKind(enum.Enum):
    ROOT = "root"
    DEVICE = "device"
    OBJECT = "object"
    PCPS_ROOT = "pcps_root"
    PCPS_DISK = "pcps_disk"
    PCPS_DIAGNOSTIC = "pcps_diagnostic"
    DISK_FS_ROOT = "disk_fs_root"
    DISK_FS_ENTRY = "disk_fs_entry"
    DISK_FS_DIAGNOSTIC = "disk_fs_diagnostic"
