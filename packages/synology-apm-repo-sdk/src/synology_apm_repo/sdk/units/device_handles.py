"""``Node.handle`` types of the VM/PC/PS device axis. ``units/device.py``
(VM), ``units/device_pcps.py`` (PC/PS listing) and
``units/device_disk_fs.py`` (the "(filesystem)" sibling axis) each build
nodes carrying one, and ``DeviceProvider``'s ``children()``/``unit()``
dispatch on its class.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import Node


@dataclasses.dataclass(frozen=True, slots=True)
class DeviceRoot:
    """A VM version's "Devices" root."""


@dataclasses.dataclass(frozen=True, slots=True)
class PcpsRoot:
    """A PC/PS version's "Disks" root."""


@dataclasses.dataclass(frozen=True, slots=True)
class Device:
    """One VM device, listing its objects."""

    config_device_id: int


@dataclasses.dataclass(frozen=True, slots=True)
class VmObject:
    """One VM object: a dedup disk image (``dedup_object``) or a plain
    sidecar file under the version's ``copy_meta_file`` directory."""

    object_id: int
    data_format: int | None
    file_path: str
    src_file_path: str
    dedup_object: bool
    unsupported: bool
    """A dedup object in a data format this SDK refuses (a CBT chain)."""


@dataclasses.dataclass(frozen=True, slots=True)
class PcpsFragment:
    """One registered fragment of a PC/PS disk; ``file_size`` is ``None``
    when ``file_meta.file_size`` is NULL."""

    fid: int
    src_file_path: str
    file_size: int | None


@dataclasses.dataclass(frozen=True, slots=True)
class PcpsDisk:
    """One PC/PS disk, assembled from its fragments when opened."""

    disk_uuid: str
    disk_index: str
    fragments: tuple[PcpsFragment, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class PcpsDiagnostic:
    """The placeholder for registered fragments missing from ``file_meta``."""

    missing_fids: tuple[int, ...]


DiskKey = tuple[str, int] | tuple[str, str, str]
"""Identifies one disk for its filesystem cache — ``("object", object_id)``
for a VM disk, ``("pcps", disk_uuid, disk_index)`` for a PC/PS one."""


@dataclasses.dataclass(frozen=True, slots=True)
class DiskFsRoot:
    """A disk image's "(filesystem)" sibling. ``source_node`` is the disk
    image's own node, opened through ``DeviceProvider.unit`` to parse it;
    every node below carries it forward, so a canonical ref resolved in a
    fresh provider can still reach the disk."""

    disk_key: DiskKey
    source_node: Node


@dataclasses.dataclass(frozen=True, slots=True)
class DiskFsEntry:
    """A partition, directory or file inside a disk image's filesystem."""

    disk_key: DiskKey
    source_node: Node
    partition_addr: int
    path: str


@dataclasses.dataclass(frozen=True, slots=True)
class DiskFsDiagnostic:
    """The placeholder for a disk on which no filesystem was recognized.
    ``raw_reason`` is the underlying error (possibly an internal path),
    kept out of user-facing text."""

    raw_reason: str | None
