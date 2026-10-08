"""``DiskFilesystem``: one disk image's partition table and
filesystem(s)/volume(s), parsed via Dissect. ``_DissectEntry`` pairs one
open Dissect volume with the ``_Format`` that operates on it.
"""

from __future__ import annotations

import asyncio
import importlib.util
from collections.abc import Callable
from typing import BinaryIO, Self

from ....errors import ContentUnavailableError
from ....units.provider_kit import dir_first_sort_key
from ...base import ContentSource
from ._apfs import _APFS_FORMAT
from ._base import _DirEntry, _Format
from ._base import _try_import as _try_import
from ._content_source import DissectFileContentSource
from ._ntfs import _NTFS_FORMAT
from ._posix_formats import _BTRFS_FORMAT, _EXTFS_FORMAT, _FAT_FORMAT, _XFS_FORMAT

# dissect.volume parses partition tables; the rest parse filesystems.
# Any one present counts as available (disk_fs_available).
_DISSECT_PACKAGES = (
    "dissect.volume",
    "dissect.ntfs",
    "dissect.extfs",
    "dissect.xfs",
    "dissect.btrfs",
    "dissect.fat",
    "dissect.apfs",
)


def disk_fs_available() -> bool:
    """Whether any Dissect package is installed (found without importing
    it) — decides whether a "(filesystem)" sibling node is offered."""
    return any(importlib.util.find_spec(name) is not None for name in _DISSECT_PACKAGES)


class DiskFilesystemUnavailableError(Exception):
    """Raised by ``DiskFilesystem.open`` when no ``dissect.*`` package is
    installed. An environment problem, not a data one, hence not an
    ``UnsupportedDataFormatError``."""


# Tried in order at every candidate that isn't an APFS container; the
# formats are mutually exclusive, so order only affects cost.
_FLAT_FORMATS = (_NTFS_FORMAT, _EXTFS_FORMAT, _XFS_FORMAT, _BTRFS_FORMAT, _FAT_FORMAT)

#: The single candidate of a disk with no recognized partition table (a
#: bare volume/container image).
_WHOLE_IMAGE_LABEL = "(whole image)"


def _partition_table_label(part: object) -> str:
    """A ``dissect.volume`` partition's label: its name if the table
    records one, else its partition type's name (e.g. "EFI System
    partition"), else the raw type value."""
    name = getattr(part, "name", "") or ""
    if name:
        return name
    type_name = getattr(part, "type_name", None)
    if type_name and type_name != "Unknown":
        return type_name  # type: ignore[no-any-return]
    return str(getattr(part, "type", "Unknown"))


def _flat_format_label(table_label: str, fmt: _Format, volume: object) -> str:
    """The displayed label for one opened flat-format volume:
    ``table_label`` plus the filesystem's volume label when set, with a
    ``_WHOLE_IMAGE_LABEL`` prefix dropped in favor of the volume label."""
    volume_label = fmt.volume_label(volume)
    if not volume_label:
        return f"{table_label} ({fmt.label})"
    if table_label == _WHOLE_IMAGE_LABEL:
        return f"{volume_label} ({fmt.label})"
    return f"{table_label} - {volume_label} ({fmt.label})"


def _build_bridge(content: ContentSource, loop: asyncio.AbstractEventLoop) -> BinaryIO:
    """A blocking ``dissect.util.stream.AlignedStream`` over the async
    ``content``. Must be read from a worker thread, never from ``loop``
    itself: each read runs on ``loop`` and blocks the calling thread until
    it completes."""
    stream_mod = _try_import("dissect.util.stream")
    assert stream_mod is not None  # caller (open()) already confirmed a dissect package is installed

    class _Bridge(stream_mod.AlignedStream):  # type: ignore[name-defined,misc]
        def __init__(self) -> None:
            super().__init__(size=content.size or 0)

        def _read(self, offset: int, length: int) -> bytes:
            future = asyncio.run_coroutine_threadsafe(content.read(offset, length), loop)
            # Dissect is typed for, and may cache or hash, bytes.
            return bytes(future.result())

    return _Bridge()


class _DissectEntry:
    """One open Dissect volume (a flat-format filesystem, or one APFS
    volume unwrapped from its container) paired with the ``_Format`` that
    operates on it. Every method is blocking and runs in a worker
    thread."""

    def __init__(self, fmt: _Format, volume: object) -> None:
        self._fmt = fmt
        self._volume = volume

    def list_dir(self, path: str) -> list[_DirEntry]:
        entry = self._fmt.resolve(self._volume, path)
        # On-disk directory order differs per format and isn't meaningful.
        return sorted(self._fmt.iterdir(entry), key=lambda e: dir_first_sort_key(e.is_dir, e.name))

    def open_file(self, path: str) -> DissectFileContentSource:
        entry = self._fmt.resolve(self._volume, path)
        reason = self._fmt.content_unavailable(entry)
        if reason is not None:
            raise ContentUnavailableError(reason)
        size = self._fmt.size(entry)
        return DissectFileContentSource(entry, size)


class DiskFilesystem:
    """One disk image's partition table and filesystem(s)/volume(s). Build
    with ``open``.

    Every partition/volume Dissect can open is kept; one it can't
    (unsupported, encrypted without a key, not a filesystem) is skipped
    silently. Not a ``UnitProvider``: ``units/device_disk_fs.py`` builds
    nodes from ``partitions``/``list_dir``/``open_file``."""

    def __init__(self, content: ContentSource, loop: asyncio.AbstractEventLoop) -> None:
        self._content = content
        self._loop = loop
        # partition_addr -> _DissectEntry, resolved once in open().
        self._filesystems: dict[int, _DissectEntry] = {}
        self._partition_labels: dict[int, str] = {}

    @classmethod
    async def open(cls, content: ContentSource) -> Self | None:
        """Parse ``content``'s partitions and filesystems.

        Returns:
            The parsed disk, or ``None`` when no partition/volume yields
            anything Dissect can open.

        Raises:
            DiskFilesystemUnavailableError: No ``dissect.*`` package is
                installed (check ``disk_fs_available`` first).
        """
        if not disk_fs_available():
            raise DiskFilesystemUnavailableError(
                "no dissect.* filesystem-parsing package is installed in this "
                "environment - disk filesystem browsing is unavailable"
            )

        loop = asyncio.get_running_loop()
        self = cls(content, loop)

        def _blocking_open() -> None:
            # (label, fh_factory): each call returns a fresh stream, since
            # every format tried needs its own clean read from offset 0.
            candidates: list[tuple[str, Callable[[], BinaryIO]]] = []
            volume_mod = _try_import("dissect.volume.disk.disk")
            if volume_mod is not None:
                try:
                    disk = volume_mod.Disk(_build_bridge(self._content, self._loop))  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001
                    disk = None
                if disk is not None and disk.partitions:
                    candidates.extend((_partition_table_label(part), part.open) for part in disk.partitions)
            if not candidates:
                # No partition table recognized, or dissect.volume missing.
                candidates = [(_WHOLE_IMAGE_LABEL, lambda: _build_bridge(self._content, self._loop))]

            apfs_mod = _try_import("dissect.apfs.apfs")

            next_addr = 0
            for label, fh_factory in candidates:
                if apfs_mod is not None:
                    claimed, minted = self._try_open_apfs(apfs_mod, fh_factory, label, next_addr)
                    if claimed:
                        next_addr += minted
                        continue
                for fmt in _FLAT_FORMATS:
                    try:
                        volume = fmt.open(fh_factory())
                    except Exception:  # noqa: BLE001
                        continue
                    self._filesystems[next_addr] = _DissectEntry(fmt, volume)
                    self._partition_labels[next_addr] = _flat_format_label(label, fmt, volume)
                    next_addr += 1
                    break

        await asyncio.to_thread(_blocking_open)
        return self if self._filesystems else None

    def _try_open_apfs(
        self, apfs_mod: object, fh_factory: Callable[[], BinaryIO], label: str, addr_base: int
    ) -> tuple[bool, int]:
        """Try this candidate as an APFS container, registering one
        partition entry (driven by ``_apfs._APFS_FORMAT``) per browsable
        volume.

        Returns:
            ``(claimed, minted)``: ``claimed`` is whether the candidate is
                an APFS container at all (the caller then skips the flat
                formats); ``minted`` is how many partition addresses were
                registered (``0`` if no volume was browsable).
        """
        try:
            container = apfs_mod.APFS(fh_factory())  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            return False, 0

        addr = addr_base
        for i, volume in enumerate(container.volumes):
            try:
                volume.get("/")
            except Exception:  # noqa: BLE001
                # A sealed system volume without a decodable root, or a
                # FileVault-locked one: nothing to browse.
                continue
            name = volume.name or f"APFS volume {i}"
            self._filesystems[addr] = _DissectEntry(_APFS_FORMAT, volume)
            # Same whole-image prefix rule as _flat_format_label.
            self._partition_labels[addr] = (
                f"{name} (APFS)" if label == _WHOLE_IMAGE_LABEL else f"{label} - {name} (APFS)"
            )
            addr += 1
        return True, addr - addr_base

    def partitions(self) -> list[tuple[int, str]]:
        """``(partition_addr, label)`` for every opened filesystem/volume,
        in partition-table order (an APFS container's volumes in container
        order)."""
        return list(self._partition_labels.items())

    async def list_dir(self, partition_addr: int, path: str) -> list[_DirEntry]:
        """The entries of directory ``path`` (``"/"`` is the volume root),
        containers first then by name, without ``.``/``..`` or format
        bookkeeping entries. ``file_state`` is ``FileState.NORMAL`` for a
        directory or a format without that concept; ``mtime`` is ``None``
        when there's no reliable value."""
        entry = self._filesystems[partition_addr]
        return await asyncio.to_thread(entry.list_dir, path)

    async def open_file(self, partition_addr: int, path: str) -> DissectFileContentSource:
        """Opens ``path`` on the filesystem mounted at ``partition_addr``.

        Raises:
            ContentUnavailableError: The file is a cloud-sync placeholder
                with no local data, or (NTFS only) is EFS-encrypted with no
                key material available.
        """
        entry = self._filesystems[partition_addr]
        return await asyncio.to_thread(entry.open_file, path)
