"""``DiskFilesystem``: one already-open disk image's own partition table
and filesystem(s)/volume(s), lazily parsed via Dissect — the discovery/
dispatch half of this package's design. ``_DissectEntry`` pairs one
already-open Dissect volume with the ``_Format`` describing how to operate
on it; ``DiskFilesystem.open()`` is the one entry point ``units/device.py``
drives, returning the ``DissectFileContentSource`` (``_content_source.py``)
each resolved file reads through.
"""

from __future__ import annotations

import asyncio
import importlib.util
from collections.abc import Callable
from typing import BinaryIO, Self

from ....errors import ContentUnavailableError
from ...base import ContentSource
from ._apfs import _APFS_FORMAT
from ._base import _DirEntry, _Format
from ._base import _try_import as _try_import
from ._content_source import DissectFileContentSource
from ._ntfs import _NTFS_FORMAT
from ._posix_formats import _BTRFS_FORMAT, _EXTFS_FORMAT, _FAT_FORMAT, _XFS_FORMAT

# The dissect.* packages that together make up this module's own
# "at least one usable engine" check and DiskFilesystemUnavailableError
# fallback -- dissect.volume for the partition-table layer,
# the rest for filesystem content.
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
    """Cheap presence check for a working Dissect install —
    checks the import system's own module registry, never actually
    imports any of them. True if *at least one* of the packages in
    ``_DISSECT_PACKAGES`` is present; callers use this to decide whether
    to even offer the "(filesystem)" sibling node at all."""
    return any(importlib.util.find_spec(name) is not None for name in _DISSECT_PACKAGES)


class DiskFilesystemUnavailableError(Exception):
    """Raised by ``DiskFilesystem.open`` when none of this package's
    ``dissect.*`` dependencies are importable in the current environment.
    Deliberately not ``UnsupportedDataFormatError`` (that means "this
    repository's own data doesn't support this" — here the repository
    data may be perfectly fine, it's this environment's own ``dissect.*``
    install that's broken or missing)."""


# Tried in this order at every candidate offset that isn't an APFS
# container (see DiskFilesystem._try_open_apfs) -- order doesn't affect
# correctness (formats are mutually exclusive by construction) but
# cheaper/more common formats first avoids a little wasted work.
_FLAT_FORMATS = (_NTFS_FORMAT, _EXTFS_FORMAT, _XFS_FORMAT, _BTRFS_FORMAT, _FAT_FORMAT)

#: The one candidate a disk with no recognized partition table falls
#: back to -- a real, observed shape (a bare volume/container image with
#: no partition table wrapping it), pulled out as a constant since
#: ``_flat_format_label``/``_try_open_apfs`` both need to recognize it,
#: not just construct it.
_WHOLE_IMAGE_LABEL = "(whole image)"


def _partition_table_label(part: object) -> str:
    """The partition-*table*-level candidate label for one
    ``dissect.volume`` partition -- its own real name if the table
    records one (Windows almost never sets this for a GPT/MBR partition
    it creates; drive letters live in the OS/registry layer above,
    invisible to a raw disk image), else ``dissect.volume``'s own
    human-readable partition *type* name (covers GPT's well-known type
    GUIDs -- e.g. "EFI System partition", "Windows Basic data
    partition" -- and MBR's numeric type byte alike), else the raw type
    value verbatim when even that lookup doesn't recognize it."""
    name = getattr(part, "name", "") or ""
    if name:
        return name
    type_name = getattr(part, "type_name", None)
    if type_name and type_name != "Unknown":
        return type_name  # type: ignore[no-any-return]
    # getattr-guarded like every other access in this function: a
    # partition object with no readable `.type` at all (some future
    # dissect.volume partition kind) still yields a label instead of
    # raising out of DiskFilesystem.open()'s to_thread call.
    return str(getattr(part, "type", "Unknown"))


def _flat_format_label(table_label: str, fmt: _Format, volume: object) -> str:
    """The final displayed label for one opened flat-format volume:
    ``table_label`` (``_partition_table_label``, or
    ``_WHOLE_IMAGE_LABEL``) plus the filesystem's own volume label when
    it set one. ``table_label`` is dropped entirely rather than shown as
    a redundant prefix when it's just ``_WHOLE_IMAGE_LABEL`` and a real
    volume label is available to say something more specific in its
    place."""
    volume_label = fmt.volume_label(volume)
    if not volume_label:
        return f"{table_label} ({fmt.label})"
    if table_label == _WHOLE_IMAGE_LABEL:
        return f"{volume_label} ({fmt.label})"
    return f"{table_label} - {volume_label} ({fmt.label})"


def _build_bridge(content: ContentSource, loop: asyncio.AbstractEventLoop) -> BinaryIO:
    """Builds a ``dissect.util.stream.AlignedStream`` subclass backed by
    this project's own async ``ContentSource`` — only needs to override
    ``_read(offset, length)``, since ``AlignedStream`` itself already
    implements ``seek``/``tell``/buffering. Only ever called on, and only
    ever called back into from, the worker thread ``asyncio.to_thread``
    runs it on — ``_read`` bounces the
    *real* read back onto ``loop`` (the event loop the disk's own
    ``ContentSource`` actually lives on) via
    ``run_coroutine_threadsafe(...).result()``, blocking this worker
    thread — never the real event loop — until it resolves."""
    stream_mod = _try_import("dissect.util.stream")
    assert stream_mod is not None  # caller (open()) already confirmed at least one dissect package imports

    class _Bridge(stream_mod.AlignedStream):  # type: ignore[name-defined,misc]
        def __init__(self) -> None:
            super().__init__(size=content.size or 0)

        def _read(self, offset: int, length: int) -> bytes:
            future = asyncio.run_coroutine_threadsafe(content.read(offset, length), loop)
            return future.result()

    return _Bridge()


class _DissectEntry:
    """One already-open Dissect volume (a flat NTFS/ext/FAT filesystem,
    or a single unwrapped APFS volume — obtained via
    ``DiskFilesystem._try_open_apfs``'s ``container.volumes`` unwrapping
    rather than ``_Format.open``, since an APFS container is a two-level
    container/volume shape the other formats don't have), paired with the ``_Format`` describing how to operate on
    it. Every method here is synchronous — only ever called from a
    worker thread via ``asyncio.to_thread``."""

    def __init__(self, fmt: _Format, volume: object) -> None:
        self._fmt = fmt
        self._volume = volume

    def list_dir(self, path: str) -> list[_DirEntry]:
        entry = self._fmt.resolve(self._volume, path)
        # Directories before files, then by name — the guest filesystem's
        # own on-disk directory order (NTFS/FAT/ext/XFS/Btrfs/APFS all
        # differ) isn't otherwise meaningful. The same policy
        # units/base.py's dir_first_sort_key states and units/fs.py's own
        # ORDER BY expresses; duplicated here (not imported) since
        # units/content/ sits below the Unit Layer that owns that helper
        # (see ARCHITECTURE.md).
        return sorted(self._fmt.iterdir(entry), key=lambda e: (0 if e.is_dir else 1, e.name))

    def open_file(self, path: str) -> DissectFileContentSource:
        entry = self._fmt.resolve(self._volume, path)
        reason = self._fmt.content_unavailable(entry)
        if reason is not None:
            raise ContentUnavailableError(reason)
        size = self._fmt.size(entry)
        return DissectFileContentSource(entry, size)


class DiskFilesystem:
    """One already-open disk image's own partition table and
    filesystem(s)/volume(s), lazily parsed. Build with ``open``, never
    the constructor directly.

    A disk image can (and typically does) contain more than one
    partition/volume (a Windows "System Reserved" boot partition
    alongside the real NTFS volume, for instance) — every one Dissect
    can open is kept, not just the first/largest, matching what a real
    disk-mount tool would show. A partition/volume Dissect can't open
    (unsupported format, encrypted without a key, or genuinely not a filesystem at
    all) is silently skipped, not reported as an error — this mirrors
    the project's existing principle of resolving real state lazily and
    only reporting what was actually found, not guessing at why
    something wasn't.

    Not a ``UnitProvider`` — like ``VirtualDiskContentSource``, it is a
    disk-content helper ``DeviceProvider`` drives directly, building its
    own ``Node``/``RestorableUnit`` objects from the plain tuples this
    class returns. ``device.py`` only ever talks to this class's three
    public methods (``partitions``, ``list_dir``, ``open_file``)."""

    def __init__(self, content: ContentSource, loop: asyncio.AbstractEventLoop) -> None:
        self._content = content
        self._loop = loop
        # partition_addr -> _DissectEntry, resolved once in open().
        self._filesystems: dict[int, _DissectEntry] = {}
        self._partition_labels: dict[int, str] = {}

    @classmethod
    async def open(cls, content: ContentSource) -> Self | None:
        """Returns ``None`` when no partition/volume on this disk yields
        anything Dissect can open at all (encrypted, unsupported, or
        genuinely not a partitioned/filesystem-bearing image) — callers
        show a diagnostic node for that case, the same shape
        ``units/device_pcps.py``'s own ``_diagnostic_node`` already uses
        for other "resolvable in principle, nothing found" cases.

        Raises:
            DiskFilesystemUnavailableError: None of this package's
                ``dissect.*`` dependencies are importable — call
                ``disk_fs_available`` first to avoid this in the common
                case (nothing installed); this path exists for the
                rarer "``find_spec`` found something, but the real
                import still fails" case (a broken/partial install).
        """
        if not disk_fs_available():
            raise DiskFilesystemUnavailableError(
                "no dissect.* filesystem-parsing package could be imported in this "
                "environment (a broken or partial install) - disk filesystem browsing "
                "is unavailable"
            )

        loop = asyncio.get_running_loop()
        self = cls(content, loop)

        def _blocking_open() -> None:
            # (label, fh_factory) candidates -- fh_factory is a zero-arg
            # callable returning a *fresh*, independently-positioned
            # stream each call, since multiple formats may each need
            # their own clean read from offset 0 of the same candidate.
            candidates: list[tuple[str, Callable[[], BinaryIO]]] = []
            volume_mod = _try_import("dissect.volume.disk.disk")
            if volume_mod is not None:
                try:
                    disk = volume_mod.Disk(_build_bridge(self._content, self._loop))  # type: ignore[attr-defined]
                except Exception:
                    disk = None
                if disk is not None and disk.partitions:
                    candidates.extend((_partition_table_label(part), part.open) for part in disk.partitions)
            if not candidates:
                # No partition table recognized at all (or dissect.volume
                # itself isn't installed) -- a real, observed shape: a
                # bare volume/container image with no partition table
                # wrapping it.
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
                    except Exception:
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
        """Tries opening this candidate offset as an APFS container.

        APFS is a *container* holding zero or more independently-openable
        *volumes* (a modern macOS disk's own ``Macintosh HD``/``Data``/
        ``Preboot``/``Recovery`` split), unlike the flat, one-volume-per-
        partition NTFS/ext2-4/XFS/FAT/Btrfs formats handled elsewhere in
        this package. This method unwraps a container into one synthetic
        partition entry per browsable volume (including a sealed/signed
        system volume); each unwrapped volume then plugs into the same
        generic ``_Format``-driven machinery the flat formats use, since
        its ``get(path)``/``.name``/``.is_dir()``/``.open()`` shape
        matches closely enough to share one adapter table entry
        (``_apfs._APFS_FORMAT``). Every candidate byte offset tries APFS
        first, then ``_FLAT_FORMATS`` in order, first match wins —
        formats are mutually exclusive by construction, so trying more
        than one is only ever wasted, cheap I/O, never wrong.

        Returns:
            ``(claimed, minted)``: ``claimed`` is whether this candidate
                offset was recognized as an APFS container at all
                (regardless of how many of its volumes turned out to be
                browsable — the caller skips trying the flat formats at
                the same offset either way, since a real APFS container
                was already found there); ``minted`` is how many
                partition_addr slots were actually minted (``0`` if the
                container opened but no volume was browsable).
        """
        try:
            container = apfs_mod.APFS(fh_factory())  # type: ignore[attr-defined]
        except Exception:
            return False, 0

        addr = addr_base
        for i, volume in enumerate(container.volumes):
            try:
                volume.get("/")
            except Exception:
                # A sealed/signed system volume without a decodable root,
                # or a FileVault-locked one without a key -- real,
                # observed, not a bug. Nothing to browse; skipped exactly
                # like an unopenable flat-format candidate is skipped.
                continue
            name = volume.name or f"APFS volume {i}"
            self._filesystems[addr] = _DissectEntry(_APFS_FORMAT, volume)
            # ``label`` is dropped entirely rather than shown as a redundant
            # "(whole image) - " prefix when it's just the whole-image
            # fallback (see ``_flat_format_label``'s own identical rule) --
            # a bare APFS container with no partition table is the
            # common, real shape (a modern macOS disk), and the
            # container's own per-volume name already says more.
            self._partition_labels[addr] = (
                f"{name} (APFS)" if label == _WHOLE_IMAGE_LABEL else f"{label} - {name} (APFS)"
            )
            addr += 1
        return True, addr - addr_base

    def partitions(self) -> list[tuple[int, str]]:
        """``(partition_addr, human_label)`` for every filesystem/volume
        successfully opened — stable ordering (insertion order:
        ``dissect.volume``'s own partition-table order, each APFS
        container's volumes in their own container order)."""
        return list(self._partition_labels.items())

    async def list_dir(self, partition_addr: int, path: str) -> list[_DirEntry]:
        """One ``_DirEntry`` per directory's real entries — ``path="/"``
        means the partition/volume's own filesystem root. ``path`` is
        resolved fresh via the underlying format's own ``get(path)`` every
        call (paths, not inode numbers, are the stable identifier here) —
        dot/dot-dot and each
        format's own synthetic bookkeeping entries are filtered out by
        that format's own ``_Format.iterdir``, never surfaced as browsable
        content. ``file_state`` is always ``FileState.NORMAL`` for a
        directory, or for a format with no cloud-sync/encryption concept.
        ``mtime`` is ``None`` when a format/entry has no reliable value."""
        entry = self._filesystems[partition_addr]
        return await asyncio.to_thread(entry.list_dir, path)

    async def open_file(self, partition_addr: int, path: str) -> DissectFileContentSource:
        """Opens ``path`` on the filesystem mounted at ``partition_addr``.

        Raises:
            ContentUnavailableError: this file is a cloud-sync placeholder
                with no local data, or (NTFS only) is EFS-encrypted with no
                key material available.
        """
        entry = self._filesystems[partition_addr]
        return await asyncio.to_thread(entry.open_file, path)
