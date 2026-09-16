"""``DiskFilesystem`` parses the partition table and filesystem(s)
*inside* a VM/PC/PS disk image via the Dissect framework (Fox-IT/NCC
Group's DFIR toolkit, ``dissect.*`` PyPI packages), so ``units/device.py``
can offer a read-only, per-file browsing/export view alongside the
whole-image ``cat``/``export`` it already supports — see that module's
own docstring for how the two are wired together (a new, additional
sibling node next to each disk-image leaf; existing disk-image refs are
completely unaffected).

Dissect's packages are pure Python (``py3-none-any`` wheels, no
per-platform native build) and cover partition tables (``dissect.volume``,
auto-detecting MBR/GPT/Apple Partition Map/BSD disklabel) plus
NTFS/ext2-4/XFS/Btrfs/FAT/APFS content (HFS+ and ISO9660 disks show the
same "no filesystem recognized" diagnostic as any other unsupported
format). A disk with no recognized partition table (``Disk(...)`` raises)
falls back to treating the whole image as one filesystem candidate — a
bare, unpartitioned APFS container is the common real-world case.

Every Dissect filesystem object exposes ``get(path)`` (NTFS via its root
``MftRecord``; ext/FAT/APFS directly on the opened volume object) that
re-resolves an absolute, forward-slash path from scratch — so a node's
own stable identifier is simply that path string, resolved fresh
whenever ``DiskFilesystem.list_dir``/``open_file`` needs it, including a
canonical ref pasted into a brand new process with no prior listing in
this instance. Every Dissect filesystem entry's own ``.open()`` already
resolves a fragmented file's runlist internally, so there is no manual
multi-extent stitching here, unlike ``VirtualDiskContentSource``.

The whole ``dissect.*`` stack is always installed (a required dependency of
this SDK, not split per format since every package involved is equally
well-packaged — pure-Python universal wheels; ``dissect.apfs``'s one native
dependency, ``pycryptodome`` for FileVault decryption, ships broad prebuilt
wheels of its own) but imported lazily, so importing ``units/device.py``
doesn't pay the real cost of importing seven packages for a caller who
never browses into a disk's filesystem — see ``disk_fs_available`` and
``DiskFilesystem.open`` for how availability is checked and reported.

Every ``dissect.*`` call is synchronous while this SDK is async-native
throughout, so every call here runs inside ``asyncio.to_thread`` — see
``_build_bridge``'s own docstring for the one place that worker thread
needs to read real bytes back across the loop boundary.
"""

from __future__ import annotations

import asyncio
import dataclasses
import importlib
import importlib.util
import os
import stat
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import BinaryIO, Self

from ...dedup.dedup_file import (
    DEFAULT_STREAM_BLOCK,
    ExportResult,
    clamp_read_length,
    stream_via_read,
)
from ...errors import DataCorruptError
from ..base import ContentSource

# Pin dissect.util's read-alignment granularity so it isn't affected by
# whichever Python interpreter's own io.DEFAULT_BUFFER_SIZE happens to be
# in effect (Python 3.14 changed that stdlib default from 8192 to
# 131072). Must run before dissect.util.stream is ever imported, which
# only happens lazily inside this module's own _try_import calls below.
# 8192 is deliberate, not just inherited: a read below this size already
# rounds up to a full aligned block regardless of the value chosen,
# while a sequential read at or above it already collapses into one
# underlying call no matter how large the alignment is
# (AlignedStream.read()'s own divmod-based batching) — so a larger
# alignment only adds wasted bytes on the many small/scattered reads
# real filesystem browsing does, with no offsetting benefit.
os.environ.setdefault("DISSECT_STREAM_BUFFER_SIZE", "8192")

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
    to even offer the "(filesystem)" sibling node at all — see
    ``DiskFilesystem.open`` for what happens after."""
    return any(importlib.util.find_spec(name) is not None for name in _DISSECT_PACKAGES)


class DiskFilesystemUnavailableError(Exception):
    """Raised by ``DiskFilesystem.open`` — see its docstring for when.
    Deliberately not ``UnsupportedDataFormatError`` (that means "this
    repository's own data doesn't support this" — here the repository
    data may be perfectly fine, it's this environment's own ``dissect.*``
    install that's broken or missing)."""


def _try_import(module_path: str) -> object | None:
    try:
        return importlib.import_module(module_path)
    except ImportError:
        return None


@dataclasses.dataclass(frozen=True)
class _Format:
    """Describes one flat, single-volume-per-offset Dissect filesystem
    format — new formats (e.g. HFS+ or ISO9660, if this project ever needs
    them) are a new entry here, not a new class. Each format's own callables are not
    assumed identical across formats just from matching method names —
    the shapes are close but not identical (see each format's own
    construction below)."""

    label: str
    open: Callable[[BinaryIO], object]
    """``fh -> volume``. Raises on a byte range that isn't this format."""
    resolve: Callable[[object, str], object]
    """``(volume, absolute_path) -> entry``. ``"/"`` is always the root."""
    iterdir: Callable[[object], list[tuple[str, bool, int | None]]]
    """``entry -> [(name, is_dir, size)]`` for one directory's real children."""
    size: Callable[[object], int | None]
    """``entry -> size`` for a resolved file entry."""
    volume_label: Callable[[object], str | None]
    """``volume -> label``, the filesystem's own human-assigned volume
    name if it set one (``None`` if absent/empty) — distinct from the
    partition *table*'s own name/type (``_partition_table_label``),
    which a caller sees regardless of whether Dissect can even open this
    offset as this format at all."""


def _module_open(module_path: str, class_name: str) -> Callable[[BinaryIO], object]:
    """Builds a ``_Format.open`` callable that lazily imports
    ``module_path`` and constructs ``class_name`` from it — the one
    shape every format's own ``open`` needs (a fresh handle per call,
    raising whenever the byte range isn't this format)."""

    def open_fn(fh: BinaryIO) -> object:
        module = _try_import(module_path)
        assert module is not None
        return getattr(module, class_name)(fh)

    return open_fn


def _default_resolve(volume: object, path: str) -> object:
    """``_Format.resolve`` for every format whose opened volume object
    resolves an absolute path via a plain ``get(path)`` call — every
    flat format except NTFS (``_ntfs_resolve``), which has no such
    method on its own root."""
    return volume.get(path or "/")  # type: ignore[attr-defined]


def _default_size(entry: object) -> int | None:
    """``_Format.size`` for every format whose resolved entry exposes a
    plain ``.size`` attribute — every flat format except NTFS
    (``_ntfs_size``), which needs its own missing-``$DATA``-stream
    fallback. Falls back to ``None`` the same defensive way if reading
    ``.size`` raises on any other format's own entry."""
    try:
        return entry.size  # type: ignore[attr-defined,no-any-return]
    except Exception:
        return None


def _ntfs_resolve(volume: object, path: str) -> object:
    root = volume.mft.get(5)  # type: ignore[attr-defined]  # MFT record 5 is always the NTFS root directory
    return root if path in ("", "/") else root.get(path.lstrip("/"))


def _ntfs_size(entry: object) -> int | None:
    try:
        return entry.size()  # type: ignore[attr-defined,no-any-return]
    except Exception:
        # A real NTFS system metadata file (e.g. $Secure, MFT record 9)
        # can have no unnamed $DATA stream at all -- MftRecord.size()
        # looks for exactly that stream and raises FileNotFoundError
        # when it's missing. Same "report what's observed, don't crash"
        # posture as everywhere else in this module.
        return None


def _ntfs_iterdir(entry: object) -> list[tuple[str, bool, int | None]]:
    # dereference=False reads each child's own embedded $FILE_NAME
    # index-entry attribute directly instead of resolving every child to
    # its own full MftRecord: NTFS's own $I30 index entries already
    # carry a copy of the $FILE_NAME attribute (name/is_dir/size)
    # inline, an order-of-magnitude cheaper listing than the
    # dereferenced path. open_file() (below) still needs the full
    # dereferenced MftRecord to actually read $DATA, so this is
    # listing-only.
    out = []
    for child in entry.iterdir(dereference=False, ignore_dos=True):  # type: ignore[attr-defined]
        attr = child.attribute
        name = attr.file_name
        if name in (".", ".."):
            # A volume's own root directory (MFT record 5) has a
            # self-referential $FILE_NAME index entry named "." whose
            # own ParentDirectory points back at the root itself — left
            # unfiltered, any canonical-ref resolution reaching into an
            # NTFS subtree recurses into "." forever the first time it
            # searches past the root level. ".." isn't known to occur in
            # practice here but is filtered too, matching the same
            # defensive posture FAT/ext already take.
            continue
        is_dir = attr.is_dir()
        out.append((name, is_dir, None if is_dir else attr.file_size))
    return out


def _ntfs_volume_label(volume: object) -> str | None:
    return getattr(volume, "volume_name", None) or None


_NTFS_FORMAT = _Format(
    label="NTFS",
    open=_module_open("dissect.ntfs.ntfs", "NTFS"),
    resolve=_ntfs_resolve,
    iterdir=_ntfs_iterdir,
    size=_ntfs_size,
    volume_label=_ntfs_volume_label,
)


def _posix_filetype_iterdir(entry: object) -> list[tuple[str, bool, int | None]]:
    # Shared by ext2/3/4 and XFS: ``.listdir()`` returns {name: INode}, and
    # each INode's own ``.filetype`` is the same raw POSIX stat mode
    # bitmask on both formats.
    out = []
    for name, child in entry.listdir().items():  # type: ignore[attr-defined]
        if name in (".", ".."):
            continue
        is_dir = stat.S_ISDIR(child.filetype)
        out.append((name, is_dir, None if is_dir else child.size))
    return out


def _extfs_volume_label(volume: object) -> str | None:
    # Most Linux distros never set an ext volume label at format time,
    # but the kernel updates the superblock's own ``last_mounted`` path
    # every time the filesystem is actually mounted — a populated
    # fallback in the common case a label isn't set.
    return getattr(volume, "volume_name", None) or getattr(volume, "last_mount", None) or None


_EXTFS_FORMAT = _Format(
    label="ext2/3/4",
    open=_module_open("dissect.extfs.extfs", "ExtFS"),
    resolve=_default_resolve,
    iterdir=_posix_filetype_iterdir,
    size=_default_size,
    volume_label=_extfs_volume_label,
)


def _xfs_volume_label(volume: object) -> str | None:
    return getattr(volume, "name", None) or None


_XFS_FORMAT = _Format(
    label="XFS",
    open=_module_open("dissect.xfs.xfs", "XFS"),
    resolve=_default_resolve,
    iterdir=_posix_filetype_iterdir,
    size=_default_size,
    volume_label=_xfs_volume_label,
)


def _btrfs_iterdir(entry: object) -> list[tuple[str, bool, int | None]]:
    # Unlike ext/XFS (a raw POSIX filetype bitmask via stat.S_ISDIR),
    # dissect.btrfs's own INode exposes is_dir()/is_file() as plain
    # methods. Its ``.listdir()`` already crosses Btrfs subvolume
    # boundaries transparently, so a subvolume's own root inode just
    # shows up as an ordinary directory entry — no separate
    # subvolume-selection logic is needed here.
    #
    # The hasattr guard is load-bearing, not redundant with the name
    # filter: ``dissect.btrfs``'s own ``Subvolume.get(path)`` — the
    # resolve() this entry always arrives through — sets a resolved
    # INode's own ``.parent`` to the bare ``Subvolume`` object itself
    # rather than a real INode, so ``entry.listdir()[".."]`` can be a
    # ``Subvolume`` instance with no ``.is_dir()``/``.size`` at all whenever a
    # listing crosses a subvolume boundary. Reporting it as skipped
    # rather than raising matches ``_ntfs_size``'s own "report what's
    # observed, don't crash" posture for NTFS's missing-$DATA-stream
    # case.
    out = []
    for name, child in entry.listdir().items():  # type: ignore[attr-defined]
        if name in (".", "..") or not hasattr(child, "is_dir"):
            continue
        is_dir = child.is_dir()
        out.append((name, is_dir, None if is_dir else child.size))
    return out


def _btrfs_volume_label(volume: object) -> str | None:
    return getattr(volume, "label", None) or None


_BTRFS_FORMAT = _Format(
    label="Btrfs",
    open=_module_open("dissect.btrfs.btrfs", "Btrfs"),
    resolve=_default_resolve,
    iterdir=_btrfs_iterdir,
    size=_default_size,
    volume_label=_btrfs_volume_label,
)


def _fat_iterdir(entry: object) -> list[tuple[str, bool, int | None]]:
    out = []
    for child in entry.iterdir():  # type: ignore[attr-defined]
        name = child.path.rsplit("\\", 1)[-1]
        if name in (".", ".."):
            continue
        is_dir = child.is_directory()
        out.append((name, is_dir, None if is_dir else child.size))
    return out


def _fat_volume_label(volume: object) -> str | None:
    # Unlike the other four formats' own volume-name attribute, FAT's
    # underlying boot-sector field layout differs across FAT12/16/32 --
    # dissect.fat always sets ``volume_label`` in FATFS.__init__() itself
    # rather than lazily, so a variant it doesn't handle would raise
    # there, not here, but the guard costs nothing and matches this
    # module's own "report what's observed, don't crash" posture.
    try:
        return volume.volume_label or None  # type: ignore[attr-defined]
    except Exception:
        return None


_FAT_FORMAT = _Format(
    label="FAT",
    open=_module_open("dissect.fat.fat", "FATFS"),
    resolve=_default_resolve,
    iterdir=_fat_iterdir,
    size=_default_size,
    volume_label=_fat_volume_label,
)

# Tried in this order at every candidate offset that isn't an APFS
# container (see DiskFilesystem._try_open_apfs) -- order doesn't affect
# correctness (formats are mutually exclusive by construction) but
# cheaper/more common formats first avoids a little wasted work.
_FLAT_FORMATS = (_NTFS_FORMAT, _EXTFS_FORMAT, _XFS_FORMAT, _BTRFS_FORMAT, _FAT_FORMAT)

#: The one candidate a disk with no recognized partition table falls
#: back to (see ``DiskFilesystem.open``'s own comment on this) -- pulled
#: out as a constant since ``_flat_format_label``/``_try_open_apfs`` both
#: need to recognize it, not just construct it.
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


def _apfs_iterdir(entry: object) -> list[tuple[str, bool, int | None]]:
    # entry.iterdir() yields lightweight DirectoryEntry objects, not the
    # full INode (DirectoryEntry has no .size of its own), so .size must
    # be read through its own .inode property instead.
    #
    # The "." / ".." filter and the hasattr guard mirror the same
    # quirks _ntfs_iterdir's and _btrfs_iterdir's own listings need,
    # applied here preemptively rather than reactively.
    out = []
    for child in entry.iterdir():  # type: ignore[attr-defined]
        if child.name in (".", "..") or not hasattr(child, "is_dir") or not hasattr(child, "inode"):
            continue
        is_dir = child.is_dir()
        size = None
        if not is_dir:
            try:
                size = child.inode.size
            except Exception:
                # An inode can carry an extended field dissect.apfs's
                # own parser fails to decode (e.g. a DIR_STATS_KEY field
                # on a file inode, normally only meaningful on a
                # directory's) — same "report what's observed, don't
                # crash" posture _default_size()/_ntfs_size() take for
                # their own formats' size-read failures.
                size = None
        out.append((child.name, is_dir, size))
    return out


def _apfs_unreachable_open(fh: BinaryIO) -> object:
    # _Format.open is never called for _APFS_FORMAT -- an APFS *volume*
    # is only ever obtained via DiskFilesystem._try_open_apfs's own
    # container.volumes unwrapping, ContentSource being the container as
    # a whole is a different, two-level shape the other formats don't
    # have (this module's own docstring).
    raise NotImplementedError(
        "_APFS_FORMAT.open is unreachable -- see DiskFilesystem._try_open_apfs"
    )  # pragma: no cover


def _apfs_volume_label_unreachable(volume: object) -> str | None:
    # Never called: _try_open_apfs reads an APFS volume's own name
    # directly (it needs it before _DissectEntry/_Format even enter the
    # picture, to mint each volume's own partition_addr), not through
    # _Format.volume_label -- present only so _APFS_FORMAT satisfies the
    # same required _Format shape every other format does.
    raise NotImplementedError(
        "_APFS_FORMAT.volume_label is unreachable -- see DiskFilesystem._try_open_apfs"
    )  # pragma: no cover


# A single opened APFS *volume* (not the container) behaves close enough
# to the flat formats above (get(path), entries with .name/.is_dir()/
# .open()) to share this same _Format shape once _try_open_apfs has
# already done the container -> volumes unwrapping a flat format never
# needs.
_APFS_FORMAT = _Format(
    label="APFS",
    open=_apfs_unreachable_open,
    resolve=_default_resolve,
    iterdir=_apfs_iterdir,
    size=_default_size,
    volume_label=_apfs_volume_label_unreachable,
)


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
    or a single unwrapped APFS volume — see ``_APFS_FORMAT``'s own
    docstring), paired with the ``_Format`` describing how to operate on
    it. Every method here is synchronous — only ever called from a
    worker thread via ``asyncio.to_thread``."""

    def __init__(self, fmt: _Format, volume: object) -> None:
        self._fmt = fmt
        self._volume = volume

    def list_dir(self, path: str) -> list[tuple[str, bool, int | None]]:
        entry = self._fmt.resolve(self._volume, path)
        # Sorted by name — the guest filesystem's own on-disk directory
        # order (NTFS/FAT/ext/XFS/Btrfs/APFS all differ) isn't otherwise
        # meaningful, unlike ``units/fs.py``'s FS-workload listing (already
        # ``ORDER BY basename``) that this mirrors.
        return sorted(self._fmt.iterdir(entry), key=lambda e: e[0])

    def open_file(self, path: str) -> DissectFileContentSource:
        entry = self._fmt.resolve(self._volume, path)
        size = self._fmt.size(entry)
        return DissectFileContentSource(entry, size)


class DiskFilesystem:
    """One already-open disk image's own partition table and
    filesystem(s)/volume(s), lazily parsed. Build with ``open``, never
    the constructor directly.

    A disk image can (and typically does) contain more than one
    partition/volume (a Windows "System Reserved" boot partition
    alongside the real NTFS volume; a modern macOS disk's
    ``Data``/``Preboot``/``Recovery`` volumes alongside its sealed
    ``Macintosh HD`` system volume) — every one Dissect can open is
    kept, not just the first/largest, matching what a real disk-mount
    tool would show. A partition/volume Dissect can't open (unsupported
    format, encrypted without a key, or genuinely not a filesystem at
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
            DiskFilesystemUnavailableError: None of this module's
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
        this module. This method unwraps a container into one synthetic
        partition entry per browsable volume (including a sealed/signed
        system volume); each unwrapped volume then plugs into the same
        generic ``_Format``-driven machinery the flat formats use, since
        its ``get(path)``/``.name``/``.is_dir()``/``.open()`` shape
        matches closely enough to share one adapter table entry
        (``_APFS_FORMAT``). Every candidate byte offset tries APFS first,
        then ``_FLAT_FORMATS`` in order, first match wins — formats are
        mutually exclusive by construction, so trying more than one is
        only ever wasted, cheap I/O, never wrong.

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

    async def list_dir(self, partition_addr: int, path: str) -> list[tuple[str, bool, int | None]]:
        """``(name, is_dir, size)`` for one directory's real entries —
        ``path="/"`` means the partition/volume's own filesystem root.
        ``path`` is resolved fresh via the underlying format's own
        ``get(path)`` every call (this module's own docstring on why
        paths, not inode numbers, are the stable identifier here) —
        dot/dot-dot and each format's own synthetic bookkeeping entries
        are filtered out by that format's own ``_Format.iterdir``,
        never surfaced as browsable content."""
        entry = self._filesystems[partition_addr]
        return await asyncio.to_thread(entry.list_dir, path)

    async def open_file(self, partition_addr: int, path: str) -> DissectFileContentSource:
        entry = self._filesystems[partition_addr]
        return await asyncio.to_thread(entry.open_file, path)


class _BlockingReadContentSource:
    """Shared ``ContentSource`` implementation for a single already-resolved
    guest-OS file, deliberately thin: every Dissect filesystem entry's own
    ``.open()`` (returning a ``dissect.util.stream.RunlistStream``/
    ``AlignedStream``) already resolves this file's own runlist/
    fragmentation/sparse regions internally (this module's own docstring),
    so there is no extent bookkeeping here at all, unlike
    ``VirtualDiskContentSource``. Streamed in blocks for ``stream()``/
    ``export_to()`` rather than materializing the whole file in memory the
    way ``LazyArtifact`` does — a guest-OS file can be arbitrarily large,
    unlike an assembled ``.eml``/``.ics``.

    Held privately by ``DissectFileContentSource`` (composition, not a base
    class — it's not meant to expose this helper on its own public
    surface) so a future second blocking-read source could share this
    same forwarding logic.
    """

    def __init__(self, size: int | None, read_blocking: Callable[[int, int], bytes]) -> None:
        """
        Args:
            size: The file's size, or ``None`` if unknown (treated as ``0``).
            read_blocking: Synchronous ``(offset, length) -> bytes`` reader,
                run via ``asyncio.to_thread``.
        """
        self._size = size if size is not None else 0
        self._read_blocking = read_blocking

    @property
    def size(self) -> int | None:
        return self._size

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``False`` — a single guest-OS file read through Dissect
        has no bucket concept to spread reads across."""
        return False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        """See ``units.base.ContentSource.read``'s EOF contract — a
        request extending past this file's own ``size`` is clamped, never
        an error.

        Unlike a chunk-map-backed ``ContentSource`` (which raises rather
        than ever returning a silently-short result), ``read_blocking``
        delegates to a Dissect stream's own ``.read(n)`` — ordinary
        file-object semantics, which *can* legitimately return fewer than
        ``n`` bytes if the guest file's real on-disk data is truncated
        relative to what the filesystem's own metadata declared. When the
        requested window reaches this file's declared end and the actual
        read still comes up short, that's raised as ``DataCorruptError``
        rather than silently handed back as a shorter file.

        Raises:
            DataCorruptError: the underlying stream returned fewer bytes than
                requested while reading up to this file's declared end.
        """
        if offset < 0 or (length is not None and length < 0):
            raise ValueError(f"read(offset={offset}, length={length}): offset/length must be non-negative")
        n = clamp_read_length(offset, length, self._size)
        if n <= 0:
            return b""
        data = await asyncio.to_thread(self._read_blocking, offset, n)
        if len(data) < n and offset + n >= self._size:
            raise DataCorruptError(
                f"read {len(data)} bytes at offset {offset}, expected {n} (declared size={self._size})"
            )
        return data

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return stream_via_read(self, block)

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> ExportResult:
        # No sparse/hole concept applies to a single guest-OS file read
        # through Dissect (unlike a dedup composition's own ZERO chunks)
        # — always a full, non-sparse write, same posture
        # device.py::_LocalFileContentSource takes for its own non-dedup
        # sidecar files.
        def _open_dst() -> BinaryIO:
            dst.parent.mkdir(parents=True, exist_ok=True)
            return dst.open("wb")

        def _read_and_write(handle: BinaryIO, offset: int, n: int) -> int:
            # One block's read *and* write inside the same
            # asyncio.to_thread() call below, rather than this.stream()'s
            # own read()-then-separate-write shape (two thread-pool round
            # trips per block for a large single-file export). Replicates
            # read()'s own short-read-at-declared-end contract, since this
            # bypasses read() itself to make the single-hop merge possible.
            data = self._read_blocking(offset, n)
            if len(data) < n and offset + n >= self._size:
                raise DataCorruptError(
                    f"read {len(data)} bytes at offset {offset}, expected {n} (declared size={self._size})"
                )
            handle.write(data)
            return len(data)

        handle = await asyncio.to_thread(_open_dst)
        try:
            written = 0
            offset = 0
            while offset < self._size:
                # offset (not written) drives the loop and the next read's
                # position, advancing by the full block n regardless of how
                # many bytes this call actually returned — matching
                # stream_via_read()'s own identical "advance by n" contract.
                # An interior short read (legitimate per read()'s own
                # docstring: the guest file's real data can be truncated
                # relative to what the filesystem declared, at any offset,
                # not just the final block) would otherwise leave `written`
                # permanently stuck below self._size, spinning forever
                # instead of completing.
                n = min(DEFAULT_STREAM_BLOCK, self._size - offset)
                written += await asyncio.to_thread(_read_and_write, handle, offset, n)
                offset += n
                if progress is not None:
                    await progress(written, self._size)
        finally:
            await asyncio.to_thread(handle.close)
        return ExportResult(bytes_written=written, logical_size=self._size, holes=0, zeros=0)


class DissectFileContentSource:
    """Backed by one already-resolved Dissect filesystem entry (an NTFS
    ``MftRecord``, an ext ``INode``, a FAT ``DirectoryEntry``, or an APFS
    ``DirectoryEntry``) — reads through that entry's own ``.open()`` stream
    rather than a format-specific random-access method, since that's the
    one thing all four formats' entries agree on (unlike read-at-offset
    method names/arg orders, which differ per format)."""

    def __init__(self, entry: object, size: int | None) -> None:
        self._entry = entry
        #: Lazily opened on the first block read, then reused for every
        #: later one — ``.open()`` re-resolves the whole file's own
        #: runlist/fragmentation internally (this class's own docstring),
        #: work identical across every block of the same file, so a
        #: streamed multi-block read/export must not pay it again per
        #: block. ``_fh_lock`` (a real ``threading.Lock``, not an
        #: ``asyncio.Lock`` — ``asyncio.to_thread`` dispatches to a real
        #: executor thread pool, so two genuine OS threads can race into
        #: ``_read_blocking`` concurrently, the same reasoning
        #: ``storage/local.py``'s own ``_fd_lock`` documents) serializes
        #: every ``seek``/``read`` pair on this one cached handle, so a
        #: caller issuing an overlapping ``read()``/``stream()`` on the
        #: same instance (unusual, but not something this class can rule
        #: out) never races another call's own ``seek``.
        self._fh: object | None = None
        self._fh_lock = threading.Lock()
        self._blocking = _BlockingReadContentSource(size, self._read_blocking)

    def _read_blocking(self, offset: int, length: int) -> bytes:
        with self._fh_lock:
            if self._fh is None:
                self._fh = self._entry.open()  # type: ignore[attr-defined]
            try:
                self._fh.seek(offset)  # type: ignore[union-attr]
                return self._fh.read(length)  # type: ignore[union-attr,no-any-return]
            except BaseException:
                # A failure mid-seek/read leaves this stream's own
                # position/internal state unknown -- drop the cached
                # handle so the next call opens a fresh one instead of
                # reusing a possibly-broken stream, the same recovery a
                # fresh per-call open always had.
                self._fh = None
                raise

    @property
    def size(self) -> int | None:
        return self._blocking.size

    @property
    def supports_concurrent_export(self) -> bool:
        return self._blocking.supports_concurrent_export

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return await self._blocking.read(offset, length)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return self._blocking.stream(block)

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> ExportResult:
        return await self._blocking.export_to(dst, sparse=sparse, progress=progress)
