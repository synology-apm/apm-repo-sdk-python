"""ext2/3/4, XFS, Btrfs, and FAT: four formats with no cloud-sync/
encryption concept of their own, individually small enough not to need
one module each — ext2/3/4 and XFS additionally share
``_posix_filetype_iterdir`` outright. Part of this package's read-only,
per-file browsing/export view of a VM/PC/PS disk image via the Dissect
framework.
"""

from __future__ import annotations

import stat
from datetime import UTC, datetime

from ...base import FileState
from ._base import (
    _default_content_unavailable,
    _default_resolve,
    _default_size,
    _DirEntry,
    _Format,
    _module_open,
)


def _safe_mtime(entry: object) -> datetime | None:
    """Mtime read off an already-fully-resolved ``INode``/directory-entry
    object -- shared by ext2/3/4, XFS, and Btrfs, all of which read
    ``.mtime`` off an object they already have in hand. Degrades to
    ``None`` rather than raising, matching every other entry field this
    package reports."""
    try:
        return entry.mtime  # type: ignore[attr-defined,no-any-return]
    except Exception:
        return None


def _posix_filetype_iterdir(entry: object) -> list[_DirEntry]:
    # Shared by ext2/3/4 and XFS: ``.listdir()`` returns {name: INode}, and
    # each INode's own ``.filetype`` is the same raw POSIX stat mode
    # bitmask on both formats.
    out = []
    for name, child in entry.listdir().items():  # type: ignore[attr-defined]
        if name in (".", ".."):
            continue
        is_dir = stat.S_ISDIR(child.filetype)
        out.append(
            _DirEntry(
                name=name,
                is_dir=is_dir,
                size=None if is_dir else child.size,
                file_state=FileState.NORMAL,
                mtime=_safe_mtime(child),
            )
        )
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
    content_unavailable=_default_content_unavailable,
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
    content_unavailable=_default_content_unavailable,
)


def _btrfs_iterdir(entry: object) -> list[_DirEntry]:
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
    # ``Subvolume`` instance with no ``.is_dir()``/``.size``/``.mtime`` at
    # all whenever a listing crosses a subvolume boundary. Reporting it
    # as skipped rather than raising matches ``_ntfs.py``'s own
    # ``_ntfs_size``'s "report what's observed, don't crash" posture for
    # NTFS's missing-$DATA-stream case.
    out = []
    for name, child in entry.listdir().items():  # type: ignore[attr-defined]
        if name in (".", "..") or not hasattr(child, "is_dir"):
            continue
        is_dir = child.is_dir()
        out.append(
            _DirEntry(
                name=name,
                is_dir=is_dir,
                size=None if is_dir else child.size,
                file_state=FileState.NORMAL,
                mtime=_safe_mtime(child),
            )
        )
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
    content_unavailable=_default_content_unavailable,
)


def _fat_entry_mtime(entry: object) -> datetime | None:
    """``_safe_mtime(entry)``, reinterpreted as UTC. FAT's on-disk
    timestamp (``dostimestamp()``) is naive -- it stores local time with
    no offset, so the writer's real timezone is unknowable from the bytes
    alone. Attaching UTC is an approximation, not a real UTC timestamp,
    but matches every other provider's convention in this codebase that a
    ``Node``'s ``mtime`` attr is a real aware ``datetime``."""
    dt = _safe_mtime(entry)
    return dt.replace(tzinfo=UTC) if dt is not None else None


def _fat_iterdir(entry: object) -> list[_DirEntry]:
    out = []
    for child in entry.iterdir():  # type: ignore[attr-defined]
        name = child.path.rsplit("\\", 1)[-1]
        if name in (".", ".."):
            continue
        is_dir = child.is_directory()
        out.append(
            _DirEntry(
                name=name,
                is_dir=is_dir,
                size=None if is_dir else child.size,
                file_state=FileState.NORMAL,
                mtime=_fat_entry_mtime(child),
            )
        )
    return out


def _fat_volume_label(volume: object) -> str | None:
    # Unlike the other four formats' own volume-name attribute, FAT's
    # underlying boot-sector field layout differs across FAT12/16/32 --
    # dissect.fat always sets ``volume_label`` in FATFS.__init__() itself
    # rather than lazily, so a variant it doesn't handle would raise
    # there, not here, but the guard costs nothing and matches this
    # package's own "report what's observed, don't crash" posture.
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
    content_unavailable=_default_content_unavailable,
)
