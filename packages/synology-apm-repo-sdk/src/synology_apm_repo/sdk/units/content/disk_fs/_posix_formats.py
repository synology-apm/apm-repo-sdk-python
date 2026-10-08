"""ext2/3/4, XFS, Btrfs and FAT: the formats with no cloud-sync/
encryption concept.
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
    """``entry.mtime``, ``None`` if reading it raises."""
    try:
        return entry.mtime  # type: ignore[attr-defined,no-any-return]
    except Exception:  # noqa: BLE001
        return None


def _posix_filetype_iterdir(entry: object) -> list[_DirEntry]:
    # ext2/3/4 and XFS: ``.listdir()`` returns {name: INode}, and
    # ``.filetype`` is a POSIX stat mode on both.
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
    # An ext volume label is rarely set; the superblock's last-mounted
    # path (``last_mount``) is the fallback.
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
    # ``.listdir()`` crosses subvolume boundaries itself, so a subvolume
    # root is an ordinary directory entry. The hasattr guard is needed:
    # ``Subvolume.get(path)`` sets a resolved INode's ``.parent`` to the
    # ``Subvolume`` object, so a listed child can be a ``Subvolume`` with
    # no ``.is_dir()``/``.size``/``.mtime``.
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
    """``_safe_mtime(entry)`` labelled UTC. FAT stores naive local time
    with no offset, so UTC is an approximation that keeps ``Node.mtime``
    an aware ``datetime`` like every other provider's."""
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
    # Defensive: the boot-sector layout differs across FAT12/16/32.
    try:
        return volume.volume_label or None  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
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
