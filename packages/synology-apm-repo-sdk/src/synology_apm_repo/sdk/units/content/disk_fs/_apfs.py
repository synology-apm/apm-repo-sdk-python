"""APFS support. One APFS *volume* fits the ``_Format`` shape once
``DiskFilesystem._try_open_apfs`` (``_disk_filesystem.py``) has unwrapped
the container into volumes. ``_apfs_is_dataless`` is also used by
``DissectFileContentSource`` to classify a read failure.
"""

from __future__ import annotations

from datetime import datetime
from typing import BinaryIO

from ...base import FileState
from ._base import _CLOUD_ONLY_REASON, _default_resolve, _DirEntry, _Format, _try_import

_DATALESS_CMPFS_ALGORITHMS = frozenset({0x80000001, 0x80000002})


def _apfs_decmpfs_header(entry: object) -> object | None:
    """This inode's ``com.apple.decmpfs`` xattr header, decoded with
    dissect.apfs's own cstruct type; ``None`` if absent or on any
    failure."""
    try:
        xattr = entry.xattr.get("com.apple.decmpfs")  # type: ignore[attr-defined]
        if xattr is None:
            return None
        c_apfs_module = _try_import("dissect.apfs.c_apfs")
        cstruct_defs = getattr(c_apfs_module, "c_apfs", None)
        if cstruct_defs is None:
            return None
        return cstruct_defs.decmpfs_header(xattr.open())  # type: ignore[no-any-return]
    except Exception:  # noqa: BLE001
        return None


def _apfs_is_dataless(entry: object) -> bool:
    """Whether this file has no local data: ``SF_DATALESS`` in
    ``bsd_flags``, or a decmpfs header whose ``algorithm`` is a dataless
    sentinel (real compression algorithms use small, disjoint values). A
    failure checking either signal reads as not dataless."""
    try:
        c_apfs_module = _try_import("dissect.apfs.c_apfs")
        cstruct_defs = getattr(c_apfs_module, "c_apfs", None)
        if cstruct_defs is not None and entry.bsd_flags & cstruct_defs.SF_DATALESS:  # type: ignore[attr-defined]
            return True
    except Exception:  # noqa: BLE001
        pass
    header = _apfs_decmpfs_header(entry)
    # Not gated behind is_compressed() (unlike _apfs_size): most dataless
    # files carrying only this signal aren't is_compressed().
    return header is not None and header.algorithm in _DATALESS_CMPFS_ALGORITHMS  # type: ignore[attr-defined]


def _apfs_size(entry: object) -> int | None:
    """``_Format.size`` for APFS, correcting a ``dissect.apfs`` bug:
    ``INode.size`` can return ``0`` for an evicted file whose real size is
    still in its decmpfs header. Only a compressed entry's zero is
    overridden; a genuinely empty file is never ``is_compressed()``."""
    try:
        size: int | None = entry.size  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return None
    if size == 0 and entry.is_compressed():  # type: ignore[attr-defined]
        header = _apfs_decmpfs_header(entry)
        if header is not None and header.uncompressed_size:  # type: ignore[attr-defined]
            return header.uncompressed_size  # type: ignore[attr-defined,no-any-return]
    return size


def _apfs_content_unavailable(entry: object) -> str | None:
    return _CLOUD_ONLY_REASON if _apfs_is_dataless(entry) else None


def _apfs_entry_mtime(inode: object) -> datetime | None:
    """The ``INode``'s mtime, ``None`` if reading it raises."""
    try:
        return inode.mtime  # type: ignore[attr-defined,no-any-return]
    except Exception:  # noqa: BLE001
        return None


def _apfs_iterdir(entry: object) -> list[_DirEntry]:
    # entry.iterdir() yields DirectoryEntry objects with no .size/.mtime;
    # both are read through .inode. The "."/".." filter and hasattr guard
    # are defensive, as in the other formats' listings.
    out = []
    for child in entry.iterdir():  # type: ignore[attr-defined]
        if child.name in (".", "..") or not hasattr(child, "is_dir") or not hasattr(child, "inode"):
            continue
        is_dir = child.is_dir()
        inode = None
        try:
            inode = child.inode
        except Exception:  # noqa: BLE001
            inode = None
        mtime = _apfs_entry_mtime(inode) if inode is not None else None
        size = None
        state = FileState.NORMAL
        if not is_dir and inode is not None:
            size = _apfs_size(inode)
            if _apfs_is_dataless(inode):
                state = FileState.CLOUD_ONLY
        out.append(_DirEntry(name=child.name, is_dir=is_dir, size=size, file_state=state, mtime=mtime))
    return out


def _apfs_unreachable_open(fh: BinaryIO) -> object:
    # Never called: APFS volumes come from DiskFilesystem._try_open_apfs.
    raise NotImplementedError(
        "_APFS_FORMAT.open is unreachable -- see DiskFilesystem._try_open_apfs"
    )  # pragma: no cover


def _apfs_volume_label_unreachable(volume: object) -> str | None:
    # Never called: _try_open_apfs reads the volume name directly.
    raise NotImplementedError(
        "_APFS_FORMAT.volume_label is unreachable -- see DiskFilesystem._try_open_apfs"
    )  # pragma: no cover


_APFS_FORMAT = _Format(
    label="APFS",
    open=_apfs_unreachable_open,
    resolve=_default_resolve,
    iterdir=_apfs_iterdir,
    size=_apfs_size,
    volume_label=_apfs_volume_label_unreachable,
    content_unavailable=_apfs_content_unavailable,
)
