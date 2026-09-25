"""APFS support. A single opened APFS *volume* (not the container) shares
the same ``_Format`` shape the flat formats use (``get(path)``, entries
with ``.name``/``.is_dir()``/``.open()``) once ``DiskFilesystem._try_open_apfs``
(in ``__init__.py``) has already done the container -> volumes unwrapping a
flat format never needs. ``_apfs_is_dataless`` is also used directly by
``_content_source.py``'s ``DissectFileContentSource`` (a confirmed, not
heuristic, dataless-file check reused as a read-failure classifier), the
one place this module's own surface reaches outside the `_Format`
machinery.
"""

from __future__ import annotations

from datetime import datetime
from typing import BinaryIO

from ...base import FileState
from ._base import _CLOUD_ONLY_REASON, _default_resolve, _DirEntry, _Format, _try_import

_DATALESS_CMPFS_ALGORITHMS = frozenset({0x80000001, 0x80000002})


def _apfs_decmpfs_header(entry: object) -> object | None:
    """Decodes this inode's own ``com.apple.decmpfs`` xattr header —
    shared by ``_apfs_is_dataless`` and ``_apfs_size``'s own size-
    correction fallback, so neither hand-rolls its own decode of the
    same xattr. A caller invoking both on the same entry (as
    ``_apfs_iterdir`` and ``open_file`` do) can still read/parse it
    twice; the xattr is small enough that this isn't worth guarding
    against. Reuses dissect.apfs's own cstruct type (the same one
    ``INode.size``'s own tier-2 branch uses internally), not a
    hand-rolled struct decode. Defensive like every other APFS accessor
    in this module (``entry.xattr`` does its own B-tree read and could
    in principle raise for a malformed inode) -- ``None`` on any
    failure, never propagated."""
    try:
        xattr = entry.xattr.get("com.apple.decmpfs")  # type: ignore[attr-defined]
        if xattr is None:
            return None
        c_apfs_module = _try_import("dissect.apfs.c_apfs")
        cstruct_defs = getattr(c_apfs_module, "c_apfs", None)
        if cstruct_defs is None:
            return None
        return cstruct_defs.decmpfs_header(xattr.open())  # type: ignore[no-any-return]
    except Exception:
        return None


def _apfs_is_dataless(entry: object) -> bool:
    """Confirmed, not a heuristic: ``SF_DATALESS`` (``bsd_flags``) or a
    decmpfs xattr whose ``algorithm`` is one of Apple's own
    dataless-marker sentinel values (real HFS+/APFS compression uses
    small, disjoint integer values instead, e.g. zlib/LZFSE, which
    dissect.apfs already decompresses fine) both mean this file has zero
    real local data, with no known false positive. Used both proactively
    (before ever calling ``.open()``) and to characterize whatever
    exception a real read still raises. Never lets a failure checking
    either signal propagate -- this must degrade to "not confirmed
    dataless," not crash a caller that's only trying to find out."""
    try:
        c_apfs_module = _try_import("dissect.apfs.c_apfs")
        cstruct_defs = getattr(c_apfs_module, "c_apfs", None)
        if cstruct_defs is not None and entry.bsd_flags & cstruct_defs.SF_DATALESS:  # type: ignore[attr-defined]
            return True
    except Exception:
        pass
    header = _apfs_decmpfs_header(entry)
    # Deliberately unconditional, not gated behind is_compressed() the
    # way _apfs_size's check below is -- most dataless files carrying
    # only this signal (no SF_DATALESS) aren't is_compressed() either,
    # so gating it the same way would stop catching them.
    return header is not None and header.algorithm in _DATALESS_CMPFS_ALGORITHMS  # type: ignore[attr-defined]


def _apfs_size(entry: object) -> int | None:
    """``_Format.size`` for APFS -- corrects a ``dissect.apfs`` bug:
    ``INode.size``'s own tier-1 shortcut (``HAS_UNCOMPRESSED_SIZE`` ->
    ``self.inode.uncompressed_size``, a fixed-header field) can return a
    stale/zeroed ``0`` for an evicted file, even though the file's real,
    pre-eviction logical size is still recorded in its decmpfs xattr's
    own header. Only ever overrides a *compressed* entry's zero -- an
    ordinary, genuinely-empty file is never ``is_compressed()``, so this
    never touches a real zero-byte file's own correct size."""
    try:
        size: int | None = entry.size  # type: ignore[attr-defined]
    except Exception:
        return None
    if size == 0 and entry.is_compressed():  # type: ignore[attr-defined]
        header = _apfs_decmpfs_header(entry)
        if header is not None and header.uncompressed_size:  # type: ignore[attr-defined]
            return header.uncompressed_size  # type: ignore[attr-defined,no-any-return]
    return size


def _apfs_content_unavailable(entry: object) -> str | None:
    return _CLOUD_ONLY_REASON if _apfs_is_dataless(entry) else None


def _apfs_entry_mtime(inode: object) -> datetime | None:
    """Mtime read off an already-dereferenced ``INode``; degrades to
    ``None`` rather than raising, matching every other entry field this
    package reports."""
    try:
        return inode.mtime  # type: ignore[attr-defined,no-any-return]
    except Exception:
        return None


def _apfs_iterdir(entry: object) -> list[_DirEntry]:
    # entry.iterdir() yields lightweight DirectoryEntry objects, not the
    # full INode (DirectoryEntry has no .size/.mtime of its own), so both
    # must be read through its own .inode property instead.
    #
    # The "." / ".." filter and the hasattr guard mirror the same
    # quirks _ntfs.py's/_posix_formats.py's own listings need, applied
    # here preemptively rather than reactively.
    out = []
    for child in entry.iterdir():  # type: ignore[attr-defined]
        if child.name in (".", "..") or not hasattr(child, "is_dir") or not hasattr(child, "inode"):
            continue
        is_dir = child.is_dir()
        # Dereferencing a DirectoryEntry to its full INode is a plain
        # B-tree lookup by oid -- doesn't touch xfields at all, so this
        # should essentially never fail, but degrades to None if it
        # somehow does, same as everywhere else in this package.
        inode = None
        try:
            inode = child.inode
        except Exception:
            inode = None
        # Unlike size/dataless (files only, below), mtime is read for
        # every entry -- a directory's own modification time is an
        # ordinary concept too, so a directory entry now pays its own
        # B-tree lookup for this, not just files.
        mtime = _apfs_entry_mtime(inode) if inode is not None else None
        size = None
        state = FileState.NORMAL
        if not is_dir and inode is not None:
            # _apfs_size (below) has its own separate try/except for the
            # real, expected failure mode: a size *read* hitting
            # dissect.apfs's own DIR_STATS_KEY/xfields decode bug.
            size = _apfs_size(inode)
            if _apfs_is_dataless(inode):
                state = FileState.CLOUD_ONLY
        out.append(_DirEntry(name=child.name, is_dir=is_dir, size=size, file_state=state, mtime=mtime))
    return out


def _apfs_unreachable_open(fh: BinaryIO) -> object:
    # _Format.open is never called for _APFS_FORMAT -- an APFS *volume*
    # is only ever obtained via DiskFilesystem._try_open_apfs's own
    # container.volumes unwrapping, ContentSource being the container as
    # a whole is a different, two-level shape the other formats don't
    # have.
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


_APFS_FORMAT = _Format(
    label="APFS",
    open=_apfs_unreachable_open,
    resolve=_default_resolve,
    iterdir=_apfs_iterdir,
    size=_apfs_size,
    volume_label=_apfs_volume_label_unreachable,
    content_unavailable=_apfs_content_unavailable,
)
