"""APFS support. One APFS *volume* fits the ``_Format`` shape once
``DiskFilesystem._try_open_apfs`` (``_disk_filesystem.py``) has unwrapped
the container into volumes. ``_apfs_is_dataless`` is also used by
``DissectFileContentSource`` to classify a read failure.
"""

from __future__ import annotations

import functools
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


def _apfs_data_stream(entry: object) -> object | None:
    """This inode's ``DSTREAM`` extended field, ``None`` if it has none."""
    c_apfs_module = _try_import("dissect.apfs.c_apfs")
    dstream = entry.xfields.get(c_apfs_module.c_apfs.INO_EXT_TYPE.DSTREAM)  # type: ignore[attr-defined]
    return dstream[1] if dstream else None


def _apfs_is_dataless(entry: object) -> bool:
    """Whether this file has no local data: ``SF_DATALESS`` in
    ``bsd_flags``, or a decmpfs header with a dataless-sentinel
    ``algorithm`` (real compression algorithms use small, disjoint values)
    on a file that is compressed or has no data stream; a downloaded cloud
    file keeps that header but is neither. Failing to read the flags or data
    stream of a file with that header reads as dataless, any other failure
    as not dataless."""
    try:
        c_apfs_module = _try_import("dissect.apfs.c_apfs")
        cstruct_defs = getattr(c_apfs_module, "c_apfs", None)
        if cstruct_defs is not None and entry.bsd_flags & cstruct_defs.SF_DATALESS:  # type: ignore[attr-defined]
            return True
    except Exception:  # noqa: BLE001
        pass
    header = _apfs_decmpfs_header(entry)
    if header is None or header.algorithm not in _DATALESS_CMPFS_ALGORITHMS:  # type: ignore[attr-defined]
        return False
    try:
        return bool(entry.is_compressed()) or _apfs_data_stream(entry) is None  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return True


def _apfs_size(entry: object) -> int | None:
    """``_Format.size`` for APFS. ``dissect.apfs``'s ``INode.size`` prefers
    a decmpfs header's size, so an uncompressed file's size is read from its
    data stream instead (a downloaded cloud file's header keeps its pre-edit
    size until the next sync). It can also return ``0`` for an evicted file,
    so a compressed entry's ``0`` is replaced by its header's size; a
    genuinely empty file is never ``is_compressed()``."""
    try:
        if not entry.is_compressed():  # type: ignore[attr-defined]
            dstream = _apfs_data_stream(entry)
            if dstream is not None:
                return dstream.size  # type: ignore[attr-defined,no-any-return]
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


@functools.cache
def _apfs_file_stream_class() -> type[BinaryIO]:
    """A ``dissect.apfs`` ``FileStream`` that reads a sparse extent
    (``phys_block_num`` 0) as zeros and can start a read inside an extent.
    Raises on an encrypted volume, which it can't decrypt."""
    stream_mod = _try_import("dissect.apfs.stream")
    assert stream_mod is not None  # only reached for an entry dissect.apfs resolved

    class _ApfsFileStream(stream_mod.FileStream):  # type: ignore[name-defined,misc]
        def _read(self, offset: int, length: int) -> bytes:
            if self.volume.is_encrypted:
                raise stream_mod.Error("encrypted APFS volume: no key to decrypt it")  # type: ignore[attr-defined]
            chunks = []
            while length:
                logical, physical, extent_length, _crypto_id = self._lookup(offset)
                skip = offset - logical
                if skip >= extent_length:
                    raise stream_mod.Error(f"no file extent for {self.oid} at offset {offset}")  # type: ignore[attr-defined]
                n = min(extent_length - skip, length)
                if physical == 0:
                    chunks.append(bytes(n))
                else:
                    block = physical + skip // self.align
                    chunks.append(self.volume.container._read_block(block, n // self.align))  # noqa: SLF001
                offset += n
                length -= n
            return b"".join(chunks)

    return _ApfsFileStream


def _apfs_open_stream(entry: object) -> BinaryIO:
    """``_Format.open_stream`` for APFS: ``entry.open()``, with a plain
    data stream read through ``_apfs_file_stream_class`` instead."""
    stream = entry.open()  # type: ignore[attr-defined]
    stream_mod = _try_import("dissect.apfs.stream")
    if stream_mod is not None and type(stream) is stream_mod.FileStream:  # type: ignore[attr-defined]
        return _apfs_file_stream_class()(stream.volume, stream.oid, stream.size)  # type: ignore[call-arg]
    return stream  # type: ignore[no-any-return]


def _apfs_entry_mtime(inode: object) -> datetime | None:
    """The ``INode``'s mtime, ``None`` if reading it raises."""
    try:
        return inode.mtime  # type: ignore[attr-defined,no-any-return]
    except Exception:  # noqa: BLE001
        return None


def _apfs_iterdir(entry: object) -> list[_DirEntry]:
    # The "."/".." filter and hasattr guard are defensive.
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
    raise NotImplementedError(
        "_APFS_FORMAT.open is unreachable -- see DiskFilesystem._try_open_apfs"
    )  # pragma: no cover


def _apfs_volume_label_unreachable(volume: object) -> str | None:
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
    open_stream=_apfs_open_stream,
)
