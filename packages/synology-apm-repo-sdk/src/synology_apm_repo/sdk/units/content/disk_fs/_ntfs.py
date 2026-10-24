"""NTFS support, including cloud-sync-placeholder and EFS-encryption
detection.
"""

from __future__ import annotations

from datetime import datetime

from ...base import FileState
from ._base import (
    _CLOUD_ONLY_REASON,
    _ENCRYPTED_REASON,
    _default_open_stream,
    _DirEntry,
    _Format,
    _module_open,
    _try_import,
)


def _is_cloud_file(obj: object) -> bool:
    """``obj.is_cloud_file()``, dissect.ntfs's IO_REPARSE_TAG_CLOUD*
    reparse-point check, on either a full ``MftRecord`` or a listing's
    ``$FILE_NAME`` attribute; ``False`` if it raises."""
    try:
        return bool(obj.is_cloud_file())  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return False


def _file_attribute_bits(*names: str) -> int:
    """The named ``c_ntfs.FILE_ATTRIBUTE`` bits OR-ed together; ``0`` if
    dissect.ntfs isn't installed."""
    c_ntfs_defs = getattr(_try_import("dissect.ntfs.c_ntfs"), "c_ntfs", None)
    if c_ntfs_defs is None:
        return 0
    bits = 0
    for name in names:
        bits |= int(getattr(c_ntfs_defs.FILE_ATTRIBUTE, name))
    return bits


def _ntfs_data_fully_allocated(record: object) -> bool:
    """Whether every cluster of a full ``MftRecord``'s unnamed ``$DATA``
    within its size is allocated on disk. ``True`` for resident or empty
    data, and for a compressed stream, whose compression units hold
    unallocated runs whatever its download state. Raises if the stream
    can't be read."""
    c_ntfs_module = _try_import("dissect.ntfs.c_ntfs")
    data = record.attributes.find("", c_ntfs_module.ATTRIBUTE_TYPE_CODE.DATA)  # type: ignore[attr-defined]
    if data.header.resident or data.header.flags & c_ntfs_module.ATTRIBUTE_FLAG_COMPRESSION_MASK:  # type: ignore[attr-defined]
        return True
    cluster = record.ntfs.cluster_size  # type: ignore[attr-defined]
    want = (data.size() + cluster - 1) // cluster
    vcn = 0
    for lcn, count in data.dataruns():
        if vcn >= want:
            break
        # A sparse run has no cluster: that range isn't on disk.
        if lcn is None:
            return False
        vcn += count
    return bool(vcn >= want)


def _ntfs_is_cloud_only(record: object) -> bool:
    """Whether a full ``MftRecord`` is a cloud file without its complete
    data on disk: ``$STANDARD_INFORMATION`` says its data is to be recalled
    (or is offline), or ``_ntfs_data_fully_allocated`` is false. A cloud
    file whose state can't be read counts as cloud-only."""
    if not _is_cloud_file(record):
        return False
    try:
        flags = record.attributes.STANDARD_INFORMATION.file_attributes  # type: ignore[attr-defined]
        if flags & _file_attribute_bits("RECALL_ON_DATA_ACCESS", "RECALL_ON_OPEN", "OFFLINE"):
            return True
        return not _ntfs_data_fully_allocated(record)
    except Exception:  # noqa: BLE001
        return True


def _ntfs_is_encrypted_attr(attr: object) -> bool:
    """Cheap EFS check on a listing's ``$FILE_NAME`` attribute: its cached
    ``FILE_ATTRIBUTE.ENCRYPTED`` bit, no ``MftRecord`` dereference
    needed."""
    try:
        return bool(attr.file_attributes & _file_attribute_bits("ENCRYPTED"))  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return False


def _ntfs_is_encrypted(entry: object) -> bool:
    """EFS check for a full ``MftRecord``: ``$STANDARD_INFORMATION``'s
    ``ENCRYPTED`` bit, or a present ``$EFS``-named
    ``$LOGGED_UTILITY_STREAM`` attribute (which holds the key-recovery
    blobs of an encrypted file). A failure checking either signal reads as
    not encrypted."""
    try:
        stdinfo = entry.attributes.STANDARD_INFORMATION  # type: ignore[attr-defined]
        if stdinfo and stdinfo.file_attributes & _file_attribute_bits("ENCRYPTED"):
            return True
    except Exception:  # noqa: BLE001
        pass
    try:
        attribute_type_code = getattr(_try_import("dissect.ntfs.c_ntfs"), "ATTRIBUTE_TYPE_CODE", None)
        if attribute_type_code is None:
            return False
        return bool(entry.attributes.find("$EFS", attribute_type_code.LOGGED_UTILITY_STREAM))  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return False


def _ntfs_resolve(volume: object, path: str) -> object:
    root = volume.mft.get(5)  # type: ignore[attr-defined]  # MFT record 5 is always the NTFS root directory
    return root if path in ("", "/") else root.get(path.lstrip("/"))


def _ntfs_size(entry: object) -> int | None:
    try:
        return entry.size()  # type: ignore[attr-defined,no-any-return]
    except Exception:  # noqa: BLE001
        # A system metadata file (e.g. $Secure, MFT record 9) can have no
        # unnamed $DATA stream, which MftRecord.size() raises on.
        return None


def _ntfs_entry_mtime(attr: object) -> datetime | None:
    """The listing ``$FILE_NAME`` attribute's modification time, ``None``
    if reading it raises."""
    try:
        return attr.last_modification_time  # type: ignore[attr-defined,no-any-return]
    except Exception:  # noqa: BLE001
        return None


def _ntfs_iterdir(entry: object) -> list[_DirEntry]:
    # dereference=False reads the $FILE_NAME copy inline in each $I30 index
    # entry instead of resolving every child's MftRecord: an
    # order-of-magnitude cheaper listing. Reading content still resolves
    # the full MftRecord (_ntfs_resolve).
    out = []
    for child in entry.iterdir(dereference=False, ignore_dos=True):  # type: ignore[attr-defined]
        attr = child.attribute
        name = attr.file_name
        if name in (".", ".."):
            # The root directory (MFT record 5) lists a self-referential
            # "." entry; left in, ref resolution would recurse into it
            # forever.
            continue
        is_dir = attr.is_dir()
        if is_dir:
            state = FileState.NORMAL
        elif _is_cloud_file(attr):
            # Downloaded or not; _ntfs_content_unavailable tells at open.
            state = FileState.CLOUD_ONLY
        elif _ntfs_is_encrypted_attr(attr):
            state = FileState.ENCRYPTED
        else:
            state = FileState.NORMAL
        out.append(
            _DirEntry(
                name=name,
                is_dir=is_dir,
                size=None if is_dir else attr.file_size,
                file_state=state,
                mtime=_ntfs_entry_mtime(attr),
            )
        )
    return out


def _ntfs_volume_label(volume: object) -> str | None:
    return getattr(volume, "volume_name", None) or None


def _ntfs_content_unavailable(entry: object) -> str | None:
    # ``entry`` is the full MftRecord, so these checks see its own
    # attributes rather than _ntfs_iterdir's $FILE_NAME copy. Cloud-only
    # wins: without its complete data there is nothing to decrypt.
    if _ntfs_is_cloud_only(entry):
        return _CLOUD_ONLY_REASON
    if _ntfs_is_encrypted(entry):
        return _ENCRYPTED_REASON
    return None


_NTFS_FORMAT = _Format(
    label="NTFS",
    open=_module_open("dissect.ntfs.ntfs", "NTFS"),
    resolve=_ntfs_resolve,
    iterdir=_ntfs_iterdir,
    size=_ntfs_size,
    volume_label=_ntfs_volume_label,
    content_unavailable=_ntfs_content_unavailable,
    open_stream=_default_open_stream,
)
