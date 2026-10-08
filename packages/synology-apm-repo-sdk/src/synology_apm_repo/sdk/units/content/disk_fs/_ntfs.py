"""NTFS support, including cloud-sync-placeholder and EFS-encryption
detection.
"""

from __future__ import annotations

from datetime import datetime

from ...base import FileState
from ._base import _CLOUD_ONLY_REASON, _ENCRYPTED_REASON, _DirEntry, _Format, _module_open, _try_import


def _is_cloud_file(obj: object) -> bool:
    """``obj.is_cloud_file()``, dissect.ntfs's IO_REPARSE_TAG_CLOUD*
    reparse-point check, on either a full ``MftRecord`` or a listing's
    ``$FILE_NAME`` attribute; ``False`` if it raises."""
    try:
        return bool(obj.is_cloud_file())  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return False


def _ntfs_is_encrypted_attr(attr: object) -> bool:
    """Cheap EFS check on a listing's ``$FILE_NAME`` attribute: its cached
    ``FILE_ATTRIBUTE.ENCRYPTED`` bit, no ``MftRecord`` dereference
    needed."""
    try:
        c_ntfs_module = _try_import("dissect.ntfs.c_ntfs")
        c_ntfs_defs = getattr(c_ntfs_module, "c_ntfs", None)
        if c_ntfs_defs is None:
            return False
        return bool(attr.file_attributes & c_ntfs_defs.FILE_ATTRIBUTE.ENCRYPTED)  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return False


def _ntfs_is_encrypted(entry: object) -> bool:
    """EFS check for a full ``MftRecord``: ``$STANDARD_INFORMATION``'s
    ``ENCRYPTED`` bit, or a present ``$EFS``-named
    ``$LOGGED_UTILITY_STREAM`` attribute (which holds the key-recovery
    blobs of an encrypted file). A failure checking either signal reads as
    not encrypted."""
    c_ntfs_module = _try_import("dissect.ntfs.c_ntfs")
    try:
        c_ntfs_defs = getattr(c_ntfs_module, "c_ntfs", None)
        stdinfo = entry.attributes.STANDARD_INFORMATION  # type: ignore[attr-defined]
        if c_ntfs_defs is not None and stdinfo and stdinfo.file_attributes & c_ntfs_defs.FILE_ATTRIBUTE.ENCRYPTED:
            return True
    except Exception:  # noqa: BLE001
        pass
    try:
        attribute_type_code = getattr(c_ntfs_module, "ATTRIBUTE_TYPE_CODE", None)
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
    # ``entry`` is the full MftRecord, so these checks see the real
    # $REPARSE_POINT/$EFS attributes rather than _ntfs_iterdir's $FILE_NAME
    # copy. Cloud-only wins: an evicted placeholder has no local bytes,
    # encrypted or not.
    if _is_cloud_file(entry):
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
)
