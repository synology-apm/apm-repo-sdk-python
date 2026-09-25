"""NTFS support: cloud-sync-placeholder and EFS-encryption detection are
NTFS-specific (unlike ext2/3/4/XFS/Btrfs/FAT, handled generically in
``_posix_formats``), so this format gets its own module rather than
folding into that one. Part of this package's read-only, per-file
browsing/export view of a VM/PC/PS disk image via the Dissect framework.
"""

from __future__ import annotations

from datetime import datetime

from ...base import FileState
from ._base import _CLOUD_ONLY_REASON, _ENCRYPTED_REASON, _DirEntry, _Format, _module_open, _try_import


def _is_cloud_file(obj: object) -> bool:
    """``obj.is_cloud_file()`` is a real IO_REPARSE_TAG_CLOUD* reparse-point
    check dissect.ntfs exposes on both a full ``MftRecord`` and the
    lighter-weight ``$FILE_NAME`` attribute a non-dereferenced listing
    already holds -- same method name, same boolean meaning, different
    objects, so this one guard is shared by both callers."""
    try:
        return bool(obj.is_cloud_file())  # type: ignore[attr-defined]
    except Exception:
        return False


def _ntfs_is_encrypted_attr(attr: object) -> bool:
    """Cheap EFS check via the lighter-weight ``$FILE_NAME`` attribute's
    own cached ``FileAttributes`` copy -- the same attribute
    ``_ntfs_iterdir``'s own ``_is_cloud_file(attr)`` call already reads,
    no ``MftRecord`` dereference needed. ``FILE_ATTRIBUTE.ENCRYPTED`` is
    Microsoft's own confirmed marker, not a heuristic."""
    try:
        c_ntfs_module = _try_import("dissect.ntfs.c_ntfs")
        c_ntfs_defs = getattr(c_ntfs_module, "c_ntfs", None)
        if c_ntfs_defs is None:
            return False
        return bool(attr.file_attributes & c_ntfs_defs.FILE_ATTRIBUTE.ENCRYPTED)  # type: ignore[attr-defined]
    except Exception:
        return False


def _ntfs_is_encrypted(entry: object) -> bool:
    """Confirmed EFS check for a full, already-dereferenced ``MftRecord``:
    ``$STANDARD_INFORMATION``'s own ``FileAttributes`` ``ENCRYPTED`` bit
    (the same flag ``_ntfs_is_encrypted_attr`` reads from ``$FILE_NAME``'s
    cached copy), cross-checked against a present ``$EFS``-named
    ``$LOGGED_UTILITY_STREAM`` attribute -- the structure actually
    holding the DDF/DRF key-recovery blobs Windows creates alongside an
    encrypted file. Same "flag plus structural signal" shape
    ``_apfs.py``'s ``_apfs_is_dataless`` uses. Never lets a failure
    checking either signal propagate -- this must degrade to "not
    confirmed encrypted," not crash a caller that's only trying to find
    out."""
    c_ntfs_module = _try_import("dissect.ntfs.c_ntfs")
    try:
        c_ntfs_defs = getattr(c_ntfs_module, "c_ntfs", None)
        stdinfo = entry.attributes.STANDARD_INFORMATION  # type: ignore[attr-defined]
        if c_ntfs_defs is not None and stdinfo and stdinfo.file_attributes & c_ntfs_defs.FILE_ATTRIBUTE.ENCRYPTED:
            return True
    except Exception:
        pass
    try:
        attribute_type_code = getattr(c_ntfs_module, "ATTRIBUTE_TYPE_CODE", None)
        if attribute_type_code is None:
            return False
        return bool(entry.attributes.find("$EFS", attribute_type_code.LOGGED_UTILITY_STREAM))  # type: ignore[attr-defined]
    except Exception:
        return False


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
        # posture as everywhere else in this package.
        return None


def _ntfs_entry_mtime(attr: object) -> datetime | None:
    """Cheap mtime read off the same lightweight ``$FILE_NAME`` attribute
    ``_ntfs_iterdir`` already reads ``name``/``is_dir``/``file_size`` from
    -- no extra ``MftRecord`` dereference needed. Degrades to ``None``
    rather than raising, matching every other entry field this package
    reports."""
    try:
        return attr.last_modification_time  # type: ignore[attr-defined,no-any-return]
    except Exception:
        return None


def _ntfs_iterdir(entry: object) -> list[_DirEntry]:
    # dereference=False reads each child's own embedded $FILE_NAME
    # index-entry attribute directly instead of resolving every child to
    # its own full MftRecord: NTFS's own $I30 index entries already
    # carry a copy of the $FILE_NAME attribute (name/is_dir/size)
    # inline, an order-of-magnitude cheaper listing than the
    # dereferenced path. open_file() (in __init__.py) still needs the
    # full dereferenced MftRecord to actually read $DATA, so this is
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
    # ``entry`` is the full, already-dereferenced MftRecord _ntfs_resolve
    # returns -- MftRecord.is_cloud_file() and the $STANDARD_INFORMATION-based
    # _ntfs_is_encrypted check both look directly at the real $REPARSE_POINT/
    # $EFS attributes, unlike _ntfs_iterdir's own attr-based
    # checks above, which only have the lighter-weight $FILE_NAME
    # attribute from its own non-dereferenced listing. CLOUD_ONLY takes
    # priority: an evicted placeholder has no local bytes to even attempt
    # reading, encrypted or not.
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
