"""Shared plumbing every per-format module in this package
(``_ntfs``/``_apfs``/``_posix_formats``) builds on: the ``_Format`` shape,
the defaults most formats use unmodified, and the
``ContentUnavailableError`` reason strings. Imports no sibling module, so
every module in the package can import it without a cycle.
"""

from __future__ import annotations

import dataclasses
import importlib
import os
from collections.abc import Callable
from datetime import datetime
from typing import BinaryIO

from ...base import FileState

# Pin dissect.util's read-alignment granularity before dissect.util.stream
# is first imported: its default follows io.DEFAULT_BUFFER_SIZE, which
# Python 3.14 raised from 8192 to 131072. A larger block only wastes bytes
# on filesystem browsing's small, scattered reads.
os.environ.setdefault("DISSECT_STREAM_BUFFER_SIZE", "8192")


def _try_import(module_path: str) -> object | None:
    try:
        return importlib.import_module(module_path)
    except ImportError:
        return None


@dataclasses.dataclass(frozen=True, slots=True)
class _DirEntry:
    """One directory child, as ``_Format.iterdir`` returns it. ``size``
    and ``mtime`` are ``None`` when the entry has no reliable value; a
    failure reading ``mtime`` degrades for that entry alone rather than
    aborting the listing."""

    name: str
    is_dir: bool
    size: int | None
    file_state: FileState
    mtime: datetime | None


@dataclasses.dataclass(frozen=True, slots=True)
class _Format:
    """One single-volume-per-offset Dissect filesystem format; a new format
    is a new ``_Format`` instance, not a new class. Dissect's per-format
    APIs look alike but differ, so each format supplies its own callables
    (see the ``_*_FORMAT`` instances in the sibling modules)."""

    label: str
    open: Callable[[BinaryIO], object]
    """``fh -> volume``. Raises on a byte range that isn't this format."""
    resolve: Callable[[object, str], object]
    """``(volume, absolute_path) -> entry``. ``"/"`` is always the root."""
    iterdir: Callable[[object], list[_DirEntry]]
    """``entry -> [_DirEntry(...), ...]`` for one directory's real
    children."""
    size: Callable[[object], int | None]
    """``entry -> size`` for a resolved file entry."""
    volume_label: Callable[[object], str | None]
    """``volume -> label``, the filesystem's own volume name (``None`` if
    absent/empty) — distinct from the partition table's name/type
    (``_partition_table_label``)."""
    content_unavailable: Callable[[object], str | None]
    """``entry -> reason``, or ``None`` if this format has no cloud-sync/
    encryption concept or this entry looks normal. Checked before
    ``.open()``, so it must not read content."""


def _module_open(module_path: str, class_name: str) -> Callable[[BinaryIO], object]:
    """A ``_Format.open`` callable that lazily imports ``module_path`` and
    constructs ``class_name`` from the handle."""

    def open_fn(fh: BinaryIO) -> object:
        module = _try_import(module_path)
        assert module is not None
        return getattr(module, class_name)(fh)

    return open_fn


def _default_resolve(volume: object, path: str) -> object:
    """``_Format.resolve`` via the volume's ``get(path)``; every format
    except NTFS (``_ntfs._ntfs_resolve``)."""
    return volume.get(path or "/")  # type: ignore[attr-defined]


def _default_size(entry: object) -> int | None:
    """``_Format.size`` via the entry's ``.size`` attribute, ``None`` if
    reading it raises. NTFS and APFS have their own."""
    try:
        return entry.size  # type: ignore[attr-defined,no-any-return]
    except Exception:  # noqa: BLE001
        return None


def _default_content_unavailable(entry: object) -> None:
    """``_Format.content_unavailable`` for a format with no cloud-sync/
    encryption concept (ext2/3/4, XFS, Btrfs, FAT): always ``None``."""
    return


#: User-facing ``ContentUnavailableError`` messages, shown verbatim
#: (unlike ``Node.file_state``, which a presentation layer renders its own
#: way). ``_CLOUD_ONLY_REASON`` serves NTFS and APFS, ``_ENCRYPTED_REASON``
#: NTFS only; this SDK has no key material to attempt decryption.
_CLOUD_ONLY_REASON = "cloud-sync placeholder — no data at backup time"
_ENCRYPTED_REASON = "EFS-encrypted — no key to decrypt it"
