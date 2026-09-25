"""Shared, dependency-free plumbing every per-format module in this
package (``_ntfs``/``_apfs``/``_posix_formats``) builds on: the
``_Format`` shape itself, the defaults most formats use unmodified, and
the two ``ContentUnavailableError`` reason strings shared across formats.
Part of this package's read-only, per-file browsing/export view of a
VM/PC/PS disk image via the Dissect framework. Nothing here imports
from a sibling format module — that's what keeps this the one leaf every
other module in the package can import from without a cycle.
"""

from __future__ import annotations

import dataclasses
import importlib
import os
from collections.abc import Callable
from datetime import datetime
from typing import BinaryIO

from ...base import FileState

# Pin dissect.util's read-alignment granularity so it isn't affected by
# whichever Python interpreter's own io.DEFAULT_BUFFER_SIZE happens to be
# in effect (Python 3.14 changed that stdlib default from 8192 to
# 131072). Must run before dissect.util.stream is ever imported, which
# only happens lazily inside this package's own _try_import calls, at
# call time -- executing this here, at this leaf module's own import
# time, is early enough since every _try_import call across the whole
# package happens later, inside a function body. 8192 is deliberate, not
# just inherited: a read below this size already rounds up to a full
# aligned block regardless of the value chosen, while a sequential read
# at or above it already collapses into one underlying call no matter how
# large the alignment is (AlignedStream.read()'s own divmod-based
# batching) — so a larger alignment only adds wasted bytes on the many
# small/scattered reads real filesystem browsing does, with no offsetting
# benefit.
os.environ.setdefault("DISSECT_STREAM_BUFFER_SIZE", "8192")


def _try_import(module_path: str) -> object | None:
    try:
        return importlib.import_module(module_path)
    except ImportError:
        return None


@dataclasses.dataclass(frozen=True)
class _DirEntry:
    """One directory's real child, as every format's own ``iterdir``
    reports it — the ``_Format.iterdir`` contract's own return element.
    ``mtime`` is ``None`` when a format/entry has no reliable value: each
    format module's own ``_safe_mtime``/``_*_entry_mtime`` helper reads it
    off a live Dissect object on real, possibly-malformed disk bytes, and
    degrades a raise there to ``None`` for that one entry rather than
    aborting the whole listing — this package's "report what's observed,
    don't crash" posture, applied here the same way ``_ntfs_size``/
    ``_apfs_size`` already apply it to a resolved entry's size."""

    name: str
    is_dir: bool
    size: int | None
    file_state: FileState
    mtime: datetime | None


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
    iterdir: Callable[[object], list[_DirEntry]]
    """``entry -> [_DirEntry(...), ...]`` for one directory's real
    children."""
    size: Callable[[object], int | None]
    """``entry -> size`` for a resolved file entry."""
    volume_label: Callable[[object], str | None]
    """``volume -> label``, the filesystem's own human-assigned volume
    name if it set one (``None`` if absent/empty) — distinct from the
    partition *table*'s own name/type (``_partition_table_label``),
    which a caller sees regardless of whether Dissect can even open this
    offset as this format at all."""
    content_unavailable: Callable[[object], str | None]
    """``entry -> reason``, or ``None`` if this format has no cloud-sync/
    encryption concept or this entry looks normal. Checked before
    ``.open()`` is ever called — must never itself trigger a real content
    read."""


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
    flat format except NTFS (``_ntfs.py``'s ``_ntfs_resolve``), which has
    no such method on its own root."""
    return volume.get(path or "/")  # type: ignore[attr-defined]


def _default_size(entry: object) -> int | None:
    """``_Format.size`` for every format whose resolved entry exposes a
    plain ``.size`` attribute — every flat format except NTFS
    (``_ntfs.py``'s ``_ntfs_size``), which needs its own missing-``$DATA``-
    stream fallback. Falls back to ``None`` the same defensive way if
    reading ``.size`` raises on any other format's own entry."""
    try:
        return entry.size  # type: ignore[attr-defined,no-any-return]
    except Exception:
        return None


def _default_content_unavailable(entry: object) -> None:
    """``_Format.content_unavailable`` for every format with no
    cloud-sync/encryption concept of its own (ext2/3/4, XFS, Btrfs,
    FAT) — always ``None``. APFS and NTFS each have their own, in
    ``_apfs.py``/``_ntfs.py``."""
    return None


#: The explanation for each of this SDK's own ``ContentUnavailableError``
#: cases, raised once per failed open — shown to the user verbatim
#: wherever that exception surfaces (the CLI's own error output, via
#: ``ApmRepoError.safe_message``; the TUI's detail-pane inline note), so
#: kept short rather than padded out to a full sentence. Distinct from
#: ``Node.attrs["file_state"]`` (a bare ``FileState`` a presentation layer
#: renders its own way, e.g. ``sdk.presentation.icons.FILE_STATE_ICON``).
#: ``_CLOUD_ONLY_REASON`` is shared verbatim between NTFS's confirmed
#: reparse-tag check and APFS's confirmed flag/xattr check — stated as
#: plain fact either way, not hedged: both are proactively trusted to
#: refuse an export on their own, not just to explain a failure that
#: already happened. ``_ENCRYPTED_REASON`` (NTFS only) is likewise
#: trusted on its own, since this SDK has no key material anywhere to
#: attempt a real decryption.
_CLOUD_ONLY_REASON = "cloud-sync placeholder — no data at backup time"
_ENCRYPTED_REASON = "EFS-encrypted — no key to decrypt it"
