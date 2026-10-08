"""File-name safety for an export into a local directory: refusing a
backed-up name that would escape the destination or be invalid on the
platform, and sanitizing one into a safe file name."""

from __future__ import annotations

import sys
from pathlib import Path

_WINDOWS_RESERVED_BASENAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
)


WINDOWS_FORBIDDEN_NAME_CHARS: frozenset[str] = frozenset('<>:"|?*') | frozenset(map(chr, range(0x20)))
"""Characters Windows rejects in a file name, besides the path separators."""


def windows_reserved_basename(name: str) -> bool:
    """Whether ``name`` is a Windows reserved device name (``CON``, ``NUL``,
    ``COM1``, ...), with or without an extension after it."""
    return name.split(".", 1)[0].upper() in _WINDOWS_RESERVED_BASENAMES


def _unsafe_on_windows(segment: str) -> bool:
    """A segment Windows would not treat as one plain file name: a ``:``
    (drive-relative ``C:foo``, an alternate data stream), another forbidden
    character, a trailing ``.``/space, or a reserved device name."""
    return (
        not WINDOWS_FORBIDDEN_NAME_CHARS.isdisjoint(segment)
        or segment.endswith((".", " "))
        or windows_reserved_basename(segment)
    )


def safe_export_join(root: Path, *segments: str) -> Path:
    """Joins ``segments`` onto ``root``, rejecting any segment that could
    escape it — for building a local export destination path from
    ``Node.name`` values collected while walking a tree (outermost
    container first). A name is display text, possibly from a file inside
    a backed-up guest, not necessarily a single safe path component.

    Rejects rather than sanitizes, so a bulk export never silently
    collides or misleads; the caller decides from the raised exception
    whether to skip, log or rename.

    ``root`` itself is never validated: it is the caller's own trusted
    output directory.

    Args:
        root: The trusted local directory every joined path stays under.
        segments: One path component per tree level, outermost first.

    Raises:
        ValueError: a segment is empty, ``"."``/``".."``, or contains a
            path separator (``/`` or a backslash, which also rules out
            absolute paths) or a NUL byte; on Windows also one that is not
            a plain file name there (a ``:``, another forbidden character,
            a trailing ``.`` or space, or a reserved device name).
    """
    windows = sys.platform == "win32"
    result = root
    for segment in segments:
        if (
            not segment
            or segment in (".", "..")
            or "/" in segment
            or "\\" in segment
            or "\x00" in segment
            or (windows and _unsafe_on_windows(segment))
        ):
            raise ValueError(f"unsafe export path segment: {segment!r}")
        result = result / segment
    return result


_SEPARATOR_CHARS = frozenset("/\\\x00")


_WINDOWS_UNSAFE_CHARS = WINDOWS_FORBIDDEN_NAME_CHARS | _SEPARATOR_CHARS


def safe_file_name(name: str) -> str:
    """``name`` made into one safe path component on this system — for a
    file name the SDK synthesizes (``Node.export_name``) or only suggests,
    never for a name from the backup, which ``safe_export_join`` rejects
    rather than alters.

    Replaces each path separator and NUL (on Windows, each forbidden
    character too) with ``_``; on Windows also strips a trailing ``.``/space
    and prefixes a reserved device name with ``_``. A result that is
    empty, ``.`` or ``..`` becomes ``_``.
    """
    windows = sys.platform == "win32"
    unsafe = _WINDOWS_UNSAFE_CHARS if windows else _SEPARATOR_CHARS
    sanitized = "".join("_" if char in unsafe else char for char in name)
    if windows:
        sanitized = sanitized.rstrip(". ")
        if windows_reserved_basename(sanitized):
            sanitized = f"_{sanitized}"
    return sanitized if sanitized not in ("", ".", "..") else "_"
