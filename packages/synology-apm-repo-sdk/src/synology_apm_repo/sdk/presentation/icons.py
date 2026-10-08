"""Short glyphs a caller renders inline next to a name."""

from __future__ import annotations

#: Keyed by ``FileState.value``, not the enum, so this leaf module imports
#: nothing from the SDK. ``""`` (``NORMAL``) means no suffix.
#:
#: ``cloud_only`` is bare U+2601 without the VS16 emoji selector, so its
#: width is one cell (``Neutral``, as ``rich.cells.cell_len`` measures it)
#: rather than up to the terminal's emoji-presentation choice.
FILE_STATE_ICON: dict[str, str] = {
    "normal": "",
    "encrypted": "🔒",
    "cloud_only": "☁",
}


def file_state_suffix(value: str) -> str:
    """A ready-to-append suffix for ``value`` (``FileState.value``): a
    leading space plus the matching icon, or ``""`` when there's no icon
    (``FileState.NORMAL``, or an unrecognized value)."""
    icon = FILE_STATE_ICON.get(value, "")
    return f" {icon}" if icon else ""


#: Appended to a ``diagnostic_node()`` placeholder (``Node.is_diagnostic``),
#: in every view: it is backup-completeness information, not an internal
#: identifier. Bare U+26A0 without VS16, for the same reason as
#: ``FILE_STATE_ICON``'s ``cloud_only``.
DIAGNOSTIC_ICON = "⚠"


def diagnostic_suffix(is_diagnostic: bool) -> str:
    """A ready-to-append suffix (leading space plus ``DIAGNOSTIC_ICON``)
    when ``is_diagnostic`` is true, else ``""``."""
    return f" {DIAGNOSTIC_ICON}" if is_diagnostic else ""
