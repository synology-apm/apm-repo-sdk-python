"""Short glyphs a caller renders inline next to a name — shared so the CLI
and TUI never each pick their own for the same concept.
"""

from __future__ import annotations

#: Keyed by ``FileState.value``, not the enum itself — this module has no
#: SDK-internal import of its own. ``""`` (``NORMAL``) means no suffix,
#: appended by the caller at render time.
#:
#: ``cloud_only``'s glyph is bare U+2601, not U+2601+U+FE0F (the VS16
#: emoji-presentation sequence): U+2601 is ``Neutral`` width per Unicode's
#: East Asian Width property, matching how ``rich.cells.cell_len`` sizes it
#: at one cell — VS16 would make that depend on the terminal/font's own
#: emoji-presentation choice instead. ``encrypted`` needs no such care:
#: U+1F512 is already ``Wide`` (two cells) either way.
FILE_STATE_ICON: dict[str, str] = {
    "normal": "",
    "encrypted": "🔒",
    "cloud_only": "☁",
}


def file_state_suffix(value: str) -> str:
    """A ready-to-append suffix for ``value`` (``FileState.value``) — a
    leading space plus the matching icon, or ``""`` when there's no icon
    (``FileState.NORMAL``, or an unrecognized value) — so every caller
    appends this directly instead of separately re-deriving "no icon
    means no suffix" at each render site."""
    icon = FILE_STATE_ICON.get(value, "")
    return f" {icon}" if icon else ""


#: Appended when a listing node is a ``diagnostic_node()`` placeholder
#: (``sdk.units.base.node_is_diagnostic``) rather than real content —
#: shown unconditionally, like ``FILE_STATE_ICON`` above, since this is
#: user-facing backup-completeness information, not an internal
#: identifier gated behind verbose mode. Bare U+26A0, not U+26A0+U+FE0F,
#: same ``Neutral``-width/``cell_len`` reasoning as ``FILE_STATE_ICON``'s
#: ``cloud_only`` comment above.
DIAGNOSTIC_ICON = "⚠"


def diagnostic_suffix(is_diagnostic: bool) -> str:
    """A ready-to-append suffix when ``is_diagnostic`` is true (a
    ``diagnostic_node()`` placeholder), or ``""`` otherwise — the same
    "no icon means no suffix" shape as ``file_state_suffix``, for a
    boolean condition instead of a keyed value."""
    return f" {DIAGNOSTIC_ICON}" if is_diagnostic else ""
