"""Short glyphs a caller renders inline next to a name — shared so the CLI
and TUI never each pick their own for the same concept.
"""

from __future__ import annotations

#: Keyed by ``FileState.value`` (``sdk.units.base.FileState``), not the
#: enum itself — this module, like every other presentation helper, has
#: no SDK-internal import of its own. A caller holding the real enum
#: looks up ``FILE_STATE_ICON[state.value]``; ``""`` (``NORMAL``) means
#: no suffix. Appended by the caller at render time, never carried
#: across the SDK/CLI/TUI boundary as a pre-rendered string — only
#: ``file_state``'s own semantic value crosses that boundary.
#:
#: ``cloud_only``'s glyph is the bare U+2601 codepoint, not U+2601+U+FE0F
#: (the VS16 emoji-presentation sequence): U+2601 is ``Neutral`` width per
#: Unicode's own East Asian Width property, so both that and
#: ``rich.cells.cell_len`` — what the TUI's ``DataTable`` sizes a column
#: from — already agree it's one cell; adding VS16 would make that
#: agreement depend on however a given terminal/font and ``rich`` version
#: each choose to size the emoji-presentation form, rather than a single
#: property this file can verify on its own. ``encrypted`` needs no such
#: care either way: U+1F512 alone is already ``Wide`` (two cells) by the
#: same property, nothing for VS16 to change.
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
