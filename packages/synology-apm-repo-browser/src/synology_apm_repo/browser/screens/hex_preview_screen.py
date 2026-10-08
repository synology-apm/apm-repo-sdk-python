"""``HexPreviewScreen``: an ``xxd``-style dump of the leaf selected in
``UnitScreen``, opened with ``x`` in verbose mode only. It reads through
``ContentSource.read``, as ``cat --offset --length`` does.
"""

from __future__ import annotations

from typing import ClassVar, override

from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.widgets import Footer, Static

from synology_apm_repo.browser.keymap import COMMON_BINDINGS
from synology_apm_repo.browser.screens._shared import NavigableScreen
from synology_apm_repo.browser.strings import HEX_WINDOW_SIZE
from synology_apm_repo.sdk import ContentSource
from synology_apm_repo.sdk.presentation import safe

_BYTES_PER_LINE = 16


def _format_dump(data: bytes | bytearray, base_offset: int) -> str:
    if not data:
        return "(empty — offset is at or past the end of this item's content)"
    lines = []
    for row_start in range(0, len(data), _BYTES_PER_LINE):
        row = data[row_start : row_start + _BYTES_PER_LINE]
        offset_col = f"{base_offset + row_start:08x}"
        hex_col = " ".join(f"{b:02x}" for b in row).ljust(_BYTES_PER_LINE * 3 - 1)
        # Escaped: '[' would otherwise start Rich markup.
        ascii_col = safe("".join(chr(b) if 0x20 <= b < 0x7F else "." for b in row))
        lines.append(f"{offset_col}  {hex_col}  {ascii_col}")
    return "\n".join(lines)


class HexPreviewScreen(NavigableScreen):
    """One leaf's content, ``HEX_WINDOW_SIZE`` bytes at a time. ``+``/``-``
    (or ``x``/``X``) page forward/back; paging back stops at offset 0."""

    BINDINGS: ClassVar[list[BindingType]] = [
        *COMMON_BINDINGS,
        Binding("plus", "page_forward", "Page +"),
        Binding("equals_sign", "page_forward", "Page +", show=False),  # '+' without shift on most layouts
        Binding("minus", "page_back", "Page -"),
        Binding("x", "page_forward", "Page +", show=False),
        # "X", not "shift+x": a real terminal reports a shifted letter as
        # the capital (Pilot accepts either, the keyboard only this).
        Binding("X", "page_back", "Page -", show=False),
        Binding("h", "go_back", "Back", show=False),
        Binding("escape", "go_back", "Back"),
    ]

    def __init__(self, content: ContentSource, name: str) -> None:
        super().__init__()
        self._content = content
        self._name = name
        self._offset = 0

    @override
    def compose(self) -> ComposeResult:
        yield Static("", id="breadcrumb")
        yield Static("", id="hex-dump")
        yield Footer(show_command_palette=False)

    # async: one bounded read needs no worker. NavigableScreen.on_mount is
    # sync, which Textual's dispatch tolerates and mypy doesn't.
    @override
    async def on_mount(self) -> None:  # type: ignore[override]
        super().on_mount()
        self._update_breadcrumb_text(f"hex preview: {safe(self._name)}")
        await self._render_dump()

    async def _render_dump(self) -> None:
        data = await self._content.read(self._offset, HEX_WINDOW_SIZE)
        self.query_one("#hex-dump", Static).update(_format_dump(data, self._offset))

    async def action_page_forward(self) -> None:
        self._offset += HEX_WINDOW_SIZE
        await self._render_dump()

    async def action_page_back(self) -> None:
        self._offset = max(0, self._offset - HEX_WINDOW_SIZE)
        await self._render_dump()
