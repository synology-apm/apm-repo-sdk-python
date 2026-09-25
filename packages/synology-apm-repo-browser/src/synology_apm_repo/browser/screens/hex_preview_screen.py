"""``HexPreviewScreen``: an ``xxd``-style offset/hex/ASCII dump of the
leaf node currently selected in ``UnitScreen``, opened with ``x``.
Diagnostic-mode only, the same positioning as ``d`` itself — looking
straight at raw bytes is exactly the kind of thing ordinary mode hides.

Reuses ``ContentSource.read`` — the exact same call
``synology-apm-repo-cli cat --offset --length`` makes — rather than
opening a second read path.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, Static

from synology_apm_repo.browser.keymap import COMMON_BINDINGS
from synology_apm_repo.browser.screens._shared import NavigableScreen
from synology_apm_repo.browser.strings import HEX_WINDOW_SIZE
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import ContentSource

_BYTES_PER_LINE = 16


def _format_dump(data: bytes, base_offset: int) -> str:
    if not data:
        return "(empty — offset is at or past the end of this item's content)"
    lines = []
    for row_start in range(0, len(data), _BYTES_PER_LINE):
        row = data[row_start : row_start + _BYTES_PER_LINE]
        offset_col = f"{base_offset + row_start:08x}"
        hex_col = " ".join(f"{b:02x}" for b in row).ljust(_BYTES_PER_LINE * 3 - 1)
        # Raw bytes 0x5b/0x5d ('['/']') are common in real content and
        # would otherwise be misparsed as Rich markup tags by the Static
        # this string is rendered into — same reasoning as every other
        # real-content interpolation in this package (sdk/presentation/markup.py).
        ascii_col = safe("".join(chr(b) if 0x20 <= b < 0x7F else "." for b in row))
        lines.append(f"{offset_col}  {hex_col}  {ascii_col}")
    return "\n".join(lines)


class HexPreviewScreen(NavigableScreen):
    """One leaf's content, ``offset``/``hex``/``ASCII`` columns, a
    ``HEX_WINDOW_SIZE``-byte window at a time. ``+``/``-`` (or ``x``/
    ``X``, doubling as a quick re-press-to-advance shortcut) page
    forward/back; going past the start clamps to 0 rather than going
    negative."""

    BINDINGS = [
        *COMMON_BINDINGS,
        Binding("plus", "page_forward", "Page +"),
        Binding("equals_sign", "page_forward", "Page +", show=False),  # '+' without shift on most layouts
        Binding("minus", "page_back", "Page -"),
        Binding("x", "page_forward", "Page +", show=False),
        # Bare "X", not "shift+x": Textual's own ``XTermParser`` reports a
        # shifted letter from a real terminal as that literal capital
        # character, never a "shift+"-prefixed key name. Pilot tests
        # bypass the real parser, so they accept either spelling — only
        # the bare capital is reachable from an actual keyboard.
        Binding("X", "page_back", "Page -", show=False),
        Binding("h", "go_back", "Back", show=False),
        Binding("escape", "go_back", "Back"),
    ]

    def __init__(self, content: ContentSource, name: str) -> None:
        super().__init__()
        self._content = content
        self._name = name
        self._offset = 0

    def compose(self) -> ComposeResult:
        # Starts empty, like every other NavigableScreen's own breadcrumb
        # -- on_mount below fills it via _update_breadcrumb_text, so a
        # background job's own tasks-hint suffix has real breadcrumb
        # text to combine with instead of blanking it.
        yield Static("", id="breadcrumb")
        yield Static("", id="hex-dump")
        yield Footer(show_command_palette=False)

    # ``on_mount``/``action_*`` are ``async def`` here because
    # ``ContentSource.read()`` is: Textual awaits a coroutine-returning
    # handler to completion just like any other, legitimate as long as
    # the call stays bounded enough to need no separate loading feedback/
    # cancellation point. This screen reads one small window per
    # keypress and so has never needed a worker of its own. The
    # ``type: ignore`` is this same legitimate-async-override shape, just
    # now also caught statically: NavigableScreen.on_mount is a real,
    # synchronous method (registering the jobs-watch for the breadcrumb
    # tasks-hint), so overriding it with an async one is a genuine
    # ``Coroutine`` vs ``None`` return-type mismatch to mypy -- exactly
    # the pattern Textual's own runtime dispatch already tolerates.
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
