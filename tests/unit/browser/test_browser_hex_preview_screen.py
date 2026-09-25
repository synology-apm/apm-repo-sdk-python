"""Unit tests for ``HexPreviewScreen`` — driven through a real Textual
``Pilot`` against a fake, in-memory
``ContentSource``, so this needs no real repository/sample and lives in
``tests/unit/``. Real end-to-end coverage (real bytes, real paging)
lives in ``tests/integration/browser/test_browser_pilot_hex_filter_refresh.py``."""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.widgets import Static

from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen, _format_dump
from synology_apm_repo.browser.strings import HEX_WINDOW_SIZE


class _FakeContentSource:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if offset >= len(self._data):
            return b""
        end = len(self._data) if length is None else min(len(self._data), offset + length)
        return self._data[offset:end]


class _FakeApp(App[None]):
    def __init__(self, content: _FakeContentSource) -> None:
        super().__init__()
        self._content = content
        # Read by NavigableScreen.on_mount/_render_breadcrumb (the jobs
        # watch backing the breadcrumb's own tasks-hint) -- always empty
        # here, since no test in this file exercises background jobs.
        self.jobs: dict[object, object] = {}

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(HexPreviewScreen(self._content, "item.bin"))  # type: ignore[arg-type]


async def test_offset_past_the_end_shows_the_empty_placeholder() -> None:
    content = _FakeContentSource(b"short")
    app = _FakeApp(content)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, HexPreviewScreen)
        screen._offset = 1_000_000  # well past the end of "short"
        await screen._render_dump()
        await pilot.pause()
        dump = str(screen.query_one("#hex-dump", Static).render())
        assert "empty" in dump.lower()


async def test_action_page_forward_advances_by_the_window_size() -> None:
    content = _FakeContentSource(bytes(range(256)) * 4)  # 1024 bytes, 2 real windows
    app = _FakeApp(content)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, HexPreviewScreen)
        await pilot.press("+")
        await pilot.pause()
        assert screen._offset == HEX_WINDOW_SIZE
        dump = str(screen.query_one("#hex-dump", Static).render())
        assert f"{HEX_WINDOW_SIZE:08x}" in dump  # the new window's own first offset column


async def test_action_page_back_clamps_to_zero_not_negative() -> None:
    content = _FakeContentSource(bytes(range(256)) * 4)
    app = _FakeApp(content)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, HexPreviewScreen)
        await pilot.press("minus")  # already at offset 0
        await pilot.pause()
        assert screen._offset == 0

        await pilot.press("+")  # advance once
        await pilot.press("X")  # then page back with the shifted-letter binding
        await pilot.pause()
        assert screen._offset == 0


async def test_x_key_pages_forward_same_as_plus() -> None:
    content = _FakeContentSource(bytes(range(256)) * 4)
    app = _FakeApp(content)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, HexPreviewScreen)
        await pilot.press("x")
        await pilot.pause()
        assert screen._offset == HEX_WINDOW_SIZE


async def test_action_go_back_pops_the_screen() -> None:
    content = _FakeContentSource(b"hello world")
    app = _FakeApp(content)
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, HexPreviewScreen)
        screen.action_go_back()
        await pilot.pause()
        assert app.screen is not screen


class TestFormatDump:
    """Direct unit tests for ``_format_dump`` -- never asserted on
    directly anywhere else in this file."""

    def test_offset_hex_and_ascii_columns_for_one_line(self) -> None:
        data = b"Hello, world!!!!"  # exactly 16 bytes -- one full line
        dump = _format_dump(data, base_offset=0)
        assert dump == "00000000  48 65 6c 6c 6f 2c 20 77 6f 72 6c 64 21 21 21 21  Hello, world!!!!"

    def test_base_offset_is_reflected_in_the_offset_column(self) -> None:
        dump = _format_dump(b"x", base_offset=HEX_WINDOW_SIZE)
        assert dump.startswith(f"{HEX_WINDOW_SIZE:08x}")

    def test_non_printable_bytes_render_as_dots_in_the_ascii_column(self) -> None:
        # 0x00/0x01/0x7f fall outside the printable 0x20<=b<0x7f range
        # (0x7f itself excluded); 0x41 ('A') is the one real printable byte.
        dump = _format_dump(bytes([0x00, 0x01, 0x41, 0x7F]), base_offset=0)
        assert dump.endswith("..A.")


__all__: list[str] = []
