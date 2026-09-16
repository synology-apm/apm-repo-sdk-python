"""``hex_preview`` domain: ``HexPreviewScreen`` (``x``) on the same picked
leaf ``navigate.py`` landed on -- a thin rendering check (does the real
first bytes render as the expected hex/ASCII columns), not a
decode-correctness one (already ``sdk/``'s job). Diagnostic-mode only,
same as the real keybinding's own gate.
"""

from __future__ import annotations

from typing import Any

from textual.widgets import Static

from .._context import SmokeContext
from ._shared import wait_until


async def run(ctx: SmokeContext, app: Any, pilot: Any) -> None:
    ref = ctx.data.get("main_ref")
    tree = ctx.data.get("unit_tree")
    if ref is None or tree is None:
        ctx.skip("hex_preview", "hex_preview.no_ref", "navigate phase never landed on a leaf")
        return

    if not app.verbose:
        await pilot.press("d")
        await pilot.pause(0.2)

    async def _open_hex() -> str:
        from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen
        from synology_apm_repo.browser.screens.unit_screen import UnitScreen

        assert isinstance(app.screen, UnitScreen), app.screen
        await pilot.press("x")
        await wait_until(
            pilot, lambda: isinstance(app.screen, HexPreviewScreen), message="HexPreviewScreen never appeared"
        )
        dump = app.screen.query_one("#hex-dump", Static)
        await wait_until(pilot, lambda: str(dump.render()).strip() != "", message="hex dump never rendered")
        return str(dump.render())

    dump_text = await ctx.call("hex_preview", f"hex_preview.{ref.sample_name}", _open_hex)
    if dump_text is not None:
        if dump_text.startswith("(empty"):
            # _format_dump's own documented shape for a real, legitimately
            # empty (0-byte) leaf -- a correct rendering, not a hex dump
            # to shape-check against.
            ctx.skip(
                "hex_preview", f"hex_preview.{ref.sample_name}.rendered_as_hex_dump", "leaf has no known/nonzero size"
            )
        else:
            # Every non-empty dump line starts with an 8-hex-digit offset
            # column (_format_dump's own shape) -- a real, thin rendering
            # check rather than re-deriving byte correctness.
            first_line = dump_text.splitlines()[0] if dump_text.splitlines() else ""
            looks_like_hex_dump = len(first_line) >= 8 and all(c in "0123456789abcdef" for c in first_line[:8])
            ctx.check("hex_preview", f"hex_preview.{ref.sample_name}.rendered_as_hex_dump", looks_like_hex_dump)
        await pilot.press("escape")


__all__ = ["run"]
