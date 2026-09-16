"""``help_screen`` domain: ``?`` opens the real ``HelpScreen``, listing at
least one binding from the active screen's real ``active_bindings`` --
worth its own light phase since no other domain exercises this screen.
"""

from __future__ import annotations

from typing import Any

from .._context import SmokeContext
from ._shared import wait_until


async def run(ctx: SmokeContext, app: Any, pilot: Any) -> None:
    async def _open_help() -> int:
        from synology_apm_repo.browser.screens.help_screen import HelpScreen

        active_before = dict(app.screen.active_bindings)
        await pilot.press("question_mark")
        await wait_until(pilot, lambda: isinstance(app.screen, HelpScreen), message="HelpScreen never appeared")
        return len(active_before)

    binding_count = await ctx.call("help_screen", "help_screen.opens", _open_help)
    if binding_count is not None:
        ctx.check("help_screen", "help_screen.opens.lists_bindings", binding_count > 0)
        await pilot.press("escape")


__all__ = ["run"]
