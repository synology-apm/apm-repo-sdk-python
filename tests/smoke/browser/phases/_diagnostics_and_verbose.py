"""``diagnostics_and_verbose`` domain: ``DiagnosticsScreen``'s real
``verify()`` findings render without crashing, and ``d``'s verbose-mode
toggle actually propagates (``refresh_for_verbose_mode``) -- a
currently-connected repository's own screen-level behavior, not decode
correctness (already ``sdk/``'s job).
"""

from __future__ import annotations

from typing import Any

from textual.widgets import DataTable, Static

from .._context import SmokeContext
from ._shared import wait_until


async def run(ctx: SmokeContext, app: Any, pilot: Any) -> None:
    if app.repo_handle is None:
        ctx.skip(
            "diagnostics_and_verbose", "diagnostics_and_verbose.no_repo", "navigate phase never connected a repository"
        )
        return

    async def _run_diagnostics() -> int:
        from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

        await pilot.press("v")
        await wait_until(
            pilot, lambda: isinstance(app.screen, DiagnosticsScreen), message="DiagnosticsScreen never appeared"
        )
        status = app.screen.query_one("#diag-status", Static)
        # _show_findings' own two possible status strings both include
        # "level=" -- the initial DIAGNOSTICS_QUICK_STATUS constant
        # doesn't, so this is the real "verify() finished" signal, not a
        # fixed sleep.
        await wait_until(
            pilot, lambda: "level=" in str(status.render()), timeout=15.0, message="verify() never finished"
        )
        table = app.screen.query_one("#diag-table", DataTable)
        return int(table.row_count)

    row_count = await ctx.call("diagnostics_and_verbose", "diagnostics_and_verbose.verify_quick", _run_diagnostics)
    if row_count is not None:
        ctx.check("diagnostics_and_verbose", "diagnostics_and_verbose.verify_quick.rendered", row_count >= 0)

    await pilot.press("escape")  # back to BrowseScreen/UnitScreen

    async def _toggle_verbose() -> bool:
        before = app.verbose
        await pilot.press("d")
        await pilot.pause(0.2)
        return app.verbose is not before

    toggled = await ctx.call("diagnostics_and_verbose", "diagnostics_and_verbose.toggle_verbose", _toggle_verbose)
    if toggled is not None:
        ctx.check("diagnostics_and_verbose", "diagnostics_and_verbose.toggle_verbose.propagated", toggled)


__all__ = ["run"]
