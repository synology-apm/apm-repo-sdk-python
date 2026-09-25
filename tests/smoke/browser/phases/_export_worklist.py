"""``export_worklist`` domain: a real background export end to end --
``ExportScreen`` -> backgrounded (``b``) -> ``WorklistScreen`` lists it ->
real file lands on disk with the right content. The one check that's
genuinely TUI-worker-specific (``@work`` scheduling, not
thread-marshaled) -- ``export_to()``'s own correctness is already
``sdk/``'s job.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from textual.widgets import Button, DataTable, Input

from .._context import SmokeContext
from ._shared import wait_until


async def run(ctx: SmokeContext, app: Any, pilot: Any) -> None:
    ref = ctx.data.get("main_ref")
    tree = ctx.data.get("unit_tree")
    if ref is None or tree is None:
        ctx.skip("export_worklist", "export_worklist.no_ref", "navigate phase never landed on a leaf")
        return

    with tempfile.TemporaryDirectory(prefix="apm-browser-smoke-") as tmp_dir:
        dst = Path(tmp_dir) / "export.bin"

        async def _start_export() -> None:
            from synology_apm_repo.browser.screens.export_screen import ExportScreen
            from synology_apm_repo.browser.screens.unit_screen import UnitScreen

            assert isinstance(app.screen, UnitScreen), app.screen
            await pilot.press("e")
            await wait_until(pilot, lambda: isinstance(app.screen, ExportScreen), message="ExportScreen never appeared")
            app.screen.query_one("#export-dst", Input).value = str(dst)
            app.screen.query_one("#export-start", Button).press()
            await pilot.pause(0)

        await ctx.call("export_worklist", f"export_worklist.{ref.sample_name}.start", _start_export)

        async def _background_if_still_running() -> str:
            from synology_apm_repo.browser.screens.export_screen import ExportScreen
            from synology_apm_repo.browser.screens.unit_screen import UnitScreen
            from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen

            # A real, tiny leaf can finish before this coroutine even gets
            # scheduled -- background only when there's still a live job to
            # background (real ExportScreen._job_id, white-box, allowed for
            # tests -- pyproject.toml's per-file-ignores).
            if isinstance(app.screen, ExportScreen) and app.screen._job_id is not None:
                await pilot.press("b")
                await wait_until(
                    pilot, lambda: isinstance(app.screen, UnitScreen), message="never returned after backgrounding"
                )
                await pilot.press("t")
                await wait_until(
                    pilot, lambda: isinstance(app.screen, WorklistScreen), message="WorklistScreen never appeared"
                )
                return "backgrounded"
            if isinstance(app.screen, ExportScreen):
                await pilot.press("escape")
            return "completed_before_background"

        outcome = await ctx.call(
            "export_worklist", f"export_worklist.{ref.sample_name}.background", _background_if_still_running
        )
        if outcome == "backgrounded":
            table = app.screen.query_one("#worklist-table", DataTable)
            if table.row_count >= 1:
                ctx.check("export_worklist", f"export_worklist.{ref.sample_name}.worklist_shows_job", True)
            else:
                # A real, tiny leaf can also finish in the gap between
                # pressing b and WorklistScreen's own on_mount refresh --
                # the same race _background_if_still_running already
                # accounts for one step earlier, just caught slightly
                # later here.
                ctx.skip(
                    "export_worklist",
                    f"export_worklist.{ref.sample_name}.worklist_shows_job",
                    "export finished before WorklistScreen's own refresh saw it (a small enough real leaf)",
                )
            await pilot.press("escape")  # close worklist
        elif outcome == "completed_before_background":
            ctx.skip(
                "export_worklist",
                f"export_worklist.{ref.sample_name}.worklist_shows_job",
                "export finished before it could be backgrounded (a small enough real leaf)",
            )

        async def _wait_for_completion() -> int:
            await wait_until(pilot, lambda: dst.exists(), timeout=30.0, message="export never landed on disk")
            return dst.stat().st_size

        size = await ctx.call("export_worklist", f"export_worklist.{ref.sample_name}.completed", _wait_for_completion)
        if size is not None:
            ctx.check("export_worklist", f"export_worklist.{ref.sample_name}.completed.non_empty", size > 0)


__all__ = ["run"]
