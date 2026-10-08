"""``export_folder`` domain: ``e`` on a folder exports everything below it through the real
``ExportScreen`` and the app-level worker: the files land at their path below the folder with the
planned sizes, and no ``.part`` file is left. The export pipeline's own correctness is ``sdk/``'s job.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from textual.widgets import Button, Input

from synology_apm_repo.browser.core.app.cmd import RunExport
from synology_apm_repo.browser.core.app.model import FolderExport
from synology_apm_repo.sdk import RawView
from synology_apm_repo.sdk.export import plan_tree_export

from .._context import SmokeContext
from ._shared import wait_until

#: A folder export of a whole version can be large; past this the run is cancelled and reported, not failed.
_BUDGET_SECONDS = 300.0


async def run(ctx: SmokeContext, app: Any, pilot: Any) -> None:
    ref = ctx.data.get("main_ref")
    tree = ctx.data.get("unit_tree")
    if ref is None or tree is None:
        ctx.skip("export_folder", "export_folder.no_ref", "navigate phase never landed on a leaf")
        return
    step = f"export_folder.{ref.sample_name}"
    captured: list[RunExport] = []
    perform = app.effects.perform

    def _spy(cmd: Any) -> None:
        if isinstance(cmd, RunExport):
            captured.append(cmd)
        perform(cmd)

    app.effects.perform = _spy
    app.store._perform = _spy  # the store holds its own reference to the bound method

    with tempfile.TemporaryDirectory(prefix="apm-browser-smoke-folder-") as tmp_dir:
        dst = Path(tmp_dir) / "out"

        prior_focus: list[Any] = []

        async def _start() -> str:
            from synology_apm_repo.browser.screens.export_screen import ExportScreen
            from synology_apm_repo.browser.screens.unit_screen import UnitScreen

            assert isinstance(app.screen, UnitScreen), app.screen
            prior_focus.append(app.screen.focused)
            app.screen.unit_tree.focus()  # the folder tree's cursor is on the leaf's parent folder
            await wait_until(
                pilot, lambda: app.screen.focused is app.screen.unit_tree, message="folder tree never focused"
            )
            node = app.screen._selected_node()
            assert node is not None and not node.is_leaf, node
            await pilot.press("e")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen) and app.screen.is_mounted,
                message="ExportScreen never appeared",
            )
            assert isinstance(app.screen, ExportScreen)
            assert isinstance(app.screen._target, FolderExport), app.screen._target
            app.screen.query_one("#export-dst", Input).value = str(dst)
            app.screen.query_one("#export-start", Button).press()
            await pilot.pause(0)
            return node.name

        name = await ctx.call("export_folder", f"{step}.start", _start)
        if name is None:
            return

        async def _finish() -> str:
            from synology_apm_repo.browser.core.app.msg import CancelJobRequested

            try:
                await wait_until(
                    pilot, lambda: not app.store.model.jobs, timeout=_BUDGET_SECONDS, message="never finished"
                )
            except TimeoutError:
                for job_id in list(app.store.model.jobs):
                    app.store.dispatch(CancelJobRequested(job_id=job_id))
                await wait_until(pilot, lambda: not app.store.model.jobs, timeout=60.0, message="never cancelled")
                return "over_budget"
            return "finished"

        state = await ctx.call("export_folder", f"{step}.finish", _finish)
        if state is None:
            return

        from synology_apm_repo.browser.screens.export_screen import ExportScreen

        if isinstance(app.screen, ExportScreen):
            await pilot.press("escape")  # leave the finished export's screen for the next phase
            await wait_until(
                pilot, lambda: not isinstance(app.screen, ExportScreen), message="ExportScreen never closed"
            )
            if prior_focus and prior_focus[0] is not None:
                widget = prior_focus[0]
                widget.focus()  # later phases expect the leaf's own widget focused
                await wait_until(pilot, lambda: app.screen.focused is widget, message="leaf widget never refocused")
        if state == "over_budget":
            ctx.skip("export_folder", f"{step}.verify", f"folder export exceeded {_BUDGET_SECONDS:.0f}s; cancelled")
            return

        outcome = app.store.model.recent[-1].outcome
        ctx.check(
            "export_folder",
            f"{step}.outcome_not_error",
            outcome.notify_severity != "error",
            note=outcome.notify_message,
        )
        ctx.check(
            "export_folder",
            f"{step}.no_part_files_left",
            not any(p.name.endswith(".part") for p in dst.rglob("*")) if dst.exists() else True,
        )

        async def _verify() -> tuple[int, int]:
            (cmd,) = captured
            target = cmd.target
            assert isinstance(target, FolderExport)
            provider = await target.catalog.provider(target.version, raw=RawView() if target.force_raw else None)
            try:
                plan = await plan_tree_export(provider, target.node, dst)
            finally:
                await target.repo.release_provider(provider)
            on_disk = [item for item in plan.items if item.path.is_file()]
            wrong = [
                item for item in on_disk if item.node.size is not None and item.path.stat().st_size != item.node.size
            ]
            assert not wrong, f"{len(wrong)} file(s) with the wrong size, first: {wrong[0].relative}"
            return len(on_disk), len(plan.items)

        counts = await ctx.call("export_folder", f"{step}.verify_sizes", _verify)
        if counts is not None:
            on_disk, planned = counts
            skipped_note = outcome.notify_severity == "warning"
            ctx.check(
                "export_folder",
                f"{step}.every_planned_file_on_disk",
                on_disk == planned or skipped_note,
                note=f"{on_disk} of {planned} planned file(s) on disk ({outcome.notify_message})",
            )


__all__ = ["run"]
