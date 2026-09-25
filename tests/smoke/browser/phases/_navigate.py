"""``navigate`` domain: connect to a real sample, drill via goto (``g``)
straight to a picked, real leaf -- the one thing ``sdk/`` smoke has no
equivalent of at all (real screen navigation against real content, not
an SDK call made directly)."""

from __future__ import annotations

from typing import Any

from textual.widgets import Input, Tree

from synology_apm_repo.browser.view.reconcile import Binding
from synology_apm_repo.sdk.units.node_ref import NodeRef

from ..._shared_refs import RepresentativeRef
from .._context import SmokeContext
from ._shared import connect_local, expand_first_repo, wait_until


async def run(ctx: SmokeContext, app: Any, pilot: Any) -> None:
    ref: RepresentativeRef | None = ctx.data.get("main_ref")
    if ref is None:
        ctx.skip("navigate", "navigate.no_ref", "no representative ref discovered by bootstrap")
        return

    async def _connect() -> bool:
        await connect_local(app, pilot, ref.repo_path)
        await expand_first_repo(app, pilot)
        return True

    connected = await ctx.call("navigate", f"navigate.{ref.sample_name}.connect", _connect)
    if connected is not True:
        # ctx.call already recorded the real failure/traceback -- avoid a
        # second, confusing FAILED step from _goto asserting against a
        # screen state the failed connect never reached.
        ctx.skip("navigate", f"navigate.{ref.sample_name}.goto", "connect step failed, see .connect above")
        return

    async def _goto() -> Tree[Binding[NodeRef]]:
        from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
        from synology_apm_repo.browser.screens.unit_screen import UnitScreen

        assert isinstance(app.screen, BrowseScreen), app.screen
        await pilot.press("g")
        app.screen.query_one("#goto-input", Input).value = ref.ref
        await pilot.press("enter")
        await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), message="UnitScreen never appeared")
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen), unit_screen
        return unit_screen.unit_tree

    tree = await ctx.call("navigate", f"navigate.{ref.sample_name}.goto", _goto)
    if tree is not None:

        async def _check_landed() -> bool:
            from synology_apm_repo.browser.screens.unit_screen import UnitScreen

            unit_screen = app.screen
            if not isinstance(unit_screen, UnitScreen):
                return False
            # ref.node is always a leaf; a leaf target's own cursor lands on
            # the folder tree's parent node, while the leaf itself is
            # selected in the file table -- _selected_node() covers
            # whichever of the two is actually focused.
            selected = unit_screen._selected_node()
            # This session's own connect_local() scan and bootstrap's
            # separate in-process scan can resolve different repo_path
            # values for the same node (bootstrap scans one level higher
            # for an unkeyed ref) -- segments alone are the stable "which
            # node" identity across both.
            return selected is not None and selected.ref.segments == ref.node.ref.segments

        landed = await ctx.call("navigate", f"navigate.{ref.sample_name}.cursor_on_leaf", _check_landed)
        if landed is not None:
            ctx.check("navigate", f"navigate.{ref.sample_name}.cursor_on_leaf.verified", landed)
        ctx.data["unit_tree"] = tree


__all__ = ["run"]
