"""``navigate`` domain: connect to a real sample and goto (``g``) straight
to a picked leaf, checking ``UnitScreen`` lands with it selected."""

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
        # ctx.call already recorded the failure.
        ctx.skip("navigate", f"navigate.{ref.sample_name}.goto", "connect step failed, see .connect above")
        return

    async def _goto() -> Tree[Binding[NodeRef]]:
        from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
        from synology_apm_repo.browser.screens.unit_screen import UnitScreen

        assert isinstance(app.screen, BrowseScreen), app.screen
        await pilot.press("g")
        app.screen.query_one("#goto-input", Input).value = ref.ref
        await pilot.press("enter")
        await wait_until(
            pilot,
            lambda: isinstance(app.screen, UnitScreen) and app.screen.is_mounted,
            message="UnitScreen never appeared",
        )
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
            # A leaf target is selected in the file table, with the folder
            # tree's cursor on its parent; _selected_node() covers either.
            selected = unit_screen._selected_node()
            # This session's scan and bootstrap's can root the same node at
            # different repo_paths; segments are the stable identity.
            return selected is not None and selected.ref.segments == ref.node.ref.segments

        landed = await ctx.call("navigate", f"navigate.{ref.sample_name}.cursor_on_leaf", _check_landed)
        if landed is not None:
            ctx.check("navigate", f"navigate.{ref.sample_name}.cursor_on_leaf.verified", landed)
        ctx.data["unit_tree"] = tree


__all__ = ["run"]
