"""``navigate`` domain: connect to a real sample, drill via goto (``g``)
straight to a picked, real leaf -- the one thing ``sdk/`` smoke has no
equivalent of at all (real screen navigation against real content, not
an SDK call made directly)."""

from __future__ import annotations

from typing import Any

from textual.widgets import Input, Tree

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

    async def _goto() -> Tree[Any]:
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
        cursor_node = tree.cursor_node
        # Compare segments only, not the whole NodeRef: this session's
        # own connect_local() scanned from ref.repo_path directly, so its
        # repo_root (and thus repo_path) legitimately differs from
        # bootstrap's in-process session, which scanned from one level
        # higher for an unkeyed ref (see _shared_refs.py's RepoInfo.
        # narrow_repo_ref) -- same node, different scan root, different
        # repo_path label. segments are the real "which node" identity.
        landed = (
            cursor_node is not None
            and cursor_node.data is not None
            and cursor_node.data.ref.segments == ref.node.ref.segments
        )
        ctx.check("navigate", f"navigate.{ref.sample_name}.cursor_on_leaf", landed)
        ctx.data["unit_tree"] = tree


__all__ = ["run"]
