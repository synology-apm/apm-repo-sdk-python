"""``key_dialog`` domain: for one encrypted sample, ``ConnectDialog`` (no
key at connect time) -> entering a connection opens ``KeyDialog``
automatically -> paste the real key -> confirms unlocked. Key verification
itself is ``sdk/phases/_catalog.py``'s job; this checks the modal flow, in
its own ``App.run_test()`` session.
"""

from __future__ import annotations

from typing import Any

from textual.widgets import Input, Tree

from synology_apm_repo.browser.core.browse.select import CatalogTreeKey
from synology_apm_repo.browser.view.reconcile import Binding

from ..._shared_refs import RepresentativeRef
from .._context import SmokeContext
from ._shared import connect_local, expand_first_repo, wait_until


async def run(ctx: SmokeContext, app: Any, pilot: Any) -> None:
    ref: RepresentativeRef | None = ctx.data.get("encrypted_ref")
    if ref is None:
        ctx.skip("key_dialog", "key_dialog.no_encrypted_sample", "no encrypted, unambiguous sample configured")
        return

    async def _connect_and_enter_connection() -> Tree[Binding[CatalogTreeKey]]:
        await connect_local(app, pilot, ref.repo_path)
        tree = await expand_first_repo(app, pilot)
        repo_node = tree.root.children[-1]
        connection_node = repo_node.children[0]
        tree.move_cursor(connection_node)
        await pilot.press("enter")
        return tree

    tree = await ctx.call("key_dialog", f"key_dialog.{ref.sample_name}.connect", _connect_and_enter_connection)
    if tree is None:
        return

    async def _unlock() -> bool:
        from synology_apm_repo.browser.screens.key_dialog import KeyDialog

        await wait_until(
            pilot,
            lambda: isinstance(app.screen, KeyDialog) and app.screen.is_mounted,
            message="KeyDialog never appeared",
        )
        app.screen.query_one("#key-input", Input).value = ref.key
        await pilot.press("enter")
        await wait_until(
            pilot, lambda: not isinstance(app.screen, KeyDialog), timeout=15.0, message="KeyDialog never dismissed"
        )
        repo = app.current_repo
        return repo is not None and repo.key_status.value == "verified"

    verified = await ctx.call("key_dialog", f"key_dialog.{ref.sample_name}.unlock", _unlock)
    if verified is not None:
        ctx.check("key_dialog", f"key_dialog.{ref.sample_name}.unlock.verified", verified)


__all__ = ["run"]
