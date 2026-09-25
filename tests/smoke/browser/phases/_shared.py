"""Real-sample navigation helpers shared across ``browser/`` phases --
adapted from ``tests/conftest.py``'s ``open_browser_pilot``/``wait_until``
pytest fixtures into plain async functions (this tool isn't part of the
pytest tree, so it can't use those directly), but the same underlying
mechanics: drive the auto-opened ``ConnectDialog``, then poll real
Textual state via ``pilot.pause()`` rather than a fixed sleep.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from textual.widgets import Button, Checkbox, Input, Select, Tabs, Tree

from synology_apm_repo.browser.core.browse.select import CatalogTreeKey
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.view.reconcile import Binding
from synology_apm_repo.sdk import AzureProfileConfig, BackendKind, S3ProfileConfig, SmbProfileConfig


async def wait_until(
    pilot: Any,
    condition: Callable[[], object],
    *,
    timeout: float = 10.0,  # noqa: ASYNC109 - a poll budget, not a cancellation scope; see docstring
    interval: float = 0.2,
    message: str = "condition not met before timeout",
) -> None:
    """Polls ``condition()`` via repeated ``pilot.pause(interval)`` calls
    until it returns truthy, raising ``TimeoutError(message)`` once
    ``timeout`` seconds have elapsed without that happening."""
    elapsed = 0.0
    while elapsed < timeout:
        await pilot.pause(interval)
        if condition():
            return
        elapsed += interval
    raise TimeoutError(message)


async def connect_local(app: Any, pilot: Any, path: str) -> None:
    """Drives the auto-opened ``ConnectDialog`` to connect a real local
    repository at ``path``, and waits for ``BrowseScreen``'s ``#col-catalogs``
    tree to gain the new repository's node. Callers only ever use this once per
    session: reconnecting a second source the same way (whether via ``c``
    or another auto-opened dialog) *replaces* the current scan's repositories/
    tree rather than adding to it (``BrowseScreen._reset_for_new_scan``),
    so a second sample needs its own fresh session (see
    ``key_dialog``'s/``remote_connect``'s own reasoning in
    ``__main__.py``), not a second call to this function in the same
    one."""
    await wait_until(pilot, lambda: isinstance(app.screen, ConnectDialog), message="ConnectDialog never appeared")
    dialog = app.screen
    assert isinstance(dialog, ConnectDialog), dialog
    # BrowseScreen is the persistent root screen (never popped, see
    # app.py's own on_mount), reachable even while ConnectDialog covers
    # it -- so "how many repositories were already connected" is known before
    # this connect adds one more. screen_stack[0] is Textual's own
    # implicit base screen, pushed before BrowseScreen -- look it up by
    # type rather than assuming a fixed index.
    browse_screen = next(s for s in app.screen_stack if isinstance(s, BrowseScreen))
    tree = browse_screen.query_one("#col-catalogs", Tree)
    before = len(tree.root.children)
    dialog.query_one("#connect-local-path", Input).value = path
    dialog.query_one("#connect-submit", Button).press()
    await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), message="BrowseScreen never appeared")
    await wait_until(
        pilot, lambda: len(tree.root.children) > before, message="new repository node never appeared in #col-catalogs"
    )


async def connect_remote(
    app: Any,
    pilot: Any,
    kind: BackendKind,
    *,
    profile_name: str | None = None,
    config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig | None = None,
    secrets: dict[str, str] | None = None,
) -> None:
    """Drives the auto-opened ``ConnectDialog`` to connect a real S3/Azure/
    SMB ``kind`` repository -- either through a saved profile
    (``profile_name`` given: picked from that tab's own profile ``Select``,
    which resolves the real keyring secret and refills every field) or by
    filling ``config``/``secrets`` into the raw fields directly
    (``profile_name`` omitted -- the same manual-entry path a first-time
    user goes through before ever saving a profile). Waits for
    ``BrowseScreen``'s ``#col-catalogs`` tree to gain the new repository's
    node. One connect per session, same as ``connect_local``: reconnecting
    a second source the same way *replaces* the current scan's
    repositories/tree rather than adding to it
    (``BrowseScreen._reset_for_new_scan``)."""
    await wait_until(pilot, lambda: isinstance(app.screen, ConnectDialog), message="ConnectDialog never appeared")
    dialog = app.screen
    assert isinstance(dialog, ConnectDialog), dialog
    browse_screen = next(s for s in app.screen_stack if isinstance(s, BrowseScreen))
    tree = browse_screen.query_one("#col-catalogs", Tree)
    before = len(tree.root.children)

    tab = {BackendKind.S3: "s3", BackendKind.AZURE: "azure", BackendKind.SMB: "smb"}[kind]
    dialog.query_one("#connect-backend-tabs", Tabs).active = tab

    if profile_name is not None:
        dialog.query_one(f"#connect-{tab}-profile-select", Select).value = profile_name
        # Lets the profile-load worker (SavedProfileManager.load_selected,
        # a keyring round trip) actually resolve the secret and refill the
        # form before #connect-submit reads it back.
        await pilot.pause(0.3)
    elif kind is BackendKind.S3:
        assert isinstance(config, S3ProfileConfig)
        secret_source = secrets or {}
        dialog.query_one("#connect-s3-bucket", Input).value = config.bucket
        dialog.query_one("#connect-s3-endpoint", Input).value = config.endpoint or ""
        dialog.query_one("#connect-s3-region", Input).value = config.region or ""
        dialog.query_one("#connect-s3-access-key", Input).value = secret_source.get("access_key", "")
        dialog.query_one("#connect-s3-secret-key", Input).value = secret_source.get("secret_key", "")
        dialog.query_one("#connect-s3-verify-tls", Checkbox).value = config.verify_tls
    elif kind is BackendKind.AZURE:
        assert isinstance(config, AzureProfileConfig)
        secret_source = secrets or {}
        dialog.query_one("#connect-azure-container", Input).value = config.container
        dialog.query_one("#connect-azure-account-url", Input).value = config.account_url or ""
        dialog.query_one("#connect-azure-credential", Input).value = secret_source.get("credential", "")
    else:
        assert isinstance(config, SmbProfileConfig)
        secret_source = secrets or {}
        dialog.query_one("#connect-smb-server", Input).value = config.server
        dialog.query_one("#connect-smb-share", Input).value = config.share
        dialog.query_one("#connect-smb-port", Input).value = str(config.port)
        dialog.query_one("#connect-smb-username", Input).value = config.username or ""
        dialog.query_one("#connect-smb-password", Input).value = secret_source.get("password", "")

    dialog.query_one("#connect-submit", Button).press()
    # Generous timeouts (vs. connect_local's default 10s): a real network
    # round trip to a live S3/Azure/SMB endpoint, not an in-memory local scan.
    await wait_until(
        pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=30.0, message="BrowseScreen never appeared"
    )
    await wait_until(
        pilot,
        lambda: len(tree.root.children) > before,
        timeout=30.0,
        message="new repository node never appeared in #col-catalogs",
    )


async def expand_first_repo(app: Any, pilot: Any) -> Tree[Binding[CatalogTreeKey]]:
    """Focuses ``#col-catalogs``, moves the cursor to the *last* repository
    node added (the one ``connect_local`` just connected -- earlier
    connects, if any, stay above it) and presses Enter, waiting for its
    connections to populate. Returns the tree, cursor already parked."""
    screen = app.screen
    assert isinstance(screen, BrowseScreen), screen
    tree = screen.query_one("#col-catalogs", Tree)
    tree.focus()
    repo_node = tree.root.children[-1]
    tree.move_cursor(repo_node)
    await pilot.press("enter")
    await wait_until(pilot, lambda: repo_node.children, message="repository node's connections never populated")
    return tree
