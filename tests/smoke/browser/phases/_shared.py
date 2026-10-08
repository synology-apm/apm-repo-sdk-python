"""Navigation helpers shared across ``browser/`` phases. ``wait_until`` is
``tests/support/pilot.py``'s, with budgets sized for real samples: this tool
runs as ``tests.smoke.*``, outside pytest's ``support.*`` import root, and
mypy would see one file under two module names if it imported it.

Each session connects one source: a second connect's ``RescanStarted``
replaces the first's tree rather than adding to it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from textual.css.query import NoMatches
from textual.pilot import Pilot
from textual.widgets import Button, Checkbox, Input, Select, Tabs, Tree

from synology_apm_repo.browser.core.browse.select import CatalogTreeKey
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.view.reconcile import Binding
from synology_apm_repo.sdk.profiles import (
    AzureProfileConfig,
    BackendKind,
    S3ProfileConfig,
    SmbProfileConfig,
    form_fields_for,
    profile_fields_with_secrets,
)


async def wait_until(
    pilot: Pilot[Any],
    condition: Callable[[], object],
    *,
    timeout: float = 10.0,  # noqa: ASYNC109 - a poll budget, not a cancellation scope
    interval: float = 0.2,
    message: str = "condition not met before timeout",
) -> None:
    """Polls ``condition()`` between ``pilot.pause(interval)`` turns until it
    is truthy; ``TimeoutError(message)`` after ``timeout`` seconds. A
    ``NoMatches`` counts as "not yet"."""
    elapsed = 0.0
    while elapsed < timeout:
        await pilot.pause(interval)
        try:
            met = condition()
        except NoMatches:
            met = False
        if met:
            return
        elapsed += interval
    raise TimeoutError(message)


async def connect_local(app: Any, pilot: Any, path: str) -> None:
    """Drives the auto-opened ``ConnectDialog`` to connect the local
    repository at ``path`` and waits for ``BrowseScreen``'s ``#col-catalogs``
    tree to gain its node."""
    await wait_until(
        pilot,
        lambda: isinstance(app.screen, ConnectDialog) and app.screen.is_mounted,
        message="ConnectDialog never appeared",
    )
    dialog = app.screen
    assert isinstance(dialog, ConnectDialog), dialog
    # BrowseScreen stays on the stack under ConnectDialog; found by type,
    # since screen_stack[0] is Textual's implicit base screen.
    browse_screen = next(s for s in app.screen_stack if isinstance(s, BrowseScreen))
    tree = browse_screen.query_one("#col-catalogs", Tree)
    before = len(tree.root.children)
    dialog.query_one("#connect-local-path", Input).value = path
    dialog.query_one("#connect-submit", Button).press()
    await wait_until(
        pilot,
        lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
        message="BrowseScreen never appeared",
    )
    await wait_until(
        pilot, lambda: len(tree.root.children) > before, message="new repository node never appeared in #col-catalogs"
    )


def _form_holds(dialog: ConnectDialog, kind: BackendKind, values: Mapping[str, str | bool | int]) -> bool:
    """Whether ``kind``'s text fields show ``values`` (absent ones blank)."""
    return all(
        dialog.query_one(f"#connect-{kind}-{field.name.replace('_', '-')}", Input).value
        == str(values.get(field.name, ""))
        for field in form_fields_for(kind)
        if not field.is_checkbox
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
    """Drives the auto-opened ``ConnectDialog`` to connect an S3/Azure/SMB
    repository, through the tab's saved-profile ``Select`` when
    ``profile_name`` is given, else by filling ``config``/``secrets`` into
    the raw fields. Waits for ``#col-catalogs`` to gain the new node."""
    await wait_until(
        pilot,
        lambda: isinstance(app.screen, ConnectDialog) and app.screen.is_mounted,
        message="ConnectDialog never appeared",
    )
    dialog = app.screen
    assert isinstance(dialog, ConnectDialog), dialog
    browse_screen = next(s for s in app.screen_stack if isinstance(s, BrowseScreen))
    tree = browse_screen.query_one("#col-catalogs", Tree)
    before = len(tree.root.children)

    tab = {BackendKind.S3: "s3", BackendKind.AZURE: "azure", BackendKind.SMB: "smb"}[kind]
    dialog.query_one("#connect-backend-tabs", Tabs).active = tab

    if profile_name is not None:
        expected = await profile_fields_with_secrets(profile_name)
        dialog.query_one(f"#connect-{tab}-profile-select", Select).value = profile_name
        # Selecting a profile refills the form from a worker (a keyring round
        # trip); submit only once the form holds the profile's values.
        await wait_until(
            pilot,
            lambda: _form_holds(dialog, kind, expected),
            message=f"the {tab} form never filled from profile {profile_name!r}",
        )
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
    # A live network scan: longer than connect_local's 10 s.
    await wait_until(
        pilot,
        lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
        timeout=30.0,
        message="BrowseScreen never appeared",
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
