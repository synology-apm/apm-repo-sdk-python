"""``RemoteOptionsBrowser``: ``ConnectDialog``'s bucket/container-listing
flow ("Browse" next to the S3 bucket / Azure container field), split out
for the same reason ``goto_walker.py``'s ``GotoChainWalker`` is split out
of ``UnitScreen`` — see that module's own docstring for the convention
this follows. Held by ``ConnectDialog`` as a private collaborator,
reaching back into it only through the small public surface it exposes
for this (``scanning``, ``s3_client_kwargs``/``azure_client_kwargs``,
plus Textual's own ``query_one``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING

from textual.widgets import OptionList, Static

from synology_apm_repo.browser.screens._shared import show_error
from synology_apm_repo.browser.screens.profile_manager import _ProfileBackend
from synology_apm_repo.browser.strings import CONNECT_NETWORK_TIMEOUT_WARNING
from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.profiles import BackendKind, list_remote_items

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog

# Belt-and-suspenders on top of the SDK's own client-level connect/read
# timeouts (``storage/s3.py``/``storage/azure.py``): bounds the whole network
# call from this dialog's side too, so an unreachable endpoint can't hang
# "listing buckets/containers..." past a duration this interactive dialog
# can visibly recover from, even if a future client type doesn't honor
# the SDK-level config.
_NETWORK_TIMEOUT_SECONDS = 20


class RemoteOptionsBrowser:
    def __init__(self, dialog: ConnectDialog) -> None:
        self._dialog = dialog
        # Guards against a second browse firing mid-request for the same
        # backend — cleared on every path out of browse(), success or
        # failure alike (unlike ConnectDialog's own _scanning, browsing
        # never itself dismisses the dialog).
        self.browsing: dict[_ProfileBackend, bool] = dict.fromkeys(_ProfileBackend, False)

    async def browse(
        self,
        backend: _ProfileBackend,
        *,
        kind: BackendKind,
        kwargs_fn: Callable[[], dict[str, object]],
        option_list_id: str,
        noun: str,
    ) -> None:
        """Lists a backend's own bucket-less/container-less items via
        ``list_remote_items`` (neither ``S3Store`` nor ``AzureStore`` has
        an equivalent method, since every one of their own methods is
        already scoped to one chosen bucket/container), populates
        ``option_list_id``'s ``OptionList``, and updates
        ``#connect-status`` — ``browse_buckets``/``browse_containers``
        differ only in which backend's own kind/kwargs/widget/noun they
        pass."""
        dialog = self._dialog
        if self.browsing[backend] or dialog.scanning:
            return
        self.browsing[backend] = True
        status = dialog.query_one("#connect-status", Static)
        option_list = dialog.query_one(option_list_id, OptionList)
        option_list.remove_class("-visible")
        status.update(f"listing {noun}s...")
        try:
            items = await asyncio.wait_for(list_remote_items(kind, **kwargs_fn()), timeout=_NETWORK_TIMEOUT_SECONDS)
        except TimeoutError:  # the SDK client's own connect/read timeouts (storage/s3.py,
            # storage/azure.py) should fire well before this -- this is a backstop in
            # case they don't.
            show_error(dialog, "#connect-status", CONNECT_NETWORK_TIMEOUT_WARNING)
            return
        except Exception as exc:  # a real, backend-specific failure (bad creds, no
            # account-level list permission, unreachable endpoint, ...) isn't an
            # ApmRepoError -- same broad-catch rationale as ConnectDialog._scan()'s
            # own, since this is exactly the same class of "expected, common outcome
            # here, not a bug" failure.
            show_error(dialog, "#connect-status", exc)
            return
        finally:
            self.browsing[backend] = False
        if not items:
            status.update(f"no {noun}s found")
            return
        option_list.clear_options()
        for item in items:
            option_list.add_option(item)
        # ``OptionList.highlighted`` starts ``None`` — Enter's own ``action_select``
        # is a no-op with nothing highlighted, so the first option is
        # highlighted explicitly rather than leaving Enter dead until an
        # arrow key is pressed first.
        option_list.highlighted = 0
        option_list.add_class("-visible")
        option_list.focus()
        status.update(f"found {len(items)} {pluralize(len(items), noun)} — pick one")

    async def browse_buckets(self) -> None:
        await self.browse(
            _ProfileBackend.S3,
            kind=BackendKind.S3,
            kwargs_fn=self._dialog.s3_client_kwargs,
            option_list_id="#connect-s3-bucket-list",
            noun="bucket",
        )

    async def browse_containers(self) -> None:
        await self.browse(
            _ProfileBackend.AZURE,
            kind=BackendKind.AZURE,
            kwargs_fn=self._dialog.azure_client_kwargs,
            option_list_id="#connect-azure-container-list",
            noun="container",
        )
