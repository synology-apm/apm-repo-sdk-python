"""``RemoteOptionsBrowser``: ``ConnectDialog``'s bucket/container-listing
flow ("Browse" next to the S3 bucket / Azure container field), held
privately and reaching back into the dialog only through its small public
surface (``scanning``, ``s3_client_kwargs``/``azure_client_kwargs``,
``query_one``/``post_message``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING

from textual.message import Message
from textual.widgets import OptionList

from synology_apm_repo.browser.screens.profile_manager import _ProfileBackend
from synology_apm_repo.browser.strings import CONNECT_NETWORK_TIMEOUT_WARNING
from synology_apm_repo.browser.widgets.progress_hint import DebouncedProgress, StaticTextSink
from synology_apm_repo.sdk.profiles import BackendKind, list_remote_items

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog

# Backstop on top of the SDK client's own connect/read timeouts
# (storage/s3.py/storage/azure.py), in case a future client type doesn't
# honor them.
_NETWORK_TIMEOUT_SECONDS = 20


class RemoteOptionsBrowser:
    class ItemsListed(Message):
        """Posted once ``browse()`` reaches an outcome -- handled by
        ``ConnectDialog.on_remote_options_browser_items_listed``.
        ``error`` set means ``items`` is meaningless, and vice versa."""

        def __init__(self, *, option_list_id: str, noun: str, items: list[str], error: object | None) -> None:
            self.option_list_id = option_list_id
            self.noun = noun
            self.items = items
            self.error = error
            super().__init__()

    def __init__(self, dialog: ConnectDialog) -> None:
        self._dialog = dialog
        # Guards against a second browse firing mid-request for the same
        # backend -- cleared on every path out of browse().
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
        """Lists a backend's bucket-less/container-less items via
        ``list_remote_items`` and posts the outcome as one ``ItemsListed``
        message -- ``browse_buckets``/``browse_containers`` differ only in
        which backend's kind/kwargs/widget/noun they pass."""
        dialog = self._dialog
        if self.browsing[backend] or dialog.scanning:
            return
        self.browsing[backend] = True
        dialog.query_one(option_list_id, OptionList).remove_class("-visible")
        items: list[str] = []
        error: object | None = None
        try:
            with DebouncedProgress(
                dialog, StaticTextSink(dialog, "#connect-status", base=lambda: f"listing {noun}s...")
            ):
                items = await asyncio.wait_for(list_remote_items(kind, **kwargs_fn()), timeout=_NETWORK_TIMEOUT_SECONDS)
        except TimeoutError:
            error = CONNECT_NETWORK_TIMEOUT_WARNING
        except Exception as exc:  # backend failure (bad creds, no permission, ...) isn't an
            # ApmRepoError -- an expected, common outcome here, not a bug.
            error = exc
        finally:
            self.browsing[backend] = False
        dialog.post_message(self.ItemsListed(option_list_id=option_list_id, noun=noun, items=items, error=error))

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
