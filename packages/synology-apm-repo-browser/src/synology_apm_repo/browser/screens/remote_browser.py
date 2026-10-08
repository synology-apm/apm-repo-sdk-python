"""``RemoteOptionsBrowser``: ``ConnectDialog``'s "Browse" flow, listing
S3 buckets or Azure containers. It reaches the dialog only through
``scanning``, ``account_client_kwargs`` and ``query_one``/``post_message``,
and hosts its ``DebouncedProgress``/``StaticTextSink`` timers on it.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from textual.message import Message
from textual.widgets import OptionList

from synology_apm_repo.browser.strings import CONNECT_NETWORK_TIMEOUT_WARNING
from synology_apm_repo.browser.widgets.progress_hint import DebouncedProgress, StaticTextSink
from synology_apm_repo.sdk.profiles import BackendKind, list_remote_items

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog

# A backstop over the storage clients' own connect/read timeouts.
_NETWORK_TIMEOUT_SECONDS = 20


class RemoteOptionsBrowser:
    class ItemsListed(Message):
        """``browse()``'s outcome: ``items``, or an ``error``."""

        def __init__(self, *, option_list_id: str, noun: str, items: list[str], error: object | None) -> None:
            self.option_list_id = option_list_id
            self.noun = noun
            self.items = items
            self.error = error
            super().__init__()

    def __init__(self, dialog: ConnectDialog) -> None:
        self._dialog = dialog
        # Per backend: a browse is in flight.
        self.browsing: dict[BackendKind, bool] = dict.fromkeys(BackendKind, False)

    async def browse(
        self,
        backend: BackendKind,
        *,
        option_list_id: str,
        noun: str,
    ) -> None:
        """Lists ``backend``'s buckets/containers (``list_remote_items``)
        and posts the outcome as an ``ItemsListed``."""
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
                kwargs = dialog.account_client_kwargs(backend)
                items = await asyncio.wait_for(list_remote_items(backend, **kwargs), timeout=_NETWORK_TIMEOUT_SECONDS)
        except TimeoutError:
            error = CONNECT_NETWORK_TIMEOUT_WARNING
        except Exception as exc:  # noqa: BLE001 - a backend failure (bad credentials, no permission, ...)
            # is an expected outcome here, not a bug.
            error = exc
        finally:
            self.browsing[backend] = False
        dialog.post_message(self.ItemsListed(option_list_id=option_list_id, noun=noun, items=items, error=error))

    async def browse_buckets(self) -> None:
        await self.browse(
            BackendKind.S3,
            option_list_id="#connect-s3-bucket-list",
            noun="bucket",
        )

    async def browse_containers(self) -> None:
        await self.browse(
            BackendKind.AZURE,
            option_list_id="#connect-azure-container-list",
            noun="container",
        )
