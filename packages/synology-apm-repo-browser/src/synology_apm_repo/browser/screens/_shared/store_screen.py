"""``StoreScreen``: a ``NavigableScreen`` whose state lives in one ``Store``."""

from __future__ import annotations

from collections.abc import Callable
from typing import override

from textual.worker import Worker

from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.worker_drain import drain

from .navigable_screen import NavigableScreen


class StoreScreen[ModelT, MsgT, CmdT](NavigableScreen):
    """Owns its store's lifecycle. ``_open_store`` (from ``on_mount``)
    builds it; it closes as the screen's removal starts
    (``_release_on_prune``), so a result still in flight finds it closed.
    On unmount this screen's own workers are drained, so none outlives the
    screen, then ``_after_store_closed`` runs."""

    store: Store[ModelT, MsgT, CmdT]

    def _open_store(
        self,
        model: ModelT,
        update: Callable[[ModelT, MsgT], tuple[ModelT, tuple[CmdT, ...]]],
        perform: Callable[[CmdT], None],
    ) -> None:
        """Builds ``self.store``; ``perform`` runs each command ``update``
        returns, from the first dispatch on."""
        self.store = Store(model, update, perform)

    def _close_store(self) -> Store[ModelT, MsgT, CmdT] | None:
        """Closes the store, if ``_open_store`` ran (``on_mount`` can fail
        before it); returns it. Idempotent."""
        store: Store[ModelT, MsgT, CmdT] | None = self.__dict__.get("store")
        if store is not None:
            store.close()
        return store

    @override
    def _release_on_prune(self) -> None:
        self._close_store()

    async def on_unmount(self) -> None:
        # Covers an unmount without a prune; a no-op after one.
        store = self._close_store()
        await drain(self._own_workers(cancel=True))
        if store is not None:
            await self._after_store_closed()

    async def _after_store_closed(self) -> None:
        """Teardown after the store is closed and this screen's workers are
        drained (only for a store that was opened); nothing by default."""

    def _own_workers(self, *, cancel: bool) -> list[Worker[None]]:
        """This screen's own workers (one an effect hosts on the App is
        not); ``cancel=True`` also cancels them."""
        workers = [w for w in self.workers if w.node is self]
        if cancel:
            for worker in workers:
                worker.cancel()
        return workers
