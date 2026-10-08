"""``StoreScreen``: a ``NavigableScreen`` whose state lives in one ``Store``."""

from __future__ import annotations

from collections.abc import Callable

from textual.worker import Worker

from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.worker_drain import drain

from .navigable_screen import NavigableScreen


class StoreScreen[ModelT, MsgT, CmdT](NavigableScreen):
    """Owns its store's lifecycle. ``_open_store`` (from ``on_mount``)
    builds it. On unmount the store closes first, so a result still in
    flight finds it closed, then this screen's own workers are cancelled and
    drained, so none outlives the screen, then ``_after_store_closed``
    runs."""

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

    async def on_unmount(self) -> None:
        # Unset when on_mount failed before _open_store: the workers are
        # still drained, and there is no store state to tear down.
        store: Store[ModelT, MsgT, CmdT] | None = self.__dict__.get("store")
        if store is not None:
            store.close()
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
