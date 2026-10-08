"""Fixtures scoped to ``tests/integration/browser/``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from integration.browser.pilot_drivers import ReplayLocalStore
from support.pilot import fast_browser_debounce as fast_browser_debounce
from support.store_fakes import WrappingStore
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.sdk.storage.base import ObjectStore


@pytest.fixture
def replay_local_store(
    monkeypatch: pytest.MonkeyPatch, record_target: Callable[..., Awaitable[ObjectStore]]
) -> ReplayLocalStore:
    """``await replay_local_store(fixture_name, *, label="vault-plain",
    allow_content=False)``: every local path the ``ConnectDialog`` opens
    resolves to ``record_target(fixture_name)``'s store, shown as ``label``.

    Going through ``record_target`` keeps the test its fixture's recording
    recipe (see ``tests/CLAUDE.md``). Each open gets its own
    ``WrappingStore``, whose ``close()`` leaves the shared store open: the
    app closes a repository's store when it closes the repository, and
    under ``--record-against`` the shared store is the session's
    ``RecordingStore`` over a real backend."""

    async def install(fixture_name: str, *, label: str = "vault-plain", allow_content: bool = False) -> None:
        store = await record_target(fixture_name, allow_content=allow_content)

        def build_local_store(self: ConnectDialog) -> tuple[ObjectStore, str]:
            return WrappingStore(store), label

        monkeypatch.setattr(ConnectDialog, "_build_local_store", build_local_store)

    return install
