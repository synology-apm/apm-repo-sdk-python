"""Unit tests for ``UnitScreen`` closing the provider it discards: on
``on_unmount`` (navigating away) and on a ``RootRequested`` reset
(refresh/verbose toggle). An unclosed provider leaks its
``SqliteSource``/``aiosqlite`` connection and background thread for the
rest of the session.

Also covers ``core/unit/update.py``'s ``RootLoaded`` stale-epoch check: a
fetch superseded by a reset while in flight closes its provider instead of
publishing it, since cancellation only lands at the worker's next
``await``.
"""

from __future__ import annotations

import asyncio
import threading
from functools import partial
from typing import Any, cast

import pytest
from textual.screen import Screen
from textual.widgets import Tree

import synology_apm_repo.sdk.api as _sdk_api
from support.fakes import faithful_to
from support.model_factories import make_version
from support.pilot import SDK_TIMEOUT, wait_until
from synology_apm_repo.browser.core.unit.msg import RootRequested
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Catalog, RawView, Session, Version
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from synology_apm_repo.sdk.units.base import ClosableUnitProvider, Node, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.screen_host_fakes import ScreenHostApp


@faithful_to(ClosableUnitProvider)
class _ClosableProvider:
    def __init__(self, name: str) -> None:
        self.name = name
        self.close_calls = 0
        self._root = Node(ref=NodeRef("repo", ("root", name)), name=name, is_leaf=True)

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return []

    async def unit(self, node: Node) -> Any:  # pragma: no cover - never exercised here
        raise NotImplementedError

    async def close(self) -> None:
        self.close_calls += 1

    async def __aenter__(self) -> _ClosableProvider:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


@faithful_to(_sdk_api.Catalog)
class _FakeCatalog:
    """Stands in for ``api.Catalog``, returning a fresh ``_ClosableProvider``
    per ``provider()`` call (``unit_screen_fakes.py``'s reuses one) so each
    can be checked for its own close."""

    def __init__(self) -> None:
        self.provided: list[_ClosableProvider] = []

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(self, version: Version, *, raw: RawView | None = None) -> _ClosableProvider:
        provider = _ClosableProvider(f"provider-{len(self.provided)}")
        self.provided.append(provider)
        return provider


@faithful_to(ClosableUnitProvider)
class _ThreadedProvider(_ClosableProvider):
    """A ``_ClosableProvider`` holding a background thread until closed, like
    ``SqliteSource``'s connection thread, so a test can assert on live
    threads rather than only on ``close_calls``."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._stop.wait, name=name, daemon=True)
        self._thread.start()

    async def close(self) -> None:
        await super().close()
        self._stop.set()
        self._thread.join(timeout=1.0)


@faithful_to(Catalog)
class _ThreadedCatalog:
    """``_FakeCatalog`` returning ``_ThreadedProvider``s."""

    def __init__(self) -> None:
        self.provided: list[_ThreadedProvider] = []

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(self, version: Version, *, raw: RawView | None = None) -> _ThreadedProvider:
        provider = _ThreadedProvider(f"provider-{len(self.provided)}")
        self.provided.append(provider)
        return provider


@faithful_to(Catalog)
class _GatedCatalog:
    """``_FakeCatalog`` whose first ``provider()`` call blocks on ``gate``, so
    a test can reset (``RootRequested``) while that fetch is in flight."""

    def __init__(self) -> None:
        self.provided: list[_ClosableProvider] = []
        self.gate = asyncio.Event()
        self.gated_once = False

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(self, version: Version, *, raw: RawView | None = None) -> _ClosableProvider:
        if not self.gated_once:
            self.gated_once = True
            await self.gate.wait()
        provider = _ClosableProvider(f"provider-{len(self.provided)}")
        self.provided.append(provider)
        return provider


@faithful_to(_sdk_api.Repository)
class _FakeRepo:
    def __init__(self, catalog: _FakeCatalog | _GatedCatalog | _ThreadedCatalog) -> None:
        self.catalog = catalog

    async def invalidate_caches(self, *names: str) -> None:
        pass

    async def release_provider(self, provider: object) -> None:
        if isinstance(provider, ClosableUnitProvider):
            await provider.close()


class _FakeApp(ScreenHostApp):
    """Pushes a base ``Screen`` under ``UnitScreen`` so a test can pop it
    (triggering ``on_unmount``); popping the only screen isn't a normal
    Textual flow."""

    def __init__(self, version: Version, repo: _FakeRepo) -> None:
        super().__init__(repo=repo, session=cast(Session, object()))
        self._repo = repo
        self._version = version

    def screens(self) -> list[Screen[Any]]:
        return [Screen(), UnitScreen(self._repo.catalog, self._version)]  # type: ignore[arg-type]


async def test_navigating_away_closes_the_providers_own_connection() -> None:
    catalog = _FakeCatalog()
    app = _FakeApp(make_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        assert len(catalog.provided) == 1
        provider = catalog.provided[0]
        assert provider.close_calls == 0

        app.pop_screen()  # back to the base Screen -- UnitScreen itself is now unmounted
        await wait_until(pilot, lambda: provider.close_calls > 0)
        assert provider.close_calls == 1


async def test_refresh_closes_the_old_provider_before_the_new_one_loads() -> None:
    catalog = _FakeCatalog()
    app = _FakeApp(make_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        assert len(catalog.provided) == 1
        first_provider = catalog.provided[0]

        await pilot.press("r")
        await wait_until(pilot, lambda: len(catalog.provided) == 2)
        await wait_until(pilot, lambda: first_provider.close_calls > 0)
        assert first_provider.close_calls == 1
        assert catalog.provided[1].close_calls == 0


async def test_verbose_toggle_closes_the_old_saas_provider() -> None:
    """A verbose toggle on a SaaS version reloads and closes the old
    provider (a Device/FS version skips the reload)."""
    catalog = _FakeCatalog()
    app = _FakeApp(make_version(target_type="M365"), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        assert len(catalog.provided) == 1
        first_provider = catalog.provided[0]

        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen.refresh_for_verbose_mode()
        await wait_until(pilot, lambda: len(catalog.provided) == 2)
        await wait_until(pilot, lambda: first_provider.close_calls > 0)
        assert first_provider.close_calls == 1


async def test_a_superseded_in_flight_load_root_closes_its_own_provider_instead_of_publishing_it() -> None:
    catalog = _GatedCatalog()
    app = _FakeApp(make_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        # The mount's root fetch is now blocked inside catalog.provider().
        await wait_until(pilot, lambda: catalog.gated_once)

        # A reset while that fetch is in flight bumps the epoch; its own
        # (ungated) fetch resolves immediately.
        screen.store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: len(catalog.provided) == 1)
        assert screen.store.model.provider is not None  # the reset's provider is published

        # Release the original, now-superseded fetch.
        catalog.gate.set()
        await wait_until(pilot, lambda: len(catalog.provided) == 2)
        stale_provider = catalog.provided[1]
        await wait_until(pilot, lambda: stale_provider.close_calls > 0)

        assert stale_provider.close_calls == 1
        current = screen.store.model.provider
        assert current is not None
        assert screen.app_state.resources.provider(current) is catalog.provided[0]


async def test_repeated_refreshes_never_accumulate_live_provider_threads() -> None:
    catalog = _ThreadedCatalog()
    app = _FakeApp(make_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        assert len(catalog.provided) == 1
        assert catalog.provided[0]._thread.is_alive()
        baseline_thread_count = threading.active_count()

        for expected_total in range(2, 6):
            old_provider = catalog.provided[-1]
            await pilot.press("r")
            await wait_until(pilot, partial(lambda total: len(catalog.provided) == total, expected_total))
            await wait_until(pilot, partial(lambda provider: provider.close_calls > 0, old_provider))
            assert not old_provider._thread.is_alive()
            assert threading.active_count() == baseline_thread_count

        live_threads = sum(1 for p in catalog.provided if p._thread.is_alive())
        assert live_threads == 1
        assert len(catalog.provided) == 5


@faithful_to(UnitProvider)
class _SlowToCancelChildrenProvider:
    """A provider whose ``children()`` blocks on ``children_gate`` and, once
    cancelled, blocks again in ``finally`` on ``cleanup_gate``. A plain
    gated ``await`` finishes the instant it's cancelled, which would make
    "drained" and "cancelled and moved on" indistinguishable;
    ``cleanup_started`` makes the in-between state observable."""

    def __init__(self) -> None:
        self._root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
        self.children_gate = asyncio.Event()
        self.cleanup_started = asyncio.Event()
        self.cleanup_gate = asyncio.Event()
        self.children_calls = 0

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        self.children_calls += 1
        try:
            await self.children_gate.wait()
        finally:
            self.cleanup_started.set()
            await self.cleanup_gate.wait()
        return []  # pragma: no cover - this test only ever cancels the fetch, never releases the gate

    async def unit(self, node: Node) -> Any:  # pragma: no cover - never exercised here
        raise NotImplementedError


async def test_refresh_drains_an_in_flight_children_fetch_before_dispatching_root_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``action_refresh`` drains an in-flight ``LoadChildren`` fetch before
    dispatching ``RootRequested``, whose ``CloseProvider`` would close the
    provider that fetch is still reading through."""
    provider = _SlowToCancelChildrenProvider()

    async def _provider(version: Version, *, raw: RawView | None = None) -> _SlowToCancelChildrenProvider:
        return provider

    catalog = _FakeCatalog()
    monkeypatch.setattr(catalog, "provider", _provider)
    app = _FakeApp(make_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: screen.store.model.root is not None)
        # Wait for the root's auto-expand fetch to reach its gate.
        await wait_until(pilot, lambda: provider.children_calls > 0)

        epoch_before = screen.store.model.epoch
        screen.action_refresh()
        # Wait for the cancelled fetch to reach its cleanup step.
        await wait_until(pilot, lambda: provider.cleanup_started.is_set(), timeout=SDK_TIMEOUT)

        # cleanup_gate is still held closed -- the cancelled fetch's own
        # cleanup hasn't finished yet, so RootRequested must not have been
        # dispatched already.
        assert screen.store.model.epoch == epoch_before

        provider.cleanup_gate.set()
        await wait_until(pilot, lambda: screen.store.model.epoch != epoch_before, timeout=SDK_TIMEOUT)
