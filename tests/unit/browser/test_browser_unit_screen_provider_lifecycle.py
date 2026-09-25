"""Unit tests for ``UnitScreen``'s own provider close-on-discard behavior
— ``on_unmount`` (navigating away) and a ``RootRequested`` reset
(refresh/verbose-toggle, handled by ``core/unit/update.py``'s own
``RootRequested`` case) must each close whatever provider they're
discarding rather than just dropping the reference, or a session spent
browsing many versions leaks one ``SqliteSource``/``aiosqlite``
connection (and its own real background thread) per version visited,
for the rest of the session -- closing each one immediately after use
keeps the thread count flat throughout instead. Same
``_FakeApp``/``_FakeRepo`` convention as this package's other
``UnitScreen`` test files (duplicated here rather than imported), plus a
closable fake provider that tracks its own ``close()`` calls.

Also covers ``UnitEffects._load_root``'s own epoch check: a fetch
superseded by a concurrent ``RootRequested`` reset while still in flight
must close its own provider rather than publish it, since cancellation
alone only lands at the worker's next ``await`` and cannot be trusted to
have landed by the time that check runs.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, cast

from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.widgets import Tree

from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.core.unit.msg import RootRequested
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Repository, Session, Version
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _version(target_type: str = "VM") -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid-1"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type=target_type,
        target_id=TargetId("target"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


class _ClosableProvider:
    """A minimal ``ClosableUnitProvider`` — one root leaf, no children —
    tracking whether/how many times ``close()`` actually ran, so a test
    can tell a real close from just dropping the reference."""

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


class _FakeCatalog:
    """Stands in for ``api.Catalog`` — returns a *fresh* ``_ClosableProvider``
    on every ``provider()`` call (unlike ``test_browser_unit_screen_gaps.py``'s
    own fake, which reuses one instance), since these tests need to tell
    each call's own provider apart to check it was individually closed."""

    def __init__(self) -> None:
        self.provided: list[_ClosableProvider] = []

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _ClosableProvider:
        provider = _ClosableProvider(f"provider-{len(self.provided)}")
        self.provided.append(provider)
        return provider


class _ThreadedProvider(_ClosableProvider):
    """Like ``_ClosableProvider``, but backed by a real background
    ``threading.Thread`` -- mirrors ``SqliteSource``'s own real
    one-background-thread-per-open-connection shape closely enough that
    a ``threading.active_count()``/``Thread.is_alive()`` assertion
    against it is genuine signal, not just a ``close_calls`` counter a
    provider that never actually held a thread would satisfy for free.
    Unclosed, this thread simply never stops."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._stop.wait, name=name, daemon=True)
        self._thread.start()

    async def close(self) -> None:
        await super().close()
        self._stop.set()
        self._thread.join(timeout=1.0)


class _ThreadedCatalog:
    """Same shape as ``_FakeCatalog``, returning a ``_ThreadedProvider``
    instead."""

    def __init__(self) -> None:
        self.provided: list[_ThreadedProvider] = []

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _ThreadedProvider:
        provider = _ThreadedProvider(f"provider-{len(self.provided)}")
        self.provided.append(provider)
        return provider


class _GatedCatalog:
    """Like ``_FakeCatalog``, but the *first* ``provider()`` call blocks on
    an ``asyncio.Event`` until the test releases it -- lets a test
    dispatch a fresh ``RootRequested`` (simulating a concurrent reset)
    while that first fetch is still in flight, proving ``update()``'s own
    stale-epoch check -- not cancellation, which only lands at the next
    await -- is what actually keeps the superseded provider from ever
    being published."""

    def __init__(self) -> None:
        self.provided: list[_ClosableProvider] = []
        self.gate = asyncio.Event()
        self.gated_once = False

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _ClosableProvider:
        if not self.gated_once:
            self.gated_once = True
            await self.gate.wait()
        provider = _ClosableProvider(f"provider-{len(self.provided)}")
        self.provided.append(provider)
        return provider


class _FakeRepo:
    def __init__(self, catalog: _FakeCatalog | _GatedCatalog | _ThreadedCatalog) -> None:
        self.catalog = catalog

    async def invalidate_directory_cache(self) -> None:
        pass


class _FakeApp(App[None]):
    """Pushes a base ``Screen`` first so ``UnitScreen`` (pushed on top of
    it) can be popped back off during a test — popping the *only* screen
    isn't a normal Textual flow, and popping is what triggers
    ``on_unmount``, the thing under test here."""

    def __init__(self, version: Version, repo: _FakeRepo) -> None:
        super().__init__()
        self._repo = repo
        self.verbose = False
        self.jobs: dict[JobId, Job] = {}
        self.resources = ResourceTable(cast(Session, object()))
        self.repo_handle: RepoHandle | None = self.resources.put_repo(cast(Repository, repo))
        self._version = version

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(Screen())
        self.push_screen(UnitScreen(self._repo.catalog, self._version))  # type: ignore[arg-type]


async def test_navigating_away_closes_the_providers_own_connection(wait_until: Any) -> None:
    catalog = _FakeCatalog()
    app = _FakeApp(_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        assert len(catalog.provided) == 1
        provider = catalog.provided[0]
        assert provider.close_calls == 0

        app.pop_screen()  # back to the base Screen -- UnitScreen itself is now unmounted
        await wait_until(pilot, lambda: provider.close_calls > 0)
        assert provider.close_calls == 1


async def test_refresh_closes_the_old_provider_before_the_new_one_loads(wait_until: Any) -> None:
    catalog = _FakeCatalog()
    app = _FakeApp(_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        assert len(catalog.provided) == 1
        first_provider = catalog.provided[0]

        await pilot.press("r")
        await wait_until(pilot, lambda: len(catalog.provided) == 2)
        await wait_until(pilot, lambda: first_provider.close_calls > 0)
        assert first_provider.close_calls == 1
        # The new provider replacing it is untouched -- only the discarded
        # one gets closed.
        assert catalog.provided[1].close_calls == 0


async def test_verbose_toggle_closes_the_old_saas_provider(wait_until: Any) -> None:
    """``refresh_for_verbose_mode`` only actually reloads (and so only
    actually discards a provider) for a SaaS version -- a Device/FS
    version's provider/tree are identical either way, so it skips the
    reload entirely."""
    catalog = _FakeCatalog()
    app = _FakeApp(_version(target_type="M365"), _FakeRepo(catalog))
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


async def test_a_superseded_in_flight_load_root_closes_its_own_provider_instead_of_publishing_it(
    wait_until: Any,
) -> None:
    catalog = _GatedCatalog()
    app = _FakeApp(_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        # The on_mount()-triggered dispatch's own effect is now stuck
        # inside catalog.provider() -- gated_once flips synchronously,
        # before the first await inside that call, so this is reliable
        # without a race window of its own.
        await wait_until(pilot, lambda: catalog.gated_once)

        # Simulate a concurrent reset (refresh/verbose-toggle) firing
        # while that first fetch is still in flight -- dispatching
        # RootRequested bumps the epoch, which is the only thing
        # update()'s own RootLoaded case actually depends on. Ungated
        # (_GatedCatalog only gates its *first* call), this second
        # fetch resolves immediately.
        screen.store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: len(catalog.provided) == 1)
        assert screen.store.model.provider is not None  # the reset's own fresh provider published normally

        # Release the original, now-superseded fetch.
        catalog.gate.set()
        await wait_until(pilot, lambda: len(catalog.provided) == 2)
        stale_provider = catalog.provided[1]
        await wait_until(pilot, lambda: stale_provider.close_calls > 0)

        assert stale_provider.close_calls == 1
        # The stale fetch's own provider was never published -- the
        # reset's own fresh one (asserted above) is still current.
        assert screen._current_provider() is not None
        assert screen._current_provider() is not stale_provider


async def test_repeated_refreshes_never_accumulate_live_provider_threads(wait_until: Any) -> None:
    """The exact regression this whole close-on-discard mechanism exists
    to prevent, reproduced at a small scale with a
    real background thread per provider (``_ThreadedProvider``) standing
    in for ``SqliteSource``'s own real one-thread-per-connection shape.
    Asserts the live thread count directly, both per-provider
    (``Thread.is_alive()``, precise
    and immune to unrelated threads elsewhere in the test process) and
    via ``threading.active_count()`` against a self-established
    baseline -- a provider that never actually held a thread (a plain
    ``close_calls`` counter alone) would pass a test that only checked
    call counts; this would not."""
    catalog = _ThreadedCatalog()
    app = _FakeApp(_version(), _FakeRepo(catalog))
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
            await wait_until(pilot, lambda total=expected_total: len(catalog.provided) == total)
            await wait_until(pilot, lambda p=old_provider: p.close_calls > 0)
            # Actually joined, not just marked closed -- close_calls > 0
            # alone wouldn't prove the real background thread stopped.
            assert not old_provider._thread.is_alive()
            assert threading.active_count() == baseline_thread_count

        # Every provider this session ever opened has been closed except
        # the one currently live.
        live_threads = sum(1 for p in catalog.provided if p._thread.is_alive())
        assert live_threads == 1
        assert len(catalog.provided) == 5


class _SlowToCancelChildrenProvider:
    """A container-rooted provider whose own ``children()`` blocks on a
    gate, then -- even once cancelled -- takes a further, real moment to
    actually finish (a ``finally`` clause blocking on a second,
    test-controlled ``cleanup_gate``, simulating a real close/cleanup step
    a genuine ``ClosableUnitProvider`` might run on its way out). Needed
    because a plain gated ``await`` alone resolves the *instant* it's
    cancelled, which would make a test unable to tell "drained, waited for
    it" apart from "cancelled and moved on immediately" -- the exact
    distinction
    ``test_refresh_drains_an_in_flight_children_fetch_before_dispatching_root_requested``
    below exists to prove. ``cleanup_gate`` is deliberately a second
    ``asyncio.Event``, not a fixed ``asyncio.sleep`` -- a real-time delay
    only proves the property on a machine fast enough to check before it
    elapses, which is exactly the kind of "wait for a duration, not a
    state" flake tests/CLAUDE.md warns against; a caller-controlled gate
    makes "cleanup has started but not finished" an observable, waitable
    state instead."""

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
    wait_until: Any, sdk_timeout: float
) -> None:
    """``action_refresh``/``refresh_for_verbose_mode`` must not dispatch
    ``RootRequested`` (whose own ``CloseProvider`` cmd closes the current
    provider on an App-hosted worker) while a screen-hosted
    ``LoadChildren`` fetch is still reading through that exact same
    provider -- the same "drain before close" discipline ``on_unmount``
    already established, now also required here too. Proven by
    ``_SlowToCancelChildrenProvider``, whose own cancellation takes a
    real, observable moment to finish -- with a provider that resolved
    its cancellation instantly, "waited for it" and "cancelled and moved
    on immediately" would be indistinguishable."""
    provider = _SlowToCancelChildrenProvider()

    async def _provider(
        version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _SlowToCancelChildrenProvider:
        return provider

    catalog = _FakeCatalog()
    catalog.provider = _provider  # type: ignore[method-assign, assignment]
    app = _FakeApp(_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: screen.store.model.root is not None)
        # The root's own auto-expand (UnitScreen._on_root_changed) fires a
        # ChildrenRequested for it -- wait until that fetch has genuinely
        # started (reached its own gate) before refreshing.
        await wait_until(pilot, lambda: provider.children_calls > 0)

        epoch_before = screen.store.model.epoch
        screen.action_refresh()
        # Wait for the cancelled fetch to genuinely reach its own cleanup
        # step (not just "one tick has passed") -- deterministic regardless
        # of real-time scheduling under a slow/busy runner.
        await wait_until(pilot, lambda: provider.cleanup_started.is_set(), timeout=sdk_timeout)

        # cleanup_gate is still held closed -- the cancelled fetch's own
        # cleanup hasn't finished yet, so RootRequested must not have been
        # dispatched already.
        assert screen.store.model.epoch == epoch_before

        provider.cleanup_gate.set()
        await wait_until(pilot, lambda: screen.store.model.epoch != epoch_before, timeout=sdk_timeout)


__all__: list[str] = []
