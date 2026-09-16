"""Unit tests for ``UnitScreen``'s own provider close-on-discard behavior
— ``on_unmount`` (navigating away) and ``_reset_tree`` (refresh/verbose-
toggle) must each close whatever provider they're discarding rather than
just dropping the reference, or a session spent browsing many versions
leaks one ``SqliteSource``/``aiosqlite`` connection (and its own real
background thread) per version visited, for the rest of the session —
confirmed directly against real ``sample-1`` data: opening every version
in one workload without closing accumulated ~9 threads each (12 -> 805
threads for 88 versions); closing each one immediately after use keeps
the thread count flat throughout. Same ``_FakeApp``/``_FakeRepo``
convention as ``test_browser_unit_screen_gaps.py`` (duplicated here
rather than imported — see ``tests/CLAUDE.md``'s "no test module ever
imports from another"), plus a closable fake provider that tracks its
own ``close()`` calls.
"""

from __future__ import annotations

from typing import Any

from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.widgets import Tree

from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Version
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


class _FakeRepo:
    def __init__(self, catalog: _FakeCatalog) -> None:
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
        self.repo = repo
        self.verbose = False
        self._version = version

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(Screen())
        self.push_screen(UnitScreen(self.repo.catalog, self._version))  # type: ignore[arg-type]


async def test_navigating_away_closes_the_providers_own_connection(wait_until: Any) -> None:
    catalog = _FakeCatalog()
    app = _FakeApp(_version(), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
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
        tree = app.screen.query_one("#unit-tree", Tree)
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
    actually discards a provider) for a SaaS version — see that method's
    own docstring."""
    catalog = _FakeCatalog()
    app = _FakeApp(_version(target_type="M365"), _FakeRepo(catalog))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        assert len(catalog.provided) == 1
        first_provider = catalog.provided[0]

        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen.refresh_for_verbose_mode()
        await wait_until(pilot, lambda: len(catalog.provided) == 2)
        await wait_until(pilot, lambda: first_provider.close_calls > 0)
        assert first_provider.close_calls == 1


__all__: list[str] = []
