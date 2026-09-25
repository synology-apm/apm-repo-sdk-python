"""Unit tests for ``UnitScreen`` covering the branches none of this
package's other ``unit_screen``-focused
test files reach: repository/provider-level errors, the detail/preview/List-
overview edge cases, the "nothing selected" guards, load-more/filter
error and edge paths, and goto-ref's error/not-found handling. Driven
through a real Textual ``Pilot`` against a configurable fake provider,
same ``_FakeApp``/``_FakeRepo`` convention as
``test_browser_unit_screen_pagination.py``/``test_browser_unit_screen_children_error_handling.py``
(duplicated here rather than imported — see ``tests/CLAUDE.md``'s "no
test module ever imports from another")."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from typing import Any, cast

import pytest
from textual.app import App, ComposeResult
from textual.coordinate import Coordinate
from textual.widgets import DataTable, Input, Static, Tree

from synology_apm_repo.browser.core.app.cmd import AppCmd
from synology_apm_repo.browser.core.app.model import AppModel, Job
from synology_apm_repo.browser.core.app.msg import AppMsg
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.core.unit.update import CHILDREN_PAGE_SIZE
from synology_apm_repo.browser.runtime.app_effects import AppEffects
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens import unit_screen as unit_screen_module
from synology_apm_repo.browser.screens.unit_file_table import FileTable
from synology_apm_repo.browser.screens.unit_screen import _PREVIEW_READ_LIMIT, UnitScreen
from synology_apm_repo.browser.strings import (
    GOTO_REF_NOT_FOUND_WARNING,
    UNIT_COPY_REF_NOTHING_SELECTED_WARNING,
    UNIT_HEX_NOTHING_SELECTED_WARNING,
    UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING,
    UNIT_LOAD_MORE_ALREADY_LOADING_WARNING,
    UNIT_NOTHING_SELECTED_WARNING,
)
from synology_apm_repo.browser.view.reconcile import find_node
from synology_apm_repo.sdk.api import Repository, Session, Version
from synology_apm_repo.sdk.errors import ApmRepoError, ContentUnavailableError
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
from synology_apm_repo.sdk.units.base import FileState, Node, RestorableUnit, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.site import SITE_LIST_OVERVIEW_ATTR


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


class _ContentSource:
    """``size`` stays ``None`` until ``read`` has actually been called at
    least once -- mirroring ``LazyArtifact``'s own real behavior (the
    ``ContentSource`` every Teams/Chat page's ``unit()`` hands back),
    not the always-known-upfront shape a ``DedupFile``-backed source has.
    A fake that reported ``size`` eagerly would hide a real
    ``_load_preview`` bug: checking ``content.size`` before any read has
    happened always sees ``None`` for a genuine ``LazyArtifact``."""

    supports_concurrent_export = False

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._read = False

    @property
    def size(self) -> int | None:
        return len(self._data) if self._read else None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        self._read = True
        return self._data[offset:] if length is None else self._data[offset : offset + length]


class _ConfigurableProvider:
    """A plain ``UnitProvider`` over a hand-built ``ref -> children`` map,
    with a few knobs the other unit_screen test files' fakes don't need:
    ``raise_children_for`` (a set of ref strings whose ``children()``
    raises ``ApmRepoError``) and ``units_by_ref`` (for
    ``provider.unit()``, needed by preview/List-overview/export/hex)."""

    def __init__(
        self,
        root: Node,
        children_by_ref: dict[str, list[Node]] | None = None,
        *,
        raise_children_for: set[str] = frozenset(),  # type: ignore[assignment]
        units_by_ref: dict[str, RestorableUnit] | None = None,
    ) -> None:
        self._root = root
        self._children_by_ref = children_by_ref or {}
        self._raise_children_for = raise_children_for
        self._units_by_ref = units_by_ref or {}

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if str(node.ref) in self._raise_children_for:
            raise ApmRepoError(f"boom at {node.ref}")
        items = self._children_by_ref.get(str(node.ref), [])
        return items[offset : offset + limit] if limit is not None else items[offset:]

    async def unit(self, node: Node) -> RestorableUnit:
        unit = self._units_by_ref.get(str(node.ref))
        if unit is None:
            raise NotImplementedError(f"no fake unit registered for {node.ref}")
        return unit


class _FakeCatalog:
    """Stands in for ``api.Catalog`` — ``UnitScreen`` now dispatches its
    root provider through the catalog, not the repository directly (see
    ``unit_screen.py``'s own ``_load_root``)."""

    def __init__(self, provider: _ConfigurableProvider | None, *, provider_error: ApmRepoError | None = None) -> None:
        self._provider = provider
        self._provider_error = provider_error
        self.provider_calls = 0

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _ConfigurableProvider:
        self.provider_calls += 1
        if self._provider_error is not None:
            raise self._provider_error
        assert self._provider is not None
        return self._provider


class _FakeRepo:
    def __init__(
        self,
        provider: _ConfigurableProvider | None,
        *,
        provider_error: ApmRepoError | None = None,
        version_for_ref_error: ApmRepoError | None = None,
        version_for_ref_result: Version | None = None,
    ) -> None:
        self.catalog = _FakeCatalog(provider, provider_error=provider_error)
        self._version_for_ref_error = version_for_ref_error
        self._version_for_ref_result = version_for_ref_result
        self.invalidate_directory_cache_calls = 0

    async def version_for_ref(self, node_ref: NodeRef) -> tuple[_FakeCatalog, Version]:
        if self._version_for_ref_error is not None:
            raise self._version_for_ref_error
        assert self._version_for_ref_result is not None
        return self.catalog, self._version_for_ref_result

    async def verify(self, level: object, **kwargs: object) -> list[object]:
        # UnitScreen itself never calls this — only here because ``d``'s
        # own action_toggle_verbose -> action_show_diagnostics test below
        # pushes a real DiagnosticsScreen, which does.
        return []

    async def invalidate_directory_cache(self) -> None:
        self.invalidate_directory_cache_calls += 1


class _FakeApp(App[None]):
    """See ``test_browser_unit_screen_pagination.py``'s own identical
    class for why a bare ``App`` (not ``ApmRepoBrowserApp``) is enough
    here: ``UnitScreen`` only ever reads ``app_state.repo_handle``/
    ``.verbose``. ``store`` still needs to be a real, working one (not
    just a same-named attribute) -- ``action_export_selected`` pushes a
    real ``ExportScreen``, whose own ``on_mount`` subscribes to it."""

    def __init__(self, version: Version, repo: _FakeRepo, *, target_ref: NodeRef | None = None) -> None:
        super().__init__()
        self._repo = repo
        self.verbose = False
        self.jobs: dict[JobId, Job] = {}
        self.resources = ResourceTable(cast(Session, object()))
        self.repo_handle: RepoHandle | None = self.resources.put_repo(cast(Repository, repo))
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, self._perform)
        self.effects = AppEffects(self, self.store)
        self._version = version
        self._target_ref = target_ref

    def _perform(self, cmd: AppCmd) -> None:
        self.effects.perform(cmd)

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(UnitScreen(self._repo.catalog, self._version, target_ref=self._target_ref))  # type: ignore[arg-type]


def _leaf(name: str, ref_segment: str, **kwargs: object) -> Node:
    return Node(ref=NodeRef("repo", ("root", ref_segment)), name=name, is_leaf=True, **kwargs)  # type: ignore[arg-type]


# -- _load_root error path -----------------------------------------------


async def test_load_root_provider_error_shows_in_the_detail_pane(wait_until: Any) -> None:
    repo = _FakeRepo(None, provider_error=ApmRepoError("repo is locked"))
    app = _FakeApp(_version(), repo)
    async with app.run_test() as pilot:
        detail = app.screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "error:" in str(detail.render()))
        assert "repo is locked" in str(detail.render())


# -- expand-to-load / re-expand guard -------------------------------------


async def test_reexpanding_an_empty_but_already_loaded_container_does_not_refetch(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """A genuinely empty container (0 real children) still gets its own
    ``model.loaded`` entry once its fetch succeeds -- ``select.py``'s own
    ``_folder_node_spec`` then renders 0 widget children, the exact same
    shape a *never-requested* node also has. ``on_tree_node_expanded`` must
    tell the two apart via ``model.loaded`` membership, not
    ``event.node.children`` alone, or every collapse/re-expand re-fetches
    through the provider for a container that will only ever come back
    empty."""
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "empty-folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="empty-folder", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): [folder], str(folder_ref): []})
    children_calls: list[str] = []
    real_children = provider.children

    async def _counting_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        children_calls.append(str(node.ref))
        return await real_children(node, offset=offset, limit=limit)

    provider.children = _counting_children  # type: ignore[method-assign]
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        children_calls.clear()  # drop the root's own auto-expand fetch -- not under test here

        folder_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, folder_node)
        await pilot.press("space")  # expand -- dispatches ChildrenRequested for the empty folder
        await wait_until(pilot, lambda: folder_ref in screen.store.model.loaded)
        assert children_calls == [str(folder_ref)]
        assert list(folder_node.children) == []

        await pilot.press("space")  # collapse
        await pilot.pause()
        await pilot.press("space")  # re-expand -- must not re-fetch
        await pilot.pause()

        assert children_calls == [str(folder_ref)], "re-expanding an empty-but-loaded container must not re-fetch"


async def test_reexpanding_a_node_while_its_children_fetch_is_still_loading_does_not_refetch(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """Textual's own ``auto_expand`` toggles the node on every Enter/space
    press, same as ``BrowseScreen``'s column 1 -- pressing it again while
    the first ``ChildrenRequested`` is still ``Loading`` must not dispatch
    a second one, or each duplicate spawns its own worker and its own
    loading-indicator sink on this same node. ``UnitModel.pending`` holds
    a node's children-fetch slot from dispatch until its current result
    lands, which is what makes "still loading" askable at all here --
    unlike ``model.loaded``, which only ever records a *finished* fetch."""
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [folder], str(folder_ref): [leaf]})
    real_children = provider.children
    children_calls: list[str] = []
    gate = asyncio.Event()

    async def _gated_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if str(node.ref) == str(folder_ref):
            children_calls.append(str(node.ref))
            await gate.wait()
        return await real_children(node, offset=offset, limit=limit)

    provider.children = _gated_children  # type: ignore[method-assign]
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        folder_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, folder_node)
        await pilot.press("space")  # expand -- starts the gated fetch
        await wait_until(pilot, lambda: children_calls == [str(folder_ref)])

        await pilot.press("space")  # collapse
        await pilot.press("space")  # re-expand while still loading -- must not refetch
        await pilot.pause()
        assert children_calls == [str(folder_ref)], "re-expanding while still loading must not refetch"

        gate.set()
        await wait_until(pilot, lambda: folder_ref in screen.store.model.loaded)
        assert children_calls == [str(folder_ref)]


# -- detail / preview -------------------------------------------------


async def test_detail_shows_ref_and_attrs_only_in_verbose_mode(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item", attrs={"custom": "value"})
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        detail_non_verbose = str(screen.query_one("#detail", Static).render())

        app.verbose = True
        screen._show_detail(leaf)
        detail_verbose = str(screen.query_one("#detail", Static).render())

        assert "ref:" not in detail_non_verbose
        assert "custom" not in detail_non_verbose
        assert "ref:" in detail_verbose
        assert "custom: value" in detail_verbose


async def test_content_only_kind_shows_no_header_outside_verbose_mode(wait_until: Any) -> None:
    """Mail/Calendar-event/Contact leaves get no Name/kind/size/modified
    header at all -- their own parsed preview already states the
    identity a generic header would just repeat (see
    ``core/unit/select.py``'s ``is_content_only_preview``)."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("mail-1.eml", "mail-1", kind=UnitKind.MAIL, attrs={"custom": "value"})
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        detail_non_verbose = str(screen.query_one("#detail", Static).render())
        assert detail_non_verbose == ""

        app.verbose = True
        screen._show_detail(leaf)
        detail_verbose = str(screen.query_one("#detail", Static).render())
        assert "mail-1.eml" not in detail_verbose
        assert "kind:" not in detail_verbose
        assert "ref:" in detail_verbose
        assert "custom: value" in detail_verbose


async def test_content_only_kind_preview_has_no_header_or_separator(wait_until: Any) -> None:
    """``append_preview`` renders the preview text alone when the header
    is empty -- no separator bar dividing it from a header that isn't
    there, and no leading blank line either."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("mail-1.eml", "mail-1", kind=UnitKind.MAIL)
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        screen._detail_pane.append_preview(leaf, "From: alice@example.com\nHello")
        detail = str(screen.query_one("#detail", Static).render())
        assert detail == "From: alice@example.com\nHello"


async def test_content_only_kind_preview_failure_shows_an_inline_error_not_a_blank_pane(
    wait_until: Any, monkeypatch: pytest.MonkeyPatch, wait_for_detail_content: Any
) -> None:
    """A content-only kind's own header is empty
    (``test_content_only_kind_shows_no_header_outside_verbose_mode``), so
    when ``preview_renderer_for``'s own render function raises,
    ``_load_preview`` must still leave *something* on screen -- never a
    completely blank pane with no indication anything was even
    selected."""

    def _raise(data: bytes) -> str:
        raise ValueError("malformed preview bytes")

    monkeypatch.setattr(unit_screen_module, "preview_renderer_for", lambda node: _raise)
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("mail-1.eml", "mail-1", kind=UnitKind.MAIL)
    unit = RestorableUnit(ref=leaf.ref, name=leaf.name, is_leaf=True, content=_ContentSource(b"anything"))  # type: ignore[arg-type]
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        # contains= pins the wait to this known substring so it can't be
        # satisfied by DebouncedProgress's transient "(loading)" cue.
        await wait_for_detail_content(pilot, screen, contains="malformed preview bytes")
        detail = str(screen.query_one("#detail", Static).render())
        assert "malformed preview bytes" in detail


async def test_content_unavailable_preview_failure_shows_a_note_not_an_error(
    wait_until: Any, wait_for_detail_content: Any
) -> None:
    """A cloud-sync placeholder/EFS-encrypted file's own
    ``ContentUnavailableError`` is an expected, already-explained state --
    unlike ``test_content_only_kind_preview_failure_shows_an_inline_error_not_a_blank_pane``'s
    genuine rendering failure, this must render as ``append_preview_note``'s
    informational styling (the exception's own message, unchanged), never
    ``append_preview_error``'s red styling."""

    class _UnavailableContentSource:
        supports_concurrent_export = False
        size: int | None = None

        async def read(self, offset: int = 0, length: int | None = None) -> bytes:
            raise ContentUnavailableError("cloud-sync placeholder -- no data at backup time")

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("cloud.bin", "cloud", attrs={"file_state": FileState.CLOUD_ONLY})
    unit = RestorableUnit(ref=leaf.ref, name=leaf.name, is_leaf=True, content=_UnavailableContentSource())  # type: ignore[arg-type]
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        await wait_for_detail_content(pilot, screen, contains="cloud-sync placeholder")
        detail = str(screen.query_one("#detail", Static).render())
        assert "note:" in detail
        assert "error:" not in detail


def _fake_channel_msg_div(i: int) -> str:
    return (
        f'<div class="msg"><div class="avatar avatar-0">U</div>'
        f'<div class="content"><div class="hdr">User{i} &middot; t{i}</div>'
        f'<div class="body">message number {i}</div></div></div>'
    )


async def test_teams_chat_message_preview_reads_the_tail_when_content_exceeds_the_read_cap(
    wait_until: Any, wait_for_detail_content: Any
) -> None:
    """A Teams/Chat message page's own content is a chronological
    transcript (oldest message first) -- when the real content exceeds
    ``_PREVIEW_READ_LIMIT``, ``_load_preview`` must read from the *end*
    of it (``core/unit/select.py``'s ``prefers_recent_content``), so the
    newest message is what actually reaches the parser, not the oldest
    one a plain head read would have kept instead."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("General", "channel-1", kind=UnitKind.TEAMS_CHAT_MESSAGE)
    body = "".join(_fake_channel_msg_div(i) for i in range(3000))
    html = f"<!doctype html><html><body>{body}</body></html>".encode()
    assert len(html) > _PREVIEW_READ_LIMIT, "the scenario this test exists to cover"
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=_ContentSource(html),  # type: ignore[arg-type]
    )
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        # contains= pins the wait to this known substring so it can't be
        # satisfied by DebouncedProgress's transient "(loading)" cue.
        await wait_for_detail_content(pilot, screen, contains="message number 2999")
        detail = str(screen.query_one("#detail", Static).render())
        assert "message number 2999" in detail
        assert "message number 0" not in detail


async def test_detail_pane_size_line_notes_zero_bytes_on_disk_for_a_cloud_file(wait_until: Any) -> None:
    """``DetailPane.header_text``'s ``size:`` line (shown unconditionally,
    not gated behind verbose mode — same as ``kind:``) appends
    ``(0 Byte on disk)`` for a leaf ``Node.attrs["file_state"]`` flags as a
    cloud-sync placeholder: ``node.size`` is still the guest OS's own
    declared/logical size, not what's actually resident on this
    backup's disk."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    normal_leaf = _leaf("normal.bin", "normal", size=1572864)
    cloud_leaf = _leaf("cloud.bin", "cloud", size=1572864, attrs={"file_state": FileState.CLOUD_ONLY})
    provider = _ConfigurableProvider(root, {str(root_ref): [normal_leaf, cloud_leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(normal_leaf)
        normal_detail = str(screen.query_one("#detail", Static).render())
        screen._show_detail(cloud_leaf)
        cloud_detail = str(screen.query_one("#detail", Static).render())

        assert "size: 1.5 MiB" in normal_detail
        assert "on disk" not in normal_detail
        assert "size: 1.5 MiB (0 Byte on disk)" in cloud_detail


async def test_detail_pane_size_line_has_no_on_disk_caveat_for_an_encrypted_file(wait_until: Any) -> None:
    """Unlike ``FileState.CLOUD_ONLY``, ``FileState.ENCRYPTED`` gets no
    ``(0 Byte on disk)`` caveat — an EFS-encrypted file's real bytes
    genuinely are on disk, just undecryptable, so ``node.size`` isn't
    misleading the way a cloud placeholder's is."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    encrypted_leaf = _leaf("secret.docx", "secret", size=1572864, attrs={"file_state": FileState.ENCRYPTED})
    provider = _ConfigurableProvider(root, {str(root_ref): [encrypted_leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(encrypted_leaf)
        encrypted_detail = str(screen.query_one("#detail", Static).render())

        assert "size: 1.5 MiB" in encrypted_detail
        assert "on disk" not in encrypted_detail


async def test_file_table_label_shows_the_cloud_file_icon(wait_until: Any) -> None:
    """A child whose ``Node.attrs["file_state"]`` the SDK set (via
    ``_Format.content_unavailable``/``_apfs_is_dataless``) gets
    ``FILE_STATE_ICON``'s matching glyph rendered in its own untitled
    column between Name and Size, in the default view (not gated behind
    verbose mode) — its own fixed-width column so it doesn't shift
    Size/Modified sideways depending on name length (unlike the folder
    tree's single-column label, still inline via ``node_label()``'s
    suffix -- these are ordinary leaves, so they render in the file
    table, never the folder tree, which excludes leaves entirely)."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    normal_leaf = _leaf("normal.txt", "normal")
    cloud_leaf = _leaf("cloud.txt", "cloud", attrs={"file_state": FileState.CLOUD_ONLY})
    encrypted_leaf = _leaf("secret.docx", "secret", attrs={"file_state": FileState.ENCRYPTED})
    provider = _ConfigurableProvider(root, {str(root_ref): [normal_leaf, cloud_leaf, encrypted_leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#file-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 3)
        rows = {str(table.get_row_at(i)[0]): str(table.get_row_at(i)[1]) for i in range(table.row_count)}
        assert rows["normal.txt"] == ""
        assert rows["cloud.txt"] == "☁"
        assert rows["secret.docx"] == "🔒"


async def test_a_name_shaped_like_rich_markup_does_not_crash_the_tree_or_the_file_table(
    wait_until: Any,
) -> None:
    """Both ``Tree.process_label`` and ``DataTable``'s own
    ``default_cell_formatter`` re-parse a plain ``str`` label/cell as Rich
    markup -- a real backup-derived name shaped like a bare closing tag
    (``"a[/]b"``) raises ``rich.errors.MarkupError`` at paint time if it
    ever reaches either widget unescaped -- ``node_label()`` escapes
    ``node.name`` via ``safe()`` for exactly this reason before handing it
    to either. Drives a
    real Pilot render (not just the pure ``select.py`` unit test) so this
    catches a regression at the actual paint boundary, not just the
    selector's own output."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "folder")), name="a[/]b folder", is_leaf=False)
    leaf = _leaf("a[/]b.txt", "leaf")
    provider = _ConfigurableProvider(root, {str(root_ref): [folder, leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#file-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 2)
        await pilot.pause()  # forces the DataTable's own deferred _on_idle paint pass
        tree = app.screen.query_one("#folder-tree", Tree)
        assert len(tree.root.children) == 1  # the folder, not the leaf


async def test_a_version_display_name_shaped_like_rich_markup_does_not_crash_the_breadcrumb_or_tree_root(
    wait_until: Any,
) -> None:
    """``Version.display_name`` is normally a pure formatted timestamp,
    but degrades to the raw ``version_uid`` on corrupt/unparseable
    catalog data (``catalog/version.py``'s own ``_version_display_name``)
    -- an internal identifier, not strictly guaranteed markup-free.
    ``compose()`` feeds it to both the breadcrumb ``Static`` and the
    folder tree's own root label; both re-parse a plain ``str`` as Rich
    markup (``Static.update()``/``Tree.process_label`` each call
    ``Text.from_markup``, same as every other widget ``safe()`` protects)."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): []})
    marked_up_version = dataclasses.replace(_version(), display_name="a[/]b")
    app = _FakeApp(marked_up_version, _FakeRepo(provider))
    # Tree.__init__ calls process_label() synchronously on its own
    # constructor argument -- an unescaped display_name would raise
    # MarkupError right here, inside compose(), before the app ever
    # finishes mounting (a crash run_test()'s own context manager would
    # propagate, not something wait_until below could catch after the
    # fact).
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        await pilot.pause()
        # Once escaped, the rendered plain text correctly reads back the
        # real, unmangled input, not the raw backslash-escaped markup
        # source -- proven on the breadcrumb, which (unlike the tree's
        # own root label) is never overwritten by a later reconcile.
        breadcrumb = screen.query_one("#breadcrumb", Static)
        assert "a[/]b" in str(breadcrumb.render())


async def test_show_detail_and_load_preview_are_no_ops_without_a_provider(wait_until: Any) -> None:
    """No provider ever loads at all here -- catalog.provider() itself
    fails, so model.provider stays None throughout, rather than trying
    to force a live screen's own already-loaded provider back to None
    (which nothing but a fresh RootRequested dispatch can do, and that
    would immediately start loading a new one)."""
    app = _FakeApp(_version(), _FakeRepo(None, provider_error=ApmRepoError("boom")))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: screen.store.model.root_error is not None)
        assert screen._current_provider() is None

        leaf = _leaf("item.bin", "item")
        screen._show_detail(leaf)
        header_only = str(screen.query_one("#detail", Static).render())
        screen._load_preview(leaf)  # must not raise
        await pilot.pause()
        # No provider -> _load_preview returns immediately; the detail
        # pane still shows only the header _show_detail() wrote.
        assert str(screen.query_one("#detail", Static).render()) == header_only


async def test_late_preview_for_a_node_the_user_moved_away_from_is_discarded(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=_ContentSource(b"<html><body><p>hello world</p></body></html>"),  # type: ignore[arg-type]
    )
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)  # sets _detail_pane._node = leaf, schedules a preview worker
        header_only = str(screen.query_one("#detail", Static).render())
        screen._detail_pane._node = None  # simulate having moved on before the worker resolves
        await pilot.pause(0.1)  # fixed pause: asserting an absence has no readiness signal to wait for
        # The preview must never have been appended — the detail pane
        # still shows only the header _show_detail() itself wrote.
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "hello world" not in header_only


# -- SharePoint List overview -------------------------------------------


def _list_overview_node() -> Node:
    return Node(
        ref=NodeRef("repo", ("root", "list")), name="MyList", is_leaf=False, attrs={SITE_LIST_OVERVIEW_ATTR: True}
    )


async def test_selecting_a_list_overview_group_does_not_change_model_selected(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """A SharePoint List-overview group never has real file-table
    contents of its own (``file_table_rows`` always renders ``()`` for
    one), so selecting it in the folder tree must leave ``model.selected``
    -- and so the file table, and ``+``/``/`` -- pointed at whichever real
    folder was selected before, not silently retarget them at a level
    that can never be loaded. The detail pane still reflects the group
    itself via ``_show_detail``'s own ``is_list_overview`` branch,
    independent of ``model.selected``."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = _ConfigurableProvider(root, {str(root_ref): [overview_node]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = screen.unit_tree
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        assert screen.store.model.selected == root_ref

        overview_tree_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, overview_tree_node)
        await pilot.press("enter")

        assert screen.store.model.selected == root_ref  # unchanged -- never the group's own ref
        detail = screen.query_one("#detail", Static)
        assert "MyList" in str(detail.render())  # the detail pane did update to the group


async def test_activating_a_list_overview_row_in_the_file_table_never_expands_it(
    wait_until: Any,
) -> None:
    """A SharePoint List-overview group's own ``TreeNode`` gets
    ``allow_expand=False`` (``folder_tree_spec``'s own ``is_container``
    decision) -- unlike the keyboard/mouse path (``Tree._toggle_node``),
    ``TreeNode.expand()`` doesn't check that flag itself, so calling it
    unconditionally here would post a real ``NodeExpanded`` for a node
    ``on_tree_node_expanded`` can't tell apart from an ordinary
    never-requested container, dispatching a ``ChildrenRequested``
    through the wrong (ordinary paginated) path for a node whose
    contents are only ever read via ``_load_list_overview``."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = _ConfigurableProvider(root, {str(root_ref): [overview_node]})
    children_calls: list[str] = []
    real_children = provider.children

    async def _counting_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        children_calls.append(str(node.ref))
        return await real_children(node, offset=offset, limit=limit)

    provider.children = _counting_children  # type: ignore[method-assign]
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        children_calls.clear()  # drop root's own auto-expand fetch -- not under test here

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        row = next(i for i, n in enumerate(screen._file_table._nodes) if n is overview_node)
        table.cursor_coordinate = Coordinate(row, 0)
        await pilot.press("enter")
        await pilot.pause()

        # _load_list_overview's own direct provider.children() call is
        # legitimate and expected -- a List's items are never
        # tree-navigable at all, so this is the only place they're ever
        # fetched -- exactly one. A second call would
        # mean the erroneous ChildrenRequested/ordinary-paginated path
        # fired too.
        assert children_calls.count(str(overview_node.ref)) == 1
        assert overview_node.ref not in screen.store.model.loaded, (
            "a List-overview group's items must never be routed through the store's own "
            "ChildrenRequested/model.loaded path"
        )

        tree_node = find_node(screen.unit_tree.root, overview_node.ref)
        assert tree_node is not None
        assert not tree_node.is_expanded


async def test_list_overview_is_a_no_op_without_a_provider(wait_until: Any) -> None:
    """Same "provider never loads at all" setup as
    test_show_detail_and_load_preview_are_no_ops_without_a_provider."""
    app = _FakeApp(_version(), _FakeRepo(None, provider_error=ApmRepoError("boom")))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: screen.store.model.root_error is not None)
        assert screen._current_provider() is None
        detail_before = str(screen.query_one("#detail", Static).render())
        screen._load_list_overview(_list_overview_node())  # must not raise
        await pilot.pause()
        # No provider -> _load_list_overview returns immediately, before
        # ever touching the detail pane.
        assert str(screen.query_one("#detail", Static).render()) == detail_before


async def test_list_overview_children_error_shows_in_detail_pane(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = _ConfigurableProvider(
        root, {str(root_ref): [overview_node]}, raise_children_for={str(overview_node.ref)}
    )
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        detail = screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "error:" in str(detail.render()))
        assert f"boom at {overview_node.ref}" in str(detail.render())


async def test_list_overview_error_for_a_node_the_user_moved_away_from_is_discarded(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = _ConfigurableProvider(
        root, {str(root_ref): [overview_node]}, raise_children_for={str(overview_node.ref)}
    )
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        header_only = str(screen.query_one("#detail", Static).render())
        screen._detail_pane._node = None  # simulate having moved on before the worker resolves
        await pilot.pause(0.1)  # fixed pause: asserting an absence has no readiness signal to wait for
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "error:" not in header_only


async def test_list_overview_success_for_a_node_the_user_moved_away_from_is_discarded(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = _ConfigurableProvider(root, {str(root_ref): [overview_node]})  # empty children -> "(no items)"
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        header_only = str(screen.query_one("#detail", Static).render())
        screen._detail_pane._node = None  # simulate having moved on before the worker resolves
        await pilot.pause(0.1)  # fixed pause: asserting an absence has no readiness signal to wait for
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "(no items)" not in header_only


async def test_list_overview_skips_nested_folders_and_malformed_items_and_reports_empty(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    nested_folder = Node(ref=NodeRef("repo", ("root", "list", "folder")), name="folder", is_leaf=False)
    malformed_item = _leaf("bad", "bad")
    malformed_unit = RestorableUnit(
        ref=malformed_item.ref,
        name=malformed_item.name,
        is_leaf=True,
        content=_ContentSource(b"not json"),  # type: ignore[arg-type]
    )
    provider = _ConfigurableProvider(
        root,
        {str(root_ref): [overview_node], str(overview_node.ref): [nested_folder, malformed_item]},
        units_by_ref={str(malformed_item.ref): malformed_unit},
    )
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        detail = screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "(no items)" in str(detail.render()))
        assert "(no items)" in str(detail.render())  # the folder was skipped, the malformed item discarded


async def test_list_overview_fetches_every_items_own_content_concurrently(wait_until: Any, sdk_timeout: float) -> None:
    """Each item's own content fetch (``provider.unit()`` + a read) is
    independent of every other's -- with three items whose own
    ``unit()`` calls block on a shared gate, all three must actually be
    in flight at once (this test's own ``entered`` event only fires once
    every one of them has started) rather than one at a time; a
    sequential implementation would never fire it and this test would
    time out instead of failing cleanly."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    items = [_leaf(f"item-{i}", f"item-{i}") for i in range(3)]
    units_by_ref = {
        str(item.ref): RestorableUnit(
            ref=item.ref,
            name=item.name,
            is_leaf=True,
            content=_ContentSource(json.dumps({"Title": item.name}).encode()),  # type: ignore[arg-type]
        )
        for item in items
    }
    provider = _ConfigurableProvider(
        root, {str(root_ref): [overview_node], str(overview_node.ref): items}, units_by_ref=units_by_ref
    )
    real_unit = provider.unit
    entered = asyncio.Event()
    release = asyncio.Event()
    in_flight = 0

    async def _gated_unit(node: Node) -> RestorableUnit:
        nonlocal in_flight
        in_flight += 1
        if in_flight == len(items):
            entered.set()
        await release.wait()
        return await real_unit(node)

    provider.unit = _gated_unit  # type: ignore[method-assign]

    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        await wait_until(pilot, lambda: entered.is_set(), timeout=sdk_timeout, message="items never ran concurrently")
        release.set()

        detail = screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "item-0" in str(detail.render()))
        assert all(f"item-{i}" in str(detail.render()) for i in range(3))


# -- "nothing selected" guards -------------------------------------------


async def test_export_and_copy_ref_and_hex_preview_warn_when_nothing_is_selected(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._selected_node = lambda: None  # type: ignore[method-assign]

        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_export_selected()
        assert warnings == [UNIT_NOTHING_SELECTED_WARNING]

        warnings.clear()
        screen.action_copy_ref()
        assert warnings == [UNIT_COPY_REF_NOTHING_SELECTED_WARNING]

        warnings.clear()
        app.verbose = True
        screen.action_hex_preview()
        assert warnings == [UNIT_HEX_NOTHING_SELECTED_WARNING]


async def test_hex_preview_outside_verbose_mode_is_a_no_op_warning(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_hex_preview()
        assert warnings == ["press d to enable verbose mode first"]


async def test_copy_ref_with_a_real_selection_copies_and_notifies(wait_until: Any) -> None:
    # The "nothing selected" warning branches above are the only ones
    # this file covers for copy_ref/export_selected/hex_preview -- their
    # real success paths (something actually selected) were untested.
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        copied: list[str] = []
        app.copy_to_clipboard = lambda text: copied.append(text)  # type: ignore[method-assign]
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_copy_ref()
        assert copied == [str(leaf.ref)]
        assert warnings == ["copied ref to clipboard"]


async def test_export_selected_with_a_real_leaf_pushes_the_export_screen(wait_until: Any) -> None:
    from synology_apm_repo.browser.screens.export_screen import ExportScreen

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=_ContentSource(b"data"),  # type: ignore[arg-type]
    )
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        screen.action_export_selected()
        await wait_until(pilot, lambda: isinstance(app.screen, ExportScreen))


async def test_export_selected_shows_the_loading_indicator_while_the_fetch_is_in_flight(
    wait_until: Any, sdk_timeout: float
) -> None:
    """``action_export_selected`` now dispatches through ``@work``, which
    wraps the worker's body in ``DebouncedProgress`` by default and
    renders it to the breadcrumb unless a ``sink`` is given, rather than
    awaiting ``provider.unit()`` inline -- proves the breadcrumb actually shows the
    loading indicator while that fetch is genuinely still in flight, not
    just that the eventual screen push still happens. A held-open
    ``asyncio.Event`` (rather than a fixed ``pilot.pause``) keeps the fetch
    reliably still running when the breadcrumb is checked.

    The indicator's own cleanup lands on this screen only once it's the
    current one again: the fetch finishing pushes ``ExportScreen`` on
    top of it in the very same breath, so ``_render_breadcrumb``'s own
    "skip while covered" guard (``_shared.py``) defers the actual widget
    write until ``on_screen_resume`` fires for the pop back -- this
    proves that catch-up, not just that the internal state ends up
    correct."""
    from synology_apm_repo.browser.screens.export_screen import ExportScreen

    gate = asyncio.Event()

    class _SlowProvider(_ConfigurableProvider):
        async def unit(self, node: Node) -> RestorableUnit:
            await gate.wait()
            return await super().unit(node)

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=_ContentSource(b"data"),  # type: ignore[arg-type]
    )
    provider = _SlowProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        screen.action_export_selected()
        breadcrumb = screen.query_one("#breadcrumb", Static)
        # DebouncedProgress only starts animating past its own 300ms arm
        # delay -- this waits out that real delay via polling (never a
        # fixed sleep-and-assume), same as every other genuinely-timed
        # wait in this suite.
        await wait_until(pilot, lambda: "Loading" in str(breadcrumb.render()), timeout=sdk_timeout)

        gate.set()
        await wait_until(pilot, lambda: isinstance(app.screen, ExportScreen))

        app.pop_screen()
        await wait_until(pilot, lambda: app.screen is screen)
        await wait_until(pilot, lambda: "Loading" not in str(breadcrumb.render()))


async def test_export_selected_degrades_to_a_toast_instead_of_crashing_the_app(wait_until: Any) -> None:
    # provider.unit() raising here (a leaf with no fake unit registered,
    # via _ConfigurableProvider.unit()'s own NotImplementedError -- a
    # deliberately non-ApmRepoError exception, exercising the broad
    # ``except Exception``) must degrade to a toast, not crash the whole
    # Textual app the way an unguarded await once did.
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})  # no units_by_ref entry for `leaf`
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_export_selected()
        await wait_until(pilot, lambda: bool(warnings))

        assert isinstance(app.screen, UnitScreen)  # still here -- no crash, no screen pushed
        assert warnings and "no fake unit registered" in warnings[0]


async def test_hex_preview_degrades_to_a_toast_instead_of_crashing_the_app(wait_until: Any) -> None:
    # Same reasoning as the export_selected test above.
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    app.verbose = True
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_hex_preview()
        await wait_until(pilot, lambda: bool(warnings))

        assert isinstance(app.screen, UnitScreen)
        assert warnings and "no fake unit registered" in warnings[0]


async def test_hex_preview_selected_but_not_a_leaf_warns(wait_until: Any) -> None:
    # Distinct from "nothing selected" (None) -- a real, non-leaf node
    # (a folder) is selected instead.
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "folder")), name="folder", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): [folder]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    app.verbose = True
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._selected_node = lambda: folder  # type: ignore[method-assign]

        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_hex_preview()
        assert warnings == [UNIT_HEX_NOTHING_SELECTED_WARNING]


async def test_action_refresh_reloads_the_tree_from_the_root(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        provider_calls_before = app._repo.catalog.provider_calls

        screen.action_refresh()
        await wait_until(pilot, lambda: app._repo.catalog.provider_calls > provider_calls_before)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        assert len(screen._file_table._nodes) == 1
        assert app._repo.catalog.provider_calls > provider_calls_before  # re-fetched, not just re-rendered
        assert app._repo.invalidate_directory_cache_calls == 1  # a real re-scan, not stale cached listings


async def test_action_show_diagnostics_pushes_the_diagnostics_screen(wait_until: Any) -> None:
    from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen.action_show_diagnostics()
        await pilot.pause()
        assert isinstance(app.screen, DiagnosticsScreen)


# -- go back closes an open goto box first --------------------------------


async def test_go_back_closes_an_open_goto_box_before_popping(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen.action_goto_ref()
        await pilot.pause()
        assert screen.query_one("#goto-input", Input).has_class("active")

        screen.action_go_back()
        await pilot.pause()
        assert not screen.query_one("#goto-input", Input).has_class("active")
        assert isinstance(app.screen, UnitScreen)  # the box closed; the screen itself is still open

        # Nothing open this time: Esc pops the screen itself.
        screen.action_go_back()
        await pilot.pause()
        assert app.screen is not screen


# -- refresh_for_verbose_mode --------------------------------------------


async def test_refresh_for_verbose_mode_reloads_for_a_saas_version(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    repo = _FakeRepo(provider)
    app = _FakeApp(_version(target_type="M365"), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        assert repo.catalog.provider_calls == 1  # the initial _load_root on mount

        screen.refresh_for_verbose_mode()
        await pilot.pause()
        # A real reload happened -- catalog.provider() was dispatched a
        # second time -- not just "provider is still non-None" (the fake
        # always returns the same object either way).
        assert repo.catalog.provider_calls == 2


async def test_refresh_for_verbose_mode_is_a_no_op_for_a_non_saas_version(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(target_type="VM"), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        handle_before = screen.store.model.provider

        screen.refresh_for_verbose_mode()
        await pilot.pause()
        assert screen.store.model.provider is handle_before  # untouched — never reset/reloaded


# -- load more / filter ---------------------------------------------------


async def test_action_load_more_warns_when_already_exhausted(wait_until: Any) -> None:
    """The "nothing loaded yet" warning branch is proven directly at the
    pure ``update()`` layer
    (``test_browser_core_unit_update.py::test_load_more_requested_warns_when_nothing_loaded``)
    -- this Pilot test only needs to prove ``action_load_more``'s own
    dispatch reaches a real ``notify()`` call, using the one branch
    naturally reachable through ordinary browsing: root's own
    auto-loaded, genuinely empty (so already exhausted) listing."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): []})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_load_more()
        assert warnings == [UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING]


async def test_pressing_load_more_twice_while_the_first_page_is_still_loading_does_not_redispatch(
    wait_until: Any,
) -> None:
    """The load-more analogue of the folder-tree race above
    (``test_reexpanding_a_node_while_its_children_fetch_is_still_loading_does_not_refetch``):
    pressing ``+`` again before the first load-more's own fetch resolves
    must not dispatch a second one."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    bulk = [_leaf(f"bulk-{i}", f"bulk-{i}") for i in range(CHILDREN_PAGE_SIZE + 1)]
    provider = _ConfigurableProvider(root, {str(root_ref): bulk})
    real_children = provider.children
    calls = 0
    gate = asyncio.Event()

    async def _gated_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        nonlocal calls
        if offset:  # only the load-more page, not the initial one, is gated
            calls += 1
            await gate.wait()
        return await real_children(node, offset=offset, limit=limit)

    provider.children = _gated_children  # type: ignore[method-assign]
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: len(screen._file_table._nodes) == CHILDREN_PAGE_SIZE)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_load_more()
        await wait_until(pilot, lambda: calls == 1)

        screen.action_load_more()  # a second press while the first page is still loading
        await pilot.pause()
        assert calls == 1, "pressing + again while still loading must not refetch"
        assert warnings == [UNIT_LOAD_MORE_ALREADY_LOADING_WARNING]

        gate.set()
        await wait_until(pilot, lambda: len(screen._file_table._nodes) == CHILDREN_PAGE_SIZE + 1)
        assert calls == 1


async def test_load_more_error_notifies_and_load_more_with_active_filter_rerenders(wait_until: Any) -> None:
    """Needs a genuinely un-exhausted initial load (more than one page's
    worth of real children) for load-more to have anything to dispatch
    at all -- ``action_load_more``'s own "already complete"/"nothing
    loaded" guards are proven separately, at the pure ``update()`` layer
    (``test_browser_core_unit_update.py``)."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    bulk = [_leaf(f"bulk-{i}", f"bulk-{i}") for i in range(CHILDREN_PAGE_SIZE + 1)]
    provider = _ConfigurableProvider(root, {str(root_ref): bulk})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        # bulk is all leaves, so they list in the file table, never the
        # folder tree (which excludes leaves entirely).
        await wait_until(pilot, lambda: len(screen._file_table._nodes) == CHILDREN_PAGE_SIZE)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        # A children() failure during load-more must notify, not crash.
        provider._raise_children_for = {str(root_ref)}
        screen.action_load_more()
        await wait_until(pilot, lambda: bool(warnings))
        assert f"boom at {root_ref}" in warnings[0]

        # A subsequent successful load-more, with an active filter on the
        # same level, must re-render through the filtered path rather than
        # appending unconditionally. "bulk-500" (the one item this
        # load-more call will add) is the only name containing that exact
        # substring among the 501 total.
        provider._raise_children_for = set()
        screen.action_filter()
        await pilot.pause()
        screen._filter.pending_text = "bulk-500"
        screen._filter._commit()
        await pilot.pause()
        warnings.clear()
        screen.action_load_more()
        await wait_until(pilot, lambda: bool(warnings))
        assert [n.name for n in screen._file_table._nodes if n is not None] == ["bulk-500"]


async def test_filtering_a_level_preserves_a_surviving_childs_own_widget_and_does_not_refetch_it(
    wait_until: Any, move_cursor_to: Any, sdk_timeout: float
) -> None:
    """The keyed reconciler (``view/reconcile.py``'s ``reconcile_children``)
    keeps a surviving child's own ``TreeNode`` object across a filter
    re-render instead of destroying and recreating it -- unlike the old
    ``id(TreeNode)``-keyed scheme (which tore down and forgot every node
    at a filtered level on every keystroke, needing its own purge/replay
    dance to cope with the resulting re-fetches). ``children_calls``
    proves this directly: the folder's real ``children()`` call happens
    exactly once, not once per filter keystroke or per re-selection,
    since its already-loaded subtree is never destroyed at all -- proven
    here via the file table (folder's own child, ``inner``, is a leaf,
    so it only ever appears there, never in the folder tree)."""
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    other = _leaf("other.bin", "other")
    inner = _leaf("inner.bin", "inner")
    provider = _ConfigurableProvider(root, {str(root_ref): [folder, other], str(folder_ref): [inner]})
    children_calls: list[str] = []
    real_children = provider.children

    async def _counting_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        children_calls.append(str(node.ref))
        return await real_children(node, offset=offset, limit=limit)

    provider.children = _counting_children  # type: ignore[method-assign]
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        children_calls.clear()  # drop the root's own auto-expand fetch -- not under test here

        folder_node = next(n for n in tree.root.children if n.data is not None and n.data.key == folder_ref)
        await move_cursor_to(pilot, tree, folder_node)
        await pilot.press("enter")  # selects+expands folder -- also sets model.selected = folder_ref
        await wait_until(pilot, lambda: folder_ref in screen.store.model.loaded, timeout=sdk_timeout)
        assert children_calls == [str(folder_ref)]

        # Re-select root before filtering -- pressing enter on folder above
        # moved model.selected to folder_ref, and action_filter() now
        # resolves its target from model.selected directly (not the tree
        # cursor), so filtering root's own children requires root to be
        # model.selected again. Done via the screen's own narrow dispatch
        # helper, not a real cursor move + enter -- root already started
        # expanded (mount-time auto-expand), and pressing enter on it again
        # would toggle it *collapsed*, hiding folder_node from view for the
        # move_cursor_to calls below.
        screen._select_folder_ref(root)
        screen.action_filter()
        await pilot.pause()
        screen._filter.pending_text = "folder"
        screen._filter._commit()
        await pilot.pause()

        new_folder_node = next(n for n in tree.root.children if n.data is not None and n.data.key == folder_ref)
        assert new_folder_node is folder_node, "the reconciler must keep a surviving child's own TreeNode"
        screen._close_filter()

        # folder's own already-loaded subtree survived the root-level
        # filter round-trip untouched: selecting it again shows "inner.bin"
        # in the file table with no additional children() call.
        # _close_filter() leaves focus on the just-closed #filter-input,
        # not the tree -- refocus it first, or the enter press below never
        # reaches the tree's own key handling at all.
        tree.focus()
        await wait_until(pilot, lambda: tree.has_focus)
        await move_cursor_to(pilot, tree, new_folder_node)
        await pilot.press("enter")
        await wait_until(pilot, lambda: screen.store.model.selected == folder_ref)
        assert [n.name for n in screen._file_table._nodes if n is not None] == ["inner.bin"]
        assert children_calls == [str(folder_ref)], "a filter round-trip must not re-fetch an already-loaded level"


async def test_action_filter_is_a_no_op_for_an_unloaded_level(wait_until: Any) -> None:
    """``action_filter`` only opens once ``model.loaded`` has an entry for
    the current level -- exercised here via a ``children()`` fetch gated
    on an ``asyncio.Event`` that never fires during the assertion, rather
    than racing the real auto-expand-on-mount window (root's own
    children fetch, unlike root itself, isn't synchronous with mount)."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): []})
    gate = asyncio.Event()
    real_children = provider.children

    async def _blocked_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        await gate.wait()
        return await real_children(node, offset=offset, limit=limit)

    provider.children = _blocked_children  # type: ignore[method-assign]
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        assert root_ref not in screen.store.model.loaded

        screen.action_filter()
        await pilot.pause()
        assert not screen.query_one("#filter-input", Input).has_class("active")

        gate.set()  # let the level load so on_unmount's own cleanup has nothing stuck
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)


async def test_enter_on_the_filter_input_closes_it(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen.action_filter()
        await pilot.pause()
        assert screen.query_one("#filter-input", Input).has_class("active")

        await pilot.press("enter")
        await pilot.pause()
        assert not screen.query_one("#filter-input", Input).has_class("active")


# -- goto ref (``g``) ---------------------------------------------------


async def test_submit_goto_version_lookup_failure_notifies(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    repo = _FakeRepo(provider, version_for_ref_error=ApmRepoError("unknown version"))
    app = _FakeApp(_version(), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        other_ref = NodeRef.canonical(
            "repo",
            catalog_id=CatalogId("catalog-1"),
            workload_id=WorkloadId(1),
            version_uid=VersionUid("some-other-version"),
        )
        screen._submit_goto(str(other_ref))
        await wait_until(pilot, lambda: bool(warnings))
        assert warnings == ["unknown version"]


async def test_submit_goto_a_different_version_pushes_a_new_unit_screen(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    other_version = dataclasses.replace(
        _version(), version_uid=VersionUid("some-other-version"), version_id=VersionId(2)
    )
    repo = _FakeRepo(provider, version_for_ref_result=other_version)
    app = _FakeApp(_version(), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        other_ref = NodeRef.canonical(
            "repo",
            catalog_id=CatalogId("catalog-1"),
            workload_id=WorkloadId(1),
            version_uid=VersionUid("some-other-version"),
        )
        screen._submit_goto(str(other_ref))
        await wait_until(pilot, lambda: app.screen is not screen)
        assert isinstance(app.screen, UnitScreen)


async def test_walk_to_target_is_a_no_op_with_no_provider(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        was_expanded = tree.root.is_expanded
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
        screen._walk_to_target(None, root_ref)
        await pilot.pause()
        # No provider -> _walk_to_target returns immediately: no
        # notification fired, no expansion-state change triggered by it.
        assert warnings == []
        assert tree.root.is_expanded == was_expanded


async def test_goto_target_not_found_notifies_and_expands_root(
    monkeypatch: pytest.MonkeyPatch, wait_until: Any
) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    missing_ref = NodeRef("repo", ("root", "missing"))
    provider = _ConfigurableProvider(root, {str(root_ref): []})
    # Patched on the class, before the app even runs: with target_ref set,
    # on_mount's _load_root() dispatches straight into _walk_to_target as
    # a background worker with nothing real to await, so it can finish
    # inside run_test()'s own startup pump -- patching screen.notify only
    # after entering the pilot context can already be too late to observe
    # the call.
    warnings: list[str] = []
    monkeypatch.setattr(UnitScreen, "notify", lambda self, message, **kwargs: warnings.append(message))
    app = _FakeApp(_version(), _FakeRepo(provider), target_ref=missing_ref)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: bool(warnings) and tree.root.is_expanded)
        assert warnings == [GOTO_REF_NOT_FOUND_WARNING]
        assert tree.root.is_expanded  # FolderTreeView.ensure_root_expanded() actually expanded it


async def test_goto_children_error_notifies_and_expands_root(monkeypatch: pytest.MonkeyPatch, wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    target_ref = NodeRef("repo", ("root", "target"))
    provider = _ConfigurableProvider(root, {}, raise_children_for={str(root_ref)})
    # Patched on the class before the app runs, same as
    # test_goto_target_not_found_notifies_and_expands_root above.
    warnings: list[str] = []
    monkeypatch.setattr(UnitScreen, "notify", lambda self, message, **kwargs: warnings.append(message))
    app = _FakeApp(_version(), _FakeRepo(provider), target_ref=target_ref)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: bool(warnings) and tree.root.is_expanded)
        assert warnings == [f"boom at {root_ref}"]
        assert tree.root.is_expanded


async def test_goto_not_found_warning_text(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): []})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
        missing_ref = NodeRef("repo", ("root", "missing"))
        current_provider = screen._current_provider()
        assert current_provider is not None
        screen._walk_to_target(current_provider, missing_ref)
        await wait_until(pilot, lambda: bool(warnings))
        assert warnings == [GOTO_REF_NOT_FOUND_WARNING]


async def test_goto_a_disk_fs_sibling_finds_it_as_a_top_level_folder(wait_until: Any) -> None:
    """The "(filesystem)" sibling is an ordinary top-level folder-tree
    entry, and the (leaf) disk-image node beside it is excluded from the
    tree entirely -- the folder tree only ever shows containers, so a
    plain leaf only ever appears as a file-table row, never a tree entry
    -- so ``GotoChainWalker`` finds the sibling as a direct child of
    root, no fallback needed."""
    from synology_apm_repo.sdk.units.device_disk_fs import DISK_FS_SIBLING_REF_ATTR

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    image_ref = NodeRef("repo", ("object", "5"))
    image_node = Node(ref=image_ref, name="disk-1.img", is_leaf=True)
    fs_ref = image_ref.child("fs")
    fs_node = Node(
        ref=fs_ref, name="disk-1.img (filesystem)", is_leaf=False, attrs={DISK_FS_SIBLING_REF_ATTR: image_ref}
    )
    provider = _ConfigurableProvider(root, {str(root_ref): [image_node, fs_node]})
    app = _FakeApp(_version(), _FakeRepo(provider), target_ref=fs_ref)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)

        assert len(tree.root.children) == 1  # only the (container) fs sibling is tree-shown
        fs_tree_node = tree.root.children[0]
        assert fs_tree_node.data is not None
        assert fs_tree_node.data.key == fs_ref
        await wait_until(pilot, lambda: tree.cursor_node is fs_tree_node)  # goto landed the cursor on it
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        assert screen.store.model.selected == fs_ref  # the target folder itself is now selected


async def test_selecting_a_subfolder_row_in_the_file_table_syncs_the_folder_tree(wait_until: Any) -> None:
    """The file table doubles as a second navigation entry point into the
    folder tree: activating a subfolder row there must dispatch
    ``FolderSelected`` and sync the folder tree's own cursor onto it too,
    not just update the file table's own state."""
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [folder, leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        table.cursor_coordinate = Coordinate(0, 0)  # the folder row
        await pilot.press("enter")

        await wait_until(pilot, lambda: screen.store.model.selected == folder_ref)
        tree_node = screen.unit_tree.cursor_node
        assert tree_node is not None and tree_node.data is not None
        assert tree_node.data.key == folder_ref
        assert tree_node.is_expanded


async def test_selecting_a_file_table_row_syncs_the_tree_cursor_even_through_a_collapsed_ancestor(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """``find_node`` locates a ``TreeNode`` regardless of any ancestor's
    own collapsed state (collapsing doesn't destroy children, it only
    hides them) -- so if the user manually collapses an *intermediate*
    folder in the tree without touching ``model.selected`` at all, then
    activates a file-table row for a folder still underneath that
    collapsed ancestor, expanding the target node alone isn't enough:
    ``move_cursor_keyed``'s own precondition needs every ancestor
    expanded too, or it silently no-ops and the tree cursor gets stuck
    while the file table/detail pane have already moved on."""
    root_ref = NodeRef("repo", ("root",))
    b_ref = NodeRef("repo", ("root", "b"))
    c_ref = NodeRef("repo", ("root", "b", "c"))
    d_ref = NodeRef("repo", ("root", "b", "c", "d"))
    e_ref = NodeRef("repo", ("root", "b", "c", "e"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    b = Node(ref=b_ref, name="b", is_leaf=False)
    c = Node(ref=c_ref, name="c", is_leaf=False)
    d = Node(ref=d_ref, name="d", is_leaf=False)
    e = Node(ref=e_ref, name="e", is_leaf=False)
    provider = _ConfigurableProvider(
        root, {str(root_ref): [b], str(b_ref): [c], str(c_ref): [d, e], str(d_ref): [], str(e_ref): []}
    )
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = screen.unit_tree
        await wait_until(pilot, lambda: len(tree.root.children) > 0)

        b_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, b_node)
        await pilot.press("enter")  # selects+expands b -- c appears
        await wait_until(pilot, lambda: b_ref in screen.store.model.loaded)

        c_node = next(n for n in b_node.children if n.data is not None and n.data.key == c_ref)
        await move_cursor_to(pilot, tree, c_node)
        await pilot.press("enter")  # selects+expands c -- d/e appear in both tree and file table
        await wait_until(pilot, lambda: c_ref in screen.store.model.loaded)

        # The user collapses b (an ancestor of c, not c itself), without
        # touching model.selected at all -- c/d/e's own TreeNodes still
        # exist underneath, just unreachable while b stays collapsed.
        b_node.collapse()
        await pilot.pause()
        assert not b_node.is_expanded

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        e_row = next(i for i, n in enumerate(screen._file_table._nodes) if n is not None and n.ref == e_ref)
        table.cursor_coordinate = Coordinate(e_row, 0)
        await pilot.press("enter")

        await wait_until(pilot, lambda: screen.store.model.selected == e_ref)
        assert b_node.is_expanded, "an ancestor above the target must be re-expanded too, not just the target itself"
        tree_node = tree.cursor_node
        assert tree_node is not None and tree_node.data is not None
        assert tree_node.data.key == e_ref, "the tree cursor must actually follow the file-table selection"


async def test_first_time_enter_on_an_unloaded_folder_anchors_the_spinner_on_the_file_table(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """The primary interaction this screen exists for: pressing Enter on a
    folder that has never been expanded before. Textual's own ``Tree``
    auto-expands a node in reaction to the very same ``NodeSelected``
    message this screen's own ``on_tree_node_selected`` handles
    (``Tree._expand_node_on_select``, a node-local handler that runs
    before the message bubbles up to this screen) -- but that auto-expand only *posts* its own
    ``NodeExpanded`` message for later processing, it doesn't handle it
    inline. So the bubbling ``NodeSelected`` reaches this screen (and
    ``FolderSelected`` fully dispatches, synchronously updating
    ``model.selected``) before the queued ``NodeExpanded`` -> ``on_tree_
    node_expanded`` -> ``ChildrenRequested`` -> ``LoadChildren`` chain
    ever runs. By the time ``UnitEffects.perform(LoadChildren)`` checks
    ``cmd.node.ref == model.selected``, the new folder is already
    selected -- the file table, not the tree node, must show the
    spinner."""
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    gate = asyncio.Event()

    class _GatedProvider(_ConfigurableProvider):
        async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
            if node.ref == folder_ref:
                await gate.wait()
            return await super().children(node, offset=offset, limit=limit)

    provider = _GatedProvider(root, {str(root_ref): [folder], str(folder_ref): []})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) >= 1)
        folder_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, folder_node)
        await pilot.press("enter")  # first-ever expand of this folder
        await wait_until(pilot, lambda: screen.store.model.selected == folder_ref)

        table = screen.file_table
        await wait_until(pilot, lambda: table.row_count > 0 and "Loading" in str(table.get_row_at(0)[0]))
        assert "Loading" not in str(folder_node.label), "the tree node itself must show no spinner suffix"

        gate.set()
        await wait_until(pilot, lambda: folder_ref in screen.store.model.loaded)


async def test_loading_a_sibling_folder_does_not_rebuild_the_currently_selected_file_table(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """``FileTableView.render``'s own subscription is scoped to
    ``file_table_rows(model, model.selected)``, not the raw
    ``model.loaded``/``model.errors`` dicts -- so an unrelated sibling
    folder's own background fetch resolving (its own children page
    landing, here) must never touch the currently-selected folder's file
    table at all. ``DataTable.clear()`` unconditionally resets the
    table's own scroll position, so a stray rebuild here would be
    user-visible as the currently-viewed folder's scroll position
    snapping back to the top the instant an unrelated folder's fetch
    completes."""
    root_ref = NodeRef("repo", ("root",))
    folder_a_ref = NodeRef("repo", ("root", "a"))
    folder_b_ref = NodeRef("repo", ("root", "b"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder_a = Node(ref=folder_a_ref, name="a", is_leaf=False)
    folder_b = Node(ref=folder_b_ref, name="b", is_leaf=False)
    provider = _ConfigurableProvider(
        root, {str(root_ref): [folder_a, folder_b], str(folder_a_ref): [], str(folder_b_ref): []}
    )
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) >= 2)

        a_node, b_node = tree.root.children[0], tree.root.children[1]
        await move_cursor_to(pilot, tree, a_node)
        await pilot.press("enter")
        await wait_until(pilot, lambda: screen.store.model.selected == folder_a_ref)

        table = screen.file_table
        clear_calls = 0
        real_clear = table.clear

        def _counting_clear(columns: bool = False) -> FileTable:
            nonlocal clear_calls
            clear_calls += 1
            return real_clear(columns)

        table.clear = _counting_clear  # type: ignore[method-assign]

        await move_cursor_to(pilot, tree, b_node)
        await pilot.press("space")  # expand (not select) folder b -- a background fetch only
        await wait_until(pilot, lambda: folder_b_ref in screen.store.model.loaded)

        assert screen.store.model.selected == folder_a_ref
        assert clear_calls == 0, "an unrelated sibling's own fetch must not rebuild this table at all"


async def test_selected_node_reads_the_currently_focused_widget(wait_until: Any) -> None:
    """``_selected_node()`` is focus-based: whichever of the folder tree
    or the file table currently has focus is what it reads from, not
    whichever one the user interacted with last."""
    root_ref = NodeRef("repo", ("root",))
    folder_ref = NodeRef("repo", ("root", "folder"))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=folder_ref, name="folder", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [folder, leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        tree = screen.unit_tree
        tree.focus()
        await wait_until(pilot, lambda: tree.has_focus)
        tree_node = next(n for n in tree.root.children if n.data is not None and n.data.key == folder_ref)
        _ = (
            tree._tree_lines
        )  # move_cursor() resolves the node through the line map, so a stale one leaves the cursor where it was
        tree.move_cursor(tree_node)
        await wait_until(pilot, lambda: tree.cursor_node is tree_node)
        assert screen._selected_node() is folder

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        row_index = screen._file_table._nodes.index(leaf)
        table.cursor_coordinate = Coordinate(row_index, 0)
        assert screen._selected_node() is leaf


async def test_selected_node_falls_back_to_the_detail_pane_when_focus_is_elsewhere(wait_until: Any) -> None:
    """``#detail-scroll`` is a ``VerticalScroll``, focusable by default in
    Textual -- a mouse click or Tab can land focus there while a leaf's
    own preview is showing. ``_selected_node()`` must not go blind the
    moment that happens: it falls back to whatever the detail pane is
    currently showing, since every navigation path keeps that in lockstep
    with the real selection regardless of which widget has focus."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        table.cursor_coordinate = Coordinate(screen._file_table._nodes.index(leaf), 0)
        await pilot.press("enter")  # fires on_data_table_row_selected -> _show_detail(leaf)
        assert screen._selected_node() is leaf  # sanity: the table itself still resolves it

        detail_scroll = screen.query_one("#detail-scroll")
        detail_scroll.focus()
        await wait_until(pilot, lambda: detail_scroll.has_focus)

        assert screen._selected_node() is leaf


async def test_refresh_clears_the_detail_panes_stale_fallback_node(wait_until: Any) -> None:
    """A refresh (or a verbose-mode reload) tears down the old
    provider/root entirely (``RootRequested`` resets ``model.provider``/
    ``model.root``/``model.selected`` to ``None`` before its own fresh
    load even starts) -- without this, ``DetailPane._node`` would still
    hold a ``Node`` from the just-closed provider generation, and
    ``_selected_node()``'s own detail-pane fallback
    would resurface that stale node the moment focus lands back on
    ``#detail-scroll``, letting an action like ``action_copy_ref``/
    ``action_export_selected`` act on a node whose own provider may no
    longer even be open."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        table.cursor_coordinate = Coordinate(screen._file_table._nodes.index(leaf), 0)
        await pilot.press("enter")  # fires on_data_table_row_selected -> _show_detail(leaf)
        assert screen._detail_pane.node is leaf  # sanity: the pane is tracking it

        screen.action_refresh()
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        detail_scroll = screen.query_one("#detail-scroll")
        detail_scroll.focus()
        await wait_until(pilot, lambda: detail_scroll.has_focus)

        assert screen._detail_pane.node is None
        assert screen._selected_node() is None


async def test_footer_shows_only_back_export_detail_and_help() -> None:
    """``UnitScreen``'s own footer is deliberately trimmed the same way
    ``BrowseScreen``'s is: ``escape``/``e``/``i``/``question_mark`` only;
    every other binding (``q``/``d``/``r``/``/``/``t`` and every nav key)
    stays fully functional, just not printed here. Unlike ``BrowseScreen``,
    Esc *is* shown here -- there, Esc's own fallback means something else
    entirely, but here ``action_go_back`` always just pops back to
    ``BrowseScreen``, so it's worth printing. ``t``
    (``toggle_worklist``) stays hidden here too, same as
    ``BrowseScreen`` -- the breadcrumb's own "N Task(s) (t)" suffix
    (``NavigableScreen._render_breadcrumb``) already names the key the
    moment it's actually reachable (a job exists), so repeating it here
    would just be a second, redundant place for the same fact to drift
    out of sync."""
    repo = _FakeRepo(None, provider_error=ApmRepoError("irrelevant to this test"))
    app = _FakeApp(_version(), repo)
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        shown = {key for key, active in screen.active_bindings.items() if active.binding.show}
        assert shown == {"escape", "e", "i", "question_mark"}


async def test_footer_hidden_bindings_still_dispatch() -> None:
    repo = _FakeRepo(None, provider_error=ApmRepoError("irrelevant to this test"))
    app = _FakeApp(_version(), repo)
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        refresh_calls = [0]
        filter_calls = [0]
        screen.action_refresh = lambda: refresh_calls.__setitem__(0, refresh_calls[0] + 1)  # type: ignore[method-assign]
        screen.action_filter = lambda: filter_calls.__setitem__(0, filter_calls[0] + 1)  # type: ignore[method-assign]

        await pilot.press("r")
        await pilot.press("slash")
        assert refresh_calls[0] == 1
        assert filter_calls[0] == 1

        # "d"/"t" resolve on the App (ApmRepoBrowserApp), not this
        # screen -- _FakeApp deliberately doesn't implement them, so
        # dispatch here just needs to not raise (same fallback-through
        # mechanism BrowseScreen's own equivalent test relies on for
        # these two keys).
        await pilot.press("d")
        await pilot.press("t")
        # "q" (quit_app) is deliberately not exercised here -- pressing it
        # would actually try to shut down _FakeApp mid-test.


__all__: list[str] = []
