"""UnitScreen detail pane and content preview: verbose-only metadata,
content-only kinds, preview failures and read caps, cloud-file size notes and
file-state icons, markup-shaped names, and late previews."""

from __future__ import annotations

import pytest
from textual.widgets import DataTable, Static, Tree

from support.fakes import faithful_to
from support.model_factories import make_version
from support.pilot import settle, wait_for_detail_content, wait_for_workers, wait_until
from synology_apm_repo.browser.core.unit.model import (
    DETAIL_GROUP,
    DETAIL_SLOT,
    PREVIEW_READ_LIMIT,
    DetailIdle,
    DetailPreview,
)
from synology_apm_repo.browser.core.unit.msg import DetailResolved
from synology_apm_repo.browser.runtime import preview as preview_module
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.errors import ApmRepoError, ContentUnavailableError
from synology_apm_repo.sdk.units.base import ContentSource, FileState, Node, RestorableUnit, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    ConfigurableProvider,
    FakeApp,
    FakeContentSource,
    FakeRepo,
    leaf_node,
)


async def test_detail_shows_ref_and_attrs_only_in_verbose_mode() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item", details={"custom": "value"})
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        detail_non_verbose = str(screen.query_one("#detail", Static).render())

        # The fake app's plain attribute has no reactive, so call the watch callback.
        app.verbose = True
        screen.refresh_for_verbose_mode()
        detail_verbose = str(screen.query_one("#detail", Static).render())

        assert "ref:" not in detail_non_verbose
        assert "custom" not in detail_non_verbose
        assert "custom: value" in detail_verbose


async def test_content_only_kind_shows_no_header_outside_verbose_mode() -> None:
    """Mail/Calendar-event/Contact leaves get no header outside verbose
    mode (``is_content_only_preview``)."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("mail-1.eml", "mail-1", kind=UnitKind.MAIL, details={"custom": "value"})
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        detail_non_verbose = str(screen.query_one("#detail", Static).render())
        assert detail_non_verbose == ""

        app.verbose = True
        screen.refresh_for_verbose_mode()
        detail_verbose = str(screen.query_one("#detail", Static).render())
        assert "mail-1.eml" not in detail_verbose
        assert "kind:" not in detail_verbose
        assert "ref:" in detail_verbose
        assert "custom: value" in detail_verbose


async def test_content_only_kind_preview_has_no_header_or_separator() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("mail-1.eml", "mail-1", kind=UnitKind.MAIL)
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        model = screen.store.model
        screen.store.dispatch(
            DetailResolved(
                epoch=model.epoch,
                request=model.inflight[DETAIL_SLOT],
                body=DetailPreview("From: alice@example.com\nHello"),
            )
        )
        detail = str(screen.query_one("#detail", Static).render())
        assert detail == "From: alice@example.com\nHello"


async def test_content_only_kind_preview_failure_shows_an_inline_error_not_a_blank_pane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    def _raise(data: bytes) -> str:
        raise ValueError("malformed preview bytes")

    monkeypatch.setattr(preview_module, "preview_renderer_for", lambda node: _raise)
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("mail-1.eml", "mail-1", kind=UnitKind.MAIL)
    unit = RestorableUnit(ref=leaf.ref, name=leaf.name, is_leaf=True, content=FakeContentSource(b"anything"))  # type: ignore[arg-type]
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        await wait_for_detail_content(pilot, screen, contains="malformed preview bytes")
        detail = str(screen.query_one("#detail", Static).render())
        assert "malformed preview bytes" in detail


async def test_content_unavailable_preview_failure_shows_a_note_not_an_error() -> None:

    @faithful_to(ContentSource)
    class _UnavailableContentSource:
        size: int | None = None

        async def read(self, offset: int = 0, length: int | None = None) -> bytes:
            raise ContentUnavailableError("cloud-sync placeholder -- no data at backup time")

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("cloud.bin", "cloud", file_state=FileState.CLOUD_ONLY)
    unit = RestorableUnit(ref=leaf.ref, name=leaf.name, is_leaf=True, content=_UnavailableContentSource())  # type: ignore[arg-type]
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = FakeApp(make_version(), FakeRepo(provider))
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


async def test_teams_chat_message_preview_reads_the_tail_when_content_exceeds_the_read_cap() -> None:
    """Teams/Chat content beyond ``PREVIEW_READ_LIMIT`` is read from the
    end (``prefers_recent_content``), keeping the newest message."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("General", "channel-1", kind=UnitKind.TEAMS_CHAT_MESSAGE)
    body = "".join(_fake_channel_msg_div(i) for i in range(3000))
    html = f"<!doctype html><html><body>{body}</body></html>".encode()
    assert len(html) > PREVIEW_READ_LIMIT
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=FakeContentSource(html),  # type: ignore[arg-type]
    )
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)
        await wait_for_detail_content(pilot, screen, contains="message number 2999")
        detail = str(screen.query_one("#detail", Static).render())
        assert "message number 2999" in detail
        assert "message number 0" not in detail


async def test_detail_pane_size_line_notes_zero_bytes_on_disk_for_a_cloud_file() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    normal_leaf = leaf_node("normal.bin", "normal", size=1572864)
    cloud_leaf = leaf_node("cloud.bin", "cloud", size=1572864, file_state=FileState.CLOUD_ONLY)
    provider = ConfigurableProvider(root, {str(root_ref): [normal_leaf, cloud_leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
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


async def test_detail_pane_size_line_has_no_on_disk_caveat_for_an_encrypted_file() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    encrypted_leaf = leaf_node("secret.docx", "secret", size=1572864, file_state=FileState.ENCRYPTED)
    provider = ConfigurableProvider(root, {str(root_ref): [encrypted_leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(encrypted_leaf)
        encrypted_detail = str(screen.query_one("#detail", Static).render())

        assert "size: 1.5 MiB" in encrypted_detail
        assert "on disk" not in encrypted_detail


async def test_file_table_label_shows_the_cloud_file_icon() -> None:
    """The icon column shows ``FILE_STATE_ICON``'s glyph for ``Node.file_state``."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    normal_leaf = leaf_node("normal.txt", "normal")
    cloud_leaf = leaf_node("cloud.txt", "cloud", file_state=FileState.CLOUD_ONLY)
    encrypted_leaf = leaf_node("secret.docx", "secret", file_state=FileState.ENCRYPTED)
    provider = ConfigurableProvider(root, {str(root_ref): [normal_leaf, cloud_leaf, encrypted_leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#file-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 3)
        rows = {str(table.get_row_at(i)[0]): str(table.get_row_at(i)[1]) for i in range(table.row_count)}
        assert rows["normal.txt"] == ""
        assert rows["cloud.txt"] == "☁"
        assert rows["secret.docx"] == "🔒"


async def test_a_name_shaped_like_rich_markup_does_not_crash_the_tree_or_the_file_table() -> None:
    """A name like ``"a[/]b"`` is escaped, so painting ``Tree`` and
    ``DataTable`` does not raise ``MarkupError``."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "folder")), name="a[/]b folder", is_leaf=False)
    leaf = leaf_node("a[/]b.txt", "leaf")
    provider = ConfigurableProvider(root, {str(root_ref): [folder, leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#file-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 2)
        await settle(pilot)  # the DataTable's deferred _on_idle paint pass runs
        tree = app.screen.query_one("#folder-tree", Tree)
        assert len(tree.root.children) == 1  # the folder, not the leaf


async def test_a_version_display_name_shaped_like_rich_markup_does_not_crash_the_breadcrumb_or_tree_root() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {str(root_ref): []})
    marked_up_version = make_version(display_name="a[/]b")
    app = FakeApp(marked_up_version, FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        # The breadcrumb, unlike the tree root label, is never overwritten by a later reconcile.
        breadcrumb = screen.query_one("#breadcrumb", Static)
        await wait_until(pilot, lambda: "a[/]b" in str(breadcrumb.render()))


async def test_show_detail_fetches_nothing_without_a_provider() -> None:
    """With no provider (``catalog.provider()`` failed), ``_show_detail`` starts no fetch."""
    app = FakeApp(make_version(), FakeRepo(None, provider_error=ApmRepoError("boom")))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: screen.store.model.root_error is not None)
        assert screen.store.model.provider is None

        leaf = leaf_node("item.bin", "item")
        screen._show_detail(leaf)
        header_only = str(screen.query_one("#detail", Static).render())
        await settle(pilot)
        assert isinstance(screen.store.model.detail.body, DetailIdle)  # type: ignore[union-attr]
        assert str(screen.query_one("#detail", Static).render()) == header_only


async def test_late_preview_for_a_node_the_user_moved_away_from_is_discarded() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=FakeContentSource(b"<html><body><p>hello world</p></body></html>"),  # type: ignore[arg-type]
    )
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)

        screen._show_detail(leaf)  # schedules a preview worker
        screen._show_detail(root)  # the user moves on before the worker resolves
        header_only = str(screen.query_one("#detail", Static).render())
        await wait_for_workers(pilot, group=DETAIL_GROUP)  # the abandoned fetch is over
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "hello world" not in header_only
        assert screen.store.model.detail.node is root  # type: ignore[union-attr]
