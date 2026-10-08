"""Unit tests for ``synology_apm_repo.sdk.units.saas.site`` over a synthetic
repository root whose ``saas_obj`` content embeds two ZSTD-compressed service
DBs (``list_version_table`` + ``item_version_table``). Item content and
names are content, so they are proven here; the real-data counterpart,
``tests/integration/browser/test_browser_screens_unit_screen_preview.py``,
checks a replayed Site listing's shape only."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

import pytest

from support.model_factories import make_version
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.dedup_file import ByteRangeView
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotRestorableError, UnsupportedDataFormatError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import Node, NodeRole, UnitKind
from synology_apm_repo.sdk.units.content.saas_site import build_values_json
from synology_apm_repo.sdk.units.provider_kit import mtime_from_epoch
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.site import (
    _display_name,
    _is_document_library,
    _is_folder,
    _self_id_of,
    open_site_provider,
)
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from unit.sdk.saas_fakes import (
    SaasStreamIds,
    site_item_db,
    site_list_db,
    write_indexed_saas_obj,
    write_saas_obj,
    write_saas_stream_dbs,
)

_STREAM_UUID = "site-stream-uuid"
_IDS = SaasStreamIds(stream_id=15, stream_uuid=_STREAM_UUID)

_FILE_CONTENT = b"the real document library file bytes"


def _build_site_repo(
    tmp_path: Path,
    *,
    session_id: int = 10,
    extra_items: list[tuple[str, str, str, str, str, str, str | None, str, str]] | None = None,
    extra_payloads: list[tuple[str, bytes]] | None = None,
) -> None:
    write_saas_stream_dbs(tmp_path, _IDS)

    list_db_bytes = site_list_db(
        [
            ("list-1", "Tasks", "meta_list_1", 0, "", 1_700_000_000),
            # A document library's top level is a non-empty root_folder_id;
            # its create_time differs from Tasks' so each mtime is traceable.
            ("list-2", "Docs", "meta_list_2", 1, "root-docs", 1_700_100_000),
        ]
    )
    item_db_bytes = site_item_db(
        [
            # a plain list row (Tasks) — no file_id, no content in META
            ("1", "list-1", "", "", "Task A", "0", "meta_item_1", "", ""),
            # a document library file (Docs) — file content via content_list.
            # Its title is a content title, unlike its url_path file name.
            ("2", "list-2", "file-abc", "root-docs", "Quarterly Report", "FILE", "meta_item_2", "/report.docx", "36"),
            # a document library folder (Docs) with a nested file: empty
            # title, item_type "1" (the numeric form of "FOLDER"), and
            # value1 "null", which is not a size.
            ("3", "list-2", "folder-xyz", "root-docs", "", "1", "meta_item_3", "/Subfolder", "null"),
            (
                "4",
                "list-2",
                "file-nested",
                "folder-xyz",
                "nested.txt",
                "FILE",
                "meta_item_4",
                "/Subfolder/nested.txt",
                "19",
            ),
            *(extra_items or []),
        ]
    )
    meta_item_1 = json.dumps({"version": "1.0", "values": {"Title": "Task A"}, "content_list": []}).encode()
    meta_item_2 = json.dumps(
        {"version": "1.0", "values": {}, "content_list": [{"type": 2, "object_id": "content_obj_2"}]}
    ).encode()
    meta_item_3 = json.dumps({"version": "1.0", "values": {}, "content_list": []}).encode()
    meta_item_4 = json.dumps(
        {"version": "1.0", "values": {}, "content_list": [{"type": 2, "object_id": "content_obj_4"}]}
    ).encode()

    payloads = [
        ("list_svc", list_db_bytes),
        ("item_svc", item_db_bytes),
        ("meta_item_1", meta_item_1),
        ("meta_item_2", meta_item_2),
        ("meta_item_3", meta_item_3),
        ("meta_item_4", meta_item_4),
        ("content_obj_2", _FILE_CONTENT),
        ("content_obj_4", b"nested file content"),
        ("meta_list_1", b'{"version": "1.0", "metadata": {}, "fields": {}, "views": {}}'),
        ("meta_list_2", b'{"version": "1.0", "metadata": {}, "fields": {}, "views": {}}'),
        *(extra_payloads or []),
    ]
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=payloads,
        db_objects=[("site_list_db", "list_svc"), ("site_item_db", "item_svc")],
    )


def _version() -> Version:
    return make_version(
        version_id=61,
        version_uid="vuid-site",
        target_type="M365",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


@pytest.fixture
async def provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider[Any]]:
    _build_site_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await open_site_provider(repo, _version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


async def _all_lists(provider: SaasWorkloadProvider[Any]) -> list[Node]:
    """Every List/Library node across the "Document Library"/"List"
    categories (``TestCategorization`` covers that level itself)."""
    categories = await provider.children(provider.root())
    lists: list[Node] = []
    for category in categories:
        lists.extend(await provider.children(category))
    return lists


class TestFolderExport:
    """``sdk.export``'s tree export over a site provider: the "List" category and a List's own group
    node are non-leaf nodes the browser shows specially (a hidden tree level, a spreadsheet overview), but
    for an export they are ordinary folders."""

    async def test_the_site_root_plans_every_list_item_and_library_file(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        from synology_apm_repo.sdk.export import plan_tree_export

        plan = await plan_tree_export(provider, provider.root(), Path("out"))

        assert plan.skipped == []
        assert sorted(item.relative for item in plan.items) == [
            "Document Library/Docs/Subfolder/nested.txt",
            "Document Library/Docs/report.docx",
            "List/Tasks/Task A",
        ]

    async def test_the_flat_list_category_exports_the_items_of_every_list_below_it(
        self, provider: SaasWorkloadProvider[Any], tmp_path: Path
    ) -> None:
        from synology_apm_repo.sdk.export import plan_tree_export, preflight_tree, run_tree_export

        [category] = [n for n in await provider.children(provider.root()) if (n.role is NodeRole.FLAT_CATEGORY)]
        out = tmp_path / "out"
        plan = await plan_tree_export(provider, category, out)
        done = await run_tree_export(provider, plan, out, preflight=preflight_tree(plan, out, force=False))

        assert [item.relative for item, _ in done.exported] == ["Tasks/Task A"]
        assert (out / "Tasks" / "Task A").stat().st_size > 0

    async def test_a_list_group_node_exports_its_items(
        self, provider: SaasWorkloadProvider[Any], tmp_path: Path
    ) -> None:
        from synology_apm_repo.sdk.export import plan_tree_export, preflight_tree, run_tree_export

        [tasks] = [n for n in await _all_lists(provider) if n.role is NodeRole.LIST_OVERVIEW]
        out = tmp_path / "out"
        plan = await plan_tree_export(provider, tasks, out)
        done = await run_tree_export(provider, plan, out, preflight=preflight_tree(plan, out, force=False))

        assert [item.relative for item, _ in done.exported] == ["Task A"]


class TestCategorization:
    async def test_root_lists_the_two_categories(self, provider: SaasWorkloadProvider[Any]) -> None:
        categories = await provider.children(provider.root())
        assert {n.name for n in categories} == {"Document Library", "List"}
        assert all(not n.is_leaf for n in categories)

    async def test_tasks_is_under_list_docs_is_under_document_library(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        categories = {n.name: n for n in await provider.children(provider.root())}
        list_names = {n.name for n in await provider.children(categories["List"])}
        doc_library_names = {n.name for n in await provider.children(categories["Document Library"])}
        assert list_names == {"Tasks"}
        assert doc_library_names == {"Docs"}

    async def test_list_category_overrides_leaf_kind_to_category_group(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        """The "List" category holds only List group nodes, so it reports
        ``CATEGORY_GROUP`` (Name/Created columns in the browser); Document
        Library keeps the provider's default ``SITE_ITEM``."""
        categories = {n.name: n for n in await provider.children(provider.root())}
        assert (categories["List"]).leaf_kind is UnitKind.CATEGORY_GROUP
        assert (categories["Document Library"]).leaf_kind is UnitKind.SITE_ITEM

    async def test_a_list_group_node_is_flagged_for_overview_a_doc_library_is_not(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        """``NodeRole.LIST_OVERVIEW`` makes the browser show a List as a
        spreadsheet-style overview instead of a tree; a document library
        browses as a folder tree. A List's items stay listable through
        ``provider.children()`` either way."""
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        assert tasks.role is NodeRole.LIST_OVERVIEW
        assert docs.role is NodeRole.ORDINARY

    async def test_both_a_list_and_a_doc_library_group_node_get_their_own_real_mtime(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        # Both take the list row's create_time.
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        assert tasks.mtime == mtime_from_epoch(1_700_000_000)
        assert docs.mtime == mtime_from_epoch(1_700_100_000)

    async def test_a_nested_document_library_folder_gets_no_list_level_mtime(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        # Only the list's own group node (key length 2) takes create_time.
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        subfolder = next(n for n in await provider.children(docs) if n.name == "Subfolder")
        assert subfolder.mtime is None


class TestTree:
    async def test_tasks_list_has_one_plain_row(self, provider: SaasWorkloadProvider[Any]) -> None:
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        items = await provider.children(tasks)
        assert len(items) == 1
        assert items[0].name == "Task A"
        assert items[0].is_leaf is True
        assert items[0].kind is UnitKind.SITE_ITEM
        # An empty file_id: value1 is not a byte size on a plain list row.
        assert items[0].size is None

    async def test_docs_list_has_a_file_and_a_folder_at_top_level(self, provider: SaasWorkloadProvider[Any]) -> None:
        """The top level is anchored at the non-empty ``root_folder_id``
        ("root-docs"), and both a file (whose ``title`` is a content title)
        and a folder (whose ``title`` is empty) are named from ``url_path``."""
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        items = await provider.children(docs)
        names = {n.name for n in items}
        assert names == {"report.docx", "Subfolder"}
        report = next(n for n in items if n.name == "report.docx")
        subfolder = next(n for n in items if n.name == "Subfolder")
        assert report.is_leaf is True
        assert subfolder.is_leaf is False
        # value1="36" is _FILE_CONTENT's length; a folder has no size.
        assert report.size == len(_FILE_CONTENT)
        assert subfolder.size is None

    async def test_nested_folder_lists_its_own_file(self, provider: SaasWorkloadProvider[Any]) -> None:
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        subfolder = next(n for n in await provider.children(docs) if n.name == "Subfolder")
        nested = await provider.children(subfolder)
        assert [n.name for n in nested] == ["nested.txt"]
        assert nested[0].size == len(b"nested file content")

    async def test_docs_list_sorts_folders_first_then_files_by_display_name_not_title(self, tmp_path: Path) -> None:
        """The two extra files' ``title``s rank opposite to their
        ``url_path`` names, so the order shows ``_NAME_ORDER_SQL`` sorts by
        the displayed name."""
        extra_items: list[tuple[str, str, str, str, str, str, str | None, str, str]] = [
            ("6", "list-2", "file-f6", "root-docs", "Zzz Title", "FILE", "meta_item_2", "/aaa-file.docx", "1"),
            ("7", "list-2", "file-f7", "root-docs", "Aaa Title", "FILE", "meta_item_2", "/zzz-file.docx", "1"),
        ]
        _build_site_repo(tmp_path, extra_items=extra_items)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_site_provider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                items = await provider.children(docs)
                assert [n.name for n in items] == ["Subfolder", "aaa-file.docx", "report.docx", "zzz-file.docx"]
            finally:
                await provider.close()

    async def test_docs_list_item_with_no_url_path_falls_back_to_title_for_sorting_too(self, tmp_path: Path) -> None:
        """``_display_name`` names a document-library item with an empty
        ``url_path`` by its ``title``; ``_NAME_ORDER_SQL`` must too, since
        keying on the empty ``url_path`` would sort it first."""
        extra_items: list[tuple[str, str, str, str, str, str, str | None, str, str]] = [
            ("6", "list-2", "file-f6", "root-docs", "zzzzz-fallback-title", "FILE", "meta_item_2", "", "1"),
        ]
        _build_site_repo(tmp_path, extra_items=extra_items)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_site_provider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                items = await provider.children(docs)
                assert [n.name for n in items] == ["Subfolder", "report.docx", "zzzzz-fallback-title"]
            finally:
                await provider.close()

    async def test_docs_list_items_with_no_title_or_url_path_sort_by_file_id_not_item_id(self, tmp_path: Path) -> None:
        """With neither ``url_path`` nor ``title``, an item is named by
        ``_self_id_of`` (``file_id`` before ``item_id``), and
        ``_NAME_ORDER_SQL`` must sort the same way; these items'
        ``item_id``s rank opposite to their ``file_id``s."""
        extra_items: list[tuple[str, str, str, str, str, str, str | None, str, str]] = [
            ("6", "list-2", "zzz-file-id-for-item-6", "root-docs", "", "FILE", "meta_item_2", "", "1"),
            ("9", "list-2", "aaa-file-id-for-item-9", "root-docs", "", "FILE", "meta_item_2", "", "1"),
        ]
        _build_site_repo(tmp_path, extra_items=extra_items)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_site_provider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                items = await provider.children(docs)
                assert [n.name for n in items] == [
                    "Subfolder",
                    "report.docx",
                    "aaa-file-id-for-item-9",
                    "zzz-file-id-for-item-6",
                ]
            finally:
                await provider.close()

    async def test_docs_list_top_level_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One folder's children cost one ``WHERE list_id = ? AND
        parent_folder_id = ?`` query."""
        from synology_apm_repo.sdk.storage.table import Table

        calls: list[tuple[str, Sequence[object]]] = []
        original_select = Table.select

        def counting_select(
            self: Table,
            where: str = "",
            params: Sequence[object] = (),
            *,
            order_by: str | None = None,
            limit: int | None = None,
            offset: int = 0,
        ) -> AsyncIterator[dict[str, object | None]]:
            calls.append((where, params))
            return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

        monkeypatch.setattr(Table, "select", counting_select)

        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        calls.clear()
        await provider.children(docs)
        assert calls == [("list_id = ? AND parent_folder_id = ?", ("list-2", "root-docs"))]

    async def test_a_leaf_item_with_an_unparseable_value1_has_no_size(self, tmp_path: Path) -> None:
        # A leaf (file_id set) whose value1 isn't a number.
        extra_item = (
            "5",
            "list-2",
            "file-garbled",
            "root-docs",
            "Garbled",
            "FILE",
            "meta_item_3",
            "/garbled.docx",
            "not-a-number",
        )
        _build_site_repo(tmp_path, extra_items=[extra_item])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_site_provider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                garbled = next(n for n in await provider.children(docs) if n.name == "garbled.docx")
                assert garbled.size is None
            finally:
                await provider.close()


class TestContent:
    async def test_plain_row_content_is_its_values_json(self, provider: SaasWorkloadProvider[Any]) -> None:
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        [task] = await provider.children(tasks)
        content = (await provider.unit(task)).content
        # LazyArtifact.size is None until assembled, so read the whole artifact.
        data = await content.read()
        assert json.loads(data) == {"Title": "Task A"}

    async def test_file_content_is_a_byte_range_view_not_an_artifact(self, provider: SaasWorkloadProvider[Any]) -> None:
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        report = next(n for n in await provider.children(docs) if n.name == "report.docx")
        content = (await provider.unit(report)).content
        assert isinstance(content, ByteRangeView)
        assert await content.read(0, content.size or 0) == _FILE_CONTENT

    async def test_nested_file_content_reads_back_correctly(self, provider: SaasWorkloadProvider[Any]) -> None:
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        subfolder = next(n for n in await provider.children(docs) if n.name == "Subfolder")
        nested_file = next(n for n in await provider.children(subfolder) if n.name == "nested.txt")
        content = (await provider.unit(nested_file)).content
        assert await content.read(0, content.size or 0) == b"nested file content"

    async def test_unit_on_an_item_with_no_meta_object_id_raises(self, tmp_path: Path) -> None:
        # A NULL meta_object_id: _content can't locate the META object.
        extra_item = ("5", "list-2", "file-no-meta", "root-docs", "No Meta", "FILE", None, "/no-meta.docx", "10")
        _build_site_repo(tmp_path, extra_items=[extra_item])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_site_provider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                no_meta = next(n for n in await provider.children(docs) if n.name == "no-meta.docx")
                with pytest.raises(NotRestorableError, match="not a restorable unit"):
                    await provider.unit(no_meta)
            finally:
                await provider.close()

    async def test_unit_on_an_item_with_malformed_meta_json_raises_data_corrupt(self, tmp_path: Path) -> None:
        extra_item = ("5", "list-2", "file-bad-meta", "root-docs", "Bad Meta", "FILE", "meta_item_5", "/bad.docx", "1")
        _build_site_repo(tmp_path, extra_items=[extra_item], extra_payloads=[("meta_item_5", b"not json at all")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_site_provider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                bad = next(n for n in await provider.children(docs) if n.name == "bad.docx")
                with pytest.raises(DataCorruptError, match="did not parse as JSON"):
                    await provider.unit(bad)
            finally:
                await provider.close()

    async def test_unit_on_an_item_with_a_stale_content_object_id_raises_not_restorable(self, tmp_path: Path) -> None:
        # META's content_list names an object_id the ObjectDB doesn't have.
        extra_item = ("5", "list-2", "file-stale", "root-docs", "Stale", "FILE", "meta_item_5", "/stale.docx", "1")
        stale_meta = json.dumps(
            {"version": "1.0", "values": {}, "content_list": [{"type": 2, "object_id": "missing_content_obj"}]}
        ).encode()
        _build_site_repo(tmp_path, extra_items=[extra_item], extra_payloads=[("meta_item_5", stale_meta)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_site_provider(repo, _version(), saas_streams)
            try:
                [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
                stale = next(n for n in await provider.children(docs) if n.name == "stale.docx")
                with pytest.raises(NotRestorableError, match="not a restorable unit"):
                    await provider.unit(stale)
            finally:
                await provider.close()

    async def test_unit_on_a_list_node_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        [tasks] = [n for n in await _all_lists(provider) if n.name == "Tasks"]
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(tasks)

    async def test_unit_on_a_folder_node_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        [docs] = [n for n in await _all_lists(provider) if n.name == "Docs"]
        subfolder = next(n for n in await provider.children(docs) if n.name == "Subfolder")
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(subfolder)

    async def test_unit_on_the_root_node_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(provider.root())

    async def test_unit_on_a_bare_category_node_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        categories = await provider.children(provider.root())
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(categories[0])


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_site_tables_exist(self, tmp_path: Path) -> None:
        write_saas_stream_dbs(tmp_path, _IDS)

        write_saas_obj(tmp_path, _IDS, session_id=10, content=b"\x00" * 4096)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError, match="no object-name index for table"):
                await open_site_provider(repo, _version(), saas_streams)

    async def test_children_of_a_list_with_no_indexed_items_returns_empty(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        # A list_id with no rows in item_version_table.
        phantom_list = Node(ref=provider.root().ref, name="phantom", is_leaf=False, details={"list_id": "no-such-list"})
        assert await provider.children(phantom_list) == []


class TestIsDocumentLibrary:
    """Includes a missing or ``None`` ``list_type``, which ``site_list_db``
    never produces."""

    @pytest.mark.parametrize(
        ("row", "expected"),
        [
            pytest.param({"list_type": 1}, True, id="list_type_1_is_a_document_library"),
            pytest.param({"list_type": 0}, False, id="list_type_0_is_not_a_document_library"),
            pytest.param({}, False, id="missing_list_type_column_is_not_a_document_library"),
            pytest.param({"list_type": None}, False, id="none_list_type_is_not_a_document_library"),
        ],
    )
    def test_is_document_library(self, row: dict[str, object | None], expected: bool) -> None:
        assert _is_document_library(row) is expected


class TestIsFolder:
    """``item_type`` is "FILE"/"FOLDER" or their numeric equivalents
    (FORMAT-SPEC.md: SharePoint Site); ``_build_site_repo``'s folder row uses
    only the numeric "1"."""

    @pytest.mark.parametrize(
        ("item_type", "expected"),
        [
            pytest.param("1", True, id="numeric_folder_encoding"),
            pytest.param("FOLDER", True, id="string_folder_encoding"),
            pytest.param("0", False, id="numeric_file_encoding_is_not_a_folder"),
            pytest.param("FILE", False, id="string_file_encoding_is_not_a_folder"),
        ],
    )
    def test_is_folder(self, item_type: str, expected: bool) -> None:
        assert _is_folder({"item_type": item_type}) is expected


class TestDisplayName:
    @pytest.mark.parametrize(
        ("row", "expected"),
        [
            pytest.param(
                {"file_id": "f1", "item_id": "i1", "url_path": "/sites/x/Shared Documents/report.docx", "title": ""},
                "report.docx",
                id="document_library_item_uses_the_url_path_basename",
            ),
            pytest.param(
                {"file_id": "", "item_id": "i1", "url_path": "", "title": "My List Item"},
                "My List Item",
                id="general_list_row_uses_its_own_title",
            ),
        ],
    )
    def test_display_name(self, row: dict[str, object | None], expected: str) -> None:
        assert _display_name(row) == expected

    @pytest.mark.parametrize(
        ("row", "expected"),
        [
            pytest.param(
                {"file_id": "", "item_id": "i1", "url_path": "", "title": ""},
                "i1",
                id="falls_back_to_self_id_when_both_title_and_url_path_are_empty",
            ),
            pytest.param(
                {"file_id": "f1", "item_id": "i1", "url_path": "", "title": ""},
                "f1",
                id="falls_back_to_file_id_not_item_id_when_file_id_is_set",
            ),
        ],
    )
    def test_display_name_falls_back_to_self_id(self, row: dict[str, object | None], expected: str) -> None:
        assert _display_name(row) == _self_id_of(row) == expected


class TestBuildValuesJson:
    """``units.content.saas_site.build_values_json``: an empty ``values``
    dict and non-string values, which no provider-level test builds."""

    def test_empty_values_serializes_to_an_empty_json_object(self) -> None:
        assert build_values_json({}) == b"{}"

    def test_non_string_values_round_trip_through_json(self) -> None:
        data = build_values_json({"Count": 3, "Active": True, "Notes": None})
        assert json.loads(data) == {"Count": 3, "Active": True, "Notes": None}

    def test_result_is_utf8_encoded_bytes(self) -> None:
        data = build_values_json({"Title": "café"})
        assert isinstance(data, bytes)
        assert json.loads(data.decode("utf-8")) == {"Title": "café"}
