"""Unit tests for ``synology_apm_repo.sdk.units.saas.drive`` over a
synthetic repository root (``unit.sdk.saas_fakes``) whose ``saas_obj``
embeds a ZSTD-compressed ``item_table`` service DB."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from support.model_factories import make_version
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import NotRestorableError, UnsupportedDataFormatError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import Node, SupportsDirectRefLookup, UnitKind
from synology_apm_repo.sdk.units.resolve import find_node, find_path_with_children
from synology_apm_repo.sdk.units.saas.drive import _extras, _root_folder_id, open_drive_provider
from synology_apm_repo.sdk.units.saas.provider import RecursiveTreeSaasProvider, SaasHandle, SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.saas.tree_strategy import RecursiveTree
from unit.sdk.saas_fakes import (
    SaasStreamIds,
    item_service_db,
    write_indexed_saas_obj,
    write_saas_obj,
    write_saas_stream_dbs,
)

_STREAM_UUID = "drive-stream-uuid"
_IDS = SaasStreamIds(stream_id=12, stream_uuid=_STREAM_UUID)

_CONTENT_A = b"file A content"
_CONTENT_B = b"file B content, a little longer"


def _build_drive_repo(
    tmp_path: Path,
    *,
    session_id: int = 7,
    missing_hash_column: bool = False,
    extra_items: list[tuple[str, str, str, int, int, str, str]] | None = None,
    root_folder_id: str | None = "root-id",
) -> None:
    write_saas_stream_dbs(tmp_path, _IDS, target_type="GW")

    items = [
        ("folder-id-1", "folder-1", "root-id", 0, 0, "", ""),
        ("item-a", "file-a.txt", "root-id", 1, len(_CONTENT_A), "content_a", "hash-a"),
        ("item-b", "file-b.txt", "folder-id-1", 1, len(_CONTENT_B), "content_b", "hash-b"),
        *(extra_items or []),
    ]
    service_db_bytes = item_service_db(
        root_folder_id=root_folder_id, items=items, missing_hash_column=missing_hash_column
    )

    payloads = [("svc_obj", service_db_bytes), ("content_a", _CONTENT_A), ("content_b", _CONTENT_B)]
    write_indexed_saas_obj(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=payloads,
        db_objects=[("drive_db", "svc_obj")],
    )


def _version() -> Version:
    return make_version(
        version_id=61,
        version_uid="vuid-drive",
        target_type="GW",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


@pytest.fixture
async def provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider[Any]]:
    _build_drive_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await open_drive_provider(repo, _version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


class TestTree:
    async def test_root_lists_top_level_entries(self, provider: SaasWorkloadProvider[Any]) -> None:
        top = await provider.children(provider.root())
        assert {n.name for n in top} == {"folder-1", "file-a.txt"}
        folder = next(n for n in top if n.name == "folder-1")
        assert folder.is_leaf is False
        file_a = next(n for n in top if n.name == "file-a.txt")
        assert file_a.is_leaf is True
        assert file_a.kind is UnitKind.DRIVE_ITEM
        assert file_a.size == len(_CONTENT_A)

    async def test_root_lists_the_folder_before_the_file_despite_alphabetical_order(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        # "file-a.txt" sorts before "folder-1" by name, so only the
        # folders-first rank puts the folder first.
        top = await provider.children(provider.root())
        assert [n.name for n in top] == ["folder-1", "file-a.txt"]

    async def test_nested_folder_lists_its_own_child(self, provider: SaasWorkloadProvider[Any]) -> None:
        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        children = await provider.children(folder)
        assert [n.name for n in children] == ["file-b.txt"]

    async def test_children_of_a_leaf_returns_empty(self, provider: SaasWorkloadProvider[Any]) -> None:
        top = await provider.children(provider.root())
        file_a = next(n for n in top if n.name == "file-a.txt")
        assert await provider.children(file_a) == []

    async def test_children_of_a_node_with_no_item_id_returns_empty(self, provider: SaasWorkloadProvider[Any]) -> None:
        stray = Node(ref=provider.root().ref, name="stray", is_leaf=False)
        assert await provider.children(stray) == []

    async def test_details_carry_the_hash_column(self, provider: SaasWorkloadProvider[Any]) -> None:
        top = await provider.children(provider.root())
        file_a = next(n for n in top if n.name == "file-a.txt")
        assert file_a.details["hash"] == "hash-a"

    async def test_a_leaf_carries_content_object_id_and_mtime(self, provider: SaasWorkloadProvider[Any]) -> None:
        top = await provider.children(provider.root())
        file_a = next(n for n in top if n.name == "file-a.txt")
        assert file_a.details["content_object_id"] == "content_a"
        assert file_a.mtime == datetime.fromtimestamp(0, UTC)

    async def test_a_folder_has_no_mtime(self, provider: SaasWorkloadProvider[Any]) -> None:
        # leaf_extras only runs for a leaf row: a folder's own mtime, though
        # item_table carries one for every row, is never surfaced.
        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        assert folder.mtime is None

    def test_an_out_of_range_mtime_degrades_to_no_mtime(self) -> None:
        """``item_table.mtime`` is unvalidated: an out-of-``datetime``-range
        value must not raise out of ``_extras`` and fail the whole listing."""
        row: dict[str, object | None] = {
            "content_object_id": "content_a",
            "hash": None,
            "mtime": 99999999999999,
        }
        extras = _extras(cast(SaasWorkloadProvider[Any], None), row)
        assert extras.mtime is None
        assert extras.details["content_object_id"] == "content_a"

    async def test_pagination(self, provider: SaasWorkloadProvider[Any]) -> None:
        full = await provider.children(provider.root())
        page = await provider.children(provider.root(), offset=0, limit=1)
        assert len(page) == 1
        assert page[0].ref == full[0].ref

    async def test_pagination_with_a_nonzero_offset(self, provider: SaasWorkloadProvider[Any]) -> None:
        full = await provider.children(provider.root())
        assert len(full) >= 2
        page = await provider.children(provider.root(), offset=1, limit=1)
        assert len(page) == 1
        assert page[0].ref == full[1].ref

    async def test_children_of_one_folder_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each ``children_of()`` call is one ``WHERE parent_folder_id = ?``
        query, not a full-table scan."""
        from synology_apm_repo.sdk.storage.table import Table

        calls: list[object] = []
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
            calls.append((where, tuple(params)))
            return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

        monkeypatch.setattr(Table, "select", counting_select)

        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        calls.clear()
        await provider.children(folder)
        assert calls == [("parent_folder_id = ?", ("folder-id-1",))]


class TestContent:
    async def test_reads_back_real_file_content(self, provider: SaasWorkloadProvider[Any]) -> None:
        top = await provider.children(provider.root())
        file_a = next(n for n in top if n.name == "file-a.txt")
        content = (await provider.unit(file_a)).content
        assert await content.read(0, content.size or 0) == _CONTENT_A

    async def test_reads_back_nested_file_content(self, provider: SaasWorkloadProvider[Any]) -> None:
        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        file_b = next(n for n in await provider.children(folder) if n.name == "file-b.txt")
        content = (await provider.unit(file_b)).content
        assert await content.read(0, content.size or 0) == _CONTENT_B

    async def test_unit_on_a_folder_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        top = await provider.children(provider.root())
        folder = next(n for n in top if n.name == "folder-1")
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(folder)

    async def test_unit_on_the_root_node_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        # The root node carries no tree row, distinct from the folder
        # case above (a row, but not a leaf).
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(provider.root())

    def test_repo_property_exposes_the_underlying_repository(self, provider: SaasWorkloadProvider[Any]) -> None:
        assert isinstance(provider.repo, DedupRepo)

    async def test_unit_on_a_leaf_item_with_no_content_object_id_raises(self, tmp_path: Path) -> None:
        # A file-type item with no content_object_id at all (real shape,
        # e.g. a Drive shortcut/link item).
        _build_drive_repo(tmp_path, extra_items=[("item-c", "empty.txt", "root-id", 1, 0, "", "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_drive_provider(repo, _version(), saas_streams)
            try:
                top = await provider.children(provider.root())
                empty_item = next(n for n in top if n.name == "empty.txt")
                with pytest.raises(NotRestorableError, match="not a restorable unit"):
                    await provider.unit(empty_item)
            finally:
                await provider.close()

    async def test_unit_on_a_leaf_item_with_a_stale_content_object_id_raises_not_restorable(
        self, tmp_path: Path
    ) -> None:
        # item_table names a content_object_id the ObjectDB doesn't have:
        # this one item isn't restorable, the caller doesn't crash.
        _build_drive_repo(tmp_path, extra_items=[("item-d", "stale.txt", "root-id", 1, 1, "missing-content-id", "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_drive_provider(repo, _version(), saas_streams)
            try:
                top = await provider.children(provider.root())
                stale_item = next(n for n in top if n.name == "stale.txt")
                with pytest.raises(NotRestorableError, match="not a restorable unit"):
                    await provider.unit(stale_item)
            finally:
                await provider.close()


class TestSchemaTolerance:
    async def test_missing_optional_hash_column_reads_back_as_none(self, tmp_path: Path) -> None:
        _build_drive_repo(tmp_path, missing_hash_column=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_drive_provider(repo, _version(), saas_streams)
            try:
                top = await provider.children(provider.root())
                file_a = next(n for n in top if n.name == "file-a.txt")
                assert file_a.details["hash"] is None
                content = (await provider.unit(file_a)).content
                assert await content.read(0, content.size or 0) == _CONTENT_A
            finally:
                await provider.close()


class TestDirectRefLookup:
    """Drive is the one SaaS shape backed by ``RecursiveTreeSaasProvider``:
    full-stack coverage of ``SupportsDirectRefLookup`` and of
    ``units.resolve``'s dispatch onto it."""

    async def test_provider_satisfies_the_protocol(self, provider: SaasWorkloadProvider[Any]) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)

    async def test_resolve_extra_finds_a_nested_item_without_walking_its_parent_folder(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Locating ``file-b.txt`` (nested under ``folder-1``) never calls ``children()``."""
        original_children = SaasWorkloadProvider.children

        calls: list[object] = []

        async def counting_children(
            self: SaasWorkloadProvider[Any], node: Node, offset: int = 0, limit: int | None = None
        ) -> list[Node]:
            calls.append(1)
            return await original_children(self, node, offset, limit)

        monkeypatch.setattr(SaasWorkloadProvider, "children", counting_children)

        assert isinstance(provider, SupportsDirectRefLookup)
        node = await provider.resolve_extra(("item-b",))
        assert node is not None
        assert node.name == "file-b.txt"
        assert node.is_leaf is True
        assert calls == []

    async def test_resolve_extra_returns_none_for_an_unknown_id(self, provider: SaasWorkloadProvider[Any]) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        assert await provider.resolve_extra(("no-such-item",)) is None

    async def test_resolve_extra_rejects_a_multi_segment_key(self, provider: SaasWorkloadProvider[Any]) -> None:
        """Drive's key is always a 1-tuple."""
        assert isinstance(provider, SupportsDirectRefLookup)
        assert await provider.resolve_extra(("item-b", "extra")) is None

    async def test_parent_of_a_top_level_item_is_the_version_root(self, provider: SaasWorkloadProvider[Any]) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        item_a = await provider.resolve_extra(("item-a",))
        assert item_a is not None
        parent = await provider.parent_of(item_a)
        assert parent == provider.root()

    async def test_parent_of_a_nested_item_is_its_own_folder(self, provider: SaasWorkloadProvider[Any]) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        item_b = await provider.resolve_extra(("item-b",))
        assert item_b is not None
        parent = await provider.parent_of(item_b)
        assert parent is not None
        assert parent.name == "folder-1"
        assert parent.is_leaf is False

    async def test_parent_of_the_version_root_is_none(self, provider: SaasWorkloadProvider[Any]) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        assert await provider.parent_of(provider.root()) is None

    async def test_a_top_level_items_parent_id_is_none(self, provider: SaasWorkloadProvider[Any]) -> None:
        tree = cast("RecursiveTree", provider._tree)
        [first, *_] = await tree.children_of(())
        assert first.row is not None
        assert tree.parent_id_of(first.row) is None

    async def test_parent_of_a_node_without_a_listed_row_is_none(self, provider: SaasWorkloadProvider[Any]) -> None:
        """A node this provider didn't list carries no row to follow upward."""
        node = dataclasses.replace(provider.root(), handle=SaasHandle(("some-id",)))
        assert isinstance(provider, RecursiveTreeSaasProvider)
        assert await provider.parent_of(node) is None

    async def test_parent_of_returns_none_when_the_parent_id_does_not_match_a_real_row(self, tmp_path: Path) -> None:
        _build_drive_repo(
            tmp_path, extra_items=[("item-orphan", "orphan.txt", "missing-parent-id", 1, 1, "content_a", "")]
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_drive_provider(repo, _version(), saas_streams)
            try:
                assert isinstance(provider, SupportsDirectRefLookup)
                orphan = await provider.resolve_extra(("item-orphan",))
                assert orphan is not None
                assert await provider.parent_of(orphan) is None
            finally:
                await provider.close()

    async def test_find_node_resolves_a_nested_item_through_the_public_entry_point(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        item_b = await provider.resolve_extra(("item-b",))
        assert item_b is not None
        assert await find_node(provider, item_b.ref) == item_b

    async def test_find_path_with_children_rebuilds_the_real_ancestor_chain(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        assert isinstance(provider, SupportsDirectRefLookup)
        item_b = await provider.resolve_extra(("item-b",))
        assert item_b is not None

        result = await find_path_with_children(provider, item_b.ref)

        assert result is not None
        chain, children_by_step = result
        assert [n.name for n in chain] == ["/", "folder-1", "file-b.txt"]
        assert [n.name for n in children_by_step[1]] == ["file-b.txt"]


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_item_table_exists(self, tmp_path: Path) -> None:
        write_saas_stream_dbs(tmp_path, _IDS, target_type="GW")

        # a saas_obj with no embedded ObjectDB at all
        write_saas_obj(tmp_path, _IDS, session_id=7, content=b"\x00" * 4096)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError, match="no object-name index for table"):
                await open_drive_provider(repo, _version(), saas_streams)

    async def test_a_data_corrupt_error_from_objectdb_load_surfaces_as_unsupported_data_format(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The index resolves but the object it names doesn't decode as an
        ObjectDB: ``_open_table_via_index()`` maps the ``DataCorruptError``
        to ``UnsupportedDataFormatError``."""
        from synology_apm_repo.sdk.errors import DataCorruptError
        from synology_apm_repo.sdk.units.saas.drive import _ITEM_TABLE
        from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb

        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise DataCorruptError("synthetic corruption for this test")

        monkeypatch.setattr(ObjectDb, "load", failing_load)

        await _drop_index_hold(provider)
        with pytest.raises(UnsupportedDataFormatError, match="did not validate"):
            await provider._open_table_via_index(_ITEM_TABLE)

    async def test_insufficient_disk_space_from_objectdb_load_surfaces_as_unsupported_data_format(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same mapping for a valid candidate rejected by
        ``ObjectDb.from_bytes``'s free-disk-space check (via a frame's
        declared size): a raw ``ResourceLimitExceededError`` would abort
        ``saas_provider_for``'s whole dispatch loop."""
        from synology_apm_repo.sdk.errors import ResourceLimitExceededError
        from synology_apm_repo.sdk.units.saas.drive import _ITEM_TABLE
        from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb

        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise ResourceLimitExceededError("synthetic: not enough disk space")

        monkeypatch.setattr(ObjectDb, "load", failing_load)

        await _drop_index_hold(provider)
        with pytest.raises(UnsupportedDataFormatError, match="did not validate"):
            await provider._open_table_via_index(_ITEM_TABLE)


async def test_concurrent_first_uses_of_the_index_object_db_take_one_hold(
    provider: SaasWorkloadProvider[Any],
) -> None:
    """Two holds released once would keep the shared ObjectDB open forever."""
    await _drop_index_hold(provider)
    index_db = provider._index_db
    assert index_db is not None

    first, second = await asyncio.gather(provider._index_object_db(), provider._index_object_db())

    assert first is second
    assert index_db._holders == 1
    await provider.close()
    assert index_db._holders == 0
    assert not index_db._once.opened


async def _drop_index_hold(provider: SaasWorkloadProvider[Any]) -> None:
    """Release the index ObjectDB create() already loaded (this provider is
    its only holder, so it closes), so the next table open loads it again."""
    assert provider._held_index_db.opened
    await provider._release_index_db()


async def test_root_folder_id_falls_back_to_empty_string_when_config_table_has_no_row(tmp_path: Path) -> None:
    # root_folder_id=None writes no config_table row at all.
    _build_drive_repo(tmp_path, root_folder_id=None)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await open_drive_provider(repo, _version(), saas_streams)
        try:
            assert await _root_folder_id(provider) == ""
        finally:
            await provider.close()
