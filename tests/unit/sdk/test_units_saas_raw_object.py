"""Unit tests for ``synology_apm_repo.sdk.units.saas.raw_object`` over a
synthetic repository root: a ``copy_target_version`` object-name index
pointing at one embedded ``ObjectDB`` blob, plus a second, index-unreferenced
one for the ``--object-db-id`` override tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest

from support.format_builders import (
    build_object_db,
)
from support.model_factories import make_version
from support.repo_builders import (
    write_copy_target_version_db,
)
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError, NotRestorableError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.saas.context import SharedIndexObjectDb, SharedSaasContext
from synology_apm_repo.sdk.units.saas.object_name_index import ObjectNameIndex
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from unit.sdk.saas_fakes import SaasStreamIds, write_saas_obj, write_saas_stream_dbs

_STREAM_UUID = "raw-stream-uuid"
_IDS = SaasStreamIds(stream_id=11, stream_uuid=_STREAM_UUID)


def _build_raw_object_repo(tmp_path: Path, *, session_id: int = 6, extra_stale_index_entry: bool = False) -> str:
    """One index-referenced ``ObjectDB`` (entries ``cat_a``/``cat_b``) at the
    front of ``saas_obj`` and an index-unreferenced one after it (entry
    ``b_object_1``). Returns the second one's ``object_db_id``."""
    write_saas_stream_dbs(tmp_path, _IDS)

    indexed_content = {"a_object_1": b'{"meta": "a1"}', "a_object_2": b'{"meta": "a2"}'}
    manual_content = {"b_object_1": b'{"meta": "b1"}'}

    def _build_blob(content: dict[str, bytes], base_offset: int) -> tuple[bytes, int, int]:
        """``(object_db_bytes + payload, object_db_offset, object_db_length)``.
        Entry offsets are absolute within ``saas_obj``, so the ``ObjectDB`` is built
        twice: once to learn its length, then with the offsets placed after it."""
        relative_rows = []
        cursor = base_offset
        for object_id, data in content.items():
            relative_rows.append((object_id, cursor, len(data)))
            cursor += len(data)
        object_db_len = len(build_object_db(relative_rows))
        real_base = base_offset + object_db_len
        absolute_rows = []
        cursor = real_base
        payload = b""
        for object_id, data in content.items():
            absolute_rows.append((object_id, cursor, len(data)))
            payload += data
            cursor += len(data)
        object_db_bytes = build_object_db(absolute_rows)
        return object_db_bytes + payload, base_offset, len(object_db_bytes)

    indexed_blob, indexed_offset, indexed_db_len = _build_blob(indexed_content, 0)
    indexed_blob_len_padded = len(indexed_blob) + (-len(indexed_blob) % 4096)
    manual_blob, manual_offset, manual_db_len = _build_blob(manual_content, indexed_blob_len_padded)

    saas_obj_content = indexed_blob + b"\x00" * (indexed_blob_len_padded - len(indexed_blob)) + manual_blob
    write_saas_obj(tmp_path, _IDS, session_id=session_id, content=saas_obj_content)
    db_objects = [("cat_a", "a_object_1"), ("cat_b", "a_object_2")]
    if extra_stale_index_entry:
        # A stale index entry naming an object the ObjectDB doesn't hold.
        db_objects.append(("cat_c", "a_object_missing"))
    write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_{indexed_offset}_{indexed_db_len}",
        db_objects=db_objects,
    )
    return f"{_STREAM_UUID}_{manual_offset}_{manual_db_len}"


def _version() -> Version:
    return make_version(
        version_id=61,
        version_uid="vuid-raw",
        target_type="M365",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


@pytest.fixture
async def manual_object_db_id(tmp_path: Path) -> str:
    return _build_raw_object_repo(tmp_path)


@pytest.fixture
async def provider(tmp_path: Path, manual_object_db_id: str) -> AsyncIterator[RawObjectProvider]:
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with (
        await DedupRepo.open(store, layout) as repo,
        SaasStreamCache(repo) as saas_streams,
        await RawObjectProvider.create(repo, _version(), saas_streams) as p,
    ):
        yield p


@pytest.fixture
async def opened_repo(tmp_path: Path, manual_object_db_id: str) -> AsyncIterator[DedupRepo]:
    """The opened ``DedupRepo``, for tests that build their own ``RawObjectProvider``."""
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo:
        yield repo


class TestTree:
    def test_root_is_not_a_leaf_and_named_after_the_stream(self, provider: RawObjectProvider) -> None:
        root = provider.root()
        assert root.is_leaf is False
        assert root.name == _STREAM_UUID

    async def test_root_children_are_the_index_named_entries(self, provider: RawObjectProvider) -> None:
        nodes = await provider.children(provider.root())
        assert {n.name for n in nodes} == {"cat_a", "cat_b"}
        assert all(n.is_leaf for n in nodes)
        assert all(n.kind is UnitKind.RAW_OBJECT for n in nodes)

    async def test_a_leaf_has_no_children(self, provider: RawObjectProvider) -> None:
        [leaf, *_] = await provider.children(provider.root())
        assert await provider.children(leaf) == []

    async def test_pagination_on_root_children(self, provider: RawObjectProvider) -> None:
        all_nodes = await provider.children(provider.root())
        first_page = await provider.children(provider.root(), offset=0, limit=1)
        assert len(first_page) == 1
        assert first_page[0].ref == all_nodes[0].ref

    async def test_refs_are_stable_and_scoped_under_the_version(self, provider: RawObjectProvider) -> None:
        nodes = await provider.children(provider.root())
        [cat_a] = [n for n in nodes if n.name == "cat_a"]
        assert str(cat_a.ref).endswith("cat_a")
        assert "cat:1" in str(cat_a.ref)
        assert "wl:1" in str(cat_a.ref)
        assert "ver:vuid-raw" in str(cat_a.ref)

    async def test_an_index_entry_the_object_db_lacks_is_silently_skipped(self, tmp_path: Path) -> None:
        # Not flagged as corruption, just absent from the listing.
        _build_raw_object_repo(tmp_path, extra_stale_index_entry=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await RawObjectProvider.create(repo, _version(), saas_streams) as provider,
        ):
            nodes = await provider.children(provider.root())
            assert {n.name for n in nodes} == {"cat_a", "cat_b"}


class TestUnit:
    async def test_unit_reads_back_the_real_content(self, provider: RawObjectProvider) -> None:
        nodes = await provider.children(provider.root())
        [node_a] = [n for n in nodes if n.name == "cat_a"]
        [node_b] = [n for n in nodes if n.name == "cat_b"]

        content_a = (await provider.unit(node_a)).content
        content_b = (await provider.unit(node_b)).content
        assert await content_a.read(0, content_a.size or 0) == b'{"meta": "a1"}'
        assert await content_b.read(0, content_b.size or 0) == b'{"meta": "a2"}'

    async def test_unit_carries_every_field_of_its_node(self, provider: RawObjectProvider) -> None:
        [node, *_] = await provider.children(provider.root())
        unit = await provider.unit(node)
        assert (unit.ref, unit.name, unit.kind, unit.size, unit.handle) == (
            node.ref,
            node.name,
            node.kind,
            node.size,
            node.handle,
        )

    async def test_unit_on_the_root_node_raises(self, provider: RawObjectProvider) -> None:
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(provider.root())


class TestNoObjectNameIndex:
    """A version with no object-name index shows nothing; Pool/ObjectDB is never scanned."""

    async def test_children_is_empty_without_an_object_name_index(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # _build_raw_object_repo() minus write_copy_target_version_db.
        write_saas_stream_dbs(tmp_path, _IDS)
        saas_obj_content = b'{"meta": "orphan"}'
        write_saas_obj(tmp_path, _IDS, session_id=6, content=saas_obj_content)
        content_reads: list[tuple[int, int | None]] = []
        real_read = DedupFile.read

        async def recording_read(self: DedupFile, offset: int = 0, length: int | None = None) -> bytes | bytearray:
            content_reads.append((offset, length))
            return await real_read(self, offset, length)

        monkeypatch.setattr(DedupFile, "read", recording_read)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await RawObjectProvider.create(repo, _version(), saas_streams) as provider,
        ):
            assert await provider.children(provider.root()) == []
        assert content_reads == []  # no ObjectDB was read out of the saas_obj


class TestObjectDbIdOverride:
    """``object_db_id`` pinned to the fixture's index-unreferenced ``ObjectDB``."""

    async def test_manual_mode_shows_only_that_objectdbs_own_objects(
        self, opened_repo: DedupRepo, manual_object_db_id: str
    ) -> None:
        # The index never names the manual ObjectDB, so the exact-set assertion
        # proves the listing came from it, not from the index.
        async with (
            SaasStreamCache(opened_repo) as saas_streams,
            await RawObjectProvider.create(
                opened_repo, _version(), saas_streams, object_db_id=manual_object_db_id
            ) as manual,
        ):
            children = await manual.children(manual.root())
            assert {n.name for n in children} == {"b_object_1"}
            assert all(n.is_leaf for n in children)

    async def test_manual_mode_unit_reads_back_the_real_content(
        self, opened_repo: DedupRepo, manual_object_db_id: str
    ) -> None:
        async with (
            SaasStreamCache(opened_repo) as saas_streams,
            await RawObjectProvider.create(
                opened_repo, _version(), saas_streams, object_db_id=manual_object_db_id
            ) as manual,
        ):
            [node] = await manual.children(manual.root())
            content = (await manual.unit(node)).content
            assert await content.read(0, content.size or 0) == b'{"meta": "b1"}'

    async def test_mismatched_stream_uuid_raises_not_found(self, opened_repo: DedupRepo) -> None:
        async with SaasStreamCache(opened_repo) as saas_streams:
            with pytest.raises(NotFoundError, match="names stream"):
                await RawObjectProvider.create(opened_repo, _version(), saas_streams, object_db_id="wrong-stream_0_100")

    async def test_malformed_object_db_id_raises_not_found(self, opened_repo: DedupRepo) -> None:
        async with SaasStreamCache(opened_repo) as saas_streams:
            with pytest.raises(NotFoundError, match="malformed"):
                await RawObjectProvider.create(
                    opened_repo, _version(), saas_streams, object_db_id="not-shaped-like-one"
                )

    async def test_mismatched_stream_uuid_closes_its_stream_instead_of_leaking_it(
        self, opened_repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The mismatch check raises before ``_manual_db``/``_indexed_db`` are
        assigned; ``create()`` must still ``close()`` on this path."""
        closed_instances = []
        original_close = RawObjectProvider.close

        async def spy_close(self: RawObjectProvider) -> None:
            closed_instances.append(self)
            await original_close(self)

        monkeypatch.setattr(RawObjectProvider, "close", spy_close)
        async with SaasStreamCache(opened_repo) as saas_streams:
            with pytest.raises(NotFoundError, match="names stream"):
                await RawObjectProvider.create(opened_repo, _version(), saas_streams, object_db_id="wrong-stream_0_100")
        assert len(closed_instances) == 1


class TestUnreadableIndex:
    """An index ObjectDB that fails to open degrades the fallback to one
    diagnostic placeholder instead of failing the whole version."""

    async def test_the_root_lists_one_diagnostic_placeholder(
        self, opened_repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise DataCorruptError("synthetic corruption for this test")

        monkeypatch.setattr(ObjectDb, "load", failing_load)

        async with (
            SaasStreamCache(opened_repo) as saas_streams,
            await RawObjectProvider.create(opened_repo, _version(), saas_streams) as provider,
        ):
            [placeholder] = await provider.children(provider.root())
            assert placeholder.is_diagnostic
            assert placeholder.name == "(object index unreadable)"
            assert placeholder.details == {"cause": "synthetic corruption for this test"}
            assert await provider.children(provider.root(), offset=1) == []
            with pytest.raises(NotFoundError, match="could not be read") as raised:
                await provider.unit(placeholder)
            assert raised.value.ref == str(placeholder.ref)

    async def test_a_failed_acquire_leaves_the_shared_hold_count_untouched(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``close()`` after a failed ``acquire()`` must not release a hold
        this provider never took: the shared count stays at zero."""

        async def failing_load(dedup_file: object, offset: int, length: int) -> None:
            raise DataCorruptError("synthetic corruption for this test")

        monkeypatch.setattr(ObjectDb, "load", failing_load)
        object_name_index = ObjectNameIndex(stream_uuid=_STREAM_UUID, offset=0, length=1, object_ids={})
        dedup_file = cast(DedupFile, object())
        hold = SharedIndexObjectDb(dedup_file, object_name_index)
        shared = SharedSaasContext(dedup_file, object_name_index, hold)

        provider = await RawObjectProvider.create(cast(Any, object()), _version(), cast(Any, object()), shared=shared)
        await provider.close()
        assert hold._holders == 0
