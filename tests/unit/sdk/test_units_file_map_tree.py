"""Unit tests for ``synology_apm_repo.sdk.units.file_map_tree`` —
synthetic repository roots written to real files
(``tests/integration/sdk/test_units_file_map_tree.py`` is the real-data
counterpart)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from support.repo_builders import (
    write_bucket,
    write_composition,
    write_file_map,
    write_repo_info,
    write_vault_encryption_key_db,
)
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import NotRestorableError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.file_map_tree import FileMapTreeProvider, _Prefix

_STREAM_ID = 4
_PLAINTEXT = (b"leaf-content----" * 256)[:4096]
assert len(_PLAINTEXT) == 4096


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[DedupRepo]:
    write_repo_info(tmp_path / "repo_info")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    write_file_map(
        tmp_path / "db" / "file_map",
        [
            ("WORKLOAD-a/2026-01-01/fileA.txt", _STREAM_ID, 9, 64, 1, 2),
            ("WORKLOAD-a/2026-01-01/sub/fileB.txt", _STREAM_ID, 9, 64, 1, 2),
            ("WORKLOAD-b/2026-01-01/fileC.txt", _STREAM_ID, 9, 64, 1, 2),
        ],
    )
    write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
    write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", [_PLAINTEXT])
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as r:
        yield r


class TestTree:
    async def test_root_lists_top_level_workload_dirs(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        top = await provider.children(provider.root())
        assert {n.name for n in top} == {"WORKLOAD-a", "WORKLOAD-b"}
        assert all(not n.is_leaf for n in top)

    async def test_drills_down_through_intermediate_directories(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        date_dir = (await provider.children(wl_a))[0]
        assert date_dir.name == "2026-01-01"
        entries = await provider.children(date_dir)
        assert {n.name for n in entries} == {"fileA.txt", "sub"}
        file_a = next(n for n in entries if n.name == "fileA.txt")
        sub_dir = next(n for n in entries if n.name == "sub")
        assert file_a.is_leaf is True
        assert file_a.kind is UnitKind.RAW_OBJECT
        assert sub_dir.is_leaf is False

    async def test_children_of_a_leaf_is_empty(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        date_dir = (await provider.children(wl_a))[0]
        file_a = next(n for n in await provider.children(date_dir) if n.name == "fileA.txt")
        assert await provider.children(file_a) == []

    async def test_unrecognized_node_children_is_empty(self, repo: DedupRepo) -> None:
        from synology_apm_repo.sdk.units.base import Node
        from synology_apm_repo.sdk.units.node_ref import NodeRef

        provider = FileMapTreeProvider(repo)
        mystery = Node(ref=NodeRef("repo", ("x",)), name="x", is_leaf=False)
        assert await provider.children(mystery) == []

    async def test_node_with_a_prefix_not_present_in_the_index_is_empty(self, repo: DedupRepo) -> None:
        """A directory node whose prefix no ``file_map`` path produces lists
        nothing — distinct from ``test_unrecognized_node_children_is_empty``,
        whose node has no ``_Prefix`` handle at all."""
        from synology_apm_repo.sdk.units.base import Node
        from synology_apm_repo.sdk.units.node_ref import NodeRef

        provider = FileMapTreeProvider(repo)
        bogus = Node(
            ref=NodeRef.raw("", "no/such/prefix"),
            name="no-such-prefix",
            is_leaf=False,
            handle=_Prefix("no/such/prefix"),
        )
        assert await provider.children(bogus) == []

    async def test_paths_are_scanned_once_and_cached(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        await provider.children(provider.root())
        first = provider._paths
        await provider.children(provider.root())
        assert provider._paths is first

    async def test_child_index_is_built_once_and_cached(self, repo: DedupRepo) -> None:
        """The prefix index (not just the raw path list) is the same object
        on a second, deeper ``children()`` call."""
        provider = FileMapTreeProvider(repo)
        await provider.children(provider.root())
        first = provider._child_index
        assert first is not None
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        await provider.children(wl_a)
        assert provider._child_index is first

    async def test_a_path_that_is_both_a_leaf_and_a_prefix_of_a_longer_path_yields_both_nodes(
        self, tmp_path: Path
    ) -> None:
        """One row's path is an exact prefix of another row's (e.g. an
        empty-directory object alongside a file nested under it):
        ``children()`` surfaces *both* a directory and a leaf named
        "dirlike"."""
        write_repo_info(tmp_path / "repo_info")
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        write_file_map(
            tmp_path / "db" / "file_map",
            [
                ("WORKLOAD-c/dirlike", _STREAM_ID, 9, 64, 1, 2),
                ("WORKLOAD-c/dirlike/nested.txt", _STREAM_ID, 9, 64, 1, 2),
            ],
        )
        write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
        write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", [_PLAINTEXT])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            provider = FileMapTreeProvider(repo)
            wl_c = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-c")
            entries = await provider.children(wl_c)
            dirlike_entries = [n for n in entries if n.name == "dirlike"]
            assert len(dirlike_entries) == 2
            assert {n.is_leaf for n in dirlike_entries} == {True, False}

            dirlike_dir = next(n for n in dirlike_entries if n.is_leaf is False)
            nested = await provider.children(dirlike_dir)
            assert {n.name for n in nested} == {"nested.txt"}


class TestContent:
    async def test_reads_the_real_dedup_content(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        date_dir = (await provider.children(wl_a))[0]
        file_a = next(n for n in await provider.children(date_dir) if n.name == "fileA.txt")
        content = (await provider.unit(file_a)).content
        assert await content.read(0, 4096) == _PLAINTEXT

    async def test_unit_on_a_directory_node_raises(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(wl_a)
