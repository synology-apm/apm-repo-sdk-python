"""Unit tests for ``synology_apm_repo.sdk.storage.layout`` against
synthetic directory trees shaped like real deployments.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.layout import (
    RepoKind,
    RepositoryLayout,
    catalog_repo_layouts,
    detect_repository_layout,
    iter_repository_layouts,
)
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _make_vault(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "repo_info").write_bytes(b"RpiF...")
    (root / "link.key").write_bytes(b"lINk...")
    (root / ".fully_created").write_bytes(b"")
    (root / "db").mkdir()
    (root / "@data").mkdir()


def _make_object_store_repo(root: Path, *, repo_info_suffixed: bool = False) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "db").mkdir()
    (root / "@data").mkdir()
    name = "repo_info.321" if repo_info_suffixed else "repo_info"
    (root / name).write_bytes(b"RpiF...")


async def test_repository_layout_single_vault_at_root(tmp_path: Path) -> None:
    _make_vault(tmp_path)
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store)
    assert layout.kind is RepoKind.VAULT
    assert layout.repo_root == ""
    assert layout.key_root is None
    assert layout.catalog_ids is None  # vault catalogs come from a later db query, not directory listing

    (only,) = [found async for found in iter_repository_layouts(store)]
    assert only == layout


async def test_repository_layout_multiple_sibling_vaults(tmp_path: Path) -> None:
    _make_vault(tmp_path / "v1" / "@ActiveProtectVault")
    _make_vault(tmp_path / "v2" / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    layouts = sorted([found async for found in iter_repository_layouts(store)], key=lambda layout: layout.repo_root)
    assert [layout.repo_root for layout in layouts] == ["v1/@ActiveProtectVault", "v2/@ActiveProtectVault"]
    assert all(layout.kind is RepoKind.VAULT for layout in layouts)


async def test_repository_layout_bucket_with_single_catalog(tmp_path: Path) -> None:
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "abcdefghijkl")
    (tmp_path / "@ActiveProtectKey").mkdir()
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_root == ""
    assert layout.key_root == "@ActiveProtectKey"
    assert layout.catalog_ids == ["abcdefghijkl"]


async def test_repository_layout_bucket_with_multiple_catalogs_lists_them_all(tmp_path: Path) -> None:
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "repoAAAAAAAA")
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "repoBBBBBBBB")
    (tmp_path / "@ActiveProtectKey").mkdir()
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.key_root == "@ActiveProtectKey"
    assert layout.catalog_ids == ["repoAAAAAAAA", "repoBBBBBBBB"]

    (only,) = [found async for found in iter_repository_layouts(store)]
    assert only == layout


async def test_repository_layout_bucket_with_no_valid_catalogs_yields_empty_list_not_none(tmp_path: Path) -> None:
    # A bucket with zero catalogs ([]), unlike a VAULT's None ("not
    # enumerable by listing").
    (tmp_path / "@ActiveProtectData" / "not-a-real-repo-id").mkdir(parents=True)
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.catalog_ids == []


async def test_repository_layout_pointing_directly_at_an_unrelated_repo_dir(tmp_path: Path) -> None:
    # No @ActiveProtectData parent: no bucket root to derive.
    _make_object_store_repo(tmp_path)
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_root == ""
    assert layout.key_root is None
    assert layout.catalog_ids is None


async def test_repository_layout_narrowed_root_redirects_to_the_whole_bucket(tmp_path: Path) -> None:
    # A root of @ActiveProtectData/<repoId> redirects to the bucket root
    # and lists every sibling.
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "abcdefghijkl")
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "zzzzzzzzzzzz")
    (tmp_path / "@ActiveProtectKey").mkdir()
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store, root="@ActiveProtectData/abcdefghijkl")
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_root == ""
    assert layout.key_root == "@ActiveProtectKey"
    assert layout.catalog_ids == ["abcdefghijkl", "zzzzzzzzzzzz"]


async def test_repository_layout_unrelated_directory_tree_yields_nothing(tmp_path: Path) -> None:
    (tmp_path / "not_a_repo").mkdir()
    (tmp_path / "not_a_repo" / "readme.txt").write_bytes(b"hello")
    store = LocalFsStore(tmp_path)

    assert [found async for found in iter_repository_layouts(store)] == []
    with pytest.raises(NotFoundError, match="no repository layout detected"):
        await detect_repository_layout(store)


async def test_repository_layout_max_depth_bounds_the_walk(tmp_path: Path) -> None:
    deep = tmp_path
    for i in range(6):
        deep = deep / f"level{i}"
    _make_vault(deep / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    assert [found async for found in iter_repository_layouts(store, max_depth=2)] == []
    assert len([found async for found in iter_repository_layouts(store, max_depth=10)]) == 1


async def test_vault_found_one_level_below_a_shared_folder_root(tmp_path: Path) -> None:
    # ``tmp_path`` plays the shared folder itself here — a connection made
    # directly to it. A vault always sits exactly one level below the shared
    # folder root, under ``@ActiveProtectVault``.
    _make_vault(tmp_path / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    (layout,) = [found async for found in iter_repository_layouts(store)]
    assert layout.kind is RepoKind.VAULT
    assert layout.repo_root == "@ActiveProtectVault"


async def test_vault_nested_two_levels_down(tmp_path: Path) -> None:
    # ``tmp_path`` plays a volume here, ``myvault`` the shared folder an
    # admin actually picked — two levels below ``tmp_path``, still within
    # ``_DEFAULT_MAX_DEPTH``.
    _make_vault(tmp_path / "myvault" / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    (layout,) = [found async for found in iter_repository_layouts(store)]
    assert layout.kind is RepoKind.VAULT
    assert layout.repo_root == "myvault/@ActiveProtectVault"


async def test_object_store_repo_info_may_be_suffixed(tmp_path: Path) -> None:
    # A real repo can have only a suffixed repo_info.<n>, no bare
    # repo_info — db/ and @data are the markers instead.
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "abcdefghijkl", repo_info_suffixed=True)
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.catalog_ids == ["abcdefghijkl"]


async def test_active_protect_data_present_with_no_valid_repo_id_stops_the_walk_there(tmp_path: Path) -> None:
    # @ActiveProtectData with no valid repo-id is still a bucket root, so
    # the sibling vault is never reached.
    (tmp_path / "@ActiveProtectData" / "not-a-real-repo-id").mkdir(parents=True)
    _make_vault(tmp_path / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    (layout,) = [found async for found in iter_repository_layouts(store)]
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.catalog_ids == []


async def test_walk_does_not_descend_into_a_found_vault(tmp_path: Path) -> None:
    # The walk stops at a vault root, so an object-store-shaped decoy under
    # its @data is never reported.
    vault_root = tmp_path / "@ActiveProtectVault"
    _make_vault(vault_root)
    _make_object_store_repo(vault_root / "@data" / "decoy")

    store = LocalFsStore(tmp_path)
    (layout,) = [found async for found in iter_repository_layouts(store)]
    assert layout.kind is RepoKind.VAULT


# -- catalog_repo_layouts ------------------------------------------------


def test_catalog_repo_layouts_of_a_vault_is_the_vault_root() -> None:
    (layout,) = catalog_repo_layouts(RepositoryLayout(kind=RepoKind.VAULT, repo_root="v/@ActiveProtectVault"))
    assert layout.kind is RepoKind.VAULT
    assert layout.repo_root == "v/@ActiveProtectVault"
    assert layout.repo_id is None


def test_catalog_repo_layouts_of_a_bucket_is_one_per_catalog_sharing_the_key_root() -> None:
    bucket = RepositoryLayout(
        kind=RepoKind.OBJECT_STORE,
        repo_root="b",
        key_root="b/@ActiveProtectKey",
        catalog_ids=["repoAAAAAAAA", "repoBBBBBBBB"],
    )
    layouts = catalog_repo_layouts(bucket)
    assert [(layout.repo_root, layout.repo_id) for layout in layouts] == [
        ("b/@ActiveProtectData/repoAAAAAAAA", "repoAAAAAAAA"),
        ("b/@ActiveProtectData/repoBBBBBBBB", "repoBBBBBBBB"),
    ]
    assert all(layout.key_root == "b/@ActiveProtectKey" for layout in layouts)


def test_catalog_repo_layouts_of_a_bare_catalog_dir_is_that_dir() -> None:
    (layout,) = catalog_repo_layouts(RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=""))
    assert layout.repo_root == ""
    assert layout.repo_id is None


def test_catalog_repo_layouts_of_a_bucket_with_no_catalogs_is_empty() -> None:
    assert catalog_repo_layouts(RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="", catalog_ids=[])) == []
