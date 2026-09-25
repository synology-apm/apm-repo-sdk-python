"""Unit tests for ``synology_apm_repo.sdk.storage.layout`` against
synthetic directory trees shaped like real deployments — no sample
repositories required.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.layout import (
    RepoKind,
    detect_layout,
    detect_repository_layout,
    iter_layouts,
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


async def test_single_vault_at_root(tmp_path: Path) -> None:
    _make_vault(tmp_path)
    store = LocalFsStore(tmp_path)

    layout = await detect_layout(store)
    assert layout.kind is RepoKind.VAULT
    assert layout.repo_root == ""
    assert layout.key_root is None

    (only,) = [found async for found in iter_layouts(store)]
    assert only == layout


async def test_vault_found_one_level_below_a_shared_folder_root(tmp_path: Path) -> None:
    # ``tmp_path`` plays the shared folder itself here — a connection made
    # directly to it. A vault always sits exactly one level below the shared
    # folder root, under ``@ActiveProtectVault``.
    _make_vault(tmp_path / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    (layout,) = [found async for found in iter_layouts(store)]
    assert layout.kind is RepoKind.VAULT
    assert layout.repo_root == "@ActiveProtectVault"


async def test_vault_nested_two_levels_down(tmp_path: Path) -> None:
    # ``tmp_path`` plays a volume here, ``myvault`` the shared folder an
    # admin actually picked — two levels below ``tmp_path``, still within
    # ``_DEFAULT_MAX_DEPTH``.
    _make_vault(tmp_path / "myvault" / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    (layout,) = [found async for found in iter_layouts(store)]
    assert layout.kind is RepoKind.VAULT
    assert layout.repo_root == "myvault/@ActiveProtectVault"


async def test_multiple_sibling_vaults(tmp_path: Path) -> None:
    # Two shared folders (``v1``/``v2``), each with its own vault, sitting
    # under one common ancestor (``tmp_path``, e.g. a whole volume, or this
    # project's own multi-sample ``samples/`` directory) — both two levels
    # below ``tmp_path``, both found from that one connection.
    _make_vault(tmp_path / "v1" / "@ActiveProtectVault")
    _make_vault(tmp_path / "v2" / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    layouts = sorted([found async for found in iter_layouts(store)], key=lambda layout: layout.repo_root)
    assert [layout.repo_root for layout in layouts] == [
        "v1/@ActiveProtectVault",
        "v2/@ActiveProtectVault",
    ]
    assert all(layout.kind is RepoKind.VAULT for layout in layouts)


async def test_object_store_bucket_with_single_repo(tmp_path: Path) -> None:
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "abcdefghijkl")
    (tmp_path / "@ActiveProtectKey").mkdir()
    store = LocalFsStore(tmp_path)

    layout = await detect_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_root == "@ActiveProtectData/abcdefghijkl"
    assert layout.key_root == "@ActiveProtectKey"
    assert layout.repo_id == "abcdefghijkl"


async def test_object_store_repo_info_may_be_suffixed(tmp_path: Path) -> None:
    # A real repo can have only a suffixed repo_info.<n>, no bare
    # repo_info — db/ must be the marker instead.
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "abcdefghijkl", repo_info_suffixed=True)
    store = LocalFsStore(tmp_path)

    layout = await detect_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_id == "abcdefghijkl"


async def test_object_store_bucket_with_multiple_repos_is_ambiguous_for_detect_layout(tmp_path: Path) -> None:
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "repoAAAAAAAA")
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "repoBBBBBBBB")
    (tmp_path / "@ActiveProtectKey").mkdir()
    store = LocalFsStore(tmp_path)

    with pytest.raises(NotFoundError):
        await detect_layout(store)

    layouts = sorted([found async for found in iter_layouts(store)], key=lambda layout: layout.repo_id or "")
    assert [layout.repo_id for layout in layouts] == ["repoAAAAAAAA", "repoBBBBBBBB"]
    assert all(layout.key_root == "@ActiveProtectKey" for layout in layouts)


async def test_pointing_directly_at_an_individual_object_store_repo_dir(tmp_path: Path) -> None:
    # The sibling @ActiveProtectKey tree lives one level *above* this
    # store's root and is therefore unreachable — key_root must be None,
    # not raise, and the repository itself must still be detected.
    _make_object_store_repo(tmp_path)
    store = LocalFsStore(tmp_path)

    layout = await detect_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_root == ""
    assert layout.key_root is None
    assert layout.repo_id is None


async def test_narrowed_root_still_resolves_sibling_key_tree(tmp_path: Path) -> None:
    # A caller can narrow a *scan* to one @ActiveProtectData/<repoId>
    # sub-path without narrowing the underlying store itself -- the real
    # @ActiveProtectKey sibling, one level above @ActiveProtectData, is
    # still reachable through the same store and must resolve, unlike
    # test_pointing_directly_at_an_individual_object_store_repo_dir above
    # (where the repository dir has no @ActiveProtectData parent segment at all,
    # so there's genuinely no bucket root to derive one from).
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "abcdefghijkl")
    (tmp_path / "@ActiveProtectKey").mkdir()
    store = LocalFsStore(tmp_path)

    layout = await detect_layout(store, root="@ActiveProtectData/abcdefghijkl")
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_root == "@ActiveProtectData/abcdefghijkl"
    assert layout.key_root == "@ActiveProtectKey"
    assert layout.repo_id == "abcdefghijkl"


async def test_unrelated_directory_tree_yields_nothing(tmp_path: Path) -> None:
    (tmp_path / "not_a_repo").mkdir()
    (tmp_path / "not_a_repo" / "readme.txt").write_bytes(b"hello")
    store = LocalFsStore(tmp_path)

    assert [found async for found in iter_layouts(store)] == []
    with pytest.raises(NotFoundError):
        await detect_layout(store)


async def test_active_protect_data_present_with_no_valid_repo_id_stops_the_walk_there(tmp_path: Path) -> None:
    # A real @ActiveProtectData whose only subdirectory fails the
    # db+@data marker check must make _layouts_at return [] (end the
    # walk along this branch), not None (fall through to iter_layouts'
    # generic recursion) -- distinct from "no @ActiveProtectData at all"
    # (test_unrelated_directory_tree_yields_nothing), which does
    # recurse. Proven by planting a real, otherwise-discoverable vault
    # as a *sibling* of @ActiveProtectData: it must not be found,
    # because the walk never recurses into the root's other children.
    (tmp_path / "@ActiveProtectData" / "not-a-real-repo-id").mkdir(parents=True)
    _make_vault(tmp_path / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    assert [found async for found in iter_layouts(store)] == []
    with pytest.raises(NotFoundError):
        await detect_layout(store)


async def test_max_depth_bounds_the_walk(tmp_path: Path) -> None:
    # A vault buried deeper than max_depth must not be found.
    deep = tmp_path
    for i in range(6):
        deep = deep / f"level{i}"
    _make_vault(deep / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    assert [found async for found in iter_layouts(store, max_depth=2)] == []
    assert len([found async for found in iter_layouts(store, max_depth=10)]) == 1


async def test_iter_layouts_does_not_descend_into_a_found_vault(tmp_path: Path) -> None:
    # Guard against wastefully scanning into Pool/Composition once a vault
    # root is found — discovery only does exists()/listdir().
    vault_root = tmp_path / "@ActiveProtectVault"
    _make_vault(vault_root)
    # Plant something under @data that would itself look like a nested
    # object-store repo, if (incorrectly) descended into.
    _make_object_store_repo(vault_root / "@data" / "decoy")

    store = LocalFsStore(tmp_path)
    layouts = [found async for found in iter_layouts(store)]
    assert len(layouts) == 1
    assert layouts[0].kind is RepoKind.VAULT


# -- RepositoryLayout / iter_repository_layouts / detect_repository_layout --
#
# The Repository-level counterpart to everything above: a bucket holding
# several sibling repo-ids is one Repository (one RepositoryLayout, with
# catalog_ids listing every sibling), not several separately-yielded
# layouts requiring the caller to pick one.


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


async def test_repository_layout_bucket_with_multiple_catalogs_is_no_longer_ambiguous(tmp_path: Path) -> None:
    # Several sibling repo-ids under one bucket all resolve into one
    # RepositoryLayout listing every sibling as catalog_ids -- not an error.
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
    # A real, listable @ActiveProtectData whose only child fails the
    # db+@data marker check is a real bucket with zero catalogs
    # (catalog_ids == []) -- distinct from a VAULT's catalog_ids (None,
    # meaning "not enumerable by listing at all").
    (tmp_path / "@ActiveProtectData" / "not-a-real-repo-id").mkdir(parents=True)
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.catalog_ids == []


async def test_repository_layout_pointing_directly_at_an_unrelated_repo_dir(tmp_path: Path) -> None:
    # No @ActiveProtectData parent segment at all -- genuinely no bucket
    # root to derive, so both key_root and catalog_ids stay unresolved.
    _make_object_store_repo(tmp_path)
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store)
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_root == ""
    assert layout.key_root is None
    assert layout.catalog_ids is None


async def test_repository_layout_narrowed_root_redirects_to_the_whole_bucket(tmp_path: Path) -> None:
    # Pointing a scan straight at one @ActiveProtectData/<repoId> no
    # longer reports just that one catalog -- it redirects to the real
    # bucket root and lists every sibling found there, exactly as if the
    # scan had started at the bucket root directly. Two siblings planted
    # here specifically to prove this isn't just echoing back the one
    # catalog the narrowed root happened to hit.
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "abcdefghijkl")
    _make_object_store_repo(tmp_path / "@ActiveProtectData" / "zzzzzzzzzzzz")
    (tmp_path / "@ActiveProtectKey").mkdir()
    store = LocalFsStore(tmp_path)

    layout = await detect_repository_layout(store, root="@ActiveProtectData/abcdefghijkl")
    assert layout.kind is RepoKind.OBJECT_STORE
    assert layout.repo_root == ""  # redirected to the bucket root, not the narrowed path
    assert layout.key_root == "@ActiveProtectKey"
    assert layout.catalog_ids == ["abcdefghijkl", "zzzzzzzzzzzz"]


async def test_repository_layout_unrelated_directory_tree_yields_nothing(tmp_path: Path) -> None:
    (tmp_path / "not_a_repo").mkdir()
    (tmp_path / "not_a_repo" / "readme.txt").write_bytes(b"hello")
    store = LocalFsStore(tmp_path)

    assert [found async for found in iter_repository_layouts(store)] == []
    with pytest.raises(NotFoundError):
        await detect_repository_layout(store)


async def test_repository_layout_max_depth_bounds_the_walk(tmp_path: Path) -> None:
    deep = tmp_path
    for i in range(6):
        deep = deep / f"level{i}"
    _make_vault(deep / "@ActiveProtectVault")
    store = LocalFsStore(tmp_path)

    assert [found async for found in iter_repository_layouts(store, max_depth=2)] == []
    assert len([found async for found in iter_repository_layouts(store, max_depth=10)]) == 1
