"""Regression tests for ``storage.layout`` against every real sample.
Layout detection only calls ``exists()``/``listdir()``.

Fixture: ``storage_layout_all_samples.json.gz``, recorded against the
directory holding every sample repository.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.storage import (
    RepoKind,
    RepoLayout,
    catalog_repo_layouts,
    detect_repository_layout,
    iter_repository_layouts,
)
from synology_apm_repo.sdk.storage.base import ObjectStore

_EXPECTED_VAULTS = {
    "vault-plain/@ActiveProtectVault",
    "vault-encrypted/@ActiveProtectVault",
    "vault-m365/@ActiveProtectVault",
    "vault-vm-m365-encrypted/@ActiveProtectVault",
}

_EXPECTED_OBJECT_STORE_REPOS = {
    ("objstore-encrypted", "BikXpRbFNGI1"),
    ("objstore-encrypted", "uoRtcQebTU5w"),
    ("objstore-m365-encrypted", "5fkUi8kPsAlP"),
    ("objstore-m365-encrypted", "gqDuTMuityBf"),
    ("objstore-m365-single-encrypted", "nJBO5b2jFgnY"),
}


async def _catalog_layouts(store: ObjectStore) -> list[RepoLayout]:
    return [layout async for repo in iter_repository_layouts(store) for layout in catalog_repo_layouts(repo)]


async def test_replayed_all_samples_detected(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("storage_layout_all_samples.json.gz")
    layouts = await _catalog_layouts(store)

    vaults = {layout.repo_root for layout in layouts if layout.kind is RepoKind.VAULT}
    object_store = {
        (layout.repo_root.split("/@ActiveProtectData/")[0], layout.repo_id)
        for layout in layouts
        if layout.kind is RepoKind.OBJECT_STORE
    }

    assert vaults == _EXPECTED_VAULTS
    assert object_store == _EXPECTED_OBJECT_STORE_REPOS
    assert len(layouts) == len(_EXPECTED_VAULTS) + len(_EXPECTED_OBJECT_STORE_REPOS)


async def test_replayed_object_store_repos_have_key_root(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_layout_all_samples.json.gz")
    for layout in await _catalog_layouts(store):
        if layout.kind is RepoKind.OBJECT_STORE:
            assert layout.key_root is not None
            assert layout.key_root.endswith("@ActiveProtectKey")


async def test_replayed_each_vault_root_detected_standalone(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    # ``root=vault_rel`` checks a subset of the keys the whole-tree walk
    # already checks.
    store = await record_target("storage_layout_all_samples.json.gz")
    for vault_rel in _EXPECTED_VAULTS:
        layout = await detect_repository_layout(store, root=vault_rel)
        assert layout.kind is RepoKind.VAULT
        assert layout.repo_root == vault_rel
