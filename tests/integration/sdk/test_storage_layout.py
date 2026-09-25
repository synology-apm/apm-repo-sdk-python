"""Regression test for ``storage.layout`` — replayed from a committed
fixture recorded against every real sample, with **no external
dependency**: this always runs, on CI or anywhere else, because it goes
through ``ReplayStore`` instead of a real ``LocalFsStore``.

The fixture (``tests/fixtures/storage_layout_all_samples.json.gz``)
was produced once by ``RecordingStore`` wrapping a real store rooted at
the whole ``samples/`` tree, recording every ``exists()``/``listdir()``
call ``iter_layouts`` makes while walking it — ``iter_layouts``/
``detect_layout`` never call ``read()``/``size()`` at all, only
``exists()``/``listdir()`` at each level, never a scan into
``Pool``/``Composition``/``db``, unlike most
other fixtures in this directory, which do read real content.
``az-test-1`` (an untracked extra sample directory) was absent from the
tree at recording time, same as it is in this repository's own CI/dev-machine
baseline.

``test_replayed_each_vault_root_detected_standalone`` doesn't rebuild a
separate ``LocalFsStore`` rooted at each vault (a fresh store there
would just be a different path *prefix* over the same real files) — it
calls ``detect_layout`` with ``root=`` set to each vault's path relative
to the one recorded, tree-rooted store instead. That produces the exact
same ``exists()`` keys the recorded walk already visited internally, so
one recording covers all three tests below with no separate per-vault
fixture needed.

``test_replayed_each_vault_root_detected_standalone`` makes a strict
superset of what the other two tests need (the whole-tree walk, plus a
``detect_layout(root=...)`` per vault) — it's this fixture's recording
recipe: ``make record-fixture TARGET=local:<path-to-samples-root>
TEST=<its node id>`` ((re-)records against the whole samples tree).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.storage import RepoKind, detect_layout, iter_layouts
from synology_apm_repo.sdk.storage.base import ObjectStore

_EXPECTED_VAULTS = {
    "apv-sample-1/@ActiveProtectVault",
    "apv-sample-2-encrypted/@ActiveProtectVault",
    "apv-sample-3/@ActiveProtectVault",
    "sample-2/@ActiveProtectVault",
}

_EXPECTED_OBJECT_STORE_REPOS = {
    ("s3-sample-2-encrypted", "BikXpRbFNGI1"),
    ("s3-sample-2-encrypted", "uoRtcQebTU5w"),
    ("sample-1", "5fkUi8kPsAlP"),
    ("sample-1", "gqDuTMuityBf"),
    ("ps-sample-1", "jkwXlvh40ECN"),
    ("ps-sample-2", "Rem7gaXV4jLT"),
    ("az-test-2-encrypted", "nJBO5b2jFgnY"),
}


async def test_replayed_all_samples_detected(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("storage_layout_all_samples.json.gz")
    layouts = [layout async for layout in iter_layouts(store)]

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
    async for layout in iter_layouts(store):
        if layout.kind is RepoKind.OBJECT_STORE:
            assert layout.key_root is not None
            assert layout.key_root.endswith("@ActiveProtectKey")


async def test_replayed_each_vault_root_detected_standalone(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    # ``root=vault_rel`` against the one tree-rooted store produces the
    # exact same ``exists()`` keys the recorded whole-tree walk already
    # visited internally, replacing what would otherwise be a separate
    # per-vault ``LocalFsStore`` (just a different path prefix over the
    # same real files) with no extra fixture needed.
    store = await record_target("storage_layout_all_samples.json.gz")
    for vault_rel in _EXPECTED_VAULTS:
        layout = await detect_layout(store, root=vault_rel)
        assert layout.kind is RepoKind.VAULT
        assert layout.repo_root == vault_rel


__all__: list[str] = []
