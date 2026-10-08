"""Regression tests for ``api``'s end-to-end wiring.

- ``api_vault_plain_walk.json.gz`` -- recorded against ``vault-plain``, opened from
  above its vault; recipe:
  ``test_replayed_session_discover_from_parent_directory_lists_real_devices``
  (a superset of the other tests using it).
- ``api_is_encrypted_all_samples.json.gz`` -- recorded against the directory
  holding every local sample (``all-local``); its one test.
- ``api_vault_encrypted_key_status.json.gz`` -- recorded against
  ``vault-encrypted``; recipe:
  ``test_replayed_repository_key_status_and_set_key_wrong_then_right`` (a
  superset of the other test using it).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING
from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.device import DeviceProvider

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"


_WINDOWS_VM_WORKLOAD_ID = 2


async def test_replayed_session_discover_from_parent_directory_lists_real_devices(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        # allow_content=True: only the MBR/GPT signature bytes are read.
        store = await record_target("api_vault_plain_walk.json.gz", allow_content=True)
        [repo] = await session.open(store)
        assert repo.layout.repo_root == "@ActiveProtectVault"
        assert repo.key_status is api.KeyStatus.NOT_ENCRYPTED

        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        catalog, vm = next((c, w) for c, w in workload_pairs if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
        version = next(v for v in await catalog.versions(vm) if v.meta is not None)

        provider = await catalog.provider(version)
        assert isinstance(provider, DeviceProvider)
        devices = await provider.children(provider.root())
        assert len(devices) == 1

        objects = await provider.children(devices[0])
        disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
        content = (await provider.unit(disk)).content
        header = await content.read(0, 520)
        assert header[510:512] == _MBR_BOOT_SIG
        assert header[512:520] == _GPT_SIG


async def test_replayed_repository_catalogs_workloads_versions_match_vault_plain(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        store = await record_target("api_vault_plain_walk.json.gz")
        [repo] = await session.open(store)

        catalogs = await repo.catalogs()
        assert len(catalogs) == 2

        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        assert len(workload_pairs) == 25

        all_versions = [v for c, w in workload_pairs for v in await c.versions(w, include_deleted=True)]
        assert len(all_versions) == 109


async def test_replayed_repository_raw_file_and_file_map_tree_fallback_axes(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        store = await record_target("api_vault_plain_walk.json.gz")
        [repo] = await session.open(store)

        tree = await repo.file_map_tree()
        root = tree.root()
        children = await tree.children(root)
        assert {c.name for c in children} == {
            "1fd2d9bd-faab-4b26-a610-109ccb5a093e",
            "33d68fc3-b01f-4b07-a280-d34986d9d100",
            "DRMdjvEJPzoxQiUC",
            "KxMWSUvtSZiaDTDy",
            "LNJAQtstRVxciJWy",
            "VM-c39e8f8a-b861-40ee-a8a1-bdf06fdedcd7",
            "VM-ebd17568-24b5-4816-9e42-a9deb290ad74",
            "XfGkaDjWyGhXVoRC",
            "tfUJpbJdYextKnPE",
            "uvWRSFkGxCcZAMwt",
            "vTQbePdWJrIrRncl",
        }

        leaf = next(c for c in children if c.name == "1fd2d9bd-faab-4b26-a610-109ccb5a093e")
        path = [leaf.name]
        while not leaf.is_leaf:
            leaf = (await tree.children(leaf))[0]
            path.append(leaf.name)
        assert path == ["1fd2d9bd-faab-4b26-a610-109ccb5a093e", "50", "dedup.img"]
        unit = await tree.unit(leaf)
        assert unit.content.size == 893317120


async def test_replayed_repository_is_encrypted_matches_every_real_sample(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """``is_encrypted`` resolves from ``probe_encrypted()`` alone, with no key
    given, for every local sample."""
    store = await record_target("api_is_encrypted_all_samples.json.gz")
    async with api.Session() as session:
        assert (await session.open(store, root="vault-plain"))[0].is_encrypted is False
        assert (await session.open(store, root="vault-encrypted"))[0].is_encrypted is True
        assert (await session.open(store, root="vault-m365"))[0].is_encrypted is False
        assert (await session.open(store, root="vault-vm-m365-encrypted"))[0].is_encrypted is True
        for repo in await session.open(store, root="objstore-m365-encrypted"):  # 2 repo ids sharing one bucket
            assert repo.is_encrypted is True
        for repo in await session.open(store, root="objstore-encrypted"):
            assert repo.is_encrypted is True
        for repo in await session.open(store, root="objstore-m365-single-encrypted"):
            assert repo.is_encrypted is True


async def test_replayed_session_close_closes_all_repos(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("api_vault_plain_walk.json.gz")
    session = api.Session()
    repos = await session.open(store)
    assert len(repos) == 1
    await session.close()
    assert not session._repos


async def test_replayed_repository_key_status_and_set_key_wrong_then_right(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """No key reports NO_KEY_PROVIDED, a wrong key INVALID, and the real key
    VERIFIED and able to decrypt content."""
    async with api.Session() as session:
        # allow_content=True: the final disk read is only an MBR/GPT check.
        store = await record_target("api_vault_encrypted_key_status.json.gz", allow_content=True)
        [repo] = await session.open(store)
        status_before_any_key = repo.key_status
        assert status_before_any_key is api.KeyStatus.NO_KEY_PROVIDED

        wrong_key = "wrongkeyid00@AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        result = await repo.set_key(wrong_key)
        assert result.verification.ok is False
        status_after_wrong_key = repo.key_status
        assert status_after_wrong_key is api.KeyStatus.INVALID

        result = await repo.set_key(VAULT_ENCRYPTED_KEY_STRING)
        assert result.verification.ok is True
        assert result.reopen_errors == ()
        status_after_real_key = repo.key_status
        assert status_after_real_key is api.KeyStatus.VERIFIED

        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        catalog, vm = next((c, w) for c, w in workload_pairs if w.workload_type == "VM")
        version = next(v for v in await catalog.versions(vm) if v.meta is not None)

        provider = await catalog.provider(version)
        devices = await provider.children(provider.root())
        objects = await provider.children(devices[0])
        disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
        content = (await provider.unit(disk)).content
        header = await content.read(0, 520)
        assert header[510:512] == _MBR_BOOT_SIG
        assert header[512:520] == _GPT_SIG


async def test_replayed_repository_is_encrypted_agrees_with_key_status(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """``is_encrypted`` and ``key_status`` never disagree, for an encrypted
    repository with no key (``NO_KEY_PROVIDED``) and with the real key
    (``VERIFIED``)."""
    async with api.Session() as session:
        store = await record_target("api_vault_encrypted_key_status.json.gz")
        [no_key] = await session.open(store)
        assert no_key.key_status is api.KeyStatus.NO_KEY_PROVIDED
        assert no_key.is_encrypted is True

        [with_key] = await session.open(store, key=VAULT_ENCRYPTED_KEY_STRING)
        assert with_key.key_status is api.KeyStatus.VERIFIED
        assert with_key.is_encrypted is True
