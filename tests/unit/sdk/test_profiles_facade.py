"""Unit tests for ``synology_apm_repo.sdk.profiles``'s public async
facade — the save/list/get/load/delete/build_store contract both the CLI
and TUI consume. ``config_dir`` always points at ``tmp_path``; secrets go
through the in-memory ``fake_keyring`` fixture (``tests/unit/conftest.py``)."""

from __future__ import annotations

from pathlib import Path

import pytest

from synology_apm_repo.sdk.errors import ProfileNotFoundError
from synology_apm_repo.sdk.profiles import (
    BackendKind,
    build_store,
    delete_profile,
    get_profile,
    list_profiles,
    list_profiles_full,
    list_remote_items,
    load_profile,
    save_profile,
    store_from_config,
)
from synology_apm_repo.sdk.profiles.model import AzureProfileConfig, S3ProfileConfig, SmbProfileConfig
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore


async def test_list_profiles_empty_when_none_saved(tmp_path: Path) -> None:
    assert await list_profiles(config_dir=tmp_path) == []


async def test_save_then_list_and_get(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.S3,
        {"bucket": "b", "endpoint": "http://minio:9000", "verify_tls": False, "access_key": "AKIA", "secret_key": "s"},
        config_dir=tmp_path,
    )
    summaries = await list_profiles(config_dir=tmp_path)
    assert [(s.name, s.kind) for s in summaries] == [("demo", BackendKind.S3)]

    profile = await get_profile("demo", config_dir=tmp_path)
    assert isinstance(profile.config, S3ProfileConfig)
    assert profile.config.bucket == "b"
    assert profile.config.endpoint == "http://minio:9000"
    assert profile.config.verify_tls is False


async def test_list_profiles_full_empty_when_none_saved(tmp_path: Path) -> None:
    assert await list_profiles_full(config_dir=tmp_path) == []


async def test_list_profiles_full_returns_the_whole_profile_not_just_the_summary(
    tmp_path: Path, fake_keyring: None
) -> None:
    """The one thing ``list_profiles_full`` exists for: a caller gets every
    saved profile's own backend fields directly, without a further
    ``get_profile()`` call per name re-reading the same file each time
    (``list_profiles()`` itself only ever hands back name+kind)."""
    await save_profile(
        "demo",
        BackendKind.S3,
        {"bucket": "b", "endpoint": "http://minio:9000", "verify_tls": False, "access_key": "AKIA", "secret_key": "s"},
        config_dir=tmp_path,
    )
    profiles = await list_profiles_full(config_dir=tmp_path)
    assert len(profiles) == 1
    profile = profiles[0]
    assert profile.name == "demo"
    assert isinstance(profile.config, S3ProfileConfig)
    assert profile.config.bucket == "b"
    assert profile.config.endpoint == "http://minio:9000"


async def test_list_profiles_full_sorted_by_name_not_read_order(
    tmp_path: Path, fake_keyring: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from synology_apm_repo.sdk.profiles import config_file
    from synology_apm_repo.sdk.profiles.model import BackendKind as _BackendKind
    from synology_apm_repo.sdk.profiles.model import Profile, S3ProfileConfig

    def fake_read_profiles(*, config_dir: Path | None = None) -> dict[str, Profile]:
        return {
            name: Profile(name=name, kind=_BackendKind.S3, config=S3ProfileConfig(bucket="b"))
            for name in ("zebra", "alpha", "mike")
        }

    monkeypatch.setattr(config_file, "read_profiles", fake_read_profiles)
    profiles = await list_profiles_full(config_dir=tmp_path)
    assert [p.name for p in profiles] == ["alpha", "mike", "zebra"]


async def test_list_profiles_sorted_by_name_not_read_order(
    tmp_path: Path, fake_keyring: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # list_profiles() is documented as sorted by name. Going through
    # save_profile()/config_file.read_profiles() for this wouldn't
    # actually prove it -- write_profiles() writes profiles.json with
    # json.dump(..., sort_keys=True), so read_profiles() already comes
    # back alphabetical regardless of save order or of list_profiles()'s
    # own sorted() call. Monkeypatching read_profiles() directly to hand
    # back a deliberately out-of-order dict isolates list_profiles()'s
    # own sort.
    from synology_apm_repo.sdk.profiles import config_file
    from synology_apm_repo.sdk.profiles.model import BackendKind as _BackendKind
    from synology_apm_repo.sdk.profiles.model import Profile, S3ProfileConfig

    def fake_read_profiles(*, config_dir: Path | None = None) -> dict[str, Profile]:
        return {
            name: Profile(name=name, kind=_BackendKind.S3, config=S3ProfileConfig(bucket="b"))
            for name in ("zebra", "alpha", "mike")
        }

    monkeypatch.setattr(config_file, "read_profiles", fake_read_profiles)
    summaries = await list_profiles(config_dir=tmp_path)
    assert [s.name for s in summaries] == ["alpha", "mike", "zebra"]


async def test_get_profile_never_carries_secrets(tmp_path: Path, fake_keyring: None) -> None:
    """``get_profile()``'s returned config carries no secret field at all,
    proving it never reads them back out of the keyring."""
    await save_profile(
        "demo", BackendKind.S3, {"bucket": "b", "access_key": "AKIA", "secret_key": "s"}, config_dir=tmp_path
    )
    profile = await get_profile("demo", config_dir=tmp_path)
    assert not hasattr(profile.config, "access_key")
    assert not hasattr(profile.config, "secret_key")
    assert "access_key" not in vars(profile.config)


async def test_get_profile_missing_raises_profile_not_found(tmp_path: Path) -> None:
    with pytest.raises(ProfileNotFoundError):
        await get_profile("nope", config_dir=tmp_path)


async def test_load_profile_s3_returns_endpoint_and_region_when_set(tmp_path: Path, fake_keyring: None) -> None:
    # Every other S3 load_profile test in this file leaves endpoint/region
    # unset (bucket-only, or blank-secret cases) -- _non_secret_fields()'s
    # single "is not None" filter would otherwise never be exercised for
    # these two fields.
    await save_profile(
        "demo",
        BackendKind.S3,
        {
            "bucket": "b",
            "endpoint": "http://minio:9000",
            "region": "us-east-1",
            "access_key": "AKIA",
            "secret_key": "s",
        },
        config_dir=tmp_path,
    )
    fields = await load_profile("demo", config_dir=tmp_path)
    assert fields == {
        "bucket": "b",
        "endpoint": "http://minio:9000",
        "region": "us-east-1",
        "verify_tls": True,
        "access_key": "AKIA",
        "secret_key": "s",
    }


async def test_load_profile_returns_everything_including_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.AZURE,
        {"container": "c", "account_url": "https://acct.blob.core.windows.net", "credential": "sas-token"},
        config_dir=tmp_path,
    )
    fields = await load_profile("demo", config_dir=tmp_path)
    assert fields == {
        "container": "c",
        "account_url": "https://acct.blob.core.windows.net",
        "verify_tls": True,
        "credential": "sas-token",
    }


async def test_load_profile_smb_returns_server_share_and_password(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.SMB,
        {"server": "nas.example.com", "share": "backups", "username": "admin", "password": "hunter2"},
        config_dir=tmp_path,
    )
    fields = await load_profile("demo", config_dir=tmp_path)
    assert fields == {
        "server": "nas.example.com",
        "share": "backups",
        "port": 445,
        "username": "admin",
        "password": "hunter2",
    }


async def test_load_profile_blank_secret_means_ambient_credential_chain(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile("demo", BackendKind.S3, {"bucket": "b", "access_key": "", "secret_key": ""}, config_dir=tmp_path)
    fields = await load_profile("demo", config_dir=tmp_path)
    assert "access_key" not in fields
    assert "secret_key" not in fields


async def test_save_profile_upserts_silently(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile("demo", BackendKind.S3, {"bucket": "one"}, config_dir=tmp_path)
    await save_profile("demo", BackendKind.S3, {"bucket": "two"}, config_dir=tmp_path)
    profile = await get_profile("demo", config_dir=tmp_path)
    assert isinstance(profile.config, S3ProfileConfig)
    assert profile.config.bucket == "two"


async def test_delete_profile_removes_config_and_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile("demo", BackendKind.S3, {"bucket": "b", "access_key": "AKIA"}, config_dir=tmp_path)
    await delete_profile("demo", config_dir=tmp_path)
    assert await list_profiles(config_dir=tmp_path) == []
    with pytest.raises(ProfileNotFoundError):
        await get_profile("demo", config_dir=tmp_path)


async def test_delete_profile_missing_raises_profile_not_found(tmp_path: Path) -> None:
    with pytest.raises(ProfileNotFoundError):
        await delete_profile("nope", config_dir=tmp_path)


async def test_resaving_after_delete_does_not_inherit_stale_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile("demo", BackendKind.S3, {"bucket": "b", "access_key": "OLD"}, config_dir=tmp_path)
    await delete_profile("demo", config_dir=tmp_path)
    await save_profile("demo", BackendKind.S3, {"bucket": "b"}, config_dir=tmp_path)
    fields = await load_profile("demo", config_dir=tmp_path)
    assert "access_key" not in fields


async def test_build_store_s3_merges_config_and_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.S3,
        {
            "bucket": "my-bucket",
            "endpoint": "http://minio:9000",
            "region": "us-east-1",
            "access_key": "AKIA",
            "secret_key": "shh",
        },
        config_dir=tmp_path,
    )
    store = await build_store("demo", config_dir=tmp_path)
    assert isinstance(store, S3Store)
    assert store._bucket == "my-bucket"  # white-box check of the merged kwargs' effect
    assert store._client_kwargs == {
        "verify": True,
        "endpoint_url": "http://minio:9000",
        "region_name": "us-east-1",
        "aws_access_key_id": "AKIA",
        "aws_secret_access_key": "shh",
    }


async def test_build_store_azure_merges_config_and_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.AZURE,
        {"container": "c", "account_url": "https://acct.blob.core.windows.net", "credential": "sas-token"},
        config_dir=tmp_path,
    )
    store = await build_store("demo", config_dir=tmp_path)
    assert isinstance(store, AzureStore)


async def test_store_from_config_s3_merges_config_and_secrets() -> None:
    """``build_store``'s own construction step, exercised directly
    with a not-yet-saved config — the same shape ``cli/commands/profile.py``'s
    ``add`` and the TUI's connect dialog use before a profile exists."""
    config = S3ProfileConfig(bucket="my-bucket", endpoint="http://minio:9000", region="us-east-1")
    store = await store_from_config(BackendKind.S3, config, {"access_key": "AKIA", "secret_key": "shh"})
    assert isinstance(store, S3Store)
    assert store._bucket == "my-bucket"
    assert store._client_kwargs == {
        "verify": True,
        "endpoint_url": "http://minio:9000",
        "region_name": "us-east-1",
        "aws_access_key_id": "AKIA",
        "aws_secret_access_key": "shh",
    }


async def test_store_from_config_azure_merges_config_and_secrets() -> None:
    config = AzureProfileConfig(container="c", account_url="https://acct.blob.core.windows.net")
    store = await store_from_config(BackendKind.AZURE, config, {"credential": "sas-token"})
    assert isinstance(store, AzureStore)


async def test_build_store_smb_merges_config_and_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.SMB,
        {"server": "nas.example.com", "share": "backups", "username": "admin", "password": "hunter2"},
        config_dir=tmp_path,
    )
    store = await build_store("demo", config_dir=tmp_path)
    assert isinstance(store, SmbStore)
    assert store._share == "backups"  # white-box check of the merged kwargs' effect
    assert store._server == "nas.example.com"
    assert store._username == "admin"
    assert store._password == "hunter2"


async def test_store_from_config_smb_merges_config_and_secrets() -> None:
    config = SmbProfileConfig(server="nas.example.com", share="backups", username="admin")
    store = await store_from_config(BackendKind.SMB, config, {"password": "hunter2"})
    assert isinstance(store, SmbStore)
    assert store._share == "backups"
    assert store._server == "nas.example.com"
    assert store._password == "hunter2"


async def test_list_remote_items_dispatches_by_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    """``list_remote_items`` is a thin dispatch to ``storage.s3.list_buckets``/
    ``storage.azure.list_containers`` — faked here rather than exercised
    against a real endpoint, same as those two functions' own tests."""
    from synology_apm_repo.sdk import storage

    async def fake_list_buckets(**kwargs: object) -> list[str]:
        return ["bucket-a"]

    async def fake_list_containers(**kwargs: object) -> list[str]:
        return ["container-a"]

    monkeypatch.setattr(storage, "list_buckets", fake_list_buckets)
    monkeypatch.setattr(storage, "list_containers", fake_list_containers)

    assert await list_remote_items(BackendKind.S3) == ["bucket-a"]
    assert await list_remote_items(BackendKind.AZURE) == ["container-a"]


async def test_list_remote_items_raises_for_smb_rather_than_silently_dispatching_elsewhere() -> None:
    """SMB has no account-level "list every share" operation — this must
    fail loudly, not silently fall through to Azure's own
    ``list_containers()`` with SMB's unrelated kwargs."""
    with pytest.raises(ValueError, match="smb"):
        await list_remote_items(BackendKind.SMB)


def test_non_secret_config_dataclasses_never_gain_a_secret_field() -> None:
    """Regression guard: no config dataclass should ever grow a field also
    named in its backend's secret-field set — that would silently
    reintroduce a secret into ``profiles.json``."""
    from synology_apm_repo.sdk.profiles.model import AZURE_SECRET_FIELDS, S3_SECRET_FIELDS, SMB_SECRET_FIELDS

    s3_fields = {f.name for f in __import__("dataclasses").fields(S3ProfileConfig)}
    azure_fields = {f.name for f in __import__("dataclasses").fields(AzureProfileConfig)}
    smb_fields = {f.name for f in __import__("dataclasses").fields(SmbProfileConfig)}
    assert s3_fields.isdisjoint(S3_SECRET_FIELDS)
    assert azure_fields.isdisjoint(AZURE_SECRET_FIELDS)
    assert smb_fields.isdisjoint(SMB_SECRET_FIELDS)
