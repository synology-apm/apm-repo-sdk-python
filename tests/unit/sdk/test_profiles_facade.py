"""Unit tests for ``synology_apm_repo.sdk.profiles``'s public async
facade the CLI and TUI consume: save/list/get/delete, ``profile_fields_with_secrets``,
``store_from_profile``/``store_from_config`` and ``list_remote_items``. ``config_dir`` always points at ``tmp_path``; secrets go
through the in-memory ``fake_keyring`` fixture (``tests/unit/conftest.py``)."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import keyring
import pytest

from synology_apm_repo.sdk.profiles import (
    BackendKind,
    delete_profile,
    get_profile,
    list_profiles,
    list_remote_items,
    profile_fields_with_secrets,
    save_profile,
    store_from_config,
    store_from_profile,
)
from synology_apm_repo.sdk.profiles.errors import ProfileFieldError, ProfileNotFoundError
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


async def test_list_profiles_returns_each_whole_profile(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.S3,
        {"bucket": "b", "endpoint": "http://minio:9000", "verify_tls": False, "access_key": "AKIA", "secret_key": "s"},
        config_dir=tmp_path,
    )
    profiles = await list_profiles(config_dir=tmp_path)
    assert len(profiles) == 1
    profile = profiles[0]
    assert profile.name == "demo"
    assert isinstance(profile.config, S3ProfileConfig)
    assert profile.config.bucket == "b"
    assert profile.config.endpoint == "http://minio:9000"


async def test_list_profiles_sorted_by_name_not_read_order(
    tmp_path: Path, fake_keyring: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # write_profiles() already sorts keys on disk, so an out-of-order
    # read_profiles() is the only way to observe list_profiles()'s own sort.
    from synology_apm_repo.sdk.profiles import config_file
    from synology_apm_repo.sdk.profiles.model import Profile, S3ProfileConfig

    def fake_read_profiles(*, config_dir: Path | None = None) -> dict[str, Profile]:
        return {name: Profile(name=name, config=S3ProfileConfig(bucket="b")) for name in ("zebra", "alpha", "mike")}

    monkeypatch.setattr(config_file, "read_profiles", fake_read_profiles)
    summaries = await list_profiles(config_dir=tmp_path)
    assert [s.name for s in summaries] == ["alpha", "mike", "zebra"]


async def test_get_profile_never_carries_secrets(tmp_path: Path, fake_keyring: None) -> None:
    """The ``Profile`` ``get_profile()`` returns has no secret field, even
    for a profile saved with secrets."""
    await save_profile(
        "demo", BackendKind.S3, {"bucket": "b", "access_key": "AKIA", "secret_key": "s"}, config_dir=tmp_path
    )
    profile = await get_profile("demo", config_dir=tmp_path)
    assert not hasattr(profile.config, "access_key")
    assert not hasattr(profile.config, "secret_key")
    assert "access_key" not in {f.name for f in dataclasses.fields(profile.config)}


async def test_get_profile_missing_raises_profile_not_found(tmp_path: Path) -> None:
    with pytest.raises(ProfileNotFoundError, match="no such profile"):
        await get_profile("nope", config_dir=tmp_path)


async def test_profile_fields_with_secrets_s3_returns_endpoint_and_region_when_set(
    tmp_path: Path, fake_keyring: None
) -> None:
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
    fields = await profile_fields_with_secrets("demo", config_dir=tmp_path)
    assert fields == {
        "bucket": "b",
        "endpoint": "http://minio:9000",
        "region": "us-east-1",
        "verify_tls": True,
        "access_key": "AKIA",
        "secret_key": "s",
    }


async def test_profile_fields_with_secrets_returns_everything_including_secrets(
    tmp_path: Path, fake_keyring: None
) -> None:
    await save_profile(
        "demo",
        BackendKind.AZURE,
        {"container": "c", "account_url": "https://acct.blob.core.windows.net", "credential": "sas-token"},
        config_dir=tmp_path,
    )
    fields = await profile_fields_with_secrets("demo", config_dir=tmp_path)
    assert fields == {
        "container": "c",
        "account_url": "https://acct.blob.core.windows.net",
        "verify_tls": True,
        "credential": "sas-token",
    }


async def test_profile_fields_with_secrets_smb_returns_server_share_and_password(
    tmp_path: Path, fake_keyring: None
) -> None:
    await save_profile(
        "demo",
        BackendKind.SMB,
        {"server": "nas.example.com", "share": "backups", "username": "admin", "password": "hunter2"},
        config_dir=tmp_path,
    )
    fields = await profile_fields_with_secrets("demo", config_dir=tmp_path)
    assert fields == {
        "server": "nas.example.com",
        "share": "backups",
        "port": 445,
        "username": "admin",
        "password": "hunter2",
    }


async def test_profile_fields_with_secrets_blank_secret_means_ambient_credential_chain(
    tmp_path: Path, fake_keyring: None
) -> None:
    await save_profile("demo", BackendKind.S3, {"bucket": "b", "access_key": "", "secret_key": ""}, config_dir=tmp_path)
    fields = await profile_fields_with_secrets("demo", config_dir=tmp_path)
    assert "access_key" not in fields
    assert "secret_key" not in fields


async def test_save_profile_upserts_silently(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile("demo", BackendKind.S3, {"bucket": "one"}, config_dir=tmp_path)
    await save_profile("demo", BackendKind.S3, {"bucket": "two"}, config_dir=tmp_path)
    profile = await get_profile("demo", config_dir=tmp_path)
    assert isinstance(profile.config, S3ProfileConfig)
    assert profile.config.bucket == "two"


async def test_a_failed_save_leaves_the_existing_profile_and_its_secrets_untouched(
    tmp_path: Path, fake_keyring: None
) -> None:
    """A save whose fields fail validation must not have already replaced the
    existing profile's keyring secrets."""
    await save_profile("demo", BackendKind.S3, {"bucket": "b", "access_key": "OLD"}, config_dir=tmp_path)
    with pytest.raises(ProfileFieldError, match="bucket is required for a s3 profile"):
        await save_profile("demo", BackendKind.S3, {"access_key": "NEW"}, config_dir=tmp_path)  # no bucket
    assert (await profile_fields_with_secrets("demo", config_dir=tmp_path))["access_key"] == "OLD"


async def test_delete_profile_removes_config_and_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile("demo", BackendKind.S3, {"bucket": "b", "access_key": "AKIA"}, config_dir=tmp_path)
    stored = keyring.get_keyring()._values  # type: ignore[attr-defined]  # fake_keyring's backend
    assert len(stored) == 1
    await delete_profile("demo", config_dir=tmp_path)
    assert await list_profiles(config_dir=tmp_path) == []
    with pytest.raises(ProfileNotFoundError, match="no such profile"):
        await get_profile("demo", config_dir=tmp_path)
    assert stored == {}


async def test_delete_profile_missing_raises_profile_not_found(tmp_path: Path) -> None:
    with pytest.raises(ProfileNotFoundError, match="no such profile"):
        await delete_profile("nope", config_dir=tmp_path)


async def test_resaving_after_delete_does_not_inherit_stale_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile("demo", BackendKind.S3, {"bucket": "b", "access_key": "OLD"}, config_dir=tmp_path)
    await delete_profile("demo", config_dir=tmp_path)
    await save_profile("demo", BackendKind.S3, {"bucket": "b"}, config_dir=tmp_path)
    fields = await profile_fields_with_secrets("demo", config_dir=tmp_path)
    assert "access_key" not in fields


async def test_store_from_profile_s3_merges_config_and_secrets(tmp_path: Path, fake_keyring: None) -> None:
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
    store = await store_from_profile("demo", config_dir=tmp_path)
    assert isinstance(store, S3Store)
    assert store._bucket == "my-bucket"  # white-box check of the merged kwargs' effect
    assert store._client_kwargs == {
        "verify": True,
        "endpoint_url": "http://minio:9000",
        "region_name": "us-east-1",
        "aws_access_key_id": "AKIA",
        "aws_secret_access_key": "shh",
    }


async def test_store_from_profile_azure_merges_config_and_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.AZURE,
        {"container": "c", "account_url": "https://acct.blob.core.windows.net", "credential": "sas-token"},
        config_dir=tmp_path,
    )
    store = await store_from_profile("demo", config_dir=tmp_path)
    assert isinstance(store, AzureStore)


async def test_store_from_config_s3_merges_config_and_secrets() -> None:
    """``store_from_config`` with a not-yet-saved config, as the TUI's connect
    dialog uses it before a profile exists."""
    config = S3ProfileConfig(bucket="my-bucket", endpoint="http://minio:9000", region="us-east-1")
    store = await store_from_config(config, {"access_key": "AKIA", "secret_key": "shh"})
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
    store = await store_from_config(config, {"credential": "sas-token"})
    assert isinstance(store, AzureStore)


async def test_store_from_config_azure_without_an_account_url_is_a_field_error() -> None:
    with pytest.raises(ProfileFieldError, match="invalid Azure account URL") as excinfo:
        await store_from_config(AzureProfileConfig(container="c"), {"credential": "sas-token"})
    assert excinfo.value.field == "account_url"


async def test_store_from_profile_smb_merges_config_and_secrets(tmp_path: Path, fake_keyring: None) -> None:
    await save_profile(
        "demo",
        BackendKind.SMB,
        {"server": "nas.example.com", "share": "backups", "username": "admin", "password": "hunter2"},
        config_dir=tmp_path,
    )
    store = await store_from_profile("demo", config_dir=tmp_path)
    assert isinstance(store, SmbStore)
    assert store._share == "backups"  # white-box check of the merged kwargs' effect
    assert store._server == "nas.example.com"
    assert store._username == "admin"
    assert store._password == "hunter2"


async def test_store_from_config_smb_merges_config_and_secrets() -> None:
    config = SmbProfileConfig(server="nas.example.com", share="backups", username="admin")
    store = await store_from_config(config, {"password": "hunter2"})
    assert isinstance(store, SmbStore)
    assert store._share == "backups"
    assert store._server == "nas.example.com"
    assert store._password == "hunter2"


async def test_list_remote_items_dispatches_by_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    """Faked ``list_buckets``/``list_containers``, as in those functions' own tests."""
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
    """SMB has no account-level "list every share" operation: this fails rather
    than falling through to Azure's ``list_containers()``."""
    with pytest.raises(ValueError, match="smb"):
        await list_remote_items(BackendKind.SMB)


def test_non_secret_config_dataclasses_never_gain_a_secret_field() -> None:
    """A field also named in its backend's secret-field set would write a
    secret into ``profiles.json``."""
    from synology_apm_repo.sdk.profiles.model import secret_fields_for

    s3_fields = {f.name for f in __import__("dataclasses").fields(S3ProfileConfig)}
    azure_fields = {f.name for f in __import__("dataclasses").fields(AzureProfileConfig)}
    smb_fields = {f.name for f in __import__("dataclasses").fields(SmbProfileConfig)}
    assert s3_fields.isdisjoint(secret_fields_for(BackendKind.S3))
    assert azure_fields.isdisjoint(secret_fields_for(BackendKind.AZURE))
    assert smb_fields.isdisjoint(secret_fields_for(BackendKind.SMB))
