"""Unit tests for ``synology_apm_repo.sdk.profiles.model`` — the
non-secret config dataclasses' translation to the underlying client's own
kwarg names, and the backend/secret-field lookup helpers."""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.profiles.errors import ProfileFieldError
from synology_apm_repo.sdk.profiles.model import (
    AzureProfileConfig,
    BackendKind,
    S3ProfileConfig,
    SmbProfileConfig,
    client_kwargs_with_secrets,
    config_from_fields,
    config_from_json,
    form_fields_for,
    secret_fields_for,
)
from synology_apm_repo.sdk.storage.smb import DEFAULT_SMB_PORT


def test_s3_client_kwargs_omits_unset_optional_fields() -> None:
    from synology_apm_repo.sdk.profiles.model import S3ProfileConfig

    config = S3ProfileConfig(bucket="my-bucket")
    assert config.client_kwargs == {"verify": True}


def test_s3_client_kwargs_translates_to_boto_names() -> None:
    from synology_apm_repo.sdk.profiles.model import S3ProfileConfig

    config = S3ProfileConfig(bucket="b", endpoint="http://minio:9000", region="us-east-1", verify_tls=False)
    assert config.client_kwargs == {
        "verify": False,
        "endpoint_url": "http://minio:9000",
        "region_name": "us-east-1",
    }


def test_azure_client_kwargs_uses_connection_verify_not_verify() -> None:
    """``azure.core.Configuration`` names this ``connection_verify``, unlike
    boto3's ``verify``."""
    config = AzureProfileConfig(container="c", account_url="https://acct.blob.core.windows.net", verify_tls=False)
    assert config.client_kwargs == {
        "connection_verify": False,
        "account_url": "https://acct.blob.core.windows.net",
    }


def test_smb_client_kwargs_omits_unset_username() -> None:
    config = SmbProfileConfig(server="nas.example.com", share="backups")
    assert config.client_kwargs == {"server": "nas.example.com", "port": 445}


def test_smb_client_kwargs_passes_domain_username_form_through_unsplit() -> None:
    """``SmbProfileConfig`` never splits ``DOMAIN\\username`` itself --
    ``smbprotocol``'s NTLM/SPNEGO layer splits the domain out of it."""
    config = SmbProfileConfig(server="nas.example.com", share="backups", port=1445, username="WORKGROUP\\admin")
    assert config.client_kwargs == {"server": "nas.example.com", "port": 1445, "username": "WORKGROUP\\admin"}


def test_secret_fields_for_every_backend_are_pairwise_disjoint() -> None:
    s3, azure, smb = (secret_fields_for(kind) for kind in (BackendKind.S3, BackendKind.AZURE, BackendKind.SMB))
    assert s3 == ("access_key", "secret_key")
    assert azure == ("credential",)
    assert smb == ("password",)
    assert set(s3).isdisjoint(azure) and set(s3).isdisjoint(smb) and set(azure).isdisjoint(smb)


def test_every_secret_field_has_a_prompt() -> None:
    for kind in BackendKind:
        assert all(spec.prompt for spec in form_fields_for(kind) if spec.secret)


class TestConfigFromFields:
    def test_builds_each_backend_and_ignores_secrets_and_blank_optionals(self) -> None:
        s3 = config_from_fields(BackendKind.S3, {"bucket": "b", "endpoint": "", "access_key": "AK"})
        assert s3 == S3ProfileConfig(bucket="b", endpoint=None, region=None, verify_tls=True)
        azure = config_from_fields(BackendKind.AZURE, {"container": "c", "verify_tls": False})
        assert azure == AzureProfileConfig(container="c", verify_tls=False)
        smb = config_from_fields(BackendKind.SMB, {"server": "nas", "share": "s", "port": "1445"})
        assert smb == SmbProfileConfig(server="nas", share="s", port=1445)

    def test_a_blank_port_means_the_default(self) -> None:
        smb = config_from_fields(BackendKind.SMB, {"server": "nas", "share": "s", "port": ""})
        assert isinstance(smb, SmbProfileConfig) and smb.port == DEFAULT_SMB_PORT

    def test_a_blank_required_field_names_itself(self) -> None:
        with pytest.raises(ProfileFieldError, match="share is required for a smb profile") as exc_info:
            config_from_fields(BackendKind.SMB, {"server": "nas", "share": ""})
        assert exc_info.value.field == "share"

    def test_a_non_numeric_port_is_a_field_error(self) -> None:
        with pytest.raises(ProfileFieldError, match="port must be a number, not") as exc_info:
            config_from_fields(BackendKind.SMB, {"server": "nas", "share": "s", "port": "abc"})
        assert exc_info.value.field == "port"

    def test_check_required_off_accepts_no_bucket_yet(self) -> None:
        s3 = config_from_fields(BackendKind.S3, {"endpoint": "http://minio:9000"}, check_required=False)
        assert s3 == S3ProfileConfig(bucket="", endpoint="http://minio:9000")


class TestConfigFromJson:
    def test_reads_a_saved_entry(self) -> None:
        raw = {"server": "nas", "share": "s", "port": 1445, "username": None}
        assert config_from_json(BackendKind.SMB, raw) == SmbProfileConfig(server="nas", share="s", port=1445)

    def test_a_missing_required_field_raises_key_error(self) -> None:
        with pytest.raises(KeyError, match="bucket"):
            config_from_json(BackendKind.S3, {"endpoint": "x"})

    @pytest.mark.parametrize(
        ("kind", "raw", "message"),
        [
            (BackendKind.S3, {"bucket": "b", "verify_tls": "false"}, "verify_tls must be bool"),
            (BackendKind.SMB, {"server": "nas", "share": "s", "port": "445"}, "port must be int"),
            (BackendKind.SMB, {"server": "nas", "share": "s", "port": True}, "port must be int"),
            (BackendKind.AZURE, {"container": 3}, "container must be str"),
        ],
    )
    def test_a_wrongly_typed_field_is_rejected_not_coerced(
        self, kind: BackendKind, raw: dict[str, object], message: str
    ) -> None:
        with pytest.raises(TypeError, match=message):
            config_from_json(kind, raw)


def test_labels_and_kind() -> None:
    assert S3ProfileConfig(bucket="b").label == "s3://b"
    assert AzureProfileConfig(container="c").label == "azure://c"
    assert SmbProfileConfig(server="nas", share="s").label == "smb://nas/s"
    assert SmbProfileConfig(server="nas", share="s").kind is BackendKind.SMB
    assert BackendKind("s3") is BackendKind.S3 and f"{BackendKind.AZURE}" == "azure"


def test_client_kwargs_with_secrets_treats_falsy_value_as_absent() -> None:
    """A present-but-empty-string secret (an untouched form field) is
    treated as absent."""
    config = S3ProfileConfig(bucket="my-bucket")
    kwargs = client_kwargs_with_secrets(config, {"access_key": "", "secret_key": "shh"})
    assert "aws_access_key_id" not in kwargs
    assert kwargs["aws_secret_access_key"] == "shh"


def test_client_kwargs_with_secrets_ignores_fields_of_the_other_kind() -> None:
    """``secrets`` may carry both backends' fields (an unsaved connection
    form); only ``config``'s own kind's fields are read."""
    config = S3ProfileConfig(bucket="my-bucket", endpoint="http://minio:9000")
    kwargs = client_kwargs_with_secrets(
        config,
        {"access_key": "AKIA", "secret_key": "shh", "credential": "azure-cred"},
    )
    assert kwargs == {
        "verify": True,
        "endpoint_url": "http://minio:9000",
        "aws_access_key_id": "AKIA",
        "aws_secret_access_key": "shh",
    }


def test_client_kwargs_with_secrets_passes_azure_credential_through() -> None:
    config = AzureProfileConfig(container="my-container")
    kwargs = client_kwargs_with_secrets(config, {"credential": "azure-cred"})
    assert kwargs == {"connection_verify": True, "credential": "azure-cred"}


def test_client_kwargs_with_secrets_passes_smb_password_through() -> None:
    config = SmbProfileConfig(server="nas.example.com", share="backups", username="admin")
    kwargs = client_kwargs_with_secrets(config, {"password": "hunter2"})
    assert kwargs == {"server": "nas.example.com", "port": 445, "username": "admin", "password": "hunter2"}
