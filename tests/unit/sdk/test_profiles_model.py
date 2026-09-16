"""Unit tests for ``synology_apm_repo.sdk.profiles.model`` — the
non-secret config dataclasses' translation to the underlying client's own
kwarg names, and the backend/secret-field lookup helpers."""

from __future__ import annotations

from synology_apm_repo.sdk.profiles.model import (
    AZURE_SECRET_FIELDS,
    S3_SECRET_FIELDS,
    SMB_SECRET_FIELDS,
    AzureProfileConfig,
    BackendKind,
    S3ProfileConfig,
    SmbProfileConfig,
    client_kwargs_with_secrets,
    secret_fields_for,
)


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
    """``azure.core.Configuration``'s own name for this is
    ``connection_verify``, deliberately different from boto3's ``verify``
    — see the dataclass's own docstring for why this asymmetry is
    intentional, not a bug to "fix" into consistency."""
    config = AzureProfileConfig(container="c", account_url="https://acct.blob.core.windows.net", verify_tls=False)
    assert config.client_kwargs == {
        "connection_verify": False,
        "account_url": "https://acct.blob.core.windows.net",
    }


def test_smb_client_kwargs_omits_unset_username() -> None:
    config = SmbProfileConfig(server="nas.example.com", share="backups")
    assert config.client_kwargs == {"server": "nas.example.com", "port": 445}


def test_smb_client_kwargs_passes_domain_username_form_through_unsplit() -> None:
    """``SmbProfileConfig`` never splits ``DOMAIN\\username`` itself — see
    the dataclass's own docstring for why that's ``smbprotocol``'s job."""
    config = SmbProfileConfig(server="nas.example.com", share="backups", port=1445, username="WORKGROUP\\admin")
    assert config.client_kwargs == {"server": "nas.example.com", "port": 1445, "username": "WORKGROUP\\admin"}


def test_secret_fields_for_every_backend_are_pairwise_disjoint() -> None:
    assert secret_fields_for(BackendKind.S3) == S3_SECRET_FIELDS
    assert secret_fields_for(BackendKind.AZURE) == AZURE_SECRET_FIELDS
    assert secret_fields_for(BackendKind.SMB) == SMB_SECRET_FIELDS
    assert set(S3_SECRET_FIELDS).isdisjoint(AZURE_SECRET_FIELDS)
    assert set(S3_SECRET_FIELDS).isdisjoint(SMB_SECRET_FIELDS)
    assert set(AZURE_SECRET_FIELDS).isdisjoint(SMB_SECRET_FIELDS)


def test_client_kwargs_with_secrets_treats_falsy_value_as_absent() -> None:
    """A present-but-empty-string secret (an untouched form field) must be
    treated the same as the key being absent entirely — the documented
    behavior, not just "whatever ``if value`` happens to do"."""
    config = S3ProfileConfig(bucket="my-bucket")
    kwargs = client_kwargs_with_secrets(config, {"access_key": "", "secret_key": "shh"})
    assert "aws_access_key_id" not in kwargs
    assert kwargs["aws_secret_access_key"] == "shh"


def test_client_kwargs_with_secrets_ignores_fields_of_the_other_kind() -> None:
    """``secrets`` may be a superset of what applies to ``config``'s own
    kind (a not-yet-saved connection form carries both backends' fields at
    once) — only the S3 fields are read when ``config`` is an
    ``S3ProfileConfig``, even though an Azure ``credential`` is also
    present in the mapping."""
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
