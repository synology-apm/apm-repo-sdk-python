"""Pure unit tests for ``core/connect/validate.py`` -- every branch
``ConnectDialog``'s own S3/Azure/SMB "Browse"/submit Pilot tests
(``test_browser_pilot_remote_browser.py``) already exercise end to end,
proven here in isolation instead: no ``App``/``Pilot``, no real
filesystem beyond ``validate_local``'s one local-path check, which is a
local ``Path.is_dir()`` stat call, not real network I/O."""

from __future__ import annotations

from pathlib import Path

import pytest

from synology_apm_repo.browser.core.connect.validate import (
    ConnectValidationError,
    azure_config_and_secrets,
    s3_config_and_secrets,
    smb_config_and_secrets,
    validate_azure,
    validate_local,
    validate_s3,
)
from synology_apm_repo.browser.strings import (
    CONNECT_NO_BUCKET_WARNING,
    CONNECT_NO_CONTAINER_WARNING,
    CONNECT_NO_PATH_WARNING,
    CONNECT_NO_SERVER_WARNING,
    CONNECT_NO_SHARE_WARNING,
    CONNECT_PATH_NOT_A_DIRECTORY_WARNING,
    CONNECT_SMB_INVALID_PORT_WARNING,
)


def test_validate_local_rejects_an_empty_path() -> None:
    with pytest.raises(ConnectValidationError, match=CONNECT_NO_PATH_WARNING):
        validate_local("")


def test_validate_local_rejects_a_path_that_is_not_a_directory(tmp_path: Path) -> None:
    file_path = tmp_path / "not-a-dir.txt"
    file_path.write_text("x")
    with pytest.raises(ConnectValidationError, match=CONNECT_PATH_NOT_A_DIRECTORY_WARNING):
        validate_local(str(file_path))


def test_validate_local_accepts_a_real_directory(tmp_path: Path) -> None:
    assert validate_local(str(tmp_path)) == tmp_path


def test_validate_local_expands_a_user_relative_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    home_child = tmp_path / "sub"
    home_child.mkdir()
    assert validate_local("~/sub") == home_child


def test_s3_config_and_secrets_builds_the_config_with_no_validation() -> None:
    """No bucket-presence check here -- an empty bucket is exactly what
    ``ConnectDialog.s3_client_kwargs`` deliberately passes for
    ``RemoteOptionsBrowser.browse_buckets``'s own placeholder."""
    config, secret_source = s3_config_and_secrets(
        bucket="", endpoint="", region="", access_key="ak", secret_key="sk", verify_tls=True
    )
    assert config.bucket == ""
    assert config.endpoint is None  # "" normalized to None, not the literal empty string
    assert config.region is None
    assert config.verify_tls is True
    assert secret_source == {"access_key": "ak", "secret_key": "sk"}


def test_validate_s3_rejects_an_empty_bucket() -> None:
    with pytest.raises(ConnectValidationError, match=CONNECT_NO_BUCKET_WARNING):
        validate_s3(bucket="", endpoint="", region="", access_key="", secret_key="", verify_tls=True)


def test_validate_s3_accepts_a_real_bucket_and_keeps_populated_fields() -> None:
    config, secret_source = validate_s3(
        bucket="my-bucket",
        endpoint="https://s3.example.com",
        region="us-east-1",
        access_key="ak",
        secret_key="sk",
        verify_tls=False,
    )
    assert config.bucket == "my-bucket"
    assert config.endpoint == "https://s3.example.com"
    assert config.region == "us-east-1"
    assert config.verify_tls is False
    assert secret_source == {"access_key": "ak", "secret_key": "sk"}


def test_azure_config_and_secrets_builds_the_config_with_no_validation() -> None:
    config, secret_source = azure_config_and_secrets(container="", account_url="", credential="cred")
    assert config.container == ""
    assert config.account_url is None
    assert secret_source == {"credential": "cred"}


def test_validate_azure_rejects_an_empty_container() -> None:
    with pytest.raises(ConnectValidationError, match=CONNECT_NO_CONTAINER_WARNING):
        validate_azure(container="", account_url="", credential="")


def test_validate_azure_accepts_a_real_container() -> None:
    config, secret_source = validate_azure(
        container="my-container", account_url="https://acct.blob.core.windows.net", credential="cred"
    )
    assert config.container == "my-container"
    assert config.account_url == "https://acct.blob.core.windows.net"
    assert secret_source == {"credential": "cred"}


def test_smb_config_and_secrets_rejects_an_empty_server() -> None:
    with pytest.raises(ConnectValidationError, match=CONNECT_NO_SERVER_WARNING):
        smb_config_and_secrets(server="", share="share", port_text="", username="", password="")


def test_smb_config_and_secrets_rejects_an_empty_share() -> None:
    with pytest.raises(ConnectValidationError, match=CONNECT_NO_SHARE_WARNING):
        smb_config_and_secrets(server="server", share="", port_text="", username="", password="")


def test_smb_config_and_secrets_rejects_a_non_numeric_port() -> None:
    with pytest.raises(ConnectValidationError, match=CONNECT_SMB_INVALID_PORT_WARNING):
        smb_config_and_secrets(server="server", share="share", port_text="not-a-port", username="", password="")


def test_smb_config_and_secrets_defaults_the_port_when_blank() -> None:
    config, secret_source = smb_config_and_secrets(
        server="server", share="share", port_text="", username="", password="pw"
    )
    assert config.port == 445
    assert config.username is None  # "" normalized to None
    assert secret_source == {"password": "pw"}


def test_smb_config_and_secrets_accepts_an_explicit_port_and_username() -> None:
    config, secret_source = smb_config_and_secrets(
        server="server", share="share", port_text="1445", username="DOMAIN\\user", password="pw"
    )
    assert config.server == "server"
    assert config.share == "share"
    assert config.port == 1445
    assert config.username == "DOMAIN\\user"
    assert secret_source == {"password": "pw"}


__all__: list[str] = []
