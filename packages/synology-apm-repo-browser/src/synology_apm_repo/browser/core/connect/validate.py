"""Pure validation for ``ConnectDialog``'s own per-backend fields -- one
function per backend tab, each translating already-read, already-
``.strip()``ped raw widget values (never an ``Input``/``Checkbox``
itself -- this module imports no Textual) into either a validated
``store_from_config``-ready ``config``/``secret_source`` pair, or a
raised :class:`ConnectValidationError`. This is deliberately *not* a
Model in the MVU sense: nothing here is held onto across a call, and no
secret value (an S3 secret key, an Azure credential, an SMB password)
is ever stored anywhere by this module -- each function's own
``secret_source`` return value is consumed immediately by
``store_from_config`` and discarded, the same one-shot flow
``ConnectDialog`` follows directly.

S3/Azure additionally expose their own config/secret builder
(``s3_config_and_secrets``/``azure_config_and_secrets``) *without* a
bucket/container-presence check -- shared by this module's own
``validate_s3``/``validate_azure`` (bucket/container already confirmed
non-empty) and ``ConnectDialog.s3_client_kwargs``/``azure_client_kwargs``'s
own deliberately bucket-less/container-less placeholders, which
``RemoteOptionsBrowser``'s "Browse" flow needs precisely because no
bucket/container has been picked yet. SMB has no such builder, since no
account-level share-listing operation exists to need a share-less
placeholder for.
"""

from __future__ import annotations

from pathlib import Path

from synology_apm_repo.browser.strings import (
    CONNECT_NO_BUCKET_WARNING,
    CONNECT_NO_CONTAINER_WARNING,
    CONNECT_NO_PATH_WARNING,
    CONNECT_NO_SERVER_WARNING,
    CONNECT_NO_SHARE_WARNING,
    CONNECT_PATH_NOT_A_DIRECTORY_WARNING,
    CONNECT_SMB_INVALID_PORT_WARNING,
)
from synology_apm_repo.sdk.profiles import AzureProfileConfig, S3ProfileConfig, SmbProfileConfig


class ConnectValidationError(Exception):
    """A field this dialog itself can check before ever attempting a
    scan (empty path/bucket/container/server/share, a local path that
    isn't a directory, or a non-numeric SMB port) — never raised for
    anything that needs real network I/O to detect."""


def _require(value: str, message: str) -> None:
    if not value:
        raise ConnectValidationError(message)


def validate_local(raw_path: str) -> Path:
    """``Path.is_dir()`` is a local stat call, not network I/O, so
    raising here still fits :class:`ConnectValidationError`'s scope."""
    _require(raw_path, CONNECT_NO_PATH_WARNING)
    path = Path(raw_path).expanduser()
    if not path.is_dir():
        raise ConnectValidationError(CONNECT_PATH_NOT_A_DIRECTORY_WARNING)
    return path


def s3_config_and_secrets(
    *, bucket: str, endpoint: str, region: str, access_key: str, secret_key: str, verify_tls: bool
) -> tuple[S3ProfileConfig, dict[str, str]]:
    config = S3ProfileConfig(bucket=bucket, endpoint=endpoint or None, region=region or None, verify_tls=verify_tls)
    return config, {"access_key": access_key, "secret_key": secret_key}


def validate_s3(
    *, bucket: str, endpoint: str, region: str, access_key: str, secret_key: str, verify_tls: bool
) -> tuple[S3ProfileConfig, dict[str, str]]:
    _require(bucket, CONNECT_NO_BUCKET_WARNING)
    return s3_config_and_secrets(
        bucket=bucket,
        endpoint=endpoint,
        region=region,
        access_key=access_key,
        secret_key=secret_key,
        verify_tls=verify_tls,
    )


def azure_config_and_secrets(
    *, container: str, account_url: str, credential: str
) -> tuple[AzureProfileConfig, dict[str, str]]:
    config = AzureProfileConfig(container=container, account_url=account_url or None)
    return config, {"credential": credential}


def validate_azure(*, container: str, account_url: str, credential: str) -> tuple[AzureProfileConfig, dict[str, str]]:
    _require(container, CONNECT_NO_CONTAINER_WARNING)
    return azure_config_and_secrets(container=container, account_url=account_url, credential=credential)


def smb_config_and_secrets(
    *, server: str, share: str, port_text: str, username: str, password: str
) -> tuple[SmbProfileConfig, dict[str, str]]:
    """Unlike S3/Azure, SMB validates ``server``/``share`` unconditionally
    — no "Browse" flow ever needs a share-less variant."""
    _require(server, CONNECT_NO_SERVER_WARNING)
    _require(share, CONNECT_NO_SHARE_WARNING)
    try:
        port = int(port_text) if port_text else 445
    except ValueError:
        raise ConnectValidationError(CONNECT_SMB_INVALID_PORT_WARNING) from None
    config = SmbProfileConfig(server=server, share=share, port=port, username=username or None)
    return config, {"password": password}
