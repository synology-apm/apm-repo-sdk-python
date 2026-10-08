"""Validation for ``ConnectDialog``'s fields: a local path, or a remote
backend's fields through ``sdk.profiles``; a problem is a
``ConnectValidationError`` carrying the dialog's inline warning.
"""

from __future__ import annotations

from collections.abc import Mapping
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
from synology_apm_repo.sdk.profiles import BackendKind, ProfileConfig, ProfileFieldError, config_from_fields


class ConnectValidationError(Exception):
    """A field problem found before any scan, without network I/O."""


def _require(value: str, message: str) -> None:
    if not value:
        raise ConnectValidationError(message)


def validate_local(raw_path: str) -> Path:
    """The expanded directory ``raw_path`` names; raises
    ``ConnectValidationError`` when it is empty or not a directory (a local
    stat, not network I/O)."""
    _require(raw_path, CONNECT_NO_PATH_WARNING)
    path = Path(raw_path).expanduser()
    if not path.is_dir():
        raise ConnectValidationError(CONNECT_PATH_NOT_A_DIRECTORY_WARNING)
    return path


_FIELD_WARNINGS = {
    "bucket": CONNECT_NO_BUCKET_WARNING,
    "container": CONNECT_NO_CONTAINER_WARNING,
    "server": CONNECT_NO_SERVER_WARNING,
    "share": CONNECT_NO_SHARE_WARNING,
    "port": CONNECT_SMB_INVALID_PORT_WARNING,
}


def remote_config(kind: BackendKind, fields: Mapping[str, str | bool]) -> ProfileConfig:
    """``sdk.profiles.config_from_fields``, with a field problem reported as
    this dialog's own inline warning."""
    try:
        return config_from_fields(kind, fields)
    except ProfileFieldError as exc:
        raise ConnectValidationError(_FIELD_WARNINGS.get(exc.field, str(exc))) from None
