"""Pure unit tests for ``core/connect/validate.py``, without an
``App``/``Pilot``; ``test_browser_screens_connect_dialog_remote_backends.py`` drives the same
checks through ``ConnectDialog``."""

from __future__ import annotations

from pathlib import Path

import pytest

from synology_apm_repo.browser.core.connect.validate import ConnectValidationError, remote_config, validate_local
from synology_apm_repo.browser.strings import (
    CONNECT_NO_BUCKET_WARNING,
    CONNECT_NO_CONTAINER_WARNING,
    CONNECT_NO_PATH_WARNING,
    CONNECT_NO_SERVER_WARNING,
    CONNECT_NO_SHARE_WARNING,
    CONNECT_PATH_NOT_A_DIRECTORY_WARNING,
    CONNECT_SMB_INVALID_PORT_WARNING,
)
from synology_apm_repo.sdk.profiles import (
    DEFAULT_SMB_PORT,
    AzureProfileConfig,
    BackendKind,
    S3ProfileConfig,
    SmbProfileConfig,
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
    # expanduser() reads HOME on POSIX and USERPROFILE on Windows.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    home_child = tmp_path / "sub"
    home_child.mkdir()
    assert validate_local("~/sub") == home_child


@pytest.mark.parametrize(
    ("kind", "fields", "warning"),
    [
        (BackendKind.S3, {"bucket": ""}, CONNECT_NO_BUCKET_WARNING),
        (BackendKind.AZURE, {"container": ""}, CONNECT_NO_CONTAINER_WARNING),
        (BackendKind.SMB, {"server": "", "share": "s"}, CONNECT_NO_SERVER_WARNING),
        (BackendKind.SMB, {"server": "nas", "share": ""}, CONNECT_NO_SHARE_WARNING),
        (BackendKind.SMB, {"server": "nas", "share": "s", "port": "abc"}, CONNECT_SMB_INVALID_PORT_WARNING),
    ],
)
def test_remote_config_reports_a_field_problem_as_the_dialogs_own_warning(
    kind: BackendKind, fields: dict[str, str], warning: str
) -> None:
    with pytest.raises(ConnectValidationError, match=warning):
        remote_config(kind, fields)


def test_remote_config_builds_each_backends_config_from_the_tab_fields() -> None:
    s3 = remote_config(
        BackendKind.S3,
        {"bucket": "b", "endpoint": "https://e", "region": "", "verify_tls": False, "access_key": "ak"},
    )
    assert s3 == S3ProfileConfig(bucket="b", endpoint="https://e", region=None, verify_tls=False)
    azure = remote_config(BackendKind.AZURE, {"container": "c", "account_url": "", "verify_tls": False})
    assert azure == AzureProfileConfig(container="c", account_url=None, verify_tls=False)
    smb = remote_config(BackendKind.SMB, {"server": "nas", "share": "s", "port": "", "username": "u"})
    assert smb == SmbProfileConfig(server="nas", share="s", port=DEFAULT_SMB_PORT, username="u")
