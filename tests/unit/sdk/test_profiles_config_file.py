"""Unit tests for ``synology_apm_repo.sdk.profiles.config_file`` —
``profiles.json`` persistence: empty/missing file, corrupt JSON, schema
validation, the atomic-write guarantee, and the XDG-everywhere default
directory resolution. Always points ``config_dir`` at ``tmp_path`` for the
persistence tests below, never the real per-user default path."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from synology_apm_repo.sdk.errors import ProfileConfigCorruptError
from synology_apm_repo.sdk.profiles import config_file
from synology_apm_repo.sdk.profiles.model import (
    AzureProfileConfig,
    BackendKind,
    Profile,
    S3ProfileConfig,
    SmbProfileConfig,
)


def test_read_profiles_missing_file_is_empty(tmp_path: Path) -> None:
    assert config_file.read_profiles(config_dir=tmp_path) == {}


def test_write_then_read_round_trips_every_backend(tmp_path: Path) -> None:
    profiles = {
        "s3-one": Profile(
            name="s3-one",
            kind=BackendKind.S3,
            config=S3ProfileConfig(bucket="b", endpoint="http://minio:9000", region=None, verify_tls=True),
        ),
        "azure-one": Profile(
            name="azure-one",
            kind=BackendKind.AZURE,
            config=AzureProfileConfig(container="c", account_url="https://acct.blob.core.windows.net"),
        ),
        "smb-one": Profile(
            name="smb-one",
            kind=BackendKind.SMB,
            config=SmbProfileConfig(server="nas.example.com", share="backups", username="admin"),
        ),
    }
    config_file.write_profiles(profiles, config_dir=tmp_path)
    assert config_file.read_profiles(config_dir=tmp_path) == profiles


def test_write_creates_config_dir(tmp_path: Path) -> None:
    nested = tmp_path / "nested" / "config"
    config_file.write_profiles({}, config_dir=nested)
    assert (nested / "profiles.json").exists()


def test_read_corrupt_json_raises_profile_config_corrupt(tmp_path: Path) -> None:
    (tmp_path / "profiles.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError):
        config_file.read_profiles(config_dir=tmp_path)


def test_read_missing_schema_version_raises(tmp_path: Path) -> None:
    (tmp_path / "profiles.json").write_text(json.dumps({"profiles": {}}), encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError):
        config_file.read_profiles(config_dir=tmp_path)


def test_read_unknown_kind_raises(tmp_path: Path) -> None:
    payload = {"schema_version": 1, "profiles": {"x": {"kind": "gcs", "bucket": "b"}}}
    (tmp_path / "profiles.json").write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError):
        config_file.read_profiles(config_dir=tmp_path)


def test_read_missing_required_field_raises(tmp_path: Path) -> None:
    payload = {"schema_version": 1, "profiles": {"x": {"kind": "s3"}}}  # no "bucket"
    (tmp_path / "profiles.json").write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError):
        config_file.read_profiles(config_dir=tmp_path)


def test_read_smb_missing_required_field_raises(tmp_path: Path) -> None:
    payload = {"schema_version": 1, "profiles": {"x": {"kind": "smb", "server": "nas.example.com"}}}  # no "share"
    (tmp_path / "profiles.json").write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError):
        config_file.read_profiles(config_dir=tmp_path)


def test_read_malformed_profiles_object_raises(tmp_path: Path) -> None:
    # A valid schema_version, but "profiles" itself isn't the expected
    # {name: entry} mapping shape.
    payload = {"schema_version": 1, "profiles": ["not", "a", "mapping"]}
    (tmp_path / "profiles.json").write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError):
        config_file.read_profiles(config_dir=tmp_path)


def test_write_failure_cleans_up_its_own_tmp_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _failing_dump(*args: object, **kwargs: object) -> None:
        raise OSError("synthetic disk-full for this test")

    monkeypatch.setattr(json, "dump", _failing_dump)
    with pytest.raises(OSError, match="synthetic disk-full"):
        config_file.write_profiles({}, config_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []  # the .tmp file was cleaned up, not left behind


def test_write_is_atomic_no_tmp_file_left_behind(tmp_path: Path) -> None:
    config_file.write_profiles(
        {"a": Profile(name="a", kind=BackendKind.S3, config=S3ProfileConfig(bucket="b"))}, config_dir=tmp_path
    )
    leftovers = [p for p in tmp_path.iterdir() if p.name != "profiles.json"]
    assert leftovers == []


def test_write_overwrites_existing_file(tmp_path: Path) -> None:
    config_file.write_profiles(
        {"a": Profile(name="a", kind=BackendKind.S3, config=S3ProfileConfig(bucket="one"))}, config_dir=tmp_path
    )
    config_file.write_profiles(
        {"a": Profile(name="a", kind=BackendKind.S3, config=S3ProfileConfig(bucket="two"))}, config_dir=tmp_path
    )
    profiles = config_file.read_profiles(config_dir=tmp_path)
    assert isinstance(profiles["a"].config, S3ProfileConfig)
    assert profiles["a"].config.bucket == "two"


def test_default_config_dir_falls_back_to_dot_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert config_file.default_config_dir() == tmp_path / ".config" / "synology-apm-repo"


def test_default_config_dir_honors_xdg_config_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    xdg = tmp_path / "xdg-config"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    assert config_file.default_config_dir() == xdg / "synology-apm-repo"


def test_default_config_dir_ignores_empty_xdg_config_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", "")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert config_file.default_config_dir() == tmp_path / ".config" / "synology-apm-repo"


def test_default_config_dir_ignores_relative_xdg_config_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Per the XDG Base Directory Specification, a relative value is
    invalid and must be treated as unset, not resolved relative to the
    current directory."""
    monkeypatch.setenv("XDG_CONFIG_HOME", "relative/path")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert config_file.default_config_dir() == tmp_path / ".config" / "synology-apm-repo"
