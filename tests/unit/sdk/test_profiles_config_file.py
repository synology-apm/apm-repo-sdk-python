"""Unit tests for ``synology_apm_repo.sdk.profiles.config_file`` —
``profiles.json`` persistence: empty/missing file, corrupt JSON, schema
validation, the atomic-write guarantee, and the XDG-everywhere default
directory resolution. Always points ``config_dir`` at ``tmp_path`` for the
persistence tests below, never the real per-user default path."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TextIO

import pytest

from synology_apm_repo.sdk.profiles import config_file
from synology_apm_repo.sdk.profiles.errors import ProfileConfigCorruptError
from synology_apm_repo.sdk.profiles.model import (
    AzureProfileConfig,
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
            config=S3ProfileConfig(bucket="b", endpoint="http://minio:9000", region=None, verify_tls=True),
        ),
        "azure-one": Profile(
            name="azure-one",
            config=AzureProfileConfig(container="c", account_url="https://acct.blob.core.windows.net"),
        ),
        "smb-one": Profile(
            name="smb-one",
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
    with pytest.raises(ProfileConfigCorruptError, match="could not parse"):
        config_file.read_profiles(config_dir=tmp_path)


def test_read_missing_schema_version_raises(tmp_path: Path) -> None:
    (tmp_path / "profiles.json").write_text(json.dumps({"profiles": {}}), encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError, match="unsupported or missing schema_version in"):
        config_file.read_profiles(config_dir=tmp_path)


@pytest.mark.parametrize(
    ("profile", "message"),
    [
        pytest.param({"kind": "gcs", "bucket": "b"}, "has an invalid or missing 'kind'", id="unknown_kind"),
        pytest.param({"kind": "s3"}, "is missing required field 'bucket'", id="missing_required_field"),
        pytest.param(
            {"kind": "smb", "server": "nas.example.com"},
            "is missing required field 'share'",
            id="smb_missing_required_field",
        ),
    ],
)
def test_read_invalid_profile_raises(tmp_path: Path, profile: dict[str, str], message: str) -> None:
    payload = {"schema_version": 1, "profiles": {"x": profile}}
    (tmp_path / "profiles.json").write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError, match=message):
        config_file.read_profiles(config_dir=tmp_path)


def test_read_malformed_profiles_object_raises(tmp_path: Path) -> None:
    # A valid schema_version, but "profiles" itself isn't the expected
    # {name: entry} mapping shape.
    payload = {"schema_version": 1, "profiles": ["not", "a", "mapping"]}
    (tmp_path / "profiles.json").write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProfileConfigCorruptError, match="malformed 'profiles' object in"):
        config_file.read_profiles(config_dir=tmp_path)


def test_write_failure_cleans_up_its_own_tmp_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _failing_dump(*args: object, **kwargs: object) -> None:
        raise OSError("synthetic disk-full for this test")

    monkeypatch.setattr(json, "dump", _failing_dump)
    with pytest.raises(OSError, match="synthetic disk-full"):
        config_file.write_profiles({}, config_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_write_is_atomic_no_tmp_file_left_behind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_file.write_profiles({"a": Profile(name="a", config=S3ProfileConfig(bucket="one"))}, config_dir=tmp_path)
    assert [p.name for p in tmp_path.iterdir()] == ["profiles.json"]
    before = (tmp_path / "profiles.json").read_bytes()

    def _dump_half_then_fail(obj: object, handle: TextIO, **kwargs: object) -> None:
        handle.write('{"schema_version": 1, "prof')
        handle.flush()
        raise OSError("synthetic crash mid-write")

    monkeypatch.setattr(json, "dump", _dump_half_then_fail)
    with pytest.raises(OSError, match="synthetic crash mid-write"):
        config_file.write_profiles({"a": Profile(name="a", config=S3ProfileConfig(bucket="two"))}, config_dir=tmp_path)

    # The half-written payload never replaced the old file.
    assert (tmp_path / "profiles.json").read_bytes() == before
    assert [p.name for p in tmp_path.iterdir()] == ["profiles.json"]


def test_write_overwrites_existing_file(tmp_path: Path) -> None:
    config_file.write_profiles({"a": Profile(name="a", config=S3ProfileConfig(bucket="one"))}, config_dir=tmp_path)
    config_file.write_profiles({"a": Profile(name="a", config=S3ProfileConfig(bucket="two"))}, config_dir=tmp_path)
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


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("", id="empty"),
        # The XDG Base Directory Specification treats a relative value as
        # invalid, i.e. unset, not resolved against the current directory.
        pytest.param("relative/path", id="relative"),
    ],
)
def test_default_config_dir_ignores_an_invalid_xdg_config_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, value: str
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", value)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert config_file.default_config_dir() == tmp_path / ".config" / "synology-apm-repo"
