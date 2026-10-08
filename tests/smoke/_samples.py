"""Loader for the gitignored ``tests/smoke/smoke_samples.toml`` (template:
``smoke_samples.toml.example``): one TOML array-of-tables per sample kind --
``[[local]]``, ``[[profile]]``, ``[[remote_storage]]``. ``[[remote_storage]]``
fields go through ``sdk.profiles``' field registry (``config_from_fields``/
``secret_fields_for``), so a sample feeds ``store_from_config()`` directly.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from synology_apm_repo.sdk.profiles import BackendKind, ProfileConfig, config_from_fields
from synology_apm_repo.sdk.profiles.model import secret_fields_for

_SAMPLES_PATH = Path(__file__).parent / "smoke_samples.toml"


@dataclass(frozen=True)
class LocalSample:
    """One ``[[local]]`` block: a real, on-disk repository root."""

    name: str
    path: str
    key: str = ""


@dataclass(frozen=True)
class ProfileSample:
    """One ``[[profile]]`` block: a saved ``sdk.profiles`` connection
    (``synology-apm-repo-cli profile add``), resolved via
    ``profiles.store_from_profile()``. ``path``, when given, is a
    store-relative prefix within the profile's bucket/container/share, as
    with the CLI's ``--profile``."""

    name: str
    profile: str
    path: str = ""
    key: str = ""


@dataclass(frozen=True)
class RemoteStorageSample:
    """One ``[[remote_storage]]`` block: a live S3/Azure/SMB target built
    from credentials in this file (``config``/``secrets`` for
    ``sdk.profiles.store_from_config()``), with no saved profile.

    The CLI has no raw-credential flag, so ``cli/`` reaches this kind
    through a throwaway saved profile (``cli/_remote_profiles.py``)."""

    name: str
    kind: BackendKind
    config: ProfileConfig
    secrets: dict[str, str]
    path: str = ""
    key: str = ""


SampleEntry = LocalSample | ProfileSample | RemoteStorageSample

_LOCAL_FIELDS = {"name", "path", "key"}
_PROFILE_FIELDS = {"name", "profile", "path", "key"}
_REMOTE_STORAGE_FIELDS = {
    "name",
    "type",
    "path",
    "key",
    "bucket",
    "endpoint",
    "region",
    "verify_tls",
    "access_key",
    "secret_key",
    "container",
    "account_url",
    "credential",
    "server",
    "share",
    "port",
    "username",
    "password",
}


def _warn_unknown(table: str, entry: dict[str, Any], known: set[str]) -> None:
    unknown = entry.keys() - known
    if unknown:
        print(f"[smoke_samples] warning: ignoring unknown key(s) in [[{table}]]: {', '.join(sorted(unknown))}")


def _parse_remote_storage(entry: dict[str, Any]) -> RemoteStorageSample:
    backend = str(entry.get("type", ""))
    try:
        kind = BackendKind(backend)
    except ValueError:
        raise ValueError(f"[[remote_storage]] 'type' must be 's3', 'azure', or 'smb', got {backend!r}") from None
    config = config_from_fields(kind, entry, check_required=False)
    secret_fields = secret_fields_for(kind)
    secrets = {field: str(entry[field]) for field in secret_fields if entry.get(field)}
    return RemoteStorageSample(
        name=str(entry["name"]),
        kind=kind,
        config=config,
        secrets=secrets,
        path=str(entry.get("path", "")),
        key=str(entry.get("key", "")),
    )


def load_smoke_samples() -> list[SampleEntry]:
    """Load ``tests/smoke/smoke_samples.toml``. Returns ``[]`` if the file
    doesn't exist -- every domain then skips every step with a reason
    pointing at ``smoke_samples.toml.example``.
    """
    if not _SAMPLES_PATH.exists():
        return []
    with open(_SAMPLES_PATH, "rb") as f:
        data = tomllib.load(f)
    entries: list[SampleEntry] = []
    for entry in data.get("local", []):
        _warn_unknown("local", entry, _LOCAL_FIELDS)
        entries.append(LocalSample(name=str(entry["name"]), path=str(entry["path"]), key=str(entry.get("key", ""))))
    for entry in data.get("profile", []):
        _warn_unknown("profile", entry, _PROFILE_FIELDS)
        entries.append(
            ProfileSample(
                name=str(entry["name"]),
                profile=str(entry["profile"]),
                path=str(entry.get("path", "")),
                key=str(entry.get("key", "")),
            )
        )
    for entry in data.get("remote_storage", []):
        _warn_unknown("remote_storage", entry, _REMOTE_STORAGE_FIELDS)
        entries.append(_parse_remote_storage(entry))
    return entries
