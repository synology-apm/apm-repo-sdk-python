"""Sample specification: reads ``tests/smoke/smoke_samples.toml`` (see
``smoke_samples.toml.example``), mirroring
``../apm-sdk-python/tests/smoke/_creds.py``'s ``smoke_creds.toml``
mechanism -- a committed ``.toml.example`` template, a gitignored real
file the developer copies it to and fills in, one array-of-tables block
per independently-optional data source.

Three sample kinds, one TOML array-of-tables each -- ``[[local]]``,
``[[profile]]``, ``[[remote_storage]]`` -- rather than one flat table with
mutually-exclusive optional fields, so each kind's own required fields are
enforced by its own dataclass instead of by validation code here.
``RemoteStorageSample`` deliberately reuses ``sdk``'s own
``S3ProfileConfig``/``AzureProfileConfig``/``SmbProfileConfig``/secret-field-name
constants (rather than re-declaring ``bucket``/``container``/``server``/...
itself) so it maps straight onto ``sdk.profiles.store_from_config()`` with
no translation layer of its own.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from synology_apm_repo.sdk import AzureProfileConfig, BackendKind, S3ProfileConfig, SmbProfileConfig
from synology_apm_repo.sdk.profiles.model import AZURE_SECRET_FIELDS, S3_SECRET_FIELDS, SMB_SECRET_FIELDS

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
    (created via ``synology-apm-repo-cli profile add``), resolved via
    ``profiles.build_store()`` at discovery time -- config-file + OS
    keyring, the same mechanism the CLI's ``--profile`` flag and the TUI's
    ``ConnectDialog`` profile picker both use. ``path``, when given, is a
    store-relative sub-path/prefix within that profile's bucket/container,
    not a filesystem path (mirrors the CLI's own ``--profile`` convention,
    see ``cli/strings.py``)."""

    name: str
    profile: str
    path: str = ""
    key: str = ""


@dataclass(frozen=True)
class RemoteStorageSample:
    """One ``[[remote_storage]]`` block: a live S3/Azure/SMB target built
    straight from credentials in this file, with no saved profile involved
    -- ``config``/``secrets`` feed directly into
    ``sdk.profiles.store_from_config()``, the same "config + secrets in
    hand" entry point ``profiles.build_store()`` itself calls after
    resolving a saved profile's own config/keyring.

    Not reopenable by a fresh CLI subprocess -- the real CLI has no
    raw-credential flag, only ``--profile <name>`` for a saved profile --
    so ``list_representative_refs(..., exclude_unreopenable_by_cli=True)``
    skips a ref derived from this kind."""

    name: str
    kind: BackendKind
    config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig
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


def _optional_str(entry: dict[str, Any], key: str) -> str | None:
    value = entry.get(key)
    return str(value) if value else None


def _parse_remote_storage(entry: dict[str, Any]) -> RemoteStorageSample:
    backend = str(entry.get("type", ""))
    if backend == "s3":
        kind = BackendKind.S3
        config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig = S3ProfileConfig(
            bucket=str(entry.get("bucket", "")),
            endpoint=_optional_str(entry, "endpoint"),
            region=_optional_str(entry, "region"),
            verify_tls=bool(entry.get("verify_tls", True)),
        )
        secret_fields: tuple[str, ...] = S3_SECRET_FIELDS
    elif backend == "azure":
        kind = BackendKind.AZURE
        config = AzureProfileConfig(
            container=str(entry.get("container", "")),
            account_url=_optional_str(entry, "account_url"),
            verify_tls=bool(entry.get("verify_tls", True)),
        )
        secret_fields = AZURE_SECRET_FIELDS
    elif backend == "smb":
        kind = BackendKind.SMB
        config = SmbProfileConfig(
            server=str(entry.get("server", "")),
            share=str(entry.get("share", "")),
            port=int(entry.get("port", 445) or 445),
            username=_optional_str(entry, "username"),
        )
        secret_fields = SMB_SECRET_FIELDS
    else:
        raise ValueError(f"[[remote_storage]] 'type' must be 's3', 'azure', or 'smb', got {backend!r}")
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
    pointing at ``smoke_samples.toml.example``, the same graceful-empty-
    report posture ``../apm-sdk-python/tests/smoke/_creds.py``'s own
    ``load_smoke_creds()`` has when ``smoke_creds.toml`` is absent.
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
