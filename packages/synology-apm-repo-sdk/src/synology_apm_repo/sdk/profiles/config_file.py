"""JSON persistence for a profile's non-secret fields.

Plain, synchronous file I/O — the same shape as ``LocalFsStore``'s own
syscalls, which is why the async boundary for this whole package lives
one layer up, in ``profiles/__init__.py``'s facade (wrapping calls here in
``asyncio.to_thread()``), not in this module itself.

One file, ``profiles.json``, under a per-user config directory shared by the
CLI and TUI alike: ``$XDG_CONFIG_HOME/synology-apm-repo`` (falling back
to ``~/.config/synology-apm-repo``) on every platform, not a
platform-native config directory — the three-distribution family name, not
this SDK distribution's own name, since both other distributions read/write
the exact same file. XDG-everywhere rather than a per-OS-native location
(``~/Library/Application Support/...`` on macOS, ``%APPDATA%`` on Windows),
down to "non-empty and absolute" being the only accepted override of the
default: this project's audience already runs other XDG-convention CLI
tooling and expects one predictable config location, not a platform-specific
one.
"""

from __future__ import annotations

import dataclasses
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from ..errors import ProfileConfigCorruptError
from .model import (
    AzureProfileConfig,
    BackendKind,
    Profile,
    S3ProfileConfig,
    SmbProfileConfig,
)

_SCHEMA_VERSION = 1
_FILE_NAME = "profiles.json"


def _xdg_config_home() -> Path:
    """``$XDG_CONFIG_HOME`` per the XDG Base Directory Specification: used
    only when set to a non-empty, absolute path; an unset, empty, or
    relative value falls back to ``~/.config``."""
    xdg = os.environ.get("XDG_CONFIG_HOME", "")
    if xdg and Path(xdg).is_absolute():
        return Path(xdg)
    return Path.home() / ".config"


def default_config_dir() -> Path:
    return _xdg_config_home() / "synology-apm-repo"


def _config_path(config_dir: Path | None) -> Path:
    return (config_dir if config_dir is not None else default_config_dir()) / _FILE_NAME


def _decode_profile(name: str, raw: dict[str, Any]) -> Profile:
    try:
        kind = BackendKind(raw["kind"])
    except (KeyError, ValueError) as exc:
        raise ProfileConfigCorruptError(f"profile {name!r} has an invalid or missing 'kind'", ref=name) from exc
    try:
        if kind is BackendKind.S3:
            config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig = S3ProfileConfig(
                bucket=raw["bucket"],
                endpoint=raw.get("endpoint"),
                region=raw.get("region"),
                verify_tls=raw.get("verify_tls", True),
            )
        elif kind is BackendKind.AZURE:
            config = AzureProfileConfig(
                container=raw["container"],
                account_url=raw.get("account_url"),
                verify_tls=raw.get("verify_tls", True),
            )
        else:
            config = SmbProfileConfig(
                server=raw["server"],
                share=raw["share"],
                port=raw.get("port", 445),
                username=raw.get("username"),
            )
    except KeyError as exc:
        raise ProfileConfigCorruptError(f"profile {name!r} is missing required field {exc}", ref=name) from exc
    return Profile(name=name, kind=kind, config=config)


def _encode_profile(profile: Profile) -> dict[str, Any]:
    return {"kind": profile.kind.value, **dataclasses.asdict(profile.config)}


def read_profiles(*, config_dir: Path | None = None) -> dict[str, Profile]:
    """Every saved profile, keyed by name. A missing file reads back as
    empty — there is nothing to migrate from, this is a brand-new
    mechanism."""
    path = _config_path(config_dir)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProfileConfigCorruptError(f"could not parse {path}: {exc}", ref=str(path)) from exc
    if not isinstance(raw, dict) or raw.get("schema_version") != _SCHEMA_VERSION:
        raise ProfileConfigCorruptError(f"unsupported or missing schema_version in {path}", ref=str(path))
    profiles_raw = raw.get("profiles", {})
    if not isinstance(profiles_raw, dict):
        raise ProfileConfigCorruptError(f"malformed 'profiles' object in {path}", ref=str(path))
    return {name: _decode_profile(name, entry) for name, entry in profiles_raw.items()}


def write_profiles(profiles: dict[str, Profile], *, config_dir: Path | None = None) -> None:
    """Overwrite ``profiles.json`` with exactly ``profiles``, atomically —
    a crash mid-write must never leave a truncated file behind, since every
    profile lookup depends on this one file surviving intact."""
    path = _config_path(config_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "profiles": {name: _encode_profile(profile) for name, profile in profiles.items()},
    }
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{_FILE_NAME}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise
