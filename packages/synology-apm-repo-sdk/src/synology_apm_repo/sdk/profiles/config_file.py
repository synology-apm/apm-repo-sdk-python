"""JSON persistence for profiles' non-secret fields, as synchronous file
I/O (``profiles/__init__.py`` moves it off the event loop).

One ``profiles.json`` shared by the CLI and TUI, under
``$XDG_CONFIG_HOME/synology-apm-repo`` (else ``~/.config/synology-apm-repo``)
on every platform.
"""

from __future__ import annotations

import dataclasses
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .errors import ProfileConfigCorruptError
from .model import BackendKind, Profile, config_from_json

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
    """The per-user directory holding ``profiles.json``."""
    return _xdg_config_home() / "synology-apm-repo"


def _config_path(config_dir: Path | None) -> Path:
    return (config_dir if config_dir is not None else default_config_dir()) / _FILE_NAME


def _decode_profile(name: str, raw: object) -> Profile:
    if not isinstance(raw, dict):
        raise ProfileConfigCorruptError(f"profile {name!r} is not a JSON object", ref=name)
    try:
        kind = BackendKind(raw["kind"])
    except (KeyError, ValueError) as exc:
        raise ProfileConfigCorruptError(f"profile {name!r} has an invalid or missing 'kind'", ref=name) from exc
    try:
        config = config_from_json(kind, raw)
    except KeyError as exc:
        raise ProfileConfigCorruptError(f"profile {name!r} is missing required field {exc}", ref=name) from exc
    except TypeError as exc:
        raise ProfileConfigCorruptError(f"profile {name!r} has a malformed field: {exc}", ref=name) from exc
    return Profile(name=name, config=config)


def _encode_profile(profile: Profile) -> dict[str, Any]:
    return {"kind": profile.kind.value, **dataclasses.asdict(profile.config)}


def read_profiles(*, config_dir: Path | None = None) -> dict[str, Profile]:
    """Every saved profile, keyed by name; none when the file is missing.

    Raises:
        ProfileConfigCorruptError: The file is unreadable, not valid JSON,
            of another schema version, or holds a malformed profile.
    """
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
    """Replace ``profiles.json`` with exactly ``profiles``, atomically, so a
    crash never leaves a truncated file."""
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
