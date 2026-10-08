"""Saved S3/Azure/SMB connection profiles, shared by the CLI and TUI: the
non-secret half in ``profiles.json``, secrets in the OS keyring. Outside
the layer stack: it builds ``ObjectStore`` instances and lists a backend's
buckets/containers through ``storage``, but reads no repository.

The I/O functions are async, running file and keyring access off the
event loop (a keyring prompt must not freeze the TUI). ``config_dir``
overrides the per-user config directory. ``get_profile`` and the listing
functions never touch the keyring, so they can't leak a secret;
``profile_fields_with_secrets`` and ``store_from_profile`` read it."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..storage.base import ObjectStore
from ..storage.smb import DEFAULT_SMB_PORT
from . import config_file, secrets
from .errors import (
    ProfileConfigCorruptError,
    ProfileFieldError,
    ProfileNotFoundError,
    ProfileSecretBackendUnavailableError,
)
from .model import (
    AzureProfileConfig,
    BackendKind,
    Profile,
    ProfileConfig,
    ProfileFieldSpec,
    S3ProfileConfig,
    SmbProfileConfig,
    client_kwargs_with_secrets,
    config_from_fields,
    form_fields_for,
    secret_fields_for,
)


def _non_secret_fields(config: ProfileConfig) -> dict[str, str | bool | int]:
    """``config``'s fields under their canonical names, minus unset
    (``None``) optional ones."""
    return {k: v for k, v in dataclasses.asdict(config).items() if v is not None}


def _get_profile_sync(name: str, config_dir: Path | None) -> Profile:
    profiles = config_file.read_profiles(config_dir=config_dir)
    try:
        return profiles[name]
    except KeyError:
        raise ProfileNotFoundError(f"no such profile: {name!r}", ref=name) from None


async def list_profiles(*, config_dir: Path | None = None) -> list[Profile]:
    """Every saved profile's non-secret half, sorted by name, from one read
    of ``profiles.json``.

    Raises:
        ProfileConfigCorruptError: ``profiles.json`` is unreadable or invalid.
    """
    profiles = await asyncio.to_thread(config_file.read_profiles, config_dir=config_dir)
    return sorted(profiles.values(), key=lambda p: p.name)


async def get_profile(name: str, *, config_dir: Path | None = None) -> Profile:
    """The non-secret half of profile ``name``, safe to display or log.

    Raises:
        ProfileNotFoundError: No such profile.
        ProfileConfigCorruptError: ``profiles.json`` is unreadable or invalid.
    """
    return await asyncio.to_thread(_get_profile_sync, name, config_dir)


async def profile_fields_with_secrets(name: str, *, config_dir: Path | None = None) -> dict[str, str | bool | int]:
    """Every field of profile ``name``, secrets included and unmasked,
    under the canonical field names, for refilling a connection form; use
    ``get_profile`` for anything displayed or logged.

    Raises:
        ProfileNotFoundError: No such profile.
        ProfileConfigCorruptError: ``profiles.json`` is unreadable or invalid.
        ProfileSecretBackendUnavailableError: No usable keyring.
    """
    profile = await get_profile(name, config_dir=config_dir)
    resolved_secrets = await asyncio.to_thread(secrets.get_secrets, name)
    return {**_non_secret_fields(profile.config), **resolved_secrets}


async def save_profile(
    name: str, kind: BackendKind, fields: Mapping[str, str | bool | int], *, config_dir: Path | None = None
) -> None:
    """Save profile ``name``, replacing an existing one's non-secret half;
    secrets are merged into those already stored (an empty one is left
    as stored). ``fields`` uses ``profile_fields_with_secrets``'s canonical names. The fields and the existing
    ``profiles.json`` are checked before anything is written, and secrets
    are written before ``profiles.json``, so a listed profile never lacks
    its secrets.

    Raises:
        ProfileFieldError: A required field is blank or malformed.
        ProfileConfigCorruptError: ``profiles.json`` is unreadable or invalid.
        ProfileSecretBackendUnavailableError: No usable keyring.
    """
    secret_field_names = secret_fields_for(kind)
    secret_values = {k: str(v) for k, v in fields.items() if k in secret_field_names and v}
    non_secret_fields = {k: v for k, v in fields.items() if k not in secret_field_names}
    config = config_from_fields(kind, non_secret_fields)

    def _write() -> None:
        profiles = config_file.read_profiles(config_dir=config_dir)
        secrets.set_secrets(name, secret_values)
        profiles[name] = Profile(name=name, config=config)
        config_file.write_profiles(profiles, config_dir=config_dir)

    await asyncio.to_thread(_write)


async def delete_profile(name: str, *, config_dir: Path | None = None) -> None:
    """Remove profile ``name`` and its keyring secrets; the
    ``profiles.json`` entry goes first.

    Raises:
        ProfileNotFoundError: No such profile.
        ProfileConfigCorruptError: ``profiles.json`` is unreadable or invalid.
        ProfileSecretBackendUnavailableError: No usable keyring (the
            profile is already removed from ``profiles.json``).
    """

    def _delete() -> None:
        profiles = config_file.read_profiles(config_dir=config_dir)
        try:
            profiles.pop(name)
        except KeyError:
            raise ProfileNotFoundError(f"no such profile: {name!r}", ref=name) from None
        config_file.write_profiles(profiles, config_dir=config_dir)
        secrets.delete_secrets(name)

    await asyncio.to_thread(_delete)


async def store_from_profile(name: str, *, config_dir: Path | None = None) -> ObjectStore:
    """Profile ``name`` with its keyring secrets, as a store for
    ``Session.discover()``/``open()``; the session then owns
    and closes it.

    Raises:
        ProfileNotFoundError: No such profile.
        ProfileConfigCorruptError: ``profiles.json`` is unreadable or invalid.
        ProfileSecretBackendUnavailableError: No usable keyring.
        ProfileFieldError: Azure rejected the saved ``account_url``.
    """
    profile = await get_profile(name, config_dir=config_dir)
    resolved_secrets = await asyncio.to_thread(secrets.get_secrets, name)
    return await store_from_config(profile.config, resolved_secrets)


async def store_from_config(config: ProfileConfig, secret_source: Mapping[str, object]) -> ObjectStore:
    """The ``ObjectStore`` for ``config`` with credentials from
    ``secret_source`` (see ``client_kwargs_with_secrets``), saved as a
    profile or not.

    Raises:
        ProfileFieldError: Azure rejected ``account_url`` (missing or
            malformed); S3 and SMB check their settings on first use.
    """
    return config.open_store(**client_kwargs_with_secrets(config, secret_source))


async def store_from_fields(kind: BackendKind, fields: Mapping[str, str | bool | int]) -> ObjectStore:
    """``store_from_config`` for a not-yet-saved connection form's raw
    ``fields`` (secrets included, canonical names).

    Raises:
        ProfileFieldError: A required field is blank or malformed, or (see
            ``store_from_config``) Azure rejected ``account_url``.
    """
    return await store_from_config(config_from_fields(kind, fields), fields)


def account_client_kwargs(kind: BackendKind, fields: Mapping[str, str | bool | int]) -> dict[str, Any]:
    """The account-level client kwargs (no bucket/container chosen yet) that
    ``list_remote_items`` takes, from a connection form's raw ``fields``."""
    return client_kwargs_with_secrets(config_from_fields(kind, fields, check_required=False), fields)


async def list_remote_items(kind: BackendKind, **client_kwargs: object) -> list[str]:
    """Every bucket (S3) or container (Azure) reachable with the resolved
    ``client_kwargs`` (see ``account_client_kwargs``).

    Raises:
        ValueError: ``kind`` is ``SMB``, which has no account-level listing.
    """
    from ..storage import list_buckets, list_containers

    if kind is BackendKind.S3:
        return await list_buckets(**client_kwargs)
    if kind is BackendKind.AZURE:
        return await list_containers(**client_kwargs)
    raise ValueError(f"list_remote_items() has no account-level listing for {kind.value!r}")


__all__ = [
    "DEFAULT_SMB_PORT",
    "AzureProfileConfig",
    "BackendKind",
    "Profile",
    "ProfileConfig",
    "ProfileConfigCorruptError",
    "ProfileFieldError",
    "ProfileFieldSpec",
    "ProfileNotFoundError",
    "ProfileSecretBackendUnavailableError",
    "S3ProfileConfig",
    "SmbProfileConfig",
    "account_client_kwargs",
    "config_from_fields",
    "delete_profile",
    "form_fields_for",
    "get_profile",
    "list_profiles",
    "list_remote_items",
    "profile_fields_with_secrets",
    "save_profile",
    "store_from_config",
    "store_from_fields",
    "store_from_profile",
]
