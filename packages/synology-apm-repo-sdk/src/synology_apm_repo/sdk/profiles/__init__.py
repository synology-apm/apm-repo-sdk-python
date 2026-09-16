"""Saved S3/Azure/SMB connection profiles — shared, identically, by the CLI and
TUI, exactly like ``presentation/`` is shared for render-identically output.

This is cross-cutting infrastructure *outside* the layer stack: it
produces inputs to ``storage/``'s ``S3Store``/``AzureStore``, it does no
``ObjectStore`` I/O itself, and it has nothing to do with ``Session``/keys/
``NodeRef``.

Every function below is ``async def`` and a plain module-level function (not
a class method) — async because the underlying work (``profiles.json`` file
I/O, OS keyring access) is exactly the kind of blocking call
``LocalFsStore`` already wraps in ``asyncio.to_thread()`` (there is no
async-native alternative, same as ``pread``/``fstat``), and blocking a TUI's
event loop on a keyring prompt would freeze the whole UI; plain functions so
tests (and callers) can ``monkeypatch.setattr(this_module, "list_profiles",
fake)`` exactly like ``storage.s3.list_buckets`` already supports. A
``config_dir`` keyword lets tests point at a temp directory instead of the
real per-user config path, standing in for constructor-based dependency
injection.

Two secret-aware entry points read differently, deliberately:
``get_profile()`` never touches the keyring (so ``list``/``show``-shaped
callers structurally cannot leak a secret), while ``load_profile()`` does,
returning every field flattened into one dict for refilling a connection
form."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Mapping
from pathlib import Path

from ..errors import ProfileNotFoundError
from ..storage.base import ObjectStore
from . import config_file, secrets
from .model import (
    AzureProfileConfig,
    BackendKind,
    Profile,
    ProfileFieldSpec,
    ProfileSummary,
    S3ProfileConfig,
    SmbProfileConfig,
    client_kwargs_with_secrets,
    form_fields_for,
    secret_fields_for,
)


def _non_secret_fields(config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig) -> dict[str, str | bool | int]:
    """Every one of ``config``'s fields (already exactly the non-secret,
    canonical field names ``load_profile``/``save_profile`` use — see
    ``S3ProfileConfig``/``AzureProfileConfig``/``SmbProfileConfig``'s own
    docstrings) except a ``None`` optional (``endpoint``/``region``/
    ``account_url``/``username``), which means "never set"."""
    return {k: v for k, v in dataclasses.asdict(config).items() if v is not None}


def _optional_str(fields: Mapping[str, str | bool | int], key: str) -> str | None:
    """``None`` for both "key absent" and "blank string" — a blank optional
    field means the same thing as never setting it."""
    value = fields.get(key)
    return str(value) if value else None


def config_from_fields(
    kind: BackendKind, fields: Mapping[str, str | bool | int]
) -> S3ProfileConfig | AzureProfileConfig | SmbProfileConfig:
    """``fields`` (``save_profile()``'s canonical field names, e.g. as
    collected by a connection form or the CLI's ``profile add`` prompts)
    turned into ``kind``'s own config dataclass — the one place this
    per-backend branch is spelled out, so ``save_profile`` and any
    caller building a not-yet-saved config from raw fields
    (``cli/commands/profile.py``'s pre-persist connectivity check) share
    it instead of each re-deriving their own."""
    if kind is BackendKind.S3:
        return S3ProfileConfig(
            bucket=str(fields["bucket"]),
            endpoint=_optional_str(fields, "endpoint"),
            region=_optional_str(fields, "region"),
            verify_tls=bool(fields.get("verify_tls", True)),
        )
    if kind is BackendKind.AZURE:
        return AzureProfileConfig(
            container=str(fields["container"]),
            account_url=_optional_str(fields, "account_url"),
            verify_tls=bool(fields.get("verify_tls", True)),
        )
    return SmbProfileConfig(
        server=str(fields["server"]),
        share=str(fields["share"]),
        port=int(fields.get("port", 445) or 445),
        username=_optional_str(fields, "username"),
    )


def _get_profile_sync(name: str, config_dir: Path | None) -> Profile:
    profiles = config_file.read_profiles(config_dir=config_dir)
    try:
        return profiles[name]
    except KeyError:
        raise ProfileNotFoundError(f"no such profile: {name!r}", ref=name) from None


async def list_profiles(*, config_dir: Path | None = None) -> list[ProfileSummary]:
    """Every saved profile's name and backend kind, sorted by name. Never
    touches the keyring."""
    profiles = await asyncio.to_thread(config_file.read_profiles, config_dir=config_dir)
    return [ProfileSummary(name=p.name, kind=p.kind) for p in sorted(profiles.values(), key=lambda p: p.name)]


async def get_profile(name: str, *, config_dir: Path | None = None) -> Profile:
    """The non-secret half of profile ``name``.

    Never touches the keyring — safe for ``--json``/log output by
    construction, not by redacting something that was fetched.

    Raises:
        ProfileNotFoundError: No such profile.
    """
    return await asyncio.to_thread(_get_profile_sync, name, config_dir)


async def load_profile(name: str, *, config_dir: Path | None = None) -> dict[str, str | bool | int]:
    """Every field of profile ``name``, secrets included and unmasked, flat
    in the canonical field names (``bucket``/``endpoint``/``region``/
    ``verify_tls``/``access_key``/``secret_key`` for S3, ``container``/
    ``account_url``/``verify_tls``/``credential`` for Azure, ``server``/
    ``share``/``port``/``username``/``password`` for SMB) — for
    refilling a connection form. Never used for display/logging: a caller
    that only needs to name or describe a profile should use
    ``get_profile`` instead."""
    profile = await get_profile(name, config_dir=config_dir)
    resolved_secrets = await asyncio.to_thread(secrets.get_secrets, name)
    return {**_non_secret_fields(profile.config), **resolved_secrets}


async def save_profile(
    name: str, kind: BackendKind, fields: Mapping[str, str | bool | int], *, config_dir: Path | None = None
) -> None:
    """Save (or overwrite — this is an upsert, no collision check) a
    profile under ``name``. ``fields`` uses the same canonical names
    ``load_profile`` returns; secret fields are split out and written to
    the keyring *before* the non-secret half is committed to
    ``profiles.json``, so a profile visible in ``profiles.json`` never
    references secrets that were never actually written."""
    secret_field_names = secret_fields_for(kind)
    secret_values = {k: str(v) for k, v in fields.items() if k in secret_field_names and v}
    non_secret_fields = {k: v for k, v in fields.items() if k not in secret_field_names}

    def _write() -> None:
        secrets.set_secrets(name, secret_values)
        profiles = config_file.read_profiles(config_dir=config_dir)
        profiles[name] = Profile(name=name, kind=kind, config=config_from_fields(kind, non_secret_fields))
        config_file.write_profiles(profiles, config_dir=config_dir)

    await asyncio.to_thread(_write)


async def delete_profile(name: str, *, config_dir: Path | None = None) -> None:
    """Remove profile ``name`` entirely.

    The ``profiles.json`` entry is removed *before* the best-effort
    keyring cleanup, so a later ``save_profile()`` re-using this name never
    silently inherits stale secrets.

    Raises:
        ProfileNotFoundError: No such profile.
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


async def build_store(name: str, *, config_dir: Path | None = None) -> ObjectStore:
    """The end-to-end convenience form: profile ``name``'s config and
    keyring secrets, resolved into an already-constructed ``S3Store``/
    ``AzureStore``/``SmbStore`` via ``store_from_config`` — ready for
    ``Session.discover_remote()``/``open_remote()``. The caller still owns
    calling ``Session.close()``, which calls ``aclose()`` on it."""
    profile = await get_profile(name, config_dir=config_dir)
    resolved_secrets = await asyncio.to_thread(secrets.get_secrets, name)
    return await store_from_config(profile.kind, profile.config, resolved_secrets)


async def store_from_config(
    kind: BackendKind,
    config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig,
    secret_source: Mapping[str, str],
) -> ObjectStore:
    """``config``'s fields and ``secret_source``'s raw credentials, merged
    via ``client_kwargs_with_secrets`` and handed to the matching
    ``ObjectStore`` constructor — the one translation ``build_store``,
    ``cli/commands/profile.py``'s pre-persist connectivity check, and the
    TUI's connect dialog all need, whether or not ``config`` has ever been
    saved as a profile."""
    # Imported here, not at module scope, so a caller that only lists/
    # reads/deletes profile metadata never pays for importing storage's
    # S3/Azure/SMB machinery - mirrors storage/__init__.py importing
    # S3Store/AzureStore/SmbStore unconditionally while their own
    # aioboto3/azure-storage-blob/smbprotocol imports stay lazy inside each
    # class.
    from ..storage import AzureStore, S3Store, SmbStore

    kwargs = client_kwargs_with_secrets(config, secret_source)
    if kind is BackendKind.S3:
        assert isinstance(config, S3ProfileConfig)
        return S3Store(config.bucket, **kwargs)
    if kind is BackendKind.AZURE:
        assert isinstance(config, AzureProfileConfig)
        return AzureStore(config.container, **kwargs)
    assert isinstance(config, SmbProfileConfig)
    return SmbStore(config.share, **kwargs)


async def list_remote_items(kind: BackendKind, **client_kwargs: object) -> list[str]:
    """Every bucket (S3) or container (Azure) reachable with
    ``client_kwargs`` (already resolved, e.g. via
    ``client_kwargs_with_secrets``) — the same account-level listing the
    TUI's connect dialog uses to populate its "browse buckets/containers"
    picker before a bucket/container name is chosen. No SMB equivalent:
    unlike an S3/Azure account, an SMB server has no single account-level
    "list every share" operation this backend relies on, so ``kind is
    BackendKind.SMB`` raises rather than silently falling through to one
    of the other two backends' own listing call."""
    from ..storage import list_buckets, list_containers

    if kind is BackendKind.S3:
        return await list_buckets(**client_kwargs)
    if kind is BackendKind.AZURE:
        return await list_containers(**client_kwargs)
    raise ValueError(f"list_remote_items() has no account-level listing for {kind.value!r}")


__all__ = [
    "AzureProfileConfig",
    "BackendKind",
    "Profile",
    "ProfileFieldSpec",
    "ProfileSummary",
    "S3ProfileConfig",
    "SmbProfileConfig",
    "build_store",
    "client_kwargs_with_secrets",
    "config_from_fields",
    "delete_profile",
    "form_fields_for",
    "get_profile",
    "list_profiles",
    "list_remote_items",
    "load_profile",
    "save_profile",
    "store_from_config",
]
