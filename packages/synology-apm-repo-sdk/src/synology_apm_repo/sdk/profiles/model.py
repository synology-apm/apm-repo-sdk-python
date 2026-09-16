"""Data model for saved S3/Azure/SMB connection profiles.

One canonical, UI-facing field-naming scheme (``bucket``/``endpoint``/
``region``/``verify_tls``/... rather than ``aws_access_key_id``/
``region_name``/...) is used everywhere a profile's fields are named — CLI
flags, the TUI's saved/loaded field dicts, and the dataclasses here.
Translation to the third-party client's own kwarg names is isolated to each
config dataclass's ``client_kwargs`` property, never duplicated at each
call site.

Secret fields (S3's access/secret key, Azure's credential, SMB's password)
never appear on these dataclasses — they live in the OS keyring (see
``secrets.py``) and are merged back in only by ``profiles.build_store``.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Mapping
from typing import Any


class BackendKind(enum.Enum):
    """Which backend a profile targets."""

    S3 = "s3"
    AZURE = "azure"
    SMB = "smb"


@dataclasses.dataclass(frozen=True)
class S3ProfileConfig:
    """Non-secret ``S3Store`` constructor inputs. ``access_key``/
    ``secret_key`` are not fields here — see ``secrets.py``."""

    bucket: str
    endpoint: str | None = None
    region: str | None = None
    verify_tls: bool = True

    @property
    def client_kwargs(self) -> dict[str, Any]:
        """Everything but ``bucket`` and the secret fields, already in
        ``S3Store``/``aioboto3``'s own kwarg names. Callers merge the
        resolved secrets in on top of this dict."""
        kwargs: dict[str, Any] = {"verify": self.verify_tls}
        if self.endpoint is not None:
            kwargs["endpoint_url"] = self.endpoint
        if self.region is not None:
            kwargs["region_name"] = self.region
        return kwargs

    @property
    def display_fields(self) -> dict[str, str | None]:
        """Ordered field-name -> value pairs for CLI/TUI display (a saved
        profile's JSON and human-readable ``show`` output alike) — the one
        place this backend's own display field list is spelled out, so
        both renderers read it from here instead of each re-declaring
        it."""
        return {"bucket": self.bucket, "endpoint": self.endpoint, "region": self.region}


@dataclasses.dataclass(frozen=True)
class AzureProfileConfig:
    """Non-secret ``AzureStore`` constructor inputs. ``credential`` is not
    a field here — see ``secrets.py``."""

    container: str
    account_url: str | None = None
    verify_tls: bool = True

    @property
    def client_kwargs(self) -> dict[str, Any]:
        """Everything but ``container`` and the secret field, already in
        ``AzureStore``/``azure-storage-blob``'s own kwarg names — note
        ``connection_verify``, not ``verify`` (``azure.core.Configuration``'s
        own name for this, deliberately different from boto3's)."""
        kwargs: dict[str, Any] = {"connection_verify": self.verify_tls}
        if self.account_url is not None:
            kwargs["account_url"] = self.account_url
        return kwargs

    @property
    def display_fields(self) -> dict[str, str | None]:
        """Same role as ``S3ProfileConfig.display_fields`` — see its own
        docstring."""
        return {"container": self.container, "account_url": self.account_url}


@dataclasses.dataclass(frozen=True)
class SmbProfileConfig:
    """Non-secret ``SmbStore`` constructor inputs. ``password`` is not a
    field here — see ``secrets.py``.

    ``username`` takes the Windows-native ``DOMAIN\\username`` (or
    ``user@domain`` UPN) form directly — ``smbprotocol``'s own NTLM/SPNEGO
    layer splits the domain back out of that single string, so there is no
    separate ``domain`` field to keep in sync with it. No ``path``/sub-root
    field either, matching ``S3ProfileConfig``/``AzureProfileConfig``: a
    share, like a bucket/container, is the whole scope one profile names."""

    server: str
    share: str
    port: int = 445
    username: str | None = None

    @property
    def client_kwargs(self) -> dict[str, Any]:
        """Everything but ``share`` and the secret field, already in
        ``SmbStore``'s own kwarg names. Callers merge the resolved secret
        in on top of this dict."""
        kwargs: dict[str, Any] = {"server": self.server, "port": self.port}
        if self.username is not None:
            kwargs["username"] = self.username
        return kwargs

    @property
    def display_fields(self) -> dict[str, str | int | None]:
        """Ordered field-name -> value pairs for CLI/TUI display, same
        role as ``S3ProfileConfig.display_fields`` — see its own
        docstring."""
        return {"server": self.server, "share": self.share, "port": self.port, "username": self.username}


@dataclasses.dataclass(frozen=True)
class Profile:
    """One saved profile, keyed by its user-chosen ``name`` (unique across
    every backend kind). Never carries secret values."""

    name: str
    kind: BackendKind
    config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig


@dataclasses.dataclass(frozen=True)
class ProfileSummary:
    """Cheap listing entry — derived from ``profiles.json`` alone, no
    keyring access needed to produce it."""

    name: str
    kind: BackendKind


#: Keyring "username" per secret field, and the ``fields`` dict key
#: ``profiles.load_profile``/``save_profile`` use for it — see
#: ``secrets.py`` for how these map onto the underlying client's own
#: kwarg names.
S3_SECRET_FIELDS: tuple[str, ...] = ("access_key", "secret_key")
AZURE_SECRET_FIELDS: tuple[str, ...] = ("credential",)
SMB_SECRET_FIELDS: tuple[str, ...] = ("password",)

_SECRET_FIELDS_BY_KIND: dict[BackendKind, tuple[str, ...]] = {
    BackendKind.S3: S3_SECRET_FIELDS,
    BackendKind.AZURE: AZURE_SECRET_FIELDS,
    BackendKind.SMB: SMB_SECRET_FIELDS,
}


def secret_fields_for(kind: BackendKind) -> tuple[str, ...]:
    """Which ``fields`` dict keys are secret (keyring-backed) for ``kind`` —
    the rest are plain config, persisted to ``profiles.json``."""
    return _SECRET_FIELDS_BY_KIND[kind]


@dataclasses.dataclass(frozen=True)
class ProfileFieldSpec:
    """One profile field's widget-binding shape, in canonical collection/
    display order, for one backend — the single place a connection form
    (the TUI's connect/saved-profile tabs today) reads a backend's full
    field list from, instead of privately re-enumerating it. ``strip`` is
    ``False`` for a value whose leading/trailing whitespace might be
    significant (``secret_key``/``credential``/``password``) — not simply
    every secret field, since ``access_key`` (also keyring-backed, per
    ``secret_fields_for``) has no such concern and is stripped like an
    ordinary field."""

    name: str
    strip: bool = True
    is_checkbox: bool = False


#: Each backend's own field table — the CLI's per-backend prompt
#: collectors (``cli/commands/profile.py``) keep their own field lists
#: (each prompt has a distinct human label typer options don't carry, so
#: looping over this table there wouldn't save anything); this is for a
#: consumer that needs the field set/order and nothing else, one entry
#: per real widget a connection form binds.
S3_FORM_FIELDS: tuple[ProfileFieldSpec, ...] = (
    ProfileFieldSpec("bucket"),
    ProfileFieldSpec("endpoint"),
    ProfileFieldSpec("region"),
    ProfileFieldSpec("verify_tls", is_checkbox=True),
    ProfileFieldSpec("access_key"),
    ProfileFieldSpec("secret_key", strip=False),
)
#: No ``verify_tls`` entry — unlike S3, no Azure connection form in this
#: project has a ``verify_tls`` widget (``AzureProfileConfig.verify_tls``
#: only ever takes its dataclass default here), so this table stays an
#: explicit list rather than one derived from ``AzureProfileConfig``'s own
#: dataclass fields.
AZURE_FORM_FIELDS: tuple[ProfileFieldSpec, ...] = (
    ProfileFieldSpec("container"),
    ProfileFieldSpec("account_url"),
    ProfileFieldSpec("credential", strip=False),
)
SMB_FORM_FIELDS: tuple[ProfileFieldSpec, ...] = (
    ProfileFieldSpec("server"),
    ProfileFieldSpec("share"),
    ProfileFieldSpec("port"),
    ProfileFieldSpec("username"),
    ProfileFieldSpec("password", strip=False),
)
_FORM_FIELDS_BY_KIND: dict[BackendKind, tuple[ProfileFieldSpec, ...]] = {
    BackendKind.S3: S3_FORM_FIELDS,
    BackendKind.AZURE: AZURE_FORM_FIELDS,
    BackendKind.SMB: SMB_FORM_FIELDS,
}


def form_fields_for(kind: BackendKind) -> tuple[ProfileFieldSpec, ...]:
    """``kind``'s full field list, in canonical order — every field a
    connection form binds, secrets included, unlike ``display_fields``
    (non-secret only, read from an already-built config instance rather
    than named ahead of one existing)."""
    return _FORM_FIELDS_BY_KIND[kind]


#: Secret field name -> the underlying client's own kwarg name for it —
#: identity for Azure's ``credential``/SMB's ``password`` but not for S3's,
#: whose keyring field names (``access_key``/``secret_key``) differ from
#: ``aioboto3``'s own (``aws_access_key_id``/``aws_secret_access_key``).
#: Keyed the same way regardless of backend so ``client_kwargs_with_secrets``
#: can look up any field ``secret_fields_for`` names, for any kind, from
#: one dict.
_SECRET_KWARG_NAMES: dict[str, str] = {
    "access_key": "aws_access_key_id",
    "secret_key": "aws_secret_access_key",
    "credential": "credential",
    "password": "password",
}

_KIND_BY_CONFIG_TYPE: dict[type, BackendKind] = {
    S3ProfileConfig: BackendKind.S3,
    AzureProfileConfig: BackendKind.AZURE,
    SmbProfileConfig: BackendKind.SMB,
}


def client_kwargs_with_secrets(
    config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig, secrets: Mapping[str, str]
) -> dict[str, Any]:
    """``config.client_kwargs``, overlaid with whichever of
    ``secret_fields_for(kind)``'s fields ``secrets`` actually carries a
    truthy value for — the shared shape behind every "resolve one
    profile's config plus its secrets into constructor kwargs" call site
    (``profiles.build_store``, ``cli/commands/profile.py::_store_from_fields``,
    ``browser/screens/connect_dialog.py``'s ``_s3_client_kwargs``/
    ``_azure_client_kwargs`` — SMB has no bucket-less/container-less
    "Browse" counterpart calling this directly, only its own
    ``_smb_config_and_secrets`` feeding ``store_from_config``).
    ``secrets`` may be a superset of what applies here (a not-yet-saved
    connection form carries every backend's fields at once) — only the
    keys ``secret_fields_for`` names for ``config``'s own kind are ever
    read from it; a present-but-falsy value (an empty string from an
    untouched form field) is treated the same as absent, matching every
    prior hand-rolled version of this check."""
    kind = _KIND_BY_CONFIG_TYPE[type(config)]
    kwargs = dict(config.client_kwargs)
    for field_name in secret_fields_for(kind):
        value = secrets.get(field_name)
        if value:
            kwargs[_SECRET_KWARG_NAMES[field_name]] = value
    return kwargs
