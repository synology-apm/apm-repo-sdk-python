"""Data model for saved S3/Azure/SMB connection profiles.

Profile fields go by one canonical name everywhere (CLI flags, TUI forms,
these dataclasses): ``bucket``/``endpoint``/``region``/``verify_tls``/...
Each config's ``client_kwargs`` translates them to its client's kwarg
names. Secret fields (S3's access/secret key, Azure's credential, SMB's
password) live in the OS keyring, never on these dataclasses;
``client_kwargs_with_secrets`` merges them in.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Callable, Mapping
from typing import Any, ClassVar

from ..storage.base import ObjectStore
from ..storage.smb import DEFAULT_SMB_PORT
from .errors import ProfileFieldError


class BackendKind(enum.StrEnum):
    """Which backend a profile targets; the value is also its CLI/TUI name."""

    S3 = "s3"
    AZURE = "azure"
    SMB = "smb"


@dataclasses.dataclass(frozen=True, slots=True)
class S3ProfileConfig:
    """Non-secret ``S3Store`` constructor inputs. ``access_key``/
    ``secret_key`` are not fields here — see ``secrets.py``."""

    kind: ClassVar[BackendKind] = BackendKind.S3

    bucket: str
    endpoint: str | None = None
    region: str | None = None
    verify_tls: bool = True

    def open_store(self, **client_kwargs: Any) -> ObjectStore:
        """The ``S3Store`` for this bucket, built with ``client_kwargs``
        (``client_kwargs_with_secrets``'s result)."""
        from ..storage import S3Store

        return S3Store(self.bucket, **client_kwargs)

    @property
    def client_kwargs(self) -> dict[str, Any]:
        """``S3Store`` kwargs for everything but ``bucket`` and the secrets."""
        kwargs: dict[str, Any] = {"verify": self.verify_tls}
        if self.endpoint is not None:
            kwargs["endpoint_url"] = self.endpoint
        if self.region is not None:
            kwargs["region_name"] = self.region
        return kwargs

    @property
    def display_fields(self) -> dict[str, str | None]:
        """Ordered field name -> value pairs the CLI and TUI display."""
        return {"bucket": self.bucket, "endpoint": self.endpoint, "region": self.region}

    @property
    def label(self) -> str:
        """``s3://<bucket>``, how CLI/TUI name the store this config opens."""
        return f"s3://{self.bucket}"


@dataclasses.dataclass(frozen=True, slots=True)
class AzureProfileConfig:
    """Non-secret ``AzureStore`` constructor inputs. ``credential`` is not
    a field here — see ``secrets.py``."""

    kind: ClassVar[BackendKind] = BackendKind.AZURE

    container: str
    account_url: str | None = None
    verify_tls: bool = True

    def open_store(self, **client_kwargs: Any) -> ObjectStore:
        """The ``AzureStore`` for this container; see
        ``S3ProfileConfig.open_store``.

        Raises:
            ProfileFieldError: Azure rejected ``account_url`` (missing or
                malformed).
        """
        from ..storage import AzureStore

        try:
            return AzureStore(self.container, **client_kwargs)
        except (TypeError, ValueError) as exc:
            # BlobServiceClient validates account_url while constructing.
            raise ProfileFieldError("account_url", f"invalid Azure account URL: {exc}") from exc

    @property
    def client_kwargs(self) -> dict[str, Any]:
        """``AzureStore`` kwargs for everything but ``container`` and the
        secret (TLS verification is ``connection_verify`` there)."""
        kwargs: dict[str, Any] = {"connection_verify": self.verify_tls}
        if self.account_url is not None:
            kwargs["account_url"] = self.account_url
        return kwargs

    @property
    def display_fields(self) -> dict[str, str | None]:
        """Same role as ``S3ProfileConfig.display_fields``."""
        return {"container": self.container, "account_url": self.account_url}

    @property
    def label(self) -> str:
        """``azure://<container>``; see ``S3ProfileConfig.label``."""
        return f"azure://{self.container}"


@dataclasses.dataclass(frozen=True, slots=True)
class SmbProfileConfig:
    """Non-secret ``SmbStore`` constructor inputs. ``password`` is not a
    field here — see ``secrets.py``. ``username`` may carry a domain as
    ``DOMAIN\\username`` or ``user@domain``; a profile names a whole
    share."""

    kind: ClassVar[BackendKind] = BackendKind.SMB

    server: str
    share: str
    port: int = DEFAULT_SMB_PORT
    username: str | None = None

    def open_store(self, **client_kwargs: Any) -> ObjectStore:
        """The ``SmbStore`` for this share; see ``S3ProfileConfig.open_store``."""
        from ..storage import SmbStore

        return SmbStore(self.share, **client_kwargs)

    @property
    def client_kwargs(self) -> dict[str, Any]:
        """``SmbStore`` kwargs for everything but ``share`` and the secret."""
        kwargs: dict[str, Any] = {"server": self.server, "port": self.port}
        if self.username is not None:
            kwargs["username"] = self.username
        return kwargs

    @property
    def display_fields(self) -> dict[str, str | int | None]:
        """Same role as ``S3ProfileConfig.display_fields``."""
        return {"server": self.server, "share": self.share, "port": self.port, "username": self.username}

    @property
    def label(self) -> str:
        """``smb://<server>/<share>``; see ``S3ProfileConfig.label``."""
        return f"smb://{self.server}/{self.share}"


ProfileConfig = S3ProfileConfig | AzureProfileConfig | SmbProfileConfig
"""Any backend's non-secret profile config."""


@dataclasses.dataclass(frozen=True, slots=True)
class Profile:
    """One saved profile, keyed by its user-chosen ``name`` (unique across
    every backend kind). Never carries secret values."""

    name: str
    config: S3ProfileConfig | AzureProfileConfig | SmbProfileConfig

    @property
    def kind(self) -> BackendKind:
        """The backend this profile targets, its config's ``kind``."""
        return self.config.kind


@dataclasses.dataclass(frozen=True, slots=True)
class ProfileFieldSpec:
    """One profile field, in canonical collection/display order for its
    backend: what a connection form binds and how ``config_from_fields``
    reads it.

    Attributes:
        name: The canonical field name.
        strip: Whether leading/trailing whitespace is dropped; ``False`` where
            it might be significant (``secret_key``/``credential``/``password``).
        is_checkbox: A boolean field, true when absent.
        is_port: A port number, ``DEFAULT_SMB_PORT`` when blank.
        required: Must be non-blank.
        secret: Kept in the OS keyring, never in ``profiles.json``.
        prompt: The interactive prompt for a secret field.
    """

    name: str
    strip: bool = True
    is_checkbox: bool = False
    is_port: bool = False
    required: bool = False
    secret: bool = False
    prompt: str = ""


_FORM_FIELDS_BY_KIND: dict[BackendKind, tuple[ProfileFieldSpec, ...]] = {
    BackendKind.S3: (
        ProfileFieldSpec("bucket", required=True),
        ProfileFieldSpec("endpoint"),
        ProfileFieldSpec("region"),
        ProfileFieldSpec("verify_tls", is_checkbox=True),
        ProfileFieldSpec("access_key", secret=True, prompt="Access key (blank for ambient credential chain)"),
        ProfileFieldSpec(
            "secret_key", strip=False, secret=True, prompt="Secret key (blank for ambient credential chain)"
        ),
    ),
    BackendKind.AZURE: (
        ProfileFieldSpec("container", required=True),
        ProfileFieldSpec("account_url"),
        ProfileFieldSpec("verify_tls", is_checkbox=True),
        ProfileFieldSpec(
            "credential",
            strip=False,
            secret=True,
            prompt="Credential — account key or SAS token (blank for ambient credential chain)",
        ),
    ),
    BackendKind.SMB: (
        ProfileFieldSpec("server", required=True),
        ProfileFieldSpec("share", required=True),
        ProfileFieldSpec("port", is_port=True),
        ProfileFieldSpec("username"),
        ProfileFieldSpec(
            "password", strip=False, secret=True, prompt="Password (blank for an anonymous/guest session)"
        ),
    ),
}

_CONFIG_CLS_BY_KIND: dict[BackendKind, Callable[..., ProfileConfig]] = {
    BackendKind.S3: S3ProfileConfig,
    BackendKind.AZURE: AzureProfileConfig,
    BackendKind.SMB: SmbProfileConfig,
}


def form_fields_for(kind: BackendKind) -> tuple[ProfileFieldSpec, ...]:
    """``kind``'s full field list, in canonical order — every field a
    connection form binds, secrets included."""
    return _FORM_FIELDS_BY_KIND[kind]


def secret_fields_for(kind: BackendKind) -> tuple[str, ...]:
    """Which field names are secret (keyring-backed) for ``kind`` — the
    rest are plain config, persisted to ``profiles.json``."""
    return tuple(field.name for field in _FORM_FIELDS_BY_KIND[kind] if field.secret)


def config_from_fields(
    kind: BackendKind, fields: Mapping[str, str | bool | int], *, check_required: bool = True
) -> ProfileConfig:
    """``fields`` (canonical names, as a connection form or the CLI collects
    them; extra keys, secrets included, are ignored) turned into ``kind``'s
    config. A blank optional field means "not set"; a blank port means
    ``DEFAULT_SMB_PORT``. ``check_required=False`` accepts blank required
    fields, for an account-level operation (listing buckets) that has none
    chosen yet.

    Raises:
        ProfileFieldError: A required field is blank, or the port is not a number.
    """
    values: dict[str, object] = {}
    for spec in _FORM_FIELDS_BY_KIND[kind]:
        if spec.secret:
            continue
        raw = fields.get(spec.name)
        if spec.is_checkbox:
            values[spec.name] = True if raw is None else bool(raw)
        elif spec.is_port:
            values[spec.name] = _port(raw)
        elif spec.required:
            if not raw and check_required:
                raise ProfileFieldError(spec.name, f"{spec.name} is required for a {kind} profile")
            values[spec.name] = str(raw) if raw else ""
        else:
            values[spec.name] = str(raw) if raw else None
    return _CONFIG_CLS_BY_KIND[kind](**values)


def _port(raw: object) -> int:
    if raw is None or raw == "":
        return DEFAULT_SMB_PORT
    try:
        return int(str(raw))
    except ValueError:
        raise ProfileFieldError("port", f"port must be a number, not {raw!r}") from None


def config_from_json(kind: BackendKind, raw: Mapping[str, Any]) -> ProfileConfig:
    """``kind``'s config from one saved ``profiles.json`` entry, strictly:
    nothing is coerced, so a hand-edited ``"false"`` is rejected rather
    than read as true.

    Raises:
        KeyError: A required field is missing.
        TypeError: A field has the wrong JSON type.
    """
    values: dict[str, object] = {}
    for spec in _FORM_FIELDS_BY_KIND[kind]:
        if spec.secret:
            continue
        expected: tuple[type, ...]
        if spec.required:
            value, expected = raw[spec.name], (str,)
        elif spec.is_checkbox:
            value, expected = raw.get(spec.name, True), (bool,)
        elif spec.is_port:
            value, expected = raw.get(spec.name, DEFAULT_SMB_PORT), (int,)
        else:
            value, expected = raw.get(spec.name), (str, type(None))
        if not isinstance(value, expected) or (expected == (int,) and isinstance(value, bool)):
            names = " or ".join(t.__name__ for t in expected)
            raise TypeError(f"{spec.name} must be {names}, not {value!r}")
        values[spec.name] = value
    return _CONFIG_CLS_BY_KIND[kind](**values)


#: Secret field name -> its store-constructor kwarg name, for every backend.
_SECRET_KWARG_NAMES: dict[str, str] = {
    "access_key": "aws_access_key_id",
    "secret_key": "aws_secret_access_key",
    "credential": "credential",
    "password": "password",
}


def client_kwargs_with_secrets(config: ProfileConfig, secrets: Mapping[str, object]) -> dict[str, Any]:
    """``config.client_kwargs`` plus each of its kind's secret fields that
    ``secrets`` holds as a non-empty string; other keys in ``secrets`` are
    ignored."""
    kind = config.kind
    kwargs = dict(config.client_kwargs)
    for field_name in secret_fields_for(kind):
        value = secrets.get(field_name)
        if value and isinstance(value, str):
            kwargs[_SECRET_KWARG_NAMES[field_name]] = value
    return kwargs
