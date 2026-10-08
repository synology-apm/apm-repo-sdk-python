"""OS-keyring-backed secret storage for a profile's credential fields.

``keyring`` is imported lazily so a caller that only reads non-secret
profile metadata doesn't pay for importing it. Without a usable keyring
backend every function raises ``ProfileSecretBackendUnavailableError``;
secrets are never written anywhere else.

Each profile has one keyring item: service
``f"{_SERVICE_PREFIX}/{profile_name}"``, username ``_SECRET_USERNAME``, and
as its password a JSON object of every secret field set
(``access_key``/``secret_key``/``credential``/``password``, by backend).
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Mapping
from types import ModuleType

from .errors import ProfileSecretBackendUnavailableError

_SERVICE_PREFIX = "synology-apm-repo/profile"
_SECRET_USERNAME = "secrets"


def _service_name(profile_name: str) -> str:
    return f"{_SERVICE_PREFIX}/{profile_name}"


def _require_keyring() -> ModuleType:
    try:
        import keyring
        import keyring.backends.fail
        import keyring.errors
    except ImportError as exc:
        raise ProfileSecretBackendUnavailableError(
            "keyring failed to import - check that this environment's install isn't broken/partial"
        ) from exc

    backend = keyring.get_keyring()
    if isinstance(backend, keyring.backends.fail.Keyring):
        raise ProfileSecretBackendUnavailableError(
            "no usable OS keyring backend is available (Keychain/Credential Manager/Secret Service) - "
            "on headless Linux, install a D-Bus Secret Service provider or 'keyrings.alt' as a fallback backend"
        )
    return keyring


def _read_blob(keyring: ModuleType, service: str) -> dict[str, str]:
    raw = keyring.get_password(service, _SECRET_USERNAME)
    return {} if raw is None else json.loads(raw)


def set_secrets(profile_name: str, secrets: Mapping[str, str]) -> None:
    """Merge ``secrets`` (secret field names, as ``secret_fields_for``
    lists them) into ``profile_name``'s keyring item; fields already stored
    and absent from ``secrets`` are kept."""
    keyring = _require_keyring()
    service = _service_name(profile_name)
    merged = {**_read_blob(keyring, service), **secrets}
    keyring.set_password(service, _SECRET_USERNAME, json.dumps(merged))


def get_secrets(profile_name: str) -> dict[str, str]:
    """Every secret field stored for ``profile_name``; a field never set
    is absent."""
    keyring = _require_keyring()
    return _read_blob(keyring, _service_name(profile_name))


def delete_secrets(profile_name: str) -> None:
    """Remove every secret field stored for ``profile_name``; a profile
    with none stored is not an error."""
    keyring = _require_keyring()
    service = _service_name(profile_name)
    with contextlib.suppress(keyring.errors.PasswordDeleteError):
        keyring.delete_password(service, _SECRET_USERNAME)
