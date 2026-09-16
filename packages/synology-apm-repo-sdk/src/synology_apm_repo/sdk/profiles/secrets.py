"""OS-keyring-backed secret storage for a profile's credential fields.

``keyring`` is imported lazily, inside each function, matching
``storage/s3.py``'s/``storage/azure.py``'s own lazy-import convention:
``keyring`` is always installed (a required dependency), but a caller that
only lists/reads non-secret profile metadata shouldn't pay for importing it.

No plaintext fallback exists anywhere in this module, by design — if no real
keyring backend is available, every function here raises
``ProfileSecretBackendUnavailableError`` rather than writing a secret to disk or
silently discarding it.

Grouping scheme: one keyring item per profile — service name
``f"{_SERVICE_PREFIX}/{profile_name}"`` (visibly groups a profile's own row in
Keychain/Credential Manager/Secret Service), fixed username
(``_SECRET_USERNAME``), and a JSON object of every secret field currently set
(``access_key``/``secret_key``/``credential``/``password``, depending on
backend) as the item's password. Because every field shares one item,
``set_secrets`` must read-modify-write: it merges its caller's fields onto
whatever's already stored rather than overwriting the whole item, so setting
one field never clobbers a sibling field set earlier on the same profile.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Mapping
from types import ModuleType

from ..errors import ProfileSecretBackendUnavailableError

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
    """Merge ``secrets`` (a subset of some ``BackendKind``'s
    ``secret_fields_for(kind)``, values already resolved — an absent field
    is simply not written) into ``profile_name``'s single keyring item,
    on top of whatever fields are already stored there. ``profiles/
    __init__.py::save_profile`` is the sole caller and already pre-filters
    to ``secret_fields_for(kind)`` before calling, so no field this doesn't
    already know how to store could reach it."""
    keyring = _require_keyring()
    service = _service_name(profile_name)
    merged = {**_read_blob(keyring, service), **secrets}
    keyring.set_password(service, _SECRET_USERNAME, json.dumps(merged))


def get_secrets(profile_name: str) -> dict[str, str]:
    """Every secret field currently stored for ``profile_name`` — a field
    with no stored value (never set, or the ambient-credential-chain case)
    is simply absent from the returned dict, not an error."""
    keyring = _require_keyring()
    return _read_blob(keyring, _service_name(profile_name))


def delete_secrets(profile_name: str) -> None:
    """Remove every secret field stored for ``profile_name``, in one item.
    A profile that never had one (``PasswordDeleteError``) is not an error
    here — the end state ("no secret stored") is already what was asked
    for."""
    keyring = _require_keyring()
    service = _service_name(profile_name)
    with contextlib.suppress(keyring.errors.PasswordDeleteError):
        keyring.delete_password(service, _SECRET_USERNAME)
