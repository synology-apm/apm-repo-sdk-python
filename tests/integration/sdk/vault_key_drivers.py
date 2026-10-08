"""The ``vault-encrypted`` sample's vault key, for replay tests that decrypt
its recorded bytes."""

from __future__ import annotations

import base64

from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING, VAULT_ENCRYPTED_WRAPPED_B64
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.format.crypto import parse_key_string, unwrap_vault_key
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import iter_repository_layouts, key_probe_layout


def vault_encrypted_key() -> bytes:
    """The vault key unwrapped from the sample's committed constants, with no
    store access."""
    user_key_id, user_key = parse_key_string(VAULT_ENCRYPTED_KEY_STRING)
    return unwrap_vault_key(user_key_id, user_key, base64.b64decode(VAULT_ENCRYPTED_WRAPPED_B64))


async def resolve_vault_encrypted_key(store: ObjectStore) -> bytes:
    """The vault key resolved through ``store`` the way the anonymizer does,
    so a recording through ``store`` carries the ``resolve_vault_key()``
    probe an AHLT ``target.db`` fixture needs (see ``tests/CLAUDE.md``)."""
    repository = await anext(iter_repository_layouts(store))
    vault_key = await KeyMaterial.from_key_string(VAULT_ENCRYPTED_KEY_STRING).resolve_vault_key(
        store, key_probe_layout(repository)
    )
    assert vault_key == vault_encrypted_key()
    return vault_key
