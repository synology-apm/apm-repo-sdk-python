"""Unit tests for ``synology_apm_repo.sdk.dedup.pool_descriptor``'s
``PoolDescriptor`` repr."""

from __future__ import annotations

from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor
from synology_apm_repo.sdk.storage.local import LocalFsStoreDescriptor


def test_repr_redacts_the_vault_key() -> None:
    descriptor = PoolDescriptor(LocalFsStoreDescriptor(root="/r"), pool_root="pool", vault_key=b"\x01" * 32)
    assert repr(b"\x01" * 32) not in repr(descriptor)
    assert "vault_key=<redacted>" in repr(descriptor)


def test_repr_shows_an_absent_vault_key_as_none() -> None:
    descriptor = PoolDescriptor(LocalFsStoreDescriptor(root="/r"), pool_root="pool", vault_key=None)
    assert "vault_key=None" in repr(descriptor)
