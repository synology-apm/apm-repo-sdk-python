"""Unit tests for ``synology_apm_repo.sdk.dedup.keys`` against synthetic
VAULT and OBJECT_STORE layouts."""

from __future__ import annotations

import base64
import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from support.repo_builders import write_vault_encryption_key_db
from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification, probe_encrypted
from synology_apm_repo.sdk.errors import DataCorruptError, KeyMismatchError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore

_USER_KEY_ID = "abcdefghijkl"  # exactly 12 characters


def _wrap(user_key_id: str, user_key: bytes, vault_key: bytes) -> bytes:
    nonce = user_key_id.encode("ascii")[:12]
    return AESGCM(user_key).encrypt(nonce, vault_key, None)


# -- resolve_vault_key -------------------------------------------------


class TestResolveVaultKeyFromVaultDb:
    async def test_correct_key_resolves(self, tmp_path: Path) -> None:
        user_key = os.urandom(32)
        vault_key = os.urandom(32)
        wrapped = _wrap(_USER_KEY_ID, user_key, vault_key)
        write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key",
            [(_USER_KEY_ID, base64.b64encode(wrapped).decode())],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=user_key)
        assert await km.resolve_vault_key(store, layout) == vault_key

    async def test_missing_row_returns_none(self, tmp_path: Path) -> None:
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        assert await km.resolve_vault_key(store, layout) is None

    async def test_missing_db_file_returns_none(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        assert await km.resolve_vault_key(store, layout) is None

    async def test_wrong_user_key_raises_key_mismatch(self, tmp_path: Path) -> None:
        correct_user_key = os.urandom(32)
        vault_key = os.urandom(32)
        wrapped = _wrap(_USER_KEY_ID, correct_user_key, vault_key)
        write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key",
            [(_USER_KEY_ID, base64.b64encode(wrapped).decode())],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km_wrong = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        with pytest.raises(KeyMismatchError, match="AES-256-GCM tag check failed"):
            await km_wrong.resolve_vault_key(store, layout)

    async def test_corrupt_db_file_raises_data_corrupt(self, tmp_path: Path) -> None:
        """A half-written vault's garbage key db is a ``DataCorruptError``,
        not a raw ``sqlite3`` error: ``api/session.py``'s
        ``_open_repository`` skips a layout only on an ``ApmRepoError``."""
        db_path = tmp_path / "db" / "vault_encryption_key"
        db_path.parent.mkdir(parents=True)
        db_path.write_bytes(b"not a real sqlite database")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        with pytest.raises(DataCorruptError, match="is not a readable vault_encryption_key database"):
            await km.resolve_vault_key(store, layout)


class TestResolveVaultKeyFromKeyFile:
    async def test_correct_key_resolves(self, tmp_path: Path) -> None:
        user_key = os.urandom(32)
        vault_key = os.urandom(32)
        wrapped = _wrap(_USER_KEY_ID, user_key, vault_key)
        key_dir = tmp_path / "@ActiveProtectKey" / "userKey"
        key_dir.mkdir(parents=True)
        (key_dir / _USER_KEY_ID).write_text(base64.b64encode(wrapped).decode())
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=user_key)
        assert await km.resolve_vault_key(store, layout) == vault_key

    async def test_no_key_root_returns_none(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root=None)
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        assert await km.resolve_vault_key(store, layout) is None

    async def test_missing_key_file_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "@ActiveProtectKey" / "userKey").mkdir(parents=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        assert await km.resolve_vault_key(store, layout) is None

    async def test_corrupt_key_file_raises_data_corrupt(self, tmp_path: Path) -> None:
        """The OBJECT_STORE counterpart of
        ``TestResolveVaultKeyFromVaultDb.test_corrupt_db_file_raises_data_corrupt``:
        no raw ``UnicodeDecodeError``/``binascii.Error``."""
        key_dir = tmp_path / "@ActiveProtectKey" / "userKey"
        key_dir.mkdir(parents=True)
        (key_dir / _USER_KEY_ID).write_bytes(b"\xff\xfe\xfd\xfc")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        with pytest.raises(DataCorruptError, match="is not a readable userKey file"):
            await km.resolve_vault_key(store, layout)


# -- probe_encrypted ---------------------------------------------------


class TestProbeEncryptedVault:
    async def test_no_encryption_row_gives_false(self, tmp_path: Path) -> None:
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        assert await probe_encrypted(store, layout) is False

    async def test_real_key_row_gives_true(self, tmp_path: Path) -> None:
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [(_USER_KEY_ID, "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        assert await probe_encrypted(store, layout) is True

    async def test_uses_the_last_inserted_row_not_the_first(self, tmp_path: Path) -> None:
        # Not a real history (encryption can't change after init); pins
        # "last-inserted row" over whichever row an unordered SELECT returns.
        write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key", [(_USER_KEY_ID, ""), ("NoEncryption", "")]
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        assert await probe_encrypted(store, layout) is False

    async def test_missing_db_file_gives_none(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        assert await probe_encrypted(store, layout) is None

    async def test_db_file_present_but_table_empty_gives_none(self, tmp_path: Path) -> None:
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        assert await probe_encrypted(store, layout) is None

    async def test_corrupt_db_file_raises_data_corrupt(self, tmp_path: Path) -> None:
        db_path = tmp_path / "db" / "vault_encryption_key"
        db_path.parent.mkdir(parents=True)
        db_path.write_bytes(b"not a real sqlite database")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        with pytest.raises(DataCorruptError, match="is not a readable vault_encryption_key database"):
            await probe_encrypted(store, layout)


class TestProbeEncryptedObjectStore:
    async def test_no_encryption_marker_gives_false(self, tmp_path: Path) -> None:
        key_dir = tmp_path / "@ActiveProtectKey" / "userKey"
        key_dir.mkdir(parents=True)
        (key_dir / "NoEncryption").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        assert await probe_encrypted(store, layout) is False

    async def test_real_key_object_gives_true(self, tmp_path: Path) -> None:
        key_dir = tmp_path / "@ActiveProtectKey" / "userKey"
        key_dir.mkdir(parents=True)
        (key_dir / _USER_KEY_ID).write_text("wrapped-key-bytes-not-checked-here")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        assert await probe_encrypted(store, layout) is True

    async def test_init_prefixed_sibling_object_does_not_confuse_the_check(self, tmp_path: Path) -> None:
        # Real shape: an "init@<userKeyID>" object can sit beside
        # "<userKeyID>" itself.
        key_dir = tmp_path / "@ActiveProtectKey" / "userKey"
        key_dir.mkdir(parents=True)
        (key_dir / _USER_KEY_ID).write_text("wrapped-key-bytes-not-checked-here")
        (key_dir / f"init@{_USER_KEY_ID}").write_text("unrelated-shorter-value")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        assert await probe_encrypted(store, layout) is True

    @pytest.mark.parametrize(
        "key_root",
        [pytest.param(None, id="no_key_root"), pytest.param("@ActiveProtectKey", id="missing_user_key_dir")],
    )
    async def test_gives_none_without_a_user_key_dir(self, tmp_path: Path, key_root: str | None) -> None:
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root=key_root)
        assert await probe_encrypted(store, layout) is None

    async def test_empty_user_key_dir_gives_none(self, tmp_path: Path) -> None:
        (tmp_path / "@ActiveProtectKey" / "userKey").mkdir(parents=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        assert await probe_encrypted(store, layout) is None


# -- verify(): the GCM unwrap alone decides; no chunk is decrypted ------


class TestVerify:
    async def test_no_encryption_short_circuits(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)  # nothing written at all
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id="NoEncryption", user_key=b"\x00" * 32)
        result = await km.verify(store, layout)
        assert result.gcm_ok is True
        assert result.ok is True

    async def test_correct_key_succeeds_with_no_pool_written_at_all(self, tmp_path: Path) -> None:
        user_key = os.urandom(32)
        vault_key = os.urandom(32)
        wrapped = _wrap(_USER_KEY_ID, user_key, vault_key)
        write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key",
            [(_USER_KEY_ID, base64.b64encode(wrapped).decode())],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=user_key)

        result = await km.verify(store, layout)
        assert result.ok is True
        assert result.gcm_ok is True
        assert result.vault_key == vault_key

    async def test_wrong_user_key_fails_at_gcm_layer(self, tmp_path: Path) -> None:
        correct_user_key = os.urandom(32)
        vault_key = os.urandom(32)
        wrapped = _wrap(_USER_KEY_ID, correct_user_key, vault_key)
        write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key",
            [(_USER_KEY_ID, base64.b64encode(wrapped).decode())],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km_wrong = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))

        result = await km_wrong.verify(store, layout)
        assert result.gcm_ok is False
        assert result.vault_key is None
        assert result.ok is False

    async def test_missing_wrapped_key_gives_gcm_false_not_key_mismatch(self, tmp_path: Path) -> None:
        write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))

        result = await km.verify(store, layout)
        assert result.gcm_ok is False
        assert result.ok is False


def test_repr_redacts_user_key() -> None:
    km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=b"supersecretkeymaterial32bytes!!!")
    text = repr(km)
    assert "supersecret" not in text
    assert _USER_KEY_ID in text
    assert "redacted" in text


def test_key_verification_repr_redacts_vault_key() -> None:
    verification = KeyVerification(gcm_ok=True, vault_key=b"supersecretvaultkeymaterial32by!")
    text = repr(verification)
    assert "supersecret" not in text
    assert "redacted" in text
    assert "gcm_ok=True" in text


def test_key_verification_repr_shows_none_when_no_vault_key() -> None:
    verification = KeyVerification(gcm_ok=True, vault_key=None)
    text = repr(verification)
    assert "vault_key=None" in text
    assert "redacted" not in text
