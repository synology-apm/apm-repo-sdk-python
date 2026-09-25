"""Unit tests for ``synology_apm_repo.sdk.dedup.keys`` — synthetic
VAULT and OBJECT_STORE layouts and a real ``vault_encryption_key``
sqlite table, all written to real files.

Per-chunk fingerprint verification (``Pool.read_chunk``'s
``verify_fingerprint`` parameter) is a separate concern, covered by
``test_dedup_pool.py::TestVerifyFingerprint``."""

from __future__ import annotations

import base64
import os
import sqlite3
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from synology_apm_repo.sdk.dedup.keys import KeyMaterial, KeyVerification, probe_encrypted
from synology_apm_repo.sdk.errors import DataCorruptError, KeyMismatchError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore

_USER_KEY_ID = "abcdefghijkl"  # exactly 12 characters


# -- synthetic file builders (self-contained, same approach as
#    test_dedup_pool.py / test_dedup_fingerprint.py) -------------------


def _write_vault_encryption_key_db(path: Path, rows: list[tuple[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE vault_encryption_key(user_key_uuid TEXT UNIQUE NOT NULL, "
        "encrypted_data_key TEXT, crtime DATETIME DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.executemany("INSERT INTO vault_encryption_key(user_key_uuid, encrypted_data_key) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _wrap(user_key_id: str, user_key: bytes, vault_key: bytes) -> bytes:
    nonce = user_key_id.encode("ascii")[:12]
    return AESGCM(user_key).encrypt(nonce, vault_key, None)


# -- resolve_vault_key -------------------------------------------------


class TestResolveVaultKeyFromVaultDb:
    async def test_correct_key_resolves(self, tmp_path: Path) -> None:
        user_key = os.urandom(32)
        vault_key = os.urandom(32)
        wrapped = _wrap(_USER_KEY_ID, user_key, vault_key)
        _write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key",
            [(_USER_KEY_ID, base64.b64encode(wrapped).decode())],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=user_key)
        assert await km.resolve_vault_key(store, layout) == vault_key

    async def test_missing_row_returns_none(self, tmp_path: Path) -> None:
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
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
        _write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key",
            [(_USER_KEY_ID, base64.b64encode(wrapped).decode())],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km_wrong = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        with pytest.raises(KeyMismatchError):
            await km_wrong.resolve_vault_key(store, layout)

    async def test_corrupt_db_file_raises_data_corrupt(self, tmp_path: Path) -> None:
        """A half-written vault can have this file present (so the
        ``exists()`` check passes) but truncated/garbage -- reported as a
        recognized ``DataCorruptError`` rather than letting a raw
        ``sqlite3``/``aiosqlite`` exception escape, since
        ``Session._open_repository`` one layer up only knows how to skip
        a vault whose key resolution fails with a recognized error, not
        abort on an arbitrary one."""
        db_path = tmp_path / "db" / "vault_encryption_key"
        db_path.parent.mkdir(parents=True)
        db_path.write_bytes(b"not a real sqlite database")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        with pytest.raises(DataCorruptError):
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
        ``TestResolveVaultKeyFromVaultDb.test_corrupt_db_file_raises_data_corrupt``
        -- a truncated/garbage ``userKey`` file (present, so the
        ``exists()`` check passes, but not valid ascii) must not let a raw
        ``UnicodeDecodeError``/``binascii.Error`` escape as an unrecognized
        exception."""
        key_dir = tmp_path / "@ActiveProtectKey" / "userKey"
        key_dir.mkdir(parents=True)
        (key_dir / _USER_KEY_ID).write_bytes(b"\xff\xfe\xfd\xfc")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        km = KeyMaterial(user_key_id=_USER_KEY_ID, user_key=os.urandom(32))
        with pytest.raises(DataCorruptError):
            await km.resolve_vault_key(store, layout)


# -- probe_encrypted() -- "is this repository encrypted at all", no key, no
#    Pool touch — reads the repository's own encryption-key record directly
#    -------------------------------------------------------------------


class TestProbeEncryptedVault:
    async def test_no_encryption_row_gives_false(self, tmp_path: Path) -> None:
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        assert await probe_encrypted(store, layout) is False

    async def test_real_key_row_gives_true(self, tmp_path: Path) -> None:
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [(_USER_KEY_ID, "")])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        assert await probe_encrypted(store, layout) is True

    async def test_uses_the_last_inserted_row_not_the_first(self, tmp_path: Path) -> None:
        # Real vaults never toggle Encryption<->NoEncryption after first
        # init, so this exact history couldn't occur for real — but it proves the query is really
        # "latest row" (ORDER BY rowid DESC), not "whichever row a plain
        # unordered SELECT happens to return first".
        _write_vault_encryption_key_db(
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
        # Distinct from test_missing_db_file_gives_none: the file (and
        # table) exist, just with zero rows in it.
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        assert await probe_encrypted(store, layout) is None

    async def test_corrupt_db_file_raises_data_corrupt(self, tmp_path: Path) -> None:
        # Same truncated/garbage-file reasoning as
        # TestResolveVaultKeyFromVaultDb.test_corrupt_db_file_raises_data_corrupt.
        db_path = tmp_path / "db" / "vault_encryption_key"
        db_path.parent.mkdir(parents=True)
        db_path.write_bytes(b"not a real sqlite database")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        with pytest.raises(DataCorruptError):
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
        # Real shape seen in s3-sample-2-encrypted: an unexplained
        # "init@<userKeyID>" object sits alongside "<userKeyID>" itself.
        # Stripping the prefix before comparing means this doesn't get
        # misread as a second, distinct real-key entry.
        key_dir = tmp_path / "@ActiveProtectKey" / "userKey"
        key_dir.mkdir(parents=True)
        (key_dir / _USER_KEY_ID).write_text("wrapped-key-bytes-not-checked-here")
        (key_dir / f"init@{_USER_KEY_ID}").write_text("unrelated-shorter-value")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        assert await probe_encrypted(store, layout) is True

    async def test_no_key_root_gives_none(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root=None)
        assert await probe_encrypted(store, layout) is None

    async def test_missing_user_key_dir_gives_none(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        assert await probe_encrypted(store, layout) is None

    async def test_empty_user_key_dir_gives_none(self, tmp_path: Path) -> None:
        # Distinct from test_missing_user_key_dir_gives_none: the
        # directory exists (listdir succeeds), it's just empty.
        (tmp_path / "@ActiveProtectKey" / "userKey").mkdir(parents=True)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        assert await probe_encrypted(store, layout) is None


# -- verify() -- GCM-unwrap layer alone, the whole answer to "is this
#    key correct" -- never confirmed by also decrypting a real
#    chunk ---------------------------------------------------------


class TestVerify:
    async def test_no_encryption_short_circuits(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)  # nothing written at all
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        km = KeyMaterial(user_key_id="NoEncryption", user_key=b"\x00" * 32)
        result = await km.verify(store, layout)
        assert result.gcm_ok is True
        assert result.ok is True

    async def test_correct_key_succeeds_with_no_pool_written_at_all(self, tmp_path: Path) -> None:
        # No Pool/*.buk file exists anywhere in this test — proof verify()
        # genuinely never touches the Pool, not just that it happens to
        # tolerate one being absent.
        user_key = os.urandom(32)
        vault_key = os.urandom(32)
        wrapped = _wrap(_USER_KEY_ID, user_key, vault_key)
        _write_vault_encryption_key_db(
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
        _write_vault_encryption_key_db(
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
        # the vault_encryption_key table exists but has no row at all for
        # this userKeyID — resolve_vault_key returns None (not a raised
        # KeyMismatchError), and verify() must still report gcm_ok=False for it.
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
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
