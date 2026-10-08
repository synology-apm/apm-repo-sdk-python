"""Key material: parsing an administrator-provided key string, and
resolving the wrapped VaultKey from whichever of the two on-disk
locations this repository's layout uses.

**"Is this key correct" is answered by ``KeyMaterial.verify`` alone, via
AES-256-GCM's own authentication tag — never by also decrypting a real
chunk** (see ``format.crypto.unwrap_vault_key``; FORMAT-SPEC.md: VaultKey
custody). Per-chunk fingerprint verification is ``Pool``'s
``VerifyPolicy``, not this module's.
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import sqlite3
from typing import Self, override

import aiosqlite

from ..errors import DataCorruptError, KeyMismatchError, NotFoundError
from ..format.crypto import NO_ENCRYPTION_USER_KEY_ID, parse_key_string, unwrap_vault_key
from ..storage.base import ObjectStore, join_path, list_names
from ..storage.layout import RepoKind, RepoLayout
from ..storage.sqlite_source import SqliteSource

_VAULT_KEY_DB = "db/vault_encryption_key"
"""A vault's key table, relative to its repository root."""

_SPEC = "FORMAT-SPEC.md: Chunk pool encryption; VaultKey custody"


async def _vault_db_row(
    store: ObjectStore, db_path: str, query: str, params: tuple[object, ...] = ()
) -> sqlite3.Row | None:
    """Run ``query`` against the vault's ``db/vault_encryption_key`` and return
    its first row (``None`` if none). A truncated or garbage ``db_path``
    raises ``DataCorruptError`` instead of a raw sqlite error, so callers
    (``Session.discover()``) can skip that repository.
    """
    try:
        async with await SqliteSource.from_raw_store(store, db_path) as source:
            cursor = await source.connection.execute(query, params)
            return await cursor.fetchone()
    except aiosqlite.Error as exc:
        raise DataCorruptError(f"{db_path} is not a readable vault_encryption_key database", ref=db_path) from exc


@dataclasses.dataclass(frozen=True, slots=True)
class KeyVerification:
    """Result of ``KeyMaterial.verify``. ``vault_key`` holds the resolved
    DEK only when ``gcm_ok`` and encryption is actually in use; it's
    ``None`` both for an unencrypted repository and for a failed unwrap.
    """

    gcm_ok: bool
    vault_key: bytes | None

    @override
    def __repr__(self) -> str:
        # vault_key is a secret like KeyMaterial.user_key; see its __repr__.
        vault_key_repr = "<redacted>" if self.vault_key is not None else None
        return f"KeyVerification(gcm_ok={self.gcm_ok!r}, vault_key={vault_key_repr})"

    @property
    def ok(self) -> bool:
        return self.gcm_ok


@dataclasses.dataclass(frozen=True, slots=True)
class KeyMaterial:
    """A parsed ``"<userKeyID>@<base64(userKey)>"`` key string.

    ``user_key_id == "NoEncryption"`` (``NO_ENCRYPTION_USER_KEY_ID``)
    marks an unencrypted repository — no VaultKey exists to resolve.
    """

    user_key_id: str
    user_key: bytes

    @classmethod
    def from_key_string(cls, key_string: str) -> Self:
        """Parse ``key_string`` (see ``format.crypto.parse_key_string``).

        Raises:
            KeyMaterialError: The string is malformed.
        """
        user_key_id, user_key = parse_key_string(key_string)
        return cls(user_key_id=user_key_id, user_key=user_key)

    @property
    def is_no_encryption(self) -> bool:
        return self.user_key_id == NO_ENCRYPTION_USER_KEY_ID

    @override
    def __repr__(self) -> str:
        # user_key must never reach a log, message, or repr (user_key_id alone is not secret).
        return f"KeyMaterial(user_key_id={self.user_key_id!r}, user_key=<redacted>)"

    # -- layer 1: GCM unwrap ---------------------------------------------

    async def resolve_vault_key(self, store: ObjectStore, layout: RepoLayout) -> bytes | None:
        """Fetch this repository's wrapped VaultKey (from whichever of the
        two on-disk locations ``layout.kind`` implies) and AES-256-GCM
        unwrap it.

        Returns:
            The VaultKey, or ``None`` if no wrapped key is on record (the
            lookup row/file is absent).

        Raises:
            KeyMismatchError: A wrapped key was found but this
                ``(userKeyID, userKey)`` pair does not open it.
            KeyMaterialError: The stored wrapped key has the wrong length.
            DataCorruptError: The key database or file is unreadable.
        """
        wrapped = await self._wrapped_vault_key(store, layout)
        if wrapped is None:
            return None
        return unwrap_vault_key(self.user_key_id, self.user_key, wrapped)

    async def _wrapped_vault_key(self, store: ObjectStore, layout: RepoLayout) -> bytes | None:
        if layout.kind is RepoKind.VAULT:
            return await self._wrapped_vault_key_from_db(store, layout)
        return await self._wrapped_vault_key_from_key_file(store, layout)

    async def _wrapped_vault_key_from_db(self, store: ObjectStore, layout: RepoLayout) -> bytes | None:
        db_path = join_path(layout.repo_root, _VAULT_KEY_DB)
        if not await store.exists(db_path):
            return None
        row = await _vault_db_row(
            store,
            db_path,
            "SELECT encrypted_data_key FROM vault_encryption_key WHERE user_key_uuid = ?",
            (self.user_key_id,),
        )
        if row is None or not row[0]:
            return None
        return base64.b64decode(row[0])

    async def _wrapped_vault_key_from_key_file(self, store: ObjectStore, layout: RepoLayout) -> bytes | None:
        if layout.key_root is None:
            return None
        path = join_path(layout.key_root, "userKey", self.user_key_id)
        if not await store.exists(path):
            return None
        raw = await store.read(path)
        try:
            return base64.b64decode(raw.decode("ascii").strip())
        except (UnicodeDecodeError, binascii.Error) as exc:
            # Same truncated/garbage handling as _vault_db_row, for the key file.
            raise DataCorruptError(f"{path} is not a readable userKey file", ref=path) from exc

    async def verify(self, store: ObjectStore, layout: RepoLayout) -> KeyVerification:
        """The whole answer to "is this key correct"; no per-chunk check follows.
        A wrong key is reported via the returned ``KeyVerification``, not
        raised. Reads only the wrapped-VaultKey record, never the Pool.

        Raises:
            KeyMaterialError: The stored wrapped key has the wrong length.
            DataCorruptError: The key database or file is unreadable.
        """
        if self.is_no_encryption:
            return KeyVerification(gcm_ok=True, vault_key=None)

        try:
            vault_key = await self.resolve_vault_key(store, layout)
        except KeyMismatchError:
            return KeyVerification(gcm_ok=False, vault_key=None)
        if vault_key is None:
            return KeyVerification(gcm_ok=False, vault_key=None)
        return KeyVerification(gcm_ok=True, vault_key=vault_key)


# -- layer 0: "is this repository encrypted at all" — no key, no Pool touch ----


async def probe_encrypted(store: ObjectStore, layout: RepoLayout) -> bool | None:
    """Whether this repository is vault-encrypted, read from its key record
    alone (no key, no bucket file, no Pool scan).

    VAULT layout: the last-inserted row of ``db/vault_encryption_key``;
    its ``user_key_uuid`` is ``"NoEncryption"`` iff never encrypted, which
    cannot change after first initialization (FORMAT-SPEC.md: VaultKey
    custody). OBJECT_STORE layout: the object names under
    ``<key_root>/userKey/``, with the same sentinel.

    Returns:
        ``True``/``False``, or ``None`` if the record is entirely absent.

    Raises:
        DataCorruptError: The VAULT key database is unreadable.
    """
    if layout.kind is RepoKind.VAULT:
        return await _probe_encrypted_from_vault_db(store, layout)
    return await _probe_encrypted_from_key_dir(store, layout)


async def _probe_encrypted_from_vault_db(store: ObjectStore, layout: RepoLayout) -> bool | None:
    db_path = join_path(layout.repo_root, _VAULT_KEY_DB)
    if not await store.exists(db_path):
        return None
    row = await _vault_db_row(
        store, db_path, "SELECT user_key_uuid FROM vault_encryption_key ORDER BY rowid DESC LIMIT 1"
    )
    if row is None:
        return None
    return bool(row[0] != NO_ENCRYPTION_USER_KEY_ID)


async def _probe_encrypted_from_key_dir(store: ObjectStore, layout: RepoLayout) -> bool | None:
    if layout.key_root is None:
        return None
    try:
        names = await list_names(store, join_path(layout.key_root, "userKey"))
    except NotFoundError:
        return None
    # An encrypted object-store bucket can carry an extra init@<userKeyID>
    # object alongside the real one — strip that prefix before comparing
    # rather than counting it as a second real-key entry.
    real_names = {name.removeprefix("init@") for name in names}
    if not real_names:
        return None
    return real_names != {NO_ENCRYPTION_USER_KEY_ID}
