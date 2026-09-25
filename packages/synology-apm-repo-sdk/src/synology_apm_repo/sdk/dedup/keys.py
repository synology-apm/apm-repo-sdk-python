"""Key material: parsing an administrator-provided key string, and
resolving the wrapped VaultKey from whichever of the two on-disk
locations this repository's layout uses.

**"Is this key correct" is answered by ``KeyMaterial.verify`` alone,
via AES-256-GCM's own authentication tag — never by also decrypting a
real chunk.** GCM tag success is already cryptographic proof the
``(userKeyID, userKey)`` pair correctly unwraps the *stored*
wrapped-VaultKey record (FORMAT-SPEC.md: vaultkey-custody); given the repository-wide
invariant that the DEK (``vaultKey``) itself never changes
after first initialization (also vaultkey-custody), that same VaultKey is, by
construction, the one used for every real chunk this repository has —
not merely "probably" so. Per-chunk fingerprint verification (decrypting
a real chunk and comparing its SHA-256 against its stored ``.fgp``) is a
separate, data-integrity concern this module has no part in — see
``Pool.read_chunk``'s own ``verify_fingerprint`` option for that.
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import sqlite3
from typing import Self

import aiosqlite

from ..errors import DataCorruptError, KeyMismatchError, NotFoundError
from ..format.crypto import NO_ENCRYPTION_USER_KEY_ID, parse_key_string, unwrap_vault_key
from ..storage.base import ObjectStore, join_path
from ..storage.layout import RepoKind, RepoLayout
from ..storage.sqlite_source import SqliteSource

_SPEC = "FORMAT-SPEC.md: chunk-pool-encryption/vaultkey-custody"


async def _vault_db_row(
    store: ObjectStore, db_path: str, query: str, params: tuple[object, ...] = ()
) -> sqlite3.Row | None:
    """Run ``query`` against the vault's own ``db/vault_encryption_key``
    and return its first row (``None`` if it has none) — the one place
    ``aiosqlite.Error`` gets wrapped into ``DataCorruptError``, shared by
    ``KeyMaterial._wrapped_vault_key_from_db`` and
    ``_probe_encrypted_from_vault_db`` below, which differ only in which
    query they run against this same file. A half-written vault can have
    ``db_path`` present (the caller's own ``exists()`` check passing) but
    truncated/garbage — reported as a recognized ``ApmRepoError`` rather
    than letting a raw sqlite3 exception escape, so callers up the stack
    (``Session.discover()``'s own "skip, don't abort" contract) can tell
    this apart from every other ``BaseException``.
    """
    try:
        async with await SqliteSource.from_raw_store(store, db_path) as source:
            cursor = await source.connection.execute(query, params)
            return await cursor.fetchone()
    except aiosqlite.Error as exc:
        raise DataCorruptError(f"{db_path} is not a readable vault_encryption_key database", ref=db_path) from exc


@dataclasses.dataclass(frozen=True)
class KeyVerification:
    """Result of ``KeyMaterial.verify``. ``vault_key`` holds the resolved
    DEK only when ``gcm_ok`` and encryption is actually in use; it's
    ``None`` both for an unencrypted repository and for a failed unwrap.
    """

    gcm_ok: bool
    vault_key: bytes | None

    def __repr__(self) -> str:
        # See KeyMaterial.__repr__ below: vault_key is
        # exactly the same kind of secret as user_key there.
        vault_key_repr = "<redacted>" if self.vault_key is not None else None
        return f"KeyVerification(gcm_ok={self.gcm_ok!r}, vault_key={vault_key_repr})"

    @property
    def ok(self) -> bool:
        return self.gcm_ok


@dataclasses.dataclass(frozen=True)
class KeyMaterial:
    """A parsed ``"<userKeyID>@<base64(userKey)>"`` key string.

    ``user_key_id == "NoEncryption"`` (``NO_ENCRYPTION_USER_KEY_ID``)
    marks an unencrypted repository — no VaultKey exists to resolve.
    """

    user_key_id: str
    user_key: bytes

    @classmethod
    def from_key_string(cls, key_string: str) -> Self:
        user_key_id, user_key = parse_key_string(key_string)
        return cls(user_key_id=user_key_id, user_key=user_key)

    @property
    def is_no_encryption(self) -> bool:
        return self.user_key_id == NO_ENCRYPTION_USER_KEY_ID

    def __repr__(self) -> str:
        # Key material must never leak into a log,
        # exception message, or repr — user_key is deliberately omitted here
        # even though user_key_id alone is not secret. Same rule applies to
        # KeyVerification.vault_key above.
        return f"KeyMaterial(user_key_id={self.user_key_id!r}, user_key=<redacted>)"

    # -- layer 1: GCM unwrap ---------------------------------------------

    async def resolve_vault_key(self, store: ObjectStore, layout: RepoLayout) -> bytes | None:
        """Fetch this repository's wrapped VaultKey (from whichever of the
        two on-disk locations ``layout.kind`` implies) and AES-256-GCM
        unwrap it.

        Returns ``None`` if no wrapped key is on record at all (e.g. the
        lookup row/file is simply absent) — as distinct from
        ``KeyMismatchError``, which means a
        wrapped key *was* found but this ``(userKeyID, userKey)`` pair does
        not open it.
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
        db_path = join_path(layout.repo_root, "db", "vault_encryption_key")
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
            # Same "half-written, present but truncated/garbage" reasoning
            # as _vault_db_row above, for the OBJECT_STORE key-file
            # counterpart of that same vault-db read.
            raise DataCorruptError(f"{path} is not a readable userKey file", ref=path) from exc

    async def verify(self, store: ObjectStore, layout: RepoLayout) -> KeyVerification:
        """The whole answer to "is this key correct" — no separate
        per-chunk check follows this. Never raises for an ordinary "wrong
        key" outcome — that is reported via the returned
        ``KeyVerification``, not an exception; the caller decides what a
        failed verification means for its flow (CLI/TUI report it,
        ``Repository.set_key`` may choose to reject it). Touches only the
        wrapped-VaultKey record itself (one sqlite row or one small key
        file) — never the Pool.
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
    """Cheaply determine whether this repository is vault-encrypted by
    reading its own encryption-key record directly — VAULT layout reads
    ``db/vault_encryption_key``'s latest row, OBJECT_STORE layout reads
    the key-dir marker under ``<key_root>/userKey/``. No bucket file
    opened, no Pool scan, no key required — the repository's own
    encryption-key record is trusted as the single source of truth here,
    the same way ``KeyMaterial.resolve_vault_key`` trusts it to find
    one *specific* candidate key's wrapped VaultKey, just reading the
    record's latest entry instead of looking one up by id.

    VAULT layout: ``db/vault_encryption_key`` is an append-only
    key-rotation log, never updated in place. Its last-inserted row's
    ``user_key_uuid`` is the currently-active key, ``"NoEncryption"`` iff
    this vault has never been encrypted (Encryption↔NoEncryption cannot
    toggle after first initialization — the DEK/``vaultKey`` itself never
    changes once set, per FORMAT-SPEC.md: vaultkey-custody).

    OBJECT_STORE layout: the equivalent record lives as individual
    objects named by ``userKeyID`` under ``<key_root>/userKey/`` rather
    than db rows, with the same ``"NoEncryption"`` sentinel written at
    bucket-creation time for an unencrypted bucket.

    Returns ``None`` only if the record itself is entirely absent —
    should not happen for a properly initialized repository — as distinct from
    a confirmed-unencrypted repository (``False``).
    """
    if layout.kind is RepoKind.VAULT:
        return await _probe_encrypted_from_vault_db(store, layout)
    return await _probe_encrypted_from_key_dir(store, layout)


async def _probe_encrypted_from_vault_db(store: ObjectStore, layout: RepoLayout) -> bool | None:
    db_path = join_path(layout.repo_root, "db", "vault_encryption_key")
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
        names = await store.listdir(join_path(layout.key_root, "userKey"))
    except NotFoundError:
        return None
    # An encrypted object-store bucket can carry an extra ``init@<userKeyID>``
    # object alongside the real ``<userKeyID>`` one (different, smaller
    # content than the wrapped key itself) — its purpose isn't confirmed,
    # so its prefix is stripped before comparing rather than risking it
    # read as a second, unaccounted-for "real key" entry.
    real_names = {name.removeprefix("init@") for name in names}
    if not real_names:
        return None
    return real_names != {NO_ENCRYPTION_USER_KEY_ID}
