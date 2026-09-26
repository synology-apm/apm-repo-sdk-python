"""Chunk and key-material cryptography (FORMAT-SPEC.md: key-hierarchy,
chunk-pool-encryption, vaultkey-custody, aHlT).

Three independent AES schemes, never confused with each other:

- **Chunk pool** (``.buk``): AES-256-CTR, key = ``vaultKey`` (the DEK), IV
  *derived* from each chunk's own ``ChunkAddress`` — never random,
  never stored, recomputed identically on every read (chunk-pool-encryption).
- **Wrapped VaultKey**: AES-256-GCM, key = ``userKey`` (the KEK), nonce =
  the first 12 ASCII bytes of ``userKeyID`` (vaultkey-custody) — this is the *only*
  place a nonce is explicit rather than derived, and it doubles as an
  integrity check (a wrong key/nonce pair fails the GCM tag outright).
- ``copy_meta_file``'s ``aHlT`` envelope reuses the *chunk-pool* DEK
  (``vaultKey``) but with AES-256-CTR and an IV stored in its own header,
  not derived (aHlT).
- ``db/copy_target_version.version_spec``: AES-256-CTR, same DEK
  (``vaultKey``) again, but its own derived-not-stored IV, taken from the
  first 16 ASCII bytes of ``hex(MD5(version_uid))`` rather than the raw
  MD5 digest (version-spec-encryption).

Nothing here decides *whether* something is encrypted — that is always a
mode-bit or magic-byte check made by the caller; this
module only ever runs once the caller has already decided encryption
applies.
"""

from __future__ import annotations

import base64
import binascii
import functools
import hashlib

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..errors import KeyMaterialError, KeyMismatchError
from .addressing import ChunkAddress
from .headers import HEADER_LEN, MAGIC, parse_index_header

_SPEC_CHUNK = "FORMAT-SPEC.md: chunk-pool-encryption"
_SPEC_GCM = "FORMAT-SPEC.md: vaultkey-custody"
_SPEC_AHLT = "FORMAT-SPEC.md: aHlT"
_SPEC_VERSION_SPEC = "FORMAT-SPEC.md: version-spec-encryption"

AES_KEY_SIZE = 32
AES_GCM_NONCE_SIZE = 12
AES_GCM_TAG_SIZE = 16
USER_KEY_ID_LENGTH = 12
#: ``user_key_id`` value marking a vault that was never encrypted.
NO_ENCRYPTION_USER_KEY_ID = "NoEncryption"

_OFF_AHLT_IV = 8
_AHLT_IV_LEN = 16


def chunk_iv(addr: ChunkAddress) -> bytes:
    """16-byte AES-CTR IV for one chunk: its own 64-bit ``ChunkAddress``,
    big-endian, repeated twice (FORMAT-SPEC.md: chunk-pool-encryption)."""
    half = addr.to_int().to_bytes(8, "big")
    return half + half


@functools.lru_cache(maxsize=8)
def _aes_algorithm(vault_key: bytes) -> algorithms.AES:
    """One ``algorithms.AES`` key-schedule object per distinct key, reused
    across every chunk decrypted with it for a session's lifetime — a real
    session only ever uses a small handful of vault keys, so an 8-entry
    bound is generous headroom, not a real limit. The per-chunk part that
    must still be rebuilt every call is the ``modes.CTR`` half — see
    ``decrypt_chunk`` for why that half can never be reused across chunks."""
    return algorithms.AES(vault_key)


def _aes_ctr_decrypt(vault_key: bytes, iv: bytes, ciphertext: bytes | memoryview) -> bytes:
    """AES-256-CTR-decrypt ``ciphertext`` with ``vault_key``'s cached key
    schedule (``_aes_algorithm``) and the given ``iv`` — the one
    build-a-``Cipher``-and-run-it shape shared by every scheme in this
    module that decrypts with the vault DEK (chunk pool, ``aHlT``,
    ``version_spec``), each differing only in how its ``iv`` is derived."""
    decryptor = Cipher(_aes_algorithm(vault_key), modes.CTR(iv)).decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()


def decrypt_chunk(vault_key: bytes, addr: ChunkAddress, ciphertext: bytes | memoryview) -> bytes:
    """AES-256-CTR-decrypt one chunk's ciphertext. Does not decompress —
    see ``compression`` for that.

    ``ciphertext`` accepts a ``memoryview`` as well as ``bytes`` — a
    caller slicing chunk ciphertext straight out of a merged run's I/O
    buffer can pass a view through without copying, since
    ``Cipher.decryptor().update()`` reads it via the buffer protocol and
    always allocates its own fresh plaintext output either way.

    **One ``Cipher`` per call, deliberately.** Each chunk's counter starts
    from its own independently derived ``chunk_iv``, not from where the
    previous chunk's ``update()`` left the running counter, so streaming
    many chunks through one decryptor would silently produce wrong
    plaintext after the first. The ``algorithms.AES`` key-schedule half
    *is* safe to reuse across chunks — see ``_aes_algorithm``.
    """
    if len(vault_key) != AES_KEY_SIZE:
        raise KeyMaterialError(f"vault_key must be {AES_KEY_SIZE} bytes, got {len(vault_key)}", spec=_SPEC_CHUNK)
    return _aes_ctr_decrypt(vault_key, chunk_iv(addr), ciphertext)


def parse_key_string(key_string: str) -> tuple[str, bytes]:
    """Split an administrator-provided key string
    ``"<userKeyID>@<base64(userKey)>"`` (FORMAT-SPEC.md: key-hierarchy) into
    ``(user_key_id, user_key)``. Splits on the *last* ``@`` per spec, and
    validates ``user_key_id`` is exactly 12 characters and ``user_key``
    decodes to exactly 32 raw bytes.

    Does not special-case ``"NoEncryption"`` — that's a caller-level
    (``KeyMaterial``) decision about *whether* a key is needed, not a
    string-parsing concern.
    """
    if "@" not in key_string:
        raise KeyMaterialError("key string is not '<userKeyID>@<base64(userKey)>'", spec=_SPEC_GCM)
    user_key_id, b64_user_key = key_string.rsplit("@", 1)
    if len(user_key_id) != USER_KEY_ID_LENGTH:
        raise KeyMaterialError(
            f"userKeyID must be {USER_KEY_ID_LENGTH} characters, got {len(user_key_id)}",
            spec=_SPEC_GCM,
        )
    try:
        user_key = base64.b64decode(b64_user_key, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise KeyMaterialError(f"userKey is not valid base64: {exc}", spec=_SPEC_GCM) from exc
    if len(user_key) != AES_KEY_SIZE:
        raise KeyMaterialError(f"userKey must decode to {AES_KEY_SIZE} bytes, got {len(user_key)}", spec=_SPEC_GCM)
    return user_key_id, user_key


def unwrap_vault_key(user_key_id: str, user_key: bytes, wrapped: bytes) -> bytes:
    """AES-256-GCM-unwrap a 48-byte wrapped VaultKey
    (``ciphertext(32) || tag(16)``) using ``userKey`` and a nonce built from
    ``userKeyID``'s first 12 ASCII bytes (FORMAT-SPEC.md: vaultkey-custody).

    A successful unwrap already proves the ``(userKeyID, userKey)`` pair
    is correct for this repository's real data too, not merely for this
    stored record in isolation — the DEK it unwraps never changes after
    first initialization.

    Raises:
        KeyMismatchError: The GCM tag check fails — proof the
            ``(userKeyID, userKey)`` pair does not match.
    """
    if len(user_key) != AES_KEY_SIZE:
        raise KeyMaterialError(f"userKey must be {AES_KEY_SIZE} bytes, got {len(user_key)}", spec=_SPEC_GCM)
    if len(user_key_id) != USER_KEY_ID_LENGTH:
        raise KeyMaterialError(
            f"userKeyID must be {USER_KEY_ID_LENGTH} characters, got {len(user_key_id)}",
            spec=_SPEC_GCM,
        )
    if len(wrapped) != AES_KEY_SIZE + AES_GCM_TAG_SIZE:
        raise KeyMaterialError(
            f"wrapped VaultKey must be {AES_KEY_SIZE + AES_GCM_TAG_SIZE} bytes, got {len(wrapped)}",
            spec=_SPEC_GCM,
        )
    nonce = user_key_id.encode("ascii")[:AES_GCM_NONCE_SIZE]
    try:
        return AESGCM(user_key).decrypt(nonce, wrapped, None)
    except InvalidTag as exc:
        raise KeyMismatchError(
            "AES-256-GCM tag check failed — userKeyID/userKey do not match this wrapped VaultKey",
            spec=_SPEC_GCM,
        ) from exc


def ahlt_decrypt(data: bytes, vault_key: bytes) -> bytes:
    """Decrypt a ``copy_meta_file`` entry's ``aHlT``-enveloped bytes:
    64-byte header (the generic ``IndexHeader`` shell, IV at ``[8,24)``)
    followed by AES-256-CTR ciphertext from byte 64 onward (FORMAT-SPEC.md:
    aHlT). Returns the plaintext body only (header stripped).

    The IV here is *stored in the header*, unlike a pool chunk's
    address-derived IV — do not reuse ``chunk_iv`` for this envelope.
    """
    parse_index_header(data, expect_magic=MAGIC["ahlt"], spec=_SPEC_AHLT)
    iv = data[_OFF_AHLT_IV : _OFF_AHLT_IV + _AHLT_IV_LEN]
    ciphertext = data[HEADER_LEN:]
    return _aes_ctr_decrypt(vault_key, iv, ciphertext)


def version_spec_iv(version_uid: str) -> bytes:
    """16-byte AES-CTR IV for one ``copy_target_version`` row's own
    ``version_spec`` column: the first 16 **ASCII bytes of the hex
    string** ``hex(MD5(version_uid))`` — not the raw 16-byte MD5 digest
    itself. MD5's raw digest is *also* exactly 16 bytes, which makes
    grabbing it directly (instead of its hex text) an easy trap: the
    on-disk format takes the hex string and copies its first 16
    *characters*, never the raw digest bytes (FORMAT-SPEC.md:
    version-spec-encryption).
    """
    hex_digest = hashlib.md5(version_uid.encode("utf-8")).hexdigest()
    return hex_digest[:16].encode("ascii")


def decrypt_version_spec(ciphertext_b64: str, version_uid: str, vault_key: bytes) -> str:
    """Decrypt one ``copy_target_version.version_spec`` column: AES-256-CTR,
    same DEK as chunk-pool/``aHlT`` (``vault_key``), IV from
    ``version_spec_iv``. Plain standard base64 text, no header/magic
    bytes to sniff — *whether* to call this at all is the caller's own
    decision (``repo.vault_key is not None``), same "no guessing" rule as
    this module's other schemes (FORMAT-SPEC.md: version-spec-encryption).

    Returns the decoded UTF-8 plaintext (a JSON string) — raises
    ``UnicodeDecodeError``/``binascii.Error`` on a wrong key or corrupt
    input rather than returning garbage silently; callers decide how to
    degrade.
    """
    if len(vault_key) != AES_KEY_SIZE:
        raise KeyMaterialError(f"vault_key must be {AES_KEY_SIZE} bytes, got {len(vault_key)}", spec=_SPEC_VERSION_SPEC)
    ciphertext = base64.b64decode(ciphertext_b64, validate=True)
    iv = version_spec_iv(version_uid)
    plaintext = _aes_ctr_decrypt(vault_key, iv, ciphertext)
    return plaintext.decode("utf-8")
