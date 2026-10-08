"""Chunk and key-material cryptography (FORMAT-SPEC.md: Key hierarchy).

Four independent AES schemes, never confused with each other:

- **Chunk pool** (``.buk``): AES-256-CTR, key = ``vaultKey`` (the DEK), IV
  *derived* from each chunk's ``ChunkAddress``, never stored (FORMAT-SPEC.md:
  Chunk pool encryption).
- **Wrapped VaultKey**: AES-256-GCM, key = ``userKey`` (the KEK), nonce =
  the first 12 ASCII bytes of ``userKeyID`` (FORMAT-SPEC.md: VaultKey
  custody). A wrong key/nonce pair fails the GCM tag, which doubles as the
  integrity check.
- ``copy_meta_file``'s ``aHlT`` envelope: AES-256-CTR with the ``vaultKey``
  DEK and an IV stored in its own header (FORMAT-SPEC.md: File-level
  encryption).
- ``db/copy_target_version.version_spec``: AES-256-CTR with the same DEK and
  an IV derived from ``hex(MD5(version_uid))`` (FORMAT-SPEC.md:
  ``copy_target_version.version_spec`` encryption).

Nothing here decides *whether* something is encrypted; callers decide by
mode bit or magic.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import re

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..errors import KeyMaterialError, KeyMismatchError
from .addressing import ChunkAddress
from .headers import HEADER_LEN, MAGIC, parse_index_header

_SPEC_CHUNK = "FORMAT-SPEC.md: Chunk pool encryption"
_SPEC_GCM = "FORMAT-SPEC.md: VaultKey custody"
_SPEC_AHLT = "FORMAT-SPEC.md: File-level encryption"
_SPEC_VERSION_SPEC = "FORMAT-SPEC.md: `copy_target_version.version_spec` encryption"

AES_KEY_SIZE = 32
AES_GCM_NONCE_SIZE = 12
AES_GCM_TAG_SIZE = 16
USER_KEY_ID_LENGTH = 12
#: ``user_key_id`` value marking a vault that was never encrypted.
NO_ENCRYPTION_USER_KEY_ID = "NoEncryption"

#: ``dedup/keys.py`` joins ``userKeyID`` into a store path
#: (``<key_root>/userKey/<user_key_id>``), so a 12-character id must not
#: contain ``/`` or ``\``. Any other ASCII character is legal (``@`` included).
#: Independent of ``storage.base.join_path``'s own containment check.
_USER_KEY_ID_PATH_SEPARATORS = re.compile(r"[/\\]")

_OFF_AHLT_IV = 8
_AHLT_IV_LEN = 16


def chunk_iv(addr: ChunkAddress) -> bytes:
    """16-byte AES-CTR IV for one chunk: its own 64-bit ``ChunkAddress``,
    big-endian, repeated twice (FORMAT-SPEC.md: Chunk pool encryption)."""
    half = addr.to_int().to_bytes(8, "big")
    return half + half


def build_aes_algorithm(vault_key: bytes) -> algorithms.AES:
    """Build one AES-256 key-schedule object from ``vault_key``, for a caller
    decrypting many chunks with one key (``decrypt_chunk``'s ``algorithm=``).

    Nothing here caches it; the caller does, for the lifetime of whatever
    owns ``vault_key`` (``Pool``).

    Raises:
        KeyMaterialError: ``vault_key`` is not 32 bytes.
    """
    if len(vault_key) != AES_KEY_SIZE:
        raise KeyMaterialError(f"vault_key must be {AES_KEY_SIZE} bytes, got {len(vault_key)}", spec=_SPEC_CHUNK)
    return algorithms.AES(vault_key)


def _aes_ctr_decrypt(
    vault_key: bytes, iv: bytes, ciphertext: bytes | bytearray | memoryview, *, algorithm: algorithms.AES | None = None
) -> bytes:
    """AES-256-CTR-decrypt ``ciphertext`` with ``iv``; shared by the ``aHlT``,
    ``version_spec`` and per-chunk paths, which differ only in their IV.
    ``algorithm``, when given, replaces building a key schedule from
    ``vault_key`` (see ``build_aes_algorithm``)."""
    if algorithm is None:
        algorithm = algorithms.AES(vault_key)
    decryptor = Cipher(algorithm, modes.CTR(iv)).decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()


def decrypt_chunk(
    vault_key: bytes, addr: ChunkAddress, ciphertext: bytes | memoryview, *, algorithm: algorithms.AES | None = None
) -> bytes:
    """AES-256-CTR-decrypt one chunk's ciphertext (no decompression; see
    ``compression``).

    A prebuilt ``algorithm`` (from ``build_aes_algorithm``) skips building the
    key schedule; it must come from the same ``vault_key`` (not checked).
    Each call uses its own ``Cipher``: every chunk's counter starts at its own
    ``chunk_iv``, so one decryptor streamed across chunks would be wrong
    (``ChunkDecryptor`` re-nonces one instead, for many chunks).

    Args:
        vault_key: The DEK; unused when ``algorithm`` is given.
        addr: The chunk's address, which derives the IV.
        ciphertext: ``bytes`` or a ``memoryview`` (not copied).

    Returns:
        The plaintext (still compressed, if the chunk was).

    Raises:
        KeyMaterialError: ``vault_key`` is not 32 bytes and no ``algorithm``
            was given.
    """
    iv = chunk_iv(addr)
    if algorithm is None and len(vault_key) != AES_KEY_SIZE:
        raise KeyMaterialError(f"vault_key must be {AES_KEY_SIZE} bytes, got {len(vault_key)}", spec=_SPEC_CHUNK)
    return _aes_ctr_decrypt(vault_key, iv, ciphertext, algorithm=algorithm)


class ChunkDecryptor:
    """``decrypt_chunk`` for many chunks: one AES-256-CTR context, re-nonced
    with each chunk's own ``chunk_iv`` (building a context per chunk costs
    several times the decryption itself). A context is not thread-safe: use
    one instance per thread.

    Raises:
        KeyMaterialError: ``vault_key`` is not 32 bytes and no ``algorithm``
            was given.
    """

    def __init__(self, vault_key: bytes, *, algorithm: algorithms.AES | None = None) -> None:
        if algorithm is None:
            algorithm = build_aes_algorithm(vault_key)
        self._context = Cipher(algorithm, modes.CTR(bytes(16))).decryptor()

    def decrypt(self, addr: ChunkAddress, ciphertext: bytes | memoryview) -> bytes:
        """One chunk's ciphertext decrypted, as ``decrypt_chunk`` would."""
        self._context.reset_nonce(chunk_iv(addr))
        return self._context.update(ciphertext)


def parse_key_string(key_string: str) -> tuple[str, bytes]:
    """Split an administrator-provided key string
    ``"<userKeyID>@<base64(userKey)>"`` (FORMAT-SPEC.md: Key hierarchy) into
    ``(user_key_id, user_key)``. Splits on the last ``@`` and validates
    ``user_key_id`` is 12 ASCII characters (the GCM nonce's 12 bytes)
    without a path separator and ``user_key`` decodes to 32 bytes.
    ``"NoEncryption"`` is not special-cased (that is ``KeyMaterial``'s
    decision).

    Raises:
        KeyMaterialError: The string is malformed.
    """
    if "@" not in key_string:
        raise KeyMaterialError("key string is not '<userKeyID>@<base64(userKey)>'", spec=_SPEC_GCM)
    user_key_id, b64_user_key = key_string.rsplit("@", 1)
    if len(user_key_id) != USER_KEY_ID_LENGTH:
        raise KeyMaterialError(
            f"userKeyID must be {USER_KEY_ID_LENGTH} characters, got {len(user_key_id)}",
            spec=_SPEC_GCM,
        )
    if not user_key_id.isascii():
        raise KeyMaterialError(f"userKeyID must be ASCII, got {user_key_id!r}", spec=_SPEC_GCM)
    if _USER_KEY_ID_PATH_SEPARATORS.search(user_key_id):
        raise KeyMaterialError(f"userKeyID must not contain a path separator, got {user_key_id!r}", spec=_SPEC_GCM)
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
    ``userKeyID``'s first 12 ASCII bytes (FORMAT-SPEC.md: VaultKey custody).

    A successful unwrap proves the ``(userKeyID, userKey)`` pair is correct
    for the repository's data too, since the DEK never changes after
    initialization.

    Raises:
        KeyMaterialError: An argument has the wrong length, or
            ``user_key_id`` is not ASCII.
        KeyMismatchError: The GCM tag check fails (wrong key pair).
    """
    if len(user_key) != AES_KEY_SIZE:
        raise KeyMaterialError(f"userKey must be {AES_KEY_SIZE} bytes, got {len(user_key)}", spec=_SPEC_GCM)
    if len(user_key_id) != USER_KEY_ID_LENGTH:
        raise KeyMaterialError(
            f"userKeyID must be {USER_KEY_ID_LENGTH} characters, got {len(user_key_id)}",
            spec=_SPEC_GCM,
        )
    if not user_key_id.isascii():
        raise KeyMaterialError(f"userKeyID must be ASCII, got {user_key_id!r}", spec=_SPEC_GCM)
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


def is_ahlt(head: bytes | bytearray) -> bool:
    """Whether ``head`` opens with the ``aHlT`` envelope's magic — only its
    first 4 bytes are inspected."""
    return head[:4] == MAGIC["ahlt"]


def ahlt_decrypt(data: bytes | bytearray, vault_key: bytes) -> bytes:
    """Decrypt a ``copy_meta_file`` entry's ``aHlT``-enveloped bytes:
    64-byte header (the generic ``IndexHeader`` shell, IV at ``[8,24)``)
    followed by AES-256-CTR ciphertext from byte 64 onward (FORMAT-SPEC.md:
    File-level encryption). The IV is stored in the header, not derived as
    in ``chunk_iv``.

    Returns:
        The plaintext body, header stripped.

    Raises:
        FormatError: ``data`` is shorter than the 64-byte header.
        DataCorruptError: A magic or header-CRC mismatch.
    """
    parse_index_header(data, expect_magic=MAGIC["ahlt"], spec=_SPEC_AHLT)
    iv = bytes(data[_OFF_AHLT_IV : _OFF_AHLT_IV + _AHLT_IV_LEN])
    # A view: slicing would copy the whole body before decrypting it.
    return _aes_ctr_decrypt(vault_key, iv, memoryview(data)[HEADER_LEN:])


def version_spec_iv(version_uid: str) -> bytes:
    """16-byte AES-CTR IV for one ``copy_target_version.version_spec``: the
    first 16 ASCII bytes of the hex string ``hex(MD5(version_uid))``, not the
    raw 16-byte digest (FORMAT-SPEC.md: ``copy_target_version.version_spec``
    encryption).
    """
    hex_digest = hashlib.md5(version_uid.encode("utf-8")).hexdigest()
    return hex_digest[:16].encode("ascii")


def decrypt_version_spec(ciphertext_b64: str, version_uid: str, vault_key: bytes) -> str:
    """Decrypt one ``copy_target_version.version_spec`` column: AES-256-CTR,
    same DEK as chunk-pool/``aHlT`` (``vault_key``), IV from
    ``version_spec_iv``. The value is plain base64 with no magic to sniff, so
    the caller decides whether to call this (``repo.vault_key is not None``)
    (FORMAT-SPEC.md: ``copy_target_version.version_spec`` encryption).

    Returns:
        The decoded UTF-8 plaintext (a JSON string).

    Raises:
        KeyMaterialError: ``vault_key`` is not 32 bytes.
        UnicodeDecodeError: A wrong key or corrupt input.
        ValueError: ``ciphertext_b64`` is not ASCII, or not valid base64
            (``binascii.Error``).
    """
    if len(vault_key) != AES_KEY_SIZE:
        raise KeyMaterialError(f"vault_key must be {AES_KEY_SIZE} bytes, got {len(vault_key)}", spec=_SPEC_VERSION_SPEC)
    ciphertext = base64.b64decode(ciphertext_b64, validate=True)
    iv = version_spec_iv(version_uid)
    plaintext = _aes_ctr_decrypt(vault_key, iv, ciphertext)
    return plaintext.decode("utf-8")
