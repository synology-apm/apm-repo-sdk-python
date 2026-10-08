"""Property tests for ``synology_apm_repo.sdk.format.crypto``: key material
and ciphertext round-trip, and any input either decodes or raises the
parser's documented error."""

from __future__ import annotations

import base64
import contextlib
import zlib

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from hypothesis import given
from hypothesis import strategies as st

from synology_apm_repo.sdk.errors import FormatError, KeyMaterialError, KeyMismatchError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.crypto import (
    ChunkDecryptor,
    ahlt_decrypt,
    decrypt_chunk,
    parse_key_string,
    unwrap_vault_key,
)
from unit.sdk.format_strategies import mutated, u64

_VAULT_KEY = bytes(range(32))

_key = st.binary(min_size=32, max_size=32)
_user_key_id = st.text(
    st.characters(min_codepoint=0x20, max_codepoint=0x7E, exclude_characters="/\\"), min_size=12, max_size=12
)


def _key_string(user_key_id: str, user_key: bytes) -> str:
    return f"{user_key_id}@{base64.b64encode(user_key).decode('ascii')}"


@given(user_key_id=_user_key_id, user_key=_key, vault_key=_key)
def test_a_key_string_unwraps_the_vault_key_it_wrapped(user_key_id: str, user_key: bytes, vault_key: bytes) -> None:
    wrapped = AESGCM(user_key).encrypt(user_key_id.encode("ascii"), vault_key, None)
    parsed_id, parsed_key = parse_key_string(_key_string(user_key_id, user_key))
    assert (parsed_id, parsed_key) == (user_key_id, user_key)
    assert unwrap_vault_key(parsed_id, parsed_key, wrapped) == vault_key


@given(
    st.text(max_size=64)
    | st.builds(_key_string, st.text(min_size=11, max_size=13), st.binary(min_size=31, max_size=33))
)
def test_any_text_parses_to_a_usable_key_or_raises_key_material_error(key_string: str) -> None:
    """A parsed key pair reaches the GCM check, never a decoding error."""
    with contextlib.suppress(KeyMaterialError):
        user_key_id, user_key = parse_key_string(key_string)
        with contextlib.suppress(KeyMismatchError):
            unwrap_vault_key(user_key_id, user_key, bytes(48))


@given(addr=u64, data=st.binary(max_size=64))
def test_chunk_decryption_is_its_own_inverse(addr: int, data: bytes) -> None:
    """AES-CTR: decrypting twice at one address restores the input; the
    reused-context decryptor agrees with the one-shot form."""
    address = ChunkAddress.from_int(addr)
    once = decrypt_chunk(_VAULT_KEY, address, data)
    assert ChunkDecryptor(_VAULT_KEY).decrypt(address, data) == once
    assert decrypt_chunk(_VAULT_KEY, address, once) == data


def _ahlt(iv: bytes, plaintext: bytes) -> bytes:
    """An ``aHlT`` envelope: the 64-byte header shell with ``iv`` at
    ``[8, 24)``, then the AES-256-CTR ciphertext."""
    header = bytearray(64)
    header[0:4] = b"aHlT"
    header[8:24] = iv
    header[60:64] = zlib.crc32(header[:60]).to_bytes(4, "big")
    encryptor = Cipher(algorithms.AES(_VAULT_KEY), modes.CTR(iv)).encryptor()
    return bytes(header) + encryptor.update(plaintext) + encryptor.finalize()


@given(iv=st.binary(min_size=16, max_size=16), plaintext=st.binary(max_size=64))
def test_ahlt_round_trips(iv: bytes, plaintext: bytes) -> None:
    assert ahlt_decrypt(_ahlt(iv, plaintext), _VAULT_KEY) == plaintext


_valid_ahlt = st.builds(_ahlt, st.binary(min_size=16, max_size=16), st.binary(max_size=16))


@given(st.binary(max_size=96) | mutated(_valid_ahlt))
def test_ahlt_decrypt_returns_or_raises_format_error(data: bytes) -> None:
    with contextlib.suppress(FormatError):
        ahlt_decrypt(data, _VAULT_KEY)
