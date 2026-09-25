"""Unit tests for ``synology_apm_repo.sdk.format.crypto``.

Round-trips are constructed independently using the ``cryptography``
library's *encrypt* side directly (not by calling back into any decrypt
helper in this module), so a bug shared between encrypt-side test fixtures
and decrypt-side implementation can't hide from these.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import zlib

import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from synology_apm_repo.sdk.errors import DataCorruptError, KeyMaterialError, KeyMismatchError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.crypto import (
    AES_KEY_SIZE,
    ahlt_decrypt,
    chunk_iv,
    decrypt_chunk,
    decrypt_version_spec,
    parse_key_string,
    unwrap_vault_key,
    version_spec_iv,
)
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId


def _addr(stream_id: int, bucket_id: int, chunk_idx: int) -> ChunkAddress:
    return ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx))


class TestChunkIv:
    def test_iv_is_address_repeated_twice(self) -> None:
        addr = _addr(1, 2, 3)
        iv = chunk_iv(addr)
        assert len(iv) == 16
        assert iv[0:8] == iv[8:16]
        assert iv[0:8] == addr.to_int().to_bytes(8, "big")

    def test_different_addresses_give_different_ivs(self) -> None:
        assert chunk_iv(_addr(1, 2, 3)) != chunk_iv(_addr(1, 2, 4))


class TestDecryptChunk:
    def test_round_trip(self) -> None:
        key = os.urandom(AES_KEY_SIZE)
        addr = _addr(132, 330, 17)
        plaintext = os.urandom(4096)

        encryptor = Cipher(algorithms.AES(key), modes.CTR(chunk_iv(addr))).encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()

        assert decrypt_chunk(key, addr, ciphertext) == plaintext

    def test_wrong_key_length_raises(self) -> None:
        with pytest.raises(KeyMaterialError):
            decrypt_chunk(b"short", _addr(1, 1, 1), b"\x00" * 16)

    def test_accepts_a_memoryview_ciphertext_without_copying_first(self) -> None:
        # A caller slicing chunk ciphertext straight out of a merged
        # run's I/O buffer can pass a memoryview through unchanged --
        # Cipher.decryptor().update() reads it via the buffer protocol
        # with no copy needed -- unlike compression.py's
        # CompressType.NONE passthrough, there's no identity-preserving
        # fast path here (Cipher.decryptor().update() always allocates a
        # fresh output), so the real thing worth pinning is that a
        # memoryview slice decrypts to the exact same plaintext bytes
        # as the equivalent bytes object would.
        key = os.urandom(AES_KEY_SIZE)
        addr = _addr(132, 330, 18)
        plaintext = os.urandom(4096)
        encryptor = Cipher(algorithms.AES(key), modes.CTR(chunk_iv(addr))).encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()

        buf = b"\x00" * 8 + ciphertext + b"\x00" * 8
        view = memoryview(buf)[8 : 8 + len(ciphertext)]

        assert decrypt_chunk(key, addr, view) == plaintext

    def test_wrong_key_gives_wrong_plaintext_not_an_exception(self) -> None:
        # AES-CTR has no built-in integrity check — decrypting with the
        # wrong key silently produces garbage, never raises. Verification
        # against the .fgp fingerprint (a higher layer) is what catches
        # this; this module has no way to detect it on its own.
        key = os.urandom(AES_KEY_SIZE)
        wrong_key = os.urandom(AES_KEY_SIZE)
        addr = _addr(1, 1, 1)
        plaintext = os.urandom(4096)
        encryptor = Cipher(algorithms.AES(key), modes.CTR(chunk_iv(addr))).encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        assert decrypt_chunk(wrong_key, addr, ciphertext) != plaintext


class TestDecryptVersionSpec:
    def test_round_trip(self) -> None:
        key = os.urandom(AES_KEY_SIZE)
        version_uid = "abcdefgh-1234-5678"
        plaintext = json.dumps({"some": "spec"}).encode("utf-8")
        encryptor = Cipher(algorithms.AES(key), modes.CTR(version_spec_iv(version_uid))).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(plaintext) + encryptor.finalize()).decode("ascii")

        assert json.loads(decrypt_version_spec(ciphertext_b64, version_uid, key)) == {"some": "spec"}

    def test_iv_is_first_16_hex_characters_of_md5_not_the_raw_digest(self) -> None:
        # Independent of version_spec_iv() itself: MD5's raw digest is
        # also exactly 16 bytes, which makes grabbing it directly
        # (instead of the first 16 *characters* of its hex text) an easy
        # trap that a round trip built via version_spec_iv() itself
        # can never catch. Build the expected IV by hand here and confirm
        # decryption only works with that value, not with the raw digest.
        key = os.urandom(AES_KEY_SIZE)
        version_uid = "some-real-looking-version-uid-1234"
        plaintext = json.dumps({"status": {"status": "COMPLETED"}}).encode("utf-8")

        raw_digest = hashlib.md5(version_uid.encode("utf-8")).digest()
        hex_digest = hashlib.md5(version_uid.encode("utf-8")).hexdigest()
        correct_iv = hex_digest[:16].encode("ascii")
        assert len(correct_iv) == 16
        assert correct_iv != raw_digest[:16]

        encryptor = Cipher(algorithms.AES(key), modes.CTR(correct_iv)).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(plaintext) + encryptor.finalize()).decode("ascii")

        assert json.loads(decrypt_version_spec(ciphertext_b64, version_uid, key)) == {"status": {"status": "COMPLETED"}}

        # Decrypting the same ciphertext with the raw-digest IV (the trap)
        # must not silently succeed with the correct plaintext.
        wrong_decryptor = Cipher(algorithms.AES(key), modes.CTR(raw_digest[:16])).decryptor()
        wrong_plaintext = wrong_decryptor.update(base64.b64decode(ciphertext_b64)) + wrong_decryptor.finalize()
        assert wrong_plaintext != plaintext

    def test_wrong_key_length_raises(self) -> None:
        with pytest.raises(KeyMaterialError):
            decrypt_version_spec("", "some-uid", b"short")

    def test_invalid_base64_raises_binascii_error(self) -> None:
        with pytest.raises(binascii.Error):
            decrypt_version_spec("not-valid-base64!!!", "some-uid", os.urandom(AES_KEY_SIZE))

    def test_decrypting_non_utf8_plaintext_raises_unicode_decode_error(self) -> None:
        # decrypt_version_spec's own documented failure mode for a wrong
        # key/corrupt input: the AES-CTR output decodes fine as bytes but
        # is not valid UTF-8. Round-tripped for real (rather than guessing
        # at what a wrong key happens to produce) so the plaintext handed
        # to .decode("utf-8") is deterministically invalid: 0x80 alone is a
        # continuation byte with no leading byte, never valid UTF-8.
        key = os.urandom(AES_KEY_SIZE)
        version_uid = "some-uid"
        encryptor = Cipher(algorithms.AES(key), modes.CTR(version_spec_iv(version_uid))).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(b"\x80") + encryptor.finalize()).decode("ascii")
        with pytest.raises(UnicodeDecodeError):
            decrypt_version_spec(ciphertext_b64, version_uid, key)


class TestParseKeyString:
    def test_valid_key_string(self) -> None:
        user_key_id, user_key = parse_key_string("n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM=")
        assert user_key_id == "n0wohSZahiKc"
        assert len(user_key) == 32

    def test_missing_at_sign_raises(self) -> None:
        with pytest.raises(KeyMaterialError):
            parse_key_string("no-at-sign-here")

    def test_wrong_length_user_key_id_raises(self) -> None:
        with pytest.raises(KeyMaterialError):
            parse_key_string("shortid@" + "AA==")

    def test_invalid_base64_raises(self) -> None:
        with pytest.raises(KeyMaterialError):
            parse_key_string("abcdefghijkl@not-valid-base64!!!")

    def test_wrong_decoded_length_raises(self) -> None:
        import base64

        short_key_b64 = base64.b64encode(b"too short").decode()
        with pytest.raises(KeyMaterialError):
            parse_key_string(f"abcdefghijkl@{short_key_b64}")

    def test_splits_on_last_at_sign(self) -> None:
        import base64

        user_key = os.urandom(32)
        b64 = base64.b64encode(user_key).decode()
        # userKeyID itself contains an '@' — rsplit(..., 1) must still find
        # the correct boundary (the *last* '@').
        user_key_id_with_at = "ab@cdefghijk"
        assert len(user_key_id_with_at) == 12
        parsed_id, parsed_key = parse_key_string(f"{user_key_id_with_at}@{b64}")
        assert parsed_id == user_key_id_with_at
        assert parsed_key == user_key


class TestUnwrapVaultKey:
    def test_round_trip(self) -> None:
        user_key = os.urandom(32)
        user_key_id = "abcdefghijkl"
        vault_key = os.urandom(32)

        nonce = user_key_id.encode("ascii")
        wrapped = AESGCM(user_key).encrypt(nonce, vault_key, None)

        assert unwrap_vault_key(user_key_id, user_key, wrapped) == vault_key

    def test_wrong_user_key_raises_key_mismatch(self) -> None:
        user_key_id = "abcdefghijkl"
        wrapped = AESGCM(os.urandom(32)).encrypt(user_key_id.encode("ascii"), os.urandom(32), None)
        with pytest.raises(KeyMismatchError):
            unwrap_vault_key(user_key_id, os.urandom(32), wrapped)

    def test_wrong_user_key_id_raises_key_mismatch(self) -> None:
        # correct key, but nonce (derived from userKeyID) does not match
        # what it was wrapped with
        user_key = os.urandom(32)
        wrapped = AESGCM(user_key).encrypt(b"originalid12", os.urandom(32), None)
        with pytest.raises(KeyMismatchError):
            unwrap_vault_key("differentid1", user_key, wrapped)

    def test_wrong_length_inputs_raise_key_material_error(self) -> None:
        with pytest.raises(KeyMaterialError):
            unwrap_vault_key("abcdefghijkl", b"short", b"\x00" * 48)
        with pytest.raises(KeyMaterialError):
            unwrap_vault_key("short", os.urandom(32), b"\x00" * 48)
        with pytest.raises(KeyMaterialError):
            unwrap_vault_key("abcdefghijkl", os.urandom(32), b"\x00" * 10)


class TestAhltDecrypt:
    def _build_ahlt(self, vault_key: bytes, plaintext: bytes) -> bytes:
        iv = os.urandom(16)
        encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(iv)).encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()

        header = bytearray(64)
        header[0:4] = b"aHlT"
        header[8:24] = iv
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        return bytes(header) + ciphertext

    def test_round_trip(self) -> None:
        vault_key = os.urandom(32)
        plaintext = b"SQLite format 3\x00" + os.urandom(4096)
        data = self._build_ahlt(vault_key, plaintext)

        assert ahlt_decrypt(data, vault_key) == plaintext

    def test_bad_magic_raises(self) -> None:
        vault_key = os.urandom(32)
        data = bytearray(self._build_ahlt(vault_key, b"x" * 100))
        data[0:4] = b"XXXX"
        with pytest.raises(DataCorruptError):
            ahlt_decrypt(bytes(data), vault_key)
