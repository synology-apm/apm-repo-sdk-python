"""Unit tests for ``synology_apm_repo.sdk.format.crypto``.

Ciphertexts come from the ``cryptography`` library's encrypt side
directly, never from a helper of ``crypto.py``'s decrypt side, so the two
can't share a blind spot.
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
from synology_apm_repo.sdk.format.crypto import (
    AES_KEY_SIZE,
    ChunkDecryptor,
    ahlt_decrypt,
    build_aes_algorithm,
    chunk_iv,
    decrypt_chunk,
    decrypt_version_spec,
    is_ahlt,
    parse_key_string,
    unwrap_vault_key,
    version_spec_iv,
)
from unit.sdk.pool_fakes import chunk_address


class TestChunkIv:
    def test_iv_is_address_repeated_twice(self) -> None:
        addr = chunk_address(1, 2, 3)
        iv = chunk_iv(addr)
        assert len(iv) == 16
        assert iv[0:8] == iv[8:16]
        assert iv[0:8] == addr.to_int().to_bytes(8, "big")

    def test_different_addresses_give_different_ivs(self) -> None:
        assert chunk_iv(chunk_address(1, 2, 3)) != chunk_iv(chunk_address(1, 2, 4))


class TestDecryptChunk:
    def test_round_trip(self) -> None:
        key = os.urandom(AES_KEY_SIZE)
        addr = chunk_address(132, 330, 17)
        plaintext = os.urandom(4096)

        encryptor = Cipher(algorithms.AES(key), modes.CTR(chunk_iv(addr))).encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()

        assert decrypt_chunk(key, addr, ciphertext) == plaintext

    def test_wrong_key_length_raises(self) -> None:
        with pytest.raises(KeyMaterialError, match="vault_key must be 32 bytes, got"):
            decrypt_chunk(b"short", chunk_address(1, 1, 1), b"\x00" * 16)

    def test_accepts_a_memoryview_ciphertext_without_copying_first(self) -> None:
        key = os.urandom(AES_KEY_SIZE)
        addr = chunk_address(132, 330, 18)
        plaintext = os.urandom(4096)
        encryptor = Cipher(algorithms.AES(key), modes.CTR(chunk_iv(addr))).encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()

        buf = b"\x00" * 8 + ciphertext + b"\x00" * 8
        view = memoryview(buf)[8 : 8 + len(ciphertext)]

        assert decrypt_chunk(key, addr, view) == plaintext

    def test_wrong_key_gives_wrong_plaintext_not_an_exception(self) -> None:
        # AES-CTR has no integrity check; the ``.fgp`` fingerprint check (a
        # higher layer) is what catches a wrong key.
        key = os.urandom(AES_KEY_SIZE)
        wrong_key = os.urandom(AES_KEY_SIZE)
        addr = chunk_address(1, 1, 1)
        plaintext = os.urandom(4096)
        encryptor = Cipher(algorithms.AES(key), modes.CTR(chunk_iv(addr))).encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        assert decrypt_chunk(wrong_key, addr, ciphertext) != plaintext


class TestChunkDecryptor:
    def test_one_instance_matches_decrypt_chunk_across_many_chunks(self) -> None:
        # Lengths not a multiple of the 16-byte AES block: a re-nonce must not
        # carry the previous chunk's partial keystream block into the next.
        key = os.urandom(AES_KEY_SIZE)
        decryptor = ChunkDecryptor(key)
        for chunk_idx, length in enumerate([4096, 17, 1, 4095, 100, 4096, 33]):
            addr = chunk_address(132, 330, chunk_idx)
            ciphertext = os.urandom(length)
            assert decryptor.decrypt(addr, memoryview(ciphertext)) == decrypt_chunk(key, addr, ciphertext)

    def test_a_prebuilt_algorithm_replaces_the_key(self) -> None:
        key = os.urandom(AES_KEY_SIZE)
        addr = chunk_address(1, 2, 3)
        ciphertext = os.urandom(4096)
        decryptor = ChunkDecryptor(b"", algorithm=build_aes_algorithm(key))
        assert decryptor.decrypt(addr, ciphertext) == decrypt_chunk(key, addr, ciphertext)

    def test_wrong_key_length_raises(self) -> None:
        with pytest.raises(KeyMaterialError, match="vault_key must be 32 bytes, got"):
            ChunkDecryptor(b"short")


class TestDecryptVersionSpec:
    def test_round_trip(self) -> None:
        key = os.urandom(AES_KEY_SIZE)
        version_uid = "abcdefgh-1234-5678"
        plaintext = json.dumps({"some": "spec"}).encode("utf-8")
        encryptor = Cipher(algorithms.AES(key), modes.CTR(version_spec_iv(version_uid))).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(plaintext) + encryptor.finalize()).decode("ascii")

        assert json.loads(decrypt_version_spec(ciphertext_b64, version_uid, key)) == {"some": "spec"}

    def test_iv_is_first_16_hex_characters_of_md5_not_the_raw_digest(self) -> None:
        # Built without version_spec_iv(): MD5's raw digest is also 16
        # bytes, so a round trip through it couldn't tell the two IVs apart.
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

        wrong_decryptor = Cipher(algorithms.AES(key), modes.CTR(raw_digest[:16])).decryptor()
        wrong_plaintext = wrong_decryptor.update(base64.b64decode(ciphertext_b64)) + wrong_decryptor.finalize()
        assert wrong_plaintext != plaintext

    def test_wrong_key_length_raises(self) -> None:
        with pytest.raises(KeyMaterialError, match="vault_key must be 32 bytes, got"):
            decrypt_version_spec("", "some-uid", b"short")

    def test_invalid_base64_raises_binascii_error(self) -> None:
        with pytest.raises(binascii.Error, match="Only base64 data is allowed"):
            decrypt_version_spec("not-valid-base64!!!", "some-uid", os.urandom(AES_KEY_SIZE))

    def test_decrypting_non_utf8_plaintext_raises_unicode_decode_error(self) -> None:
        # A wrong key/corrupt input yields non-UTF-8 plaintext; a lone 0x80
        # continuation byte is deterministically invalid.
        key = os.urandom(AES_KEY_SIZE)
        version_uid = "some-uid"
        encryptor = Cipher(algorithms.AES(key), modes.CTR(version_spec_iv(version_uid))).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(b"\x80") + encryptor.finalize()).decode("ascii")
        with pytest.raises(UnicodeDecodeError, match="can't decode byte"):
            decrypt_version_spec(ciphertext_b64, version_uid, key)


class TestParseKeyString:
    def test_valid_key_string(self) -> None:
        user_key_id, user_key = parse_key_string("AliceKey0001@" + base64.b64encode(bytes(range(32))).decode("ascii"))
        assert user_key_id == "AliceKey0001"
        assert user_key == bytes(range(32))

    @pytest.mark.parametrize(
        ("key_string", "message"),
        [
            pytest.param("no-at-sign-here", "key string is not", id="missing_at_sign"),
            pytest.param("shortid@" + "AA==", "userKeyID must be 12 characters", id="wrong_length_user_key_id"),
            pytest.param("abcdefghijkl@not-valid-base64!!!", "userKey is not valid base64", id="invalid_base64"),
        ],
    )
    def test_malformed_key_string_raises(self, key_string: str, message: str) -> None:
        with pytest.raises(KeyMaterialError, match=message):
            parse_key_string(key_string)

    def test_wrong_decoded_length_raises(self) -> None:
        import base64

        short_key_b64 = base64.b64encode(b"too short").decode()
        with pytest.raises(KeyMaterialError, match="userKey must decode to"):
            parse_key_string(f"abcdefghijkl@{short_key_b64}")

    @pytest.mark.parametrize("separator", ["/", "\\"])
    @pytest.mark.parametrize("position", [0, 5, 11])
    def test_a_path_separator_in_the_user_key_id_raises(self, separator: str, position: int) -> None:
        user_key_id = "abcdefghijkl"[:position] + separator + "abcdefghijkl"[position + 1 :]
        assert len(user_key_id) == 12
        b64 = base64.b64encode(os.urandom(32)).decode()

        with pytest.raises(KeyMaterialError, match="userKeyID must not contain a path separator"):
            parse_key_string(f"{user_key_id}@{b64}")

    def test_a_non_ascii_user_key_id_raises(self) -> None:
        b64 = base64.b64encode(bytes(32)).decode()
        with pytest.raises(KeyMaterialError, match="userKeyID must be ASCII"):
            parse_key_string(f"AliceKey000é@{b64}")

    def test_splits_on_last_at_sign(self) -> None:
        import base64

        user_key = os.urandom(32)
        b64 = base64.b64encode(user_key).decode()
        # A userKeyID may itself contain '@'; the key follows the last one.
        user_key_id_with_at = "ab@cdefghijk"
        assert len(user_key_id_with_at) == 12
        parsed_id, parsed_key = parse_key_string(f"{user_key_id_with_at}@{b64}")
        assert parsed_id == user_key_id_with_at
        assert parsed_key == user_key


class TestBuildAesAlgorithm:
    def test_accepts_a_32_byte_key(self) -> None:
        algorithm = build_aes_algorithm(os.urandom(AES_KEY_SIZE))

        assert algorithm.key_size == 256

    @pytest.mark.parametrize("length", [0, 16, 24, 31, 33, 64])
    def test_any_other_key_length_raises(self, length: int) -> None:
        with pytest.raises(KeyMaterialError, match=f"vault_key must be 32 bytes, got {length}"):
            build_aes_algorithm(os.urandom(length))


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
        with pytest.raises(KeyMismatchError, match="AES-256-GCM tag check failed"):
            unwrap_vault_key(user_key_id, os.urandom(32), wrapped)

    def test_wrong_user_key_id_raises_key_mismatch(self) -> None:
        # The right key, but the nonce (from userKeyID) differs from the wrapping one.
        user_key = os.urandom(32)
        wrapped = AESGCM(user_key).encrypt(b"originalid12", os.urandom(32), None)
        with pytest.raises(KeyMismatchError, match="AES-256-GCM tag check failed"):
            unwrap_vault_key("differentid1", user_key, wrapped)

    def test_wrong_length_inputs_raise_key_material_error(self) -> None:
        with pytest.raises(KeyMaterialError, match="userKey must be"):
            unwrap_vault_key("abcdefghijkl", b"short", b"\x00" * 48)
        with pytest.raises(KeyMaterialError, match="userKeyID must be"):
            unwrap_vault_key("short", os.urandom(32), b"\x00" * 48)
        with pytest.raises(KeyMaterialError, match="wrapped VaultKey must be"):
            unwrap_vault_key("abcdefghijkl", os.urandom(32), b"\x00" * 10)

    def test_a_non_ascii_user_key_id_raises_key_material_error(self) -> None:
        with pytest.raises(KeyMaterialError, match="userKeyID must be ASCII"):
            unwrap_vault_key("abcdefghijké", os.urandom(32), b"\x00" * 48)


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
        with pytest.raises(DataCorruptError, match="bad magic"):
            ahlt_decrypt(bytes(data), vault_key)


def test_is_ahlt_checks_only_the_leading_magic() -> None:
    assert is_ahlt(b"aHlT" + b"\x00" * 60)
    assert not is_ahlt(b"\x28\xb5\x2f\xfd" + b"aHlT")
    assert not is_ahlt(b"aHl")
