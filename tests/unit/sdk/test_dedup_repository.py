"""Unit tests for ``synology_apm_repo.sdk.dedup.repository`` — synthetic
repository roots written to real files, no sample repositories
required."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sqlite3
import struct
import zlib
from pathlib import Path

import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from synology_apm_repo.sdk.asynccache import AsyncKeyedCache
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, KeyMismatchError, KeyRequiredError, NotFoundError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS, MODE_VAULT_ENCRYPT
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.crypto import chunk_iv
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.format.repo_info import MAGIC as REPO_INFO_MAGIC
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, CompOffset, SessionId, StreamId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.sqlite_source import SqliteSource

_STREAM_ID = StreamId(7)
_SESSION_ID = SessionId(3)
_HEAD_OFF = CompOffset(64)
_PATH = "VM-abc/2026-08-06/disk.img"
_PLAINTEXT = bytes([1]) * 4096


def _write_repo_info(path: Path, *, uuid: str = "abcdefghijklmnop") -> None:
    payload_obj = {
        "repo_type": 2,
        "repo_flag": 0,
        "is_global_dedup_supported": True,
        "is_worm_supported": False,
        "storage_algorithm": {"compress_algorithm": 1, "encrypt_algorithm": 0},
    }
    payload = json.dumps(payload_obj).encode("utf-8")
    header = bytearray(64)
    header[0:4] = REPO_INFO_MAGIC
    header[4:6] = (2).to_bytes(2, "big")
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[20:36] = uuid.encode("ascii")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + payload)


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


def _write_file_map(path: Path, rows: list[tuple[str, int, int, int, int, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE file_map(path TEXT PRIMARY KEY, crtime DATETIME, mtime DATETIME, "
        "stream_id INTEGER, session_id INTEGER, comp_offset INTEGER, block INTEGER, status INTEGER)"
    )
    conn.executemany(
        "INSERT INTO file_map(path, stream_id, session_id, comp_offset, block, status) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _write_file_meta(path: Path, rows: list[tuple[str, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE file_meta(path TEXT, file_size INTEGER)")
    conn.executemany("INSERT INTO file_meta(path, file_size) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    idx_bytes = file_chunk_idx.to_bytes(7, "big")
    return bytes([type_byte]) + idx_bytes + addr_int.to_bytes(8, "big") + tail_u32.to_bytes(4, "big")


def _mapping_record(file_offset: int, bucket_id: int, chunk_idx: int, map_num: int = 1) -> bytes:
    addr_int = (bucket_id << 16) | chunk_idx  # stream_id=0 implicitly
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value,
        file_chunk_idx=file_offset >> 12,
        addr_int=addr_int,
        tail_u32=map_num << 16,
    )


def _composition_header_bytes() -> bytes:
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = SUB_FILE_SIZE.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _record_head_bytes(*, map_num: int, mode: int = 0x0001) -> bytes:
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = map_num.to_bytes(8, "big")
    head[18:20] = mode.to_bytes(2, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(head)


def _write_composition(root: Path) -> None:
    entries = _mapping_record(0, 0, 0, map_num=1)
    record_bytes = _record_head_bytes(map_num=1) + entries
    path = root / str(_STREAM_ID) / f"{_SESSION_ID}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_composition_header_bytes() + record_bytes)


def _encode_size_store(entries: list[tuple[int, int]]) -> bytes:
    n = len(entries)
    tight_len = (n * 15 + 7) >> 3
    buf = bytearray(tight_len + 4)
    for idx, (type_value, size) in enumerate(entries):
        bit_off = idx * 15
        byte_off = bit_off >> 3
        bit_shift = 17 - (bit_off & 7)
        blob = (type_value << 12) | size
        window = int.from_bytes(buf[byte_off : byte_off + 4], "big")
        window |= (blob << bit_shift) & 0xFFFFFFFF
        buf[byte_off : byte_off + 4] = window.to_bytes(4, "big")
    return bytes(buf[:tight_len])


def _write_bucket(path: Path, plaintext: bytes) -> None:
    compressed = zstandard.ZstdCompressor().compress(plaintext)
    tight = _encode_size_store([(CompressType.ZSTD.value, len(compressed))])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", 1)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    trailer = os.urandom(4 + redundancy_size((15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region_pad(tight) + compressed + trailer)


def _write_encrypted_bucket(path: Path, plaintext: bytes, vault_key: bytes, *, bucket_id: int = 0) -> None:
    """Same shape as ``_write_bucket``, real AES-CTR encrypted under
    ``vault_key`` and flagged ``MODE_VAULT_ENCRYPT``."""
    compressed = zstandard.ZstdCompressor().compress(plaintext)
    addr = ChunkAddress(StreamId(0), BucketId(bucket_id), ChunkIdx(0))
    encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(chunk_iv(addr))).encryptor()
    ciphertext = encryptor.update(compressed) + encryptor.finalize()
    tight = _encode_size_store([(CompressType.ZSTD.value, len(ciphertext))])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC | MODE_VAULT_ENCRYPT)
    header[12:16] = struct.pack(">I", 1)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    trailer = os.urandom(4 + redundancy_size((15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region_pad(tight) + ciphertext + trailer)


def _build_full_repo(tmp_path: Path, *, encryption_user_key_uuid: str = "NoEncryption") -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [(encryption_user_key_uuid, "")])
    _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
    _write_file_meta(tmp_path / "db" / "file_meta", [(_PATH, 4096)])
    _write_composition(tmp_path / "@data" / "Composition")
    _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", _PLAINTEXT)


@pytest.fixture
def vault_layout() -> RepoLayout:
    return RepoLayout(kind=RepoKind.VAULT, repo_root="")


class TestOpen:
    async def test_opens_and_reads_repo_info(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert repo.info.uuid == "abcdefghijklmnop"
            assert repo.info.repo_type == 2

    async def test_resolves_sequence_suffixed_repo_info(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        (tmp_path / "repo_info").rename(tmp_path / "repo_info.5")
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert repo.info.uuid == "abcdefghijklmnop"

    async def test_no_encryption_never_touches_the_key_db(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _write_repo_info(tmp_path / "repo_info")
        # deliberately no db/vault_encryption_key at all — open() must not
        # need it when keys.is_no_encryption is True.
        store = _SpyStore(LocalFsStore(tmp_path))
        keys = KeyMaterial(user_key_id="NoEncryption", user_key=b"\x00" * 32)
        async with await DedupRepo.open(store, vault_layout, keys) as repo:
            assert repo.info.uuid == "abcdefghijklmnop"
            assert not any("vault_encryption_key" in touched for touched in store.touched_paths)

    async def test_missing_keys_argument_defers_failure_to_first_read(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # open() with keys=None never raises even for an encrypted repository —
        # KeyRequiredError only surfaces later, from Pool.read_chunk(). Build a
        # bucket whose header marks it vault-encrypted; the stored bytes
        # never actually get decrypted or decompressed, since read_chunk()
        # raises as soon as it sees a vault-encrypted chunk with no key.
        _write_repo_info(tmp_path / "repo_info")
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        _write_file_meta(tmp_path / "db" / "file_meta", [(_PATH, 4096)])
        _write_composition(tmp_path / "@data" / "Composition")

        # The vault key here is never actually used to decrypt anything —
        # KeyRequiredError fires before read_chunk() ever gets that far — so
        # any random 32 bytes stand in for it.
        _write_encrypted_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", _PLAINTEXT, os.urandom(32))

        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout, keys=None) as repo:
            with pytest.raises(KeyRequiredError):
                await (await repo.open_file(_PATH)).read(0, 4096)

    async def test_missing_wrapped_key_raises_key_required(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        store = LocalFsStore(tmp_path)
        keys = KeyMaterial(user_key_id="abcdefghijkl", user_key=os.urandom(32))
        with pytest.raises(KeyRequiredError):
            await DedupRepo.open(store, vault_layout, keys)

    async def test_wrong_key_propagates_key_mismatch(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _write_repo_info(tmp_path / "repo_info")
        user_key_id = "abcdefghijkl"
        correct_user_key = os.urandom(32)
        vault_key = os.urandom(32)
        nonce = user_key_id.encode("ascii")[:12]
        wrapped = AESGCM(correct_user_key).encrypt(nonce, vault_key, None)
        _write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key", [(user_key_id, base64.b64encode(wrapped).decode())]
        )
        store = LocalFsStore(tmp_path)
        wrong_keys = KeyMaterial(user_key_id=user_key_id, user_key=os.urandom(32))
        with pytest.raises(KeyMismatchError):
            await DedupRepo.open(store, vault_layout, wrong_keys)

    async def test_correct_key_enables_reading_encrypted_content(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        user_key_id = "abcdefghijkl"
        user_key = os.urandom(32)
        vault_key = os.urandom(32)
        nonce = user_key_id.encode("ascii")[:12]
        wrapped = AESGCM(user_key).encrypt(nonce, vault_key, None)

        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(
            tmp_path / "db" / "vault_encryption_key", [(user_key_id, base64.b64encode(wrapped).decode())]
        )
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        _write_file_meta(tmp_path / "db" / "file_meta", [(_PATH, 4096)])
        _write_composition(tmp_path / "@data" / "Composition")
        _write_encrypted_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", _PLAINTEXT, vault_key)

        store = LocalFsStore(tmp_path)
        keys = KeyMaterial(user_key_id=user_key_id, user_key=user_key)
        async with await DedupRepo.open(store, vault_layout, keys) as repo:
            assert await (await repo.open_file(_PATH)).read(0, 4096) == _PLAINTEXT


def sizestore_region_pad(tight: bytes) -> bytes:
    return tight + b"\x00" * (16320 - len(tight))


class TestProbeEncrypted:
    """``DedupRepo.probe_encrypted`` — cheap, keyless, never raises
    ``KeyRequiredError`` (the SDK-side half of refusing to list an encrypted
    vault's workloads and asking for the encryption key up front — the
    browser needs to know *before* trying to load a workload list, not
    after hitting ``KeyRequiredError`` three clicks deep).

    Reads ``db/vault_encryption_key`` directly now (see ``probe_encrypted``'s
    own docstring for why) rather than opening a real bucket's header — the
    bucket this module's other tests write (``_write_bucket``) never
    carries a real vault-encrypted header at all, so these tests vary the
    db row instead of the bucket bytes."""

    async def test_returns_false_when_the_key_record_says_no_encryption(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)  # default: encryption_user_key_uuid="NoEncryption"
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert await repo.probe_encrypted() is False

    async def test_returns_true_when_the_key_record_names_a_real_key(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path, encryption_user_key_uuid="rEalUserKeyID")
        store = LocalFsStore(tmp_path)
        # keys=None — the whole point: probe_encrypted() must not need one.
        async with await DedupRepo.open(store, vault_layout, keys=None) as repo:
            assert await repo.probe_encrypted() is True

    async def test_returns_none_when_the_encryption_key_record_is_entirely_absent(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # repo_info only — no db/vault_encryption_key at all (should not
        # happen for a properly initialized repo). None is a genuinely
        # different, honest answer from False — "couldn't tell", not
        # "confirmed not encrypted" — not an error either.
        _write_repo_info(tmp_path / "repo_info")
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert await repo.probe_encrypted() is None

    async def test_result_is_cached_for_the_repository_lifetime(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        spy = _SpyStore(LocalFsStore(tmp_path))
        async with await DedupRepo.open(spy, vault_layout) as repo:
            first = await repo.probe_encrypted()
            touched_after_first = list(spy.touched_paths)
            second = await repo.probe_encrypted()
            assert second == first
            # The second call must not touch the store again at all — same
            # "cheap enough to call more than once without worrying about
            # cost" contract the docstring promises.
            assert spy.touched_paths == touched_after_first


class TestDb:
    async def test_returns_a_working_connection(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            conn = await repo.db("file_map")
            cursor = await conn.execute("SELECT path FROM file_map")
            row = await cursor.fetchone()
            assert row == (_PATH,)

    async def test_caches_the_connection(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            first = await repo.db("file_map")
            second = await repo.db("file_map")
            assert first is second

    async def test_resolves_sequence_suffixed_db_file(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        (tmp_path / "db" / "file_map").rename(tmp_path / "db" / "file_map.42")
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            conn = await repo.db("file_map")
            cursor = await conn.execute("SELECT path FROM file_map")
            row = await cursor.fetchone()
            assert row == (_PATH,)

    async def test_concurrent_opens_for_the_same_name_build_the_source_only_once(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_db_sources`` is an ``AsyncKeyedCache`` (see its own docstring
        for the in-flight de-dup mechanism this exercises): concurrent
        misses on the *same* name must build exactly one ``SqliteSource``,
        with every caller sharing that one cached connection afterwards."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            builds = 0
            real_from_raw_store = SqliteSource.from_raw_store.__func__  # type: ignore[attr-defined]

            async def counting_from_raw_store(cls: type[SqliteSource], store: ObjectStore, path: str) -> SqliteSource:
                nonlocal builds
                builds += 1
                return await real_from_raw_store(cls, store, path)  # type: ignore[no-any-return]

            monkeypatch.setattr(SqliteSource, "from_raw_store", classmethod(counting_from_raw_store))

            connections = await asyncio.gather(*(repo.db("file_map") for _ in range(10)))

            assert builds == 1
            assert all(c is connections[0] for c in connections)
            assert len(repo._db_sources) == 1

    async def test_concurrent_opens_for_different_names_do_not_serialize_on_each_other(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test for the AsyncKeyedCache migration: unlike a
        single lock guarding the whole ``_db_sources`` dict, two different
        names' fetches must never block behind each other — only two
        misses on the *same* name contend (the sibling test above)."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            file_map_started = asyncio.Event()
            release_file_map = asyncio.Event()
            real_from_raw_store = SqliteSource.from_raw_store.__func__  # type: ignore[attr-defined]

            async def blocking_from_raw_store(cls: type[SqliteSource], store: ObjectStore, path: str) -> SqliteSource:
                if path.endswith("file_map"):
                    file_map_started.set()
                    await release_file_map.wait()
                return await real_from_raw_store(cls, store, path)  # type: ignore[no-any-return]

            monkeypatch.setattr(SqliteSource, "from_raw_store", classmethod(blocking_from_raw_store))

            file_map_task = asyncio.create_task(repo.db("file_map"))
            await asyncio.wait_for(file_map_started.wait(), timeout=1)
            # file_map's own fetch is still parked on release_file_map — a
            # different name must resolve without waiting behind it.
            await asyncio.wait_for(repo.db("file_meta"), timeout=1)
            release_file_map.set()
            await file_map_task


class TestLocateFile:
    async def test_returns_the_expected_triple_and_size(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.stream_id == _STREAM_ID
            assert loc.session_id == _SESSION_ID
            assert loc.comp_offset == _HEAD_OFF
            assert loc.file_size == 4096

    async def test_missing_path_raises_not_found(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            with pytest.raises(NotFoundError):
                await repo.locate_file("no/such/path")

    async def test_deleted_renamed_path_still_resolves_via_its_own_triple(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        deleted_path = f"{_PATH}_deleted_1699999999000"
        _write_file_map(tmp_path / "db" / "file_map", [(deleted_path, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        _write_composition(tmp_path / "@data" / "Composition")
        _write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", _PLAINTEXT)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(deleted_path)
            assert loc.stream_id == _STREAM_ID
            assert loc.session_id == _SESSION_ID
            assert loc.comp_offset == _HEAD_OFF
            assert await (await repo.open_file(deleted_path)).read(0, 4096) == _PLAINTEXT

    async def test_missing_file_meta_db_file_gives_none_size_not_a_crash(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        # no file_meta db file at all — the NotFoundError branch of _file_size_from_meta.
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    async def test_file_meta_db_exists_but_lacks_the_file_meta_table_gives_none_size(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # A genuinely different failure mode from the "db file doesn't
        # exist at all" case above — this repository shape *has* a db/file_meta
        # file, it just doesn't contain a file_meta table (some other
        # table, or none). Table.exists_in()'s branch, not NotFoundError's.
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        (tmp_path / "db").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp_path / "db" / "file_meta")
        conn.execute("CREATE TABLE some_other_table(x INTEGER)")
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    async def test_file_meta_table_has_no_row_for_this_path_gives_none_size(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # A fourth, still-different failure mode: the file_meta table
        # exists and has rows, just none for this exact path.
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        _write_file_meta(tmp_path / "db" / "file_meta", [("some/other/path", 4096)])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    async def test_file_meta_table_exists_but_lacks_file_size_column_gives_none_size(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # A third, still-different failure mode: the file_meta table
        # exists (with ``path``) but this repository shape's schema never grew a
        # file_size column — Table's own missing-optional-column
        # handling, not Table.exists_in() and not NotFoundError.
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        (tmp_path / "db").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp_path / "db" / "file_meta")
        conn.execute("CREATE TABLE file_meta(path TEXT)")
        conn.execute("INSERT INTO file_meta(path) VALUES (?)", (_PATH,))
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    async def test_file_meta_row_with_null_file_size_gives_none_size(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2)])
        (tmp_path / "db").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp_path / "db" / "file_meta")
        conn.execute("CREATE TABLE file_meta(path TEXT, file_size INTEGER)")
        conn.execute("INSERT INTO file_meta(path, file_size) VALUES (?, NULL)", (_PATH,))
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            loc = await repo.locate_file(_PATH)
            assert loc.file_size is None

    @pytest.mark.parametrize("status", [0, 1, 3])
    async def test_not_yet_or_no_longer_complete_status_raises_not_found(
        self, tmp_path: Path, vault_layout: RepoLayout, status: int
    ) -> None:
        # Initialized (0) / Written (1) / Compacted (3): not documented as
        # "known-bad" the way Corrupted/Tainted are, but also not the one
        # trustworthy Complete (2) value — FORMAT-SPEC.md: file_map-status.
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, status)])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            with pytest.raises(NotFoundError):
                await repo.locate_file(_PATH)

    @pytest.mark.parametrize("status", [4, 5])
    async def test_known_bad_status_raises_data_corrupt(
        self, tmp_path: Path, vault_layout: RepoLayout, status: int
    ) -> None:
        # Corrupted (4) / Tainted (5): FORMAT-SPEC.md's own "known-bad"
        # values — never returned as a readable FileLocation.
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, status)])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            with pytest.raises(DataCorruptError):
                await repo.locate_file(_PATH)


class TestFileMapPathsWithPrefix:
    async def test_status_filter_excludes_non_matching_rows(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                ("prefix/a", _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 2),
                ("prefix/b", _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 1),
            ],
        )
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert await repo.file_map_paths_with_prefix("prefix/") == ["prefix/a", "prefix/b"]
            assert await repo.file_map_paths_with_prefix("prefix/", status=2) == ["prefix/a"]
            assert await repo.file_map_paths_with_prefix("prefix/", status=1) == ["prefix/b"]
            assert await repo.file_map_paths_with_prefix("prefix/", status=4) == []


class TestOpenFileAndComposition:
    async def test_open_file_reads_real_content(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            f = await repo.open_file(_PATH)
            assert f.size == 4096
            assert await f.read(0, 4096) == _PLAINTEXT

    async def test_open_file_surfaces_the_same_status_gate_as_locate_file(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        # open_file() has no status check of its own -- it must be getting
        # this for free from locate_file(), the single choke point both go
        # through.
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key", [("NoEncryption", "")])
        _write_file_map(tmp_path / "db" / "file_map", [(_PATH, _STREAM_ID, _SESSION_ID, _HEAD_OFF, 1, 4)])
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            with pytest.raises(DataCorruptError):
                await repo.open_file(_PATH)

    async def test_open_composition_bypasses_file_map(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            f = repo.open_composition(_STREAM_ID, _SESSION_ID, _HEAD_OFF, size=4096)
            assert await f.read(0, 4096) == _PLAINTEXT


class TestCloseAndContextManager:
    async def test_close_clears_connection_cache(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        repo = await DedupRepo.open(store, vault_layout)
        await repo.db("file_map")
        await repo.close()
        assert repo._db_sources == {}

    async def test_used_as_a_context_manager(self, tmp_path: Path, vault_layout: RepoLayout) -> None:
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        async with await DedupRepo.open(store, vault_layout) as repo:
            assert await (await repo.open_file(_PATH)).read(0, 4096) == _PLAINTEXT
        assert repo._db_sources == {}

    async def test_a_db_call_still_in_flight_when_close_runs_still_gets_closed(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        """Same in-flight race as ``api.Repository.close()``'s own
        ``AsyncKeyedCache.settle_all()``-based fix (``test_api.py``) --
        a ``db()`` call already in flight (its own ``SqliteSource`` open
        not yet settled) when ``close()`` runs must still have that
        connection closed once it lands, not left invisible to a plain
        ``_db_sources.values()`` snapshot -- see
        ``AsyncKeyedCache.known_keys()``'s own docstring for why."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        repo = await DedupRepo.open(store, vault_layout)

        started = asyncio.Event()
        release = asyncio.Event()
        created: list[SqliteSource] = []
        real_build = repo._build_db_source

        async def _slow_build(name: str) -> SqliteSource:
            started.set()
            await release.wait()
            source = await real_build(name)
            created.append(source)
            return source

        # A fresh cache bound to the slow fetch -- db()'s own resolve()
        # call passes no per-call override, so the fetch has to be rebound
        # here rather than monkeypatched on the instance (AsyncKeyedCache
        # captures whatever bound method it's constructed with).
        repo._db_sources = AsyncKeyedCache(_slow_build)

        resolve_task = asyncio.create_task(repo.db("file_map"))
        await started.wait()  # the open is in flight (owner determined), not yet settled

        close_task = asyncio.create_task(repo.close())
        await asyncio.sleep(0)  # let close() take its known_keys() snapshot while still in flight
        release.set()  # let the open finish

        await resolve_task
        await close_task

        assert len(created) == 1
        assert created[0]._closed is True
        assert repo._db_sources == {}

    async def test_close_reports_but_does_not_abort_when_one_source_fails_to_close(
        self, tmp_path: Path, vault_layout: RepoLayout, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same "attempt all, then report" posture as
        ``api.Repository.close()`` -- one source failing (or hanging) to
        close must not stop every other already-opened source from
        getting its own close attempt, and the failure must still
        surface via ``ExceptionGroup`` rather than be silently
        swallowed."""
        _build_full_repo(tmp_path)
        store = LocalFsStore(tmp_path)
        repo = await DedupRepo.open(store, vault_layout)
        await repo.db("file_map")
        await repo.db("file_meta")

        failing_source = repo._db_sources["file_map"]
        other_source = repo._db_sources["file_meta"]

        async def _failing_close() -> None:
            raise RuntimeError("synthetic close failure")

        monkeypatch.setattr(failing_source, "close", _failing_close)

        with pytest.raises(ExceptionGroup) as exc_info:
            await repo.close()
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], RuntimeError)
        # the other source still got its own close attempt despite the
        # first one's failure, and the cache itself was still cleared.
        assert other_source._closed is True
        assert repo._db_sources == {}


async def test_bad_repo_info_magic_raises_data_corrupt(tmp_path: Path, vault_layout: RepoLayout) -> None:
    path = tmp_path / "repo_info"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"X" * 64)
    store = LocalFsStore(tmp_path)
    with pytest.raises(DataCorruptError):
        await DedupRepo.open(store, vault_layout)


class _SpyStore:
    """Wraps a real ``ObjectStore``, recording every path passed to
    ``read``/``listdir``/``exists`` — used to prove *absence* of reads
    against specific paths, not just that the right bytes come back."""

    def __init__(self, backing: LocalFsStore) -> None:
        self._backing = backing
        self.touched_paths: list[str] = []

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self.touched_paths.append(path)
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        self.touched_paths.append(path)
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        self.touched_paths.append(path)
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        self.touched_paths.append(path)
        return await self._backing.listdir(path)


class TestRestorePathNeverTouchesAuxiliaryFiles:
    async def test_ref_hot_and_sample_index_files_are_never_read(
        self, tmp_path: Path, vault_layout: RepoLayout
    ) -> None:
        _build_full_repo(tmp_path)
        (tmp_path / "@data" / "Pool" / "0" / "0.ref").write_bytes(b"bRfC" + b"\x00" * 60)
        (tmp_path / "@data" / "sample.index").write_bytes(b"sMPl" + b"\x00" * 60)
        (tmp_path / "@data" / "Hot").mkdir(parents=True, exist_ok=True)
        (tmp_path / "@data" / "Hot" / "1.idx").write_bytes(b"HoOt" + b"\x00" * 60)
        store = LocalFsStore(tmp_path)
        spy = _SpyStore(store)
        async with await DedupRepo.open(spy, vault_layout) as repo:
            content = await (await repo.open_file(_PATH)).read(0, 4096)

            assert content == _PLAINTEXT
            forbidden = (".ref", "sample.index", "Hot/")
            assert not any(pat in touched for touched in spy.touched_paths for pat in forbidden)
