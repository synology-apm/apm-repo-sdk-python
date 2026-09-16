"""Unit tests for ``synology_apm_repo.sdk.units.file_map_tree`` —
synthetic repository roots written to real files, no sample repositories
required (see ``tests/integration/sdk/test_units_file_map_tree.py``
for the real-apv-sample-3 cross-check)."""

from __future__ import annotations

import json
import os
import sqlite3
import struct
import zlib
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.file_map_tree import FileMapTreeProvider

_STREAM_ID = 4
_PLAINTEXT = (b"leaf-content----" * 256)[:4096]
assert len(_PLAINTEXT) == 4096


def _write_repo_info(path: Path) -> None:
    payload = json.dumps({"repo_type": 2}).encode("utf-8")
    header = bytearray(64)
    header[0:4] = b"RpiF"
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[20:36] = b"a" * 16
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + payload)


def _write_vault_encryption_key_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE vault_encryption_key(user_key_uuid TEXT UNIQUE, encrypted_data_key TEXT)")
    conn.execute("INSERT INTO vault_encryption_key VALUES ('NoEncryption', '')")
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
    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    trailer = os.urandom(4 + redundancy_size((15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + compressed + trailer)


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    return (
        bytes([type_byte])
        + file_chunk_idx.to_bytes(7, "big")
        + addr_int.to_bytes(8, "big")
        + tail_u32.to_bytes(4, "big")
    )


def _write_composition(root: Path, *, stream_id: int, session_id: int) -> None:
    addr_int = ChunkAddress(StreamId(stream_id), BucketId(0), ChunkIdx(0)).to_int()
    entry = _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value, file_chunk_idx=0, addr_int=addr_int, tail_u32=1 << 16
    )
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = (1).to_bytes(8, "big")
    head[18:20] = (1).to_bytes(2, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    record_bytes = bytes(head) + entry

    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = SUB_FILE_SIZE.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    path = root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + record_bytes)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[DedupRepo]:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_file_map(
        tmp_path / "db" / "file_map",
        [
            ("WORKLOAD-a/2026-01-01/fileA.txt", _STREAM_ID, 9, 64, 1, 2),
            ("WORKLOAD-a/2026-01-01/sub/fileB.txt", _STREAM_ID, 9, 64, 1, 2),
            ("WORKLOAD-b/2026-01-01/fileC.txt", _STREAM_ID, 9, 64, 1, 2),
        ],
    )
    _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
    _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _PLAINTEXT)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as r:
        yield r


class TestTree:
    async def test_root_lists_top_level_workload_dirs(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        top = await provider.children(provider.root())
        assert {n.name for n in top} == {"WORKLOAD-a", "WORKLOAD-b"}
        assert all(not n.is_leaf for n in top)

    async def test_drills_down_through_intermediate_directories(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        date_dir = (await provider.children(wl_a))[0]
        assert date_dir.name == "2026-01-01"
        entries = await provider.children(date_dir)
        assert {n.name for n in entries} == {"fileA.txt", "sub"}
        file_a = next(n for n in entries if n.name == "fileA.txt")
        sub_dir = next(n for n in entries if n.name == "sub")
        assert file_a.is_leaf is True
        assert file_a.kind is UnitKind.RAW_OBJECT
        assert sub_dir.is_leaf is False

    async def test_children_of_a_leaf_is_empty(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        date_dir = (await provider.children(wl_a))[0]
        file_a = next(n for n in await provider.children(date_dir) if n.name == "fileA.txt")
        assert await provider.children(file_a) == []

    async def test_unrecognized_node_children_is_empty(self, repo: DedupRepo) -> None:
        from synology_apm_repo.sdk.units.base import Node
        from synology_apm_repo.sdk.units.node_ref import NodeRef

        provider = FileMapTreeProvider(repo)
        mystery = Node(ref=NodeRef("repo", ("x",)), name="x", is_leaf=False, attrs={})
        assert await provider.children(mystery) == []

    async def test_node_with_a_prefix_not_present_in_the_index_is_empty(self, repo: DedupRepo) -> None:
        """Distinct from ``test_unrecognized_node_children_is_empty``
        (no "prefix" attr at all -> the earlier ``prefix is None``
        guard): this Node *has* a real "prefix" attr, it's just a
        string no real file_map row's path ever produces -- exercises
        ``_index().get(normalized_prefix, (set(), {}))``'s own fallback
        instead."""
        from synology_apm_repo.sdk.units.base import Node
        from synology_apm_repo.sdk.units.node_ref import NodeRef

        provider = FileMapTreeProvider(repo)
        bogus = Node(
            ref=NodeRef.raw("", "no/such/prefix"),
            name="no-such-prefix",
            is_leaf=False,
            attrs={"prefix": "no/such/prefix"},
        )
        assert await provider.children(bogus) == []

    async def test_paths_are_scanned_once_and_cached(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        await provider.children(provider.root())
        first = provider._paths
        await provider.children(provider.root())
        assert provider._paths is first

    async def test_child_index_is_built_once_and_cached(self, repo: DedupRepo) -> None:
        """Companion to ``test_paths_are_scanned_once_and_cached`` for the
        prefix index itself (not just the raw path list it's built
        from) — the same object must come back on a second, deeper
        ``children()`` call, not get rebuilt."""
        provider = FileMapTreeProvider(repo)
        await provider.children(provider.root())
        first = provider._child_index
        assert first is not None
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        await provider.children(wl_a)
        assert provider._child_index is first

    async def test_a_path_that_is_both_a_leaf_and_a_prefix_of_a_longer_path_yields_both_nodes(
        self, tmp_path: Path
    ) -> None:
        """A real, if unusual, file_map shape: one row's own path is
        itself an exact prefix of another row's path (e.g. an
        empty-directory object registered alongside a file nested under
        that same path) — asserts the prefix-indexed rewrite of
        ``children()`` still surfaces *both* a directory node and a leaf
        node named "dirlike", exactly as the original per-call rescan
        did (a naive "name -> is_dir" index would silently collapse one
        away)."""
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_file_map(
            tmp_path / "db" / "file_map",
            [
                ("WORKLOAD-c/dirlike", _STREAM_ID, 9, 64, 1, 2),
                ("WORKLOAD-c/dirlike/nested.txt", _STREAM_ID, 9, 64, 1, 2),
            ],
        )
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _PLAINTEXT)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            provider = FileMapTreeProvider(repo)
            wl_c = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-c")
            entries = await provider.children(wl_c)
            dirlike_entries = [n for n in entries if n.name == "dirlike"]
            assert len(dirlike_entries) == 2
            assert {n.is_leaf for n in dirlike_entries} == {True, False}

            dirlike_dir = next(n for n in dirlike_entries if n.is_leaf is False)
            nested = await provider.children(dirlike_dir)
            assert {n.name for n in nested} == {"nested.txt"}


class TestContent:
    async def test_reads_the_real_dedup_content(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        date_dir = (await provider.children(wl_a))[0]
        file_a = next(n for n in await provider.children(date_dir) if n.name == "fileA.txt")
        content = (await provider.unit(file_a)).open()
        assert await content.read(0, 4096) == _PLAINTEXT

    async def test_unit_on_a_directory_node_raises(self, repo: DedupRepo) -> None:
        provider = FileMapTreeProvider(repo)
        wl_a = next(n for n in await provider.children(provider.root()) if n.name == "WORKLOAD-a")
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(wl_a)
