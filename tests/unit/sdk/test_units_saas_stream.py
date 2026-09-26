"""Unit tests for ``synology_apm_repo.sdk.units.saas.stream`` —
synthetic repository roots written to real files, no sample repositories
required (see ``tests/integration/sdk/test_units_saas_stream.py`` for the
end-to-end cross-check against ``apv-sample-1``)."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import sqlite3
import struct
import zlib
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import (
    BucketId,
    ChunkIdx,
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamId,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.saas import stream as stream_mod
from synology_apm_repo.sdk.units.saas.stream import SaasStream, SaasStreamCache, _nearest_live_generation

_STREAM_ID = 9
_CCID = ConnectionConfigId(1)
_CONNECTION_ID = "conn-1"
_STREAM_UUID = StreamUuid("stream-uuid-1")
_SAAS_OBJ = b"saas-obj-content" * 272  # not chunk-aligned on purpose — read() must still slice correctly
assert len(_SAAS_OBJ) == 4352


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


def _write_connection_config(path: Path, rows: list[tuple[int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, connection_id TEXT)")
    conn.executemany("INSERT INTO connection_config VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _write_saas_snapshot_db(
    path: Path,
    snapshots: list[tuple[int, str, int, int]],
    distribution: list[tuple[int, int, int, int]],
) -> None:
    """``snapshots``: (snapshot_id, snapshot_uuid, first_version_id, stream_version).
    ``distribution``: (offset, length, snapshot_id, version_id)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE snapshot_info(snapshot_id INTEGER PRIMARY KEY, snapshot_uuid TEXT, "
        "first_version_id INTEGER, stream_version INTEGER)"
    )
    conn.executemany("INSERT INTO snapshot_info VALUES (?, ?, ?, ?)", snapshots)
    conn.execute(
        "CREATE TABLE snapshot_distribution(offset INTEGER, length INTEGER, snapshot_id INTEGER, version_id INTEGER)"
    )
    conn.executemany("INSERT INTO snapshot_distribution VALUES (?, ?, ?, ?)", distribution)
    conn.commit()
    conn.close()


def _write_saas_version_db(
    path: Path,
    versions: list[tuple[int, int, int, int]],
    target_type: str | None,
    *,
    latest_complete_version: int | None = None,
) -> None:
    """``versions``: (snapshot_id, version_id, stream_version, deleted).
    ``latest_complete_version`` defaults to ``max(stream_version)`` across
    ``versions`` when not given explicitly (the real writer never leaves
    it behind the highest ``version_info`` row it just committed) — pass
    it explicitly to simulate a stale/behind value (forward-resolution's
    own cap) or crash-garbage rows past it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_info(snapshot_id INTEGER, version_id INTEGER, stream_version INTEGER, deleted INTEGER)"
    )
    conn.executemany(
        "INSERT INTO version_info(snapshot_id, version_id, stream_version, deleted) VALUES (?, ?, ?, ?)", versions
    )
    conn.execute("CREATE TABLE stream_info(id INTEGER PRIMARY KEY, target_type TEXT, latest_complete_version INTEGER)")
    if target_type is not None:
        resolved_latest = (
            latest_complete_version
            if latest_complete_version is not None
            else max((v[2] for v in versions), default=None)
        )
        conn.execute(
            "INSERT INTO stream_info(id, target_type, latest_complete_version) VALUES (1, ?, ?)",
            (target_type, resolved_latest),
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


def _write_bucket(path: Path, plaintexts: list[bytes]) -> None:
    compressor = zstandard.ZstdCompressor()
    payloads = [compressor.compress(p) for p in plaintexts]
    entries = [(CompressType.ZSTD.value, len(p)) for p in payloads]
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    trailer = os.urandom(4 * len(plaintexts) + redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(payloads) + trailer)


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    return (
        bytes([type_byte])
        + file_chunk_idx.to_bytes(7, "big")
        + addr_int.to_bytes(8, "big")
        + tail_u32.to_bytes(4, "big")
    )


def _write_composition(root: Path, *, stream_id: int, session_id: int, num_chunks: int) -> None:
    addr_int = ChunkAddress(StreamId(stream_id), BucketId(0), ChunkIdx(0)).to_int()
    entry = _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value, file_chunk_idx=0, addr_int=addr_int, tail_u32=num_chunks << 16
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


def _build_saas_repo(
    tmp_path: Path,
    *,
    session_id: int = 5,
    middle_segment: str = _CONNECTION_ID,
    stream_version: int = 1,
    saas_snapshot_suffix: str = "",
) -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    saas_obj_path = f"{_STREAM_UUID}/{middle_segment}/{stream_version}/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, 2, 2)])
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(
        stream_db_dir / f"saas_snapshot{saas_snapshot_suffix}",
        snapshots=[(1, "snap-uuid-1", 3, 1)],
        distribution=[(0, len(_SAAS_OBJ), 1, 3)],
    )
    _write_saas_version_db(
        stream_db_dir / "saas_version",
        versions=[(1, 3, stream_version, 0)],
        target_type="M365",
    )

    _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=2)
    _write_bucket(
        tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk",
        [_SAAS_OBJ[0:4096], _SAAS_OBJ[4096:4352] + b"\x00" * (4096 - (len(_SAAS_OBJ) - 4096))],
    )


def _version(*, saas_version_id: int = 3, connection_config_id: int = _CCID) -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-saas"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(connection_config_id),
        target_type="M365",
        target_id=TargetId(_STREAM_UUID),
        saas_stream_uuid=StreamUuid(_STREAM_UUID),
        saas_snapshot_uuid=SnapshotUuid("snap-uuid-1"),
        saas_version_id=SaasVersionId(saas_version_id),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[DedupRepo]:
    _build_saas_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as r:
        yield r


class TestDbPathResolution:
    async def test_prefers_bare_file_over_suffixed(self, tmp_path: Path) -> None:
        """saas_version/saas_snapshot prefer the un-suffixed live
        file — the bare file wins even if a higher-numbered suffixed one
        also exists."""
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        # a decoy suffixed snapshot db with a *different* snapshot_uuid —
        # if the decoy were read instead of the bare file, stream_version_for
        # (which looks up snapshot_uuid="snap-uuid-1") would raise NotFoundError.
        _write_saas_snapshot_db(
            stream_db_dir / "saas_snapshot.5",
            snapshots=[(99, "decoy-uuid", 1, 1)],
            distribution=[],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1

    async def test_falls_back_to_largest_suffix_when_no_bare_file(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        bare = stream_db_dir / "saas_snapshot"
        suffixed = stream_db_dir / "saas_snapshot.3"
        bare.rename(suffixed)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1

    async def test_falls_back_to_suffix_when_bare_file_is_empty(self, tmp_path: Path) -> None:
        """A bare file that merely *exists* but is empty (0 bytes) — an
        empty placeholder left behind by some earlier rotation — must not
        be mistaken for a live file; resolution falls back to the largest
        suffixed generation instead, the same as when the bare file is
        absent entirely."""
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        bare = stream_db_dir / "saas_snapshot"
        suffixed = stream_db_dir / "saas_snapshot.3"
        suffixed.write_bytes(bare.read_bytes())  # the real content, at a numbered generation
        bare.write_bytes(b"")  # an empty placeholder left behind, still present
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1

    async def test_falls_back_to_suffix_when_version_bare_file_is_empty(self, tmp_path: Path) -> None:
        """Same as ``test_falls_back_to_suffix_when_bare_file_is_empty``,
        for ``saas_version`` instead of ``saas_snapshot`` — the two go
        through independent connections/caches
        (``_version_connection``/``_version_connection_via_generation_fallback``),
        so this is a genuinely separate code path, not just the same
        assertion repeated."""
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        bare = stream_db_dir / "saas_version"
        suffixed = stream_db_dir / "saas_version.7"
        suffixed.write_bytes(bare.read_bytes())
        bare.write_bytes(b"")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1


class TestVersionChain:
    async def test_stream_version_for_resolves_the_real_chain(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            assert await stream.stream_version_for(_version()) == 1

    async def test_stream_version_for_unknown_snapshot_uuid_raises_not_found(self, repo: DedupRepo) -> None:
        bad_version = _version()
        bad_version = Version(**{**bad_version.__dict__, "saas_snapshot_uuid": "no-such-uuid"})
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError):
                await stream.stream_version_for(bad_version)

    async def test_stream_version_for_unknown_version_id_raises_not_found(self, repo: DedupRepo) -> None:
        bad_version = _version(saas_version_id=999)
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError):
                await stream.stream_version_for(bad_version)

    async def test_stream_version_for_raises_not_found_when_snapshot_info_table_is_missing(
        self, tmp_path: Path
    ) -> None:
        """A stream whose ``snapshot_info`` table is entirely absent — a
        valid, openable SQLite database with zero tables, not a garbage
        file — must degrade to ``NotFoundError``, the same "no matching row"
        signal ``has_resolvable_saas_obj`` already treats as "not
        resolvable, exclude this version," not ``DataCorruptError`` propagating
        and aborting ``Repository.versions()``'s whole filtering loop for
        every other version in this stream too."""
        _build_saas_repo(tmp_path)
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        bare = stream_db_dir / "saas_snapshot"
        bare.unlink()
        conn = sqlite3.connect(bare)
        conn.execute("CREATE TABLE _placeholder(x)")
        conn.execute("DROP TABLE _placeholder")
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError):
                await stream.stream_version_for(_version())


class TestOpenSaasObj:
    async def test_opens_via_connection_id_when_that_is_the_file_map_hit(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert f.stream_id == _STREAM_ID
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ

    async def test_opens_via_connection_config_id_when_that_is_the_file_map_hit(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path, middle_segment=str(_CCID))
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ

    async def test_raises_not_found_when_neither_candidate_hits(self, tmp_path: Path) -> None:
        _build_saas_repo(tmp_path, middle_segment="some-other-connection")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError):
                await stream.open_saas_obj(_version())

    async def test_missing_connection_config_row_still_tries_the_ccid_candidate(self, tmp_path: Path) -> None:
        # middle segment is the numeric ccid, and connection_config has no
        # matching row at all (no connection_id candidate to try first).
        _build_saas_repo(tmp_path, middle_segment=str(_CCID))
        (tmp_path / "db" / "connection_config").unlink()
        conn = sqlite3.connect(tmp_path / "db" / "connection_config")
        conn.execute("CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, connection_id TEXT)")
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ


def _build_saas_repo_multi_gen(
    tmp_path: Path,
    *,
    live_stream_versions: list[int],
    latest_complete_version: int | None,
    session_id: int = 5,
    middle_segment: str = _CONNECTION_ID,
    non_complete_stream_versions: frozenset[int] = frozenset(),
) -> None:
    """Like ``_build_saas_repo`` but ``version_info`` records
    ``stream_version=1`` for ``_version()``'s default row (the
    "requested" generation) while ``file_map`` only has rows for
    ``live_stream_versions`` — the real shape left behind once older
    generations are server-side GC'd (FORMAT-SPEC.md: saas-addressing). Every live
    generation shares one physical composition/bucket (same
    stream_id/session_id) — these tests only need to prove *which*
    generation resolution picks, not that content differs across
    generations. ``non_complete_stream_versions`` gives those entries
    ``status=1`` (Written) instead of ``2`` (Complete)."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_file_map(
        tmp_path / "db" / "file_map",
        [
            (
                f"{_STREAM_UUID}/{middle_segment}/{v}/saas_obj",
                _STREAM_ID,
                session_id,
                64,
                2,
                1 if v in non_complete_stream_versions else 2,
            )
            for v in live_stream_versions
        ],
    )
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(
        stream_db_dir / "saas_snapshot",
        snapshots=[(1, "snap-uuid-1", 3, 1)],
        distribution=[(0, len(_SAAS_OBJ), 1, 3)],
    )
    _write_saas_version_db(
        stream_db_dir / "saas_version",
        versions=[(1, 3, 1, 0)],
        target_type="M365",
        latest_complete_version=latest_complete_version,
    )

    _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=2)
    _write_bucket(
        tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk",
        [_SAAS_OBJ[0:4096], _SAAS_OBJ[4096:4352] + b"\x00" * (4096 - (len(_SAAS_OBJ) - 4096))],
    )


class TestNearestLiveGeneration:
    """Pure-function tests for ``_nearest_live_generation`` — no I/O, no
    fixture, every edge case directly exercisable."""

    def test_empty_list_returns_none(self) -> None:
        assert _nearest_live_generation([], requested=1, cap=None) is None

    def test_exact_match_wins(self) -> None:
        assert _nearest_live_generation([(1, "m"), (3, "m")], requested=1, cap=None) == (1, "m")

    def test_picks_nearest_above_requested_not_the_furthest(self) -> None:
        assert _nearest_live_generation([(3, "m"), (5, "m")], requested=1, cap=None) == (3, "m")

    def test_nothing_at_or_above_requested_returns_none(self) -> None:
        assert _nearest_live_generation([(1, "m")], requested=5, cap=None) is None

    def test_candidate_beyond_cap_returns_none(self) -> None:
        assert _nearest_live_generation([(5, "m")], requested=1, cap=3) is None

    def test_candidate_at_cap_is_accepted(self) -> None:
        assert _nearest_live_generation([(3, "m")], requested=1, cap=3) == (3, "m")

    def test_tie_between_two_middles_picks_first_in_sort_order(self) -> None:
        assert _nearest_live_generation([(3, "copy"), (3, "tiering")], requested=1, cap=None) == (3, "copy")


class TestForwardResolution:
    async def test_resolves_forward_when_requested_generation_is_gone(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[3], latest_complete_version=3)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ

    async def test_picks_the_nearest_not_the_furthest_live_generation(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[3, 5], latest_complete_version=5)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            resolved = await stream._resolve_forward(1)
            assert resolved is not None and resolved[0] == 3

    async def test_a_non_complete_generation_is_skipped_for_a_later_complete_one(self, tmp_path: Path) -> None:
        """A ``file_map`` row existing isn't enough on its own — resolution
        only treats a generation as live when its own row is Complete
        (FORMAT-SPEC.md: file_map-status); stream_version=3's row here is
        Written (1), so resolution continues past it to 5."""
        _build_saas_repo_multi_gen(
            tmp_path,
            live_stream_versions=[3, 5],
            latest_complete_version=5,
            non_complete_stream_versions=frozenset({3}),
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            resolved = await stream._resolve_forward(1)
            assert resolved is not None and resolved[0] == 5

    async def test_only_a_non_complete_generation_within_cap_is_a_genuine_gap(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(
            tmp_path,
            live_stream_versions=[3],
            latest_complete_version=3,
            non_complete_stream_versions=frozenset({3}),
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError):
                await stream.open_saas_obj(_version())

    async def test_does_not_cross_latest_complete_version(self, tmp_path: Path) -> None:
        """A live file_map row exists at stream_version=5, but
        latest_complete_version=2 (simulating crash garbage left behind
        by an incomplete write that was later rolled back) — must not be
        used as a substitute."""
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[5], latest_complete_version=2)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError):
                await stream.open_saas_obj(_version())

    async def test_genuine_gap_raises_not_found_with_originally_requested_path(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[], latest_complete_version=5)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            with pytest.raises(NotFoundError) as excinfo:
                await stream.open_saas_obj(_version())
        assert f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj" in str(excinfo.value)

    async def test_generation_scan_runs_once_per_middle_across_repeated_calls(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[3], latest_complete_version=3)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            calls = 0
            real_fn = r.file_map_paths_with_prefix

            async def _counted(prefix: str, *, status: int | None = None) -> list[str]:
                nonlocal calls
                calls += 1
                return await real_fn(prefix, status=status)

            monkeypatch.setattr(r, "file_map_paths_with_prefix", _counted)
            for _ in range(3):
                f = await stream.open_saas_obj(_version())
                assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ
            # 2 candidate middles (connection_id, numeric ccid), scanned
            # once each across all three calls -- not once per call.
            assert calls == 2

    async def test_only_the_middle_with_a_live_hit_is_used(self, tmp_path: Path) -> None:
        """The Copy/connectionId middle has no live generation at all;
        the Tiering/connectionConfigId middle has one. Confirms
        forward-resolution's merge across candidate middles surfaces a
        hit from either, not just whichever is tried first."""
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_file_map(tmp_path / "db" / "file_map", [(f"{_STREAM_UUID}/{_CCID}/3/saas_obj", _STREAM_ID, 5, 64, 2, 2)])
        _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        _write_saas_snapshot_db(
            stream_db_dir / "saas_snapshot",
            snapshots=[(1, "snap-uuid-1", 3, 1)],
            distribution=[(0, len(_SAAS_OBJ), 1, 3)],
        )
        _write_saas_version_db(
            stream_db_dir / "saas_version", versions=[(1, 3, 1, 0)], target_type="M365", latest_complete_version=3
        )
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=5, num_chunks=2)
        _write_bucket(
            tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk",
            [_SAAS_OBJ[0:4096], _SAAS_OBJ[4096:4352] + b"\x00" * (4096 - (len(_SAAS_OBJ) - 4096))],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            f = await stream.open_saas_obj(_version())
            assert await f.read(0, len(_SAAS_OBJ)) == _SAAS_OBJ


class TestLastOpenResolution:
    """``SaasStream.last_open_resolution``/``SaasStreamCache.
    last_open_resolution`` — a synchronous, zero-I/O readback of the most
    recent successful ``open_saas_obj`` call's resolution, used by
    ``verify_reachable``'s label enrichment."""

    async def test_returns_none_before_any_open_call(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            assert stream.last_open_resolution(_version()) is None

    async def test_requested_equals_resolved_when_no_substitution_happened(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            await stream.open_saas_obj(_version())
            assert stream.last_open_resolution(_version()) == (1, 1)

    async def test_requested_and_resolved_differ_after_a_substitution(self, tmp_path: Path) -> None:
        _build_saas_repo_multi_gen(tmp_path, live_stream_versions=[3], latest_complete_version=3)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as r, SaasStream(r, _CCID, _STREAM_UUID) as stream:
            await stream.open_saas_obj(_version())
            assert stream.last_open_resolution(_version()) == (1, 3)

    async def test_returns_none_for_a_different_version_than_the_last_call(self, repo: DedupRepo) -> None:
        other_version = dataclasses.replace(_version(), version_uid=VersionUid("some-other-vuid"))
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            await stream.open_saas_obj(_version())
            assert stream.last_open_resolution(other_version) is None

    async def test_saas_stream_cache_mirrors_it(self, repo: DedupRepo) -> None:
        async with SaasStreamCache(repo) as cache:
            assert cache.last_open_resolution(_version()) is None
            await cache.open_saas_obj(_version())
            assert cache.last_open_resolution(_version()) == (1, 1)


class TestResourceManagement:
    async def test_close_is_idempotent_and_releases_connections(self, repo: DedupRepo) -> None:
        stream = SaasStream(repo, _CCID, _STREAM_UUID)
        await stream.stream_version_for(_version())  # opens both the snapshot and version connections
        assert stream._db_source_cache.get("saas_snapshot") is not None
        assert stream._db_source_cache.get("saas_version") is not None
        await stream.close()
        assert stream._db_source_cache.get("saas_snapshot") is None
        assert stream._db_source_cache.get("saas_version") is None
        await stream.close()  # idempotent

    async def test_context_manager_closes_on_exit(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            await stream.stream_version_for(_version())
        assert stream._db_source_cache.get("saas_snapshot") is None


class TestBoundedEviction:
    """SaasStreamCache's own bounded LRU — eviction must close the
    evicted stream's connections, not just drop the reference, or
    descriptors/threads leak. Uses a faked ``SaasStream``: cache
    mechanics need no real repository/sqlite I/O."""

    @staticmethod
    def _install_fake_stream(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, str]]:
        closed: list[tuple[int, str]] = []

        class _FakeStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                self._key = (int(ccid), str(stream_uuid))

            async def close(self) -> None:
                closed.append(self._key)

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _FakeStream)
        return closed

    async def test_maxsize_evicts_the_least_recently_used_and_closes_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=2)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))
        await cache._stream_for((2, "b"))
        assert closed == []
        await cache._stream_for((3, "c"))  # over the cap -- evicts (1, "a")
        assert closed == [(1, "a")]
        assert set(cache._streams) == {(2, "b"), (3, "c")}

    async def test_resolving_an_existing_key_refreshes_its_recency(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=2)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))
        await cache._stream_for((2, "b"))
        await cache._stream_for((1, "a"))  # touches (1, "a") again
        await cache._stream_for((3, "c"))  # must evict (2, "b"), not (1, "a")
        assert closed == [(2, "b")]

    async def test_an_evicted_key_is_reconstructed_as_a_genuinely_new_stream(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed = self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]
        first = await cache._stream_for((1, "a"))
        await cache._stream_for((2, "b"))  # evicts (1, "a")
        assert closed == [(1, "a")]
        second = await cache._stream_for((1, "a"))  # rebuilt, not silently missing
        assert second is not first

    async def test_close_closes_every_remaining_stream(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = self._install_fake_stream(monkeypatch)
        cache = SaasStreamCache(repo=None, maxsize=8)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))
        await cache._stream_for((2, "b"))
        await cache.close()
        assert sorted(closed) == [(1, "a"), (2, "b")]

    async def test_default_maxsize_is_bounded_not_unbounded(self, repo: DedupRepo) -> None:
        # The default constructor call (as _ReachabilityWalker uses it)
        # must stay bounded, not unbounded.
        cache = SaasStreamCache(repo)
        assert cache._maxsize == stream_mod._DEFAULT_STREAM_CACHE_SIZE

    async def test_eviction_close_failure_does_not_propagate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A stream that raises while being closed on eviction must not
        break the caller getting its own, newly-built stream back --
        closing a stream we're already done with is best-effort and must
        never abort an otherwise-successful run."""

        class _RaisingCloseStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                pass

            async def close(self) -> None:
                raise OSError("simulated close failure")

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _RaisingCloseStream)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]
        await cache._stream_for((1, "a"))
        new_stream = await cache._stream_for((2, "b"))  # evicts (1, "a"); its close() raises
        assert new_stream is not None
        assert set(cache._streams) == {(2, "b")}

    async def test_maxsize_below_one_is_rejected_at_construction(self) -> None:
        # A cache of size 0 would evict (and close) every stream the
        # instant it's inserted, then hand the now-closed instance back
        # to the caller as if live -- rejected up front instead.
        with pytest.raises(ValueError, match="maxsize"):
            SaasStreamCache(repo=None, maxsize=0)  # type: ignore[arg-type]


class TestStreamReuseAcrossVersions:
    """Two catalog Versions sharing one ``(connection_config_id,
    saas_stream_uuid)`` pair must resolve through one shared ``SaasStream``
    instance, not a fresh one per version."""

    async def test_two_versions_of_the_same_stream_share_one_saas_stream_instance(self, repo: DedupRepo) -> None:
        cache = SaasStreamCache(repo)
        try:
            await cache.open_saas_obj(_version())
            first = cache._streams[(int(_CCID), str(_STREAM_UUID))]
            other_version = dataclasses.replace(_version(), version_uid=VersionUid("some-other-vuid"))
            await cache.open_saas_obj(other_version)
            second = cache._streams[(int(_CCID), str(_STREAM_UUID))]
            assert first is second
        finally:
            await cache.close()


class TestConcurrentConnectionResolution:
    """Concurrent callers resolving the same not-yet-opened connection on
    one ``SaasStream`` must genuinely de-duplicate (via ``AsyncKeyedCache``'s
    own in-flight sharing), not double-open."""

    async def test_concurrent_snapshot_connection_calls_resolve_to_one_connection(self, repo: DedupRepo) -> None:
        async with SaasStream(repo, _CCID, _STREAM_UUID) as stream:
            first, second = await asyncio.gather(stream._snapshot_connection(), stream._snapshot_connection())
            assert first is second


class TestEvictionDefersForAnInUseStream:
    """``SaasStreamCache.open_saas_obj`` marks its stream in-use for the
    call's duration, so a concurrent call for a different key can't
    evict-and-close it out from under the first caller."""

    async def test_a_stream_still_mid_open_is_not_evicted_by_a_different_keys_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed: list[tuple[int, str]] = []
        entered = asyncio.Event()
        release = asyncio.Event()

        class _FakeStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                self._key = (int(ccid), str(stream_uuid))

            async def open_saas_obj(self, version: Version) -> object:
                if self._key == (1, "a"):
                    entered.set()
                    await release.wait()
                return object()

            async def close(self) -> None:
                closed.append(self._key)

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _FakeStream)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]
        version_a = dataclasses.replace(
            _version(), connection_config_id=ConnectionConfigId(1), saas_stream_uuid=StreamUuid("a")
        )
        version_b = dataclasses.replace(
            _version(), connection_config_id=ConnectionConfigId(2), saas_stream_uuid=StreamUuid("b")
        )

        task = asyncio.create_task(cache.open_saas_obj(version_a))
        try:
            await entered.wait()
            # Cache is at capacity (1) and (1, "a") is still mid-open --
            # a concurrent call for a different key must not evict it.
            await cache.open_saas_obj(version_b)
            assert closed == []
            assert (1, "a") in cache._streams
        finally:
            release.set()
            await task

    async def test_a_just_inserted_key_is_protected_even_while_its_own_eviction_is_still_closing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_in_use`` must be set before ``_stream_for`` runs, not after
        it returns, or a concurrent eviction can race a still-mid-insert
        key and evict it out from under this call."""
        closed: list[tuple[int, str]] = []
        c_close_started = asyncio.Event()
        release_c_close = asyncio.Event()

        class _FakeStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                self._key = (int(ccid), str(stream_uuid))

            async def open_saas_obj(self, version: Version) -> object:
                return object()

            async def close(self) -> None:
                if self._key == (3, "c"):
                    c_close_started.set()
                    await release_c_close.wait()
                closed.append(self._key)

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _FakeStream)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]
        # Seed the cache with (3, "c") as the sole, already-resident entry.
        await cache._stream_for((3, "c"))

        version_a = dataclasses.replace(
            _version(), connection_config_id=ConnectionConfigId(1), saas_stream_uuid=StreamUuid("a")
        )
        version_b = dataclasses.replace(
            _version(), connection_config_id=ConnectionConfigId(2), saas_stream_uuid=StreamUuid("b")
        )

        # Task A: opens A -- cache is at capacity (1), so this evicts
        # (3, "c"), whose close() blocks with (1, "a") already inserted
        # but not yet returned.
        task_a = asyncio.create_task(cache.open_saas_obj(version_a))
        try:
            await c_close_started.wait()
            assert (1, "a") in cache._streams  # inserted before the blocking close

            # Task B: opens a different key concurrently while task A is
            # still blocked in eviction. (1, "a") must already be
            # protected, or this eviction loop would treat it as free.
            await cache.open_saas_obj(version_b)
            assert (1, "a") in cache._streams  # never evicted out from under task A
            assert (3, "c") not in closed  # task A's own close() hasn't finished yet
        finally:
            release_c_close.set()
            await task_a
        assert closed == [(3, "c")]


class TestCloseRacingAnInFlightOpen:
    """``SaasStreamCache.close()`` clears ``_in_use`` unconditionally --
    it's tearing down the whole cache, not just evicting one entry -- so a
    concurrent, still in-flight ``open_saas_obj()`` call must tolerate its
    own key already being gone by the time its ``finally`` block runs."""

    async def test_close_running_concurrently_does_not_mask_the_callers_own_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Must not raise ``KeyError`` there and silently replace this
        call's real return value with an unrelated exception."""
        entered = asyncio.Event()
        release = asyncio.Event()
        completed: list[str] = []

        class _FakeStream:
            def __init__(self, repo: object, ccid: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
                pass

            async def open_saas_obj(self, version: Version) -> object:
                entered.set()
                await release.wait()
                completed.append("real result")
                return object()

            async def close(self) -> None:
                pass

        monkeypatch.setattr("synology_apm_repo.sdk.units.saas.stream.SaasStream", _FakeStream)
        cache = SaasStreamCache(repo=None, maxsize=1)  # type: ignore[arg-type]

        task = asyncio.create_task(cache.open_saas_obj(_version()))
        await entered.wait()
        await cache.close()  # races the in-flight call above, clearing _in_use
        release.set()

        await task  # must not raise KeyError -- would mask this real completion
        assert completed == ["real result"]
