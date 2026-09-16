"""Regression tests for ``--object-db-id`` at the CLI — replayed from a
committed fixture recorded against real bytes, with **no external
dependency**: same ``patch_profile_store`` fixture
(``tests/conftest.py``) this project's other CLI replay tests use — a
canonical ``cat:``/``wl:``/``ver:`` ref never needs its ``fs_path``
half at all (see ``Repository.resolve``'s own docstring:
"``ref.repo_path`` is ignored entirely"), so ``--profile`` supplying the
store in place of a real local path changes nothing about what either
scenario below exercises.

The fixture (``tests/fixtures/object_db_id_cli_teams_chat_apv1.json.gz``)
was produced once by ``RecordingStore`` wrapping
a real store rooted at ``apv-sample-1/@ActiveProtectVault``, recording the
real Teams/USER_CHAT version on stream ``uvWRSFkGxCcZAMwt``
(connection_config_id 3, workload_id 16, this specific chat version) —
the same real data
``tests/integration/sdk/test_object_db_id.py``'s own fixture covers, but
recorded through ``Session``/``Repository`` (the exact facade ``ls``
itself calls) rather than the lower-level ``raw_fallback_provider_for()``
that fixture's own recipe uses — then listing that node's children both
with no ``object_db_id`` override and with a syntactically well-formed
but content-mismatched one.

The second scenario
(``test_object_db_id_cli_end_to_end_ls_and_export_against_a_synthetic_degraded_version``)
has zero real-sample dependency in the original (a hand-built repository under
``tmp_path``, no ``samples_dir``/``ReplayStore`` involved at all) — moved
here as-is rather than left in ``tests/integration/``, which it never
needed.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import Callable
from pathlib import Path

import zstandard
from typer.testing import CliRunner

import synology_apm_repo.cli.browse as browse_mod
from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.storage.local import LocalFsStore

runner = CliRunner()

# Real values the fixture's recorded stream/version resolve to (see this
# module's own docstring): stream uvWRSFkGxCcZAMwt, connection_config_id 3,
# workload_id 16, this specific real chat version.
_CCID = 3
_WORKLOAD_ID = 16
_VERSION_UID = "882d6f32-6cab-44e1-9b5c-cbb9d1bddcd3"
_STREAM_UUID = "uvWRSFkGxCcZAMwt"


def test_object_db_id_is_ignored_by_the_cli_once_real_dispatch_succeeds_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("object_db_id_cli_teams_chat_apv1.json.gz", browse_mod, allow_content=True)

    ref = f"#cat:{_CCID}/wl:{_WORKLOAD_ID}/ver:{_VERSION_UID}"
    bogus_object_db_id = f"{_STREAM_UUID}_999999999_1234"

    plain_result = runner.invoke(app, ["--json", "ls", ref, "--profile", "anything"])
    assert plain_result.exit_code == 0, plain_result.output

    pinned_result = runner.invoke(
        app, ["--json", "ls", ref, "--profile", "anything", "--object-db-id", bogus_object_db_id]
    )
    assert pinned_result.exit_code == 0, pinned_result.output

    # identical output with and without --object-db-id — real, direct
    # proof it never reached the constructed TeamsChatProvider at all.
    assert plain_result.stdout == pinned_result.stdout


# -- synthetic fixture for the CLI end-to-end round trip (see this
# module's own docstring) — duplicated from
# tests/integration/sdk/test_api.py's _build_degraded_saas_repo rather than
# imported, matching this codebase's "tests/ isn't a package" convention.
_SYN_STREAM_ID = 41
_SYN_CCID = 6
_SYN_CONNECTION_ID = "conn-synthetic-2"
_SYN_STREAM_UUID = "synthetic-degraded-stream-2"
_SYN_UNRECOGNIZED_SUB_TYPE = "SOME_FUTURE_CONNECTOR_TYPE"


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


def _db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(path)


def _write_vault_encryption_key_db(path: Path) -> None:
    conn = _db(path)
    conn.execute("CREATE TABLE vault_encryption_key(user_key_uuid TEXT UNIQUE, encrypted_data_key TEXT)")
    conn.execute("INSERT INTO vault_encryption_key VALUES ('NoEncryption', '')")
    conn.commit()
    conn.close()


def _write_connection_config(path: Path, rows: list[tuple[int, str, int]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, "
        "connection_id TEXT, version_type INTEGER)"
    )
    conn.executemany("INSERT INTO connection_config VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_workload_config(path: Path, rows: list[tuple[int, str, str, dict[str, object]]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE workload_config(workload_id INTEGER PRIMARY KEY, workload_uid TEXT, "
        "workload_type TEXT, workload_spec TEXT)"
    )
    conn.executemany(
        "INSERT INTO workload_config VALUES (?, ?, ?, ?)",
        [(wid, uid, wtype, json.dumps(spec)) for wid, uid, wtype, spec in rows],
    )
    conn.commit()
    conn.close()


def _write_copy_target_version(path: Path, rows: list[tuple[object, ...]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE copy_target_version(version_id INTEGER PRIMARY KEY, workload_id INTEGER, "
        "connection_config_id INTEGER, version_uid TEXT, target_type TEXT, target_id TEXT, "
        "saas_stream_uuid TEXT, saas_snapshot_uuid TEXT, saas_version_id INTEGER, deleted INTEGER, "
        "version_spec TEXT)"
    )
    conn.executemany("INSERT INTO copy_target_version VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _version_spec_json(start_time: object = None, status: object = "COMPLETED") -> str:
    """Minimal real-shaped ``version_spec`` blob — just the
    ``status.start_time``/``status.status`` fields ``catalog/version.py``
    actually reads (the latter's own ``_BROWSABLE_VERSION_STATUSES``
    filter excludes any version whose status isn't confirmed
    ``COMPLETED``/``PARTIAL``/``CANCELED``, so every fixture in this file
    defaults to ``"COMPLETED"`` unless a test is deliberately checking
    that filter itself)."""
    status_obj: dict[str, object] = {}
    if start_time is not None:
        status_obj["start_time"] = str(start_time)
    if status is not None:
        status_obj["status"] = status
    return json.dumps({"status": status_obj})


def _write_file_map(path: Path, rows: list[tuple[str, int, int, int, int, int]]) -> None:
    conn = _db(path)
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


def _write_saas_snapshot_db(path: Path) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE snapshot_info(snapshot_id INTEGER PRIMARY KEY, snapshot_uuid TEXT, "
        "first_version_id INTEGER, stream_version INTEGER)"
    )
    conn.execute("INSERT INTO snapshot_info VALUES (1, 'snap-uuid', 1, 1)")
    conn.execute(
        "CREATE TABLE snapshot_distribution(offset INTEGER, length INTEGER, snapshot_id INTEGER, version_id INTEGER)"
    )
    conn.commit()
    conn.close()


def _write_saas_version_db(path: Path) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE version_info(snapshot_id INTEGER, version_id INTEGER, stream_version INTEGER, deleted INTEGER)"
    )
    conn.execute("INSERT INTO version_info VALUES (1, 1, 1, 0)")
    conn.execute("CREATE TABLE stream_info(target_type TEXT)")
    conn.execute("INSERT INTO stream_info VALUES ('GW')")
    conn.commit()
    conn.close()


def _build_object_db(rows: list[tuple[str, int, int]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE object_table(object_id TEXT PRIMARY KEY, offset INTEGER, length INTEGER)")
        conn.executemany("INSERT INTO object_table VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        return path.read_bytes()


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


def _chunk_it(buf: bytes) -> list[bytes]:
    padded = buf + b"\x00" * (-len(buf) % 4096)
    return [padded[i : i + 4096] for i in range(0, len(padded), 4096)]


def _build_synthetic_degraded_repo(tmp_path: Path, *, session_id: int = 8) -> str:
    """A workload whose ``sub_type`` dispatch.py has no candidate
    providers for at all, **and** with no object-name index recorded either
    (``_version_spec_json()`` below carries no ``additional_meta``) — the
    real case a manual ``--object-db-id`` override exists for:
    automatic discovery has nothing to show at all, but a caller who
    already knows the exact location can still reach it directly.
    Matches ``tests/integration/sdk/test_api.py``'s degraded-workload
    fixture in every other respect (duplicated, not imported — see this
    module's own docstring). Returns the one real ObjectDB's own
    ``object_db_id`` string, known here by construction rather than
    discovered via any scan."""
    _write_repo_info(tmp_path / "repo_info")
    (tmp_path / "link.key").write_bytes(b"")
    (tmp_path / ".fully_created").write_bytes(b"")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_SYN_CCID, _SYN_CONNECTION_ID, 1)])
    _write_workload_config(
        tmp_path / "db" / "workload_config",
        [(1, "synthetic-workload-uid-2", "GW", {"spec": {"workload_type": _SYN_UNRECOGNIZED_SUB_TYPE}})],
    )
    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                1,
                1,
                _SYN_CCID,
                "synthetic-version-uid-2",
                "GW",
                _SYN_STREAM_UUID,
                _SYN_STREAM_UUID,
                "snap-uuid",
                1,
                0,
                _version_spec_json(start_time=1767225600),
            )
        ],
    )

    stream_db_dir = tmp_path / "saas" / str(_SYN_CCID) / _SYN_STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    payloads = [("v1_object_1", b"aaaaa"), ("v1_object_2", b"bbbbb"), ("v1_object_3", b"ccccc")]
    relative_rows = []
    cursor = 0
    content = b""
    for object_id, payload in payloads:
        relative_rows.append((object_id, cursor, len(payload)))
        content += payload
        cursor += len(payload)
    object_db_len = len(_build_object_db(relative_rows))
    absolute_rows = [(oid, off + object_db_len, ln) for oid, off, ln in relative_rows]
    object_db_bytes = _build_object_db(absolute_rows)
    saas_obj_content = object_db_bytes + content
    plaintexts = _chunk_it(saas_obj_content)

    saas_obj_path = f"{_SYN_STREAM_UUID}/{_SYN_CONNECTION_ID}/1/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _SYN_STREAM_ID, session_id, 64, len(plaintexts), 2)])
    _write_composition(
        tmp_path / "@data" / "Composition", stream_id=_SYN_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    _write_bucket(tmp_path / "@data" / "Pool" / str(_SYN_STREAM_ID) / "0.buk", plaintexts)
    return f"{_SYN_STREAM_UUID}_0_{object_db_len}"


def test_object_db_id_cli_end_to_end_ls_and_export_against_a_synthetic_degraded_version(tmp_path: Path) -> None:
    """The manual-override CLI round trip against a synthetic fixture
    (see this module's own docstring for why: no real version in this
    project's samples reaches ``RawObjectProvider`` through normal CLI
    dispatch, so testing ``--object-db-id`` actually *taking effect* end
    to end needs one that does). The fixture's own object-name index is
    deliberately absent (see its own docstring) — automatic discovery
    has nothing to show, so this test also proves the manual override
    doesn't depend on one existing at all. Zero real-sample dependency —
    the CLI runs against a real, hand-built local repository under
    ``tmp_path``, no ``ReplayStore``/fixture involved."""
    repo_root = tmp_path / "repo"
    object_db_id = _build_synthetic_degraded_repo(repo_root)

    async def _resolve() -> tuple[int, int, str]:
        store = LocalFsStore(repo_root)
        layout = await detect_layout(store)
        async with await DedupRepo.open(store, layout) as repo:
            all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
            [workload] = all_workloads
            [version] = await versions(repo, workload)
            return version.connection_config_id, workload.workload_id, version.version_uid

    ccid, workload_id, version_uid = asyncio.run(_resolve())

    ref = f"{repo_root}#cat:{ccid}/wl:{workload_id}/ver:{version_uid}"

    ls_result = runner.invoke(app, ["--json", "ls", ref, "--object-db-id", object_db_id])
    assert ls_result.exit_code == 0, ls_result.output
    rows = json.loads(ls_result.stdout)
    assert len(rows) == 3  # confirmed by the fixture itself (3 real payloads)
    first_object_name = rows[0]["name"]

    export_ref = f"{ref}/{first_object_name}"
    dst = tmp_path / "object.bin"
    export_result = runner.invoke(app, ["export", export_ref, "-o", str(dst), "--object-db-id", object_db_id])
    assert export_result.exit_code == 0, export_result.output
    assert dst.exists()
    assert dst.stat().st_size == rows[0]["size"]


__all__: list[str] = []
