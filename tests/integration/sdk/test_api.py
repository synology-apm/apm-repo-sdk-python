"""Regression tests for ``api``'s real end-to-end wiring — replayed from
committed fixtures recorded against real bytes, with **no external
dependency**: this always runs, on CI or anywhere else, because it goes
through ``ReplayStore`` instead of a real ``LocalFsStore``.

Fixtures (``tests/fixtures/``, recorded once by ``RecordingStore`` via each
test's own ``record_target()`` call — see ``tests/conftest.py`` and
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...`` workflow that (re-)records these; there is no
separate recipe module anymore):

- ``api_apv1_walk.json.gz`` — ``apv-sample-1`` opened via
  ``Session.open_remote()``: the discover-from-a-parent-directory VM disk
  read via ``DeviceProvider``, every workload's full (including-deleted)
  version list (counts asserted below, not repeated here), and one
  ``file_map_tree`` leaf traversal.
- ``api_is_encrypted_six_samples.json.gz`` — ``Repository.is_encrypted``
  against all 6 real samples the original test names, no key ever given
  (``probe_encrypted()`` is cheap — no Pool scan), narrowing one store
  rooted at the samples directory per sample via ``Session.open_remote()``'s
  own ``root=`` parameter. This test *is* its own recording recipe --
  ``--record-against`` needs a backend rooted at the parent directory of
  all 6 real samples (``local:<path-to-samples-root>``).
- ``api_apv2_encrypted_key_status.json.gz`` — ``apv-sample-2-encrypted``
  opened via ``Session.open_remote()``: a rejected key attempt followed
  by the real key recovering to ``VERIFIED`` and reading a real VM disk
  image header, plus a separate no-key open and a with-real-key open for
  the ``is_encrypted``/``key_status`` agreement check. ``KeyMaterial.verify()``'s
  GCM unwrap is real crypto on bytes ``ReplayStore`` returns unchanged, so
  recovering to ``VERIFIED`` needs the real key — embedded below as a
  literal constant (this sample's own generated vault key, not customer
  data) rather than read from a real sample tree at test time.

``test_repository_provider_falls_back_to_raw_object_provider_via_the_object_name_index``
isn't reproduced via ``ReplayStore`` at all: it already has zero
real-sample dependency in the original (a synthetic on-disk repository built
fresh under ``tmp_path``) — moved here as-is.
"""

from __future__ import annotations

import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import Awaitable, Callable
from pathlib import Path

import zstandard

from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.device import DeviceProvider
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider

_MBR_BOOT_SIG = bytes.fromhex("55aa")
_GPT_SIG = b"EFI PART"

#: apv-sample-2-encrypted's real vault key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_APV2_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="

#: An internal catalog identifier -- stable and non-identifying (never
#: touched by catalog-metadata anonymization).
_WINDOWS_VM_WORKLOAD_ID = 2


async def test_replayed_session_discover_from_parent_directory_lists_real_devices(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        # allow_content=True: reads only the disk's MBR/GPT signature bytes
        # -- a structural oracle, never the disk's own real content.
        store = await record_target("api_apv1_walk.json.gz", allow_content=True)
        [repo] = await session.open_remote(store)
        assert repo.layout.repo_root == "@ActiveProtectVault"
        assert repo.key_status is api.KeyStatus.NOT_ENCRYPTED

        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        catalog, vm = next((c, w) for c, w in workload_pairs if w.workload_id == _WINDOWS_VM_WORKLOAD_ID)
        version = next(v for v in await catalog.versions(vm) if v.meta is not None)

        provider = await catalog.provider(version)
        assert isinstance(provider, DeviceProvider)
        devices = await provider.children(provider.root())
        assert len(devices) == 1

        objects = await provider.children(devices[0])
        disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
        content = (await provider.unit(disk)).open()
        header = await content.read(0, 520)
        assert header[510:512] == _MBR_BOOT_SIG
        assert header[512:520] == _GPT_SIG


async def test_replayed_repository_connections_workloads_versions_match_apv_sample_1(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        store = await record_target("api_apv1_walk.json.gz")
        [repo] = await session.open_remote(store)

        catalogs = await repo.catalogs()
        assert len(catalogs) == 2

        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        assert len(workload_pairs) == 25

        all_versions = [v for c, w in workload_pairs for v in await c.versions(w, include_deleted=True)]
        # Catalog.versions() is the raw copy_target_version read now --
        # nothing filtered out in advance (a version whose content turns
        # out unresolvable raises when actually opened instead). The real
        # count across all 25 workloads is 109; the previous, much lower
        # 12 reflected the retired listing-time availability filter
        # silently excluding most real rows whose meta/generation
        # happened not to resolve, not this fixture recording fewer rows.
        assert len(all_versions) == 109


async def test_replayed_repository_raw_file_and_file_map_tree_fallback_axes(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with api.Session() as session:
        store = await record_target("api_apv1_walk.json.gz")
        [repo] = await session.open_remote(store)

        tree = await repo.file_map_tree()
        root = tree.root()
        children = await tree.children(root)
        # The real, deterministic root-level file_map_tree entries this
        # fixture recorded -- a mix of raw session-uuid and vault-target
        # directories.
        assert {c.name for c in children} == {
            "1fd2d9bd-faab-4b26-a610-109ccb5a093e",
            "33d68fc3-b01f-4b07-a280-d34986d9d100",
            "DRMdjvEJPzoxQiUC",
            "KxMWSUvtSZiaDTDy",
            "LNJAQtstRVxciJWy",
            "VM-c39e8f8a-b861-40ee-a8a1-bdf06fdedcd7",
            "VM-ebd17568-24b5-4816-9e42-a9deb290ad74",
            "XfGkaDjWyGhXVoRC",
            "tfUJpbJdYextKnPE",
            "uvWRSFkGxCcZAMwt",
            "vTQbePdWJrIrRncl",
        }

        leaf = next(c for c in children if c.name == "1fd2d9bd-faab-4b26-a610-109ccb5a093e")
        path = [leaf.name]
        while not leaf.is_leaf:
            leaf = (await tree.children(leaf))[0]
            path.append(leaf.name)
        # The real, deterministic first-child-at-each-level descent this
        # fixture recorded, down to the leaf dedup.img and its real size.
        assert path == ["1fd2d9bd-faab-4b26-a610-109ccb5a093e", "50", "dedup.img"]
        unit = await tree.unit(leaf)
        assert unit.open().size == 893317120


async def test_replayed_repository_is_encrypted_matches_every_real_sample(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """See this file's own docstring for why all 6 samples share one
    fixture: ``Repository.is_encrypted`` resolves purely from the cheap
    ``probe_encrypted()`` check, no key ever given."""
    store = await record_target("api_is_encrypted_six_samples.json.gz")
    async with api.Session() as session:
        assert (await session.open_remote(store, root="apv-sample-1"))[0].is_encrypted is False
        assert (await session.open_remote(store, root="apv-sample-2-encrypted"))[0].is_encrypted is True
        assert (await session.open_remote(store, root="apv-sample-3"))[0].is_encrypted is False
        assert (await session.open_remote(store, root="sample-2"))[0].is_encrypted is True
        for repo in await session.open_remote(store, root="sample-1"):  # 2 repo ids sharing one bucket
            assert repo.is_encrypted is True
        for repo in await session.open_remote(store, root="s3-sample-2-encrypted"):
            assert repo.is_encrypted is True


async def test_replayed_session_close_closes_all_repos(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("api_apv1_walk.json.gz")
    session = api.Session()
    repos = await session.open_remote(store)
    assert len(repos) == 1
    await session.close()
    assert session._repos == []


async def test_replayed_repository_key_status_and_set_key_wrong_then_right(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """Regression for the INVALID vs NO_KEY_PROVIDED distinction: opening
    an encrypted repository without a key reports NO_KEY_PROVIDED; a wrong key
    reports INVALID (not silently reverting to NO_KEY_PROVIDED); the real
    key afterwards reports VERIFIED and actually decrypts real content."""
    async with api.Session() as session:
        # allow_content=True: the disk read at the end is only an MBR/GPT
        # signature check -- a structural oracle, never the disk's own
        # real content.
        store = await record_target("api_apv2_encrypted_key_status.json.gz", allow_content=True)
        [repo] = await session.open_remote(store)
        status_before_any_key = repo.key_status
        assert status_before_any_key is api.KeyStatus.NO_KEY_PROVIDED

        wrong_key = "wrongkeyid00@AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        verification = await repo.set_key(wrong_key)
        assert verification.ok is False
        status_after_wrong_key = repo.key_status
        assert status_after_wrong_key is api.KeyStatus.INVALID

        verification = await repo.set_key(_APV2_ENCRYPTED_KEY_STRING)
        assert verification.ok is True
        status_after_real_key = repo.key_status
        assert status_after_real_key is api.KeyStatus.VERIFIED

        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        catalog, vm = next((c, w) for c, w in workload_pairs if w.workload_type == "VM")
        version = next(v for v in await catalog.versions(vm) if v.meta is not None)

        provider = await catalog.provider(version)
        devices = await provider.children(provider.root())
        objects = await provider.children(devices[0])
        disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
        content = (await provider.unit(disk)).open()
        header = await content.read(0, 520)
        assert header[510:512] == _MBR_BOOT_SIG
        assert header[512:520] == _GPT_SIG


async def test_replayed_repository_is_encrypted_agrees_with_key_status(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """``is_encrypted``/``key_status`` are derived from the same
    underlying state (``_keys``/``_encrypted``) via two independent
    branches — this pins down that they never disagree, for both a
    confirmed-encrypted repository given no key (``NO_KEY_PROVIDED``) and one
    given a real key (``VERIFIED``)."""
    async with api.Session() as session:
        store = await record_target("api_apv2_encrypted_key_status.json.gz")
        [no_key] = await session.open_remote(store)
        assert no_key.key_status is api.KeyStatus.NO_KEY_PROVIDED
        assert no_key.is_encrypted is True

        [with_key] = await session.open_remote(store, key=_APV2_ENCRYPTED_KEY_STRING)
        assert with_key.key_status is api.KeyStatus.VERIFIED
        assert with_key.is_encrypted is True


# -- synthetic degraded-SaaS-workload fixture -------------------------------
#
# Zero real-sample dependency in the original (an on-disk repository built
# fresh under tmp_path, no ReplayStore/samples_dir involved at all) —
# moved here as-is rather than left in tests/integration/, which it never
# needed. Exercises the RawObjectProvider fallback path, since no real
# sample workload reaches it.
_STREAM_ID = 40
_CCID = 5
_CONNECTION_ID = "conn-synthetic"
_STREAM_UUID = "synthetic-degraded-stream"
_UNRECOGNIZED_SUB_TYPE = "SOME_FUTURE_CONNECTOR_TYPE"


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


def _version_spec_json(
    start_time: object = None,
    *,
    additional_meta: dict[str, object] | None = None,
    status: object = "COMPLETED",
) -> str:
    """Minimal real-shaped ``version_spec`` blob — the ``status.start_time``
    field ``catalog/version.py``'s ``_version_display_name()`` actually reads, plus,
    optionally, ``status.additional_meta`` (itself a JSON-encoded string,
    per ``object_name_index.py``'s own on-disk shape) — the connector's
    catalog bookkeeping ``RawObjectProvider``/every application-layer
    provider resolves their content through exclusively.
    ``status.status`` defaults to ``"COMPLETED"`` —
    ``catalog/version.py``'s own ``_BROWSABLE_VERSION_STATUSES`` filter excludes
    any version whose status isn't confirmed
    ``COMPLETED``/``PARTIAL``/``CANCELED``, so every fixture in this file
    gets a browsable one unless a test is deliberately checking that
    filter itself."""
    status_obj: dict[str, object] = {}
    if start_time is not None:
        status_obj["start_time"] = str(start_time)
    if additional_meta is not None:
        status_obj["additional_meta"] = json.dumps(additional_meta)
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


def _build_degraded_saas_repo(tmp_path: Path, *, session_id: int = 7) -> None:
    """One synthetic SaaS workload with a real-shaped ``ObjectDB`` (3
    objects) whose ``sub_type`` has no dispatch candidate, reaching
    ``RawObjectProvider`` directly."""
    _write_repo_info(tmp_path / "repo_info")
    (tmp_path / "link.key").write_bytes(b"")
    (tmp_path / ".fully_created").write_bytes(b"")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID, 1)])
    _write_workload_config(
        tmp_path / "db" / "workload_config",
        [(1, "synthetic-workload-uid", "GW", {"spec": {"workload_type": _UNRECOGNIZED_SUB_TYPE}})],
    )

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
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

    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                1,
                1,
                _CCID,
                "synthetic-version-uid",
                "GW",
                _STREAM_UUID,
                _STREAM_UUID,
                "snap-uuid",
                1,
                0,
                _version_spec_json(
                    start_time=1767225600,
                    additional_meta={
                        "object_db_id": f"{_STREAM_UUID}_0_{object_db_len}",
                        "db_object_ids": {
                            "db_objects": [
                                {"name": "obj_1", "object_id": "v1_object_1"},
                                {"name": "obj_2", "object_id": "v1_object_2"},
                                {"name": "obj_3", "object_id": "v1_object_3"},
                            ]
                        },
                    },
                ),
            )
        ],
    )

    saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, len(plaintexts), 2)])
    _write_composition(
        tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", plaintexts)


async def test_repository_provider_falls_back_to_raw_object_provider_via_the_object_name_index(
    tmp_path: Path,
) -> None:
    """A degraded SaaS workload (no application-layer provider for its
    ``sub_type``), through the full ``Session``/``Repository`` facade
    every real CLI/TUI invocation actually goes through — not
    ``RawObjectProvider`` constructed directly, and not any lower-level
    scan. ``resolve_object_name_index()`` names all 3 real objects this
    synthetic repository's ``ObjectDB`` holds (see ``_build_degraded_saas_repo``'s
    own docstring); every one of them must actually be listed and read
    back correctly through the public facade alone."""
    repo_root = tmp_path / "repo"
    _build_degraded_saas_repo(repo_root)
    async with api.Session() as session:
        [repo] = await session.open(repo_root)
        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        [(catalog, workload)] = workload_pairs
        [version] = await catalog.versions(workload)

        provider = await catalog.provider(version)
        assert isinstance(provider, RawObjectProvider)
        nodes = await provider.children(provider.root())
        assert {n.name for n in nodes} == {"obj_1", "obj_2", "obj_3"}

        [obj_1] = [n for n in nodes if n.name == "obj_1"]
        content = (await provider.unit(obj_1)).open()
        assert await content.read(0, content.size or 0) == b"aaaaa"


__all__: list[str] = []
