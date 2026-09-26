"""Unit tests for ``synology_apm_repo.sdk.units.device`` — synthetic
repository roots written to real files, no sample repositories required
(see ``tests/integration/sdk/test_units_device.py`` for the byte-for-byte
cross-check against real VM disk images)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
import struct
import zlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

import synology_apm_repo.sdk.units.device as device_module
import synology_apm_repo.sdk.units.device_pcps as device_pcps_module
from synology_apm_repo.sdk.catalog.version import Version, VersionMeta
from synology_apm_repo.sdk.dedup.repository import DedupRepo, FileLocation
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError, UnsupportedDataFormatError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS, MODE_VAULT_ENCRYPT
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.crypto import chunk_iv
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import (
    BucketId,
    CatalogId,
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
from synology_apm_repo.sdk.units.base import FileState, Node, UnitKind
from synology_apm_repo.sdk.units.content.disk_fs import DiskFilesystem, DiskFilesystemUnavailableError
from synology_apm_repo.sdk.units.content.disk_fs._base import _DirEntry
from synology_apm_repo.sdk.units.content.pcps_disk import VirtualDiskContentSource
from synology_apm_repo.sdk.units.device import DeviceProvider
from synology_apm_repo.sdk.units.device_kind import _NodeKind
from synology_apm_repo.sdk.units.device_pcps import _pcps_disk_key
from synology_apm_repo.sdk.units.node_ref import NodeRef

_STREAM_ID = 5
_STREAM_ID_B = 6  # a second, independent composition/bucket for a disk's second fragment
_DISK_PLAINTEXT = b"\x55\xaa" + b"\x00" * 4094  # a fake but recognizable "MBR"
_DISK_PLAINTEXT_B = b"\xbb\xcc" + b"\x00" * 4094  # distinguishable from _DISK_PLAINTEXT
assert len(_DISK_PLAINTEXT) == 4096
assert len(_DISK_PLAINTEXT_B) == 4096


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
    conn.execute(
        "CREATE TABLE vault_encryption_key(user_key_uuid TEXT UNIQUE, encrypted_data_key TEXT, crtime DATETIME)"
    )
    conn.execute("INSERT INTO vault_encryption_key VALUES ('NoEncryption', '', CURRENT_TIMESTAMP)")
    conn.commit()
    conn.close()


def _write_repo_info_and_vault_key_db(tmp_path: Path) -> tuple[LocalFsStore, RepoLayout]:
    """Writes ``repo_info`` + an all-``NoEncryption`` vault_encryption_key
    db and returns ``(store, layout)`` ready for ``DedupRepo.open()``
    — the boilerplate every hand-built repository below (that doesn't use the
    shared ``repo`` fixture) needs before its own additional writes
    (file_map, buckets, ...). Store/layout construction has no ordering
    dependency on those other writes, so this can run before or
    interleaved with them."""
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    return LocalFsStore(tmp_path), RepoLayout(kind=RepoKind.VAULT, repo_root="")


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


def _write_pcps_file_meta(path: Path, rows: list[tuple[int, str, int | None]]) -> None:
    """``rows``: ``(fid, path, file_size)`` — the real ``file_meta``
    schema has several more columns (``connection_config_id``,
    ``target_type``, ...), none of which ``PcpsDiskTree.object_nodes()`` reads."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE file_meta(fid INTEGER PRIMARY KEY, path TEXT, file_size INTEGER)")
    conn.executemany("INSERT INTO file_meta VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_target_db(
    path: Path,
    *,
    version_id: int = 1,
    config_device_id: int = 1,
    device_uuid: str = "device-uuid",
    host_name: str = "my-vm",
    os_name: str = "Windows",
    objects: list[tuple[int, int, str, str, str, int, int]],
) -> None:
    """``objects``: (object_id, data_format, file_path, src_file_path, temp_postfix, dedup_object, file_size)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_table(id INTEGER PRIMARY KEY, version_id INTEGER, data_format INTEGER, "
        "status INTEGER, folder_name TEXT)"
    )
    conn.execute("INSERT INTO version_table VALUES (1, ?, 1, 1, 'folder')", (version_id,))
    conn.execute(
        "CREATE TABLE device_table(device_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "device_uuid TEXT, host_name TEXT, os_name TEXT)"
    )
    conn.execute(
        "INSERT INTO device_table VALUES (1, ?, ?, ?, ?, ?)",
        (version_id, config_device_id, device_uuid, host_name, os_name),
    )
    conn.execute(
        "CREATE TABLE object_table(object_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "data_format INTEGER, file_path TEXT, src_file_path TEXT, temp_postfix TEXT, dedup_object INTEGER, "
        "file_size INTEGER)"
    )
    conn.executemany(
        "INSERT INTO object_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (object_id, version_id, config_device_id, data_format, file_path, src_file_path, temp_postfix, dedup, size)
            for object_id, data_format, file_path, src_file_path, temp_postfix, dedup, size in objects
        ],
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


def _write_bucket(path: Path, plaintext: bytes, *, vault_key: bytes | None = None) -> None:
    compressed = zstandard.ZstdCompressor().compress(plaintext)
    if vault_key is not None:
        addr = ChunkAddress(StreamId(_STREAM_ID), BucketId(0), ChunkIdx(0))
        encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(chunk_iv(addr))).encryptor()
        payload = encryptor.update(compressed) + encryptor.finalize()
    else:
        payload = compressed
    tight = _encode_size_store([(CompressType.ZSTD.value, len(payload))])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC | (MODE_VAULT_ENCRYPT if vault_key else 0))
    header[12:16] = struct.pack(">I", 1)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    trailer = os.urandom(4 + redundancy_size((15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + payload + trailer)


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    return (
        bytes([type_byte])
        + file_chunk_idx.to_bytes(7, "big")
        + addr_int.to_bytes(8, "big")
        + tail_u32.to_bytes(4, "big")
    )


def _write_composition(root: Path, *, stream_id: int, session_id: int, file_offset: int = 0) -> None:
    addr_int = ChunkAddress(StreamId(stream_id), BucketId(0), ChunkIdx(0)).to_int()
    entry = _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value, file_chunk_idx=file_offset >> 12, addr_int=addr_int, tail_u32=1 << 16
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


def _build_vm_repo(
    tmp_path: Path,
    *,
    stream_id: int = _STREAM_ID,
    session_id: int = 9,
    extra_objects: list[tuple[int, int, str, str, str, int, int]] | None = None,
) -> None:
    _write_repo_info_and_vault_key_db(tmp_path)
    src_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
    _write_file_map(tmp_path / "db" / "file_map", [(src_path, stream_id, session_id, 64, 1, 2)])
    objects = [(1, 1, "ActiveBackup_2026-01-01/my-vm/disk.img", src_path, "", 1, 4096)]
    if extra_objects:
        objects += extra_objects
    _write_target_db(tmp_path / "copy_meta_file" / "VM_uid1" / "target.db", objects=objects)
    _write_composition(tmp_path / "@data" / "Composition", stream_id=stream_id, session_id=session_id)
    _write_bucket(tmp_path / "@data" / "Pool" / str(stream_id) / "0.buk", _DISK_PLAINTEXT)


def _version(
    target_meta_path: str = "/pv/20/copy_meta_file/VM_uid1",
    *,
    meta_filenames: tuple[str, ...] = ("target.db",),
    meta_status: int = 1,
) -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid-1"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type="VM",
        target_id=TargetId("VM-uid"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=VersionMeta(target_meta_path=target_meta_path, meta_filenames=meta_filenames, status=meta_status),
    )


def _version_no_meta() -> Version:
    """A VM version with no ``copy_target_version_meta`` row at all — same
    shape as ``_version`` but ``meta=None``, the other real way
    ``DeviceProvider._resolve_meta_dir`` raises ``NotFoundError``."""
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid-1"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type="VM",
        target_id=TargetId("VM-uid"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


def _pcps_version(version_uid: str = "vuid-pcps", target_type: str = "PC", meta: VersionMeta | None = None) -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid(version_uid),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type=target_type,
        target_id=TargetId("PC-uid"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=meta,
    )


@pytest.fixture(autouse=True)
def _no_disk_fs_sibling(monkeypatch: pytest.MonkeyPatch) -> None:
    """This whole file is about ``DeviceProvider``'s own VM/PC-PS object
    listing/pagination/dispatch logic, not the ``disk-fs``/``pytsk3``
    sibling-node feature — disabled here (regardless of whether pytsk3
    happens to be installed in this environment, e.g. via the dev
    dependency group) so every existing assertion about "the children
    of a device are exactly its disk-image/file objects" stays true
    without each test having to filter out the additive "(filesystem)"
    node itself. See ``tests/unit/sdk/test_units_disk_fs.py`` for that
    feature's own dedicated tests.

    Patched in both modules that check it: the VM path
    (``units/device.py``'s own ``_object_nodes``) and the PC/PS path
    (``units/device_pcps.py``'s ``PcpsDiskTree``) each import
    ``disk_fs_available`` independently, so both bindings need patching
    for this to actually disable the sibling on every code path this
    file exercises."""
    monkeypatch.setattr(device_module, "disk_fs_available", lambda: False)
    monkeypatch.setattr(device_pcps_module, "disk_fs_available", lambda: False)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[DedupRepo]:
    _build_vm_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as r:
        yield r


class TestTree:
    async def test_root_lists_no_devices_kind(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, _version()) as provider:
            root = provider.root()
            assert root.attrs["_kind"] == _NodeKind.ROOT
            assert root.is_leaf is False

    async def test_children_of_an_unrecognized_node_is_empty(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, _version()) as provider:
            mystery_node = Node(ref=NodeRef("repo", ("mystery",)), name="mystery", is_leaf=False, attrs={"_kind": "?"})
            assert await provider.children(mystery_node) == []

    async def test_unit_on_an_unrecognized_node_raises(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, _version()) as provider:
            mystery_node = Node(ref=NodeRef("repo", ("mystery",)), name="mystery", is_leaf=True, attrs={"_kind": "?"})
            with pytest.raises(ValueError, match="not a restorable unit"):
                await provider.unit(mystery_node)

    async def test_children_of_root_are_devices(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, _version()) as provider:
            devices = await provider.children(provider.root())
            assert len(devices) == 1
            assert devices[0].name == "my-vm"
            assert devices[0].attrs["os_name"] == "Windows"

    async def test_children_of_device_are_objects(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, _version()) as provider:
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)
            assert len(objects) == 1
            assert objects[0].kind is UnitKind.DISK_IMAGE
            assert objects[0].size == 4096

    async def test_close_closes_the_cached_target_db_connection(self, repo: DedupRepo) -> None:
        # DeviceProvider.close() has no direct test anywhere in this
        # file -- it's required, not merely tidiness: an un-daemonized
        # aiosqlite worker thread would keep the interpreter alive
        # forever if never closed.
        async with await DeviceProvider.create(repo, _version()) as provider:
            device = (await provider.children(provider.root()))[0]
            await provider.children(device)  # populates _target_db via _object_nodes -> _target_db_source()
            target_db = provider._target_db
            assert target_db is not None
            # target.db now opens via SqliteSource.from_enveloped_store(),
            # which always materializes into a TemporaryDirectory (its
            # own peel/WAL-merge transform forces open_sqlite's slow
            # path unconditionally) rather than from_bytes()'s single
            # mkstemp file -- so this checks _tmp_dir, not _path.
            tmp_dir = target_db._tmp_dir
            assert tmp_dir is not None
            assert os.path.exists(tmp_dir.name)

            await provider.close()

            assert provider._target_db is None
            assert not os.path.exists(tmp_dir.name)  # SqliteSource.close() removes its temp dir

    async def test_dedup_object_gets_a_disk_fs_sibling_node_when_available(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The module-level ``_no_disk_fs_sibling`` autouse fixture keeps
        every other test in this file free of the additive "(filesystem)"
        node -- re-enabled here, locally, to actually exercise
        ``_object_nodes``'s own ``if dedup_object and not unsupported and
        disk_fs_available():`` branch, which every other test in the
        file bypasses entirely."""
        monkeypatch.setattr(device_module, "disk_fs_available", lambda: True)
        async with await DeviceProvider.create(repo, _version()) as provider:
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)

            assert len(objects) == 2
            disk = next(n for n in objects if n.kind is UnitKind.DISK_IMAGE)
            sibling = next(n for n in objects if n is not disk)
            assert sibling.name == f"{disk.name} (filesystem)"
            assert sibling.kind is UnitKind.DISK_FILESYSTEM
            # disk_fs_containers_before_leaves(): the browsable
            # "(filesystem)" sibling is a container (is_leaf=False) and
            # must list ahead of its own disk-image leaf, not after it.
            assert objects == [sibling, disk]

    async def test_device_and_object_refs_round_trip_through_str_and_parse(self, repo: DedupRepo) -> None:
        """Device/object/pcps refs must be built flatly via
        ``NodeRef.canonical(..., extra=...)``, never by nesting a
        stringified parent ref as the child's ``repo_path`` — that would
        bake a literal ``#`` into the middle of the string, and
        ``NodeRef.parse()`` (which splits on the *first* ``#`` only)
        would then silently fold the device/object segment into the
        ``ver:`` segment on round-trip, losing it entirely."""
        async with await DeviceProvider.create(repo, _version()) as provider:
            device = (await provider.children(provider.root()))[0]
            obj = (await provider.children(device))[0]

            for node in (device, obj):
                roundtripped = NodeRef.parse(str(node.ref))
                assert roundtripped == node.ref
                assert roundtripped.canonical_ids == (
                    CatalogId(str(_version().connection_config_id)),
                    _version().workload_id,
                    _version().version_uid,
                )
            assert device.ref.extra_segments == (f"device:{device.attrs['config_device_id']}",)
            assert obj.ref.extra_segments == (
                f"device:{device.attrs['config_device_id']}",
                f"object:{obj.attrs['object_id']}",
            )

    async def test_temp_postfix_rows_are_filtered_out(self, tmp_path: Path) -> None:
        _build_vm_repo(
            tmp_path,
            extra_objects=[(2, 1, "path", "VM-uid/incomplete", "some-postfix", 1, 100)],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)
            assert len(objects) == 1  # the temp_postfix row is excluded

    async def test_non_dedup_row_is_kind_file(self, tmp_path: Path) -> None:
        _build_vm_repo(
            tmp_path,
            extra_objects=[(2, 0, "ActiveBackup_2026-01-01/my-vm/disk.img.delta", "", "", 0, 64)],
        )
        (tmp_path / "copy_meta_file" / "VM_uid1" / "ActiveBackup_2026-01-01" / "my-vm").mkdir(parents=True)
        (tmp_path / "copy_meta_file" / "VM_uid1" / "ActiveBackup_2026-01-01" / "my-vm" / "disk.img.delta").write_bytes(
            b"CbTT" + b"\x00" * 60
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)
            delta = next(o for o in objects if o.kind is UnitKind.FILE)
            unit = await provider.unit(delta)
            content = unit.open()
            assert await content.read(0, 4) == b"CbTT"


class TestVmTargetDbMissingPropagatesRaw:
    """A VM version whose ``target.db`` isn't actually readable surfaces
    as a plain, uncaught ``NotFoundError`` from ``DeviceProvider.children()``.
    ``Repository.versions()`` filters out every case knowable ahead of time
    (no meta row; ``target.db`` not among ``meta_filenames``; the
    ``copy_meta_file/<dir>`` removed from the store — see
    ``catalog/version.py``'s ``vm_meta_available()``/``copy_meta_dir_exists()``)
    before a version ever reaches a real ``DeviceProvider`` — covered
    directly here anyway, since a caller can still construct one from an
    arbitrary ``Version`` bypassing that filter, as every test in this
    file does."""

    async def test_no_meta_row_at_all_raises_not_found(self, tmp_path: Path) -> None:
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version_no_meta()) as provider,
        ):
            with pytest.raises(NotFoundError):
                await provider.children(provider.root())

    async def test_target_db_not_registered_in_meta_filenames_raises_not_found(self, tmp_path: Path) -> None:
        """``resolve_meta_filename()`` itself raises here — a purely
        catalog-driven, zero-store-I/O failure (this is exactly the
        condition ``catalog/version.py``'s ``vm_meta_available()`` already
        filters out of a real ``Repository.versions()`` call before it
        ever reaches here)."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version(meta_filenames=("snapshot_info.json",))) as provider,
        ):
            with pytest.raises(NotFoundError, match="target.db"):
                await provider.children(provider.root())

    async def test_target_db_registered_but_missing_from_the_store_raises_not_found(self, tmp_path: Path) -> None:
        """The one residual case neither ``vm_meta_available()`` nor
        ``copy_meta_dir_exists()`` can see: ``meta_filenames`` says
        ``target.db`` landed and the upload was ``Complete``, and the
        ``copy_meta_file/<dir>`` itself genuinely exists (so the
        listing-time directory-presence check passes) — just without a
        ``target.db`` inside it. Real gaps found in practice remove the
        whole directory at once; this narrower, only-one-file-missing case is
        deliberately left uncaught rather than degraded."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        (tmp_path / "copy_meta_file" / "VM_uid1").mkdir(parents=True)
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            with pytest.raises(NotFoundError, match="target.db"):
                await provider.children(provider.root())


class TestOpenDiskImage:
    async def test_reads_the_real_dedup_content(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, _version()) as provider:
            device = (await provider.children(provider.root()))[0]
            disk = (await provider.children(device))[0]
            unit = await provider.unit(disk)
            assert (await unit.open().read(0, 4096)) == _DISK_PLAINTEXT

    async def test_unsupported_data_format_raises_on_open_not_on_list(self, tmp_path: Path) -> None:
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        _write_target_db(
            tmp_path / "copy_meta_file" / "VM_uid1" / "target.db",
            objects=[(1, 2, "disk.img", "VM-uid/disk.img", "", 1, 4096)],  # data_format=2 (CBT)
        )
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)
            assert len(objects) == 1  # still listed
            assert objects[0].attrs["unsupported"] is True
            with pytest.raises(UnsupportedDataFormatError):
                await provider.unit(objects[0])


class TestEncryptedTargetDb:
    async def test_ahlt_enveloped_target_db_is_decrypted(self, tmp_path: Path) -> None:
        from synology_apm_repo.sdk.dedup.keys import KeyMaterial

        vault_key = os.urandom(32)
        user_key_id = "abcdefghijkl"
        user_key = os.urandom(32)
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        wrapped = AESGCM(user_key).encrypt(user_key_id.encode()[:12], vault_key, None)

        _write_repo_info(tmp_path / "repo_info")
        conn_path = tmp_path / "db" / "vault_encryption_key"
        conn_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(conn_path)
        conn.execute("CREATE TABLE vault_encryption_key(user_key_uuid TEXT UNIQUE, encrypted_data_key TEXT)")
        import base64

        conn.execute(
            "INSERT INTO vault_encryption_key VALUES (?, ?)", (user_key_id, base64.b64encode(wrapped).decode())
        )
        conn.commit()
        conn.close()

        src_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
        _write_file_map(tmp_path / "db" / "file_map", [(src_path, _STREAM_ID, 9, 64, 1, 2)])
        plain_db_path = tmp_path / "_plain_target.db"
        _write_target_db(plain_db_path, objects=[(1, 1, "path", src_path, "", 1, 4096)])
        plain_bytes = plain_db_path.read_bytes()

        iv = os.urandom(16)
        encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(iv)).encryptor()
        ciphertext = encryptor.update(plain_bytes) + encryptor.finalize()
        ahlt_header = bytearray(64)
        ahlt_header[0:4] = b"aHlT"
        ahlt_header[8:24] = iv
        ahlt_header[60:64] = (zlib.crc32(bytes(ahlt_header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        target_db_path = tmp_path / "copy_meta_file" / "VM_uid1" / "target.db"
        target_db_path.parent.mkdir(parents=True, exist_ok=True)
        target_db_path.write_bytes(bytes(ahlt_header) + ciphertext)

        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT, vault_key=vault_key)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        keys = KeyMaterial(user_key_id=user_key_id, user_key=user_key)
        async with (
            await DedupRepo.open(store, layout, keys) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            disk = (await provider.children(device))[0]
            content = (await provider.unit(disk)).open()
            assert await content.read(0, 4096) == _DISK_PLAINTEXT


def _write_copy_target_version_and_file(
    path: Path, *, version_rows: list[tuple[int, str]], file_rows: list[tuple[int, int]]
) -> None:
    """One physical sqlite file holding *both* ``copy_target_version`` and
    ``copy_target_file`` — that's the real on-disk shape, not two separate
    files. Writing them separately would test a layout that doesn't
    actually occur and would silently pass even if
    ``db("copy_target_file")`` were broken."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE copy_target_version(version_id INTEGER PRIMARY KEY, version_uid TEXT)")
    conn.executemany("INSERT INTO copy_target_version VALUES (?, ?)", version_rows)
    conn.execute("CREATE TABLE copy_target_file(version_id INTEGER, fid INTEGER)")
    conn.executemany("INSERT INTO copy_target_file VALUES (?, ?)", file_rows)
    conn.commit()
    conn.close()


class TestVmVsPcPsDispatch:
    """``_is_pcps`` is decided purely from ``Version.target_type`` — never
    by probing which files exist, since a PC/PS landing directory can look
    identical to a normally-landed one."""

    async def test_ps_with_no_meta_row_at_all_is_pcps(self, tmp_path: Path) -> None:
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version(target_type="PS", meta=None)) as provider,
        ):
            assert provider.root().attrs["_kind"] == _NodeKind.PCPS_ROOT

    async def test_pc_with_a_real_but_target_db_less_meta_dir_is_still_pcps(self, tmp_path: Path) -> None:
        """A PC/PS version whose meta directory genuinely landed
        (``copy_target_version_meta`` has a row, ``meta_filenames``
        lists only ``snapshot_info.json`` — FORMAT-SPEC.md:
        copy_meta_file-layout's documented PC/PS shape) must classify as PC/PS, not VM,
        even though the directory itself exists -- directory presence
        alone can't distinguish VM from PC/PS."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        meta_dir = tmp_path / "copy_meta_file" / "vuid-pcps"
        meta_dir.mkdir(parents=True)
        (meta_dir / "snapshot_info.json").write_text("{}")
        meta = VersionMeta(
            target_meta_path="copy_meta_file/vuid-pcps", meta_filenames=("snapshot_info.json",), status=1
        )
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version(target_type="PC", meta=meta)) as provider,
        ):
            assert provider.root().attrs["_kind"] == _NodeKind.PCPS_ROOT
            # Must not raise trying to open a target.db that was never there.
            assert await provider.children(provider.root()) == []


class TestPcPsDiskKey:
    """``_pcps_disk_key()``'s regex, checked directly against the
    ``D(diskUuid)O(offset)[V(volumeUuid)]S(diskIndex)`` naming
    convention rather than only through a full synthetic repository — every
    component is independently optional, and the encoder only emits a
    component when its value is non-empty, so a real object can
    legitimately omit ``O(...)`` (or ``V(...)``)."""

    def test_the_usual_shape_with_offset_and_no_volume(self) -> None:
        path = ".../D(AAAA)O(17408)S(0).img"
        assert _pcps_disk_key(1, path) == ("AAAA", "0")

    def test_offset_omitted_still_groups_by_disk_and_index(self) -> None:
        """Not hypothetical: the encoder only appends ``O(...)`` when
        ``offset != ""``, so an object missing it must still group with
        its real siblings, not fall back to the singleton case."""
        path = ".../D(AAAA)S(0).img"
        assert _pcps_disk_key(1, path) == ("AAAA", "0")

    def test_volume_present_is_ignored_for_the_grouping_key(self) -> None:
        path = ".../D(AAAA)O(135266304)V(BBBB)S(0).img"
        assert _pcps_disk_key(1, path) == ("AAAA", "0")

    def test_offset_and_volume_both_omitted(self) -> None:
        path = ".../D(AAAA)S(1).img"
        assert _pcps_disk_key(1, path) == ("AAAA", "1")

    def test_a_trailing_seq_suffix_does_not_break_the_match(self) -> None:
        # The real on-disk suffix is "_{N}" (curly braces), not "_N" --
        # .search() doesn't care what follows S(...) either way, but this
        # pins down the real shape rather than an imagined one.
        path = ".../D(AAAA)O(0)S(0)_{2}.img"
        assert _pcps_disk_key(1, path) == ("AAAA", "0")

    def test_no_match_at_all_falls_back_to_a_singleton_keyed_by_fid(self) -> None:
        # The real macOS shape: no D()/S() structure whatsoever.
        path = ".../FC881262-2E21-4DCD-B3E7-CAE6D8BB3C94_43C072D1-C078-42E0-86A8-85C5428213F1.img"
        assert _pcps_disk_key(42, path) == ("_single", "42")


class TestPcPsFallback:
    async def test_missing_copy_target_version_file_gives_no_disks(self, tmp_path: Path) -> None:
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        # no db/copy_target_version at all — and since copy_target_file
        # has no on-disk object of its own (it shares copy_target_version's
        # physical file), this is the only real "genuinely nothing here"
        # case left to test.
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            assert await provider.children(provider.root()) == []

    async def test_unknown_version_uid_gives_no_disks(self, tmp_path: Path) -> None:
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        _write_copy_target_version_and_file(tmp_path / "db" / "copy_target_version", version_rows=[], file_rows=[])
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version(version_uid="no-such-version")) as provider,
        ):
            assert await provider.children(provider.root()) == []

    async def test_no_matching_fids_gives_no_disks(self, tmp_path: Path) -> None:
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version", version_rows=[(1, "vuid-pcps")], file_rows=[]
        )
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            assert await provider.children(provider.root()) == []

    async def test_pcps_lists_disks_via_copy_target_file_chain(self, tmp_path: Path) -> None:
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_file_map(tmp_path / "db" / "file_map", [(src_path, _STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )

        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])

        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1
            assert disks[0].name == "disk0.img"
            # The real gap this fixed: file_meta.file_size was fetched at
            # open time but never at listing time, unlike the VM path.
            assert disks[0].size == 4096
            unit = await provider.unit(disks[0])
            assert (await unit.open().read(0, 4096)) == _DISK_PLAINTEXT

    async def test_null_file_size_falls_back_from_listing_to_the_real_extent_on_open(self, tmp_path: Path) -> None:
        """``file_meta.file_size`` is genuinely ``int | None`` — NULL for
        every fragment of a disk is a real on-disk possibility, not just a
        theoretical case — and must not crash or silently report a wrong
        size: the listed ``Node.size`` degrades to ``None`` (nothing cheap
        to show yet), and the *opened* unit's real size is still correctly
        re-derived from the composition's own extent."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_file_map(tmp_path / "db" / "file_map", [(src_path, _STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, None)])

        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1
            assert disks[0].size is None  # nothing cheap available at listing time
            unit = await provider.unit(disks[0])
            assert unit.size == len(_DISK_PLAINTEXT)  # re-derived from the real extent once opened
            assert (await unit.open().read(0, len(_DISK_PLAINTEXT))) == _DISK_PLAINTEXT

    async def test_fid_registered_but_absent_from_file_meta_surfaces_a_diagnostic_node(self, tmp_path: Path) -> None:
        """The real gap found on a real sample: ``copy_target_file``
        registers a fid for this version, but ``file_meta``'s currently
        resolved generation has no row for it at all (an older, since
        superseded generation did — see FORMAT-SPEC.md: generation-selection's
        rule). Must surface *something*, not a
        silent empty list."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        # file_meta exists but has no row for fid=100 at all.
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [])

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            nodes = await provider.children(provider.root())
            assert len(nodes) == 1
            assert nodes[0].attrs["_kind"] == _NodeKind.PCPS_DIAGNOSTIC
            assert nodes[0].attrs["missing_fids"] == (100,)
            with pytest.raises(NotFoundError):
                await provider.unit(nodes[0])

    async def test_diagnostic_node_not_appended_when_the_first_page_is_already_full(self, tmp_path: Path) -> None:
        """A real disk plus a never-resolved fid both land on what would
        be the first page — the diagnostic must not push the page past
        ``limit``, unlike every sibling diagnostic-node site in this class
        (``_device_nodes``, ``DiskFsSibling.children()``), which substitute a
        diagnostic for real content rather than add to it."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_file_map(tmp_path / "db" / "file_map", [(src_path, _STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 200)],  # fid 200 has no file_meta row at all
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            nodes = await provider.children(provider.root(), offset=0, limit=1)
            assert len(nodes) == 1
            assert nodes[0].attrs["_kind"] == _NodeKind.PCPS_DISK

    async def test_pcps_object_nodes_only_builds_once_across_repeated_calls(self, tmp_path: Path) -> None:
        """Regression test for the ``(all_nodes, never_resolved)`` cache
        in ``PcpsDiskTree._build_nodes()`` — repeated ``children()`` calls must
        not re-run the ``copy_target_version``/``copy_target_file``/
        ``file_meta`` query chain."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_file_map(tmp_path / "db" / "file_map", [(src_path, _STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            calls = 0
            original = provider._pcps._build_nodes

            async def _counting_build() -> tuple[list[Node], list[int]]:
                nonlocal calls
                calls += 1
                return await original()

            provider._pcps._build_nodes = _counting_build  # type: ignore[method-assign]
            await provider.children(provider.root(), offset=0, limit=1)
            await provider.children(provider.root(), offset=0, limit=1)
            assert calls == 1

    async def test_open_pcps_disk_twice_returns_the_same_cached_unit(self, tmp_path: Path) -> None:
        """Regression test for the ``RestorableUnit`` cache in
        ``PcpsDiskTree.open_disk()`` — a second ``unit()`` call on the same
        disk must not re-run every fragment's own
        ``locate_file()``/composition-open/``extent()`` resolution, and
        hands back the identical cached object."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_file_map(tmp_path / "db" / "file_map", [(src_path, _STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disk_node = (await provider.children(provider.root()))[0]
            unit_1 = await provider.unit(disk_node)
            unit_2 = await provider.unit(disk_node)
            assert unit_1 is unit_2

    async def test_fid_resolves_in_file_meta_but_every_fragment_unresolvable_raises_on_open_not_on_list(
        self, tmp_path: Path
    ) -> None:
        """The listing itself stays cheap and succeeds — grouping only
        needs file_meta's own path/file_size, no locate_file() at all:
        node construction is pure, with fragment resolution/opening
        deferred entirely to ``open_disk`` (see
        test_units_device_pcps.py for the metadata-only real
        sample this cheapness matters for). Only opening the disk does
        the real per-fragment locate_file() work (PcpsDiskTree.open_disk); since
        this disk's only fragment fails there, there is nothing left to
        build a disk from at all, and that raises NotFoundError directly (see
        test_one_fragment_of_a_disk_unresolvable_surfaces_in_that_disks_own_diagnostic_attrs
        for the partial-failure case, where the disk's *other* fragments
        are still healthy and it opens successfully instead)."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        _write_file_map(tmp_path / "db" / "file_map", [])  # no row for src_path
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            nodes = await provider.children(provider.root())
            assert len(nodes) == 1  # listing succeeds -- looks like a normal disk node
            assert nodes[0].attrs["_kind"] == _NodeKind.PCPS_DISK
            with pytest.raises(NotFoundError, match="file_map"):
                await provider.unit(nodes[0])

    async def test_two_fragments_of_the_same_disk_assemble_into_one_disk_node(self, tmp_path: Path) -> None:
        """Real Windows PC/PS shape: one physical disk
        lands as several independently-registered fragment objects
        sharing the same D(diskUuid)/S(diskIndex) — grouped here into one
        disk node backed by one VirtualDiskContentSource, with a real gap
        between the two fragments neither one covers."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        _write_file_map(
            tmp_path / "db" / "file_map",
            [(path_a, _STREAM_ID, 9, 64, 1, 2), (path_b, _STREAM_ID_B, 9, 64, 1, 2)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9, file_offset=0)
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID_B, session_id=9, file_offset=8192)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID_B) / "0.buk", _DISK_PLAINTEXT_B)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1  # both fragments grouped into one disk, not two nodes
            disk = disks[0]
            assert disk.name == "Disk 0"
            assert disk.size == disk_total
            assert len(disk.attrs["fragments"]) == 2
            assert "diagnostic" not in disk.attrs

            content = (await provider.unit(disk)).open()
            assert await content.read(0, 4096) == _DISK_PLAINTEXT
            assert await content.read(4096, 4096) == bytes(4096)  # the gap between the two fragments
            assert await content.read(8192, 4096) == _DISK_PLAINTEXT_B

    async def test_disks_are_listed_by_disk_index_not_disk_uuid(self, tmp_path: Path) -> None:
        """Regression test: two disks are grouped by ``(disk_uuid,
        disk_index)`` (``_pcps_disk_key()``), and must be listed in
        ``disk_index`` order — the disk_uuid alone has no relationship to
        which disk a user would call "Disk 0" vs "Disk 1". Picks two UUIDs
        whose alphabetical order is the reverse of their disk index, so a
        regression back to sorting by ``disk_uuid`` would list "Disk 1"
        before "Disk 0"."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_disk0 = "PC-uid/ActiveBackup_2026-01-01/D(ZZZZ)S(0).img"
        path_disk1 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)S(1).img"
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        _write_pcps_file_meta(
            tmp_path / "db" / "file_meta",
            [(100, path_disk0, 4096), (101, path_disk1, 8192)],
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert [d.name for d in disks] == ["Disk 0", "Disk 1"]

    async def test_disk_with_non_numeric_disk_index_sorts_after_numeric_disks(self, tmp_path: Path) -> None:
        """Regression test: a real (non-singleton) disk whose ``S(...)``
        capture isn't numeric must sort after every well-formed numeric
        disk, not before "Disk 0" — the ``int()`` ``ValueError`` fallback's
        own sort key must rank it after every numeric disk, not ahead of
        them."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_disk0 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)S(0).img"
        path_disk1 = "PC-uid/ActiveBackup_2026-01-01/D(BBBB)S(1).img"
        path_malformed = "PC-uid/ActiveBackup_2026-01-01/D(CCCC)S(x).img"
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101), (1, 102)],
        )
        _write_pcps_file_meta(
            tmp_path / "db" / "file_meta",
            [(100, path_disk0, 4096), (101, path_disk1, 8192), (102, path_malformed, 2048)],
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert [d.name for d in disks] == ["Disk 0", "Disk 1", "Disk x"]

    async def test_disk_fs_siblings_are_listed_ahead_of_every_disk_image(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test: each disk's "(filesystem)" sibling is a real
        browsable container (``Node.is_leaf=False``) —
        ``disk_fs_containers_before_leaves()`` must list every sibling
        ahead of every disk-image leaf, not interleave
        disk-image-then-its-own-sibling pair by pair, while keeping each
        group's own disk-index order intact."""
        monkeypatch.setattr(device_pcps_module, "disk_fs_available", lambda: True)
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_disk0 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)S(0).img"
        path_disk1 = "PC-uid/ActiveBackup_2026-01-01/D(BBBB)S(1).img"
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        _write_pcps_file_meta(
            tmp_path / "db" / "file_meta",
            [(100, path_disk0, 4096), (101, path_disk1, 8192)],
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            nodes = await provider.children(provider.root())
            assert [(n.name, n.is_leaf) for n in nodes] == [
                ("Disk 0 (filesystem)", False),
                ("Disk 1 (filesystem)", False),
                ("Disk 0", True),
                ("Disk 1", True),
            ]

    async def test_checkpoint_chained_fragments_sharing_one_nominal_offset_assemble_correctly(
        self, tmp_path: Path
    ) -> None:
        """A real, routine (not hypothetical) shape: a backup job long
        enough to cross a checkpoint boundary closes the current physical
        file and reopens the *same* logical
        D+O+V+S segment with an incremented ``_{seq}``, continuing to
        write the segment's next byte range into a new object — not a
        duplicate of the first, and nothing in the copy pipeline
        collapses the two into one fid. Both share the exact same
        ``D(...)O(...)S(...)`` prefix here, differing only by the
        ``_{1}`` suffix our regex doesn't even look at.

        No special-casing is needed for this to work: ``_pcps_disk_key()``
        already groups both into the same disk (it never looks at ``O``
        or the seq suffix), and each fragment's own real, measured
        ``extent()`` naturally lands in a distinct, non-overlapping
        sub-range regardless of the two sharing one nominal ``O(...)``."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_seq0 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_seq1 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0)_{1}.img"
        disk_total = 8192
        _write_file_map(
            tmp_path / "db" / "file_map",
            [(path_seq0, _STREAM_ID, 9, 64, 1, 2), (path_seq1, _STREAM_ID_B, 9, 64, 1, 2)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        _write_pcps_file_meta(
            tmp_path / "db" / "file_meta", [(100, path_seq0, disk_total), (101, path_seq1, disk_total)]
        )
        # seq=0 covers [0, 4096); seq=1 continues at exactly where seq=0
        # left off, [4096, 8192) -- the real "chained, non-overlapping
        # continuation" shape, not a second copy of seq=0's own range.
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9, file_offset=0)
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID_B, session_id=9, file_offset=4096)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID_B) / "0.buk", _DISK_PLAINTEXT_B)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1  # both checkpoint files are one disk, not two
            disk = disks[0]
            assert len(disk.attrs["fragments"]) == 2

            content = (await provider.unit(disk)).open()
            assert await content.read(0, 4096) == _DISK_PLAINTEXT  # seq=0's own data
            assert await content.read(4096, 4096) == _DISK_PLAINTEXT_B  # seq=1's continuation, no gap
            assert await content.read(0, disk_total) == _DISK_PLAINTEXT + _DISK_PLAINTEXT_B

    async def test_one_fragment_of_a_disk_unresolvable_surfaces_in_that_disks_own_diagnostic_attrs(
        self, tmp_path: Path
    ) -> None:
        """A different failure point than the whole-version 'never
        resolved in file_meta at all' diagnostic: this fragment *does*
        resolve in file_meta (so its disk grouping is known), but
        file_map has no row for it in the current generation — only
        discovered when the disk is actually opened (PcpsDiskTree.open_disk),
        not at listing time (node construction is pure and defers all
        fragment resolution to open_disk). The disk's other fragment
        is healthy — opening still succeeds, with the gap recorded in the
        *opened* RestorableUnit's own attrs["diagnostic"], not as a
        separate sibling node."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        _write_file_map(tmp_path / "db" / "file_map", [(path_a, _STREAM_ID, 9, 64, 1, 2)])  # no row for path_b
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9, file_offset=0)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1  # still one real disk node, not a diagnostic sibling
            disk = disks[0]
            assert disk.attrs["_kind"] == _NodeKind.PCPS_DISK
            assert "diagnostic" not in disk.attrs  # not yet known at listing time
            assert len(disk.attrs["fragments"]) == 2  # both still listed -- the gap isn't known yet

            unit = await provider.unit(disk)
            assert "fid=101" in unit.attrs["diagnostic"]
            content = unit.open()
            assert await content.read(0, 4096) == _DISK_PLAINTEXT

    async def test_a_compacted_file_map_row_degrades_the_same_way_as_a_missing_one(self, tmp_path: Path) -> None:
        """Same shape as the "missing row" test above, but ``path_b`` does
        have a ``file_map`` row this time -- just not a ``Complete`` (2)
        one. Status 3 (Compacted) isn't documented as "known-bad" the way
        Corrupted/Tainted are (FORMAT-SPEC.md: file_map-status), so
        ``locate_file()`` raises ``NotFoundError`` for it, same as no row
        at all, and ``device_pcps.py``'s per-fragment catch degrades it to
        a hole exactly the same way."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        _write_file_map(
            tmp_path / "db" / "file_map",
            [(path_a, _STREAM_ID, 9, 64, 1, 2), (path_b, _STREAM_ID, 9, 64, 1, 3)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9, file_offset=0)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            disk = disks[0]
            unit = await provider.unit(disk)
            assert "fid=101" in unit.attrs["diagnostic"]
            content = unit.open()
            assert await content.read(0, 4096) == _DISK_PLAINTEXT

    async def test_a_corrupted_file_map_row_aborts_the_whole_disk_instead_of_degrading(self, tmp_path: Path) -> None:
        """Different from the Compacted case above: status 4 (Corrupted)
        is FORMAT-SPEC.md's own "known-bad" value, so ``locate_file()``
        raises ``DataCorruptError`` instead of ``NotFoundError`` for it —
        ``_open_one()``'s per-fragment catch only swallows ``NotFoundError``,
        so any other exception (like this one) still propagates through
        ``asyncio.gather()`` and fails the whole disk's assembly loudly,
        rather than silently becoming one more hole."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        _write_file_map(
            tmp_path / "db" / "file_map",
            [(path_a, _STREAM_ID, 9, 64, 1, 2), (path_b, _STREAM_ID, 9, 64, 1, 4)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9, file_offset=0)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            disk = disks[0]
            with pytest.raises(DataCorruptError):
                await provider.unit(disk)

    async def test_fragments_are_opened_concurrently_not_one_at_a_time(self, tmp_path: Path) -> None:
        """Every fragment's ``locate_file()``/``extent()`` is independent of
        every other fragment's (nothing reads what another fragment's call
        produced), so all of them run concurrently via ``asyncio.gather()``
        instead of one at a time — proven directly by parking one fragment's
        ``locate_file()`` call on an ``asyncio.Event`` and observing the
        *other* fragment's own ``locate_file()`` has already been called
        before that park is released. A strictly sequential ``for`` loop
        could never reach the second fragment while the first is still
        awaiting inside it."""
        store, layout = _write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        # Both fragments resolve to the same composition record -- this
        # test only cares about locate_file() call concurrency, not each
        # fragment's own distinct byte content.
        _write_file_map(
            tmp_path / "db" / "file_map",
            [(path_a, _STREAM_ID, 9, 64, 1, 2), (path_b, _STREAM_ID, 9, 64, 1, 2)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        _write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=9, file_offset=0)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", _DISK_PLAINTEXT)

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            disk = disks[0]

            called: list[str] = []
            path_a_called = asyncio.Event()
            release = asyncio.Event()
            real_locate_file = DedupRepo.locate_file

            async def _tracking_locate_file(self: DedupRepo, path: str) -> FileLocation:
                called.append(path)
                if path == path_a:
                    path_a_called.set()
                    await release.wait()  # park fragment A's own lookup
                return await real_locate_file(self, path)

            DedupRepo.locate_file = _tracking_locate_file  # type: ignore[method-assign]
            try:
                task = asyncio.create_task(provider.unit(disk))
                # A fixed count of zero-duration ``asyncio.sleep(0)`` ticks
                # only guarantees event-loop turns, not real wall-clock
                # time for locate_file()'s own real sqlite lookup
                # (asyncio.to_thread-backed) to actually resolve under
                # load -- wait on the real condition instead.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(path_a_called.wait(), timeout=5.0)
                assert path_a in called
                # The whole point: fragment B's own locate_file() must
                # already have been called too.
                assert path_b in called
                release.set()
                unit = await task
            finally:
                DedupRepo.locate_file = real_locate_file  # type: ignore[method-assign]

            content = unit.open()
            assert isinstance(content, VirtualDiskContentSource)
            assert len(content.fragments) == 2  # both resolved successfully


class TestLocalFileContentSource:
    async def test_stream_reassembles_to_the_same_bytes_as_read(self, tmp_path: Path) -> None:
        _build_vm_repo(
            tmp_path,
            extra_objects=[(2, 0, "ActiveBackup_2026-01-01/my-vm/disk.img.delta", "", "", 0, 64)],
        )
        delta_dir = tmp_path / "copy_meta_file" / "VM_uid1" / "ActiveBackup_2026-01-01" / "my-vm"
        delta_dir.mkdir(parents=True)
        delta_bytes = b"CbTT" + os.urandom(60)
        (delta_dir / "disk.img.delta").write_bytes(delta_bytes)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            delta = next(o for o in await provider.children(device) if o.kind is UnitKind.FILE)
            content = (await provider.unit(delta)).open()
            reassembled = b"".join([chunk async for _offset, chunk in content.stream(block=10)])
            assert reassembled == delta_bytes

    async def test_export_to_writes_the_full_content(self, tmp_path: Path) -> None:
        _build_vm_repo(
            tmp_path,
            extra_objects=[(2, 0, "ActiveBackup_2026-01-01/my-vm/disk.img.delta", "", "", 0, 64)],
        )
        delta_dir = tmp_path / "copy_meta_file" / "VM_uid1" / "ActiveBackup_2026-01-01" / "my-vm"
        delta_dir.mkdir(parents=True)
        delta_bytes = b"CbTT" + os.urandom(60)
        (delta_dir / "disk.img.delta").write_bytes(delta_bytes)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            delta = next(o for o in await provider.children(device) if o.kind is UnitKind.FILE)
            content = (await provider.unit(delta)).open()
            dst = tmp_path / "out.bin"
            await content.export_to(dst)
            assert dst.read_bytes() == delta_bytes


async def test_object_nodes_raises_not_found_when_version_table_is_empty(tmp_path: Path) -> None:
    store, layout = _write_repo_info_and_vault_key_db(tmp_path)
    _write_file_map(tmp_path / "db" / "file_map", [])
    target_db_path = tmp_path / "copy_meta_file" / "VM_uid1" / "target.db"
    target_db_path.parent.mkdir(parents=True)
    conn = sqlite3.connect(target_db_path)
    conn.execute("CREATE TABLE version_table(id INTEGER PRIMARY KEY, version_id INTEGER)")
    conn.execute(
        "CREATE TABLE device_table(device_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "device_uuid TEXT, host_name TEXT, os_name TEXT)"
    )
    conn.execute("INSERT INTO device_table VALUES (1, 1, 1, 'uuid', 'host', 'os')")
    conn.execute(
        "CREATE TABLE object_table(object_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "data_format INTEGER, file_path TEXT, src_file_path TEXT, temp_postfix TEXT, dedup_object INTEGER, "
        "file_size INTEGER)"
    )
    conn.commit()
    conn.close()
    async with (
        await DedupRepo.open(store, layout) as repo,
        await DeviceProvider.create(repo, _version()) as provider,
    ):
        device = (await provider.children(provider.root()))[0]
        with pytest.raises(NotFoundError):
            await provider.children(device)


class TestPagination:
    """``children()``'s ``offset``/``limit`` semantics differ by node kind:
    device-level listing pushes a real ``ORDER BY host_name, device_id
    LIMIT ? OFFSET ?`` down to SQL, while object-level listing fetches
    every row unpaginated and slices in Python — a dedup object can
    contribute two nodes (itself plus a "(filesystem)" sibling), so a
    row-count-based SQL ``LIMIT``/``OFFSET`` wouldn't line up with the
    actual returned node count."""

    async def test_object_pagination_matches_full_list_slice_sorted_by_file_path(self, tmp_path: Path) -> None:
        # Deliberately inserted out of file_path order — the returned
        # order must come from the real ORDER BY, not insertion order.
        extra = [(oid, 0, f"file{oid}.dat", "", "", 0, 10) for oid in (6, 3, 5, 4, 2)]
        _build_vm_repo(tmp_path, extra_objects=extra)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            full = await provider.children(device)
            assert len(full) == 6  # the original dedup object (id=1) + 5 extra
            paths = [n.attrs["file_path"] for n in full]
            assert paths == sorted(paths)

            page = await provider.children(device, offset=2, limit=2)
            assert [n.attrs["file_path"] for n in page] == [n.attrs["file_path"] for n in full[2:4]]

    async def test_object_pagination_offset_past_end_returns_empty(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, _version()) as provider:
            device = (await provider.children(provider.root()))[0]
            assert await provider.children(device, offset=100, limit=10) == []

    async def test_object_pagination_still_excludes_temp_postfix_rows(self, tmp_path: Path) -> None:
        # temp_postfix filtering is in the WHERE clause itself, not a
        # post-fetch filter, so a filtered-out row is excluded before the
        # Python-side paginate() slice ever sees it -- it must never eat
        # into the requested window.
        extra = [
            (2, 1, "real2.img", "VM-uid/real2", "", 1, 10),
            (3, 1, "interrupted.img", "VM-uid/interrupted", "some-postfix", 1, 10),
            (4, 1, "real3.img", "VM-uid/real3", "", 1, 10),
        ]
        _build_vm_repo(tmp_path, extra_objects=extra)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            page = await provider.children(device, offset=0, limit=3)
            assert len(page) == 3  # the temp_postfix row never counted against the window
            assert all("interrupted" not in n.attrs["file_path"] for n in page)

    async def test_device_pagination_limit_none_returns_everything(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, _version()) as provider:
            assert len(await provider.children(provider.root(), offset=0, limit=None)) == 1


class _StubUnit:
    def open(self) -> object:
        return object()


class TestDiskFsResolution:
    """Direct unit tests for ``DeviceProvider``'s ``disk_fs`` collaborator
    (``DiskFsSibling``)'s
    resolution methods (``resolve``/``children``/``open_entry``) —
    integration replay against real samples only ever exercises the
    "Dissect recognized something" success path, never these
    failure/diagnostic branches. ``provider.unit(source_node)`` is
    stubbed out (that dispatch is already covered elsewhere in this
    file) so only ``DiskFilesystem.open()``'s own outcome drives each
    branch."""

    async def _provider_and_fs_node(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> tuple[DeviceProvider, Node]:
        # Returns the still-open provider for the caller to keep using and
        # close itself -- never close it here, that would hand back an
        # already-closed instance.
        provider = await DeviceProvider.create(repo, _version())

        async def _stub_unit(node: Node) -> _StubUnit:
            return _StubUnit()

        monkeypatch.setattr(provider, "unit", _stub_unit)
        source_node = Node(
            ref=NodeRef("repo", ("disk",)), name="disk0", is_leaf=True, attrs={"_kind": _NodeKind.OBJECT}
        )
        fs_node = provider._disk_fs.root_node(disk_key=("object", 1), source_node=source_node, name="disk0")
        return provider, fs_node

    async def test_disk_filesystem_unavailable_resolves_to_none_without_recording_a_reason(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        provider, fs_node = await self._provider_and_fs_node(repo, monkeypatch)
        try:

            async def _raising_open(cls: object, content: object) -> None:
                raise DiskFilesystemUnavailableError("dissect not installed")

            monkeypatch.setattr(DiskFilesystem, "open", classmethod(_raising_open))

            disk_fs = await provider._disk_fs.resolve(("object", 1), fs_node)

            assert disk_fs is None
            assert ("object", 1) not in provider._disk_fs._diagnostic_reasons
            # Cached the same as a genuine None result, so a repeat call
            # doesn't re-invoke DiskFilesystem.open() a second time.
            assert provider._disk_fs._filesystems[("object", 1)] is None
        finally:
            await provider.close()

    async def test_not_found_resolves_to_none_and_records_the_reason(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        provider, fs_node = await self._provider_and_fs_node(repo, monkeypatch)
        try:
            exc = NotFoundError("no such composition chunk", ref="some/path")

            async def _raising_open(cls: object, content: object) -> None:
                raise exc

            monkeypatch.setattr(DiskFilesystem, "open", classmethod(_raising_open))

            disk_fs = await provider._disk_fs.resolve(("object", 1), fs_node)

            assert disk_fs is None
            assert provider._disk_fs._diagnostic_reasons[("object", 1)] == str(exc)
        finally:
            await provider.close()

    async def test_children_of_an_unresolvable_disk_offers_a_diagnostic_node_only_on_the_first_page(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        provider, fs_node = await self._provider_and_fs_node(repo, monkeypatch)
        try:

            async def _raising_open(cls: object, content: object) -> None:
                raise DiskFilesystemUnavailableError("dissect not installed")

            monkeypatch.setattr(DiskFilesystem, "open", classmethod(_raising_open))

            first_page = await provider._disk_fs.children(fs_node, offset=0)
            assert [n.attrs["_kind"] for n in first_page] == [_NodeKind.DISK_FS_DIAGNOSTIC]

            later_page = await provider._disk_fs.children(fs_node, offset=1)
            assert later_page == []

            # provider.unit() itself is stubbed out by _provider_and_fs_node()
            # above (needed so DiskFsSibling.resolve()'s own unit(source_node) call
            # doesn't require a real disk image) — call the real class method
            # directly here to exercise unit()'s own _DISK_FS_DIAGNOSTIC branch.
            with pytest.raises(NotFoundError):
                await DeviceProvider.unit(provider, first_page[0])
        finally:
            await provider.close()

    async def test_open_disk_fs_entry_resolves_lazily_when_not_already_cached(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A pasted canonical ref can land directly on an entry node in a
        *fresh* ``DeviceProvider`` instance, whose own ``DiskFsSibling``
        collaborator has an empty ``_filesystems`` cache — unlike a live
        tree walk via ``DiskFsSibling.children()`` first, ``open_entry``
        must resolve the filesystem itself in that case rather than assume
        it's already cached."""
        provider, fs_node = await self._provider_and_fs_node(repo, monkeypatch)
        try:

            async def _raising_open(cls: object, content: object) -> None:
                raise DiskFilesystemUnavailableError("dissect not installed")

            monkeypatch.setattr(DiskFilesystem, "open", classmethod(_raising_open))

            entry_node = Node(
                ref=fs_node.ref.child("p1"),
                name="file.txt",
                is_leaf=True,
                attrs={
                    "_kind": _NodeKind.DISK_FS_ENTRY,
                    "disk_key": ("object", 1),
                    "source_node": fs_node.attrs["source_node"],
                    "partition_addr": 0,
                    "path": "/file.txt",
                },
            )
            assert ("object", 1) not in provider._disk_fs._filesystems

            with pytest.raises(NotFoundError, match="no filesystem recognized"):
                await provider._disk_fs.open_entry(entry_node)
        finally:
            await provider.close()

    async def test_children_carries_the_file_state_into_node_attrs_and_the_opened_unit(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A file this SDK believes is a cloud-sync placeholder
        (disk_fs.py's ``_Format.content_unavailable``/
        ``_apfs_is_dataless``) carries its ``FileState`` all the way from
        ``DiskFilesystem.list_dir``'s own ``_DirEntry`` into
        ``Node.attrs["file_state"]``, and from there (via
        ``open_entry``'s own ``attrs=node.attrs``) into the resulting
        ``RestorableUnit`` too — always ``FileState.NORMAL`` for a
        partition/directory node, which has no placeholder concept of
        its own. ``mtime`` rides the same path into ``Node.attrs["mtime"]``."""
        provider, fs_node = await self._provider_and_fs_node(repo, monkeypatch)
        try:
            mtime = datetime(2024, 5, 6, 7, 8, 9, tzinfo=UTC)

            class _FakeContent:
                size = 10

            class _FakeDiskFs:
                def partitions(self) -> list[tuple[int, str]]:
                    return [(0, "fake partition")]

                async def list_dir(self, partition_addr: int, path: str) -> list[_DirEntry]:
                    return [
                        _DirEntry(name="cloud.txt", is_dir=False, size=10, file_state=FileState.CLOUD_ONLY, mtime=mtime)
                    ]

                async def open_file(self, partition_addr: int, path: str) -> _FakeContent:
                    return _FakeContent()

            async def _fake_open(cls: object, content: object) -> _FakeDiskFs:
                return _FakeDiskFs()

            monkeypatch.setattr(DiskFilesystem, "open", classmethod(_fake_open))

            (partition_node,) = await provider._disk_fs.children(fs_node, offset=0)
            assert partition_node.attrs["file_state"] is FileState.NORMAL
            assert "mtime" not in partition_node.attrs

            (file_node,) = await provider._disk_fs.children(partition_node, offset=0)
            assert file_node.attrs["file_state"] is FileState.CLOUD_ONLY
            assert file_node.attrs["mtime"] == mtime

            unit = await provider._disk_fs.open_entry(file_node)
            assert unit.attrs["file_state"] is FileState.CLOUD_ONLY
        finally:
            await provider.close()
