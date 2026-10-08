"""Unit tests for ``synology_apm_repo.sdk.units.device`` — synthetic
repository roots written to real files, no sample repositories required.
``tests/integration/sdk/test_units_device.py`` is the real-data
counterpart."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import zlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

import synology_apm_repo.sdk.units.device as device_module
import synology_apm_repo.sdk.units.device_pcps as device_pcps_module
from support.model_factories import make_version
from support.repo_builders import write_bucket, write_composition, write_file_map, write_pcps_file_meta, write_repo_info
from synology_apm_repo.sdk.catalog.version import Version, VersionMeta, open_target_db
from synology_apm_repo.sdk.dedup.repository import DedupRepo, FileLocation
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError, NotRestorableError, UnsupportedDataFormatError
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import Node, UnitKind
from synology_apm_repo.sdk.units.content.pcps_disk import VirtualDiskContentSource
from synology_apm_repo.sdk.units.device import DeviceProvider
from synology_apm_repo.sdk.units.device_handles import (
    Device,
    DeviceRoot,
    DiskFsDiagnostic,
    DiskFsEntry,
    DiskFsRoot,
    PcpsDiagnostic,
    PcpsDisk,
    PcpsRoot,
    VmObject,
)
from synology_apm_repo.sdk.units.device_pcps import PcpsDiskTree, _pcps_disk_key
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.sdk.device_fakes import (
    DISK_PLAINTEXT,
    VM_STREAM_ID,
    build_vm_repo,
    vm_version,
    write_repo_info_and_vault_key_db,
    write_target_db,
)

_STREAM_ID_B = 6  # a second, independent composition/bucket for a disk's second fragment
_DISK_PLAINTEXT_B = b"\xbb\xcc" + b"\x00" * 4094  # distinguishable from DISK_PLAINTEXT
assert len(_DISK_PLAINTEXT_B) == 4096


def _pcps_disk(node: Node) -> PcpsDisk:
    assert isinstance(node.handle, PcpsDisk)
    return node.handle


def _version_no_meta() -> Version:
    """Like ``vm_version`` but ``meta=None`` (no ``copy_target_version_meta``
    row), which makes ``resolve_copy_meta_dir`` raise ``NotFoundError``."""
    return make_version(target_id="VM-uid")


def _pcps_version(version_uid: str = "vuid-pcps", target_type: str = "PC", meta: VersionMeta | None = None) -> Version:
    return make_version(version_uid=version_uid, target_type=target_type, target_id="PC-uid", meta=meta)


pytestmark = pytest.mark.usefixtures("no_disk_fs_sibling")


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[DedupRepo]:
    build_vm_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as r:
        yield r


class TestTree:
    async def test_root_is_a_device_root_container(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            root = provider.root()
            assert isinstance(root.handle, DeviceRoot)
            assert root.is_leaf is False

    async def test_children_of_an_unrecognized_node_is_empty(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            mystery_node = Node(ref=NodeRef("repo", ("mystery",)), name="mystery", is_leaf=False, handle="?")
            assert await provider.children(mystery_node) == []

    async def test_unit_on_an_unrecognized_node_raises(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            mystery_node = Node(ref=NodeRef("repo", ("mystery",)), name="mystery", is_leaf=True, handle="?")
            with pytest.raises(NotRestorableError, match="not a restorable unit"):
                await provider.unit(mystery_node)

    async def test_children_of_root_are_devices(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            devices = await provider.children(provider.root())
            assert len(devices) == 1
            assert devices[0].name == "my-vm"
            assert devices[0].details["os_name"] == "Windows"

    async def test_children_of_device_are_objects(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)
            assert len(objects) == 1
            assert objects[0].kind is UnitKind.DISK_IMAGE
            assert objects[0].size == 4096

    async def test_close_closes_the_cached_target_db_connection(self, repo: DedupRepo) -> None:
        # close() is required: an unclosed aiosqlite worker thread is
        # non-daemon and keeps the interpreter alive.
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            device = (await provider.children(provider.root()))[0]
            await provider.children(device)  # populates _target_db via _object_nodes -> _target_db_source()
            target_db = await provider._target_db_source()
            # from_enveloped_store() always materializes into a
            # TemporaryDirectory, so check _tmp_dir, not _path.
            tmp_dir = target_db._tmp_dir
            assert tmp_dir is not None
            assert os.path.exists(tmp_dir.name)

            await provider.close()

            assert not provider._target_db.opened
            assert not os.path.exists(tmp_dir.name)  # SqliteSource.close() removes its temp dir

    async def test_concurrent_first_uses_open_target_db_once(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two ``children()`` calls racing the first open must share one
        ``target.db`` connection: a second one would be overwritten and never
        closed."""
        opened: list[object] = []
        real = open_target_db

        async def counting(*args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(0)
            source = await real(*args, **kwargs)
            opened.append(source)
            return source

        monkeypatch.setattr(device_module, "open_target_db", counting)
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            root = provider.root()
            await asyncio.gather(provider.children(root), provider.children(root))
        assert len(opened) == 1

    async def test_dedup_object_gets_a_disk_fs_sibling_node_when_available(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Re-enables what the module's ``no_disk_fs_sibling`` fixture disables, to
        exercise ``_object_nodes``'s ``disk_fs_available()`` branch."""
        monkeypatch.setattr(device_module, "disk_fs_available", lambda: True)
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)

            assert len(objects) == 2
            disk = next(n for n in objects if n.kind is UnitKind.DISK_IMAGE)
            sibling = next(n for n in objects if n is not disk)
            assert sibling.name == f"{disk.name} (filesystem)"
            assert sibling.kind is UnitKind.DISK_FILESYSTEM
            # disk_fs_containers_before_leaves(): the container sibling
            # lists ahead of its disk-image leaf.
            assert objects == [sibling, disk]

    async def test_device_and_object_refs_round_trip_through_str_and_parse(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            device = (await provider.children(provider.root()))[0]
            obj = (await provider.children(device))[0]

            for node in (device, obj):
                roundtripped = NodeRef.parse(str(node.ref))
                assert roundtripped == node.ref
                assert roundtripped.canonical_ids == (
                    CatalogId(str(vm_version().connection_config_id)),
                    vm_version().workload_id,
                    vm_version().version_uid,
                )
            assert isinstance(device.handle, Device)
            assert device.ref.extra_segments == (f"device:{device.handle.config_device_id}",)
            assert obj.ref.extra_segments == (
                f"device:{device.handle.config_device_id}",
                f"object:{obj.details['object_id']}",
            )

    async def test_temp_postfix_rows_are_filtered_out(self, tmp_path: Path) -> None:
        build_vm_repo(
            tmp_path,
            extra_objects=[(2, 1, "path", "VM-uid/incomplete", "some-postfix", 1, 100)],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)
            assert len(objects) == 1  # the temp_postfix row is excluded

    async def test_non_dedup_row_is_kind_file(self, tmp_path: Path) -> None:
        build_vm_repo(
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
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)
            delta = next(o for o in objects if o.kind is UnitKind.FILE)
            unit = await provider.unit(delta)
            content = unit.content
            assert await content.read(0, 4) == b"CbTT"


class TestVmTargetDbMissingRaisesNotFound:
    """A VM version whose ``target.db`` isn't readable surfaces as an
    uncaught ``NotFoundError`` from ``DeviceProvider.children()``."""

    async def test_no_meta_row_at_all_raises_not_found(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _version_no_meta()) as provider,
        ):
            with pytest.raises(NotFoundError, match="has no copy_target_version_meta row"):
                await provider.children(provider.root())

    async def test_target_db_not_registered_in_meta_filenames_raises_not_found(self, tmp_path: Path) -> None:
        """``resolve_meta_filename()`` raises here, a catalog-driven failure
        with no store I/O."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, vm_version(meta_filenames=("snapshot_info.json",))) as provider,
        ):
            with pytest.raises(NotFoundError, match=r"target.db"):
                await provider.children(provider.root())

    async def test_target_db_registered_but_missing_from_the_store_raises_not_found(self, tmp_path: Path) -> None:
        """``target.db`` is registered and ``Complete`` but absent from an existing
        ``copy_meta_file/<dir>``: ``NotFoundError`` propagates undegraded."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        (tmp_path / "copy_meta_file" / "VM_uid1").mkdir(parents=True)
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            with pytest.raises(NotFoundError, match=r"target.db"):
                await provider.children(provider.root())


class TestOpenDiskImage:
    async def test_reads_the_real_dedup_content(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            device = (await provider.children(provider.root()))[0]
            disk = (await provider.children(device))[0]
            unit = await provider.unit(disk)
            assert (await unit.content.read(0, 4096)) == DISK_PLAINTEXT

    async def test_unsupported_data_format_raises_on_open_not_on_list(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        write_target_db(
            tmp_path / "copy_meta_file" / "VM_uid1" / "target.db",
            objects=[(1, 2, "disk.img", "VM-uid/disk.img", "", 1, 4096)],  # data_format=2 (CBT)
        )
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            objects = await provider.children(device)
            assert len(objects) == 1  # still listed
            assert isinstance(objects[0].handle, VmObject)
            assert objects[0].handle.unsupported is True
            with pytest.raises(UnsupportedDataFormatError, match="has unsupported data_format"):
                await provider.unit(objects[0])


class TestEncryptedTargetDb:
    async def test_ahlt_enveloped_target_db_is_decrypted(self, tmp_path: Path) -> None:
        from synology_apm_repo.sdk.dedup.keys import KeyMaterial

        vault_key = os.urandom(32)
        user_key_id = "abcdefghijkl"
        user_key = os.urandom(32)
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        wrapped = AESGCM(user_key).encrypt(user_key_id.encode()[:12], vault_key, None)

        write_repo_info(tmp_path / "repo_info")
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
        write_file_map(tmp_path / "db" / "file_map", [(src_path, VM_STREAM_ID, 9, 64, 1, 2)])
        plain_db_path = tmp_path / "_plain_target.db"
        write_target_db(plain_db_path, objects=[(1, 1, "path", src_path, "", 1, 4096)])
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

        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk",
            [DISK_PLAINTEXT],
            vault_key=vault_key,
            stream_id=VM_STREAM_ID,
        )

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        keys = KeyMaterial(user_key_id=user_key_id, user_key=user_key)
        async with (
            await DedupRepo.open(store, layout, keys) as repo,
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            disk = (await provider.children(device))[0]
            content = (await provider.unit(disk)).content
            assert await content.read(0, 4096) == DISK_PLAINTEXT


def _write_copy_target_version_and_file(
    path: Path, *, version_rows: list[tuple[int, str]], file_rows: list[tuple[int, int]]
) -> None:
    """One sqlite file holding both ``copy_target_version`` and
    ``copy_target_file``, the real on-disk shape."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE copy_target_version(version_id INTEGER PRIMARY KEY, version_uid TEXT)")
    conn.executemany("INSERT INTO copy_target_version VALUES (?, ?)", version_rows)
    conn.execute("CREATE TABLE copy_target_file(version_id INTEGER, fid INTEGER)")
    conn.executemany("INSERT INTO copy_target_file VALUES (?, ?)", file_rows)
    conn.commit()
    conn.close()


class TestVmVsPcPsDispatch:
    """``_is_pcps`` depends only on ``Version.target_type``, never on which
    files exist."""

    async def test_ps_with_no_meta_row_at_all_is_pcps(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version(target_type="PS", meta=None)) as provider,
        ):
            assert isinstance(provider.root().handle, PcpsRoot)

    async def test_pc_with_a_real_but_target_db_less_meta_dir_is_still_pcps(self, tmp_path: Path) -> None:
        """A PC/PS version whose meta directory landed with only
        ``snapshot_info.json`` (FORMAT-SPEC.md: Landing directory layout) is still PC/PS, not
        VM."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
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
            assert isinstance(provider.root().handle, PcpsRoot)
            # Must not raise trying to open a target.db that was never there.
            assert await provider.children(provider.root()) == []


class TestPcPsDiskKey:
    """``_pcps_disk_key()``'s regex against the
    ``D(diskUuid)O(offset)[V(volumeUuid)]S(diskIndex)`` naming; the
    encoder omits an empty component, so ``O(...)`` or ``V(...)`` may be
    absent."""

    @pytest.mark.parametrize(
        ("fid", "path", "expected"),
        [
            pytest.param(
                1, ".../D(AAAA)O(17408)S(0).img", ("AAAA", "0"), id="the_usual_shape_with_offset_and_no_volume"
            ),
            # Groups with its siblings rather than becoming a singleton.
            pytest.param(1, ".../D(AAAA)S(0).img", ("AAAA", "0"), id="offset_omitted_still_groups_by_disk_and_index"),
            pytest.param(
                1,
                ".../D(AAAA)O(135266304)V(BBBB)S(0).img",
                ("AAAA", "0"),
                id="volume_present_is_ignored_for_the_grouping_key",
            ),
            pytest.param(1, ".../D(AAAA)S(1).img", ("AAAA", "1"), id="offset_and_volume_both_omitted"),
            # The real suffix is "_{N}"; .search() ignores whatever follows S(...).
            pytest.param(
                1, ".../D(AAAA)O(0)S(0)_{2}.img", ("AAAA", "0"), id="a_trailing_seq_suffix_does_not_break_the_match"
            ),
            # The macOS shape: no D()/S() structure.
            pytest.param(
                42,
                ".../00000000-0000-4000-8000-0000000000A1_00000000-0000-4000-8000-0000000000B2.img",
                ("_single", "42"),
                id="no_match_at_all_falls_back_to_a_singleton_keyed_by_fid",
            ),
        ],
    )
    def test_pcps_disk_key(self, fid: int, path: str, expected: tuple[str, str]) -> None:
        assert _pcps_disk_key(fid, path) == expected


class TestPcPsFallback:
    async def test_missing_copy_target_version_file_gives_no_disks(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        # No db/copy_target_version at all; copy_target_file shares its file.
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            assert await provider.children(provider.root()) == []

    async def test_unknown_version_uid_gives_no_disks(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        _write_copy_target_version_and_file(tmp_path / "db" / "copy_target_version", version_rows=[], file_rows=[])
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version(version_uid="no-such-version")) as provider,
        ):
            assert await provider.children(provider.root()) == []

    async def test_no_matching_fids_gives_no_disks(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version", version_rows=[(1, "vuid-pcps")], file_rows=[]
        )
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            assert await provider.children(provider.root()) == []

    async def test_pcps_lists_disks_via_copy_target_file_chain(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        write_file_map(tmp_path / "db" / "file_map", [(src_path, VM_STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )

        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])

        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1
            assert disks[0].name == "disk0.img"
            # The listing carries file_meta.file_size, as the VM path does.
            assert disks[0].size == 4096
            unit = await provider.unit(disks[0])
            assert (await unit.content.read(0, 4096)) == DISK_PLAINTEXT

    async def test_null_file_size_falls_back_from_listing_to_the_real_extent_on_open(self, tmp_path: Path) -> None:
        """``file_meta.file_size`` may be NULL for every fragment: the
        listed ``Node.size`` is then ``None``, and the opened unit's size
        is re-derived from the composition's extent."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        write_file_map(tmp_path / "db" / "file_map", [(src_path, VM_STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, None)])

        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1
            assert disks[0].size is None  # nothing cheap available at listing time
            unit = await provider.unit(disks[0])
            assert unit.size == len(DISK_PLAINTEXT)  # re-derived from the real extent once opened
            assert (await unit.content.read(0, len(DISK_PLAINTEXT))) == DISK_PLAINTEXT

    async def test_fid_registered_but_absent_from_file_meta_surfaces_a_diagnostic_node(self, tmp_path: Path) -> None:
        """A fid registered in ``copy_target_file`` with no row in
        ``file_meta``'s resolved generation (FORMAT-SPEC.md: Multi-generation selection) surfaces
        as a diagnostic node, not a silent empty list."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        # file_meta exists but has no row for fid=100 at all.
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [])

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            nodes = await provider.children(provider.root())
            assert len(nodes) == 1
            assert nodes[0].handle == PcpsDiagnostic((100,))
            with pytest.raises(NotFoundError, match="copy_target_file registers"):
                await provider.unit(nodes[0])

    async def test_diagnostic_node_not_appended_when_the_first_page_is_already_full(self, tmp_path: Path) -> None:
        """The never-resolved-fid diagnostic is appended only while the first
        page has room; it never pushes the page past ``limit``."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        write_file_map(tmp_path / "db" / "file_map", [(src_path, VM_STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 200)],  # fid 200 has no file_meta row at all
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])
        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            nodes = await provider.children(provider.root(), offset=0, limit=1)
            assert len(nodes) == 1
            assert isinstance(nodes[0].handle, PcpsDisk)

    async def test_pcps_object_nodes_builds_once_across_concurrent_and_repeated_calls(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Concurrent first ``children()`` calls share one ``PcpsDiskTree._build_nodes()``,
        and later calls reuse its result."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        write_file_map(tmp_path / "db" / "file_map", [(src_path, VM_STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])
        calls = 0
        original = PcpsDiskTree._build_nodes

        async def _counting_build(tree: PcpsDiskTree) -> tuple[list[Node], list[int]]:
            nonlocal calls
            calls += 1
            await asyncio.sleep(0)  # let the concurrent caller arrive mid-build
            return await original(tree)

        monkeypatch.setattr(PcpsDiskTree, "_build_nodes", _counting_build)
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            root = provider.root()
            first, second = await asyncio.gather(
                provider.children(root, offset=0, limit=1), provider.children(root, offset=0, limit=1)
            )
            assert first == second
            await provider.children(root, offset=0, limit=1)
            assert calls == 1

    async def test_open_pcps_disk_twice_returns_the_same_cached_unit(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        write_file_map(tmp_path / "db" / "file_map", [(src_path, VM_STREAM_ID, 9, 64, 1, 2)])
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])
        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )

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
        """Listing succeeds from ``file_meta`` alone; ``open_disk`` raises ``NotFoundError``
        when its only fragment is unresolvable."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        write_file_map(tmp_path / "db" / "file_map", [])  # no row for src_path
        src_path = "PC-uid/ActiveBackup_2026-01-01/disk0.img"
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, src_path, 4096)])

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            nodes = await provider.children(provider.root())
            assert len(nodes) == 1  # listing succeeds -- looks like a normal disk node
            assert isinstance(nodes[0].handle, PcpsDisk)
            with pytest.raises(NotFoundError, match="file_map"):
                await provider.unit(nodes[0])

    async def test_two_fragments_of_the_same_disk_assemble_into_one_disk_node(self, tmp_path: Path) -> None:
        """Fragment objects sharing D(diskUuid)/S(diskIndex) group into one disk node
        backed by one ``VirtualDiskContentSource``; a gap between them reads as zeros."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        write_file_map(
            tmp_path / "db" / "file_map",
            [(path_a, VM_STREAM_ID, 9, 64, 1, 2), (path_b, _STREAM_ID_B, 9, 64, 1, 2)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9, file_offset=0)
        write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID_B, session_id=9, file_offset=8192)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )
        write_bucket(
            tmp_path / "@data" / "Pool" / str(_STREAM_ID_B) / "0.buk", [_DISK_PLAINTEXT_B], stream_id=VM_STREAM_ID
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1  # both fragments grouped into one disk, not two nodes
            disk = disks[0]
            assert disk.name == "Disk 0"
            assert disk.size == disk_total
            assert len(_pcps_disk(disk).fragments) == 2
            assert not disk.is_diagnostic

            content = (await provider.unit(disk)).content
            assert await content.read(0, 4096) == DISK_PLAINTEXT
            assert await content.read(4096, 4096) == bytes(4096)  # the gap between the two fragments
            assert await content.read(8192, 4096) == _DISK_PLAINTEXT_B

    async def test_disks_are_listed_by_disk_index_not_disk_uuid(self, tmp_path: Path) -> None:
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_disk0 = "PC-uid/ActiveBackup_2026-01-01/D(ZZZZ)S(0).img"
        path_disk1 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)S(1).img"
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        write_pcps_file_meta(
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
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_disk0 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)S(0).img"
        path_disk1 = "PC-uid/ActiveBackup_2026-01-01/D(BBBB)S(1).img"
        path_malformed = "PC-uid/ActiveBackup_2026-01-01/D(CCCC)S(x).img"
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101), (1, 102)],
        )
        write_pcps_file_meta(
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
        """Every "(filesystem)" container sibling lists ahead of every disk-image leaf,
        keeping disk-index order within each group."""
        monkeypatch.setattr(device_pcps_module, "disk_fs_available", lambda: True)
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_disk0 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)S(0).img"
        path_disk1 = "PC-uid/ActiveBackup_2026-01-01/D(BBBB)S(1).img"
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        write_pcps_file_meta(
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
        """Fragments differing only in the ``_{seq}`` suffix group into one disk, and
        each fragment's measured ``extent()`` lands in a distinct sub-range."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_seq0 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_seq1 = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0)_{1}.img"
        disk_total = 8192
        write_file_map(
            tmp_path / "db" / "file_map",
            [(path_seq0, VM_STREAM_ID, 9, 64, 1, 2), (path_seq1, _STREAM_ID_B, 9, 64, 1, 2)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        write_pcps_file_meta(
            tmp_path / "db" / "file_meta", [(100, path_seq0, disk_total), (101, path_seq1, disk_total)]
        )
        # seq=0 covers [0, 4096); seq=1 continues at [4096, 8192).
        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9, file_offset=0)
        write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID_B, session_id=9, file_offset=4096)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )
        write_bucket(
            tmp_path / "@data" / "Pool" / str(_STREAM_ID_B) / "0.buk", [_DISK_PLAINTEXT_B], stream_id=VM_STREAM_ID
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1  # both checkpoint files are one disk, not two
            disk = disks[0]
            assert len(_pcps_disk(disk).fragments) == 2

            content = (await provider.unit(disk)).content
            assert await content.read(0, 4096) == DISK_PLAINTEXT  # seq=0's own data
            assert await content.read(4096, 4096) == _DISK_PLAINTEXT_B  # seq=1's continuation, no gap
            assert await content.read(0, disk_total) == DISK_PLAINTEXT + _DISK_PLAINTEXT_B

    async def test_one_fragment_of_a_disk_unresolvable_degrades_that_disks_opened_unit(self, tmp_path: Path) -> None:
        """A fragment with no ``file_map`` row is discovered only at ``open_disk``; the
        disk still opens with ``degraded`` set and the gap in ``details["missing_fragments"]``."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        write_file_map(tmp_path / "db" / "file_map", [(path_a, VM_STREAM_ID, 9, 64, 1, 2)])  # no row for path_b
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9, file_offset=0)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            assert len(disks) == 1  # still one real disk node, not a diagnostic sibling
            disk = disks[0]
            assert isinstance(disk.handle, PcpsDisk)
            assert not disk.is_diagnostic  # not yet known at listing time
            assert len(_pcps_disk(disk).fragments) == 2  # both still listed -- the gap isn't known yet

            unit = await provider.unit(disk)
            assert unit.degraded == "1 of 2 parts of this disk are missing from the backup; they read as zeros"
            assert "fid=101" in str(unit.details["missing_fragments"])
            content = unit.content
            assert await content.read(0, 4096) == DISK_PLAINTEXT

    async def test_a_compacted_file_map_row_degrades_the_same_way_as_a_missing_one(self, tmp_path: Path) -> None:
        """A ``file_map`` row with status 3 (Compacted, FORMAT-SPEC.md: db/file_map) degrades to a hole."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        write_file_map(
            tmp_path / "db" / "file_map",
            [(path_a, VM_STREAM_ID, 9, 64, 1, 2), (path_b, VM_STREAM_ID, 9, 64, 1, 3)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9, file_offset=0)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            disk = disks[0]
            unit = await provider.unit(disk)
            assert unit.degraded == "1 of 2 parts of this disk are missing from the backup; they read as zeros"
            assert "fid=101" in str(unit.details["missing_fragments"])
            content = unit.content
            assert await content.read(0, 4096) == DISK_PLAINTEXT

    async def test_a_corrupted_file_map_row_aborts_the_whole_disk_instead_of_degrading(self, tmp_path: Path) -> None:
        """A ``file_map`` row with status 4 (Corrupted, FORMAT-SPEC.md: db/file_map) raises
        ``DataCorruptError`` for the whole disk instead of becoming a hole."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        write_file_map(
            tmp_path / "db" / "file_map",
            [(path_a, VM_STREAM_ID, 9, 64, 1, 2), (path_b, VM_STREAM_ID, 9, 64, 1, 4)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9, file_offset=0)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )

        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, _pcps_version()) as provider,
        ):
            disks = await provider.children(provider.root())
            disk = disks[0]
            with pytest.raises(DataCorruptError, match=r"file_map row for path .* \(Corrupted/Tainted\)"):
                await provider.unit(disk)

    async def test_fragments_are_opened_concurrently_not_one_at_a_time(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With fragment A's ``locate_file()`` parked, fragment B's has already been called."""
        store, layout = write_repo_info_and_vault_key_db(tmp_path)
        path_a = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(0)S(0).img"
        path_b = "PC-uid/ActiveBackup_2026-01-01/D(AAAA)O(8192)S(0).img"
        disk_total = 12288
        # Both fragments share one composition; only call concurrency matters.
        write_file_map(
            tmp_path / "db" / "file_map",
            [(path_a, VM_STREAM_ID, 9, 64, 1, 2), (path_b, VM_STREAM_ID, 9, 64, 1, 2)],
        )
        _write_copy_target_version_and_file(
            tmp_path / "db" / "copy_target_version",
            version_rows=[(1, "vuid-pcps")],
            file_rows=[(1, 100), (1, 101)],
        )
        write_pcps_file_meta(tmp_path / "db" / "file_meta", [(100, path_a, disk_total), (101, path_b, disk_total)])
        write_composition(tmp_path / "@data" / "Composition", stream_id=VM_STREAM_ID, session_id=9, file_offset=0)
        write_bucket(
            tmp_path / "@data" / "Pool" / str(VM_STREAM_ID) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID
        )

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

            monkeypatch.setattr(DedupRepo, "locate_file", _tracking_locate_file)
            task = asyncio.create_task(provider.unit(disk))
            # Wait on the condition; sleep(0) ticks don't cover
            # locate_file()'s to_thread-backed lookup under load.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(path_a_called.wait(), timeout=5.0)
            assert path_a in called
            assert path_b in called
            release.set()
            unit = await task

            content = unit.content
            assert isinstance(content, VirtualDiskContentSource)
            assert len(content.fragments) == 2  # both resolved successfully


async def test_object_nodes_raises_not_found_when_version_table_is_empty(tmp_path: Path) -> None:
    store, layout = write_repo_info_and_vault_key_db(tmp_path)
    write_file_map(tmp_path / "db" / "file_map", [])
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
        await DeviceProvider.create(repo, vm_version()) as provider,
    ):
        device = (await provider.children(provider.root()))[0]
        with pytest.raises(NotFoundError, match=r"target\.db has no version_table row"):
            await provider.children(device)


class TestPagination:
    """``children()`` pagination: device listing pushes ``ORDER BY host_name,
    device_id LIMIT ? OFFSET ?`` to SQL, while object listing slices in
    Python, since a dedup object can yield two nodes (itself and a
    "(filesystem)" sibling)."""

    async def test_object_pagination_matches_full_list_slice_sorted_by_file_path(self, tmp_path: Path) -> None:
        # Inserted out of file_path order; the ORDER BY decides the result.
        extra = [(oid, 0, f"file{oid}.dat", "", "", 0, 10) for oid in (6, 3, 5, 4, 2)]
        build_vm_repo(tmp_path, extra_objects=extra)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            full = await provider.children(device)
            assert len(full) == 6  # the original dedup object (id=1) + 5 extra
            paths = [str(n.details["file_path"]) for n in full]
            assert paths == sorted(paths)

            page = await provider.children(device, offset=2, limit=2)
            assert [n.details["file_path"] for n in page] == [n.details["file_path"] for n in full[2:4]]

    async def test_object_pagination_offset_past_end_returns_empty(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            device = (await provider.children(provider.root()))[0]
            assert await provider.children(device, offset=100, limit=10) == []

    async def test_object_pagination_still_excludes_temp_postfix_rows(self, tmp_path: Path) -> None:
        # The WHERE clause excludes temp_postfix rows before paginate(),
        # so they never eat into the window.
        extra = [
            (2, 1, "real2.img", "VM-uid/real2", "", 1, 10),
            (3, 1, "interrupted.img", "VM-uid/interrupted", "some-postfix", 1, 10),
            (4, 1, "real3.img", "VM-uid/real3", "", 1, 10),
        ]
        build_vm_repo(tmp_path, extra_objects=extra)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            await DeviceProvider.create(repo, vm_version()) as provider,
        ):
            device = (await provider.children(provider.root()))[0]
            page = await provider.children(device, offset=0, limit=3)
            assert len(page) == 3  # the temp_postfix row never counted against the window
            assert all("interrupted" not in str(n.details["file_path"]) for n in page)

    async def test_device_pagination_limit_none_returns_everything(self, repo: DedupRepo) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            assert len(await provider.children(provider.root(), offset=0, limit=None)) == 1


class TestDiskFsDispatch:
    """``DeviceProvider`` hands a "(filesystem)" sibling's nodes to its
    ``DiskFsSibling`` (``test_units_device_disk_fs.py`` tests that class);
    the synthetic disk image carries no recognizable filesystem."""

    async def _sibling_node(self, provider: DeviceProvider, monkeypatch: pytest.MonkeyPatch) -> Node:
        monkeypatch.setattr(device_module, "disk_fs_available", lambda: True)
        device = (await provider.children(provider.root()))[0]
        return next(n for n in await provider.children(device) if n.kind is UnitKind.DISK_FILESYSTEM)

    async def test_children_of_a_sibling_parse_the_disk_image_through_unit(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            sibling = await self._sibling_node(provider, monkeypatch)
            (diagnostic,) = await provider.children(sibling)
            assert isinstance(diagnostic.handle, DiskFsDiagnostic)

    async def test_unit_of_a_sibling_diagnostic_raises_not_found(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            (diagnostic,) = await provider.children(await self._sibling_node(provider, monkeypatch))
            with pytest.raises(NotFoundError, match="no filesystem could be recognized") as exc_info:
                await provider.unit(diagnostic)
            assert exc_info.value.ref == "vuid-1"

    async def test_unit_of_a_sibling_entry_is_opened_by_the_sibling(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with await DeviceProvider.create(repo, vm_version()) as provider:
            sibling = await self._sibling_node(provider, monkeypatch)
            assert isinstance(sibling.handle, DiskFsRoot)
            entry = Node(
                ref=sibling.ref.child("p0"),
                name="file.txt",
                is_leaf=True,
                handle=DiskFsEntry(sibling.handle.disk_key, sibling.handle.source_node, 0, "/file.txt"),
            )
            with pytest.raises(NotFoundError, match="no filesystem recognized"):
                await provider.unit(entry)
