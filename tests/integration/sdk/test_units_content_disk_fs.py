"""Regression tests for the ``"(filesystem)"`` sibling node
(``synology_apm_repo.sdk.units.content.disk_fs``, wired into
``synology_apm_repo.sdk.units.device``) against real NTFS/FAT32/ext4/Btrfs
partitions. Each fixture's one test is its recording recipe:

- ``disk_fs_ntfs_fat32_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``, its Windows VM disk:
  the sibling's partition listing, the NTFS partition's top-level listing
  plus ``Windows/win.ini``'s content, and the FAT32 EFI partition's
  top-level listing.
- ``disk_fs_ntfs_system32_vault_plain.json.gz`` — the same root and disk's NTFS
  ``/Windows/System32`` directory (4445 entries via its ``$I30`` index).
  One flat directory keeps the fixture bounded; a recursive walk would not.
- ``disk_fs_ext4_btrfs_vault_encrypted.json.gz`` — recorded against
  ``vault-encrypted/@ActiveProtectVault``, its Fedora VM disk: the ext4 ``/boot`` top-level listing and the Btrfs partition's
  top-level subvolumes (``home``/``root``). Crossing into subvolumes is
  covered by ``tests/unit/sdk/test_units_content_disk_fs.py``'s
  ``tiny_btrfs_subvols.raw.tar.gz``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING
from support.store_fakes import CountingStore
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.content.disk_fs import DiskFilesystem
from synology_apm_repo.sdk.units.device import DeviceProvider

#: Each sample assigns its own workload ids.
_VAULT_PLAIN_WINDOWS_VM_WORKLOAD_ID = 2
_VAULT_ENCRYPTED_FEDORA_40_VM_WORKLOAD_ID = 1


async def test_replayed_vm_disk_filesystem_sibling_lists_real_ntfs_and_fat32(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: listing a partition parses the disk image's bytes,
    # and win.ini is an OS-shipped default file, not user content.
    store = await record_target("disk_fs_ntfs_fat32_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _VAULT_PLAIN_WINDOWS_VM_WORKLOAD_ID)
        version = next(v for v in await versions(repo, vm) if v.meta is not None)

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            objects = await provider.children(devices[0])
            disk = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
            fs_root = next(o for o in objects if o.kind is UnitKind.DISK_FILESYSTEM)
            assert fs_root.name == f"{disk.name} (filesystem)"
            assert fs_root.is_leaf is False

            partitions = await provider.children(fs_root)
            assert all(p.kind is UnitKind.DISK_FILESYSTEM for p in partitions)
            assert {p.name for p in partitions} == {
                "EFI system partition - NO NAME (FAT)",
                "Basic data partition (NTFS)",
                "Windows Recovery Environment (NTFS)",
            }

            # The first NTFS partition is the C: drive, not the recovery one.
            ntfs_partition = next(p for p in partitions if "NTFS" in p.name)
            fat_partition = next(p for p in partitions if "FAT" in p.name)

            entries = await provider.children(ntfs_partition)
            names = {e.name for e in entries}
            assert names == {
                "$AttrDef",
                "$BadClus",
                "$Bitmap",
                "$Boot",
                "$Extend",
                "$LogFile",
                "$MFT",
                "$MFTMirr",
                "$Recycle.Bin",
                "$Secure",
                "$UpCase",
                "$Volume",
                "Documents and Settings",
                "DumpStack.log.tmp",
                "PerfLogs",
                "Program Files",
                "Program Files (x86)",
                "ProgramData",
                "Recovery",
                "System Volume Information",
                "Users",
                "Windows",
                "pagefile.sys",
                "swapfile.sys",
            }
            windows_dir = next(e for e in entries if e.name == "Windows")
            assert windows_dir.is_leaf is False
            assert windows_dir.kind is UnitKind.DISK_FILESYSTEM
            assert windows_dir.mtime is not None

            windows_entries = await provider.children(windows_dir)
            win_ini = next(e for e in windows_entries if e.name == "win.ini")
            assert win_ini.mtime is not None
            content = (await provider.unit(win_ini)).content
            data = await content.read(0, content.size)
            assert data == (
                b"; for 16-bit app support\r\n[fonts]\r\n[extensions]\r\n"
                b"[mci extensions]\r\n[files]\r\n[Mail]\r\nMAPI=1\r\n"
            )

            fat_entries = await provider.children(fat_partition)
            assert {e.name: e.is_leaf for e in fat_entries} == {
                "EFI": False,
                "System Volume Information": False,
            }


async def test_replayed_ntfs_iterdir_reads_the_file_name_index_attribute_not_a_full_mft_dereference(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """``_ntfs_iterdir`` reads name/is_dir/size/mtime off the ``$I30`` index
    entries (``dereference=False``) instead of resolving each child's
    ``MftRecord``: listing 4445 entries takes a roughly constant number of
    reads, counted by ``CountingStore``, where dereferencing would take at
    least one per entry."""
    # allow_content=True: list_dir() reads the real $I30 index bytes; the
    # assertions are structural (read counts, name/is_dir/size).
    backing = await record_target("disk_fs_ntfs_system32_vault_plain.json.gz", allow_content=True)
    store = CountingStore(backing)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _VAULT_PLAIN_WINDOWS_VM_WORKLOAD_ID)
        version = next(v for v in await versions(repo, vm) if v.meta is not None)

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            objects = await provider.children(devices[0])
            disk_node = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
            content = (await provider.unit(disk_node)).content

            disk_fs = await DiskFilesystem.open(content)
            assert disk_fs is not None
            ntfs_addr = next(addr for addr, label in disk_fs.partitions() if "NTFS" in label)

            reads_before = store.read_count
            entries = await disk_fs.list_dir(ntfs_addr, "/Windows/System32")
            reads_during_list_dir = store.read_count - reads_before
            assert reads_during_list_dir < 500, (
                f"expected a small, roughly constant number of $I30 index reads for 4445 entries, "
                f"got {reads_during_list_dir} -- did _ntfs_iterdir regress back to dereference=True?"
            )

            assert len(entries) == 4445
            by_name = {e.name: e for e in entries}
            assert (by_name["drivers"].is_dir, by_name["drivers"].size) == (True, None)
            assert (by_name["config"].is_dir, by_name["config"].size) == (True, None)
            assert (by_name["notepad.exe"].is_dir, by_name["notepad.exe"].size) == (False, 211968)
            assert (by_name["calc.exe"].is_dir, by_name["calc.exe"].size) == (False, 27648)
            assert (by_name["win32k.sys"].is_dir, by_name["win32k.sys"].size) == (False, 596992)
            assert (by_name["kernel32.dll"].is_dir, by_name["kernel32.dll"].size) == (False, 770144)
            # mtime from the $FILE_NAME index entry, for a file and a directory.
            assert by_name["kernel32.dll"].mtime is not None
            assert by_name["drivers"].mtime is not None


async def test_replayed_vm_disk_filesystem_sibling_lists_real_ext4_boot_and_btrfs_subvolumes(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """The encrypted Fedora VM's ``"(filesystem)"`` sibling lists its ext4
    ``/boot`` partition (with file and directory mtimes) and its Btrfs
    partition's top-level subvolumes."""
    # allow_content=True: listing a partition parses the disk image's real
    # bytes.
    store = await record_target("disk_fs_ext4_btrfs_vault_encrypted.json.gz", allow_content=True)
    keys = KeyMaterial.from_key_string(VAULT_ENCRYPTED_KEY_STRING)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))

    async with await DedupRepo.open(store, layout, keys) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _VAULT_ENCRYPTED_FEDORA_40_VM_WORKLOAD_ID)
        version = next(v for v in await versions(repo, vm) if v.meta is not None)

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            objects = await provider.children(devices[0])
            fs_root = next(o for o in objects if o.kind is UnitKind.DISK_FILESYSTEM)

            partitions = await provider.children(fs_root)
            ext4_partition = next((p for p in partitions if "ext" in p.name.lower()), None)
            assert ext4_partition is not None, (
                f"expected a real ext4 /boot partition, got: {[p.name for p in partitions]}"
            )
            assert "/boot" in ext4_partition.name

            entries = await provider.children(ext4_partition)
            names = {e.name for e in entries}
            assert any(name.startswith("vmlinuz-") for name in names)
            assert any(name.startswith("initramfs-") for name in names)
            assert "grub2" in names
            vmlinuz = next(e for e in entries if e.name.startswith("vmlinuz-"))
            # A file's mtime comes from the inode already read for .size.
            assert vmlinuz.mtime is not None
            grub2_dir = next(e for e in entries if e.name == "grub2")
            # A directory's type comes from the dirent, so its mtime needs
            # its own inode read.
            assert grub2_dir.mtime is not None

            btrfs_partition = next(p for p in partitions if "Btrfs" in p.name)
            top_level_entries = await provider.children(btrfs_partition)
            top_level_names = {e.name for e in top_level_entries}
            assert {"home", "root"} <= top_level_names
