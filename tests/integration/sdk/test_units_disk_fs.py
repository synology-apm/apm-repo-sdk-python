"""Regression test for the additive ``"(filesystem)"`` sibling node
(``synology_apm_repo.sdk.units.content.disk_fs``, wired into
``synology_apm_repo.sdk.units.device``) against real NTFS/FAT32/ext4/Btrfs
partitions, replayed from committed fixtures recorded against real bytes,
with **no external dependency**: these always run, on CI or anywhere
else, because they go through ``ReplayStore`` instead of a real
``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded against a real store — see
``tests/CLAUDE.md``'s "Recording a fixture" section for the ``pytest
--record-against=...``/``make record-fixture`` workflow that
(re-)records these):

- ``disk_fs_ntfs_fat32_apv1.json.gz`` — apv-sample-1's Windows VM
  disk: the ``"(filesystem)"`` sibling's partition listing, the NTFS
  partition's top-level directory listing plus one real file's content
  (``Windows/win.ini``), and the FAT32 EFI system partition's top-level
  listing.
- ``disk_fs_ntfs_system32_apv1.json.gz`` (the largest of the
  three here) — the same VM disk's real
  NTFS ``/Windows/System32`` directory (4445 entries via a real ``$I30``
  index) — right at the edge of what's
  proportionate for a directory-listing regression test; a full recursive
  tree walk would be far larger (this project's own recording guidance
  warns about exactly that shape of blowup), but one flat directory's
  listing stays bounded by that directory's own entry count.
- ``disk_fs_ext4_btrfs_apv2.json.gz`` — apv-sample-2-encrypted's
  Fedora VM disk: the ext4 ``/boot`` partition's top-level listing, and
  the Btrfs partition's own top-level subvolume listing (``home``/
  ``root``). Deeper subvolume-crossing coverage lives in
  ``tests/unit/sdk/test_units_disk_fs.py``'s own Docker-built
  ``tiny_btrfs_subvols.raw.tar.gz`` (a real multi-subvolume Btrfs image
  this project owns outright), not here.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.content.disk_fs import DiskFilesystem
from synology_apm_repo.sdk.units.device import DeviceProvider

#: apv-sample-2-encrypted's real key — see ``tests/CLAUDE.md``'s
#: "Recording a fixture" section for why this literal is safe to commit.
_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="

#: Internal catalog identifiers -- stable and non-identifying (never
#: touched by catalog-metadata anonymization). Each real sample assigns
#: its own ids independently, so apv-sample-1's VM and apv-sample-2-
#: encrypted's VM need separate constants even though both are "the
#: Windows"/"the Fedora" VM informally.
_APV1_WINDOWS_VM_WORKLOAD_ID = 2
_APV2_ENCRYPTED_FEDORA_40_VM_WORKLOAD_ID = 1


async def test_replayed_vm_disk_filesystem_sibling_lists_real_ntfs_and_fat32(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: listing a real NTFS/FAT32 partition's directory
    # entries requires parsing the disk image's own real bytes (not just
    # metadata), and win.ini's content below is a deterministic OS-shipped
    # default file, not user content.
    store = await record_target("disk_fs_ntfs_fat32_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _APV1_WINDOWS_VM_WORKLOAD_ID)
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
            # The real, deterministic partition set this fixture recorded --
            # one FAT32 EFI system partition and two NTFS partitions (the C:
            # drive and the recovery partition).
            assert {p.name for p in partitions} == {
                "EFI system partition - NO NAME (FAT)",
                "Basic data partition (NTFS)",
                "Windows Recovery Environment (NTFS)",
            }

            # ``next(...)`` picks the first NTFS match in partitions' recorded
            # order -- the C: drive ("Basic data partition"), not the recovery
            # partition.
            ntfs_partition = next(p for p in partitions if "NTFS" in p.name)
            fat_partition = next(p for p in partitions if "FAT" in p.name)

            entries = await provider.children(ntfs_partition)
            names = {e.name for e in entries}
            # The real, deterministic top-level C: drive listing this fixture
            # recorded (NTFS metadata files included).
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

            windows_entries = await provider.children(windows_dir)
            win_ini = next(e for e in windows_entries if e.name == "win.ini")
            content = (await provider.unit(win_ini)).open()
            data = await content.read(0, content.size)
            assert data == (
                b"; for 16-bit app support\r\n[fonts]\r\n[extensions]\r\n"
                b"[mci extensions]\r\n[files]\r\n[Mail]\r\nMAPI=1\r\n"
            )

            # The real, deterministic FAT32 EFI system partition top-level
            # listing this fixture recorded -- both entries are directories.
            fat_entries = await provider.children(fat_partition)
            assert {e.name: e.is_leaf for e in fat_entries} == {
                "EFI": False,
                "System Volume Information": False,
            }


class _CountingStore:
    """Wraps a real ``ObjectStore``, counting ``read()`` calls —
    used by the regression test below to pin down *which* code path ran
    by how many real reads it took, rather than by how long it took."""

    def __init__(self, backing: ObjectStore) -> None:
        self._backing = backing
        self.read_count = 0

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self.read_count += 1
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._backing.listdir(path)


async def test_replayed_ntfs_iterdir_reads_the_file_name_index_attribute_not_a_full_mft_dereference(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """Regression test pinning ``_ntfs_iterdir``'s use of
    ``dereference=False`` + ``IndexEntry.attribute`` to read
    name/is_dir/size directly off NTFS's own ``$I30`` index entries,
    rather than resolving every child to its own full ``MftRecord``
    (``dereference=True``): both give byte-identical (name, is_dir, size)
    results, but a regression to the latter would need one or more real
    reads per entry instead of a small, roughly constant number of index
    reads regardless of entry count — asserted directly below via
    ``_CountingStore.read_count`` rather than via elapsed real time,
    which a replayed, in-memory fixture like this one has no reason to
    take measurably long either way. Such a regression would also issue
    ``ObjectStore`` reads this fixture never recorded, so ``ReplayStore``
    would raise first in practice regardless — the read-count assertion
    is this regression's own direct guard, not reliant on that as a side
    effect."""
    # allow_content=True: DiskFilesystem.open()/list_dir() reads the real
    # NTFS $I30 index bytes off the disk image -- the assertions below are
    # purely structural (read counts, name/is_dir/size), never content
    # meaning, but the bytes themselves are still real.
    backing = await record_target("disk_fs_ntfs_system32_apv1.json.gz", allow_content=True)
    store = _CountingStore(backing)
    layout = await detect_layout(store)

    async with await DedupRepo.open(store, layout) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _APV1_WINDOWS_VM_WORKLOAD_ID)
        version = next(v for v in await versions(repo, vm) if v.meta is not None)

        async with await DeviceProvider.create(repo, version) as provider:
            devices = await provider.children(provider.root())
            objects = await provider.children(devices[0])
            disk_node = next(o for o in objects if o.kind is UnitKind.DISK_IMAGE)
            content = (await provider.unit(disk_node)).open()

            disk_fs = await DiskFilesystem.open(content)
            assert disk_fs is not None
            ntfs_addr = next(addr for addr, label in disk_fs.partitions() if "NTFS" in label)

            reads_before = store.read_count
            entries = await disk_fs.list_dir(ntfs_addr, "/Windows/System32")
            reads_during_list_dir = store.read_count - reads_before
            # 139 real reads for these 4445 entries via the index path;
            # a per-entry MftRecord dereference would need thousands.
            assert reads_during_list_dir < 500, (
                f"expected a small, roughly constant number of $I30 index reads for 4445 entries, "
                f"got {reads_during_list_dir} -- did _ntfs_iterdir regress back to dereference=True?"
            )

            assert len(entries) == 4445
            by_name = {name: (is_dir, size) for name, is_dir, size in entries}
            assert by_name["drivers"] == (True, None)
            assert by_name["config"] == (True, None)
            assert by_name["notepad.exe"] == (False, 211968)
            assert by_name["calc.exe"] == (False, 27648)
            assert by_name["win32k.sys"] == (False, 596992)
            assert by_name["kernel32.dll"] == (False, 770144)


async def test_replayed_vm_disk_filesystem_sibling_lists_real_ext4_boot_partition(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    """``apv-sample-2-encrypted`` needs real ``KeyMaterial`` --
    ``DedupRepo.open()`` takes it as an optional third positional
    arg, matching ``test_units_device.py``'s own encrypted-sample
    pattern."""
    # allow_content=True: listing a real ext4/Btrfs partition's directory
    # entries requires parsing the disk image's own real bytes, and
    # fstab's content below is a deterministic OS-shipped default file,
    # not user content.
    store = await record_target("disk_fs_ext4_btrfs_apv2.json.gz", allow_content=True)
    keys = KeyMaterial.from_key_string(_ENCRYPTED_KEY_STRING)
    layout = await detect_layout(store)

    async with await DedupRepo.open(store, layout, keys) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        vm = next(w for w in all_workloads if w.workload_id == _APV2_ENCRYPTED_FEDORA_40_VM_WORKLOAD_ID)
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

            # Only the top-level subvolume listing is checked here -- proof
            # that a real, dedup-reconstructed Btrfs partition (chunked,
            # decrypted) parses and enumerates subvolumes correctly through
            # the *whole* repository pipeline, which the Docker-built fixture
            # below never exercises (it opens a plain raw file directly).
            # Deeper subvolume-crossing content coverage (entering a
            # subvolume, reading a real file from two different ones) used
            # to live here against this same real Fedora VM disk, but now
            # lives in tests/unit/sdk/test_units_disk_fs.py's own
            # tiny_btrfs_subvols.raw.tar.gz -- a real, Docker-built
            # multi-subvolume Btrfs image this project owns outright, rather
            # than only reachable through this one real sample. Trimmed here
            # accordingly, which is also why this fixture was re-recorded
            # smaller (see this module's own docstring).
            btrfs_partition = next(p for p in partitions if "Btrfs" in p.name)
            top_level_entries = await provider.children(btrfs_partition)
            top_level_names = {e.name for e in top_level_entries}
            assert {"home", "root"} <= top_level_names


__all__: list[str] = []
