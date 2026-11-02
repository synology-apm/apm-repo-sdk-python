"""Restore the disks of a VM, PC or PS version into a new Nutanix AHV VM.

Flow (Nutanix v4 has no random-access write to a VM vdisk, so the bytes go through
Nutanix Volumes over iSCSI):

1. Resolve the ref and list its disks; detect the firmware from the first one.
2. Create a Volume Group (VG) with one same-sized disk per source disk, and allow
   this host's initiator IQN to log in over iSCSI.
3. ``run_export`` each source disk into a ``BlockSink`` bound to the LUN whose number is its Volume Group
   disk's ``index``, after checking that the LUN reports exactly that disk's size.
4. Create the VM with each disk cloned from the matching VG disk (``VolumeDiskReference``),
   wait for the clone to hydrate, then delete the VG (unless ``--keep-volume-group``, or a disk's hydration is
   disabled and the VM still depends on it).

Caveats:
    * Written against ``ntnx-*-py-client`` 4.x and ``cython-iscsi`` 1.0.
    * The VM gets ``--cpus`` / ``--memory-gib``, UEFI (without Secure Boot) or legacy BIOS, its disks on SCSI,
      and no NIC. Other guest hardware settings are not read from the backup.
    * This host must reach ``--dsip`` on iSCSI (3260).

Requirements (not dependencies of this repository)::

    pip install synology-apm-repo-sdk ntnx-vmm-py-client ntnx-volumes-py-client \\
        ntnx-prism-py-client cython-iscsi   # cython-iscsi builds against libiscsi (brew/apt) and needs pkg-config

Example::

    NTNX_PASSWORD=... python examples/hypervisor-restore/restore_to_nutanix_ahv.py \\
        "/backups/repo#MySource/web-01/2026-08-07 09:00:08" --pc-host pc.example.com --pc-user admin \\
        --cluster <cluster-uuid> --container <storage-container-uuid> \\
        --dsip 10.0.0.50 --vm-name web-01-restored
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import functools
import json
import os
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from apm_restore_common import (
    BLOCK_SIZES,
    GIB,
    RestoreError,
    RestorePlan,
    Rollback,
    SourceDisk,
    add_common_args,
    check_common_args,
    describe_plan,
    export_disk,
    open_repository,
    parse_ref,
    plan_restore,
    resolve_password,
    restore_name,
    run_main,
    warn,
)
from synology_apm_repo.sdk import Session
from synology_apm_repo.sdk.export import (
    AbortOutcome,
    ExportTuning,
    RandomAccessExportSink,
    SinkCaps,
    SinkDescriptor,
    WorkerTarget,
)

_ZERO_CHUNK = 1 << 20
_ZERO_BLOCK = bytes(_ZERO_CHUNK)
_POLL_SECONDS = 2.0
_HYDRATION_SETTLE_SECONDS = 10.0
_TASK_TIMEOUT_SECONDS = 300.0
_HYDRATION_TIMEOUT_SECONDS = 300.0
_EXPORT_WORKERS = 4
"""Worker processes per disk export, each writing over its own iSCSI session: one session's writes are
serial, so throughput grows with the sessions until the network is full."""


# --------------------------------------------------------------------------- #
# Block-device sink: a RandomAccessExportSink over any blocking, block-addressed writer.
# --------------------------------------------------------------------------- #


class BlockWriter(Protocol):
    """A blocking, block-addressed destination (one iSCSI LUN)."""

    def geometry(self) -> tuple[int, int]:
        """``(block_size, block_count)``."""
        ...

    def write(self, offset: int, data: bytes) -> None: ...

    def close(self) -> None: ...


def read_capacity16_cdb() -> bytes:
    """READ CAPACITY(16): opcode 0x9E, service action 0x10, allocation length 32."""
    return bytes([0x9E, 0x10]) + bytes(8) + (32).to_bytes(4, "big") + bytes(2)


def write16_cdb(lba: int, blocks: int, fua: int = 0x00) -> bytes:
    """WRITE(16): opcode 0x8A; ``fua`` is byte 1 (0x08 makes the write durable before it completes)."""
    return bytes([0x8A, fua]) + lba.to_bytes(8, "big") + blocks.to_bytes(4, "big") + bytes(2)


def sync_cache16_cdb() -> bytes:
    """SYNCHRONIZE CACHE(16): opcode 0x91, LBA 0 and 0 blocks = the whole LUN."""
    return bytes([0x91, 0]) + bytes(8) + bytes(4) + bytes(2)


def _load_iscsi() -> Any:
    """The ``iscsi`` module (cython-iscsi); a seam so tests can supply a fake."""
    import iscsi

    return iscsi


class LibiscsiWriter:
    """``BlockWriter`` over ``iscsi://<portal>/<target-iqn>/<lun>`` through the ``iscsi`` module (cython-iscsi)."""

    _MAX_TRANSFER = 1 << 20  # the target's Block Limits VPD is not queried
    _FUA = 0x00
    _GOOD = 0

    def __init__(self, url: str, initiator_iqn: str) -> None:
        iscsi = _load_iscsi()
        self._iscsi = iscsi
        self._ctx = iscsi.Context(initiator_iqn)
        parsed = iscsi.URL(self._ctx, url)
        self._lun = parsed.lun
        self._ctx.set_targetname(parsed.target)
        self._ctx.set_session_type(iscsi.iscsi_session_type.ISCSI_SESSION_NORMAL)
        self._ctx.set_header_digest(iscsi.iscsi_header_digest.ISCSI_HEADER_DIGEST_NONE_CRC32C)
        self._ctx.connect(parsed.portal, self._lun)
        reply = bytearray(32)
        self._command(read_capacity16_cdb(), datain=reply)
        self._block_count = int.from_bytes(reply[0:8], "big") + 1
        self._block_size = int.from_bytes(reply[8:12], "big")

    def _command(self, cdb: bytes, *, dataout: bytearray | None = None, datain: bytearray | None = None) -> None:
        direction = self._iscsi.scsi_xfer_dir
        if dataout is not None:
            task = self._iscsi.Task(cdb, direction.SCSI_XFER_WRITE, len(dataout))
        elif datain is not None:
            task = self._iscsi.Task(cdb, direction.SCSI_XFER_READ, len(datain))
        else:
            task = self._iscsi.Task(cdb, direction.SCSI_XFER_NONE, 0)
        self._ctx.command(self._lun, task, dataout or bytearray(), datain or bytearray())
        if task.status != self._GOOD:
            raise OSError(f"SCSI command 0x{cdb[0]:02x} failed with status {task.status}")

    def geometry(self) -> tuple[int, int]:
        return self._block_size, self._block_count

    def write(self, offset: int, data: bytes) -> None:
        step = self._MAX_TRANSFER - self._MAX_TRANSFER % self._block_size
        for start in range(0, len(data), step):
            chunk = data[start : start + step]
            lba, rem = divmod(offset + start, self._block_size)
            if rem or len(chunk) % self._block_size:
                raise ValueError(f"unaligned write at {offset + start} of {len(chunk)} bytes")
            self._command(write16_cdb(lba, len(chunk) // self._block_size, self._FUA), dataout=bytearray(chunk))

    def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self.write(offset, bytes(data))

    def close(self) -> None:
        try:
            self._command(sync_cache16_cdb())
        finally:
            self._ctx.disconnect()


@dataclass(frozen=True)
class LibiscsiDescriptor:
    """``SinkDescriptor``: how an export worker process opens its own iSCSI session. cython-iscsi is synchronous
    and holds the GIL, so the sessions are spread over processes, not threads."""

    url: str
    initiator_iqn: str

    def open_writer(self) -> LibiscsiWriter:
        return LibiscsiWriter(self.url, self.initiator_iqn)


class BlockSink(RandomAccessExportSink):
    """``RandomAccessExportSink`` writing into one ``BlockWriter``, whose blocking calls it serializes off the
    event loop. ``open`` refuses a LUN whose size is not ``capacity``."""

    def __init__(
        self,
        writer_factory: Callable[[], BlockWriter],
        descriptor: SinkDescriptor | None = None,
        *,
        capacity: int | None = None,
    ) -> None:
        self._factory = writer_factory
        self._descriptor = descriptor
        self._capacity = capacity
        self._writer: BlockWriter | None = None
        self._lock = threading.Lock()
        self._ever_written = False

    @property
    def caps(self) -> SinkCaps:
        # A new Volume Group disk reads back zero until written, so skipped ranges stay zero.
        return SinkCaps(supports_sparse=True)

    @property
    def preallocated(self) -> bool:
        return False

    async def open(self, logical_size: int, *, sparse: bool) -> None:
        writer = await asyncio.to_thread(self._factory)
        block_size, block_count = writer.geometry()
        if block_size not in BLOCK_SIZES:
            await asyncio.to_thread(writer.close)
            raise ValueError(f"the LUN's block size is {block_size} B; only {BLOCK_SIZES} are supported")
        if self._capacity is not None and block_size * block_count != self._capacity:
            await asyncio.to_thread(writer.close)
            raise ValueError(
                f"the LUN reports {block_size * block_count} B but the Volume Group disk behind it is "
                f"{self._capacity} B: this is not the LUN of that disk"
            )
        if logical_size % block_size or block_size * block_count < logical_size:
            await asyncio.to_thread(writer.close)
            raise ValueError(
                f"LUN of {block_count} x {block_size} B cannot hold a {logical_size} B disk "
                f"(size must be a multiple of the block size)"
            )
        self._writer = writer

    def _locked(self, write: Callable[..., None], *args: Any) -> None:
        with self._lock:
            write(*args)

    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        writer = self._begin_write()
        await asyncio.to_thread(self._locked, writer.write, offset, bytes(data))

    async def write_zero(self, offset: int, length: int) -> None:
        for start in range(0, length, _ZERO_CHUNK):
            await self.write_at(offset + start, _ZERO_BLOCK[: min(_ZERO_CHUNK, length - start)])

    def _begin_write(self) -> BlockWriter:
        """The open writer, with the sink marked as written to."""
        if self._writer is None:
            raise RuntimeError("sink is not open")
        self._ever_written = True
        return self._writer

    def worker_target(self) -> WorkerTarget | None:
        """Lets the export's worker processes write straight to the LUN, each over its own session;
        ``None`` keeps every write in this process."""
        return WorkerTarget(self._descriptor) if self._descriptor is not None else None

    def note_worker_write(self) -> None:
        self._ever_written = True

    async def commit(self) -> None:
        await self._close()

    async def abort(self) -> AbortOutcome:
        await self._close()
        return AbortOutcome(kept=self._ever_written, ever_written=self._ever_written)

    async def _close(self) -> None:
        writer, self._writer = self._writer, None
        if writer is not None:
            await asyncio.to_thread(writer.close)


# --------------------------------------------------------------------------- #
# Nutanix side: Volume Group + VM, synchronous SDK calls moved off the loop.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class VolumeGroupDisk:
    """A Volume Group disk as Prism Central reports it. ``index`` is its LUN number on the group's iSCSI target,
    which Prism Central does not document; ``BlockSink`` therefore checks the LUN's size."""

    ext_id: str
    index: int
    size_bytes: int


@dataclass(frozen=True)
class VolumeGroupInfo:
    ext_id: str
    target_name: str
    disks: tuple[VolumeGroupDisk, ...]  # in source-disk order, each created by its own task

    @property
    def disk_ext_ids(self) -> tuple[str, ...]:
        return tuple(disk.ext_id for disk in self.disks)


# ``rel`` of a task's entity reference is ``namespace:module[:submodule]:entityType``.
_VM_SUFFIX = ":vm"
_VG_SUFFIX = ":volume-group"
_VG_DISK_SUFFIX = ":volume-group:disk"  # a VM disk ends with ":vm:disk"


def _status_name(status: object) -> str:
    return str(getattr(status, "name", status)).rsplit(".", 1)[-1]


class NutanixSdk:
    """The only code that touches the ``ntnx_*`` packages: the API objects, model construction and the
    throwaway clients used for deletes. Kept thin so ``AhvTarget`` (the logic) is tested against a fake of this
    class; a change here is only verified by a run against a real Prism Central."""

    def __init__(self, host: str, user: str, password: str, *, verify_tls: bool) -> None:
        import ntnx_prism_py_client as prism
        import ntnx_vmm_py_client as vmm
        import ntnx_volumes_py_client as volumes

        self._modules = {"volumes": volumes, "vmm": vmm, "prism": prism}
        self._connection = (host, user, password, verify_tls)
        self.volume_groups = volumes.api.VolumeGroupsApi(api_client=self._client(volumes))
        self.vms = vmm.api.VmApi(api_client=self._client(vmm))
        self.tasks = prism.api.TasksApi(api_client=self._client(prism))

    def _client(self, module: Any) -> Any:
        host, user, password, verify_tls = self._connection
        config = module.Configuration()
        config.host, config.port = host, 9440
        config.username, config.password, config.verify_ssl = user, password, verify_tls
        return module.ApiClient(configuration=config)

    def api_error(self, exc: BaseException) -> str | None:
        """What Prism Central said when it refused a call with ``exc``; ``None`` if ``exc`` is not such a refusal."""
        if not any(isinstance(exc, module.rest.ApiException) for module in self._modules.values()):
            return None
        refusal: Any = exc
        try:
            detail = "; ".join(error["message"] for error in json.loads(refusal.body)["data"]["error"])
        except (TypeError, ValueError, KeyError):
            detail = ""
        return f"HTTP {refusal.status} {refusal.reason}: {detail}".rstrip(": ")

    def scoped(self, kind: str) -> tuple[Any, Any]:
        """A throwaway ``(client, api)`` for ``"volume_group"`` or ``"vm"``, so an ``If-Match`` header set on it
        never leaks into other calls."""
        module, api_name = (
            (self._modules["volumes"], "VolumeGroupsApi") if kind == "volume_group" else (self._modules["vmm"], "VmApi")
        )
        client = self._client(module)
        return client, getattr(module.api, api_name)(api_client=client)

    def volume_group(self, name: str, cluster: str) -> Any:
        from ntnx_volumes_py_client.models.volumes.v4.config.Protocol import Protocol
        from ntnx_volumes_py_client.models.volumes.v4.config.VolumeGroup import VolumeGroup

        return VolumeGroup(name=name, cluster_reference=cluster, protocol=Protocol.ISCSI)

    def volume_disk(self, index: int, size_bytes: int, container: str) -> Any:
        from ntnx_volumes_py_client.models.common.v1.config.EntityReference import EntityReference
        from ntnx_volumes_py_client.models.common.v1.config.EntityType import EntityType
        from ntnx_volumes_py_client.models.volumes.v4.config.VolumeDisk import VolumeDisk

        # The API requires an (empty-disk) data source: the storage container to allocate from.
        container_ref = EntityReference(ext_id=container, entity_type=EntityType.STORAGE_CONTAINER)
        return VolumeDisk(index=index, disk_size_bytes=size_bytes, disk_data_source_reference=container_ref)

    def iscsi_client(self, iqn: str) -> Any:
        from ntnx_volumes_py_client.models.volumes.v4.config.IscsiClient import IscsiClient

        # Exactly one of initiator name / client UUID / network id may be given.
        return IscsiClient(iscsi_initiator_name=iqn)

    def iscsi_attachment(self, ext_id: str) -> Any:
        from ntnx_volumes_py_client.models.volumes.v4.config.IscsiClientAttachment import IscsiClientAttachment

        return IscsiClientAttachment(ext_id=ext_id)

    def vm(
        self,
        name: str,
        cluster: str,
        container: str,
        volume_group: VolumeGroupInfo,
        sizes: list[int],
        *,
        cpus: int,
        memory_bytes: int,
        uefi: bool,
    ) -> Any:
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.ClusterReference import ClusterReference
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.DataSource import DataSource
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.Disk import Disk
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.DiskAddress import DiskAddress
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.DiskBusType import DiskBusType
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.UefiBoot import UefiBoot
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.Vm import Vm
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.VmDisk import VmDisk
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.VmDiskContainerReference import VmDiskContainerReference
        from ntnx_vmm_py_client.models.vmm.v4.ahv.config.VolumeDiskReference import VolumeDiskReference

        disks = [
            Disk(
                disk_address=DiskAddress(bus_type=DiskBusType.SCSI, index=index),
                backing_info=VmDisk(
                    disk_size_bytes=size,
                    storage_container=VmDiskContainerReference(ext_id=container),
                    data_source=DataSource(
                        reference=VolumeDiskReference(disk_ext_id=disk_id, volume_group_ext_id=volume_group.ext_id)
                    ),
                ),
            )
            for index, (disk_id, size) in enumerate(zip(volume_group.disk_ext_ids, sizes, strict=True))
        ]
        return Vm(
            name=name,
            num_sockets=cpus,
            num_cores_per_socket=1,
            memory_size_bytes=memory_bytes,
            cluster=ClusterReference(ext_id=cluster),
            disks=disks,
            boot_config=UefiBoot(is_secure_boot_enabled=False) if uefi else None,
        )


class AhvTarget:
    """The Prism Central operations the restore needs, over a ``NutanixSdk``.

    Every object is addressed by id: a new object's id comes from its create task's entity references, and
    only ids created by this instance are ever deleted."""

    def __init__(
        self,
        sdk: NutanixSdk,
        *,
        poll_seconds: float = _POLL_SECONDS,
        settle_seconds: float = _HYDRATION_SETTLE_SECONDS,
        task_timeout_seconds: float = _TASK_TIMEOUT_SECONDS,
        hydration_timeout_seconds: float = _HYDRATION_TIMEOUT_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
    ) -> None:
        self._sdk = sdk
        self._poll_seconds = poll_seconds
        self._settle_seconds = settle_seconds
        self._task_timeout = task_timeout_seconds
        self._hydration_timeout = hydration_timeout_seconds
        self._clock = clock
        self._sleep = sleep
        self._created: set[str] = set()

    async def _call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """``fn(*args, **kwargs)`` off the event loop; a call Prism Central refuses raises ``RestoreError``."""
        try:
            return await asyncio.to_thread(functools.partial(fn, *args, **kwargs))
        except Exception as exc:
            if (message := self._sdk.api_error(exc)) is None:
                raise
            raise RestoreError(f"Prism Central refused {getattr(fn, '__name__', 'a call')}: {message}") from exc

    async def _finished(self, task_id: str) -> Any:
        """Polls ``task_id`` until it reaches a final state and returns it.

        Raises:
            RestoreError: The task was still running after the task timeout.
        """
        deadline = self._clock() + self._task_timeout
        while True:
            task = (await self._call(self._sdk.tasks.get_task_by_id, task_id)).data
            status = _status_name(task.status)
            if status in ("SUCCEEDED", "FAILED", "CANCELED"):
                return task
            if self._clock() >= deadline:
                raise RestoreError(f"Nutanix task {task_id} still {status} after {self._task_timeout:g} s")
            await self._sleep(self._poll_seconds)

    async def _wait(self, response: Any) -> Any:
        """Waits for the task a mutating call returned and returns the finished task. A task that outlasts the
        timeout is asked to cancel; its rollback then waits for it to end before removing what it made.

        Raises:
            RestoreError: The task failed, or did not finish within the task timeout.
        """
        task_id = response.data.ext_id
        try:
            task = await self._finished(task_id)
        except RestoreError:
            with contextlib.suppress(Exception):
                await self._call(self._sdk.tasks.cancel_task, task_id)
            raise
        status = _status_name(task.status)
        if status != "SUCCEEDED":
            raise RestoreError(f"Nutanix task {task_id} {status}: {task.error_messages or task.legacy_error_message}")
        return task

    @staticmethod
    def _new_entities(task: Any, rel_suffix: str) -> list[Any]:
        """The entities a finished task lists whose ``rel`` ends with ``rel_suffix``, each id once. A task lists
        every object it touched, of several types: a create-VM task lists the VM and its new disks, a
        create-disk task the disk and its Volume Group."""
        wanted = [e for e in task.entities_affected or [] if str(e.rel).endswith(rel_suffix)]
        unique = {str(e.ext_id): e for e in reversed(wanted)}
        return list(reversed(unique.values()))

    async def _created_id(
        self,
        task: Any,
        what: str,
        rel_suffix: str,
    ) -> str:
        """The id of the one ``what`` a finished create task made: the task's entities whose ``rel`` ends with
        ``rel_suffix``. Anything but exactly one id raises."""
        ids = [str(e.ext_id) for e in self._new_entities(task, rel_suffix)]
        if len(ids) != 1:
            raise RestoreError(
                f"task {task.ext_id} finished without exactly one reference to the {what} it created "
                f"(found {len(ids)} ending with {rel_suffix!r})"
            )
        self._created.add(ids[0])
        return ids[0]

    async def _create(
        self,
        response: Any,
        what: str,
        rel_suffix: str,
        *,
        rollback: Rollback | None = None,
        delete: Callable[[str], Awaitable[None]] | None = None,
    ) -> str:
        """The id of the ``what`` that the create call behind ``response`` made. With ``rollback`` and ``delete``,
        removing it is registered before waiting, so a failure anywhere after the call removes it again."""
        if rollback is not None and delete is not None:
            rollback.add(self._discard, response.data.ext_id, what, rel_suffix, delete)
        return await self._created_id(await self._wait(response), what, rel_suffix)

    async def _discard(
        self,
        task_id: str,
        what: str,
        rel_suffix: str,
        delete: Callable[[str], Awaitable[None]],
    ) -> None:
        """Rollback of a create call: once task ``task_id`` has ended, deletes what it made; a task that lists no
        such entity made nothing."""
        task = await self._finished(task_id)
        if self._new_entities(task, rel_suffix):
            await delete(await self._created_id(task, what, rel_suffix))

    async def create_volume_group(self, name: str, cluster: str, container: str, sizes: list[int]) -> VolumeGroupInfo:
        vgs = self._sdk.volume_groups
        async with Rollback() as rollback:
            response = await self._call(vgs.create_volume_group, self._sdk.volume_group(name, cluster))
            group_id = await self._create(
                response, "Volume Group", _VG_SUFFIX, rollback=rollback, delete=self.delete_volume_group
            )
            disks = []
            for index, size in enumerate(sizes):
                response = await self._call(
                    vgs.create_volume_disk, group_id, self._sdk.volume_disk(index, size, container)
                )
                disk_id = await self._create(response, "Volume Group disk", _VG_DISK_SUFFIX)
                reported = (await self._call(vgs.get_volume_disk_by_id, group_id, disk_id)).data
                if reported.index is None or reported.disk_size_bytes is None:
                    raise RestoreError(f"Volume Group disk {disk_id} reports no index or size")
                disks.append(VolumeGroupDisk(disk_id, int(reported.index), int(reported.disk_size_bytes)))
            group = (await self._call(vgs.get_volume_group_by_id, group_id)).data
            rollback.clear()
            return VolumeGroupInfo(group_id, group.target_name, tuple(disks))

    async def allow_initiator(self, volume_group_id: str, iqn: str) -> None:
        body = self._sdk.iscsi_client(iqn)
        await self._wait(await self._call(self._sdk.volume_groups.attach_iscsi_client, volume_group_id, body))

    async def create_vm(
        self,
        name: str,
        cluster: str,
        container: str,
        group: VolumeGroupInfo,
        sizes: list[int],
        *,
        cpus: int,
        memory_bytes: int,
        uefi: bool,
    ) -> str:
        """Creates the VM and returns its id."""
        body = self._sdk.vm(name, cluster, container, group, sizes, cpus=cpus, memory_bytes=memory_bytes, uefi=uefi)
        async with Rollback() as rollback:
            response = await self._call(self._sdk.vms.create_vm, body)
            vm_id = await self._create(response, "VM", _VM_SUFFIX, rollback=rollback, delete=self.delete_vm)
            rollback.clear()
            return vm_id

    async def wait_hydrated(self, vm_id: str) -> bool:
        """Waits until no disk is hydrating from the VG; ``False`` if a disk has hydration disabled.

        A clone from a Volume Group disk may report no hydration info at all, so "nothing in progress" must
        hold for a settle period before it counts, in case a state shows up late.

        Raises:
            RestoreError: A disk was still hydrating after the hydration timeout.
        """
        deadline = self._clock() + self._hydration_timeout
        quiet_since: float | None = None
        while True:
            disks = (await self._call(self._sdk.vms.list_disks_by_vm_id, vm_id)).data
            states = [
                _status_name(info.disk_hydration_status)
                for disk in disks
                if (info := getattr(disk.backing_info, "vm_disk_hydration_info", None)) is not None
            ]
            if "FAILED" in states:
                raise RestoreError("a restored disk failed to hydrate from the Volume Group")
            if "DISABLED" in states:
                return False
            now = self._clock()
            if "IN_PROGRESS" in states:
                quiet_since = None
            elif quiet_since is None:
                quiet_since = now
            elif now - quiet_since >= self._settle_seconds:
                return True
            if now >= deadline:
                raise RestoreError(f"disks of VM {vm_id} still hydrating after {self._hydration_timeout:g} s")
            await self._sleep(self._poll_seconds)

    def _require_created(self, ext_id: str) -> None:
        if ext_id not in self._created:
            raise RestoreError(f"refusing to delete {ext_id}: it was not created by this run")

    async def _delete(self, kind: str, get_name: str, delete_name: str, ext_id: str) -> None:
        """Deletes ``ext_id`` with its ETag as ``If-Match``."""
        client, api = self._sdk.scoped(kind)
        current = await self._call(getattr(api, get_name), ext_id)
        client.add_default_header(header_name="If-Match", header_value=client.get_etag(current))
        await self._wait(await self._call(getattr(api, delete_name), ext_id))

    async def delete_volume_group(self, ext_id: str) -> None:
        # Checked before anything else: detaching the clients of a group this run did not create would
        # already be a change to it.
        self._require_created(ext_id)
        vgs = self._sdk.volume_groups
        # A Volume Group with an iSCSI client attached cannot be deleted.
        attachments = (await self._call(vgs.list_external_iscsi_attachments_by_volume_group_id, ext_id)).data
        for attachment in attachments or []:
            body = self._sdk.iscsi_attachment(attachment.ext_id)
            await self._wait(await self._call(vgs.detach_iscsi_client, ext_id, body))
        await self._delete("volume_group", "get_volume_group_by_id", "delete_volume_group_by_id", ext_id)

    async def delete_vm(self, ext_id: str) -> None:
        self._require_created(ext_id)
        await self._delete("vm", "get_vm_by_id", "delete_vm_by_id", ext_id)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def lun_url(dsip: str, iqn_prefix: str, target_name: str, lun: int) -> str:
    return f"iscsi://{dsip}:3260/{iqn_prefix}{target_name}/{lun}"


@dataclass(frozen=True)
class RestoreSettings:
    """What ``execute_restore`` needs from the command line."""

    vm_name: str
    cluster: str
    container: str
    dsip: str
    iqn_prefix: str
    initiator_iqn: str
    cpus: int
    memory_gib: int
    keep_volume_group: bool

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> RestoreSettings:
        return cls(
            vm_name=args.vm_name,
            cluster=args.cluster,
            container=args.container,
            dsip=args.dsip,
            iqn_prefix=args.iqn_prefix,
            initiator_iqn=args.initiator_iqn,
            cpus=args.cpus,
            memory_gib=args.memory_gib,
            keep_volume_group=args.keep_volume_group,
        )


class RestoreTarget(Protocol):
    """What ``execute_restore`` asks of the destination; ``AhvTarget`` is the real one."""

    async def create_volume_group(
        self, name: str, cluster: str, container: str, sizes: list[int]
    ) -> VolumeGroupInfo: ...

    async def allow_initiator(self, volume_group_id: str, iqn: str) -> None: ...

    async def create_vm(
        self,
        name: str,
        cluster: str,
        container: str,
        group: VolumeGroupInfo,
        sizes: list[int],
        *,
        cpus: int,
        memory_bytes: int,
        uefi: bool,
    ) -> str: ...

    async def wait_hydrated(self, vm_id: str) -> bool: ...

    async def delete_volume_group(self, ext_id: str) -> None: ...

    async def delete_vm(self, ext_id: str) -> None: ...


ExportDisk = Callable[[SourceDisk, str, int, int, int], Awaitable[None]]
"""Exports one source disk to the LUN at the given URL: ``(disk, url, index, total, capacity)``, where
``capacity`` is the size the LUN must report."""


async def export_to_lun(
    disk: SourceDisk, url: str, index: int, total: int, capacity: int, *, initiator_iqn: str
) -> None:
    """The real ``ExportDisk``: ``run_export`` into a ``BlockSink`` bound to the LUN at ``url``, with progress."""
    sink = BlockSink(
        functools.partial(LibiscsiWriter, url, initiator_iqn),
        LibiscsiDescriptor(url, initiator_iqn),
        capacity=capacity,
    )
    await export_disk(disk, sink, f"disk {index + 1}/{total}", tuning=ExportTuning(workers=_EXPORT_WORKERS))


async def execute_restore(
    plan: RestorePlan, settings: RestoreSettings, target: RestoreTarget, export_disk: ExportDisk
) -> str:
    """Creates the VG, exports every disk into its LUN, creates the VM from the VG's disks and drops the VG.
    Whatever was created is removed again if any step fails, except that a final VG delete failing only
    warns (the VM is complete). Returns the new VM's id."""
    sizes = [disk.size for disk in plan.disks]
    async with Rollback() as rollback:
        group = await target.create_volume_group(
            restore_name("apm-restore", settings.vm_name), settings.cluster, settings.container, sizes
        )
        rollback.add(target.delete_volume_group, group.ext_id)
        await target.allow_initiator(group.ext_id, settings.initiator_iqn)

        for position, (disk, group_disk) in enumerate(zip(plan.disks, group.disks, strict=True)):
            url = lun_url(settings.dsip, settings.iqn_prefix, group.target_name, group_disk.index)
            await export_disk(disk, url, position, len(plan.disks), group_disk.size_bytes)

        vm_id = await target.create_vm(
            settings.vm_name,
            settings.cluster,
            settings.container,
            group,
            sizes,
            cpus=settings.cpus,
            memory_bytes=settings.memory_gib * GIB,
            uefi=plan.firmware == "uefi",
        )
        rollback.add(target.delete_vm, vm_id)
        print(f"created VM {settings.vm_name} ({vm_id}); waiting for disks to hydrate")
        detachable = await target.wait_hydrated(vm_id)

        # Success: keep the VM; drop the VG only once the VM no longer depends on it.
        rollback.clear()
        if detachable and not settings.keep_volume_group:
            try:
                await target.delete_volume_group(group.ext_id)
            except Exception as exc:  # noqa: BLE001
                warn(
                    f"VM {vm_id} was restored, but Volume Group {group.ext_id} could not be deleted "
                    f"and was left behind: {exc}"
                )
        else:
            print(f"kept Volume Group {group.ext_id} (the VM's disks still depend on it or --keep-volume-group)")
    return vm_id


async def restore(args: argparse.Namespace, password: str) -> None:
    ref = parse_ref(args.ref)
    async with Session() as session:
        repo = await open_repository(session, ref, profile=args.profile, key=args.key)
        plan = await plan_restore(repo, ref, firmware=args.firmware)
        describe_plan(plan, requested_firmware=args.firmware)
        if args.dry_run:
            return
        sdk = NutanixSdk(args.pc_host, args.pc_user, password, verify_tls=not args.insecure)
        target = AhvTarget(sdk)
        settings = RestoreSettings.from_args(args)
        export_disk = functools.partial(export_to_lun, initiator_iqn=settings.initiator_iqn)
        vm_id = await execute_restore(plan, settings, target, export_disk)
        print(f"restored {len(plan.disks)} disk(s) into VM {vm_id}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n", 1)[0] if __doc__ else None,
        epilog="Find refs with: synology-apm-repo-cli ls <path>[#<source>[/<workload>]] --ref",
    )
    add_common_args(parser, disk_bus=False)
    parser.add_argument("--pc-host", help="Prism Central host")
    parser.add_argument("--pc-user", help="Prism Central user; the password comes from NTNX_PASSWORD or a prompt")
    parser.add_argument("--cluster", help="target cluster ext id")
    parser.add_argument("--container", help="storage container ext id for the Volume Group and VM disks")
    parser.add_argument("--dsip", help="cluster data-services IP (iSCSI portal)")
    parser.add_argument(
        "--initiator-iqn", default="iqn.2026-10.org.apm-repo-sdk:restore", help="this host's initiator IQN"
    )
    parser.add_argument("--iqn-prefix", default="iqn.2010-06.com.nutanix:", help="prefix before the VG target name")
    parser.add_argument("--keep-volume-group", action="store_true", help="do not delete the temporary Volume Group")
    args = parser.parse_args(argv)
    check_common_args(parser, args, ("pc_host", "pc_user", "cluster", "container", "dsip"))
    if not args.dry_run and ("'" in args.vm_name or '"' in args.vm_name):
        # Checked here: Prism Central refuses such a name only when the VM is created, after every disk is written.
        parser.error("--vm-name cannot contain quotes (Prism Central rejects such VM names)")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_main(restore, args, resolve_password(os.environ, "NTNX_PASSWORD", "Prism Central", dry_run=args.dry_run))


if __name__ == "__main__":
    main()
