"""Restore the disks of a VM, PC or PS version into a new VMware vSphere VM (ESXi or vCenter).

The VM is imported the way an OVF is deployed: an OVF descriptor built for the backup's disks goes to
``OvfManager.CreateImportSpec`` and ``ResourcePool.ImportVApp``, and each disk is then exported from the backup
as a streamOptimized VMDK (``stream_vmdk.py``) straight into the import's NFC upload URL. Only the disk's data
is sent, deflated; nothing is staged on this host.

Flow:

1. Resolve the ref and list its disks; detect the firmware from the first one.
2. Create the import spec (thin disks on ``--datastore``) and start the import on one host (``--server`` is that
   ESXi host, or a vCenter that manages it; ``--host`` picks one of several).
3. Upload each disk into the lease's device URL while the export runs, keeping the lease alive.
4. Complete the lease: the VM exists, powered off (``--power-on`` starts it). A failure aborts the lease, which
   removes the half-imported VM.

Caveats:
    * The VM gets ``--cpus`` / ``--memory-gib``, EFI (without Secure Boot) or BIOS firmware, the disks on one
      AHCI (``--disk-bus sata``) or PVSCSI controller, and a vmxnet3 NIC only with ``--network``. Other guest
      hardware settings are not read from the backup.
    * This host must reach the ESXi host on HTTPS (443): under vCenter the upload URLs name the ESXi host.

Requirements (not dependencies of this repository)::

    pip install synology-apm-repo-sdk pyvmomi

Example::

    VSPHERE_PASSWORD=... python examples/hypervisor-restore/restore_to_vmware_vsphere.py \\
        "/backups/repo#MySource/web-01/2026-08-07 09:00:08" --server vcenter.example.com \\
        --user administrator@vsphere.local --host esxi01.example.com --datastore datastore1 \\
        --network "VM Network" --vm-name web-01-restored --power-on
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import functools
import os
import urllib.parse
from collections.abc import Callable
from typing import Any
from xml.sax.saxutils import escape, quoteattr

from apm_restore_common import (
    RestoreError,
    RestorePlan,
    Rollback,
    add_common_args,
    check_common_args,
    describe_plan,
    open_repository,
    parse_ref,
    plan_restore,
    resolve_password,
    run_main,
    stage,
    tls_context,
    wait_until,
)
from stream_vmdk import HttpUploadOutput, export_vmdk
from synology_apm_repo.sdk import Session

_LEASE_TIMEOUT_SECONDS = 300.0
_KEEPALIVE_SECONDS = 30.0
_HARDWARE_VERSION = "vmx-13"
"""ESXi 6.5 and later run it."""
_STREAM_VMDK = "http://www.vmware.com/interfaces/specifications/vmdk.html#streamOptimized"
_NETWORK = "net0"


def unit_numbers(count: int, bus: str) -> list[int]:
    """The controller unit of each disk: a SCSI controller keeps unit 7 for itself."""
    if count > (30 if bus == "sata" else 15):
        raise RestoreError(f"{count} disks do not fit on one {bus.upper()} controller")
    return [n for n in range(count + 1) if bus == "sata" or n != 7][:count]


def disk_file(index: int) -> str:
    """The OVF file name of disk ``index``, which ``CreateImportSpec`` reports back as its ``fileItem.path``."""
    return f"disk{index}.vmdk"


def _item(**fields: str) -> str:
    """One ``<Item>`` of the virtual hardware section; ``rasd`` elements must come in alphabetical order."""
    body = "".join(f"<rasd:{name}>{escape(value)}</rasd:{name}>" for name, value in sorted(fields.items()))
    return f"<Item>{body}</Item>"


def ovf_descriptor(args: argparse.Namespace, plan: RestorePlan) -> str:
    """The OVF descriptor of the new VM: its hardware, and one streamOptimized disk per source disk."""
    sizes = [disk.size for disk in plan.disks]
    files = "".join(f'<File ovf:id="file{i}" ovf:href="{disk_file(i)}"/>' for i in range(len(sizes)))
    disks = "".join(
        f'<Disk ovf:diskId="vmdisk{i}" ovf:fileRef="file{i}" ovf:capacity="{size}" '
        f'ovf:capacityAllocationUnits="byte" ovf:format="{_STREAM_VMDK}"/>'
        for i, size in enumerate(sizes)
    )
    if args.disk_bus == "sata":
        controller = _item(
            Address="0",
            ElementName="SATA controller 0",
            InstanceID="3",
            ResourceSubType="vmware.sata.ahci",
            ResourceType="20",
        )
    else:
        controller = _item(
            Address="0",
            ElementName="SCSI controller 0",
            InstanceID="3",
            ResourceSubType="VirtualSCSI",
            ResourceType="6",
        )
    items = [
        _item(
            AllocationUnits="hertz * 10^6",
            ElementName=f"{args.cpus} virtual CPU(s)",
            InstanceID="1",
            ResourceType="3",
            VirtualQuantity=str(args.cpus),
        ),
        _item(
            AllocationUnits="byte * 2^20",
            ElementName=f"{args.memory_gib} GiB of memory",
            InstanceID="2",
            ResourceType="4",
            VirtualQuantity=str(args.memory_gib * 1024),
        ),
        controller,
    ]
    for index, unit in enumerate(unit_numbers(len(sizes), args.disk_bus)):
        items.append(
            _item(
                AddressOnParent=str(unit),
                ElementName=f"Hard disk {index + 1}",
                HostResource=f"ovf:/disk/vmdisk{index}",
                InstanceID=str(10 + index),
                Parent="3",
                ResourceType="17",
            )
        )
    network_section = ""
    if args.network:
        network_section = (
            f'<NetworkSection><Info>The networks</Info><Network ovf:name="{_NETWORK}">'
            "<Description>The VM's network</Description></Network></NetworkSection>"
        )
        items.append(
            _item(
                AutomaticAllocation="true",
                Connection=_NETWORK,
                ElementName="Network adapter 1",
                InstanceID="4",
                ResourceSubType="VmxNet3",
                ResourceType="10",
            )
        )
    firmware = "efi" if plan.firmware == "uefi" else "bios"
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Envelope xmlns="http://schemas.dmtf.org/ovf/envelope/1" xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1" '
        'xmlns:rasd="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ResourceAllocationSettingData" '
        'xmlns:vssd="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_VirtualSystemSettingData" '
        'xmlns:vmw="http://www.vmware.com/schema/ovf">'
        f"<References>{files}</References>"
        f"<DiskSection><Info>The virtual disks</Info>{disks}</DiskSection>"
        f"{network_section}"
        f"<VirtualSystem ovf:id={quoteattr(args.vm_name)}><Info>A virtual machine</Info>"
        f"<Name>{escape(args.vm_name)}</Name>"
        f'<OperatingSystemSection ovf:id="0" vmw:osType={quoteattr(args.guest_id)}><Info>The guest OS</Info>'
        "</OperatingSystemSection>"
        "<VirtualHardwareSection><Info>The virtual hardware</Info>"
        "<System><vssd:ElementName>Virtual Hardware Family</vssd:ElementName><vssd:InstanceID>0</vssd:InstanceID>"
        f"<vssd:VirtualSystemType>{_HARDWARE_VERSION}</vssd:VirtualSystemType></System>"
        f"{''.join(items)}"
        f'<vmw:Config ovf:required="false" vmw:key="firmware" vmw:value="{firmware}"/>'
        "</VirtualHardwareSection></VirtualSystem></Envelope>"
    )


def upload_url(device_url: str, server: str) -> str:
    """A lease's device URL with the ``*`` an ESXi host puts for its own name replaced by ``server``."""
    parts = urllib.parse.urlsplit(device_url)
    if parts.hostname != "*":
        return device_url
    return urllib.parse.urlunsplit(parts._replace(netloc=parts.netloc.replace("*", server, 1)))


class VsphereSdk:
    """The only code that touches pyVmomi: the connection and the ``vim`` types. ``VsphereTarget`` (the logic)
    is tested against a fake of this class; a change here is only verified by a run against a real host."""

    def __init__(self, server: str, user: str, password: str, *, insecure: bool) -> None:
        from pyVim.connect import SmartConnect
        from pyVmomi import vim

        self.vim = vim
        self.si = SmartConnect(host=server, user=user, pwd=password, disableSslCertValidation=insecure)

    @property
    def cookie(self) -> str:
        """The session cookie, which the NFC upload takes as well."""
        # pyVmomi exposes the session cookie only on its stub.
        return str(self.si._stub.cookie).split(";", 1)[0]  # noqa: SLF001

    def disconnect(self) -> None:
        from pyVim.connect import Disconnect

        Disconnect(self.si)


class VsphereTarget:
    """The vSphere calls the restore needs, on one host, over a ``VsphereSdk`` (of the host itself or of a
    vCenter that manages it). Every call blocks; the restore runs them off the event loop."""

    def __init__(self, sdk: VsphereSdk, host_name: str | None) -> None:
        self._sdk = sdk
        vim = sdk.vim
        self._content = sdk.si.RetrieveContent()
        self.api_type = self._content.about.apiType
        view = self._content.viewManager.CreateContainerView(self._content.rootFolder, [vim.HostSystem], True)
        hosts = list(view.view)
        view.Destroy()
        if host_name and self.api_type == "VirtualCenter":
            hosts = [h for h in hosts if h.name == host_name]
        if len(hosts) != 1:
            names = ", ".join(h.name for h in hosts) or "none"
            raise RestoreError(f"pass --host to pick one ESXi host (found: {names})")
        self.host = hosts[0]
        parent = self.host.parent
        while not isinstance(parent, vim.Datacenter):
            parent = parent.parent
        self.datacenter = parent

    def datastore(self, name: str) -> Any:
        found = [d for d in self.host.datastore if d.name == name]
        if not found:
            raise RestoreError(f"host {self.host.name} has no datastore {name!r}")
        return found[0]

    def network(self, name: str) -> Any:
        found = [n for n in self.host.network if n.name == name]
        if not found:
            raise RestoreError(f"host {self.host.name} has no network {name!r}")
        return found[0]

    def start_import(self, descriptor: str, vm_name: str, datastore: Any, network: Any | None) -> tuple[Any, list[Any]]:
        """Starts importing the VM ``descriptor`` describes; returns its lease and the import spec's file items."""
        vim = self._sdk.vim
        mapping = [vim.OvfManager.NetworkMapping(name=_NETWORK, network=network)] if network is not None else []
        params = vim.OvfManager.CreateImportSpecParams(
            entityName=vm_name, diskProvisioning="thin", networkMapping=mapping
        )
        pool = self.host.parent.resourcePool
        result = self._content.ovfManager.CreateImportSpec(descriptor, pool, datastore, params)
        if result.error:
            raise RestoreError(
                "the import spec was refused: " + "; ".join(e.localizedMessage or str(e) for e in result.error)
            )
        lease = pool.ImportVApp(result.importSpec, self.datacenter.vmFolder, self.host)
        return lease, list(result.fileItem or [])

    @staticmethod
    def lease_state(lease: Any) -> str | None:
        """``"ready"`` once the lease's upload URLs exist, ``None`` while it is still initializing."""
        state = str(lease.state)
        if state == "error":
            raise RestoreError(f"the import failed: {lease.error.msg if lease.error else 'unknown error'}")
        return None if state == "initializing" else state

    @staticmethod
    def device_urls(lease: Any) -> dict[str, str]:
        """Each upload URL of a ready lease, by its import key (a file item's ``deviceId``)."""
        return {d.importKey: d.url for d in lease.info.deviceUrl if d.importKey}

    @staticmethod
    def progress(lease: Any, percent: int) -> None:
        lease.HttpNfcLeaseProgress(percent)

    @staticmethod
    def complete(lease: Any) -> Any:
        """Completes the import; returns the new VM."""
        vm = lease.info.entity  # the lease forgets its info once complete
        lease.HttpNfcLeaseComplete()
        return vm

    @staticmethod
    def abort(lease: Any) -> None:
        """Aborts an unfinished import, which removes the VM it was creating."""
        if str(lease.state) in ("initializing", "ready"):
            lease.HttpNfcLeaseAbort()

    @staticmethod
    def power_on(vm: Any) -> Any:
        """Starts powering the VM on; returns the task."""
        return vm.PowerOnVM_Task()

    @staticmethod
    def task_done(task: Any, what: str) -> bool | None:
        """``True`` once ``task`` succeeded, ``None`` while it runs."""
        state = str(task.info.state)
        if state == "error":
            raise RestoreError(f"{what} failed: {task.info.error.msg}")
        return True if state == "success" else None


async def keep_alive(target: VsphereTarget, lease: Any, percent: Callable[[], int]) -> None:
    """Reports the import's progress every ``_KEEPALIVE_SECONDS``: a lease left without one times out."""
    while True:
        await asyncio.sleep(_KEEPALIVE_SECONDS)
        await asyncio.to_thread(target.progress, lease, percent())


async def import_vm(args: argparse.Namespace, plan: RestorePlan, target: VsphereTarget, cookie: str) -> Any:
    """Imports the VM with every disk of ``plan`` uploaded into it; returns the VM. A failure aborts the import."""
    datastore = await asyncio.to_thread(target.datastore, args.datastore)
    network = await asyncio.to_thread(target.network, args.network) if args.network else None
    descriptor = ovf_descriptor(args, plan)
    context = tls_context(insecure=args.insecure)
    headers = {"Cookie": cookie, "Content-Type": "application/x-vnd.vmware-streamVmdk"}
    async with Rollback() as rollback:
        async with stage(f"starting the import of VM {args.vm_name} on {target.host.name}"):
            lease, items = await asyncio.to_thread(target.start_import, descriptor, args.vm_name, datastore, network)
            rollback.add(asyncio.to_thread, target.abort, lease)
            await wait_until(
                lambda: asyncio.to_thread(target.lease_state, lease), what="the import", limit=_LEASE_TIMEOUT_SECONDS
            )
        urls = await asyncio.to_thread(target.device_urls, lease)
        by_file = {item.path: item for item in items}
        done = 0
        alive = asyncio.create_task(keep_alive(target, lease, lambda: 100 * done // len(plan.disks)))
        try:
            for index, disk in enumerate(plan.disks):
                item = by_file.get(disk_file(index))
                if item is None or item.deviceId not in urls:
                    raise RestoreError(f"the import offers no upload URL for disk {index + 1}")
                url = upload_url(urls[item.deviceId], args.server)
                method = "PUT" if item.create else "POST"
                await export_vmdk(
                    disk,
                    f"disk {index + 1}/{len(plan.disks)}",
                    functools.partial(HttpUploadOutput, url, method=method, headers=headers, ssl_context=context),
                    two_pass=args.upload == "two-pass",
                )
                done += 1
        finally:
            alive.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await alive
        vm = await asyncio.to_thread(target.complete, lease)
        rollback.clear()
    return vm


async def restore(args: argparse.Namespace, password: str) -> None:
    ref = parse_ref(args.ref)
    async with Session() as session:
        repo = await open_repository(session, ref, profile=args.profile, key=args.key)
        plan = await plan_restore(repo, ref, firmware=args.firmware)
        describe_plan(plan, requested_firmware=args.firmware)
        if args.dry_run:
            return
        sdk = await asyncio.to_thread(VsphereSdk, args.server, args.user, password, insecure=args.insecure)
        try:
            target = await asyncio.to_thread(VsphereTarget, sdk, args.host)
            vm = await import_vm(args, plan, target, sdk.cookie)
            print(f"restored {len(plan.disks)} disk(s) into VM {args.vm_name} on {args.datastore}")
            if args.power_on:
                async with stage("powering on the VM"):
                    task = await asyncio.to_thread(target.power_on, vm)
                    await wait_until(
                        lambda: asyncio.to_thread(target.task_done, task, "powering on the VM"),
                        what="powering on the VM",
                        limit=_LEASE_TIMEOUT_SECONDS,
                    )
        finally:
            await asyncio.to_thread(sdk.disconnect)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n", 1)[0] if __doc__ else None,
        epilog="Find refs with: synology-apm-repo-cli ls <path>[#<source>[/<workload>]] --ref",
    )
    add_common_args(parser)
    parser.add_argument("--server", help="ESXi host, or a vCenter that manages it")
    parser.add_argument(
        "--user",
        help="user, such as root (ESXi) or administrator@vsphere.local (vCenter); the password comes from "
        "VSPHERE_PASSWORD or a prompt",
    )
    parser.add_argument("--host", help="under vCenter: the ESXi host to restore on, as vCenter names it")
    parser.add_argument("--datastore", help="datastore for the VM and its disks")
    parser.add_argument("--network", help="give the VM a vmxnet3 NIC on this port group")
    parser.add_argument("--guest-id", default="otherGuest64", help="vSphere guest OS id (default: otherGuest64)")
    parser.add_argument("--power-on", action="store_true", help="power the VM on once restored")
    parser.add_argument(
        "--upload",
        choices=("stream", "two-pass"),
        default="stream",
        help="stream: upload each disk once, chunked; two-pass: export it twice to send its size first (default: "
        "stream)",
    )
    args = parser.parse_args(argv)
    check_common_args(parser, args, ("server", "user", "datastore"))
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_main(restore, args, resolve_password(os.environ, "VSPHERE_PASSWORD", "vSphere", dry_run=args.dry_run))


if __name__ == "__main__":
    main()
