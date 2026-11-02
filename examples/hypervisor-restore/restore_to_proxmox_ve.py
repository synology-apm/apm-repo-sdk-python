"""Restore the disks of a VM, PC or PS version into a new Proxmox VE VM.

Each disk is exported from the backup as a streamOptimized VMDK (``stream_vmdk.py``) straight into Proxmox VE's
upload API, so only its data is sent, deflated, and nothing is staged on this host. The upload API takes a disk
image only into a storage with the ``import`` content type, so the run adds one of its own, through the API, and
removes it again; no existing storage is changed.

Flow:

1. Resolve the ref and list its disks; detect the firmware from the first one.
2. Add a ``dir`` storage (named after ``--name-prefix``, content ``import``) on ``--node`` over a new directory
   below ``--staging-dir``.
3. Upload each disk into it as ``<storage>:import/<name>.vmdk`` while the export runs.
4. Create the VM with each disk imported from its upload (``import-from``) into ``--target-storage``.
5. Delete the uploads and the storage. The VM is left powered off (``--power-on`` starts it). A failure before
   this step removes what the run created; a failure in it only warns, as the VM is complete.

Caveats:
    * The VM gets ``--cpus`` / ``--memory-gib``, a q35 machine, OVMF (without pre-enrolled keys) for a UEFI disk
      with its EFI vars on ``--efi-storage``, and a virtio NIC only with ``--bridge``. Other guest hardware
      settings are not read from the backup.
    * Proxmox VE's API server refuses a chunked upload (HTTP 501), so each disk is exported twice: once to learn
      its VMDK's size, then uploaded with that ``Content-Length`` (``--upload``).
    * The node keeps the empty directory the storage was over: the API cannot remove a directory.
    * ``--staging-dir`` holds each upload until the VM is created: it needs room for every disk's data,
      compressed.
    * Only ``root@pam`` was tried as the API user.

Requirements (the standard library plus this SDK)::

    pip install synology-apm-repo-sdk

Example::

    PVE_PASSWORD=... python examples/hypervisor-restore/restore_to_proxmox_ve.py \\
        "/backups/repo#MySource/web-01/2026-08-07 09:00:08" --pve-host pve.example.com --node pve1 \\
        --target-storage local-lvm --bridge vmbr0 --vm-name web-01-restored
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import functools
import json
import os
import secrets
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

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
    restore_name,
    run_main,
    stage,
    tls_context,
    wait_until,
    warn,
)
from stream_vmdk import HttpUploadOutput, export_vmdk
from synology_apm_repo.sdk import Session

_PORT = 8006
_TASK_TIMEOUT_SECONDS = 600.0
_SATA_PORTS = 6


def vm_config(args: argparse.Namespace, plan: RestorePlan, volumes: list[str], *, vmid: int) -> dict[str, Any]:
    """The ``POST /nodes/{node}/qemu`` parameters of the new VM, each disk imported from its upload ``volumes``."""
    if args.disk_bus == "sata" and len(volumes) > _SATA_PORTS:
        raise RestoreError(f"{len(volumes)} disks do not fit on the {_SATA_PORTS} SATA ports; use --disk-bus scsi")
    config: dict[str, Any] = {
        "vmid": vmid,
        "name": args.vm_name,
        "cores": args.cpus,
        "memory": args.memory_gib * 1024,
        "cpu": args.cpu_type,
        "ostype": args.ostype,
        "machine": "q35",
        "boot": f"order={args.disk_bus}0",
    }
    if args.disk_bus == "scsi":
        config["scsihw"] = "virtio-scsi-single"
    for index, volume in enumerate(volumes):
        config[f"{args.disk_bus}{index}"] = f"{args.target_storage}:0,import-from={volume}"
    if plan.firmware == "uefi":
        config["bios"] = "ovmf"
        config["efidisk0"] = f"{args.efi_storage or args.target_storage}:1,efitype=4m,pre-enrolled-keys=0"
    if args.bridge:
        config["net0"] = f"virtio,bridge={args.bridge}"
    return config


def multipart_framing(filename: str, *, boundary: str) -> tuple[bytes, bytes]:
    """The bytes before and after the file in the upload API's multipart form (``content=import``)."""
    head = (
        f'--{boundary}\r\nContent-Disposition: form-data; name="content"\r\n\r\nimport\r\n'
        f'--{boundary}\r\nContent-Disposition: form-data; name="filename"; filename="{filename}"\r\n'
        "Content-Type: application/octet-stream\r\n\r\n"
    )
    return head.encode(), f"\r\n--{boundary}--\r\n".encode()


class PveApi:
    """Proxmox VE's REST API over ``urllib``, logged in with a ticket. Every call blocks."""

    def __init__(self, host: str, user: str, password: str, *, context: ssl.SSLContext) -> None:
        self.base = f"https://{host}:{_PORT}/api2/json"
        self.context = context
        login = self._call("POST", "/access/ticket", {"username": user, "password": password}, auth=False)
        self.ticket: str = login["ticket"]
        self.csrf: str = login["CSRFPreventionToken"]

    @property
    def auth_headers(self) -> dict[str, str]:
        return {"Cookie": f"PVEAuthCookie={self.ticket}", "CSRFPreventionToken": self.csrf}

    def _call(self, method: str, path: str, params: dict[str, Any] | None = None, *, auth: bool = True) -> Any:
        url = self.base + path
        body = None
        if params and method in ("GET", "DELETE"):  # PVE refuses a DELETE with a body
            url += "?" + urllib.parse.urlencode(params)
        elif params:
            body = urllib.parse.urlencode(params).encode()
        request = urllib.request.Request(url, data=body, method=method, headers=self.auth_headers if auth else {})
        try:
            with urllib.request.urlopen(request, context=self.context) as response:
                return json.load(response)["data"]
        except urllib.error.HTTPError as exc:
            with exc:
                detail = exc.read().decode(errors="replace").strip()
            raise RestoreError(f"{method} {path} failed: HTTP {exc.code} {exc.reason} {detail}") from exc

    def get(self, path: str, /, **params: Any) -> Any:
        return self._call("GET", path, params)

    def post(self, path: str, /, **params: Any) -> Any:
        return self._call("POST", path, params)

    def delete(self, path: str, /, **params: Any) -> Any:
        return self._call("DELETE", path, params)


class PveTarget:
    """The Proxmox VE calls the restore needs, on one node, over a ``PveApi``."""

    def __init__(self, api: PveApi, node: str) -> None:
        self._api = api
        self.node = node

    async def _run(self, method: str, path: str, /, **params: Any) -> Any:
        return await asyncio.to_thread(getattr(self._api, method), path, **params)

    async def wait_task(self, upid: str, what: str, *, limit: float | None = _TASK_TIMEOUT_SECONDS) -> None:
        async def done() -> bool | None:
            status = await self._run("get", f"/nodes/{self.node}/tasks/{upid}/status")
            if status["status"] != "stopped":
                return None
            if status.get("exitstatus") != "OK":
                raise RestoreError(f"{what} failed: {status.get('exitstatus')}")
            return True

        await wait_until(done, what=what, limit=limit)

    async def check_storages(self, names: set[str]) -> None:
        """Fails unless ``--node`` has every storage of ``names``."""
        present = {item["storage"] for item in await self._run("get", f"/nodes/{self.node}/storage")}
        if missing := sorted(names - present):
            raise RestoreError(f"node {self.node} has no storage {', '.join(missing)}")

    async def next_vmid(self) -> int:
        return int(await self._run("get", "/cluster/nextid"))

    async def add_import_storage(self, storage: str, path: str) -> None:
        await self._run("post", "/storage", storage=storage, type="dir", path=path, content="import", nodes=self.node)

    async def remove_storage(self, storage: str) -> None:
        await self._run("delete", f"/storage/{storage}")

    def upload(self, storage: str, filename: str, *, content_length: int | None) -> HttpUploadOutput:
        """An upload of ``filename`` into ``storage``'s ``import`` content."""
        boundary = f"apm-restore-{secrets.token_hex(12)}"
        prefix, suffix = multipart_framing(filename, boundary=boundary)
        return HttpUploadOutput(
            f"{self._api.base}/nodes/{self.node}/storage/{storage}/upload",
            headers={**self._api.auth_headers, "Content-Type": f"multipart/form-data; boundary={boundary}"},
            content_length=content_length,
            prefix=prefix,
            suffix=suffix,
            ssl_context=self._api.context,
        )

    async def delete_volume(self, storage: str, volid: str) -> None:
        upid = await self._run(
            "delete", f"/nodes/{self.node}/storage/{storage}/content/{urllib.parse.quote(volid, safe='')}"
        )
        if upid:
            await self.wait_task(upid, f"deleting {volid}")

    async def create_vm(self, config: dict[str, Any]) -> str:
        """Starts creating the VM; returns its task. The VM id is this run's once this returns."""
        return str(await self._run("post", f"/nodes/{self.node}/qemu", **config))

    async def start_vm(self, vmid: int) -> None:
        await self.wait_task(await self._run("post", f"/nodes/{self.node}/qemu/{vmid}/status/start"), "starting the VM")

    async def destroy_vm(self, vmid: int) -> None:
        """Destroys the VM this run created, with its disks; one its failed create already removed is left alone."""
        try:
            upid = await self._run("delete", f"/nodes/{self.node}/qemu/{vmid}", purge=1)
        except RestoreError as exc:
            if "does not exist" in str(exc):
                return
            raise
        await self.wait_task(upid, "destroying the VM")

    async def discard_vm(self, upid: str, vmid: int) -> None:
        """Rollback of ``create_vm``: destroys the VM once its create task ``upid`` has ended. The VM is locked
        until then, so a destroy would fail."""
        with contextlib.suppress(RestoreError):
            await self.wait_task(upid, "creating the VM", limit=None)
        await self.destroy_vm(vmid)


def upload_volume(response: bytes, storage: str, filename: str) -> tuple[str, str]:
    """The upload task's id from the upload API's ``response``, and the volume the upload becomes."""
    upid = json.loads(response)["data"]
    if not upid:
        raise RestoreError(f"the upload of {filename} returned no task")
    return upid, f"{storage}:import/{filename}"


async def execute_restore(args: argparse.Namespace, plan: RestorePlan, pve: PveTarget, *, vmid: int) -> None:
    """Adds the staging storage, uploads every disk, creates the VM from them, and removes the uploads and the
    storage. Whatever was created is removed again if a step before that last one fails."""
    storage = restore_name(args.name_prefix, args.vm_name)
    path = f"{args.staging_dir.rstrip('/')}/{storage}"
    await pve.check_storages({args.target_storage, args.efi_storage or args.target_storage})
    async with Rollback() as rollback:
        async with stage(f"adding storage {storage} ({path}) on {pve.node}"):
            await pve.add_import_storage(storage, path)
            rollback.add(pve.remove_storage, storage)
        volumes = []
        for index, disk in enumerate(plan.disks):
            filename = f"{storage}-{index}.vmdk"
            upload = await export_vmdk(
                disk,
                f"disk {index + 1}/{len(plan.disks)}",
                functools.partial(pve.upload, storage, filename),
                two_pass=args.upload == "two-pass",
                file_name=filename,
            )
            upid, volid = upload_volume(upload.response, storage, filename)
            rollback.add(pve.delete_volume, storage, volid)
            await pve.wait_task(upid, f"storing {filename}")
            volumes.append(volid)
        async with stage(f"creating VM {vmid}"):
            upid = await pve.create_vm(vm_config(args, plan, volumes, vmid=vmid))
            rollback.add(pve.discard_vm, upid, vmid)
            await pve.wait_task(upid, "creating the VM (importing its disks)", limit=None)
        rollback.clear()
    try:
        for volid in volumes:
            await pve.delete_volume(storage, volid)
        await pve.remove_storage(storage)
    except RestoreError as exc:
        warn(f"VM {vmid} was restored, but storage {storage} and its uploads were left behind: {exc}")


async def restore(args: argparse.Namespace, password: str) -> None:
    ref = parse_ref(args.ref)
    async with Session() as session:
        repo = await open_repository(session, ref, profile=args.profile, key=args.key)
        plan = await plan_restore(repo, ref, firmware=args.firmware)
        describe_plan(plan, requested_firmware=args.firmware)
        if args.dry_run:
            return
        context = tls_context(insecure=args.insecure)
        api = await asyncio.to_thread(PveApi, args.pve_host, args.pve_user, password, context=context)
        pve = PveTarget(api, args.node)
        vmid = args.vmid or await pve.next_vmid()
        await execute_restore(args, plan, pve, vmid=vmid)
        print(f"restored {len(plan.disks)} disk(s) into VM {vmid} ({args.vm_name}) on {args.target_storage}")
        if args.power_on:
            async with stage(f"starting VM {vmid}"):
                await pve.start_vm(vmid)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n", 1)[0] if __doc__ else None,
        epilog="Find refs with: synology-apm-repo-cli ls <path>[#<source>[/<workload>]] --ref",
    )
    add_common_args(parser)
    parser.add_argument("--pve-host", help="Proxmox VE host (API on port 8006)")
    parser.add_argument(
        "--pve-user", default="root@pam", help="API user; the password comes from PVE_PASSWORD or a prompt"
    )
    parser.add_argument("--node", help="node to restore on")
    parser.add_argument("--target-storage", help="storage the disks end up on")
    parser.add_argument("--efi-storage", help="storage of a UEFI VM's EFI vars disk (default: --target-storage)")
    parser.add_argument(
        "--staging-dir",
        default="/var/tmp",
        help="directory on the node the temporary import storage is created below (default: /var/tmp)",
    )
    parser.add_argument(
        "--name-prefix", default="apm-restore", help="prefix of the temporary storage's name (default: apm-restore)"
    )
    parser.add_argument("--vmid", type=int, help="id of the new VM (default: the cluster's next free id)")
    parser.add_argument("--bridge", help="give the VM a virtio NIC on this bridge")
    parser.add_argument("--ostype", default="l26", help="Proxmox VE guest OS type (default: l26)")
    parser.add_argument("--cpu-type", default="x86-64-v2-AES", help="CPU type (default: x86-64-v2-AES)")
    parser.add_argument("--power-on", action="store_true", help="start the VM once restored")
    parser.add_argument(
        "--upload",
        choices=("stream", "two-pass"),
        default="two-pass",
        help="two-pass: export each disk twice, to send its size first; stream: upload it once, chunked, which "
        "Proxmox VE 9.1 refuses (default: two-pass)",
    )
    args = parser.parse_args(argv)
    check_common_args(parser, args, ("pve_host", "node", "target_storage"))
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_main(restore, args, resolve_password(os.environ, "PVE_PASSWORD", "Proxmox VE", dry_run=args.dry_run))


if __name__ == "__main__":
    main()
