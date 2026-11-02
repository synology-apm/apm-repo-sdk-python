"""Restore the disks of a VM, PC or PS version into a new QEMU VM on this host.

A QEMU VM is a folder, ``--dest-dir/<vm>``: its disks, its UEFI variables, and ``<vm>.sh``, the command that
starts it (detached, with a QMP socket ``<vm>.qmp`` beside it). Each disk is exported from the backup into a
sparse raw file, ``<vm>_<n>.img``, which holds only the disk's data; ``--disk-format qcow2`` then converts it
(``qemu-img convert``) into ``<vm>_<n>.qcow2``. The VM is left stopped (``--power-on`` runs ``<vm>.sh``). A
failure removes the folder.

Caveats:
    * The VM gets ``--cpus`` / ``--memory-gib``, a q35 machine with OVMF (without Secure Boot) for a UEFI disk,
      the disks on the q35 AHCI controller (``--disk-bus sata``) or virtio-scsi, and a NIC on ``--netdev``. Other
      guest hardware settings are not read from the backup.
    * ``--disk-format qcow2`` briefly needs room for a disk's data twice: the raw file is removed only once
      converted.
    * Needs ``qemu-system-x86_64`` (and ``qemu-img`` for qcow2), and OVMF for a UEFI disk. ``--insecure`` does not
      apply: QEMU runs on this host.

Requirements (the standard library plus this SDK)::

    pip install synology-apm-repo-sdk

Example::

    python examples/hypervisor-restore/restore_to_qemu.py \\
        "/backups/repo#MySource/web-01/2026-08-07 09:00:08" --dest-dir /var/lib/vms --vm-name web-01-restored
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from apm_restore_common import (
    RestoreError,
    RestorePlan,
    Rollback,
    add_common_args,
    check_common_args,
    describe_plan,
    export_disk,
    open_repository,
    parse_ref,
    plan_restore,
    run_main,
    stage,
)
from synology_apm_repo.sdk import Session
from synology_apm_repo.sdk.export import LocalFileSink

_OVMF = (
    ("/usr/share/OVMF/OVMF_CODE_4M.fd", "/usr/share/OVMF/OVMF_VARS_4M.fd"),  # Debian, Ubuntu
    ("/usr/share/edk2/ovmf/OVMF_CODE.fd", "/usr/share/edk2/ovmf/OVMF_VARS.fd"),  # Fedora, RHEL
    ("/usr/share/OVMF/OVMF_CODE.fd", "/usr/share/OVMF/OVMF_VARS.fd"),
    ("/usr/share/edk2/x64/OVMF_CODE.4m.fd", "/usr/share/edk2/x64/OVMF_VARS.4m.fd"),  # Arch
)
_AHCI_PORTS = 6


@dataclass(frozen=True)
class VmFiles:
    """The files of VM ``name`` in its folder ``folder``; its disks are ``disk_format`` (``raw`` or ``qcow2``)."""

    folder: Path
    name: str
    disk_format: str

    def raw(self, index: int) -> Path:
        return self.folder / f"{self.name}_{index}.img"

    def disk(self, index: int) -> Path:
        return self.raw(index) if self.disk_format == "raw" else self.folder / f"{self.name}_{index}.qcow2"

    @property
    def vars(self) -> Path:
        return self.folder / f"{self.name}_VARS.fd"

    @property
    def qmp(self) -> Path:
        return self.folder / f"{self.name}.qmp"

    @property
    def pidfile(self) -> Path:
        return self.folder / f"{self.name}.pid"

    @property
    def launcher(self) -> Path:
        return self.folder / f"{self.name}.sh"


def find_ovmf(code: str | None, variables: str | None) -> tuple[str, str]:
    """The OVMF code and variables-template files: the ones given, else the first pair installed."""
    if code and variables:
        return code, variables
    for pair in _OVMF:
        if all(Path(p).is_file() for p in pair):
            return pair
    raise RestoreError(
        "cannot find OVMF for a UEFI disk; install it (ovmf / edk2-ovmf) or pass --ovmf-code/--ovmf-vars"
    )


def disk_node(files: VmFiles, index: int) -> dict[str, Any]:
    """The ``-blockdev`` node of disk ``index``."""
    image = {"driver": "file", "filename": str(files.disk(index))}
    return {"driver": files.disk_format, "node-name": f"disk{index}", "file": image}


def qemu_command(args: argparse.Namespace, plan: RestorePlan, files: VmFiles, *, ovmf_code: str | None) -> list[str]:
    """The ``qemu-system`` command of the VM: detached, with QMP on ``files.qmp``, booting the first disk."""
    count = len(plan.disks)
    if args.disk_bus == "sata" and count > _AHCI_PORTS:
        raise RestoreError(f"{count} disks do not fit on the {_AHCI_PORTS} SATA ports of q35; use --disk-bus scsi")
    kvm = args.accel == "kvm"
    argv = [
        args.qemu,
        *("-name", files.name),
        *("-machine", f"q35,accel={args.accel}"),
        *("-cpu", "host" if kvm else "max"),
        *("-smp", str(args.cpus)),
        *("-m", f"{args.memory_gib}G"),
    ]
    if plan.firmware == "uefi":
        argv += [
            *("-drive", f"if=pflash,format=raw,readonly=on,file={ovmf_code}"),
            *("-drive", f"if=pflash,format=raw,file={files.vars}"),
        ]
    if args.disk_bus == "scsi":
        argv += ["-device", "virtio-scsi-pci,id=scsi0"]
    for index in range(count):
        bus = f"bus=ide.{index}" if args.disk_bus == "sata" else "bus=scsi0.0"
        device = "ide-hd" if args.disk_bus == "sata" else "scsi-hd"
        boot = ",bootindex=0" if index == 0 else ""
        argv += ["-blockdev", json.dumps(disk_node(files, index), separators=(",", ":"))]
        argv += ["-device", f"{device},{bus},drive=disk{index},id=dev{index}{boot}"]
    if args.netdev != "none":
        argv += ["-netdev", f"{args.netdev},id=net0", "-device", f"{args.nic_model},netdev=net0"]
    argv += [
        *("-qmp", f"unix:{files.qmp},server=on,wait=off"),
        *("-display", "none"),
        *(["-vnc", args.vnc] if args.vnc else []),
        *("-pidfile", str(files.pidfile)),
        "-daemonize",
    ]
    return argv


def launcher(argv: list[str]) -> str:
    """``<vm>.sh``: starts the VM with ``argv``."""
    return (
        f"#!/bin/sh\n# Starts the VM (detached); stop it with QMP 'quit' on its .qmp socket.\nexec {shlex.join(argv)}\n"
    )


async def run(argv: list[str]) -> None:
    process = await asyncio.create_subprocess_exec(*argv)
    if await process.wait() != 0:
        raise RestoreError(f"{Path(argv[0]).name} exited with status {process.returncode}")


async def make_folder(dest_dir: str, vm_name: str, disk_format: str) -> VmFiles:
    folder = Path(dest_dir) / vm_name
    if folder.exists():
        raise RestoreError(f"{folder} exists already")
    await asyncio.to_thread(folder.mkdir, parents=True)
    return VmFiles(folder, vm_name, disk_format)


async def execute_restore(args: argparse.Namespace, plan: RestorePlan, *, ovmf: tuple[str, str] | None) -> VmFiles:
    """Creates the VM's folder with its disks, its UEFI variables and its launcher; a failure removes it."""
    files = await make_folder(args.dest_dir, args.vm_name, args.disk_format)
    async with Rollback() as rollback:
        rollback.add(asyncio.to_thread, shutil.rmtree, files.folder, True)
        if ovmf is not None:
            await asyncio.to_thread(shutil.copyfile, ovmf[1], files.vars)
        for index, disk in enumerate(plan.disks):
            label = f"disk {index + 1}/{len(plan.disks)}"
            await export_disk(disk, LocalFileSink(files.raw(index), staged=True), label)
            if args.disk_format == "qcow2":
                async with stage(f"{label}: converting to {files.disk(index).name}"):
                    await run([args.qemu_img, "convert", "-O", "qcow2", str(files.raw(index)), str(files.disk(index))])
                    await asyncio.to_thread(files.raw(index).unlink)
        argv = qemu_command(args, plan, files, ovmf_code=ovmf[0] if ovmf else None)
        await asyncio.to_thread(files.launcher.write_text, launcher(argv))
        await asyncio.to_thread(files.launcher.chmod, 0o755)
        rollback.clear()
    return files


async def restore(args: argparse.Namespace, _password: str) -> None:
    ref = parse_ref(args.ref)
    async with Session() as session:
        repo = await open_repository(session, ref, profile=args.profile, key=args.key)
        plan = await plan_restore(repo, ref, firmware=args.firmware)
        describe_plan(plan, requested_firmware=args.firmware)
        if args.dry_run:
            return
        ovmf = find_ovmf(args.ovmf_code, args.ovmf_vars) if plan.firmware == "uefi" else None
        files = await execute_restore(args, plan, ovmf=ovmf)
    print(f"restored {len(plan.disks)} disk(s) into {files.folder}; start the VM with {files.launcher}")
    if args.power_on:
        async with stage(f"starting VM {args.vm_name}"):
            await run([str(files.launcher)])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n", 1)[0] if __doc__ else None,
        epilog="Find refs with: synology-apm-repo-cli ls <path>[#<source>[/<workload>]] --ref",
    )
    add_common_args(parser)
    parser.add_argument("--dest-dir", help="folder the VM's folder is created in")
    parser.add_argument(
        "--disk-format",
        choices=("raw", "qcow2"),
        default="raw",
        help="raw: sparse files holding only the data; qcow2: converted with qemu-img (default: raw)",
    )
    parser.add_argument("--qemu", default="qemu-system-x86_64", help="the QEMU system emulator (default: %(default)s)")
    parser.add_argument(
        "--qemu-img", help="qemu-img, for --disk-format qcow2 (default: the one beside --qemu, else on PATH)"
    )
    parser.add_argument(
        "--accel",
        choices=("kvm", "tcg"),
        default="kvm" if os.path.exists("/dev/kvm") else "tcg",
        help="kvm, or tcg (emulation) where /dev/kvm is missing (default: %(default)s)",
    )
    parser.add_argument(
        "--ovmf-code",
        help="OVMF code file for a UEFI disk, with --ovmf-vars (default: the first pair installed in the usual places)",
    )
    parser.add_argument("--ovmf-vars", help="OVMF variables template, with --ovmf-code")
    parser.add_argument(
        "--netdev", default="user", help="QEMU -netdev backend of the VM's NIC, or 'none' (default: %(default)s)"
    )
    parser.add_argument("--nic-model", default="e1000e", help="the NIC's device model (default: %(default)s)")
    parser.add_argument("--vnc", help="QEMU -vnc display, such as 127.0.0.1:0 (default: no display)")
    parser.add_argument("--power-on", action="store_true", help="start the VM once restored")
    args = parser.parse_args(argv)
    check_common_args(parser, args, ("dest_dir",))
    if bool(args.ovmf_code) != bool(args.ovmf_vars):
        parser.error("--ovmf-code and --ovmf-vars go together")
    if args.qemu_img is None:
        beside = Path(shutil.which(args.qemu) or args.qemu).with_name("qemu-img")
        args.qemu_img = str(beside) if beside.is_file() else "qemu-img"
    return args


def main(argv: list[str] | None = None) -> None:
    run_main(restore, parse_args(argv), "")


if __name__ == "__main__":
    main()
