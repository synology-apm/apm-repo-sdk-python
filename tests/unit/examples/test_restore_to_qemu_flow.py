"""Tests for examples/hypervisor-restore/restore_to_qemu.py's VM folder: ``qemu_command``, ``launcher``,
``find_ovmf``, ``execute_restore`` (raw and qcow2, with ``run`` replaced) and ``restore``."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from unit.examples.hypervisor_restore_fakes import GIB, MIB, ExtentContent, FakeRepo, make_plan, patch_session, vm_tree


def _args(dest_dir: Path, **overrides: Any) -> argparse.Namespace:
    values: dict[str, Any] = {
        "dest_dir": str(dest_dir),
        "vm_name": "apm-test-vm",
        "disk_format": "raw",
        "qemu": "qemu-system-x86_64",
        "qemu_img": "/opt/qemu/bin/qemu-img",
        "accel": "kvm",
        "cpus": 2,
        "memory_gib": 4,
        "disk_bus": "sata",
        "netdev": "user",
        "nic_model": "e1000e",
        "vnc": None,
    }
    return argparse.Namespace(**(values | overrides))


def _option_values(argv: list[str], option: str) -> list[str]:
    return [argv[i + 1] for i, arg in enumerate(argv) if arg == option]


# -- qemu_command and launcher ---------------------------------------------------


def test_qemu_command_for_a_uefi_vm_on_sata(ex: ModuleType, tmp_path: Path) -> None:
    files = ex.VmFiles(tmp_path, "vm", "raw")

    argv = ex.qemu_command(_args(tmp_path), make_plan(ex, GIB, GIB), files, ovmf_code="/ovmf/CODE.fd")

    assert argv[:11] == [
        "qemu-system-x86_64", "-name", "vm", "-machine", "q35,accel=kvm", "-cpu", "host", "-smp", "2", "-m", "4G"
    ]  # fmt: skip
    assert _option_values(argv, "-drive") == [
        "if=pflash,format=raw,readonly=on,file=/ovmf/CODE.fd",
        f"if=pflash,format=raw,file={tmp_path / 'vm_VARS.fd'}",
    ]
    assert [json.loads(node) for node in _option_values(argv, "-blockdev")] == [
        {
            "driver": "raw",
            "node-name": f"disk{i}",
            "file": {"driver": "file", "filename": str(tmp_path / f"vm_{i}.img")},
        }
        for i in range(2)
    ]
    assert _option_values(argv, "-device") == [
        "ide-hd,bus=ide.0,drive=disk0,id=dev0,bootindex=0",
        "ide-hd,bus=ide.1,drive=disk1,id=dev1",
        "e1000e,netdev=net0",
    ]
    assert _option_values(argv, "-netdev") == ["user,id=net0"]
    assert _option_values(argv, "-qmp") == [f"unix:{tmp_path / 'vm.qmp'},server=on,wait=off"]
    assert _option_values(argv, "-pidfile") == [str(tmp_path / "vm.pid")]
    assert argv[-1] == "-daemonize" and "-vnc" not in argv


def test_qemu_command_for_a_bios_vm_on_scsi_with_qcow2_and_tcg(ex: ModuleType, tmp_path: Path) -> None:
    files = ex.VmFiles(tmp_path, "vm", "qcow2")
    args = _args(tmp_path, disk_bus="scsi", accel="tcg", netdev="none", vnc="127.0.0.1:0")

    argv = ex.qemu_command(args, make_plan(ex, GIB, firmware="bios"), files, ovmf_code=None)

    assert _option_values(argv, "-cpu") == ["max"] and _option_values(argv, "-drive") == []
    assert _option_values(argv, "-device") == [
        "virtio-scsi-pci,id=scsi0",
        "scsi-hd,bus=scsi0.0,drive=disk0,id=dev0,bootindex=0",
    ]
    (node,) = _option_values(argv, "-blockdev")
    assert json.loads(node)["driver"] == "qcow2" and json.loads(node)["file"]["filename"] == str(
        tmp_path / "vm_0.qcow2"
    )
    assert _option_values(argv, "-netdev") == [] and _option_values(argv, "-vnc") == ["127.0.0.1:0"]


def test_qemu_command_refuses_more_disks_than_ahci_ports(ex: ModuleType, tmp_path: Path) -> None:
    with pytest.raises(ex.RestoreError, match="7 disks do not fit on the 6 SATA ports of q35; use --disk-bus scsi"):
        ex.qemu_command(_args(tmp_path), make_plan(ex, *[GIB] * 7), ex.VmFiles(tmp_path, "vm", "raw"), ovmf_code="c")


def test_launcher_runs_the_command_quoted(ex: ModuleType) -> None:
    script = ex.launcher(["qemu", "-blockdev", '{"a":"b c"}'])

    assert script.startswith("#!/bin/sh\n")
    assert script.splitlines()[-1] == 'exec qemu -blockdev \'{"a":"b c"}\''


def test_find_ovmf_takes_the_given_pair_else_the_first_installed(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    installed = (str(tmp_path / "CODE.fd"), str(tmp_path / "VARS.fd"))
    for path in installed:
        Path(path).write_bytes(b"")
    monkeypatch.setattr(ex, "_OVMF", ((str(tmp_path / "missing"), installed[1]), installed))

    assert ex.find_ovmf("/x/CODE", "/x/VARS") == ("/x/CODE", "/x/VARS")
    assert ex.find_ovmf(None, None) == installed
    monkeypatch.setattr(ex, "_OVMF", ())
    with pytest.raises(ex.RestoreError, match="cannot find OVMF for a UEFI disk"):
        ex.find_ovmf("/x/CODE", None)


# -- execute_restore -------------------------------------------------------------


def _plan(ex: ModuleType, *contents: ExtentContent, firmware: str = "uefi") -> Any:
    plan = make_plan(ex, *(c.size for c in contents), firmware=firmware)
    disks = [type(d)(d.label, SimpleNamespace(content=c), c.size) for d, c in zip(plan.disks, contents, strict=True)]
    return ex.RestorePlan(disks, firmware)


@pytest.fixture
def ovmf(tmp_path: Path) -> tuple[str, str]:
    code, variables = tmp_path / "CODE.fd", tmp_path / "VARS.fd"
    code.write_bytes(b"code")
    variables.write_bytes(b"vars template")
    return str(code), str(variables)


async def test_execute_restore_writes_sparse_raw_disks_and_the_launcher(
    ex: ModuleType, tmp_path: Path, ovmf: tuple[str, str]
) -> None:
    data = ExtentContent(4 * MIB, {MIB: b"boot sector"})
    plan = _plan(ex, data, ExtentContent(2 * MIB, {}))

    files = await ex.execute_restore(_args(tmp_path / "vms"), plan, ovmf=ovmf)

    assert files.folder == tmp_path / "vms" / "apm-test-vm"
    assert sorted(p.name for p in files.folder.iterdir()) == [
        "apm-test-vm.sh",
        "apm-test-vm_0.img",
        "apm-test-vm_1.img",
        "apm-test-vm_VARS.fd",
    ]
    image = files.raw(0).read_bytes()
    assert len(image) == 4 * MIB and image[MIB : MIB + 11] == b"boot sector" and image.count(0) == 4 * MIB - 11
    assert files.raw(1).stat().st_size == 2 * MIB
    assert files.vars.read_bytes() == b"vars template"
    assert os.name == "nt" or files.launcher.stat().st_mode & 0o777 == 0o755
    assert f"file={ovmf[0]}" in files.launcher.read_text()


async def test_execute_restore_converts_to_qcow2_and_drops_the_raw_file(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    commands: list[list[str]] = []

    async def run(argv: list[str]) -> None:
        commands.append(argv)
        Path(argv[-1]).write_bytes(Path(argv[-2]).read_bytes()[:8])

    monkeypatch.setattr(ex, "run", run)
    plan = _plan(ex, ExtentContent(MIB, {0: b"x"}), firmware="bios")

    files = await ex.execute_restore(_args(tmp_path, disk_format="qcow2"), plan, ovmf=None)

    assert commands == [["/opt/qemu/bin/qemu-img", "convert", "-O", "qcow2", str(files.raw(0)), str(files.disk(0))]]
    assert sorted(p.name for p in files.folder.iterdir()) == ["apm-test-vm.sh", "apm-test-vm_0.qcow2"]
    assert "pflash" not in files.launcher.read_text()


async def test_execute_restore_removes_the_folder_when_a_step_fails(
    ex: ModuleType, tmp_path: Path, ovmf: tuple[str, str]
) -> None:
    plan = _plan(ex, ExtentContent(MIB, {0: b"x"}), ExtentContent(MIB, {0: b"y"}, fail_at=0))

    with pytest.raises(RuntimeError, match="synthetic failure"):
        await ex.execute_restore(_args(tmp_path), plan, ovmf=ovmf)

    assert not (tmp_path / "apm-test-vm").exists()


async def test_execute_restore_refuses_an_existing_folder_and_leaves_it(ex: ModuleType, tmp_path: Path) -> None:
    (tmp_path / "apm-test-vm").mkdir()
    (tmp_path / "apm-test-vm" / "mine.txt").write_text("keep")

    with pytest.raises(ex.RestoreError, match="apm-test-vm exists already"):
        await ex.execute_restore(_args(tmp_path), _plan(ex, ExtentContent(MIB, {})), ovmf=None)

    assert (tmp_path / "apm-test-vm" / "mine.txt").read_text() == "keep"


# -- restore ---------------------------------------------------------------------


async def test_restore_runs_the_launcher_with_power_on(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    patch_session(ex, monkeypatch, FakeRepo(*vm_tree()))
    commands: list[list[str]] = []

    async def execute_restore(args: Any, plan: Any, *, ovmf: Any) -> Any:
        assert ovmf is None
        return ex.VmFiles(tmp_path, args.vm_name, args.disk_format)

    async def run(argv: list[str]) -> None:
        commands.append(argv)

    monkeypatch.setattr(ex, "execute_restore", execute_restore)
    monkeypatch.setattr(ex, "run", run)
    args = _args(tmp_path, ref="repo#a/b/c", profile=None, key=None, firmware="bios", dry_run=False, power_on=True)

    await ex.restore(args, "")

    assert commands == [[str(tmp_path / "apm-test-vm.sh")]]
    assert f"restored 1 disk(s) into {tmp_path}" in capsys.readouterr().out


async def test_run_reports_a_failed_command(ex: ModuleType) -> None:
    with pytest.raises(ex.RestoreError, match="exited with status 3"):
        await ex.run([sys.executable, "-c", "raise SystemExit(3)"])
