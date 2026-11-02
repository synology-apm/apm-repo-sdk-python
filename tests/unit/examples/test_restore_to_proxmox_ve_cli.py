"""Tests for the command line of examples/hypervisor-restore/restore_to_proxmox_ve.py: ``parse_args``, ``main``
and ``restore``'s wiring of the VM id and ``--power-on``."""

from __future__ import annotations

from types import ModuleType
from typing import Any

import pytest

from support.fakes import unchecked_fake
from unit.examples.hypervisor_restore_fakes import FakeRepo, patch_session, vm_tree

_REQUIRED = ["--pve-host", "pve", "--node", "pve1", "--target-storage", "local-lvm", "--vm-name", "vm"]


def test_parse_args_defaults(ex: ModuleType) -> None:
    args = ex.parse_args(["repo#a/b/c", *_REQUIRED])

    assert (args.pve_user, args.upload, args.staging_dir, args.name_prefix) == (
        "root@pam",
        "two-pass",
        "/var/tmp",
        "apm-restore",
    )
    assert (args.ostype, args.cpu_type) == ("l26", "x86-64-v2-AES")
    assert args.vmid is None and args.bridge is None and args.efi_storage is None and args.power_on is False


def test_parse_args_takes_the_overrides(ex: ModuleType) -> None:
    args = ex.parse_args(
        [
            "repo#a/b/c",
            *_REQUIRED,
            "--vmid",
            "130",
            "--bridge",
            "vmbr1",
            "--efi-storage",
            "nvme",
            "--staging-dir",
            "/srv/tmp",
            "--upload",
            "stream",
            "--power-on",
        ]
    )

    assert (args.vmid, args.bridge, args.efi_storage, args.staging_dir) == (130, "vmbr1", "nvme", "/srv/tmp")
    assert (args.upload, args.power_on) == ("stream", True)


def test_parse_args_names_the_missing_options(ex: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", "--vm-name", "vm"])

    assert "required unless --dry-run: --pve-host, --node, --target-storage" in capsys.readouterr().err


def test_main_takes_the_password_from_pve_password(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    async def fake_restore(args: Any, password: str) -> None:
        seen.append(password)

    monkeypatch.setattr(ex, "restore", fake_restore)
    monkeypatch.setenv("PVE_PASSWORD", "from-env")

    ex.main(["repo#a/b/c", *_REQUIRED])

    assert seen == ["from-env"]


@unchecked_fake("examples/hypervisor-restore/restore_to_proxmox_ve.py's PveTarget, loaded at test time")
class _Target:
    """Records what ``restore`` asks of ``PveTarget``."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    async def next_vmid(self) -> int:
        self.calls.append(("next_vmid",))
        return 105

    async def start_vm(self, vmid: int) -> None:
        self.calls.append(("start", vmid))


@pytest.mark.parametrize(
    ("extra", "vmid", "calls"), [([], 105, [("next_vmid",)]), (["--vmid", "130", "--power-on"], 130, [("start", 130)])]
)
async def test_restore_picks_the_vm_id_and_starts_the_vm(
    ex: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    extra: list[str],
    vmid: int,
    calls: list[tuple[Any, ...]],
) -> None:
    patch_session(ex, monkeypatch, FakeRepo(*vm_tree()))
    target = _Target()
    restored: list[int] = []
    monkeypatch.setattr(ex, "PveApi", lambda host, user, password, *, context: (host, user, password))
    monkeypatch.setattr(ex, "PveTarget", lambda api, node: target)

    async def execute_restore(args: Any, plan: Any, pve: Any, *, vmid: int) -> None:
        restored.append(vmid)

    monkeypatch.setattr(ex, "execute_restore", execute_restore)

    await ex.restore(ex.parse_args(["repo#a/b/c", *_REQUIRED, "--firmware", "bios", *extra]), "pw")

    assert restored == [vmid] and target.calls == calls
    assert f"restored 1 disk(s) into VM {vmid} (vm) on local-lvm" in capsys.readouterr().out
