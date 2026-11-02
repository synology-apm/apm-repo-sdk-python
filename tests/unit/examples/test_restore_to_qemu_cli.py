"""Tests for the command line of examples/hypervisor-restore/restore_to_qemu.py: ``parse_args`` and ``main``."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_REQUIRED = ["--dest-dir", "/vms", "--vm-name", "vm"]


def test_parse_args_defaults(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex.shutil, "which", lambda name: None)

    args = ex.parse_args(["repo#a/b/c", *_REQUIRED])

    assert (args.disk_format, args.qemu, args.qemu_img) == ("raw", "qemu-system-x86_64", "qemu-img")
    assert (args.netdev, args.nic_model, args.vnc, args.power_on) == ("user", "e1000e", None, False)
    assert args.accel in ("kvm", "tcg") and args.ovmf_code is None and args.ovmf_vars is None


def test_parse_args_finds_qemu_img_beside_qemu(ex: ModuleType, tmp_path: Path) -> None:
    qemu, qemu_img = tmp_path / "qemu-system-x86_64", tmp_path / "qemu-img"
    qemu.write_bytes(b"")
    qemu_img.write_bytes(b"")

    args = ex.parse_args(["repo#a/b/c", *_REQUIRED, "--qemu", str(qemu)])

    assert args.qemu_img == str(qemu_img)


def test_parse_args_takes_the_overrides(ex: ModuleType) -> None:
    args = ex.parse_args(
        [
            "repo#a/b/c",
            *_REQUIRED,
            "--disk-format",
            "qcow2",
            "--qemu-img",
            "/opt/qemu-img",
            "--accel",
            "tcg",
            "--ovmf-code",
            "/c.fd",
            "--ovmf-vars",
            "/v.fd",
            "--netdev",
            "none",
            "--vnc",
            "127.0.0.1:1",
            "--power-on",
        ]
    )

    assert (args.disk_format, args.qemu_img, args.accel) == ("qcow2", "/opt/qemu-img", "tcg")
    assert (args.ovmf_code, args.ovmf_vars, args.netdev, args.vnc, args.power_on) == (
        "/c.fd",
        "/v.fd",
        "none",
        "127.0.0.1:1",
        True,
    )


def test_parse_args_needs_a_dest_dir(ex: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", "--vm-name", "vm"])

    assert "required unless --dry-run: --dest-dir" in capsys.readouterr().err


@pytest.mark.parametrize("given", [["--ovmf-code", "/c.fd"], ["--ovmf-vars", "/v.fd"]])
def test_parse_args_takes_the_ovmf_files_only_as_a_pair(
    ex: ModuleType, given: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", *_REQUIRED, *given])

    assert "--ovmf-code and --ovmf-vars go together" in capsys.readouterr().err


def test_main_runs_the_restore_without_a_password(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, str]] = []

    async def fake_restore(args: Any, password: str) -> None:
        seen.append((args.dest_dir, password))

    monkeypatch.setattr(ex, "restore", fake_restore)

    ex.main(["repo#a/b/c", *_REQUIRED])

    assert seen == [("/vms", "")]
