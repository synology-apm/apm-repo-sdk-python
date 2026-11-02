"""Tests for the command line of examples/hypervisor-restore/restore_to_vmware_vsphere.py: ``parse_args`` and
``main``."""

from __future__ import annotations

from types import ModuleType
from typing import Any

import pytest

_REQUIRED = ["--server", "esxi", "--user", "root", "--datastore", "ds1", "--vm-name", "vm"]


def test_parse_args_defaults(ex: ModuleType) -> None:
    args = ex.parse_args(["repo#a/b/c", *_REQUIRED])

    assert (args.upload, args.guest_id, args.disk_bus) == ("stream", "otherGuest64", "sata")
    assert args.host is None and args.network is None and args.power_on is False


def test_parse_args_takes_the_overrides(ex: ModuleType) -> None:
    args = ex.parse_args(
        [
            "repo#a/b/c",
            *_REQUIRED,
            "--host",
            "esxi-2",
            "--network",
            "VM Network",
            "--guest-id",
            "rhel9_64Guest",
            "--upload",
            "two-pass",
            "--disk-bus",
            "scsi",
            "--power-on",
        ]
    )

    assert (args.host, args.network, args.guest_id) == ("esxi-2", "VM Network", "rhel9_64Guest")
    assert (args.upload, args.disk_bus, args.power_on) == ("two-pass", "scsi", True)


def test_parse_args_names_the_missing_options(ex: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", "--server", "esxi"])

    assert "required unless --dry-run: --vm-name, --user, --datastore" in capsys.readouterr().err


def test_parse_args_rejects_an_unknown_upload_mode(ex: ModuleType) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", *_REQUIRED, "--upload", "staged"])


def test_main_takes_the_password_from_vsphere_password(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[Any, str]] = []

    async def fake_restore(args: Any, password: str) -> None:
        seen.append((args.server, password))

    monkeypatch.setattr(ex, "restore", fake_restore)
    monkeypatch.setenv("VSPHERE_PASSWORD", "from-env")

    ex.main(["repo#a/b/c", *_REQUIRED])

    assert seen == [("esxi", "from-env")]
