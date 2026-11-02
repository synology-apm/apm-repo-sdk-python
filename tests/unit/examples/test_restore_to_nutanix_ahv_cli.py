"""Tests for the command line of examples/hypervisor-restore/restore_to_nutanix_ahv.py: ``parse_args``, ``main``
and ``lun_url``."""

from __future__ import annotations

from types import ModuleType
from typing import Any

import pytest

from synology_apm_repo.sdk import ApmRepoError, NotFoundError

_REQUIRED = ["--pc-host", "pc", "--pc-user", "u", "--cluster", "cl", "--container", "ct", "--dsip", "10.0.0.9"]


def test_parse_args_defaults_to_one_cpu_and_two_gib(ex: ModuleType) -> None:
    args = ex.parse_args(["repo#a/b/c", *_REQUIRED, "--vm-name", "vm"])

    assert (args.cpus, args.memory_gib) == (1, 2)
    assert args.firmware == "auto"
    assert args.profile is None and args.key is None
    assert args.keep_volume_group is False and args.insecure is False and args.dry_run is False
    assert args.iqn_prefix == "iqn.2010-06.com.nutanix:"


def test_parse_args_takes_the_overrides(ex: ModuleType) -> None:
    args = ex.parse_args(
        [
            "repo#a/b/c",
            *_REQUIRED,
            "--vm-name",
            "vm",
            "--cpus",
            "4",
            "--memory-gib",
            "16",
            "--firmware",
            "uefi",
            "--profile",
            "p",
            "--key",
            "id@k",
            "--insecure",
            "--keep-volume-group",
            "--initiator-iqn",
            "iqn.x",
        ]
    )

    assert (args.cpus, args.memory_gib, args.firmware) == (4, 16, "uefi")
    assert (args.profile, args.key, args.initiator_iqn) == ("p", "id@k", "iqn.x")
    assert args.insecure and args.keep_volume_group


def test_parse_args_dry_run_needs_only_the_ref(ex: ModuleType) -> None:
    args = ex.parse_args(["repo#a/b/c", "--dry-run"])

    assert args.dry_run is True and args.pc_host is None and args.vm_name is None


def test_parse_args_names_every_missing_option_for_a_real_run(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as caught:
        ex.parse_args(["repo#a/b/c", "--pc-host", "pc"])

    assert caught.value.code == 2
    err = capsys.readouterr().err
    assert "required unless --dry-run" in err
    for option in ("--pc-user", "--cluster", "--container", "--dsip", "--vm-name"):
        assert option in err
    assert "--pc-host," not in err  # the one that was given is not listed


@pytest.mark.parametrize("name", ["it's", 'say "hi"', "web'01"])
def test_parse_args_rejects_a_vm_name_with_quotes(
    ex: ModuleType, capsys: pytest.CaptureFixture[str], name: str
) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", *_REQUIRED, "--vm-name", name])

    assert "--vm-name cannot contain quotes" in capsys.readouterr().err


@pytest.mark.parametrize("option", ["--cpus", "--memory-gib"])
@pytest.mark.parametrize("value", ["0", "-1"])
def test_parse_args_rejects_a_cpu_or_memory_count_below_one(
    ex: ModuleType, capsys: pytest.CaptureFixture[str], option: str, value: str
) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", *_REQUIRED, "--vm-name", "vm", option, value])

    assert "--cpus and --memory-gib must be at least 1" in capsys.readouterr().err


def test_parse_args_accepts_a_vm_name_with_spaces_and_punctuation(ex: ModuleType) -> None:
    args = ex.parse_args(["repo#a/b/c", *_REQUIRED, "--vm-name", "web 01 (restored) #2"])

    assert args.vm_name == "web 01 (restored) #2"


def test_parse_args_rejects_an_unknown_firmware(ex: ModuleType) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", "--dry-run", "--firmware", "coreboot"])


def test_parse_args_needs_a_ref(ex: ModuleType) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["--dry-run"])


def test_main_runs_the_restore_with_the_parsed_arguments(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    seen: dict[str, Any] = {}

    async def fake_restore(args: Any, password: str) -> None:
        seen["args"], seen["password"] = args, password

    monkeypatch.setattr(ex, "restore", fake_restore)
    monkeypatch.setenv("NTNX_PASSWORD", "from-env")

    ex.main(["repo#a/b/c", *_REQUIRED, "--vm-name", "vm"])

    assert seen["password"] == "from-env" and seen["args"].vm_name == "vm"
    assert "done in" in capsys.readouterr().err


def test_main_turns_a_repository_error_into_a_clean_exit(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    async def failing_restore(args: Any, password: str) -> None:
        raise NotFoundError("no workload named 'x'", ref="x")

    monkeypatch.setattr(ex, "restore", failing_restore)

    with pytest.raises(SystemExit, match="error: no workload named") as caught:
        ex.main(["repo#a/b/c", "--dry-run"])

    assert isinstance(caught.value.__cause__, ApmRepoError)
    assert str(caught.value).startswith("error: no workload named 'x'")


def test_main_lets_other_errors_through(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    async def broken_restore(args: Any, password: str) -> None:
        raise RuntimeError("bug")

    monkeypatch.setattr(ex, "restore", broken_restore)

    with pytest.raises(RuntimeError, match="bug"):
        ex.main(["repo#a/b/c", "--dry-run"])


def test_lun_url_addresses_the_target_and_lun(ex: ModuleType) -> None:
    assert ex.lun_url("10.0.0.9", "iqn.p:", "vg-target", 3) == "iscsi://10.0.0.9:3260/iqn.p:vg-target/3"
