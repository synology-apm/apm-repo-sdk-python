"""Tests for the command line and output helpers of examples/restore_to_nutanix_ahv.py: ``parse_args``,
``resolve_password``, ``main``, ``lun_url``, ``DiskProgress`` and ``stage``."""

from __future__ import annotations

import asyncio
import io
import sys
from types import ModuleType
from typing import Any

import pytest

from synology_apm_repo.sdk import ApmRepoError, NotFoundError

_REQUIRED = ["--pc-host", "pc", "--pc-user", "u", "--cluster", "cl", "--container", "ct", "--dsip", "10.0.0.9"]


# -- parse_args ------------------------------------------------------------------


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


# -- resolve_password / main -----------------------------------------------------


def test_resolve_password_prefers_the_environment(ex: ModuleType) -> None:
    def refuse(prompt: str) -> str:
        raise AssertionError("must not prompt")

    assert ex.resolve_password({"NTNX_PASSWORD": "from-env"}, dry_run=False, prompt=refuse) == "from-env"


def test_resolve_password_prompts_when_the_environment_has_none(ex: ModuleType) -> None:
    prompts: list[str] = []

    def prompt(text: str) -> str:
        prompts.append(text)
        return "typed"

    assert ex.resolve_password({}, dry_run=False, prompt=prompt) == "typed"
    assert prompts == ["Prism Central password: "]


def test_resolve_password_is_empty_for_a_dry_run_without_prompting(ex: ModuleType) -> None:
    def refuse(prompt: str) -> str:
        raise AssertionError("must not prompt")

    assert ex.resolve_password({}, dry_run=True, prompt=refuse) == ""
    assert ex.resolve_password({"NTNX_PASSWORD": ""}, dry_run=True, prompt=refuse) == ""


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


# -- small helpers -----------------------------------------------------------------


def test_lun_url_addresses_the_target_and_lun(ex: ModuleType) -> None:
    assert ex.lun_url("10.0.0.9", "iqn.p:", "vg-target", 3) == "iscsi://10.0.0.9:3260/iqn.p:vg-target/3"


# -- DiskProgress -------------------------------------------------------------------


class _Stream(io.StringIO):
    def __init__(self, *, tty: bool) -> None:
        super().__init__()
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty


async def test_disk_progress_prints_plain_lines_when_not_a_tty(ex: ModuleType) -> None:
    stream = _Stream(tty=False)
    progress = ex.DiskProgress("disk 1/2", stream)

    await progress.update(5 * 2**30, 10 * 2**30)
    progress.finish()

    lines = stream.getvalue().splitlines()
    assert len(lines) == 2  # the first update, then the final line from finish()
    for line in lines:
        assert line.startswith("disk 1/2   50%  5.0 GiB/10.0 GiB")
        assert "elapsed" in line
    assert "\r" not in stream.getvalue() and "\033" not in stream.getvalue()


async def test_disk_progress_rewrites_one_line_on_a_tty(ex: ModuleType) -> None:
    stream = _Stream(tty=True)
    progress = ex.DiskProgress("disk 1/1", stream)

    await progress.update(2**30, 4 * 2**30)
    assert stream.getvalue().startswith("\r") and stream.getvalue().endswith("\033[K")  # not finished: no newline
    progress.finish()

    assert stream.getvalue().endswith("\033[K\n")


async def test_disk_progress_reports_a_zero_total_as_complete(ex: ModuleType) -> None:
    stream = _Stream(tty=False)
    progress = ex.DiskProgress("disk 1/1", stream)

    await progress.update(0, 0)

    assert "100%" in stream.getvalue()


def test_disk_progress_finish_before_any_update_prints_nothing(ex: ModuleType) -> None:
    stream = _Stream(tty=False)

    ex.DiskProgress("disk 1/1", stream).finish()

    assert stream.getvalue() == ""


def test_disk_progress_defaults_to_stderr(ex: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    progress = ex.DiskProgress("disk 1/1")

    progress._emit("hello", final=True)

    assert capsys.readouterr().err == "hello\n"


# -- stage ----------------------------------------------------------------------------


async def test_stage_announces_and_reports_how_long_the_step_took(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    async with ex.stage("opening repository"):
        pass

    lines = capsys.readouterr().err.splitlines()
    assert lines[0] == "opening repository..."
    assert lines[-1].startswith("opening repository: ") and lines[-1].endswith("s")


class _TickWatch(io.StringIO):
    """``sys.stderr`` stand-in that sets ``ticked`` once a tick line is written."""

    def __init__(self) -> None:
        super().__init__()
        self.ticked = asyncio.Event()

    def write(self, text: str) -> int:
        if "elapsed" in text:
            self.ticked.set()
        return super().write(text)


async def test_stage_reports_elapsed_time_while_a_slow_step_runs(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    stderr = _TickWatch()
    monkeypatch.setattr(sys, "stderr", stderr)

    async with ex.stage("listing", tick_seconds=0):
        await asyncio.wait_for(stderr.ticked.wait(), 10)  # the step lasts until a tick was printed

    lines = stderr.getvalue().splitlines()
    assert any(line.startswith("listing... ") and "elapsed" in line for line in lines)


async def test_stage_stops_ticking_and_does_not_claim_success_when_the_step_fails(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(ValueError, match="step failed"):
        async with ex.stage("listing", tick_seconds=0):
            raise ValueError("step failed")

    err = capsys.readouterr().err
    assert err == "listing...\n"
    # At a 0 s interval a live ticker prints on every event-loop turn.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert capsys.readouterr().err == ""  # the ticker was cancelled


async def test_stage_uses_a_slower_tick_when_not_a_tty(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    intervals: list[float] = []
    slept = asyncio.Event()
    real_sleep = asyncio.sleep

    async def spy_sleep(seconds: float, *args: Any) -> None:
        intervals.append(seconds)
        slept.set()
        await real_sleep(0, *args)

    monkeypatch.setattr(ex.asyncio, "sleep", spy_sleep)
    monkeypatch.setattr(sys.stderr, "isatty", lambda: False, raising=False)

    async with ex.stage("step"):
        await asyncio.wait_for(slept.wait(), 10)  # the ticker reached its first sleep

    assert intervals and intervals[0] == 15.0
