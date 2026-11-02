"""Tests for the command-line and output helpers of examples/hypervisor-restore/apm_restore_common.py:
add_common_args/check_common_args, resolve_password, restore_name, tls_context, run_main, DiskProgress and
stage."""

from __future__ import annotations

import argparse
import asyncio
import io
import ssl
import sys
from types import ModuleType
from typing import Any

import pytest


def test_resolve_password_prefers_the_environment(ex: ModuleType) -> None:
    def refuse(prompt: str) -> str:
        raise AssertionError("must not prompt")

    assert (
        ex.resolve_password({"X_PASSWORD": "from-env"}, "X_PASSWORD", "Hypervisor", dry_run=False, prompt=refuse)
        == "from-env"
    )


def test_resolve_password_prompts_when_the_environment_has_none(ex: ModuleType) -> None:
    prompts: list[str] = []

    def prompt(text: str) -> str:
        prompts.append(text)
        return "typed"

    assert ex.resolve_password({}, "X_PASSWORD", "Hypervisor", dry_run=False, prompt=prompt) == "typed"
    assert prompts == ["Hypervisor password: "]


def test_resolve_password_is_empty_for_a_dry_run_without_prompting(ex: ModuleType) -> None:
    def refuse(prompt: str) -> str:
        raise AssertionError("must not prompt")

    assert ex.resolve_password({}, "X_PASSWORD", "Hypervisor", dry_run=True, prompt=refuse) == ""
    assert ex.resolve_password({"X_PASSWORD": ""}, "X_PASSWORD", "Hypervisor", dry_run=True, prompt=refuse) == ""


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


# -- add_common_args / check_common_args -----------------------------------------


def _parse(ex: ModuleType, argv: list[str], *, disk_bus: bool = True) -> Any:
    parser = argparse.ArgumentParser()
    ex.add_common_args(parser, disk_bus=disk_bus)
    parser.add_argument("--server")
    args = parser.parse_args(argv)
    ex.check_common_args(parser, args, ("server",))
    return args


def test_common_args_defaults(ex: ModuleType) -> None:
    args = _parse(ex, ["repo#a/b/c", "--dry-run"])

    assert (args.cpus, args.memory_gib, args.firmware, args.disk_bus) == (1, 2, "auto", "sata")
    assert args.profile is None and args.key is None and args.vm_name is None
    assert args.insecure is False


def test_common_args_without_a_disk_bus(ex: ModuleType) -> None:
    assert not hasattr(_parse(ex, ["repo#a/b/c", "--dry-run"], disk_bus=False), "disk_bus")


def test_common_args_name_every_option_a_real_run_misses(ex: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        _parse(ex, ["repo#a/b/c"])

    assert "required unless --dry-run: --vm-name, --server" in capsys.readouterr().err


@pytest.mark.parametrize("option", ["--cpus", "--memory-gib"])
def test_common_args_reject_a_count_below_one(ex: ModuleType, capsys: pytest.CaptureFixture[str], option: str) -> None:
    with pytest.raises(SystemExit):
        _parse(ex, ["repo#a/b/c", "--vm-name", "vm", "--server", "s", option, "0"])

    assert "--cpus and --memory-gib must be at least 1" in capsys.readouterr().err


# -- small helpers ---------------------------------------------------------------


def test_restore_name_keeps_only_safe_characters(ex: ModuleType) -> None:
    name = ex.restore_name("apm", "web 01/é:" + "x" * 60)

    prefix, _, suffix = name.rpartition("-")
    assert prefix == "apm-web-01---" + "x" * 31
    assert len(suffix) == 6 and ex.restore_name("apm", "a") != ex.restore_name("apm", "a")


def test_tls_context_checks_certificates_unless_insecure(ex: ModuleType) -> None:
    strict = ex.tls_context(insecure=False)
    loose = ex.tls_context(insecure=True)

    assert (strict.verify_mode, strict.check_hostname) == (ssl.CERT_REQUIRED, True)
    assert (loose.verify_mode, loose.check_hostname) == (ssl.CERT_NONE, False)


# -- run_main --------------------------------------------------------------------


def _args(*, insecure: bool = False) -> argparse.Namespace:
    return argparse.Namespace(insecure=insecure)


def test_run_main_passes_the_password_and_reports_the_time(ex: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    seen: list[str] = []

    async def restore(args: argparse.Namespace, password: str) -> None:
        seen.append(password)

    ex.run_main(restore, _args(), "secret")

    assert seen == ["secret"]
    assert capsys.readouterr().err.startswith("done in ")


def test_run_main_prints_the_rollback_notes_before_the_error(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    async def restore(args: argparse.Namespace, password: str) -> None:
        error = ex.RestoreError("the upload failed")
        error.add_note("cleanup: could not delete volume v")
        raise error

    with pytest.raises(SystemExit, match=r"^error: the upload failed$"):
        ex.run_main(restore, _args(), "")

    assert capsys.readouterr().err == "cleanup: could not delete volume v\n"


def test_run_main_reports_a_cancelled_run_as_interrupted(ex: ModuleType) -> None:
    async def restore(args: argparse.Namespace, password: str) -> None:
        task = asyncio.current_task()
        assert task is not None
        task.cancel()  # what SIGTERM's handler does
        await asyncio.sleep(10)

    with pytest.raises(SystemExit, match="interrupted; what this run created was removed"):
        ex.run_main(restore, _args(), "")


def test_run_main_reports_what_an_interrupted_rollback_left_behind(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    async def leave(name: str) -> None:
        raise ex.RestoreError(f"could not delete {name}")

    async def restore(args: argparse.Namespace, password: str) -> None:
        async with ex.Rollback() as rollback:
            rollback.add(leave, "volume v")
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            await asyncio.sleep(10)

    with pytest.raises(SystemExit, match="interrupted; some of what this run created was left behind"):
        ex.run_main(restore, _args(), "")

    assert capsys.readouterr().err == "rollback step failed: could not delete volume v\n"


def test_run_main_silences_the_unverified_https_warning_only_when_insecure(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    filters: list[tuple[Any, ...]] = []
    monkeypatch.setattr(ex.warnings, "filters", filters)

    async def restore(args: argparse.Namespace, password: str) -> None:
        pass

    ex.run_main(restore, _args(), "")
    assert filters == []
    ex.run_main(restore, _args(insecure=True), "")
    assert [f[0] for f in filters] == ["ignore"]
