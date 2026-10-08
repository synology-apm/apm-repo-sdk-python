"""Tests for scripts/bench_export.py's argument parsing, ``matrix``'s child
command line and aggregation, and its report formatting; no export is
measured (``run``'s measurement and ``matrix``'s subprocesses are faked)."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from support.modules import load_module

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "bench_export.py"
_MIB = 1 << 20
_REF = "cat:3/wl:3/ver:vuid-1/device:5/object:1"

# The script imports ``resource``, which Windows lacks.
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="scripts/bench_export.py is POSIX only")


@pytest.fixture
def bench() -> ModuleType:
    return load_module("bench_export", _SCRIPT_PATH)


def _parse(bench: ModuleType, argv: list[str], monkeypatch: pytest.MonkeyPatch) -> argparse.Namespace:
    """``main(argv)``'s parsed namespace, with both handlers replaced."""
    seen: list[argparse.Namespace] = []

    def record(args: argparse.Namespace) -> int:
        seen.append(args)
        return 7

    monkeypatch.setattr(bench, "_run_command", record)
    monkeypatch.setattr(bench, "_matrix_command", record)
    assert bench.main(argv) == 7
    [args] = seen
    return args


def _record(**overrides: Any) -> dict[str, Any]:
    """One ``run`` JSON record, as ``_run_command`` prints it."""
    record: dict[str, Any] = {
        "mode": "default",
        "wall_s": 2.0,
        "parent_cpu_s": 1.0,
        "child_cpu_s": 4.0,
        "parent_peak_rss_mib": 100.0,
        "child_peak_rss_mib": 50.0,
        "bytes_written": 512 * _MIB,
        "logical_size": 1024 * _MIB,
        "holes": 1,
        "zeros": 2,
        "sha256": None,
        "store_calls": None,
        "allocated_mib": None,
        "wchar_mib": None,
        "sink_wait_s": None,
        "throughput_mib_s": 256.0,
    }
    return record | overrides


class TestParsing:
    def test_run_defaults(self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
        args = _parse(bench, ["run", "--repo", "/repo", "--ref", _REF], monkeypatch)
        assert args.command == "run"
        assert (args.repo, args.sample, args.ref, args.mode) == ("/repo", "", _REF, "default")
        assert (args.window_entries, args.workers, args.view_mib, args.view_offset_mib) == (0, 0, 0, 0)
        assert (args.sink_mib_s, args.sink_model) == (0.0, "queued")
        assert (args.segment_mib, args.buffered_segments, args.storage) == (64, 2, "memory")
        assert not any((args.count_calls, args.dev_null, args.dense, args.no_progress, args.hash))
        assert not hasattr(args, "repeat")

    def test_matrix_adds_its_own_options(self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
        args = _parse(bench, ["matrix", "--ref", _REF], monkeypatch)
        assert (args.command, args.repeat, args.warmup, args.label) == ("matrix", 5, 1, "")
        args = _parse(
            bench, ["matrix", "--ref", _REF, "--repeat", "3", "--warmup", "0", "--label", "baseline"], monkeypatch
        )
        assert (args.repeat, args.warmup, args.label) == (3, 0, "baseline")

    def test_every_run_option_parses(self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
        argv = [
            "run",
            "--sample",
            "nas-a",
            "--ref",
            _REF,
            "--mode",
            "segmented",
            "--workdir",
            "/scratch",
            "--window-entries",
            "128",
            "--workers",
            "3",
            "--view-mib",
            "16",
            "--view-offset-mib",
            "4",
            "--count-calls",
            "--dev-null",
            "--sink-mib-s",
            "12.5",
            "--sink-model",
            "inline",
            "--dense",
            "--no-progress",
            "--segment-mib",
            "8",
            "--buffered-segments",
            "4",
            "--storage",
            "spool",
            "--hash",
        ]
        args = _parse(bench, argv, monkeypatch)
        assert (args.sample, args.mode, args.workdir) == ("nas-a", "segmented", "/scratch")
        assert (args.window_entries, args.workers, args.view_mib, args.view_offset_mib) == (128, 3, 16, 4)
        assert (args.sink_mib_s, args.sink_model) == (12.5, "inline")
        assert (args.segment_mib, args.buffered_segments, args.storage) == (8, 4, "spool")
        assert all((args.count_calls, args.dev_null, args.dense, args.no_progress, args.hash))

    @pytest.mark.parametrize(
        "argv",
        [
            pytest.param([], id="no_command"),
            pytest.param(["run"], id="no_ref"),
            pytest.param(["run", "--ref", _REF, "--mode", "turbo"], id="unknown_mode"),
            pytest.param(["run", "--ref", _REF, "--storage", "disk"], id="unknown_storage"),
            pytest.param(["run", "--ref", _REF, "--sink-model", "async"], id="unknown_sink_model"),
            pytest.param(["run", "--ref", _REF, "--repeat", "3"], id="matrix_option_on_run"),
        ],
    )
    def test_rejects_an_invalid_command_line(
        self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch, argv: list[str]
    ) -> None:
        with pytest.raises(SystemExit) as exc_info:
            _parse(bench, argv, monkeypatch)
        assert exc_info.value.code == 2

    def test_mode_choices_are_the_documented_modes(self, bench: ModuleType) -> None:
        assert bench.MODES == ("default", "inprocess", "stream", "sink-parent", "segmented", "plan-only", "count-only")
        for mode in bench.MODES:
            assert f"``{mode}``" in (bench.__doc__ or "")


class TestRunCommand:
    def test_prints_the_measured_result_as_one_json_line(
        self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        result = bench.RunResult(
            mode="stream",
            wall_s=4.0,
            parent_cpu_s=1.5,
            child_cpu_s=0.0,
            parent_peak_rss_mib=80.0,
            child_peak_rss_mib=0.0,
            bytes_written=100 * _MIB,
            logical_size=200 * _MIB,
            holes=0,
            zeros=0,
        )

        async def fake_measure(args: argparse.Namespace) -> Any:
            assert args.mode == "stream"
            return result

        monkeypatch.setattr(bench, "_measure", fake_measure)
        assert bench.main(["run", "--repo", "/repo", "--ref", _REF, "--mode", "stream"]) == 0
        printed = json.loads(capsys.readouterr().out)
        assert printed == dataclasses.asdict(result) | {"throughput_mib_s": 25.0}


class TestMatrixCommand:
    def test_runs_warmup_plus_repeat_children_and_summarizes_only_the_measured_ones(
        self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        calls: list[list[str]] = []
        walls = iter([99.0, 1.0, 3.0, 2.0])

        def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            assert kwargs == {"check": True, "capture_output": True, "text": True}
            calls.append(cmd)
            # Only the last stdout line is the record; earlier ones are noise.
            stdout = "progress noise\n" + json.dumps(_record(wall_s=next(walls))) + "\n"
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout)

        monkeypatch.setattr(subprocess, "run", fake_run)
        assert bench.main(["matrix", "--repo", "/repo", "--ref", _REF, "--repeat", "3", "--warmup", "1"]) == 0

        assert len(calls) == 4
        report = capsys.readouterr().out
        # The 99s warmup run is discarded: median 2.0 of [1, 3, 2], min 1, max 3.
        assert "wall   2.00s (min 1.00 max 3.00)" in report

    def test_each_child_gets_the_run_options_and_only_the_enabled_flags(
        self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(_record()))

        monkeypatch.setattr(subprocess, "run", fake_run)
        bench.main(
            [
                "matrix",
                "--sample",
                "nas-a",
                "--ref",
                _REF,
                "--mode",
                "sink-parent",
                "--workdir",
                "/scratch",
                "--sink-mib-s",
                "50",
                "--sink-model",
                "inline",
                "--hash",
                "--dense",
                "--repeat",
                "1",
                "--warmup",
                "0",
            ]
        )

        [cmd] = calls
        assert cmd[:3] == [sys.executable, str(_SCRIPT_PATH), "run"]
        options = dict(zip(cmd[3:-2:2], cmd[4:-2:2], strict=True))
        assert options == {
            "--repo": "",
            "--sample": "nas-a",
            "--ref": _REF,
            "--mode": "sink-parent",
            "--workdir": "/scratch",
            "--window-entries": "0",
            "--workers": "0",
            "--view-mib": "0",
            "--view-offset-mib": "0",
            "--sink-mib-s": "50.0",
            "--sink-model": "inline",
            "--segment-mib": "64",
            "--buffered-segments": "2",
            "--storage": "memory",
        }
        assert cmd[-2:] == ["--dense", "--hash"]

    def test_every_boolean_flag_is_forwarded(self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(_record()))

        monkeypatch.setattr(subprocess, "run", fake_run)
        flags = ["--dense", "--no-progress", "--hash", "--dev-null", "--count-calls"]
        bench.main(["matrix", "--ref", _REF, "--repeat", "1", "--warmup", "0", *flags])
        [cmd] = calls
        assert cmd[-5:] == flags

    def test_a_failing_child_stops_the_matrix(self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            raise subprocess.CalledProcessError(1, cmd)

        monkeypatch.setattr(subprocess, "run", fake_run)
        with pytest.raises(subprocess.CalledProcessError):
            bench.main(["matrix", "--ref", _REF])


def _summary_args(label: str = "", mode: str = "default") -> argparse.Namespace:
    return argparse.Namespace(label=label, mode=mode)


class TestSummarize:
    def test_reports_medians_of_the_runs(self, bench: ModuleType) -> None:
        runs = [
            _record(wall_s=1.0, throughput_mib_s=100.0, parent_cpu_s=1.0, child_peak_rss_mib=10.0),
            _record(wall_s=5.0, throughput_mib_s=300.0, parent_cpu_s=3.0, child_peak_rss_mib=30.0),
            _record(wall_s=2.0, throughput_mib_s=200.0, parent_cpu_s=2.0, child_peak_rss_mib=20.0),
        ]
        line = bench._summarize(_summary_args(), runs)
        assert line.startswith("default ")
        assert "wall   2.00s (min 1.00 max 5.00)" in line
        assert "    200 MiB/s" in line
        assert "cpu parent   2.0s child    4.0s" in line
        assert "rss parent    100 MiB child-max    20 MiB" in line
        assert "written      512 MiB" in line

    def test_the_label_replaces_the_mode(self, bench: ModuleType) -> None:
        line = bench._summarize(_summary_args(label="no progress", mode="inprocess"), [_record()])
        assert line.startswith(f"{'no progress':<28} wall")

    def test_optional_figures_are_left_out_when_absent(self, bench: ModuleType) -> None:
        line = bench._summarize(_summary_args(), [_record()])
        for absent in ("allocated", "write syscalls", "store calls", "sha256"):
            assert absent not in line

    def test_optional_figures_are_reported_when_present(self, bench: ModuleType) -> None:
        calls = {"read": [12, 1.5], "exists": [3, 0.25]}
        runs = [
            _record(allocated_mib=100.0, wchar_mib=500.0, store_calls=calls, sha256="ab" * 32),
            _record(allocated_mib=300.0, wchar_mib=700.0, store_calls=None, sha256="ab" * 32),
        ]
        line = bench._summarize(_summary_args(), runs)
        assert "  allocated 200 MiB" in line
        assert "  write syscalls 600 MiB" in line
        # From the first run, sorted by method.
        assert "  store calls exists: 3x 0.25s, read: 12x 1.50s" in line
        assert line.endswith("  sha256 abababababab")

    def test_differing_hashes_are_reported_as_a_mismatch(self, bench: ModuleType) -> None:
        runs = [_record(sha256="aa" * 32), _record(sha256="bb" * 32), _record(sha256=None)]
        line = bench._summarize(_summary_args(), runs)
        assert "sha256 MISMATCH" in line
        assert "aa" * 32 in line and "bb" * 32 in line


class TestRunResult:
    def _result(self, bench: ModuleType, *, wall_s: float, bytes_written: int) -> Any:
        return bench.RunResult(
            mode="default",
            wall_s=wall_s,
            parent_cpu_s=0.0,
            child_cpu_s=0.0,
            parent_peak_rss_mib=0.0,
            child_peak_rss_mib=0.0,
            bytes_written=bytes_written,
            logical_size=bytes_written,
            holes=0,
            zeros=0,
        )

    def test_throughput_is_mib_written_per_wall_second(self, bench: ModuleType) -> None:
        assert self._result(bench, wall_s=2.0, bytes_written=300 * _MIB).throughput_mib_s == 150.0

    def test_throughput_of_a_zero_length_run_is_zero(self, bench: ModuleType) -> None:
        assert self._result(bench, wall_s=0.0, bytes_written=_MIB).throughput_mib_s == 0.0


class TestHelpers:
    @pytest.mark.parametrize(
        ("platform", "ru_maxrss", "expected"),
        [
            pytest.param("darwin", 3 * _MIB, 3.0, id="darwin_reports_bytes"),
            pytest.param("linux", 3 * 1024, 3.0, id="linux_reports_kilobytes"),
        ],
    )
    def test_rss_mib(
        self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch, platform: str, ru_maxrss: int, expected: float
    ) -> None:
        monkeypatch.setattr(sys, "platform", platform)
        assert bench._rss_mib(ru_maxrss) == expected

    def test_file_sha256(self, bench: ModuleType, tmp_path: Path) -> None:
        path = tmp_path / "out.bin"
        data = b"exported bytes" * 1000
        path.write_bytes(data)
        assert bench._file_sha256(path) == hashlib.sha256(data).hexdigest()

    def test_count_calls_totals_each_method(self, bench: ModuleType) -> None:
        events = [
            SimpleNamespace(method="read", elapsed=0.5),
            SimpleNamespace(method="size", elapsed=0.25),
            SimpleNamespace(method="read", elapsed=1.0),
        ]
        assert bench._count_calls(events) == {"read": [2, 1.5], "size": [1, 0.25]}

    def test_fresh_output_dir_creates_it_and_removes_a_stale_output(self, bench: ModuleType, tmp_path: Path) -> None:
        workdir = tmp_path / "a" / "b"
        assert bench._fresh_output_dir(workdir) == workdir
        (workdir / "bench.out").write_bytes(b"stale")
        (workdir / "keep.txt").write_bytes(b"other")
        bench._fresh_output_dir(workdir)
        assert sorted(p.name for p in workdir.iterdir()) == ["keep.txt"]

    def test_wchar_reads_proc_self_io(self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        io = tmp_path / "io"
        io.write_text("rchar: 10\nwchar: 4096\nsyscr: 1\n")
        real_open = open
        monkeypatch.setattr(
            "builtins.open", lambda path, *a, **k: real_open(io if path == "/proc/self/io" else path, *a, **k)
        )
        assert bench._wchar() == 4096

    @pytest.mark.parametrize(
        "content",
        [pytest.param(None, id="no_proc_file"), pytest.param("rchar: 10\n", id="no_wchar_line")],
    )
    def test_wchar_is_none_without_a_wchar_figure(
        self, bench: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, content: str | None
    ) -> None:
        io = tmp_path / "io"
        if content is not None:
            io.write_text(content)
        real_open = open
        monkeypatch.setattr(
            "builtins.open", lambda path, *a, **k: real_open(io if path == "/proc/self/io" else path, *a, **k)
        )
        assert bench._wchar() is None
