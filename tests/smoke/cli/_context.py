"""Run state for one ``python -m tests.smoke.cli`` invocation -- records
one real subprocess invocation per step (``ctx.run``), plus a pure
output-shape assertion (``ctx.check``, kept unlike the reference
project's own CLI context, which dropped it -- those structural
assertions are exactly the CLI-specific value this tool exists to add:
"does `--json` and the table agree", "does an id-looking field appear
only with `--verbose`").
"""

from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .._context import DomainStats, StepResult, StepStatus, _truncate, step_slug, to_jsonable
from ._cli_runner import CliResult, CliRunner

#: Strips ANSI escape sequences (cursor moves, colors, rich's live-updating
#: progress bars) and any other C0 control byte but newline/tab, before
#: writing stdout/stderr into the Markdown report -- real output is
#: otherwise unreadable in a plain editor and defeats ``grep`` (a file
#: with raw escape/control bytes reads as binary).
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b[()][A-Za-z0-9]|\x1b.")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


def _clean(text: str) -> str:
    return _CONTROL_RE.sub("", _ANSI_RE.sub("", text))


#: The five domains this tool's phases are split across -- see
#: ``phases/_<domain>.py`` and this package's README for what each covers.
DOMAINS = ("commands", "global_flags", "export_lifecycle", "profile", "errors")


@dataclass
class SmokeContext:
    report_dir: Path
    runner: CliRunner = field(default_factory=CliRunner)
    data: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, DomainStats] = field(default_factory=lambda: {d: DomainStats() for d in DOMAINS})
    step_results: dict[str, list[StepResult]] = field(default_factory=lambda: {d: [] for d in DOMAINS})

    _files: dict[str, io.TextIOWrapper] = field(default_factory=dict, repr=False)
    _emitted: set[tuple[str, str]] = field(default_factory=set, repr=False)

    def __post_init__(self) -> None:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        for domain in DOMAINS:
            f = (self.report_dir / f"{domain}.md").open("w", encoding="utf-8")
            f.write(f"# {domain} -- CLI smoke test report\n\n")
            self._files[domain] = f

    def _mark(self, domain: str, step: str) -> None:
        self._emitted.add((domain, step))

    def run(
        self,
        domain: str,
        step: str,
        *args: str,
        env_overrides: dict[str, str] | None = None,
        expect_exit: int = 0,
        note: str = "",
        **kwargs: Any,
    ) -> CliResult:
        """Run one real ``synology-apm-repo-cli`` invocation, recorded
        into ``<domain>.md`` as step ``step``. PASSED when
        ``result.exit_code == expect_exit``, FAILED otherwise (a
        subprocess timeout is always FAILED regardless of
        ``expect_exit``) -- there is no DEGRADED here, unlike ``sdk/``'s
        ``ctx.call``: a real subprocess invocation either exits the code
        this smoke test expected or it didn't, there's no equivalent of a
        known, sample-specific data gap at this layer."""
        result = self.runner.run(*args, env_overrides=env_overrides, **kwargs)
        return self._record(domain, step, result, expect_exit=expect_exit, note=note)

    def run_cancellable(
        self,
        domain: str,
        step: str,
        *args: str,
        cancel_after: float,
        env_overrides: dict[str, str] | None = None,
        expect_exit: int = 0,
        note: str = "",
        **kwargs: Any,
    ) -> CliResult:
        """Same recording as ``run``, driving ``CliRunner.
        run_cancellable`` instead -- a real, timed ``SIGINT`` mid-
        invocation. ``expect_exit`` defaults to ``0``: a first-press
        Ctrl-C during ``export`` is a *clean*, handled cancellation
        (``export.py``'s own ``except asyncio.CancelledError`` branch
        just prints a message and returns), not a failure exit."""
        result = self.runner.run_cancellable(*args, cancel_after=cancel_after, env_overrides=env_overrides, **kwargs)
        return self._record(domain, step, result, expect_exit=expect_exit, note=note)

    def _record(self, domain: str, step: str, result: CliResult, *, expect_exit: int, note: str) -> CliResult:
        stats = self.stats[domain]
        self._mark(domain, step)
        stats.ran += 1
        ok = not result.timed_out and result.exit_code == expect_exit
        status: StepStatus = "passed" if ok else "failed"
        if not ok:
            stats.unexpected += 1
        self._write_run(domain, step, result, expect_exit=expect_exit, status=status, note=note)
        return result

    def check(self, domain: str, step: str, condition: bool, *, note: str = "") -> bool:
        """Record a pure, no-subprocess assertion over already-captured
        output -- PASSED/FAILED, never SKIPPED."""
        stats = self.stats[domain]
        self._mark(domain, step)
        if condition:
            stats.checks_passed += 1
            status: StepStatus = "passed"
            label = "PASSED"
        else:
            stats.checks_failed += 1
            status = "failed"
            label = "FAILED"
        self.step_results[domain].append(StepResult(step, status=status, label=label, has_detail=False, note=note))
        return condition

    def skip(self, domain: str, step: str, reason: str) -> None:
        """Record a conditional skip -- prerequisite data this sample set
        legitimately doesn't have, not a hard failure."""
        self._mark(domain, step)
        self.stats[domain].skipped += 1
        self.step_results[domain].append(
            StepResult(step, status="skipped", label=f"SKIPPED: {reason}", has_detail=False)
        )

    def _write_run(
        self,
        domain: str,
        step: str,
        result: CliResult,
        *,
        expect_exit: int,
        status: StepStatus,
        note: str = "",
    ) -> None:
        slug = step_slug(step)
        f = self._files[domain]
        f.write(f'<a id="{slug}"></a>\n')
        f.write(f"### `{step}`\n\n")
        f.write(f"- argv: `synology-apm-repo-cli --no-input {' '.join(result.args)}`\n")
        f.write(f"- exit_code: {result.exit_code} (expected {expect_exit})\n")
        if result.timed_out:
            f.write("- timed out\n")
        stdout_display, stdout_note = _truncate(_clean(result.stdout).splitlines())
        f.write("```\n")
        f.write("\n".join(stdout_display) if isinstance(stdout_display, list) else json.dumps(to_jsonable(result)))
        f.write("\n```\n")
        if stdout_note:
            f.write(f"\n{stdout_note}\n")
        if result.stderr:
            f.write("\nstderr:\n```\n")
            f.write(_clean(result.stderr))
            f.write("\n```\n")
        f.write("\n")
        f.flush()

        label = "PASSED" if status == "passed" else f"FAILED: exit_code={result.exit_code}"
        self.step_results[domain].append(StepResult(step, status=status, label=label, has_detail=True, note=note))

    def close(self) -> None:
        for f in self._files.values():
            f.close()
