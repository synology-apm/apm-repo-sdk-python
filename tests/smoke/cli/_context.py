"""Run state for one ``python -m tests.smoke.cli`` invocation: one real
subprocess invocation per step (``ctx.run``), plus output-shape assertions
(``ctx.check``) such as "does ``--json`` agree with the table".
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from .._context import ReportContext, StepResult, StepStatus, _truncate, to_jsonable
from ._cli_runner import CliResult, CliRunner

#: ``verify``'s exit statuses for a run that completed: clean (0) or with
#: unresolved findings (3) -- a sample's own data decides which.
VERIFY_COMPLETED = frozenset({0, 3})

#: Strips ANSI escape sequences (cursor moves, colors, rich's live-updating
#: progress bars) and any other C0 control byte but newline/tab, before
#: writing stdout/stderr into the Markdown report -- real output is
#: otherwise unreadable in a plain editor and defeats ``grep`` (a file
#: with raw escape/control bytes reads as binary).
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b[()][A-Za-z0-9]|\x1b.")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


def _clean(text: str) -> str:
    return _CONTROL_RE.sub("", _ANSI_RE.sub("", text))


#: One ``phases/_<domain>.py`` each.
DOMAINS = ("commands", "global_flags", "export_lifecycle", "profile", "errors", "remote_connect")


@dataclass
class SmokeContext(ReportContext):
    DOMAINS = DOMAINS
    TITLE = "CLI"

    runner: CliRunner = field(default_factory=CliRunner, kw_only=True)

    def run(
        self,
        domain: str,
        step: str,
        *args: str,
        env_overrides: dict[str, str] | None = None,
        expect_exit: int | frozenset[int] = 0,
        note: str = "",
        **kwargs: Any,
    ) -> CliResult:
        """Run one real ``synology-apm-repo-cli`` invocation, recorded
        into ``<domain>.md`` as step ``step``. PASSED when
        ``result.exit_code`` is ``expect_exit`` (or one of them), FAILED
        otherwise, and always FAILED on a subprocess timeout. There is no
        DEGRADED outcome here."""
        result = self.runner.run(*args, env_overrides=env_overrides, **kwargs)
        return self._record(domain, step, result, expect_exit=expect_exit, note=note)

    def run_cancellable(
        self,
        domain: str,
        step: str,
        *args: str,
        cancel_after: float,
        env_overrides: dict[str, str] | None = None,
        expect_exit: int = 130,
        note: str = "",
        **kwargs: Any,
    ) -> CliResult:
        """``run``, but through ``CliRunner.run_cancellable``: a real ``SIGINT``
        ``cancel_after`` seconds in. ``expect_exit`` defaults to 130
        (``ExitCode.CANCELLED``)."""
        result = self.runner.run_cancellable(*args, cancel_after=cancel_after, env_overrides=env_overrides, **kwargs)
        return self._record(domain, step, result, expect_exit=expect_exit, note=note)

    def _record(
        self, domain: str, step: str, result: CliResult, *, expect_exit: int | frozenset[int], note: str
    ) -> CliResult:
        stats = self.stats[domain]
        self._mark(domain, step)
        stats.ran += 1
        expected = expect_exit if isinstance(expect_exit, frozenset) else frozenset({expect_exit})
        ok = not result.timed_out and result.exit_code in expected
        status: StepStatus = "passed" if ok else "failed"
        if not ok:
            stats.unexpected += 1
        self._write_run(domain, step, result, expect_exit=expect_exit, status=status, note=note)
        return result

    def _write_run(
        self,
        domain: str,
        step: str,
        result: CliResult,
        *,
        expect_exit: int | frozenset[int],
        status: StepStatus,
        note: str = "",
    ) -> None:
        f = self._open_section(domain, step)
        f.write(f"- argv: `synology-apm-repo-cli --no-input {' '.join(result.args)}`\n")
        expected = expect_exit if isinstance(expect_exit, int) else " or ".join(map(str, sorted(expect_exit)))
        f.write(f"- exit_code: {result.exit_code} (expected {expected})\n")
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
